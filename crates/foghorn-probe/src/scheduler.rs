use crate::{
    cluster::{compute_clusters, ClusterInput},
    discovery::{get_opted_in_indexers, get_safe_block},
    executor::{
        DISPATCH_PAID,
        execute_gateway_probe, execute_paid_probe, execute_probe, GatewayProbeRequest,
        PaidProbeRequest, ProbeRequest, RawObservation,
    },
};
use anyhow::Result;
use chrono::Utc;
use foghorn_core::{config::FoghornConfig, deployment::normalise_deployment_id, types::TestSet};
use sha2::{Digest, Sha256};
use sqlx::PgPool;
use std::time::Duration;
use tracing::{error, info, warn};
use uuid::Uuid;

pub async fn run_probe_scheduler(config: FoghornConfig, pool: PgPool) -> Result<()> {
    info!(
        interval_secs = config.probe_interval_secs,
        gateway = config.gateway.is_some(),
        "Probe scheduler starting"
    );

    let mut test_sets = load_test_sets(&config.test_sets_dir)?;
    info!(count = test_sets.len(), "Curated test sets loaded");

    // Broaden correctness coverage: auto-discover the most-indexed deployments
    // and generate block-pinned probe queries via schema introspection.
    if config.auto_discover_limit > 0 {
        let discovered = crate::autodiscover::discover_test_sets(&config, config.auto_discover_limit).await;
        // Curated sets win over discovered ones for the same deployment: they carry hand-written
        // queries chosen to be deterministic, where auto-discovery guesses an entity. Before ids
        // were normalised these never collided, so ens and premia were each probed twice per round
        // under two different ids — double the traffic, and each deployment's rollup split across
        // two rows that each looked like half a deployment.
        let curated: std::collections::HashSet<String> =
            test_sets.iter().map(|ts| ts.deployment.id.clone()).collect();
        let before = discovered.len();
        let discovered: Vec<_> = discovered
            .into_iter()
            .filter(|ts| !curated.contains(&ts.deployment.id))
            .collect();
        if before != discovered.len() {
            info!(
                dropped = before - discovered.len(),
                "Discovered deployments already covered by a curated test set"
            );
        }
        test_sets.extend(discovered);
        info!(total = test_sets.len(), "Test sets after auto-discovery");
    }

    if test_sets.is_empty() {
        warn!("No test sets found in '{}' — probe scheduler will idle", config.test_sets_dir);
    }

    // Built once, before the loop. Absent when TAP is disabled or unconfigured, in which case
    // probing continues through the gateway exactly as before — a missing signer key must degrade
    // to the old behaviour, not to silence.
    let paid: Option<(tap_query::PaidQueryClient, usize)> = if config.tap.enabled {
        match build_paid_client(&config) {
            Ok(c) => {
                info!(payer = %config.tap.payer, "Paid direct dispatch enabled");
                Some(c)
            }
            Err(e) => {
                warn!(error = %e, "TAP enabled but the client could not be built — falling back to gateway dispatch");
                None
            }
        }
    } else {
        None
    };

    loop {
        match run_probe_round(&config, &pool, &test_sets, paid.as_ref()).await {
            Ok(n) => info!(probes = n, "Probe round complete"),
            Err(e) => error!(error = %e, "Probe round failed"),
        }

        // Resolve new allocation keys to real indexer addresses after each round.
        if let Some(gw) = &config.gateway {
            if let Err(e) = crate::resolver::resolve_allocation_keys(&pool, &gw.url, &gw.api_key).await {
                warn!(error = %e, "Allocation key resolution failed");
            }
        }

        tokio::time::sleep(Duration::from_secs(config.probe_interval_secs)).await;
    }
}

async fn run_probe_round(
    config: &FoghornConfig,
    pool: &PgPool,
    test_sets: &[TestSet],
    // Some(client, max_targets) when TAP is configured. Built once per round rather than per query:
    // constructing it parses a key and builds an HTTP client, neither of which belongs in a loop.
    paid_client: Option<&(tap_query::PaidQueryClient, usize)>,
) -> Result<usize> {
    let mut total_probes = 0;

    for test_set in test_sets {
        let network = &test_set.deployment.network;

        let (block_number, block_hash) = match config.rpc_urls.get(network) {
            Some(rpc_url) => match get_safe_block(rpc_url, config.reorg_threshold).await {
                Ok(b) => b,
                Err(e) => {
                    warn!(network = %network, error = %e, "Failed to get safe block, skipping deployment");
                    continue;
                }
            },
            None => {
                warn!(network = %network, "No RPC URL configured for this network, skipping");
                continue;
            }
        };

        info!(
            deployment = %test_set.deployment.description,
            block_number,
            "Starting probes for deployment"
        );

        for query in &test_set.queries {
            if query.category == "Q_freshness" {
                continue;
            }

            let parameterisations: Vec<Option<&str>> = if query.entity_ids.is_empty() {
                vec![None]
            } else {
                query.entity_ids.iter().map(|id| Some(id.as_str())).collect()
            };

            for entity_id_opt in &parameterisations {
                let final_query = match entity_id_opt {
                    Some(id) => query.template.replace("$id", id).replace("$block_hash", &block_hash),
                    None => query.template.replace("$block_hash", &block_hash),
                };

                let query_hash = {
                    let mut h = Sha256::new();
                    h.update(final_query.as_bytes());
                    hex::encode(h.finalize())
                };

                let probe_id = Uuid::new_v4();
                let now = Utc::now();

                // Paid direct dispatch takes priority when enabled and there are indexers we can
                // actually pay for this deployment. It is the only mode where WE choose who
                // answers, which is what makes the resulting success rate a measurement rather
                // than an upper bound.
                //
                // Falls back to the gateway rather than skipping: escrow propagation is slow (an
                // indexer's tap-agent has to observe the deposit before it stops denylisting us),
                // so early on most deployments have no payable target and gateway coverage is
                // better than none.
                let paid_targets: Vec<crate::allocations::PayableTarget> = if let Some(tap) = paid_client.as_ref() {
                    crate::allocations::payable_targets_for_deployment(
                        pool,
                        &test_set.deployment.ipfs_hash,
                        &config.tap.excluded_indexers,
                    )
                    .await
                    .unwrap_or_default()
                    .into_iter()
                    .take(tap.1)
                    .collect()
                } else {
                    Vec::new()
                };

                // Paid dispatch is ADDITIVE, never a replacement.
                //
                // It used to be exclusive: any deployment with a payable target was probed only
                // through paid dispatch. That looks right until you count what actually comes back.
                // Escrow propagation is slow and one-sided — an indexer's tap-agent must observe our
                // deposit before it stops denylisting us — and at the time of writing 15 of 17
                // funded indexers still refuse. Exclusive mode would therefore have swapped ~8
                // gateway observations per probe for 2 paid ones: less coverage, and far less
                // correctness signal, since clustering needs several indexers answering the
                // IDENTICAL probe and a majority of at least two before it will call anyone wrong.
                //
                // Running both against the same `probe_id` is strictly better than either alone.
                // Paid observations are unbiased (we choose the indexer); gateway observations are
                // broad but selection-biased (it routes to indexers it already trusts). Each is
                // tagged with how it was dispatched so neither claim has to be made about the other.
                let mut raw_observations: Vec<RawObservation> = Vec::new();

                if let (Some((client, _)), false) =
                    (paid_client.as_ref(), paid_targets.is_empty())
                {
                    let mut obs = Vec::new();
                    for target in &paid_targets {
                        let req = PaidProbeRequest {
                            indexer_address: target.indexer_address.clone(),
                            indexer_url: target.indexer_url.clone().unwrap_or_default(),
                            allocation_id: target.allocation_id.clone(),
                            deployment_ipfs_hash: test_set.deployment.ipfs_hash.clone(),
                            query: final_query.clone(),
                            block_hash: block_hash.clone(),
                            stake_weight: 1.0,
                        };
                        obs.push(execute_paid_probe(client, req).await);
                        if config.max_qps_per_indexer > 0.0 {
                            let delay_ms = (1000.0 / config.max_qps_per_indexer) as u64;
                            tokio::time::sleep(Duration::from_millis(delay_ms)).await;
                        }
                    }
                    raw_observations.extend(obs);
                }

                let fallback_observations = if let Some(gw) = &config.gateway {
                    // Gateway mode: fire probe_count queries, each may come from a different indexer
                    let subgraph_id = test_set
                        .deployment
                        .gateway_subgraph_id
                        .as_deref()
                        .unwrap_or(&test_set.deployment.ipfs_hash);

                    let mut obs = Vec::new();
                    for i in 0..gw.probe_count {
                        let req = GatewayProbeRequest {
                            gateway_url: gw.url.clone(),
                            api_key: gw.api_key.clone(),
                            subgraph_id: subgraph_id.to_string(),
                            _deployment_id: test_set.deployment.id.clone(),
                            query: final_query.clone(),
                            block_hash: block_hash.clone(),
                        };
                        obs.push(execute_gateway_probe(req).await);

                        // Small delay between gateway requests
                        if i + 1 < gw.probe_count {
                            tokio::time::sleep(Duration::from_millis(500)).await;
                        }
                    }
                    obs
                } else {
                    // Direct mode: probe opted-in indexers
                    let indexers = get_opted_in_indexers(config).await?;
                    if indexers.is_empty() {
                        warn!("No opted-in indexers configured and no gateway — skipping");
                        return Ok(0);
                    }
                    let mut obs = Vec::new();
                    for indexer in &indexers {
                        let stake_weight = parse_stake_weight(indexer.stake_grt.as_deref());
                        let req = ProbeRequest {
                            indexer_address: indexer.address.clone(),
                            indexer_url: indexer.url.clone(),
                            deployment_ipfs_hash: test_set.deployment.ipfs_hash.clone(),
                            query: final_query.clone(),
                            block_hash: block_hash.clone(),
                            auth_token: indexer.auth_token.clone(),
                            stake_weight,
                        };
                        obs.push(execute_probe(req).await);
                        if config.max_qps_per_indexer > 0.0 {
                            let delay_ms = (1000.0 / config.max_qps_per_indexer) as u64;
                            tokio::time::sleep(Duration::from_millis(delay_ms)).await;
                        }
                    }
                    obs
                };
                raw_observations.extend(fallback_observations);

                // Deduplicate by indexer_address — only keep one observation per address
                // (same allocation key = same indexer allocation)
                let deduped_observations = dedup_by_address(raw_observations);

                let cluster_inputs: Vec<ClusterInput> = deduped_observations
                    .iter()
                    .map(|o| ClusterInput {
                        indexer_address: o.indexer_address.clone(),
                        response_hash: o.response_hash.clone(),
                        raw_response: o.raw_response.clone(),
                        stake_weight: o.stake_weight,
                    })
                    .collect();

                let clusters = compute_clusters(&cluster_inputs);

                if clusters.is_divergent {
                    info!(
                        probe_id = %probe_id,
                        cluster_count = clusters.cluster_count,
                        "Divergence detected"
                    );
                }

                store_results(
                    pool,
                    probe_id,
                    &test_set.deployment.id,
                    block_number,
                    &block_hash,
                    &query_hash,
                    &query.category,
                    &final_query,
                    now,
                    &deduped_observations,
                    &clusters,
                )
                .await?;

                total_probes += 1;
            }
        }
    }

    Ok(total_probes)
}

/// Deduplicate observations: same indexer_address = same allocation, keep first.
/// One observation per indexer, keeping the most informative one.
///
/// This was first-wins, which was fine while every observation came from a single dispatch mode.
/// Once paid and gateway probes run against the same `probe_id`, first-wins becomes a silent data
/// loss: paid observations are pushed first, so a `payment_denylisted` — which is not an observation
/// of the indexer at all, only of our escrow — would evict a perfectly good gateway response from
/// the same operator. We would then have measured nothing and not noticed.
///
/// Rank: a real response beats any error; a genuine error (timeout, 500, bad data) beats a payment
/// refusal, because it at least describes the indexer.
fn dedup_by_address(obs: Vec<RawObservation>) -> Vec<RawObservation> {
    fn rank(o: &RawObservation) -> u8 {
        if o.response_hash.is_some() {
            2
        } else if !is_payment_error(o.error_class.as_deref()) {
            1
        } else {
            0
        }
    }

    let mut best: Vec<RawObservation> = Vec::new();
    for o in obs {
        match best.iter().position(|b| b.indexer_address == o.indexer_address) {
            Some(i) if rank(&o) > rank(&best[i]) => best[i] = o,
            Some(_) => {}
            None => best.push(o),
        }
    }
    best
}

/// A refusal to accept our payment, as opposed to anything the indexer did wrong.
pub fn is_payment_error(error_class: Option<&str>) -> bool {
    matches!(error_class, Some(e) if e.starts_with("payment_"))
}

fn parse_stake_weight(stake_grt: Option<&str>) -> f64 {
    stake_grt
        .and_then(|s| s.parse::<f64>().ok())
        .map(|grt| (1.0 + grt / 100_000.0).ln())
        .unwrap_or(1.0)
}

async fn store_results(
    pool: &PgPool,
    probe_id: Uuid,
    deployment_id: &str,
    block_number: u64,
    block_hash: &str,
    query_hash: &str,
    query_category: &str,
    query_text: &str,
    dispatched_at: chrono::DateTime<Utc>,
    observations: &[RawObservation],
    clusters: &crate::cluster::ClusterResult,
) -> Result<()> {
    sqlx::query(
        "INSERT INTO probe (id, deployment_id, block_hash, block_number, query_hash, query_category, query_text, dispatched_at)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
         ON CONFLICT (id) DO NOTHING",
    )
    .bind(probe_id)
    .bind(deployment_id)
    .bind(block_hash)
    .bind(block_number as i64)
    .bind(query_hash)
    .bind(query_category)
    .bind(query_text)
    .bind(dispatched_at)
    .execute(pool)
    .await?;

    for obs in observations {
        sqlx::query(
            "INSERT INTO observation (probe_id, indexer_address, response_hash, latency_ms, meta_block_number, meta_block_hash, http_status, error_class, stake_weight, dispatch_mode, request_cid, response_cid, attestation)
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, CAST($13 AS jsonb))
             ON CONFLICT (probe_id, indexer_address) DO NOTHING",
        )
        .bind(probe_id)
        .bind(&obs.indexer_address)
        .bind(&obs.response_hash)
        .bind(obs.latency_ms)
        .bind(obs.meta_block_number)
        .bind(&obs.meta_block_hash)
        .bind(obs.http_status)
        .bind(&obs.error_class)
        .bind(obs.stake_weight)
        .bind(&obs.dispatch_mode)
        .bind(&obs.request_cid)
        .bind(&obs.response_cid)
        .bind(&obs.attestation)
        .execute(pool)
        .await?;
    }

    // Record the cluster result for EVERY corroborated probe, not only the ones that disagreed.
    //
    // This was `if clusters.is_divergent`, and the consequence was that agreement — the common case,
    // and the one an operator actually wants evidenced — was never written down. `comparable_count`
    // in the rollup counts responses that have a cluster row, so with no row for agreement it read
    // 13 against 325 probes that genuinely had two or more indexers answering. `correctness_rate`
    // was therefore NULL for every indexer that always agreed, and non-null only for those already
    // caught in a divergence: a correctness metric that could say "this indexer was in a
    // disagreement" but never "this indexer has been checked and matched, 300 times".
    //
    // The row is now written whenever at least two indexers answered the identical probe, which is
    // exactly the condition under which the comparison means anything. `cluster_count = 1` is a
    // perfectly good finding: everyone agreed.
    if clusters.largest_by_count_size >= 2 && !clusters.largest_by_count_hash.is_empty() {
        sqlx::query(
            "INSERT INTO divergence (probe_id, cluster_count, diff_patches, largest_by_count_hash, largest_by_count_size, largest_by_stake_hash, largest_by_stake_weight, created_at)
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
             ON CONFLICT (probe_id) DO NOTHING",
        )
        .bind(probe_id)
        .bind(clusters.cluster_count)
        .bind(&clusters.diff_patches)
        .bind(&clusters.largest_by_count_hash)
        .bind(clusters.largest_by_count_size)
        .bind(&clusters.largest_by_stake_hash)
        .bind(clusters.largest_by_stake_weight)
        .bind(dispatched_at)
        .execute(pool)
        .await?;
    }

    Ok(())
}

fn load_test_sets(dir: &str) -> Result<Vec<TestSet>> {
    let path = std::path::Path::new(dir);
    if !path.exists() {
        warn!(dir = %dir, "Test sets directory not found, using empty set");
        return Ok(vec![]);
    }

    let mut test_sets = Vec::new();
    for entry in std::fs::read_dir(path)? {
        let entry = entry?;
        let p = entry.path();
        if p.extension().and_then(|e| e.to_str()) == Some("yaml") {
            let content = std::fs::read_to_string(&p)?;
            match serde_yaml::from_str::<TestSet>(&content) {
                Ok(mut ts) => {
                    // Curated test-sets are hand-written and four of them carried bytes32 ids while
                    // auto-discovery produces `Qm…`. Both went into the same `deployment_id` column,
                    // served under a field named `subgraph_deployment_ipfs_hash` — so the id was
                    // wrong for a consumer, AND two of those deployments were being probed twice
                    // under two names, splitting their rollups in half.
                    ts.deployment.id = normalise_deployment_id(&ts.deployment.id);
                    info!(file = ?p, deployment = %ts.deployment.description, "Loaded test set");
                    test_sets.push(ts);
                }
                Err(e) => warn!(file = ?p, error = %e, "Failed to parse test set"),
            }
        }
    }
    Ok(test_sets)
}

/// Build the paid-query client from config.
///
/// Kept separate so a misconfiguration is one clear error at startup rather than a failure per
/// probe. The signer key is read from config (env `FOGHORN__TAP__SIGNER_KEY` in production) and
/// never logged: it signs receipts, so leaking it lets someone spend the escrow.
fn build_paid_client(
    config: &FoghornConfig,
) -> anyhow::Result<(tap_query::PaidQueryClient, usize)> {
    let key = config
        .tap
        .signer_key
        .as_deref()
        .filter(|k| !k.is_empty())
        .ok_or_else(|| anyhow::anyhow!("tap.signer_key is not set"))?;
    let signer: tap_query::PrivateKeySigner =
        key.parse().map_err(|_| anyhow::anyhow!("tap.signer_key is not a valid private key"))?;

    let ctx = tap_query::PaymentContext {
        chain_id: tap_query::ARBITRUM_ONE_CHAIN_ID,
        verifier: config.tap.verifier.parse()?,
        data_service: config.tap.data_service.parse()?,
        payer: config.tap.payer.parse()?,
    };
    let client = tap_query::PaidQueryClient::new(
        signer,
        ctx,
        Duration::from_secs(30),
        config.tap.receipt_value,
    )?;
    // Cap targets per deployment so one very widely-allocated subgraph cannot consume a whole
    // round's budget. Reuses the gateway's probe_count, which is already the "how many opinions do
    // we want per query" knob.
    let max_targets = config.gateway.as_ref().map(|g| g.probe_count as usize).unwrap_or(8);
    Ok((client, max_targets))
}

#[cfg(test)]
mod dedup_tests {
    use super::*;

    fn obs(addr: &str, hash: Option<&str>, err: Option<&str>) -> RawObservation {
        RawObservation {
            indexer_address: addr.to_string(),
            response_hash: hash.map(str::to_string),
            raw_response: None,
            latency_ms: 10,
            meta_block_number: None,
            meta_block_hash: None,
            http_status: hash.map(|_| 200),
            error_class: err.map(str::to_string),
            stake_weight: 1.0,
            dispatch_mode: DISPATCH_PAID.to_string(),
            request_cid: None,
            response_cid: None,
            attestation: None,
        }
    }

    /// The regression that made paid dispatch safe to enable.
    ///
    /// Paid observations are collected first, so under the old first-wins rule a denylisted payment
    /// evicted a real gateway response from the same indexer — we would have thrown away the only
    /// measurement we had and recorded nothing.
    #[test]
    fn a_payment_refusal_never_evicts_a_real_response() {
        let deduped = dedup_by_address(vec![
            obs("0xa", None, Some("payment_denylisted")),
            obs("0xa", Some("hash"), None),
        ]);
        assert_eq!(deduped.len(), 1);
        assert_eq!(deduped[0].response_hash.as_deref(), Some("hash"));
    }

    /// A genuine fault still beats a payment refusal: it describes the indexer, which is the point.
    #[test]
    fn a_real_error_beats_a_payment_refusal() {
        let deduped = dedup_by_address(vec![
            obs("0xa", None, Some("payment_refused")),
            obs("0xa", None, Some("timeout")),
        ]);
        assert_eq!(deduped[0].error_class.as_deref(), Some("timeout"));
    }

    /// And a payment refusal is still kept when it is all we have — dropping it silently would hide
    /// that we tried and could not pay.
    #[test]
    fn a_lone_payment_refusal_is_kept() {
        let deduped = dedup_by_address(vec![obs("0xa", None, Some("payment_denylisted"))]);
        assert_eq!(deduped.len(), 1);
        assert_eq!(deduped[0].error_class.as_deref(), Some("payment_denylisted"));
    }

    #[test]
    fn distinct_indexers_are_all_kept() {
        let deduped = dedup_by_address(vec![
            obs("0xa", Some("h1"), None),
            obs("0xb", Some("h2"), None),
            obs("0xa", Some("h3"), None),
        ]);
        assert_eq!(deduped.len(), 2);
        // First real response wins for a given address; later duplicates do not overwrite it.
        assert_eq!(deduped[0].response_hash.as_deref(), Some("h1"));
    }

    #[test]
    fn payment_errors_are_recognised_by_prefix() {
        assert!(is_payment_error(Some("payment_denylisted")));
        assert!(is_payment_error(Some("payment_refused")));
        assert!(!is_payment_error(Some("timeout")));
        assert!(!is_payment_error(None));
    }
}
