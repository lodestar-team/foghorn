//! Keeping the active-allocation table fresh.
//!
//! A paid probe needs the allocation being served, because the TAP receipt bills `collection_id`
//! (the allocation address padded to 32 bytes) and the indexer refuses anything not currently
//! active. Nothing in Foghorn knew this: `allocation_map` holds ecrecovered attestation *signers*,
//! which are allocation-specific keys rather than allocation ids.
//!
//! Refreshed from the network subgraph on an interval. Closed allocations are deleted rather than
//! marked, because billing a closed allocation is rejected outright — keeping the row would produce
//! failures that look like indexer faults and are not.

use anyhow::{Context, Result};
use serde_json::{json, Value};
use sqlx::PgPool;
use std::time::Duration;
use tracing::{info, warn};

/// The network subgraph, via the gateway. Same id `autodiscover` and `resolver` already use.
const NETWORK_SUBGRAPH_ID: &str = "DZz4kDTdmzWLWsV373w2bSmoar3umKKH9y82SUKr5qmp";
const GATEWAY_BASE: &str = "https://gateway-arbitrum.network.thegraph.com/api";

pub async fn run_allocation_sync_loop(
    api_key: Option<String>,
    interval_secs: u64,
    nest: Option<crate::nest::NestClient>,
    chain_tip: std::sync::Arc<dyn Fn() -> Option<u64> + Send + Sync>,
    pool: PgPool,
) {
    let Some(api_key) = api_key else {
        warn!("Allocation sync needs [gateway].api_key — paid probing will have nothing to bill");
        return;
    };
    info!(
        interval = interval_secs,
        nest = nest.is_some(),
        "Active allocation sync starting"
    );
    loop {
        // The nest first when configured, the gateway as fallback.
        //
        // Not because the gateway is a better source - it is the dependency we are trying to shed -
        // but because the nest refuses to answer while it is still backfilling, and losing the
        // allocation table would stop paid probing entirely. The fallback is a bridge, and the log
        // says plainly which source each refresh came from so "we are off the gateway now" is a
        // checkable claim rather than an assumption.
        let mut done = false;
        if let Some(client) = nest.as_ref() {
            match chain_tip() {
                Some(tip) => match sync_from_nest(client, tip, &pool).await {
                    Ok(n) => {
                        info!(allocations = n, source = "nest", "Active allocations refreshed");
                        done = true;
                    }
                    Err(e) => warn!(error = %e, "Nest allocation sync unavailable, falling back to the gateway"),
                },
                None => warn!("Chain tip unknown, cannot judge nest freshness — using the gateway"),
            }
        }
        if !done {
            match sync_once(&api_key, &pool).await {
                Ok(n) => info!(allocations = n, source = "gateway", "Active allocations refreshed"),
                Err(e) => warn!(error = %e, "Allocation sync failed"),
            }
        }
        tokio::time::sleep(Duration::from_secs(interval_secs)).await;
    }
}

/// Replace the allocation table from our own nest.
///
/// Same delete-then-insert-in-one-transaction shape as `sync_once`, and for the same reason: the
/// table must never contain a closed allocation even briefly, because the scheduler reads it
/// continuously and would bill something guaranteed to be refused.
pub async fn sync_from_nest(
    client: &crate::nest::NestClient,
    chain_tip: u64,
    pool: &PgPool,
) -> Result<usize> {
    let allocations = client.allocations(chain_tip).await?;
    // Endpoints are a separate query because they fold a different event; an indexer with an
    // allocation but no registration is normal and must not drop the allocation.
    let endpoints: std::collections::HashMap<String, String> =
        client.endpoints().await.unwrap_or_default().into_iter().collect();

    let mut tx = pool.begin().await?;
    sqlx::query("DELETE FROM active_allocation").execute(&mut *tx).await?;
    for a in &allocations {
        let indexer = a.indexer.to_lowercase();
        sqlx::query(
            r#"INSERT INTO active_allocation
                   (allocation_id, indexer_address, deployment_id, indexer_url, allocated_tokens, refreshed_at)
               VALUES ($1, $2, $3, $4, $5, NOW())
               ON CONFLICT (allocation_id) DO UPDATE SET
                   indexer_address = EXCLUDED.indexer_address,
                   deployment_id   = EXCLUDED.deployment_id,
                   indexer_url     = EXCLUDED.indexer_url,
                   allocated_tokens = EXCLUDED.allocated_tokens,
                   refreshed_at    = NOW()"#,
        )
        .bind(&a.allocation_id.to_lowercase())
        .bind(&indexer)
        .bind(&a.deployment_id)
        .bind(endpoints.get(&indexer))
        .bind(a.tokens)
        .execute(&mut *tx)
        .await?;
    }
    tx.commit().await?;
    Ok(allocations.len())
}

/// Pull every active allocation and replace the table contents.
///
/// Written as delete-then-insert inside one transaction rather than an upsert plus a tidy-up pass:
/// the table must not contain a closed allocation even briefly, since the scheduler reads it
/// continuously and would bill something that is guaranteed to be refused.
pub async fn sync_once(api_key: &str, pool: &PgPool) -> Result<usize> {
    let url = format!("{GATEWAY_BASE}/{api_key}/subgraphs/id/{NETWORK_SUBGRAPH_ID}");
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(30))
        .build()?;

    let mut rows: Vec<(String, String, String, Option<String>, Option<String>)> = Vec::new();
    let mut last_id = String::new();

    // Keyset pagination: `skip` degrades badly on graph-node and silently caps.
    for _ in 0..40 {
        let q = json!({
            "query": format!(
                r#"{{ allocations(first: 1000, orderBy: id, orderDirection: asc,
                       where: {{ status: Active, id_gt: "{last_id}" }})
                     {{ id allocatedTokens indexer {{ id url }} subgraphDeployment {{ ipfsHash }} }} }}"#
            )
        });
        let v: Value = client
            .post(&url)
            .json(&q)
            .send()
            .await
            .context("network subgraph request failed")?
            .json()
            .await
            .context("network subgraph returned unparseable JSON")?;

        if let Some(errors) = v.get("errors") {
            anyhow::bail!("network subgraph rejected the allocations query: {errors}");
        }
        let Some(items) = v.pointer("/data/allocations").and_then(|x| x.as_array()) else {
            break;
        };
        if items.is_empty() {
            break;
        }
        let full = items.len() == 1000;
        last_id = items
            .last()
            .and_then(|r| r["id"].as_str())
            .unwrap_or_default()
            .to_string();

        for a in items {
            let (Some(id), Some(indexer), Some(dep)) = (
                a["id"].as_str(),
                a.pointer("/indexer/id").and_then(|x| x.as_str()),
                a.pointer("/subgraphDeployment/ipfsHash").and_then(|x| x.as_str()),
            ) else {
                continue;
            };
            rows.push((
                id.to_lowercase(),
                indexer.to_lowercase(),
                dep.to_string(),
                a.pointer("/indexer/url").and_then(|x| x.as_str()).map(str::to_string),
                a["allocatedTokens"].as_str().map(str::to_string),
            ));
        }

        if !full || last_id.is_empty() {
            break;
        }
    }

    // An empty result is far more likely to be a broken query than a network with no allocations.
    // Wiping the table on that would silently stop all paid probing.
    if rows.is_empty() {
        anyhow::bail!("network subgraph returned no active allocations — refusing to clear the table");
    }

    let mut tx = pool.begin().await?;
    sqlx::query("DELETE FROM active_allocation").execute(&mut *tx).await?;
    for (id, indexer, dep, url, tokens) in &rows {
        sqlx::query(
            r#"INSERT INTO active_allocation
                   (allocation_id, indexer_address, deployment_id, indexer_url, allocated_tokens, refreshed_at)
               VALUES ($1, $2, $3, $4, CAST($5 AS numeric), NOW())
               ON CONFLICT (allocation_id) DO UPDATE SET
                   indexer_address = EXCLUDED.indexer_address,
                   deployment_id   = EXCLUDED.deployment_id,
                   indexer_url     = EXCLUDED.indexer_url,
                   allocated_tokens = EXCLUDED.allocated_tokens,
                   refreshed_at    = NOW()"#,
        )
        .bind(id)
        .bind(indexer)
        .bind(dep)
        .bind(url)
        .bind(tokens)
        .execute(&mut *tx)
        .await?;
    }
    tx.commit().await?;

    Ok(rows.len())
}

/// Allocations we can actually pay for: active, with a URL, and belonging to an indexer we hold
/// escrow with.
///
/// Filtering on escrow here rather than discovering it per query matters — an unfunded indexer
/// returns 402 for every probe, which costs traffic and records a failure that says nothing about
/// the indexer's health.
pub async fn payable_targets(
    pool: &PgPool,
    limit: i64,
    excluded: &[String],
) -> Result<Vec<PayableTarget>> {
    // Excluded operators are filtered here as well as in the escrow sync. Belt and braces on
    // purpose: a stale `tap_escrow` row from before an exclusion was added would otherwise put a
    // retiring indexer back into the probe set, and its closing allocations would produce failures
    // that describe our target list rather than its health.
    let lowered: Vec<String> = excluded.iter().map(|e| e.to_lowercase()).collect();
    let rows = sqlx::query_as::<_, PayableTarget>(
        r#"SELECT a.allocation_id, a.indexer_address, a.deployment_id, a.indexer_url
           FROM active_allocation a
           JOIN tap_escrow e ON e.indexer_address = a.indexer_address
           WHERE a.indexer_url IS NOT NULL
             AND a.indexer_url <> ''
             AND e.balance_wei > 0
             AND NOT (a.indexer_address = ANY($2))
           ORDER BY a.allocated_tokens DESC NULLS LAST
           LIMIT $1"#,
    )
    .bind(limit)
    .bind(&lowered)
    .fetch_all(pool)
    .await?;
    Ok(rows)
}

#[derive(Debug, Clone, sqlx::FromRow)]
pub struct PayableTarget {
    pub allocation_id: String,
    pub indexer_address: String,
    pub deployment_id: String,
    pub indexer_url: Option<String>,
}

/// Payable targets for one deployment.
///
/// The per-deployment form exists because probes are organised by deployment: asking for a global
/// list and filtering in the scheduler would pull thousands of rows to use a handful, on every
/// query of every round.
pub async fn payable_targets_for_deployment(
    pool: &PgPool,
    deployment_id: &str,
    excluded: &[String],
) -> Result<Vec<PayableTarget>> {
    let lowered: Vec<String> = excluded.iter().map(|e| e.to_lowercase()).collect();
    let rows = sqlx::query_as::<_, PayableTarget>(
        r#"SELECT a.allocation_id, a.indexer_address, a.deployment_id, a.indexer_url
           FROM active_allocation a
           JOIN tap_escrow e ON e.indexer_address = a.indexer_address
           WHERE a.deployment_id = $1
             AND a.indexer_url IS NOT NULL
             AND a.indexer_url <> ''
             AND e.balance_wei > 0
             AND NOT (a.indexer_address = ANY($2))
           ORDER BY a.allocated_tokens DESC NULLS LAST"#,
    )
    .bind(deployment_id)
    .bind(&lowered)
    .fetch_all(pool)
    .await?;
    Ok(rows)
}

/// Keep an independent view of Arbitrum's chain tip.
///
/// The nest reports how far it has sealed; it cannot tell us how far behind that is. Asking the
/// nest itself would be circular — a stalled nest would report a stalled tip and look perfectly
/// caught up, which is the failure mode this oracle exists to catch, reproduced inside the check
/// for it. So the tip is read from the chain, separately.
pub async fn run_chain_tip_loop(
    rpc_url: String,
    tip: std::sync::Arc<std::sync::atomic::AtomicU64>,
) {
    let client = match reqwest::Client::builder().timeout(Duration::from_secs(20)).build() {
        Ok(c) => c,
        Err(e) => {
            warn!(error = %e, "Chain tip loop could not build an HTTP client");
            return;
        }
    };
    loop {
        match fetch_block_number(&client, &rpc_url).await {
            Ok(n) => tip.store(n, std::sync::atomic::Ordering::Relaxed),
            Err(e) => warn!(error = %e, "Chain tip read failed"),
        }
        tokio::time::sleep(Duration::from_secs(60)).await;
    }
}

async fn fetch_block_number(client: &reqwest::Client, rpc_url: &str) -> Result<u64> {
    let v: Value = client
        .post(rpc_url)
        .json(&json!({"jsonrpc":"2.0","id":1,"method":"eth_blockNumber","params":[]}))
        .send()
        .await
        .context("eth_blockNumber request failed")?
        .json()
        .await
        .context("eth_blockNumber returned unparseable JSON")?;
    let hex = v
        .get("result")
        .and_then(|r| r.as_str())
        .context("eth_blockNumber returned no result")?;
    u64::from_str_radix(hex.trim_start_matches("0x"), 16).context("block number was not hex")
}
