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

pub async fn run_allocation_sync_loop(api_key: Option<String>, interval_secs: u64, pool: PgPool) {
    let Some(api_key) = api_key else {
        warn!("Allocation sync needs [gateway].api_key — paid probing will have nothing to bill");
        return;
    };
    info!(interval = interval_secs, "Active allocation sync starting");
    loop {
        match sync_once(&api_key, &pool).await {
            Ok(n) => info!(allocations = n, "Active allocations refreshed"),
            Err(e) => warn!(error = %e, "Allocation sync failed"),
        }
        tokio::time::sleep(Duration::from_secs(interval_secs)).await;
    }
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
