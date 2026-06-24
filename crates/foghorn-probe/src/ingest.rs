//! Ingestion loop: pull the Lodestar enriched roster + per-indexer QoS into
//! `indexer_profile`, keyed by the real indexer address. Roster covers everyone
//! (stake / coverage / REO / fees); QoS is fetched for indexers Foghorn has
//! actually observed serving its probes (resolved via `allocation_map`).

use crate::lodestar::LodestarClient;
use anyhow::Result;
use chrono::Utc;
use foghorn_core::config::LodestarConfig;
use serde_json::{json, Value};
use sqlx::PgPool;
use std::time::Duration;
use tokio::task::JoinSet;
use tracing::{info, warn};

const QOS_WINDOW_DAYS: usize = 30;
const QOS_CONCURRENCY: usize = 6;

// QoS oracle (Edge & Node) — per-allocation (indexer × deployment) daily metrics.
const QOS_ORACLE_ID: &str = "Dtr9rETvwokot4BSXaD5tECanXfqfJKcvHuaaEgPDD2D";
const QOS_ORACLE_BASE: &str = "https://gateway-arbitrum.network.thegraph.com/api";
const ALLOC_MIN_QUERIES: i64 = 50; // ignore negligible-traffic allocations

pub async fn run_ingest_loop(cfg: LodestarConfig, api_key: Option<String>, pool: PgPool) {
    info!(base_url = %cfg.base_url, interval = cfg.ingest_interval_secs, "Lodestar ingest loop starting");
    let client = match LodestarClient::new(&cfg.base_url, cfg.api_key.clone(), 30) {
        Ok(c) => c,
        Err(e) => {
            warn!(error = %e, "Failed to build Lodestar client — ingest disabled");
            return;
        }
    };

    loop {
        match run_ingest_once(&client, &pool).await {
            Ok((roster, qos)) => info!(roster, qos, "Ingest cycle complete"),
            Err(e) => warn!(error = %e, "Ingest cycle failed"),
        }
        // Per-allocation QoS from the oracle (success rate + lag per deployment).
        if let Some(key) = &api_key {
            match ingest_allocation_qos(key, &pool).await {
                Ok(n) => info!(allocation_qos = n, "Allocation QoS ingest complete"),
                Err(e) => warn!(error = %e, "Allocation QoS ingest failed"),
            }
        }
        tokio::time::sleep(Duration::from_secs(cfg.ingest_interval_secs)).await;
    }
}

/// Ingest per-(indexer, deployment) QoS from the oracle's AllocationDailyDataPoint
/// for the latest day — the granularity that reveals "synced but serving errors
/// on subgraph X". Returns rows stored.
async fn ingest_allocation_qos(api_key: &str, pool: &PgPool) -> Result<usize> {
    let run_start = Utc::now();
    let url = format!("{}/{}/subgraphs/id/{}", QOS_ORACLE_BASE, api_key, QOS_ORACLE_ID);
    let client = reqwest::Client::builder().timeout(Duration::from_secs(25)).build()?;

    // Latest day available (the oracle's dayNumber epoch differs from ours).
    let max_day: i64 = {
        let q = json!({"query": "{ allocationDailyDataPoints(first:1, orderBy: dayNumber, orderDirection: desc){ dayNumber } }"});
        let v: Value = client.post(&url).json(&q).send().await?.json().await?;
        v.pointer("/data/allocationDailyDataPoints/0/dayNumber")
            .and_then(|d| d.as_str())
            .and_then(|s| s.parse().ok())
            .or_else(|| v.pointer("/data/allocationDailyDataPoints/0/dayNumber").and_then(|d| d.as_i64()))
            .unwrap_or(0)
    };
    if max_day == 0 {
        return Ok(0);
    }

    let mut stored = 0usize;
    let mut last_id = String::new();
    for _ in 0..15 {
        let q = json!({
            "query": format!(
                r#"{{ allocationDailyDataPoints(first: 1000, orderBy: id, orderDirection: asc, where: {{ dayNumber_gte: {max_day}, query_count_gte: "{ALLOC_MIN_QUERIES}", id_gt: "{last_id}" }}) {{ id indexer_wallet query_count proportion_indexer_200_responses avg_indexer_blocks_behind subgraphDeployment {{ id }} }} }}"#
            )
        });
        let v: Value = client.post(&url).json(&q).send().await?.json().await?;
        let rows = match v.pointer("/data/allocationDailyDataPoints").and_then(|x| x.as_array()) {
            Some(r) if !r.is_empty() => r.clone(),
            _ => break,
        };
        for r in &rows {
            let indexer = r["indexer_wallet"].as_str().unwrap_or_default().to_lowercase();
            let dep = r.pointer("/subgraphDeployment/id").and_then(|x| x.as_str()).unwrap_or_default().to_string();
            if indexer.is_empty() || dep.is_empty() {
                continue;
            }
            let qc: i64 = r["query_count"].as_str().and_then(|s| s.parse().ok()).unwrap_or(0);
            let sr: f64 = r["proportion_indexer_200_responses"].as_str().and_then(|s| s.parse().ok()).unwrap_or(1.0);
            let bb: f64 = r["avg_indexer_blocks_behind"].as_str().and_then(|s| s.parse().ok()).unwrap_or(0.0);
            sqlx::query(
                r#"INSERT INTO allocation_qos (indexer_address, deployment_id, day_number, success_rate, blocks_behind, query_count, updated_at)
                   VALUES ($1,$2,$3,$4,$5,$6, NOW())
                   ON CONFLICT (indexer_address, deployment_id) DO UPDATE SET
                     day_number = EXCLUDED.day_number, success_rate = EXCLUDED.success_rate,
                     blocks_behind = EXCLUDED.blocks_behind, query_count = EXCLUDED.query_count, updated_at = NOW()"#,
            )
            .bind(&indexer).bind(&dep).bind(max_day as i32).bind(sr).bind(bb).bind(qc)
            .execute(pool).await?;
            stored += 1;
        }
        if rows.len() < 1000 {
            break;
        }
        last_id = rows.last().and_then(|r| r["id"].as_str()).unwrap_or_default().to_string();
        if last_id.is_empty() {
            break;
        }
    }

    // Drop rows not refreshed this run (allocation closed or traffic dried up).
    sqlx::query("DELETE FROM allocation_qos WHERE updated_at < $1").bind(run_start).execute(pool).await?;
    Ok(stored)
}

/// One ingest pass. Returns (roster_count, qos_count).
pub async fn run_ingest_once(client: &LodestarClient, pool: &PgPool) -> Result<(usize, usize)> {
    // 1. Roster — upsert everyone (QoS columns left untouched).
    let roster = client.fetch_enriched().await?;
    for ix in &roster {
        let addr = ix.id.to_lowercase();
        if addr.is_empty() {
            continue;
        }
        sqlx::query(
            r#"INSERT INTO indexer_profile
                 (indexer_address, ens_name, url, created_at, self_stake_grt, delegated_grt,
                  allocation_count, query_fees_collected_grt, reo_status, reo_source,
                  lodestar_score, lodestar_grade, ingested_at)
               VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12, NOW())
               ON CONFLICT (indexer_address) DO UPDATE SET
                 ens_name = EXCLUDED.ens_name,
                 url = EXCLUDED.url,
                 created_at = EXCLUDED.created_at,
                 self_stake_grt = EXCLUDED.self_stake_grt,
                 delegated_grt = EXCLUDED.delegated_grt,
                 allocation_count = EXCLUDED.allocation_count,
                 query_fees_collected_grt = EXCLUDED.query_fees_collected_grt,
                 reo_status = EXCLUDED.reo_status,
                 reo_source = EXCLUDED.reo_source,
                 lodestar_score = EXCLUDED.lodestar_score,
                 lodestar_grade = EXCLUDED.lodestar_grade,
                 ingested_at = NOW()"#,
        )
        .bind(&addr)
        .bind(&ix.ens_name)
        .bind(&ix.url)
        .bind(ix.created_at)
        .bind(ix.self_stake_grt)
        .bind(ix.delegated_grt)
        .bind(ix.allocation_count)
        .bind(ix.query_fees_collected_grt)
        .bind(&ix.reo_status)
        .bind(&ix.reo_source)
        .bind(ix.score)
        .bind(&ix.score_grade)
        .execute(pool)
        .await?;
    }

    // 2. QoS — for the full roster, so every indexer's availability/value/freshness
    //    sub-scores populate immediately rather than waiting on Foghorn probe coverage.
    let observed: Vec<String> = roster
        .iter()
        .map(|ix| ix.id.to_lowercase())
        .filter(|a| !a.is_empty())
        .collect();

    // Bounded-concurrency QoS fetch + update.
    let mut qos_count = 0usize;
    let mut i = 0;
    while i < observed.len() {
        let chunk = &observed[i..(i + QOS_CONCURRENCY).min(observed.len())];
        let mut set = JoinSet::new();
        for addr in chunk {
            let client = client.clone();
            let addr = addr.clone();
            set.spawn(async move {
                let agg = client.fetch_qos(&addr, QOS_WINDOW_DAYS).await.ok();
                (addr, agg)
            });
        }
        while let Some(joined) = set.join_next().await {
            if let Ok((addr, Some(agg))) = joined {
                let res = sqlx::query(
                    r#"UPDATE indexer_profile
                         SET qos_query_count = $2, qos_success_rate = $3,
                             qos_latency_ms = $4, qos_blocks_behind = $5, ingested_at = NOW()
                       WHERE indexer_address = $1"#,
                )
                .bind(&addr)
                .bind(agg.query_count)
                .bind(agg.success_rate)
                .bind(agg.latency_ms)
                .bind(agg.blocks_behind)
                .execute(pool)
                .await;
                if res.is_ok() {
                    qos_count += 1;
                }
            }
        }
        i += QOS_CONCURRENCY;
    }

    Ok((roster.len(), qos_count))
}
