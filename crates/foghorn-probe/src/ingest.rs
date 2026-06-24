//! Ingestion loop: pull the Lodestar enriched roster + per-indexer QoS into
//! `indexer_profile`, keyed by the real indexer address. Roster covers everyone
//! (stake / coverage / REO / fees); QoS is fetched for indexers Foghorn has
//! actually observed serving its probes (resolved via `allocation_map`).

use crate::lodestar::LodestarClient;
use anyhow::Result;
use foghorn_core::config::LodestarConfig;
use sqlx::PgPool;
use std::time::Duration;
use tokio::task::JoinSet;
use tracing::{info, warn};

const QOS_WINDOW_DAYS: usize = 30;
const QOS_CONCURRENCY: usize = 6;

pub async fn run_ingest_loop(cfg: LodestarConfig, pool: PgPool) {
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
        tokio::time::sleep(Duration::from_secs(cfg.ingest_interval_secs)).await;
    }
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
