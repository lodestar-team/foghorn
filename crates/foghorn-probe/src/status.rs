//! Direct indexer `/status` health probing. The indexer status endpoint is
//! unauthenticated (no TAP, no payment) so this works against any indexer today.
//! For each indexer with a known URL we POST the standard `indexingStatuses`
//! query and record sync state / chainhead lag / fatal errors per deployment.
//!
//! This replaces the old `freshness.rs` stub (which never wrote samples).

use anyhow::Result;
use foghorn_core::config::StatusProbeConfig;
use serde_json::Value;
use sqlx::{PgPool, Row};
use std::time::Duration;
use tokio::task::JoinSet;
use tracing::{info, warn};

const STATUS_QUERY: &str = r#"{ indexingStatuses { subgraph synced health fatalError { message } chains { chainHeadBlock { number } latestBlock { number } } } }"#;

/// Max indexers to probe per cycle — a guard against the roster growing huge.
const MAX_TARGETS: i64 = 600;

pub async fn run_status_loop(cfg: StatusProbeConfig, pool: PgPool) {
    if !cfg.enabled {
        info!("Status probing disabled");
        return;
    }
    info!(interval = cfg.interval_secs, "Status probe loop starting");
    loop {
        match run_status_once(&pool, &cfg).await {
            Ok(n) => info!(samples = n, "Status probe cycle complete"),
            Err(e) => warn!(error = %e, "Status probe cycle failed"),
        }
        tokio::time::sleep(Duration::from_secs(cfg.interval_secs)).await;
    }
}

/// One status pass over all indexers with a known URL. Returns samples written.
pub async fn run_status_once(pool: &PgPool, cfg: &StatusProbeConfig) -> Result<usize> {
    let targets = load_targets(pool).await?;
    if targets.is_empty() {
        return Ok(0);
    }

    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(cfg.timeout_secs))
        .build()?;

    let mut written = 0usize;
    let mut i = 0;
    let concurrency = cfg.concurrency.max(1);
    while i < targets.len() {
        let chunk = &targets[i..(i + concurrency).min(targets.len())];
        let mut set = JoinSet::new();
        for (addr, url) in chunk {
            let client = client.clone();
            let addr = addr.clone();
            let url = url.clone();
            set.spawn(async move { probe_one(&client, &addr, &url).await });
        }
        while let Some(joined) = set.join_next().await {
            if let Ok(samples) = joined {
                for s in samples {
                    if store_sample(pool, &s).await.is_ok() {
                        written += 1;
                    }
                }
            }
        }
        i += concurrency;
    }
    Ok(written)
}

struct Sample {
    indexer_address: String,
    deployment_id: String,
    synced: Option<bool>,
    health: Option<String>,
    chain_head_block: Option<i64>,
    latest_block: Option<i64>,
    lag_blocks: Option<i64>,
    fatal_error: Option<String>,
    probe_error: Option<String>,
}

async fn load_targets(pool: &PgPool) -> Result<Vec<(String, String)>> {
    let rows = sqlx::query(
        r#"SELECT indexer_address, url FROM (
               SELECT indexer_address, url FROM indexer_profile
                 WHERE url IS NOT NULL AND url <> ''
               UNION
               SELECT indexer_address, indexer_url AS url FROM allocation_map
                 WHERE indexer_address IS NOT NULL AND indexer_url IS NOT NULL AND indexer_url <> ''
           ) t
           GROUP BY indexer_address, url
           LIMIT $1"#,
    )
    .bind(MAX_TARGETS)
    .fetch_all(pool)
    .await?;
    Ok(rows
        .iter()
        .map(|r| (r.get::<String, _>("indexer_address").to_lowercase(), r.get::<String, _>("url")))
        .collect())
}

async fn probe_one(client: &reqwest::Client, address: &str, base_url: &str) -> Vec<Sample> {
    let url = format!("{}/status", base_url.trim_end_matches('/'));
    let body = serde_json::json!({ "query": STATUS_QUERY });

    let err_sample = |msg: String| -> Vec<Sample> {
        vec![Sample {
            indexer_address: address.to_string(),
            deployment_id: String::new(),
            synced: None,
            health: None,
            chain_head_block: None,
            latest_block: None,
            lag_blocks: None,
            fatal_error: None,
            probe_error: Some(msg),
        }]
    };

    let resp = match client.post(&url).json(&body).send().await {
        Ok(r) => r,
        Err(e) => return err_sample(format!("network: {e}")),
    };
    if !resp.status().is_success() {
        return err_sample(format!("http {}", resp.status()));
    }
    let json: Value = match resp.json().await {
        Ok(v) => v,
        Err(e) => return err_sample(format!("decode: {e}")),
    };

    let statuses = match json.pointer("/data/indexingStatuses").and_then(|v| v.as_array()) {
        Some(s) => s,
        None => return err_sample("no indexingStatuses in response".to_string()),
    };

    let mut out = Vec::new();
    for st in statuses {
        let deployment_id = st["subgraph"].as_str().unwrap_or_default().to_string();
        let synced = st["synced"].as_bool();
        let health = st["health"].as_str().map(str::to_string);
        let fatal_error = st
            .pointer("/fatalError/message")
            .and_then(|v| v.as_str())
            .map(str::to_string);
        let chain = st["chains"].as_array().and_then(|c| c.first());
        let chain_head_block = chain
            .and_then(|c| c.pointer("/chainHeadBlock/number"))
            .and_then(parse_block_num);
        let latest_block = chain
            .and_then(|c| c.pointer("/latestBlock/number"))
            .and_then(parse_block_num);
        let lag_blocks = match (chain_head_block, latest_block) {
            (Some(h), Some(l)) => Some((h - l).max(0)),
            _ => None,
        };
        out.push(Sample {
            indexer_address: address.to_string(),
            deployment_id,
            synced,
            health,
            chain_head_block,
            latest_block,
            lag_blocks,
            fatal_error,
            probe_error: None,
        });
    }
    if out.is_empty() {
        return err_sample("empty indexingStatuses".to_string());
    }
    out
}

/// Block numbers come back as JSON strings (or occasionally numbers).
fn parse_block_num(v: &Value) -> Option<i64> {
    v.as_str().and_then(|s| s.parse::<i64>().ok()).or_else(|| v.as_i64())
}

async fn store_sample(pool: &PgPool, s: &Sample) -> Result<()> {
    sqlx::query(
        r#"INSERT INTO status_sample
             (indexer_address, deployment_id, synced, health, chain_head_block,
              latest_block, lag_blocks, fatal_error, probe_error)
           VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)"#,
    )
    .bind(&s.indexer_address)
    .bind(&s.deployment_id)
    .bind(s.synced)
    .bind(&s.health)
    .bind(s.chain_head_block)
    .bind(s.latest_block)
    .bind(s.lag_blocks)
    .bind(&s.fatal_error)
    .bind(&s.probe_error)
    .execute(pool)
    .await?;
    Ok(())
}
