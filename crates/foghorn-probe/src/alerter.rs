//! Discord alerting. Pushes each new critical needs-attention item to a Discord
//! webhook (#foghorn-alerts) exactly once, so serving failures / outages /
//! genuine lag are caught the moment Foghorn detects them — no one has to be
//! watching the dashboard. Disabled unless `alert_webhook` is configured.

use anyhow::Result;
use serde_json::{json, Value};
use sqlx::{PgPool, Row};
use std::collections::HashMap;
use std::time::Duration;
use tracing::{info, warn};

// Check hourly — matches the QoS-oracle ingest cadence (no new signal arrives
// between ingests, so polling faster catches nothing sooner). An hour's worth of
// new issues batches into a single message.
const POLL_SECS: u64 = 3600;
const MSG_LIMIT: usize = 1800; // Discord hard-caps at 2000 chars; chunk under it
const DASHBOARD: &str = "https://lodestar-dashboard.com";

/// Kinds the alerter pushes (serving failures + genuine per-deployment lag).
const ALERT_FILTER: &str = "(severity = 'critical' OR kind IN \
    ('behind-deployment','behind-deployments','serving-errors-deployment','behind-chainhead'))";

pub async fn run_alert_loop(webhook: String, pool: PgPool) {
    info!("Discord alert loop starting");
    let client = match reqwest::Client::builder().timeout(Duration::from_secs(15)).build() {
        Ok(c) => c,
        Err(e) => {
            warn!(error = %e, "Failed to build alert client — alerting disabled");
            return;
        }
    };
    loop {
        if let Err(e) = run_cycle(&client, &webhook, &pool).await {
            warn!(error = %e, "Alert cycle failed");
        }
        tokio::time::sleep(Duration::from_secs(POLL_SECS)).await;
    }
}

/// One poll cycle: push new issues if any, otherwise a daily liveness heartbeat.
async fn run_cycle(client: &reqwest::Client, webhook: &str, pool: &PgPool) -> Result<()> {
    let n = alert_once(client, webhook, pool).await?;
    if n > 0 {
        info!(alerted = n, "Pushed alerts to Discord");
    } else {
        maybe_heartbeat(client, webhook, pool).await?;
    }
    Ok(())
}

/// Post a "still on watch" heartbeat when there's nothing new AND it's been >24h
/// since we last posted anything (alert or heartbeat). Proves the alerter is
/// alive on quiet days without spamming on restarts or routine quiet cycles.
async fn maybe_heartbeat(client: &reqwest::Client, webhook: &str, pool: &PgPool) -> Result<()> {
    let due: bool = sqlx::query_scalar(
        "SELECT last_post IS NULL OR last_post < NOW() - INTERVAL '24 hours' FROM alerter_state",
    )
    .fetch_one(pool)
    .await
    .unwrap_or(true);
    if !due {
        return Ok(());
    }

    let flagged: i64 = sqlx::query_scalar(&format!(
        "SELECT COUNT(DISTINCT indexer_address) FROM attention_item WHERE {ALERT_FILTER}"
    ))
    .fetch_one(pool)
    .await?;

    let content = if flagged == 0 {
        format!(
            "📯 **Foghorn — all clear.** No flagged indexers and no new issues in the last 24h. Still on watch.\nFor more details — {}/foghorn",
            DASHBOARD
        )
    } else {
        format!(
            "📯 **Foghorn — still on watch.** No new issues in the last 24h; {flagged} indexer{} currently flagged.\nFor more details — {}/foghorn",
            if flagged == 1 { "" } else { "s" },
            DASHBOARD
        )
    };

    let body = json!({ "username": "Foghorn", "content": content, "allowed_mentions": { "parse": [] } });
    let resp = client.post(webhook).json(&body).send().await?;
    if !resp.status().is_success() {
        warn!(status = %resp.status(), "Discord webhook rejected the heartbeat");
        return Ok(());
    }
    touch_last_post(pool).await?;
    info!("Posted liveness heartbeat to Discord");
    Ok(())
}

/// Record that we just posted to Discord, resetting the heartbeat timer.
async fn touch_last_post(pool: &PgPool) -> Result<()> {
    sqlx::query("UPDATE alerter_state SET last_post = NOW()")
        .execute(pool)
        .await?;
    Ok(())
}

#[derive(Default)]
struct IndexerAlert {
    label: String,
    critical: bool,
    serving_no_data: bool,
    serving_err_deps: i64,
    behind_deps: i64,
    behind_head: bool,
}

async fn alert_once(client: &reqwest::Client, webhook: &str, pool: &PgPool) -> Result<usize> {
    let rows = sqlx::query(&format!(
        r#"SELECT a.indexer_address, a.kind, a.severity, a.detail,
                  COALESCE(p.ens_name, a.indexer_address) AS label
           FROM attention_item a
           LEFT JOIN indexer_profile p ON p.indexer_address = a.indexer_address
           WHERE a.alerted_at IS NULL AND {ALERT_FILTER}"#
    ))
    .fetch_all(pool)
    .await?;

    if rows.is_empty() {
        return Ok(0);
    }

    // Group every issue under its indexer — one line per indexer, nothing hidden.
    let mut by: HashMap<String, IndexerAlert> = HashMap::new();
    for r in &rows {
        let addr: String = r.get("indexer_address");
        let kind: String = r.get("kind");
        let sev: String = r.get("severity");
        let detail: Value = r.get("detail");
        let dep_count = detail.get("deployment_count").and_then(|v| v.as_i64()).unwrap_or(1);
        let e = by.entry(addr).or_default();
        if e.label.is_empty() {
            e.label = r.get("label");
        }
        if sev == "critical" {
            e.critical = true;
        }
        match kind.as_str() {
            "serving-no-data" => e.serving_no_data = true,
            "serving-errors" => e.serving_err_deps += dep_count,
            "serving-errors-deployment" => e.serving_err_deps += 1,
            "behind-deployments" => e.behind_deps += dep_count,
            "behind-deployment" => e.behind_deps += 1,
            "behind-chainhead" => e.behind_head = true,
            _ => {}
        }
    }

    let mut alerts: Vec<IndexerAlert> = by.into_values().collect();
    alerts.sort_by(|a, b| b.critical.cmp(&a.critical).then(b.serving_err_deps.cmp(&a.serving_err_deps)));
    let plural = |n: i64| if n == 1 { "" } else { "s" };

    let lines: Vec<String> = alerts.iter().map(|a| {
        let emoji = if a.critical { "🔴" } else { "🟠" };
        let mut parts: Vec<String> = Vec::new();
        if a.serving_no_data {
            parts.push("serving no data".to_string());
        }
        if a.serving_err_deps > 0 {
            parts.push(format!("serving errors on {} deployment{}", a.serving_err_deps, plural(a.serving_err_deps)));
        }
        if a.behind_deps > 0 {
            parts.push(format!("behind on {} deployment{}", a.behind_deps, plural(a.behind_deps)));
        }
        if a.behind_head {
            parts.push("behind chainhead".to_string());
        }
        format!("{emoji} **{}** — {}", a.label, parts.join("; "))
    }).collect();

    // Chunk into messages under Discord's limit — show ALL, never truncate.
    let header = format!("📯 **Foghorn — {} indexer{} need attention**", alerts.len(), plural(alerts.len() as i64));
    let footer = format!("For more details — {}/foghorn", DASHBOARD);
    let mut messages: Vec<String> = Vec::new();
    let mut cur = String::new();
    for line in &lines {
        if cur.len() + line.len() + 1 > MSG_LIMIT {
            messages.push(std::mem::take(&mut cur));
        }
        if !cur.is_empty() {
            cur.push('\n');
        }
        cur.push_str(line);
    }
    if !cur.is_empty() {
        messages.push(cur);
    }
    if let Some(first) = messages.first_mut() {
        *first = format!("{header}\n{first}");
    }
    if let Some(last) = messages.last_mut() {
        last.push_str(&format!("\n{footer}"));
    }

    // Post every chunk; if any fails, leave alerted_at NULL to retry next cycle.
    for msg in &messages {
        let body = json!({ "username": "Foghorn", "content": msg, "allowed_mentions": { "parse": [] } });
        let resp = client.post(webhook).json(&body).send().await?;
        if !resp.status().is_success() {
            warn!(status = %resp.status(), "Discord webhook rejected the alert");
            return Ok(0);
        }
        tokio::time::sleep(Duration::from_millis(400)).await; // be kind to the rate limit
    }

    sqlx::query(&format!(
        "UPDATE attention_item SET alerted_at = NOW() WHERE alerted_at IS NULL AND {ALERT_FILTER}"
    ))
    .execute(pool)
    .await?;
    touch_last_post(pool).await?; // a real alert resets the 24h heartbeat timer

    Ok(alerts.len())
}
