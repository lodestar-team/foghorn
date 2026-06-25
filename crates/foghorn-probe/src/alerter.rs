//! Discord alerting. Pushes each new critical needs-attention item to a Discord
//! webhook (#foghorn-alerts) exactly once, so serving failures / outages /
//! genuine lag are caught the moment Foghorn detects them — no one has to be
//! watching the dashboard. Disabled unless `alert_webhook` is configured.

use anyhow::Result;
use serde_json::json;
use sqlx::{PgPool, Row};
use std::time::Duration;
use tracing::{info, warn};

const POLL_SECS: u64 = 120;
const MAX_LINES: usize = 12; // cap a single Discord message; summarise the rest
const DASHBOARD: &str = "https://lodestar-dashboard.com";

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
        match alert_once(&client, &webhook, &pool).await {
            Ok(n) if n > 0 => info!(alerted = n, "Pushed alerts to Discord"),
            Ok(_) => {}
            Err(e) => warn!(error = %e, "Alert cycle failed"),
        }
        tokio::time::sleep(Duration::from_secs(POLL_SECS)).await;
    }
}

async fn alert_once(client: &reqwest::Client, webhook: &str, pool: &PgPool) -> Result<usize> {
    // New, un-alerted items worth pushing: serving failures + genuine lag.
    let rows = sqlx::query(
        r#"SELECT a.indexer_address, a.kind, a.severity, a.title, a.deployment_id,
                  COALESCE(p.ens_name, a.indexer_address) AS label
           FROM attention_item a
           LEFT JOIN indexer_profile p ON p.indexer_address = a.indexer_address
           WHERE a.alerted_at IS NULL
             AND (a.severity = 'critical' OR a.kind IN
                  ('behind-deployment','behind-deployments','serving-errors-deployment','behind-chainhead'))
           ORDER BY a.urgency DESC"#,
    )
    .fetch_all(pool)
    .await?;

    if rows.is_empty() {
        return Ok(0);
    }

    let total = rows.len();
    let mut lines: Vec<String> = Vec::new();
    for r in rows.iter().take(MAX_LINES) {
        let sev: String = r.get("severity");
        let emoji = if sev == "critical" { "🔴" } else { "🟠" };
        let label: String = r.get("label");
        let title: String = r.get("title");
        let dep: String = r.get("deployment_id");
        let dep_note = if dep.is_empty() { String::new() } else { format!(" · `{}`", &dep[..dep.len().min(14)]) };
        lines.push(format!("{emoji} **{label}** — {title}{dep_note}"));
    }
    let mut content = format!("📯 **Foghorn — {total} new issue{}**\n{}", if total == 1 { "" } else { "s" }, lines.join("\n"));
    if total > MAX_LINES {
        content.push_str(&format!("\n…and {} more", total - MAX_LINES));
    }
    content.push_str(&format!("\n{}/foghorn", DASHBOARD));

    let body = json!({
        "username": "Foghorn",
        "content": content,
        "allowed_mentions": { "parse": [] },
    });
    let resp = client.post(webhook).json(&body).send().await?;
    if !resp.status().is_success() {
        warn!(status = %resp.status(), "Discord webhook rejected the alert");
        return Ok(0); // leave alerted_at NULL so we retry next cycle
    }

    // Mark everything we considered this cycle as alerted (even beyond MAX_LINES,
    // so the summarised overflow doesn't re-alert next cycle).
    sqlx::query(
        r#"UPDATE attention_item SET alerted_at = NOW()
           WHERE alerted_at IS NULL
             AND (severity = 'critical' OR kind IN
                  ('behind-deployment','behind-deployments','serving-errors-deployment','behind-chainhead'))"#,
    )
    .execute(pool)
    .await?;

    Ok(total)
}
