//! Discord alerting. Posts the FULL current failure roster to a Discord webhook
//! (#foghorn-alerts) whenever it changes — an indexer appears, clears, or its
//! failure summary shifts — so the channel always shows the complete picture, not
//! a delta that would read as "everyone else recovered". Reposts once a day even
//! when unchanged, as a liveness heartbeat. Disabled unless `alert_webhook` is set.

use anyhow::Result;
use serde_json::{json, Value};
use sqlx::{PgPool, Row};
use std::collections::HashMap;
use std::time::Duration;
use tracing::{info, warn};

// Check hourly — matches the QoS-oracle ingest cadence (no new signal arrives
// between ingests, so polling faster catches nothing sooner). We post at most
// once an hour, and only when the failure roster has actually changed.
const POLL_SECS: u64 = 3600;
const MSG_LIMIT: usize = 1800; // Discord hard-caps at 2000 chars; chunk under it
const DASHBOARD: &str = "https://lodestar-dashboard.com";

/// Kinds the alerter reports (serving failures + genuine per-deployment lag).
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

/// One poll cycle. Builds the full current failure roster, and posts it (in full)
/// if it changed since the last post, or once a day as a liveness repost.
async fn run_cycle(client: &reqwest::Client, webhook: &str, pool: &PgPool) -> Result<()> {
    let roster = current_failure_roster(pool).await?;

    // Read last-posted fingerprint + whether 24h has elapsed (daily liveness repost).
    let (last_fingerprint, due): (Option<String>, bool) = sqlx::query_as(
        "SELECT last_fingerprint, (last_post IS NULL OR last_post < NOW() - INTERVAL '24 hours') FROM alerter_state",
    )
    .fetch_one(pool)
    .await
    .unwrap_or((None, true));

    // Change detection is on the roster + severity, NOT the exact counts — so an
    // indexer drifting 26→27 deployments doesn't re-ping the channel; only a new
    // failing indexer, a recovery, or a severity change does. The posted message
    // still carries live counts, and the daily digest refreshes them regardless.
    let changed = roster.fingerprint != last_fingerprint.unwrap_or_default();
    if !changed && !due {
        return Ok(()); // roster unchanged and not yet time for the daily digest
    }
    let lines = roster.lines;

    let content = if lines.is_empty() {
        // No failures — an all-clear liveness note (only on change or the daily tick).
        format!(
            "📯 **Foghorn — all clear.** No indexers are currently serving errors or behind. Still on watch.\nFor more details — {}/foghorn",
            DASHBOARD
        )
    } else {
        // Full roster. Mark a no-change daily repost as a status check so an
        // identical re-post doesn't look like a glitch.
        let n = lines.len();
        let header = if changed {
            format!("📯 **Foghorn — {} indexer{} need attention**", n, plural(n as i64))
        } else {
            format!("📯 **Foghorn — daily status · {} indexer{} need attention**", n, plural(n as i64))
        };
        build_message_body(&header, &lines)
    };

    if !post_chunks(client, webhook, &content).await? {
        return Ok(()); // webhook rejected — leave fingerprint untouched, retry next cycle
    }

    sqlx::query("UPDATE alerter_state SET last_post = NOW(), last_fingerprint = $1")
        .bind(&roster.fingerprint)
        .execute(pool)
        .await?;
    info!(indexers = lines.len(), changed, "Posted failure roster to Discord");
    Ok(())
}

fn plural(n: i64) -> &'static str {
    if n == 1 { "" } else { "s" }
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

struct Roster {
    /// One human-readable summary line per failing indexer (with live counts).
    lines: Vec<String>,
    /// Coarse change-detection signature: sorted `address:severity`, no counts.
    fingerprint: String,
}

/// The full set of currently-failing indexers: display lines (with counts) plus a
/// roster+severity fingerprint for change detection. Both deterministically ordered.
async fn current_failure_roster(pool: &PgPool) -> Result<Roster> {
    let rows = sqlx::query(&format!(
        r#"SELECT a.indexer_address, a.kind, a.severity, a.detail,
                  COALESCE(p.ens_name, a.indexer_address) AS label
           FROM attention_item a
           LEFT JOIN indexer_profile p ON p.indexer_address = a.indexer_address
           WHERE {ALERT_FILTER}"#
    ))
    .fetch_all(pool)
    .await?;

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

    // Fingerprint = sorted address:severity, independent of counts and display
    // order, so only roster/severity changes flip it (not a count drift or reorder).
    let mut fp: Vec<String> = by
        .iter()
        .map(|(addr, a)| format!("{}:{}", addr, if a.critical { "C" } else { "H" }))
        .collect();
    fp.sort();
    let fingerprint = fp.join(",");

    let mut alerts: Vec<IndexerAlert> = by.into_values().collect();
    // Critical first, then by serving-error breadth, then label — fully deterministic.
    alerts.sort_by(|a, b| {
        b.critical
            .cmp(&a.critical)
            .then(b.serving_err_deps.cmp(&a.serving_err_deps))
            .then(a.label.cmp(&b.label))
    });

    let lines: Vec<String> = alerts
        .iter()
        .map(|a| {
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
        })
        .collect();

    Ok(Roster { lines, fingerprint })
}

/// Header + all lines + footer, joined (caller chunks it for Discord's limit).
fn build_message_body(header: &str, lines: &[String]) -> String {
    let footer = format!("For more details — {}/foghorn", DASHBOARD);
    format!("{header}\n{}\n{footer}", lines.join("\n"))
}

/// Post `content`, splitting on newlines into messages under Discord's char cap.
/// Returns false (without erroring) if the webhook rejects a chunk.
async fn post_chunks(client: &reqwest::Client, webhook: &str, content: &str) -> Result<bool> {
    let mut messages: Vec<String> = Vec::new();
    let mut cur = String::new();
    for line in content.split('\n') {
        if !cur.is_empty() && cur.len() + line.len() + 1 > MSG_LIMIT {
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

    for msg in &messages {
        let body = json!({ "username": "Foghorn", "content": msg, "allowed_mentions": { "parse": [] } });
        let resp = client.post(webhook).json(&body).send().await?;
        if !resp.status().is_success() {
            warn!(status = %resp.status(), "Discord webhook rejected the message");
            return Ok(false);
        }
        tokio::time::sleep(Duration::from_millis(400)).await; // be kind to the rate limit
    }
    Ok(true)
}
