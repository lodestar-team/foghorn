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

/// How often the Edge & Node oracle stall check runs.
///
/// Deliberately far shorter than `POLL_SECS`. The failure-roster digest is hourly on purpose — it
/// is a summary and re-posting it often would train people to mute the channel. Oracle liveness is
/// the opposite: on 2026-07-29 the community discovered a stall by hand at hour 16, and an hourly
/// check would have added up to another hour on top of the detection threshold. Publishing every 5
/// minutes means a 5-minute check is proportionate.
const ORACLE_POLL_SECS: u64 = 300;

/// Watch Edge & Node's publisher, independently of the roster digest's cadence.
pub async fn run_oracle_watch_loop(webhook: String, pool: PgPool) {
    info!(interval = ORACLE_POLL_SECS, "Oracle stall watch starting");
    let client = match reqwest::Client::builder().timeout(Duration::from_secs(15)).build() {
        Ok(c) => c,
        Err(e) => {
            warn!(error = %e, "Failed to build oracle watch client — oracle alerting disabled");
            return;
        }
    };
    loop {
        if let Err(e) = alert_oracle_stall(&client, &webhook, &pool).await {
            warn!(error = %e, "Oracle stall alert failed");
        }
        tokio::time::sleep(Duration::from_secs(ORACLE_POLL_SECS)).await;
    }
}

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

    // Advance the hysteresis machine and get the debounced fingerprint. Change
    // detection is on this committed roster+severity, NOT exact counts and NOT
    // single-cycle flicker — so a count drift (26→27), a long-tail indexer blinking
    // in for one cycle, or a one-cycle severity flip won't re-ping the channel. Only
    // a sustained new failure, recovery, or severity change does. The posted message
    // still carries the full live roster, and the daily digest refreshes it anyway.
    let fingerprint = debounced_fingerprint(pool, &roster.states).await?;

    // Read last-posted fingerprint + whether 24h has elapsed (daily liveness repost).
    let (last_fingerprint, due): (Option<String>, bool) = sqlx::query_as(
        "SELECT last_fingerprint, (last_post IS NULL OR last_post < NOW() - INTERVAL '24 hours') FROM alerter_state",
    )
    .fetch_one(pool)
    .await
    .unwrap_or((None, true));

    let changed = fingerprint != last_fingerprint.unwrap_or_default();
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
        .bind(&fingerprint)
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
    /// Raw observed (address, severity 'C'|'H') for this cycle — fed to the
    /// hysteresis debounce, which decides what actually moves the trigger.
    states: Vec<(String, String)>,
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

    // Raw observed states (address, severity), independent of counts — the
    // hysteresis debounce in run_cycle turns these into the committed fingerprint.
    let states: Vec<(String, String)> = by
        .iter()
        .map(|(addr, a)| (addr.clone(), if a.critical { "C" } else { "H" }.to_string()))
        .collect();

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

    Ok(Roster { lines, states })
}

/// Cycles a new per-indexer state must hold before it moves the trigger
/// fingerprint. At the hourly cadence, 2 ≈ "stable for ~2h" — enough to swallow
/// the long-tail flicker (a single erroring deployment blinking past the volume
/// floor) and one-cycle severity flips, without delaying genuine changes much.
const CONFIRM_CYCLES: i32 = 2;

/// Advance the hysteresis state machine with this cycle's raw observations and
/// return the committed (debounced) fingerprint — sorted `address:severity` over
/// indexers whose state has settled. Transient flickers never reach the threshold,
/// so they don't change the fingerprint and don't trigger a post.
async fn debounced_fingerprint(pool: &PgPool, states: &[(String, String)]) -> Result<String> {
    use std::collections::HashMap as Map;
    let observed: Map<&str, &str> = states.iter().map(|(a, s)| (a.as_str(), s.as_str())).collect();

    // Existing member state.
    let rows = sqlx::query("SELECT indexer_address, stable_state, candidate_state, streak FROM alert_member")
        .fetch_all(pool)
        .await?;
    let mut members: Map<String, (String, String, i32)> = Map::new();
    for r in &rows {
        members.insert(
            r.get("indexer_address"),
            (r.get("stable_state"), r.get("candidate_state"), r.get("streak")),
        );
    }

    // Union of observed + known addresses.
    let mut addrs: Vec<String> = members.keys().cloned().collect();
    for (a, _) in states {
        if !members.contains_key(a) {
            addrs.push(a.clone());
        }
    }

    let mut committed: Vec<String> = Vec::new();
    for addr in &addrs {
        let obs = observed.get(addr.as_str()).copied().unwrap_or("absent");
        let (mut stable, mut candidate, mut streak) = members
            .get(addr)
            .cloned()
            .unwrap_or_else(|| ("absent".to_string(), "absent".to_string(), 0));

        if obs == stable {
            // Reverted to the committed state before confirming a change.
            candidate = obs.to_string();
            streak = 0;
        } else if obs == candidate {
            streak += 1;
            if streak >= CONFIRM_CYCLES {
                stable = obs.to_string(); // confirmed — commit it
                streak = 0;
            }
        } else {
            candidate = obs.to_string(); // new candidate, restart the count
            streak = 1;
        }

        if stable == "absent" && candidate == "absent" {
            sqlx::query("DELETE FROM alert_member WHERE indexer_address = $1")
                .bind(addr)
                .execute(pool)
                .await?;
        } else {
            sqlx::query(
                r#"INSERT INTO alert_member (indexer_address, stable_state, candidate_state, streak, updated_at)
                   VALUES ($1,$2,$3,$4, NOW())
                   ON CONFLICT (indexer_address) DO UPDATE SET
                     stable_state = EXCLUDED.stable_state, candidate_state = EXCLUDED.candidate_state,
                     streak = EXCLUDED.streak, updated_at = NOW()"#,
            )
            .bind(addr).bind(&stable).bind(&candidate).bind(streak)
            .execute(pool)
            .await?;
            if stable != "absent" {
                committed.push(format!("{addr}:{stable}"));
            }
        }
    }

    committed.sort();
    Ok(committed.join(","))
}

/// Header + all lines + footer, joined (caller chunks it for Discord's limit).
fn build_message_body(header: &str, lines: &[String]) -> String {
    let footer = format!("For more details — {}/foghorn", DASHBOARD);
    format!("{header}\n{}\n{footer}", lines.join("\n"))
}


/// Post when Edge & Node's QoS oracle stops publishing, and once when it recovers.
///
/// Thresholds reflect the publisher's real behaviour rather than a guess: it posts every 5 minutes
/// with a steady ~30-minute lag, so 90 minutes of silence is unambiguous and well clear of routine
/// jitter. State is a single row keyed `oracle_stall`, so a continuing outage is not re-posted every
/// cycle — the channel gets one alert on the way down, one on the way back up.
async fn alert_oracle_stall(client: &reqwest::Client, webhook: &str, pool: &PgPool) -> Result<()> {
    const STALL_SECS: i64 = 90 * 60;

    let (posted, bucket, lag): (
        Option<chrono::DateTime<chrono::Utc>>,
        Option<chrono::DateTime<chrono::Utc>>,
        Option<i32>,
    ) = sqlx::query_as(
        "SELECT max(posted_at),
                (SELECT bucket_ts   FROM oracle_message ORDER BY posted_at DESC LIMIT 1),
                (SELECT lag_seconds FROM oracle_message ORDER BY posted_at DESC LIMIT 1)
         FROM oracle_message",
    )
    .fetch_one(pool)
    .await?;

    // No observations yet means the DataEdge poller has not run, not that the oracle is dead. Saying
    // "stalled" here would be a false alarm on every fresh deployment.
    let Some(posted) = posted else { return Ok(()) };

    let age = (chrono::Utc::now() - posted).num_seconds();
    let stalled = age >= STALL_SECS;
    let was_stalled: bool = sqlx::query_scalar(
        "SELECT COALESCE((SELECT value = 'stalled' FROM alerter_flag WHERE key = 'oracle_stall'), false)",
    )
    .fetch_one(pool)
    .await
    .unwrap_or(false);

    if stalled == was_stalled {
        return Ok(());
    }

    let hours = age as f64 / 3600.0;
    let content = if stalled {
        format!(
            "**Canonical QoS oracle has stopped publishing**\n\
             Last DataEdge post: `{}` ({:.1}h ago)\n\
             Last bucket published: `{}`\n\
             Publish lag at the time: `{}`\n\
             Read from Gnosis directly, not from the subgraph — a stale subgraph answers exactly \
             like a fresh one.\n\
             Canonical history and Lodestar's own measurements remain queryable: \
             <https://www.lodestar-dashboard.com/qos>",
            posted.to_rfc3339(),
            hours,
            bucket.map(|b| b.to_rfc3339()).unwrap_or_else(|| "unknown".into()),
            lag.map(|l| format!("{}m", l / 60)).unwrap_or_else(|| "unknown".into()),
        )
    } else {
        format!(
            "**Canonical QoS oracle is publishing again**\n\
             Latest post: `{}` ({}m ago). Worth checking whether it backfilled the gap or resumed \
             from tip — a resumed-from-tip publisher leaves a permanent hole that looks like \
             complete data.",
            posted.to_rfc3339(),
            age / 60,
        )
    };

    if !post_chunks(client, webhook, &content).await? {
        // Webhook rejected: leave the state untouched so the alert retries rather than being lost.
        return Ok(());
    }

    sqlx::query(
        r#"INSERT INTO alerter_flag (key, value, updated_at) VALUES ('oracle_stall', $1, NOW())
           ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value, updated_at = NOW()"#,
    )
    .bind(if stalled { "stalled" } else { "live" })
    .execute(pool)
    .await?;

    Ok(())
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
