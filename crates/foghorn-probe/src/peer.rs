//! Edge & Node's oracle, watched as a peer — not mirrored.
//!
//! Foghorn used to hold a full copy of their published history and serve it. That is over. There is
//! no canonical QoS oracle: there is the Lodestar Oracle, which measures what it measures, and
//! Edge & Node's, which measures what it measures, and neither is authoritative over the other.
//! Republishing someone else's numbers under our name made us a dependency of their pipeline for no
//! benefit we could not get by measuring the thing ourselves.
//!
//! What remains is the part that is genuinely ours to report: whether the feed we compare against
//! is actually current. A subgraph can sit at chain tip with no indexing errors and still be
//! rejecting every message the publisher sends, which is precisely what happened on 2026-07-01 and
//! precisely the failure no uptime check finds. See [`check_subgraph_health`].

use anyhow::{Context, Result};
use foghorn_core::config::PeerOracleConfig;
use serde_json::{json, Value};
use sqlx::PgPool;
use std::time::Duration;
use tracing::{info, warn};

pub async fn run_peer_watch_loop(cfg: PeerOracleConfig, api_key: Option<String>, pool: PgPool) {
    let Some(api_key) = api_key else {
        warn!("Peer oracle watch needs [gateway].api_key — comparison freshness will be unknown");
        return;
    };
    let url = format!(
        "https://gateway-arbitrum.network.thegraph.com/api/{}/subgraphs/id/{}",
        api_key, cfg.subgraph_id
    );
    let client = match reqwest::Client::builder().timeout(Duration::from_secs(60)).build() {
        Ok(c) => c,
        Err(e) => {
            warn!(error = %e, "Peer oracle watch could not build an HTTP client");
            return;
        }
    };
    info!(subgraph = %cfg.subgraph_id, interval = cfg.interval_secs, "Peer oracle watch starting");
    loop {
        if let Err(e) = check_subgraph_health(&client, &url, &pool).await {
            warn!(error = %e, "Peer oracle health check failed");
        }
        tokio::time::sleep(Duration::from_secs(cfg.interval_secs)).await;
    }
}

/// Check whether the peer's subgraph is ACCEPTING the publisher's messages.
///
/// Added after discovering the deployment usually called canonical had rejected every message for
/// 34 days while looking perfectly healthy — at chain tip, no indexing errors, simply refusing each
/// post with "…is not a valid submitter." and therefore materialising nothing. Publisher liveness
/// could not see it, and neither could a row count.
///
/// This is why comparison is worth keeping while mirroring is not: knowing whether a second opinion
/// is CURRENT is most of what makes it worth having.
///
/// Failures here are logged, never fatal: it is a diagnostic, and losing it must not take anything
/// else down with it.
async fn check_subgraph_health(client: &reqwest::Client, url: &str, pool: &PgPool) -> Result<()> {
    let q = json!({
        "query": "{ _meta { block { number } hasIndexingErrors } \
                    oracleMessages(first: 1, orderBy: createdAt, orderDirection: desc) \
                      { createdAt valid errorMessage } \
                    allocationDailyDataPoints(first: 1, orderBy: dayNumber, orderDirection: desc) \
                      { dayNumber dayStart } }"
    });
    let v: Value = client.post(url).json(&q).send().await?.json().await?;
    if let Some(errors) = v.get("errors") {
        anyhow::bail!("subgraph health query rejected: {errors}");
    }

    let indexed_block = int(v.pointer("/data/_meta/block/number"));
    let has_errors = v
        .pointer("/data/_meta/hasIndexingErrors")
        .and_then(|x| x.as_bool());
    let msg_at = int(v.pointer("/data/oracleMessages/0/createdAt"));
    let msg_valid = v
        .pointer("/data/oracleMessages/0/valid")
        .and_then(|x| x.as_bool());
    let msg_error = text(v.pointer("/data/oracleMessages/0/errorMessage"));
    let day = int(v.pointer("/data/allocationDailyDataPoints/0/dayNumber"));
    let day_start = int(v.pointer("/data/allocationDailyDataPoints/0/dayStart"));

    // Loud, because this is the failure mode that hid for a month.
    if msg_valid == Some(false) {
        warn!(
            error = msg_error.as_deref().unwrap_or("unknown"),
            newest_valid_day = day,
            "CANONICAL SUBGRAPH IS REJECTING THE PUBLISHER'S MESSAGES — no new data is being \
             materialised even though the publisher is posting"
        );
    }

    sqlx::query(
        r#"INSERT INTO oracle_subgraph_health
               (id, indexed_block, has_indexing_errors, newest_message_at, newest_message_valid,
                newest_message_error, newest_valid_day, newest_valid_day_start, checked_at)
           VALUES (TRUE, $1, $2, to_timestamp($3), $4, $5, $6, to_timestamp($7), NOW())
           ON CONFLICT (id) DO UPDATE SET
               indexed_block = EXCLUDED.indexed_block,
               has_indexing_errors = EXCLUDED.has_indexing_errors,
               newest_message_at = EXCLUDED.newest_message_at,
               newest_message_valid = EXCLUDED.newest_message_valid,
               newest_message_error = EXCLUDED.newest_message_error,
               newest_valid_day = EXCLUDED.newest_valid_day,
               newest_valid_day_start = EXCLUDED.newest_valid_day_start,
               checked_at = NOW()"#,
    )
    .bind(indexed_block)
    .bind(has_errors)
    .bind(msg_at.map(|v| v as f64))
    .bind(msg_valid)
    .bind(msg_error)
    .bind(day.map(|d| d as i32))
    .bind(day_start.map(|v| v as f64))
    .execute(pool)
    .await?;
    Ok(())
}

/// The newest `dayNumber` the oracle has published.
///
/// The sync window is anchored to this rather than to our own clock: the oracle's `dayNumber` uses
/// its own epoch, so deriving a floor from wall-time would silently sync nothing whenever the two
/// calendars disagreed.
async fn latest_day(client: &reqwest::Client, url: &str) -> Result<i64> {
    let q = json!({
        "query": "{ allocationDailyDataPoints(first: 1, orderBy: dayNumber, orderDirection: desc) { dayNumber } }"
    });
    let v: Value = client.post(url).json(&q).send().await?.json().await?;
    if let Some(errors) = v.get("errors") {
        anyhow::bail!("oracle subgraph rejected the latest-day query: {errors}");
    }
    num(v.pointer("/data/allocationDailyDataPoints/0/dayNumber"))
        .map(|n| n as i64)
        .context("oracle subgraph returned no dayNumber — cannot resolve a sync window")
}

// ── JSON readers ──────────────────────────────────────────────────────────────
//
// graph-node returns BigInt and BigDecimal as JSON *strings*, not numbers, and switches between the
// two forms depending on the scalar. Reading with `as_i64()` alone silently yields None for every
// string-encoded field, which then reads as "absent" — the failure this whole module exists to
// catch, reproduced inside the catcher.

fn text(v: Option<&Value>) -> Option<String> {
    match v {
        Some(Value::String(s)) if !s.is_empty() => Some(s.clone()),
        Some(Value::Number(n)) => Some(n.to_string()),
        _ => None,
    }
}

fn num(v: Option<&Value>) -> Option<f64> {
    match v {
        Some(Value::String(s)) => s.parse().ok(),
        Some(Value::Number(n)) => n.as_f64(),
        _ => None,
    }
}

fn int(v: Option<&Value>) -> Option<i64> {
    num(v).map(|n| n as i64)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reads_graph_node_scalars_in_both_forms() {
        // graph-node sends BigInt/BigDecimal as strings and Int as a number. Both must work, or a
        // whole field silently reads as null — and null is how a stalled feed looks healthy.
        let v: Value = serde_json::from_str(
            r#"{"dayNumber": 20664, "query_count": "1234", "avg_query_fee": "0.000000004"}"#,
        )
        .unwrap();
        assert_eq!(int(v.get("dayNumber")), Some(20664));
        assert_eq!(int(v.get("query_count")), Some(1234));
        // Precision is preserved by keeping it textual: parsing to f64 and back would not.
        assert_eq!(text(v.get("avg_query_fee")).as_deref(), Some("0.000000004"));
    }

    #[test]
    fn absent_and_empty_fields_are_none_not_zero() {
        let v: Value = serde_json::from_str(r#"{"indexer_url": ""}"#).unwrap();
        assert_eq!(text(v.get("indexer_url")), None);
        assert_eq!(text(v.get("missing")), None);
        assert_eq!(num(v.get("missing")), None);
    }
}
