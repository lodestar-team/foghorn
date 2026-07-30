//! The canonical oracle's publisher liveness, read from Gnosis.
//!
//! Foghorn's view of the Edge & Node QoS oracle used to come entirely through its subgraph, which
//! means every freshness figure was really a measure of *our own ingest clock*. With the oracle 37
//! hours dead on 2026-07-30, `qos/status` reported its age as 187 seconds. This module removes
//! that blind spot by reading the source of truth: the `DataEdge` contract the publisher posts to.
//!
//! The calldata is plain ASCII JSON wrapped in ABI string encoding —
//! `{"topic": "gateway_indexer_attempt_qos_5_minutes_prod_v3", "hash": "Qm…", "timestamp": …}` —
//! so decoding needs no ABI and no node. Blockscout's public API serves the transaction list
//! without an API key, which keeps this path free of every dependency the oracle itself has.
//!
//! ## Capturing payloads while they are still fetchable
//!
//! Historical CIDs are unreachable from public IPFS — eight gateways, four request forms, every one
//! `504 no providers found for the CID`. Indexers must have fetched them when they were fresh, which
//! means availability decays: a payload is retrievable near publication and stops being so later.
//!
//! So each newly-seen message gets one immediate fetch attempt. When it succeeds the raw bytes are
//! ours, independent of anyone's subgraph continuing to exist. When it fails nothing is lost — the
//! subgraph mirror still carries the parsed metrics. Attempts are made once per message, never
//! retried in a later cycle, because a CID that was not served within minutes will not appear later.

use anyhow::{Context, Result};
use chrono::{DateTime, TimeZone, Utc};
use foghorn_core::config::DataEdgeConfig;
use serde::Deserialize;
use sqlx::PgPool;
use std::time::Duration;
use tracing::{debug, info, warn};

#[derive(Debug, Deserialize)]
struct BlockscoutPage {
    items: Vec<BlockscoutTx>,
}

#[derive(Debug, Deserialize)]
struct BlockscoutTx {
    hash: String,
    raw_input: String,
    timestamp: String,
    block_number: i64,
    /// "ok" for successful transactions. A reverted post is not a publication.
    status: Option<String>,
}

/// The publisher's payload, as carried in calldata.
#[derive(Debug, Deserialize)]
struct OraclePayload {
    topic: String,
    hash: String,
    /// Epoch seconds of the 5-minute bucket the payload describes.
    timestamp: i64,
}

pub async fn run_dataedge_loop(cfg: DataEdgeConfig, pool: PgPool) {
    if !cfg.enabled {
        info!("DataEdge poller disabled by config");
        return;
    }
    info!(
        address = %cfg.address,
        interval = cfg.interval_secs,
        "DataEdge poller starting (canonical oracle publisher liveness)"
    );
    loop {
        match poll_once(&cfg, &pool).await {
            Ok(n) => {
                if n > 0 {
                    info!(new_messages = n, "DataEdge poll stored new oracle messages");
                } else {
                    debug!("DataEdge poll: nothing new");
                }
            }
            Err(e) => warn!(error = %e, "DataEdge poll failed"),
        }
        tokio::time::sleep(Duration::from_secs(cfg.interval_secs)).await;
    }
}

/// Fetch the most recent posts and store any we have not seen.
///
/// Only the first page is read. This runs on a short interval against a publisher that posts every
/// five minutes, so a page of 50 is many multiples of what can accumulate between polls; paging
/// further would re-read the same history forever. The consequence is bounded and acceptable: if
/// Foghorn is down for longer than a page's worth of posts, that window is missing from the lag
/// series. `posted_at` gaps make such a hole visible rather than silent, which is the property the
/// oracle's own pipeline lacks.
pub async fn poll_once(cfg: &DataEdgeConfig, pool: &PgPool) -> Result<u64> {
    let url = format!(
        "{}/api/v2/addresses/{}/transactions?filter=to",
        cfg.explorer_base.trim_end_matches('/'),
        cfg.address
    );
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(cfg.timeout_secs))
        .build()?;
    let page: BlockscoutPage = client
        .get(&url)
        .send()
        .await
        .context("DataEdge explorer request failed")?
        .json()
        .await
        .context("DataEdge explorer returned unparseable JSON")?;

    let mut stored = 0u64;
    for tx in &page.items {
        // A reverted transaction published nothing, so counting it as liveness would mask an
        // outage where the publisher is trying and failing.
        if tx.status.as_deref() == Some("error") {
            continue;
        }
        let Some(payload) = decode_payload(&tx.raw_input) else {
            // Not every call to a DataEdge has to be a QoS payload. Skip quietly rather than
            // failing the whole poll over one unexpected transaction.
            debug!(tx = %tx.hash, "DataEdge tx with no decodable payload");
            continue;
        };
        let Some(bucket_ts) = Utc.timestamp_opt(payload.timestamp, 0).single() else {
            warn!(tx = %tx.hash, ts = payload.timestamp, "DataEdge payload has an impossible timestamp");
            continue;
        };
        let posted_at: DateTime<Utc> = match tx.timestamp.parse::<DateTime<Utc>>() {
            Ok(t) => t,
            Err(e) => {
                warn!(tx = %tx.hash, error = %e, "unparseable explorer timestamp");
                continue;
            }
        };
        let lag = (posted_at - bucket_ts).num_seconds();

        let res = sqlx::query(
            r#"INSERT INTO oracle_message
                   (tx_hash, topic, ipfs_hash, bucket_ts, posted_at, block_number, lag_seconds)
               VALUES ($1, $2, $3, $4, $5, $6, $7)
               ON CONFLICT (tx_hash) DO NOTHING"#,
        )
        .bind(&tx.hash)
        .bind(&payload.topic)
        .bind(&payload.hash)
        .bind(bucket_ts)
        .bind(posted_at)
        .bind(tx.block_number)
        .bind(lag as i32)
        .execute(pool)
        .await?;
        let inserted = res.rows_affected();
        stored += inserted;

        // Only for messages we have not seen before. A CID that was not served within minutes of
        // publication will not appear later, so retrying old ones on every cycle would be pure waste.
        if inserted > 0 && cfg.capture_payloads {
            capture_payload(&client, pool, &payload.hash, &payload.topic, bucket_ts, cfg).await;
        }
    }

    Ok(stored)
}


/// One best-effort fetch of a pinned payload, stored verbatim on success.
///
/// Deliberately quiet about failure: a miss is the expected case for anything but a very fresh CID,
/// and logging it as an error every five minutes would bury real problems. Recorded `via` so the
/// decay of gateway availability is measurable rather than folklore.
async fn capture_payload(
    client: &reqwest::Client,
    pool: &PgPool,
    cid: &str,
    topic: &str,
    bucket_ts: DateTime<Utc>,
    cfg: &DataEdgeConfig,
) {
    for base in cfg.ipfs_gateways.iter() {
        let url = format!("{}/{}", base.trim_end_matches('/'), cid);
        let Ok(resp) = client.get(&url).send().await else { continue };
        if !resp.status().is_success() {
            continue;
        }
        let Ok(body) = resp.text().await else { continue };
        // A gateway error page is HTML, not a payload. Requiring valid JSON keeps junk out of a table
        // whose entire value is being the publisher's own bytes.
        if serde_json::from_str::<serde_json::Value>(&body).is_err() {
            continue;
        }
        let bytes = body.len() as i32;
        let res = sqlx::query(
            r#"INSERT INTO oracle_payload (ipfs_hash, topic, bucket_ts, raw, bytes, via)
               VALUES ($1, $2, $3, $4, $5, $6)
               ON CONFLICT (ipfs_hash) DO NOTHING"#,
        )
        .bind(cid)
        .bind(topic)
        .bind(bucket_ts)
        .bind(&body)
        .bind(bytes)
        .bind(base)
        .execute(pool)
        .await;
        if res.is_ok() {
            info!(cid, topic, bytes, via = %base, "Captured oracle payload from IPFS");
        }
        return;
    }
    debug!(cid, "Oracle payload not retrievable from any configured IPFS gateway");
}

/// Pull the JSON object out of ABI-encoded calldata.
///
/// Properly ABI-decoding the string argument would need the DataEdge's ABI, which is not published
/// (the contract is unverified). The payload is ASCII JSON inside the encoded string, so the
/// object is located by scanning for its braces. Deliberately narrow: the first `{` to the last `}`
/// of a well-formed object, parsed strictly, so garbage is rejected rather than half-read.
fn decode_payload(raw_input: &str) -> Option<OraclePayload> {
    let hex = raw_input.strip_prefix("0x").unwrap_or(raw_input);
    let bytes = hex::decode(hex).ok()?;
    let start = bytes.iter().position(|&b| b == b'{')?;
    let end = bytes.iter().rposition(|&b| b == b'}')?;
    if end <= start {
        return None;
    }
    let json = std::str::from_utf8(&bytes[start..=end]).ok()?;
    serde_json::from_str(json).ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Real calldata from tx 0x7d0c254a… — the last message the oracle published before it died
    /// on 2026-07-29. Keeping a genuine sample means this test fails if the decode ever stops
    /// handling what the publisher actually emits.
    const REAL_CALLDATA: &str = "0x53b734470000000000000000000000000000000000000000000000000000000000000020000000000000000000000000000000000000000000000000000000000000008d7b22746f70696322\
3a2022676174657761795f696e64657865725f617474656d70745f716f735f355f6d696e757465735f70726f645f7633222c202268617368223a2022516d6646666a5a56356f485154344c6e4e45543555745a54687445337553733776377868756\
34377377670424152222c202274696d657374616d70223a20313738353238343730307d00000000000000000000000000000000000000";

    #[test]
    fn decodes_real_publisher_calldata() {
        let p = decode_payload(&REAL_CALLDATA.replace('\n', "")).expect("should decode");
        assert_eq!(p.topic, "gateway_indexer_attempt_qos_5_minutes_prod_v3");
        assert_eq!(p.hash, "QmfFfjZV5oHQT4LnNET5UtZThtE3uSs7v7xhucCw7vpBAR");
        assert_eq!(p.timestamp, 1785284700);
    }

    #[test]
    fn rejects_calldata_without_a_payload() {
        assert!(decode_payload("0xdeadbeef").is_none());
        assert!(decode_payload("0x").is_none());
        // A brace-free ASCII body must not be mistaken for a payload.
        assert!(decode_payload("0x68656c6c6f").is_none());
    }

    #[test]
    fn rejects_malformed_json_rather_than_half_reading_it() {
        // "{"topic": "x"" — truncated, no closing brace for the object's fields.
        let hex = format!("0x{}", hex::encode(b"{\"topic\": \"x\""));
        assert!(decode_payload(&hex).is_none());
    }
}
