//! A full mirror of the canonical Gateway QoS Oracle.
//!
//! [`ingest`](crate::ingest) takes six fields of one entity and reduces them to a scoring input.
//! This takes *everything*, unreduced, into tables Lodestar owns — so when the publisher stalls
//! (37 hours on 2026-07-29, unannounced) indexers still have a queryable, API-key-free copy of
//! every number ever published, in the oracle's own field names.
//!
//! ## Why the subgraph is the only possible source
//!
//! The metrics come from Edge & Node's private gateway telemetry, so nobody else can recompute
//! them. They are not on-chain either: the DataEdge carries only `{topic, ipfs_hash, timestamp}`,
//! and the pinned payloads are unreachable from public IPFS — eight gateways, four request forms,
//! every one `504 no providers found for the CID`. Their subgraph is where the history survives.
//!
//! ## What this cannot do
//!
//! It cannot produce data for a window the publisher never generated. During an outage the mirror
//! is as frozen as its source. The difference is that the freeze is visible (`oracle_message`, read
//! from Gnosis) and the history stays served.
//!
//! ## Pagination
//!
//! Keyset by `id_gt` rather than `skip`, because graph-node's `skip` degrades badly and silently
//! caps. Each cycle re-pulls a trailing window of days: the newest days are still being written by
//! the publisher, so a one-shot sync would freeze partial values forever. Primary keys are the
//! oracle's entity ids, so re-pulling converges instead of duplicating.

use anyhow::{Context, Result};
use serde_json::{json, Value};
use sqlx::PgPool;
use std::time::Duration;
use tracing::{info, warn};

use foghorn_core::config::OracleMirrorConfig;

/// Fields per entity, verbatim from the reference schema
/// (`juanmardefago/gateway-qos-oracle-example-subgraph`). Kept as literal GraphQL selections so a
/// mismatch fails loudly at query time rather than silently omitting a column.
const ALLOCATION_DAILY_FIELDS: &str = "id dayNumber dayStart dayEnd dataPointCount indexer_wallet \
    indexer_url subgraph_deployment_ipfs_hash avg_indexer_blocks_behind avg_indexer_latency_ms \
    avg_query_fee max_indexer_blocks_behind max_indexer_latency_ms max_query_fee \
    num_indexer_200_responses proportion_indexer_200_responses query_count total_query_fees \
    start_epoch end_epoch chain_id gateway_id";

const INDEXER_DAILY_FIELDS: &str = ALLOCATION_DAILY_FIELDS;

const QUERY_DAILY_FIELDS: &str = "id dayNumber dayStart dayEnd dataPointCount \
    subgraph_deployment_ipfs_hash avg_gateway_latency_ms max_gateway_latency_ms avg_query_fee \
    max_query_fee gateway_query_success_rate user_attributed_error_rate most_recent_query_ts \
    query_count total_query_fees start_epoch end_epoch chain_id gateway_id";

/// The 5-minute entity. Adds `stdev_indexer_latency_ms`, which the daily rollups drop.
const ALLOCATION_POINT_FIELDS: &str = "id dayNumber dayStart dayEnd indexer_wallet indexer_url \
    subgraph_deployment_ipfs_hash avg_indexer_blocks_behind avg_indexer_latency_ms \
    stdev_indexer_latency_ms avg_query_fee max_indexer_blocks_behind max_indexer_latency_ms \
    max_query_fee num_indexer_200_responses proportion_indexer_200_responses query_count \
    total_query_fees start_epoch end_epoch chain_id gateway_id";

pub async fn run_mirror_loop(cfg: OracleMirrorConfig, api_key: Option<String>, pool: PgPool) {
    if !cfg.enabled {
        info!("Oracle mirror disabled by config");
        return;
    }
    let Some(api_key) = api_key else {
        // The oracle's subgraph is only reachable through the gateway, which needs a key. Say so
        // once and stop, rather than logging a failure every cycle forever.
        warn!("Oracle mirror needs [gateway].api_key to reach the oracle subgraph — disabled");
        return;
    };
    info!(
        subgraph = %cfg.subgraph_id,
        window_days = cfg.window_days,
        interval = cfg.interval_secs,
        "Oracle mirror loop starting (full canonical QoS copy)"
    );
    loop {
        match sync_once(&cfg, &api_key, &pool).await {
            Ok(c) => info!(
                allocation_daily = c.allocation_daily,
                indexer_daily = c.indexer_daily,
                query_daily = c.query_daily,
                allocation_points = c.allocation_points,
                "Oracle mirror cycle complete"
            ),
            Err(e) => warn!(error = %e, "Oracle mirror cycle failed"),
        }
        tokio::time::sleep(Duration::from_secs(cfg.interval_secs)).await;
    }
}

#[derive(Debug, Default)]
pub struct SyncCounts {
    pub allocation_daily: u64,
    pub indexer_daily: u64,
    pub query_daily: u64,
    pub allocation_points: u64,
}

pub async fn sync_once(
    cfg: &OracleMirrorConfig,
    api_key: &str,
    pool: &PgPool,
) -> Result<SyncCounts> {
    let url = format!(
        "{}/{}/subgraphs/id/{}",
        cfg.gateway_base.trim_end_matches('/'),
        api_key,
        cfg.subgraph_id
    );
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(cfg.timeout_secs))
        .build()?;

    // The oracle's dayNumber epoch is its own, so the floor is derived from the newest day the
    // oracle itself reports rather than from our clock. Doing it the other way round would silently
    // sync nothing whenever the two calendars disagreed.
    let latest = latest_day(&client, &url).await?;
    let floor = latest.saturating_sub(cfg.window_days.max(1) as i64 - 1);
    info!(latest_day = latest, day_floor = floor, "Oracle mirror window resolved");

    let mut counts = SyncCounts::default();

    let rows = paginate(&client, &url, "allocationDailyDataPoints", ALLOCATION_DAILY_FIELDS, floor, cfg).await?;
    counts.allocation_daily = upsert_allocation_daily(pool, &rows).await?;

    let rows = paginate(&client, &url, "indexerDailyDataPoints", INDEXER_DAILY_FIELDS, floor, cfg).await?;
    counts.indexer_daily = upsert_indexer_daily(pool, &rows).await?;

    let rows = paginate(&client, &url, "queryDailyDataPoints", QUERY_DAILY_FIELDS, floor, cfg).await?;
    counts.query_daily = upsert_query_daily(pool, &rows).await?;

    // Highest-volume entity by far (one row per indexer × deployment × 5 minutes), so it is synced
    // over its own, shorter window. Pulling the full day window of these would be most of the
    // request budget for the least-queried table.
    let point_floor = latest.saturating_sub(cfg.point_window_days.max(1) as i64 - 1);
    let rows = paginate(&client, &url, "allocationDataPoints", ALLOCATION_POINT_FIELDS, point_floor, cfg).await?;
    counts.allocation_points = upsert_allocation_point(pool, &rows).await?;

    Ok(counts)
}

/// The newest `dayNumber` the oracle has published.
async fn latest_day(client: &reqwest::Client, url: &str) -> Result<i64> {
    let q = json!({
        "query": "{ allocationDailyDataPoints(first: 1, orderBy: dayNumber, orderDirection: desc) { dayNumber } }"
    });
    let v: Value = client.post(url).json(&q).send().await?.json().await?;
    num(v.pointer("/data/allocationDailyDataPoints/0/dayNumber"))
        .map(|n| n as i64)
        .context("oracle subgraph returned no dayNumber — cannot resolve a sync window")
}

/// Keyset-paginate one entity.
async fn paginate(
    client: &reqwest::Client,
    url: &str,
    entity: &str,
    fields: &str,
    day_gte: i64,
    cfg: &OracleMirrorConfig,
) -> Result<Vec<Value>> {
    let mut out: Vec<Value> = Vec::new();
    let mut last_id = String::new();
    for page in 0..cfg.max_pages.max(1) {
        let q = json!({
            "query": format!(
                r#"{{ {entity}(first: 1000, orderBy: id, orderDirection: asc,
                       where: {{ dayNumber_gte: {day_gte}, id_gt: "{last_id}" }})
                     {{ {fields} }} }}"#
            )
        });
        let v: Value = client
            .post(url)
            .json(&q)
            .send()
            .await
            .with_context(|| format!("{entity} page {page} request failed"))?
            .json()
            .await
            .with_context(|| format!("{entity} page {page} returned unparseable JSON"))?;

        // A GraphQL error here means the query is wrong (a renamed field, a missing entity) and
        // every later page would fail identically. Surface it instead of returning a short read
        // that looks like the end of the data.
        if let Some(errors) = v.get("errors") {
            anyhow::bail!("{entity} query rejected by the oracle subgraph: {errors}");
        }

        let Some(items) = v.pointer(&format!("/data/{entity}")).and_then(|x| x.as_array()) else {
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
        out.extend(items.iter().cloned());
        if !full || last_id.is_empty() {
            break;
        }
        if page + 1 == cfg.max_pages.max(1) {
            // Never let a cap look like completeness.
            warn!(entity, pages = cfg.max_pages, rows = out.len(),
                  "Oracle mirror hit its page cap — this entity is INCOMPLETE for this cycle");
        }
    }
    Ok(out)
}

// ── Field readers ───────────────────────────────────────────────────────────
//
// graph-node returns BigInt/BigDecimal as strings and Int as a number, so every reader accepts
// both. Numeric values are kept as STRINGS and cast to NUMERIC in SQL: parsing them into f64 here
// would quietly lose precision on query fees, which are small enough for it to matter.

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

macro_rules! bind_numerics {
    ($q:expr, $r:expr, $($field:literal),+ $(,)?) => {{
        let mut q = $q;
        $( q = q.bind(text($r.get($field))); )+
        q
    }};
}

async fn upsert_allocation_daily(pool: &PgPool, rows: &[Value]) -> Result<u64> {
    let mut n = 0u64;
    for r in rows {
        let Some(id) = r["id"].as_str() else { continue };
        let q = sqlx::query(
            r#"INSERT INTO oracle_allocation_daily (
                   id, day_number, day_start, day_end, data_point_count,
                   indexer_wallet, indexer_url, subgraph_deployment_ipfs_hash,
                   avg_indexer_blocks_behind, avg_indexer_latency_ms, avg_query_fee,
                   max_indexer_blocks_behind, max_indexer_latency_ms, max_query_fee,
                   num_indexer_200_responses, proportion_indexer_200_responses,
                   query_count, total_query_fees, start_epoch, end_epoch,
                   chain_id, gateway_id, synced_at)
               VALUES ($1,$2,$3,$4,$5,$6,$7,$8,
                       CAST($9 AS numeric),CAST($10 AS numeric),CAST($11 AS numeric),
                       CAST($12 AS numeric),CAST($13 AS numeric),CAST($14 AS numeric),
                       CAST($15 AS numeric),CAST($16 AS numeric),CAST($17 AS numeric),
                       CAST($18 AS numeric),CAST($19 AS numeric),CAST($20 AS numeric),
                       $21,$22,NOW())
               ON CONFLICT (id) DO UPDATE SET
                   day_start = EXCLUDED.day_start, day_end = EXCLUDED.day_end,
                   data_point_count = EXCLUDED.data_point_count,
                   indexer_url = EXCLUDED.indexer_url,
                   avg_indexer_blocks_behind = EXCLUDED.avg_indexer_blocks_behind,
                   avg_indexer_latency_ms = EXCLUDED.avg_indexer_latency_ms,
                   avg_query_fee = EXCLUDED.avg_query_fee,
                   max_indexer_blocks_behind = EXCLUDED.max_indexer_blocks_behind,
                   max_indexer_latency_ms = EXCLUDED.max_indexer_latency_ms,
                   max_query_fee = EXCLUDED.max_query_fee,
                   num_indexer_200_responses = EXCLUDED.num_indexer_200_responses,
                   proportion_indexer_200_responses = EXCLUDED.proportion_indexer_200_responses,
                   query_count = EXCLUDED.query_count,
                   total_query_fees = EXCLUDED.total_query_fees,
                   start_epoch = EXCLUDED.start_epoch, end_epoch = EXCLUDED.end_epoch,
                   chain_id = EXCLUDED.chain_id, gateway_id = EXCLUDED.gateway_id,
                   synced_at = NOW()"#,
        )
        .bind(id)
        .bind(int(r.get("dayNumber")).unwrap_or_default() as i32)
        .bind(int(r.get("dayStart")))
        .bind(int(r.get("dayEnd")))
        .bind(int(r.get("dataPointCount")))
        .bind(text(r.get("indexer_wallet")).unwrap_or_default().to_lowercase())
        .bind(text(r.get("indexer_url")))
        .bind(text(r.get("subgraph_deployment_ipfs_hash")).unwrap_or_default());
        let q = bind_numerics!(
            q, r,
            "avg_indexer_blocks_behind", "avg_indexer_latency_ms", "avg_query_fee",
            "max_indexer_blocks_behind", "max_indexer_latency_ms", "max_query_fee",
            "num_indexer_200_responses", "proportion_indexer_200_responses",
            "query_count", "total_query_fees", "start_epoch", "end_epoch",
        );
        n += q
            .bind(text(r.get("chain_id")))
            .bind(text(r.get("gateway_id")))
            .execute(pool)
            .await?
            .rows_affected();
    }
    Ok(n)
}

async fn upsert_indexer_daily(pool: &PgPool, rows: &[Value]) -> Result<u64> {
    let mut n = 0u64;
    for r in rows {
        let Some(id) = r["id"].as_str() else { continue };
        let q = sqlx::query(
            r#"INSERT INTO oracle_indexer_daily (
                   id, day_number, day_start, day_end, data_point_count,
                   indexer_wallet, indexer_url, subgraph_deployment_ipfs_hash,
                   avg_indexer_blocks_behind, avg_indexer_latency_ms, avg_query_fee,
                   max_indexer_blocks_behind, max_indexer_latency_ms, max_query_fee,
                   num_indexer_200_responses, proportion_indexer_200_responses,
                   query_count, total_query_fees, start_epoch, end_epoch,
                   chain_id, gateway_id, synced_at)
               VALUES ($1,$2,$3,$4,$5,$6,$7,$8,
                       CAST($9 AS numeric),CAST($10 AS numeric),CAST($11 AS numeric),
                       CAST($12 AS numeric),CAST($13 AS numeric),CAST($14 AS numeric),
                       CAST($15 AS numeric),CAST($16 AS numeric),CAST($17 AS numeric),
                       CAST($18 AS numeric),CAST($19 AS numeric),CAST($20 AS numeric),
                       $21,$22,NOW())
               ON CONFLICT (id) DO UPDATE SET
                   query_count = EXCLUDED.query_count,
                   num_indexer_200_responses = EXCLUDED.num_indexer_200_responses,
                   proportion_indexer_200_responses = EXCLUDED.proportion_indexer_200_responses,
                   avg_indexer_latency_ms = EXCLUDED.avg_indexer_latency_ms,
                   avg_indexer_blocks_behind = EXCLUDED.avg_indexer_blocks_behind,
                   total_query_fees = EXCLUDED.total_query_fees,
                   synced_at = NOW()"#,
        )
        .bind(id)
        .bind(int(r.get("dayNumber")).unwrap_or_default() as i32)
        .bind(int(r.get("dayStart")))
        .bind(int(r.get("dayEnd")))
        .bind(int(r.get("dataPointCount")))
        .bind(text(r.get("indexer_wallet")).unwrap_or_default().to_lowercase())
        .bind(text(r.get("indexer_url")))
        .bind(text(r.get("subgraph_deployment_ipfs_hash")));
        let q = bind_numerics!(
            q, r,
            "avg_indexer_blocks_behind", "avg_indexer_latency_ms", "avg_query_fee",
            "max_indexer_blocks_behind", "max_indexer_latency_ms", "max_query_fee",
            "num_indexer_200_responses", "proportion_indexer_200_responses",
            "query_count", "total_query_fees", "start_epoch", "end_epoch",
        );
        n += q
            .bind(text(r.get("chain_id")))
            .bind(text(r.get("gateway_id")))
            .execute(pool)
            .await?
            .rows_affected();
    }
    Ok(n)
}

async fn upsert_query_daily(pool: &PgPool, rows: &[Value]) -> Result<u64> {
    let mut n = 0u64;
    for r in rows {
        let Some(id) = r["id"].as_str() else { continue };
        let q = sqlx::query(
            r#"INSERT INTO oracle_query_daily (
                   id, day_number, day_start, day_end, data_point_count,
                   subgraph_deployment_ipfs_hash,
                   avg_gateway_latency_ms, max_gateway_latency_ms, avg_query_fee, max_query_fee,
                   gateway_query_success_rate, user_attributed_error_rate, most_recent_query_ts,
                   query_count, total_query_fees, start_epoch, end_epoch,
                   chain_id, gateway_id, synced_at)
               VALUES ($1,$2,$3,$4,$5,$6,
                       CAST($7 AS numeric),CAST($8 AS numeric),CAST($9 AS numeric),
                       CAST($10 AS numeric),CAST($11 AS numeric),CAST($12 AS numeric),
                       CAST($13 AS numeric),CAST($14 AS numeric),CAST($15 AS numeric),
                       CAST($16 AS numeric),CAST($17 AS numeric),
                       $18,$19,NOW())
               ON CONFLICT (id) DO UPDATE SET
                   query_count = EXCLUDED.query_count,
                   gateway_query_success_rate = EXCLUDED.gateway_query_success_rate,
                   user_attributed_error_rate = EXCLUDED.user_attributed_error_rate,
                   avg_gateway_latency_ms = EXCLUDED.avg_gateway_latency_ms,
                   most_recent_query_ts = EXCLUDED.most_recent_query_ts,
                   total_query_fees = EXCLUDED.total_query_fees,
                   synced_at = NOW()"#,
        )
        .bind(id)
        .bind(int(r.get("dayNumber")).unwrap_or_default() as i32)
        .bind(int(r.get("dayStart")))
        .bind(int(r.get("dayEnd")))
        .bind(int(r.get("dataPointCount")))
        .bind(text(r.get("subgraph_deployment_ipfs_hash")).unwrap_or_default());
        let q = bind_numerics!(
            q, r,
            "avg_gateway_latency_ms", "max_gateway_latency_ms", "avg_query_fee", "max_query_fee",
            "gateway_query_success_rate", "user_attributed_error_rate", "most_recent_query_ts",
            "query_count", "total_query_fees", "start_epoch", "end_epoch",
        );
        n += q
            .bind(text(r.get("chain_id")))
            .bind(text(r.get("gateway_id")))
            .execute(pool)
            .await?
            .rows_affected();
    }
    Ok(n)
}

async fn upsert_allocation_point(pool: &PgPool, rows: &[Value]) -> Result<u64> {
    let mut n = 0u64;
    for r in rows {
        let Some(id) = r["id"].as_str() else { continue };
        let q = sqlx::query(
            r#"INSERT INTO oracle_allocation_point (
                   id, day_number, day_start, day_end,
                   indexer_wallet, indexer_url, subgraph_deployment_ipfs_hash,
                   avg_indexer_blocks_behind, avg_indexer_latency_ms, stdev_indexer_latency_ms,
                   avg_query_fee, max_indexer_blocks_behind, max_indexer_latency_ms, max_query_fee,
                   num_indexer_200_responses, proportion_indexer_200_responses,
                   query_count, total_query_fees, start_epoch, end_epoch,
                   chain_id, gateway_id, synced_at)
               VALUES ($1,$2,$3,$4,$5,$6,$7,
                       CAST($8 AS numeric),CAST($9 AS numeric),CAST($10 AS numeric),
                       CAST($11 AS numeric),CAST($12 AS numeric),CAST($13 AS numeric),
                       CAST($14 AS numeric),CAST($15 AS numeric),CAST($16 AS numeric),
                       CAST($17 AS numeric),CAST($18 AS numeric),CAST($19 AS numeric),
                       CAST($20 AS numeric),
                       $21,$22,NOW())
               -- 5-minute points are immutable once published, so a conflict means we already have
               -- it. Nothing to update.
               ON CONFLICT (id) DO NOTHING"#,
        )
        .bind(id)
        .bind(int(r.get("dayNumber")).unwrap_or_default() as i32)
        .bind(int(r.get("dayStart")))
        .bind(int(r.get("dayEnd")))
        .bind(text(r.get("indexer_wallet")).unwrap_or_default().to_lowercase())
        .bind(text(r.get("indexer_url")))
        .bind(text(r.get("subgraph_deployment_ipfs_hash")).unwrap_or_default());
        let q = bind_numerics!(
            q, r,
            "avg_indexer_blocks_behind", "avg_indexer_latency_ms", "stdev_indexer_latency_ms",
            "avg_query_fee", "max_indexer_blocks_behind", "max_indexer_latency_ms", "max_query_fee",
            "num_indexer_200_responses", "proportion_indexer_200_responses",
            "query_count", "total_query_fees", "start_epoch", "end_epoch",
        );
        n += q
            .bind(text(r.get("chain_id")))
            .bind(text(r.get("gateway_id")))
            .execute(pool)
            .await?
            .rows_affected();
    }
    Ok(n)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reads_graph_node_scalars_in_both_forms() {
        // graph-node sends BigInt/BigDecimal as strings and Int as a number. Both must work, or a
        // whole column silently mirrors as null.
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
