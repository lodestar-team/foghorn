//! QoS Foghorn measured itself.
//!
//! [`ingest`](crate::ingest) pulls QoS from Edge & Node's oracle, which means every QoS
//! number Foghorn serves is downstream of a ten-link private pipeline. On 2026-07-29 one
//! link died mid-bucket and the feed went dark for 35+ hours while Foghorn kept quoting the
//! stale figures. This loop derives the same information from observations Foghorn has
//! already collected and stored, so the surface stays live when theirs does not.
//!
//! It is a *rollup*, not a probe: it adds no network traffic and no new dependency. Every
//! run is a full recompute of the trailing window, upserted by primary key, so it is
//! idempotent — a restart mid-window, a double-run, or a late-arriving observation all
//! converge to the same rows. That matters more than efficiency here: the current bucket is
//! partial by definition and gets rewritten on each pass until the window moves past it.
//!
//! ## What this is not
//!
//! Not a census of real query traffic. The oracle counts what the gateway actually routed;
//! this counts what Foghorn chose to probe. `probe_count` is therefore a statement about
//! Foghorn's cadence, never about an indexer's popularity, and anything served from this
//! table has to say so — see the `source` field on the API responses.

use anyhow::Result;
use foghorn_core::config::QosRollupConfig;
use sqlx::PgPool;
use std::time::Duration;
use tracing::{info, warn};

pub async fn run_qos_rollup_loop(cfg: QosRollupConfig, pool: PgPool) {
    if !cfg.enabled {
        info!("QoS rollup disabled by config");
        return;
    }
    info!(
        bucket_secs = cfg.bucket_secs,
        lookback_secs = cfg.lookback_secs,
        interval = cfg.interval_secs,
        "QoS rollup loop starting"
    );
    loop {
        match rollup_once(&cfg, &pool).await {
            Ok(n) => info!(buckets = n, "QoS rollup complete"),
            Err(e) => warn!(error = %e, "QoS rollup failed"),
        }
        tokio::time::sleep(Duration::from_secs(cfg.interval_secs)).await;
    }
}

/// Recompute every (indexer, deployment, bucket) touched by the trailing window.
///
/// Latency percentiles come from *successful* probes only. Including errors would let a
/// fast 500 flatter an indexer, and the failure is already counted in `success_rate` —
/// counting it twice, once as a failure and once as excellent latency, would be perverse.
///
/// `correctness_rate` is NULL rather than 1.0 when nothing in the bucket was comparable, so
/// "we did not check" can never be read as "verified correct". That distinction is the whole
/// reason Foghorn's correctness signal is worth anything.
pub async fn rollup_once(cfg: &QosRollupConfig, pool: &PgPool) -> Result<u64> {
    let bucket = cfg.bucket_secs as f64;
    let lookback = cfg.lookback_secs as f64;

    let result = sqlx::query(
        r#"
        WITH obs AS (
            SELECT
                o.indexer_address,
                p.deployment_id,
                to_timestamp(floor(extract(epoch FROM p.dispatched_at) / $1) * $1) AS bucket_start,
                o.latency_ms,
                (o.error_class IS NULL AND o.http_status = 200) AS ok,
                o.response_hash,
                d.largest_by_stake_hash
            FROM observation o
            JOIN probe p ON p.id = o.probe_id
            LEFT JOIN divergence d ON d.probe_id = o.probe_id
            WHERE p.dispatched_at >= NOW() - make_interval(secs => $2)
        ),
        agg AS (
            SELECT
                indexer_address,
                deployment_id,
                bucket_start,
                count(*)                                     AS query_count,
                count(*) FILTER (WHERE ok)                   AS num_200,
                avg(latency_ms::double precision)
                    FILTER (WHERE ok)                        AS avg_latency,
                max(latency_ms::double precision)
                    FILTER (WHERE ok)                        AS max_latency,
                stddev_samp(latency_ms::double precision)
                    FILTER (WHERE ok)                        AS stdev_latency,
                percentile_cont(0.50) WITHIN GROUP (ORDER BY latency_ms)
                    FILTER (WHERE ok AND latency_ms IS NOT NULL) AS p50,
                percentile_cont(0.95) WITHIN GROUP (ORDER BY latency_ms)
                    FILTER (WHERE ok AND latency_ms IS NOT NULL) AS p95,
                percentile_cont(0.99) WITHIN GROUP (ORDER BY latency_ms)
                    FILTER (WHERE ok AND latency_ms IS NOT NULL) AS p99,
                count(*) FILTER (
                    WHERE response_hash IS NOT NULL AND largest_by_stake_hash IS NOT NULL
                )                                            AS comparable_count,
                count(*) FILTER (
                    WHERE response_hash IS NOT NULL
                      AND largest_by_stake_hash IS NOT NULL
                      AND response_hash <> largest_by_stake_hash
                )                                            AS divergent_count
            FROM obs
            GROUP BY 1, 2, 3
        ),
        fresh AS (
            SELECT
                indexer_address,
                deployment_id,
                to_timestamp(floor(extract(epoch FROM sampled_at) / $1) * $1) AS bucket_start,
                avg(chainhead_lag_blocks::double precision) AS avg_blocks_behind,
                max(chainhead_lag_blocks::double precision) AS max_blocks_behind
            FROM freshness_sample
            WHERE sampled_at >= NOW() - make_interval(secs => $2)
            GROUP BY 1, 2, 3
        ),
        -- allocation_map is keyed per allocation, so an indexer appears once per allocation.
        -- Any of its URLs identifies the same operator; collapse to one to keep the join 1:1.
        urls AS (
            SELECT indexer_address, max(indexer_url) AS indexer_url
            FROM allocation_map
            WHERE indexer_address IS NOT NULL
              AND indexer_url IS NOT NULL
              AND indexer_url <> ''
            GROUP BY 1
        )
        INSERT INTO foghorn_qos (
            indexer_address, deployment_id, bucket_start, bucket_secs,
            indexer_url, chain_id, gateway_id,
            query_count, num_indexer_200_responses, proportion_indexer_200_responses,
            avg_indexer_latency_ms, max_indexer_latency_ms, stdev_indexer_latency_ms,
            latency_p50_ms, latency_p95_ms, latency_p99_ms,
            avg_indexer_blocks_behind, max_indexer_blocks_behind,
            comparable_count, divergent_count, correctness_rate,
            computed_at
        )
        SELECT
            a.indexer_address,
            a.deployment_id,
            a.bucket_start,
            $3,
            u.indexer_url,
            $4,
            $5,
            a.query_count,
            a.num_200,
            a.num_200::double precision / a.query_count::double precision,
            a.avg_latency,
            a.max_latency,
            a.stdev_latency,
            a.p50::int,
            a.p95::int,
            a.p99::int,
            f.avg_blocks_behind,
            f.max_blocks_behind,
            a.comparable_count,
            a.divergent_count,
            CASE WHEN a.comparable_count > 0
                 THEN 1.0 - (a.divergent_count::double precision
                             / a.comparable_count::double precision)
                 ELSE NULL
            END,
            NOW()
        FROM agg a
        LEFT JOIN fresh f
               ON f.indexer_address = a.indexer_address
              AND f.deployment_id   = a.deployment_id
              AND f.bucket_start    = a.bucket_start
        LEFT JOIN urls u ON u.indexer_address = a.indexer_address
        ON CONFLICT (indexer_address, deployment_id, bucket_start, bucket_secs) DO UPDATE SET
            indexer_url                      = EXCLUDED.indexer_url,
            chain_id                         = EXCLUDED.chain_id,
            gateway_id                       = EXCLUDED.gateway_id,
            query_count                      = EXCLUDED.query_count,
            num_indexer_200_responses        = EXCLUDED.num_indexer_200_responses,
            proportion_indexer_200_responses = EXCLUDED.proportion_indexer_200_responses,
            avg_indexer_latency_ms           = EXCLUDED.avg_indexer_latency_ms,
            max_indexer_latency_ms           = EXCLUDED.max_indexer_latency_ms,
            stdev_indexer_latency_ms         = EXCLUDED.stdev_indexer_latency_ms,
            latency_p50_ms                   = EXCLUDED.latency_p50_ms,
            latency_p95_ms                   = EXCLUDED.latency_p95_ms,
            latency_p99_ms                   = EXCLUDED.latency_p99_ms,
            avg_indexer_blocks_behind        = EXCLUDED.avg_indexer_blocks_behind,
            max_indexer_blocks_behind        = EXCLUDED.max_indexer_blocks_behind,
            comparable_count                 = EXCLUDED.comparable_count,
            divergent_count                  = EXCLUDED.divergent_count,
            correctness_rate                 = EXCLUDED.correctness_rate,
            computed_at                      = EXCLUDED.computed_at
        "#,
    )
    .bind(bucket)
    .bind(lookback)
    .bind(cfg.bucket_secs as i32)
    .bind(&cfg.chain_id)
    .bind(&cfg.gateway_id)
    .execute(pool)
    .await?;

    Ok(result.rows_affected())
}
