//! Serving Foghorn's own QoS in the oracle's shape.
//!
//! `foghorn_qos` stores 5-minute buckets (matching the oracle's cadence). Consumers, however,
//! query the oracle's *daily* entity, `AllocationDailyDataPoint`. This module is the single
//! place that rolls buckets up into that shape, so the REST routes and the GraphQL compat
//! layer cannot drift apart — two implementations of one aggregation is how a QoS feed ends up
//! disagreeing with itself, which is worse than being briefly absent.
//!
//! ## Why there are no percentiles on the daily rollup
//!
//! Percentiles do not recombine. The p95 of a day is not the max, mean, or any other function
//! of twelve bucket p95s, and pretending otherwise would publish a confident wrong number.
//! Percentiles are therefore exposed only at bucket resolution, where they were actually
//! computed. Everything the oracle itself publishes (avg, max) recombines correctly and is
//! present here.
//!
//! ## Weighting
//!
//! `avg_indexer_latency_ms` is a mean over *successful* probes, so rolling it up weights each
//! bucket by `num_indexer_200_responses` rather than by `query_count`. Weighting by the latter
//! would let a bucket that was mostly failures drag the latency of a bucket that mostly
//! succeeded. `avg_indexer_blocks_behind` is an unweighted mean of bucket means, because
//! freshness is sampled on its own schedule rather than per probe.

use serde_json::{json, Value};
use sqlx::{postgres::PgRow, PgPool, Row};

/// Sort orders the compat layer accepts.
///
/// A closed enum rather than a caller-supplied string: the ORDER BY clause is interpolated into
/// SQL, so the set of legal values has to be fixed at compile time. Both variants exist because
/// real consumers use both — `indexer-tools-v3` sorts by day to find the latest one, and
/// Foghorn's own ingest paginates by id.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DailyOrder {
    IdAsc,
    IdDesc,
    DayAsc,
    DayDesc,
    QueryCountAsc,
    QueryCountDesc,
}

impl DailyOrder {
    /// Fixed SQL fragments. Never interpolate anything caller-controlled here.
    fn clause(self) -> &'static str {
        match self {
            Self::IdAsc => "id ASC",
            Self::IdDesc => "id DESC",
            Self::DayAsc => "day_number ASC, query_count DESC",
            Self::DayDesc => "day_number DESC, query_count DESC",
            Self::QueryCountAsc => "query_count ASC, id ASC",
            Self::QueryCountDesc => "query_count DESC, id ASC",
        }
    }
}

/// Optional predicates for the daily rollup, covering every filter the oracle's consumers
/// actually send. All-None means "everything we have", which `limit` bounds.
#[derive(Debug, Clone)]
pub struct DailyFilter {
    pub indexer: Option<String>,
    pub deployment: Option<String>,
    pub day_eq: Option<i32>,
    pub day_gte: Option<i32>,
    pub day_lte: Option<i32>,
    /// The oracle's `query_count_gte`, used to drop near-zero-traffic allocations.
    pub query_count_gte: Option<i64>,
    /// The oracle's `id_gt` keyset pagination.
    pub id_gt: Option<String>,
    pub order: DailyOrder,
    pub skip: i64,
    pub limit: i64,
}

impl Default for DailyFilter {
    fn default() -> Self {
        Self {
            indexer: None,
            deployment: None,
            day_eq: None,
            day_gte: None,
            day_lte: None,
            query_count_gte: None,
            id_gt: None,
            order: DailyOrder::DayDesc,
            skip: 0,
            limit: 1000,
        }
    }
}

impl DailyFilter {
    pub fn for_indexer(address: &str) -> Self {
        Self {
            indexer: Some(address.to_lowercase()),
            ..Default::default()
        }
    }

    pub fn for_deployment(deployment_id: &str) -> Self {
        Self {
            deployment: Some(deployment_id.to_string()),
            ..Default::default()
        }
    }

    /// Apply GraphQL `first`/`skip`, clamped as graph-node clamps them.
    ///
    /// The 1000 cap matches graph-node so a consumer's pagination loop — usually written around
    /// exactly that cap — behaves identically against this endpoint.
    pub fn first_skip(&mut self, first: Option<i32>, skip: Option<i32>) {
        self.limit = first.unwrap_or(100).clamp(1, 1000) as i64;
        self.skip = skip.unwrap_or(0).max(0) as i64;
    }
}

/// One (indexer, deployment, day) rollup, carrying the oracle's field names.
///
/// `day_number` is days since the Unix epoch. The oracle's own `dayNumber` uses a different
/// epoch, which `ingest.rs` already documents; that is precisely why `day_start`/`day_end` are
/// served alongside it, so a consumer never has to guess which calendar we are on.
pub async fn daily_points(pool: &PgPool, f: &DailyFilter) -> sqlx::Result<Vec<PgRow>> {
    // ORDER BY cannot be a bind parameter. The fragment comes from a closed enum, never from
    // caller input, so this interpolation carries no injection surface.
    let sql = format!(
        r#"
        WITH b AS (
            -- day_number is derived once, in a subquery, so that day_start/day_end below are
            -- functions of a GROUPED COLUMN rather than of `bucket_start`. Computing all three
            -- inline looks tidier and Postgres rejects it: it will not infer that two separate
            -- expressions over the same ungrouped column are functionally dependent.
            SELECT
                indexer_address,
                deployment_id,
                (floor(extract(epoch FROM bucket_start) / 86400))::int AS day_number,
                indexer_url,
                gateway_id,
                chain_id,
                query_count,
                num_indexer_200_responses,
                avg_indexer_latency_ms,
                max_indexer_latency_ms,
                avg_indexer_blocks_behind,
                max_indexer_blocks_behind,
                comparable_count,
                divergent_count,
                computed_at
            FROM foghorn_qos
            WHERE ($1::text IS NULL OR indexer_address = $1)
              AND ($2::text IS NULL OR deployment_id   = $2)
        ),
        g AS (
            SELECT
                indexer_address,
                deployment_id,
                day_number,
                (day_number::bigint * 86400)         AS day_start,
                (day_number::bigint * 86400 + 86399) AS day_end,
                max(indexer_url)  AS indexer_url,
                max(gateway_id)   AS gateway_id,
                max(chain_id)     AS chain_id,

                sum(query_count)::bigint                  AS query_count,
                sum(num_indexer_200_responses)::bigint    AS num_indexer_200_responses,
                sum(num_indexer_200_responses)::double precision
                    / NULLIF(sum(query_count), 0)::double precision
                                                          AS proportion_indexer_200_responses,

                -- Weighted by successful probes; see the module note on weighting.
                sum(avg_indexer_latency_ms * num_indexer_200_responses::double precision)
                    / NULLIF(sum(num_indexer_200_responses) FILTER (
                          WHERE avg_indexer_latency_ms IS NOT NULL
                      ), 0)::double precision              AS avg_indexer_latency_ms,
                max(max_indexer_latency_ms)                AS max_indexer_latency_ms,

                avg(avg_indexer_blocks_behind)             AS avg_indexer_blocks_behind,
                max(max_indexer_blocks_behind)             AS max_indexer_blocks_behind,

                sum(comparable_count)::bigint              AS comparable_count,
                sum(divergent_count)::bigint               AS divergent_count,
                CASE WHEN sum(comparable_count) > 0
                     THEN 1.0 - (sum(divergent_count)::double precision
                                 / sum(comparable_count)::double precision)
                     ELSE NULL
                END                                        AS correctness_rate,

                count(*)::bigint                           AS data_point_count,
                max(computed_at)                           AS computed_at
            FROM b
            WHERE ($3::int IS NULL OR day_number  = $3)
              AND ($4::int IS NULL OR day_number >= $4)
              AND ($5::int IS NULL OR day_number <= $5)
            GROUP BY 1, 2, 3
        )
        SELECT
            g.*,
            -- Composite id in the oracle's convention, so a consumer keys on ours exactly as it
            -- keys on theirs. Built here rather than in Rust because `id_gt` pagination has to
            -- filter and sort on it.
            (indexer_address || '-' || deployment_id || '-' || day_number::text) AS id
        FROM g
        WHERE ($6::bigint IS NULL OR query_count >= $6)
          AND ($7::text IS NULL
               OR (indexer_address || '-' || deployment_id || '-' || day_number::text) > $7)
        ORDER BY {order}
        LIMIT $8 OFFSET $9
        "#,
        order = f.order.clause(),
    );

    sqlx::query(&sql)
        .bind(&f.indexer)
        .bind(&f.deployment)
        .bind(f.day_eq)
        .bind(f.day_gte)
        .bind(f.day_lte)
        .bind(f.query_count_gte)
        .bind(&f.id_gt)
        .bind(f.limit)
        .bind(f.skip)
        .fetch_all(pool)
        .await
}

/// Which feed a query is asking for.
///
/// Only one remains. Foghorn used to serve a mirror of Edge & Node's published rows alongside its
/// own, and `gateway_id` chose between them — with theirs as the default, because they were "the
/// canonical oracle". Neither of those things is true now: there is no canonical oracle, and
/// Lodestar does not republish anyone else's numbers. This endpoint serves the Lodestar Oracle.
///
/// The type is kept rather than deleted because `gateway_id` is still a legitimate filter — the
/// schema carries it precisely so several gateways can publish — and a consumer asking for a
/// gateway we are not is asking for nothing, which must return empty rather than quietly returning
/// ours under someone else's name.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Feed {
    /// The Lodestar Oracle's own measurements.
    Measured,
    /// A gateway_id we do not publish for. Serves nothing.
    Foreign,
}

impl Feed {
    pub fn from_gateway_id(gateway_id: Option<&str>) -> Self {
        match gateway_id {
            None => Self::Measured,
            Some(g) if g.eq_ignore_ascii_case("lodestar") || g.eq_ignore_ascii_case("foghorn") => {
                Self::Measured
            }
            Some(_) => Self::Foreign,
        }
    }
}

/// Fetch daily points from whichever feed was asked for.
pub async fn points_for(pool: &PgPool, feed: Feed, f: &DailyFilter) -> sqlx::Result<Vec<PgRow>> {
    match feed {
        Feed::Measured => daily_points(pool, f).await,
        Feed::Foreign => Ok(Vec::new()),
    }
}

/// Render a rollup row in the oracle's field names, plus Foghorn's additions.
pub fn row_to_oracle_json(r: &PgRow) -> Value {
    json!({
        "id": r.get::<String, _>("id"),
        "dayNumber": r.get::<i32, _>("day_number"),
        "dayStart": r.get::<i64, _>("day_start"),
        "dayEnd": r.get::<i64, _>("day_end"),
        "dataPointCount": r.get::<i64, _>("data_point_count"),

        "indexer_wallet": r.get::<String, _>("indexer_address"),
        "indexer_url": r.get::<Option<String>, _>("indexer_url"),
        "subgraph_deployment_ipfs_hash": r.get::<String, _>("deployment_id"),
        "gateway_id": r.get::<Option<String>, _>("gateway_id"),
        "chain_id": r.get::<Option<String>, _>("chain_id"),

        "query_count": r.get::<Option<i64>, _>("query_count"),
        "num_indexer_200_responses": r.get::<Option<i64>, _>("num_indexer_200_responses"),
        "proportion_indexer_200_responses": r.get::<Option<f64>, _>("proportion_indexer_200_responses"),
        "avg_indexer_latency_ms": r.get::<Option<f64>, _>("avg_indexer_latency_ms"),
        "max_indexer_latency_ms": r.get::<Option<f64>, _>("max_indexer_latency_ms"),
        "avg_indexer_blocks_behind": r.get::<Option<f64>, _>("avg_indexer_blocks_behind"),
        "max_indexer_blocks_behind": r.get::<Option<f64>, _>("max_indexer_blocks_behind"),

        // Foghorn additions. The oracle knows an indexer answered fast with a 200; only this
        // says the answer was right.
        "comparable_count": r.get::<Option<i64>, _>("comparable_count"),
        "divergent_count": r.get::<Option<i64>, _>("divergent_count"),
        "correctness_rate": r.get::<Option<f64>, _>("correctness_rate"),
    })
}

/// The provenance block attached to every measured payload.
///
/// Served inline rather than kept in documentation on purpose: `query_count` here counts probes
/// Foghorn chose to dispatch, not organic traffic a gateway routed. Anyone reading probe volume
/// as market demand would be badly wrong, so the correction travels with the data.
pub fn measured_provenance(gateway_id: Option<&str>) -> Value {
    json!({
        "source": "foghorn",
        "gateway_id": gateway_id.unwrap_or("lodestar"),
        "method": "active block-pinned probing, JCS-canonicalised response clustering",
        "query_count_means": "probes dispatched by Foghorn, NOT organic gateway traffic",
        "independent_of": "Edge & Node QoS oracle pipeline",
        // Stated in-band because it is the one caveat that changes how a number should be read,
        // and because the oracle comparison exposes it plainly: across 20 overlapping allocations
        // our success rate was higher than theirs every single time, never lower. That is not them
        // being wrong — probes are dispatched through E&N's gateway, which routes to indexers it
        // believes are healthy, so failures it already avoids are invisible to us.
        //
        // Removing the bias needs direct-to-indexer dispatch, which needs TAP receipts: every
        // indexer tested returns `402 No Tap receipt was found in the request`, so unpaid direct
        // probing is not possible. Until then this field is a ceiling, not a measurement.
        "success_rate_bias": "OPTIMISTIC — probes are routed by E&N's gateway, so indexers it \
                              declines to route to are never observed failing. Treat \
                              proportion_indexer_200_responses as an upper bound.",
        "unbiased_fields": "avg/max_indexer_blocks_behind (chainhead resolved independently) and \
                            correctness_rate (responses compared against each other, not reported \
                            by the indexer)",
    })
}
