//! GraphQL compatibility with the Gateway QoS Oracle subgraph.
//!
//! The point of this module is that an existing consumer changes a URL and nothing else. It
//! mirrors the entity and field names of the reference oracle subgraph
//! (`juanmardefago/gateway-qos-oracle-example-subgraph`), and it exists because the two
//! consumers that matter query in incompatible shapes:
//!
//!   * **indexer-tools-v3** asks nested — `indexer(id: $addr) { allocationDailyDataPoints(
//!     first: 1000, where: { dayNumber: $day }) { … } }` — and separately calls
//!     `queryDailyDataPoints(orderBy: dayNumber, first: 1, orderDirection: desc)` purely to
//!     discover which day is the latest.
//!   * **Foghorn's own `ingest.rs`** asks top-level and paginates by keyset:
//!     `allocationDailyDataPoints(first: 1000, orderBy: id, orderDirection: asc,
//!     where: { dayNumber_gte, query_count_gte, id_gt })`.
//!
//! Supporting only one shape would have made the compatibility claim false for half the
//! audience, so both are served.
//!
//! ## Why the numbers are strings
//!
//! graph-node serialises `BigInt` and `BigDecimal` as JSON **strings**, not numbers. Foghorn's
//! own ingest proves it: every field is read with `as_str().and_then(parse)` before falling back
//! to a numeric read. Returning JSON numbers here would look equivalent in a browser and break
//! any consumer that trusts the documented scalar types, so `BigInt`/`BigDecimal` fields are
//! `String` and only `dayNumber` (a genuine `Int!` in their schema) is a number.
//!
//! ## What is deliberately absent
//!
//! Fee fields (`avg_query_fee`, `max_query_fee`, `total_query_fees`) resolve to null until
//! probes are TAP-paid, and `OracleMessage`/`MessageDataPoint` do not exist here at all: those
//! describe on-chain publication, which this feed does not do yet. Null is the honest answer;
//! zero would assert that queries were free.

use async_graphql::{Context, EmptyMutation, EmptySubscription, Enum, InputObject, Object, Schema};
use sqlx::{postgres::PgRow, PgPool, Row};

use crate::qos::{daily_points, DailyFilter, DailyOrder};

pub type QosSchema = Schema<QueryRoot, EmptyMutation, EmptySubscription>;

pub fn schema(pool: PgPool) -> QosSchema {
    Schema::build(QueryRoot, EmptyMutation, EmptySubscription)
        .data(pool)
        .finish()
}

/// graph-node's `OrderDirection`, whose values are lowercase.
///
/// The variant names must be pinned explicitly: async-graphql renders Rust variants as
/// SCREAMING_SNAKE by default, which would reject the `orderDirection: asc` every real consumer
/// sends.
#[derive(Enum, Copy, Clone, Eq, PartialEq)]
pub enum OrderDirection {
    #[graphql(name = "asc")]
    Asc,
    #[graphql(name = "desc")]
    Desc,
}

/// graph-node exposes `orderBy` as a generated *enum* of field names, not as a String, so
/// consumers write `orderBy: id` unquoted. Accepting a String here made every real query fail
/// validation with "expected type String".
///
/// Values beyond those actually sortable are still accepted and fall back to day ordering rather
/// than erroring — see `order_of`.
#[derive(Enum, Copy, Clone, Eq, PartialEq)]
#[graphql(name = "AllocationDailyDataPoint_orderBy")]
pub enum AllocationOrderBy {
    #[graphql(name = "id")]
    Id,
    #[graphql(name = "dayNumber")]
    DayNumber,
    #[graphql(name = "dayStart")]
    DayStart,
    #[graphql(name = "dayEnd")]
    DayEnd,
    #[graphql(name = "query_count")]
    QueryCount,
    #[graphql(name = "proportion_indexer_200_responses")]
    ProportionIndexer200Responses,
    #[graphql(name = "avg_indexer_latency_ms")]
    AvgIndexerLatencyMs,
    #[graphql(name = "avg_indexer_blocks_behind")]
    AvgIndexerBlocksBehind,
}

/// The subset of the oracle's `where` arguments that consumers actually send.
///
/// Field names keep their snake_case exactly as the subgraph exposes them — renaming them to
/// Rust conventions would break the drop-in property this module exists to provide.
#[derive(InputObject, Default)]
#[allow(non_snake_case)]
pub struct AllocationDailyDataPointFilter {
    pub dayNumber: Option<i32>,
    #[graphql(name = "dayNumber_gte")]
    pub dayNumber_gte: Option<i32>,
    #[graphql(name = "dayNumber_lte")]
    pub dayNumber_lte: Option<i32>,
    /// Sent as a string by the oracle's consumers because it filters a BigDecimal.
    #[graphql(name = "query_count_gte")]
    pub query_count_gte: Option<String>,
    #[graphql(name = "id_gt")]
    pub id_gt: Option<String>,
    #[graphql(name = "indexer_wallet")]
    pub indexer_wallet: Option<String>,
    #[graphql(name = "subgraph_deployment_ipfs_hash")]
    pub subgraph_deployment_ipfs_hash: Option<String>,
}

impl AllocationDailyDataPointFilter {
    fn apply(self, f: &mut DailyFilter) {
        f.day_eq = self.dayNumber;
        f.day_gte = self.dayNumber_gte;
        f.day_lte = self.dayNumber_lte;
        // A malformed numeric string is ignored rather than fatal: the oracle would simply have
        // matched nothing useful, and a 500 here would look like our feed was down.
        f.query_count_gte = self.query_count_gte.and_then(|s| s.parse().ok());
        f.id_gt = self.id_gt;
        if let Some(w) = self.indexer_wallet {
            f.indexer = Some(w.to_lowercase());
        }
        if let Some(d) = self.subgraph_deployment_ipfs_hash {
            f.deployment = Some(d);
        }
    }
}

fn order_of(order_by: Option<AllocationOrderBy>, dir: Option<OrderDirection>) -> DailyOrder {
    let desc = matches!(dir, Some(OrderDirection::Desc));
    match order_by {
        Some(AllocationOrderBy::Id) if desc => DailyOrder::IdDesc,
        Some(AllocationOrderBy::Id) => DailyOrder::IdAsc,
        Some(AllocationOrderBy::QueryCount) if desc => DailyOrder::QueryCountDesc,
        Some(AllocationOrderBy::QueryCount) => DailyOrder::QueryCountAsc,
        // Anything else falls back to day ordering rather than erroring: an unsupported sort is
        // better answered with sensible data than with a failure a consumer cannot work around.
        _ if desc => DailyOrder::DayDesc,
        _ => DailyOrder::DayAsc,
    }
}

// ── Entities ────────────────────────────────────────────────────────────────

pub struct AllocationDailyDataPoint(PgRow);

#[Object]
#[allow(non_snake_case)]
impl AllocationDailyDataPoint {
    async fn id(&self) -> String {
        self.0.get("id")
    }
    async fn dayNumber(&self) -> i32 {
        self.0.get("day_number")
    }
    async fn dayStart(&self) -> String {
        self.0.get::<i64, _>("day_start").to_string()
    }
    async fn dayEnd(&self) -> String {
        self.0.get::<i64, _>("day_end").to_string()
    }
    async fn dataPointCount(&self) -> String {
        self.0.get::<i64, _>("data_point_count").to_string()
    }

    #[graphql(name = "indexer_wallet")]
    async fn indexer_wallet(&self) -> String {
        self.0.get("indexer_address")
    }
    #[graphql(name = "indexer_url")]
    async fn indexer_url(&self) -> String {
        // Non-null in their schema, so an unresolved URL is an empty string rather than null.
        self.0
            .get::<Option<String>, _>("indexer_url")
            .unwrap_or_default()
    }
    #[graphql(name = "subgraph_deployment_ipfs_hash")]
    async fn subgraph_deployment_ipfs_hash(&self) -> String {
        self.0.get("deployment_id")
    }
    #[graphql(name = "chain_id")]
    async fn chain_id(&self) -> Option<String> {
        self.0.get("chain_id")
    }
    #[graphql(name = "gateway_id")]
    async fn gateway_id(&self) -> Option<String> {
        self.0.get("gateway_id")
    }

    #[graphql(name = "query_count")]
    async fn query_count(&self) -> String {
        big(self.0.get::<Option<i64>, _>("query_count").map(|v| v as f64))
    }
    #[graphql(name = "num_indexer_200_responses")]
    async fn num_indexer_200_responses(&self) -> String {
        big(self
            .0
            .get::<Option<i64>, _>("num_indexer_200_responses")
            .map(|v| v as f64))
    }
    #[graphql(name = "proportion_indexer_200_responses")]
    async fn proportion_indexer_200_responses(&self) -> String {
        big(self.0.get("proportion_indexer_200_responses"))
    }
    #[graphql(name = "avg_indexer_latency_ms")]
    async fn avg_indexer_latency_ms(&self) -> String {
        big(self.0.get("avg_indexer_latency_ms"))
    }
    #[graphql(name = "max_indexer_latency_ms")]
    async fn max_indexer_latency_ms(&self) -> String {
        big(self.0.get("max_indexer_latency_ms"))
    }
    #[graphql(name = "avg_indexer_blocks_behind")]
    async fn avg_indexer_blocks_behind(&self) -> String {
        big(self.0.get("avg_indexer_blocks_behind"))
    }
    #[graphql(name = "max_indexer_blocks_behind")]
    async fn max_indexer_blocks_behind(&self) -> String {
        big(self.0.get("max_indexer_blocks_behind"))
    }

    // Fees: null until probes are TAP-paid. See the module note.
    #[graphql(name = "avg_query_fee")]
    async fn avg_query_fee(&self) -> Option<String> {
        None
    }
    #[graphql(name = "max_query_fee")]
    async fn max_query_fee(&self) -> Option<String> {
        None
    }
    #[graphql(name = "total_query_fees")]
    async fn total_query_fees(&self) -> Option<String> {
        None
    }

    // ── Foghorn additions ──
    /// Responses that could be compared against a stake-weighted majority cluster.
    #[graphql(name = "comparable_count")]
    async fn comparable_count(&self) -> Option<String> {
        self.0
            .get::<Option<i64>, _>("comparable_count")
            .map(|v| v.to_string())
    }
    /// Responses that disagreed with that majority — confident garbage.
    #[graphql(name = "divergent_count")]
    async fn divergent_count(&self) -> Option<String> {
        self.0
            .get::<Option<i64>, _>("divergent_count")
            .map(|v| v.to_string())
    }
    /// Null when nothing was comparable, so "not checked" never reads as "verified correct".
    #[graphql(name = "correctness_rate")]
    async fn correctness_rate(&self) -> Option<String> {
        self.0
            .get::<Option<f64>, _>("correctness_rate")
            .map(|v| v.to_string())
    }

    async fn subgraphDeployment(&self) -> SubgraphDeployment {
        SubgraphDeployment {
            id: self.0.get("deployment_id"),
        }
    }
    async fn indexer(&self) -> Indexer {
        Indexer {
            id: self.0.get("indexer_address"),
        }
    }
}

/// BigDecimal-as-string, with null rendered as "0" because the oracle's equivalents are non-null.
fn big(v: Option<f64>) -> String {
    v.unwrap_or(0.0).to_string()
}

pub struct SubgraphDeployment {
    id: String,
}

#[Object]
impl SubgraphDeployment {
    async fn id(&self) -> &str {
        &self.id
    }

    #[allow(non_snake_case)]
    async fn allocationDailyDataPoints(
        &self,
        ctx: &Context<'_>,
        first: Option<i32>,
        skip: Option<i32>,
        #[graphql(name = "orderBy")] order_by: Option<AllocationOrderBy>,
        #[graphql(name = "orderDirection")] order_direction: Option<OrderDirection>,
        r#where: Option<AllocationDailyDataPointFilter>,
    ) -> async_graphql::Result<Vec<AllocationDailyDataPoint>> {
        let mut f = DailyFilter::for_deployment(&self.id);
        if let Some(w) = r#where {
            w.apply(&mut f);
        }
        // The parent deployment always wins over a `where` clause naming a different one: a
        // nested query means "this deployment's points", and silently widening it would be wrong.
        f.deployment = Some(self.id.clone());
        f.order = order_of(order_by, order_direction);
        f.first_skip(first, skip);
        fetch(ctx, f).await
    }
}

pub struct Indexer {
    id: String,
}

#[Object]
impl Indexer {
    async fn id(&self) -> &str {
        &self.id
    }

    /// The shape `indexer-tools-v3` actually sends.
    #[allow(non_snake_case)]
    async fn allocationDailyDataPoints(
        &self,
        ctx: &Context<'_>,
        first: Option<i32>,
        skip: Option<i32>,
        #[graphql(name = "orderBy")] order_by: Option<AllocationOrderBy>,
        #[graphql(name = "orderDirection")] order_direction: Option<OrderDirection>,
        r#where: Option<AllocationDailyDataPointFilter>,
    ) -> async_graphql::Result<Vec<AllocationDailyDataPoint>> {
        let mut f = DailyFilter::for_indexer(&self.id);
        if let Some(w) = r#where {
            w.apply(&mut f);
        }
        f.indexer = Some(self.id.clone());
        f.order = order_of(order_by, order_direction);
        f.first_skip(first, skip);
        fetch(ctx, f).await
    }
}

/// The gateway-wide per-deployment daily entity.
///
/// Present because `indexer-tools-v3` queries it solely to learn the latest `dayNumber` before
/// asking for real data. The fields it cannot honestly fill — gateway-side latency and
/// user-attributed error rate, which describe a gateway's own behaviour rather than an
/// indexer's — resolve to null rather than to invented numbers.
pub struct QueryDailyDataPoint(PgRow);

#[Object]
#[allow(non_snake_case)]
impl QueryDailyDataPoint {
    async fn id(&self) -> String {
        format!(
            "{}-{}",
            self.0.get::<String, _>("deployment_id"),
            self.0.get::<i32, _>("day_number")
        )
    }
    async fn dayNumber(&self) -> i32 {
        self.0.get("day_number")
    }
    async fn dayStart(&self) -> String {
        self.0.get::<i64, _>("day_start").to_string()
    }
    async fn dayEnd(&self) -> String {
        self.0.get::<i64, _>("day_end").to_string()
    }
    #[graphql(name = "query_count")]
    async fn query_count(&self) -> String {
        big(self.0.get::<Option<i64>, _>("query_count").map(|v| v as f64))
    }
    #[graphql(name = "subgraph_deployment_ipfs_hash")]
    async fn subgraph_deployment_ipfs_hash(&self) -> String {
        self.0.get("deployment_id")
    }
    #[graphql(name = "chain_id")]
    async fn chain_id(&self) -> Option<String> {
        self.0.get("chain_id")
    }
    #[graphql(name = "gateway_id")]
    async fn gateway_id(&self) -> Option<String> {
        self.0.get("gateway_id")
    }
    async fn subgraphDeployment(&self) -> SubgraphDeployment {
        SubgraphDeployment {
            id: self.0.get("deployment_id"),
        }
    }

    #[graphql(name = "avg_gateway_latency_ms")]
    async fn avg_gateway_latency_ms(&self) -> Option<String> {
        None
    }
    #[graphql(name = "gateway_query_success_rate")]
    async fn gateway_query_success_rate(&self) -> Option<String> {
        None
    }
    #[graphql(name = "user_attributed_error_rate")]
    async fn user_attributed_error_rate(&self) -> Option<String> {
        None
    }
}

// ── Root ────────────────────────────────────────────────────────────────────

pub struct QueryRoot;

#[Object]
#[allow(non_snake_case)]
impl QueryRoot {
    /// Top-level, keyset-paginated — the shape Foghorn's own `ingest.rs` sends.
    async fn allocationDailyDataPoints(
        &self,
        ctx: &Context<'_>,
        first: Option<i32>,
        skip: Option<i32>,
        #[graphql(name = "orderBy")] order_by: Option<AllocationOrderBy>,
        #[graphql(name = "orderDirection")] order_direction: Option<OrderDirection>,
        r#where: Option<AllocationDailyDataPointFilter>,
    ) -> async_graphql::Result<Vec<AllocationDailyDataPoint>> {
        let mut f = DailyFilter::default();
        if let Some(w) = r#where {
            w.apply(&mut f);
        }
        f.order = order_of(order_by, order_direction);
        f.first_skip(first, skip);
        fetch(ctx, f).await
    }

    async fn queryDailyDataPoints(
        &self,
        ctx: &Context<'_>,
        first: Option<i32>,
        skip: Option<i32>,
        #[graphql(name = "orderBy")] order_by: Option<AllocationOrderBy>,
        #[graphql(name = "orderDirection")] order_direction: Option<OrderDirection>,
        r#where: Option<AllocationDailyDataPointFilter>,
    ) -> async_graphql::Result<Vec<QueryDailyDataPoint>> {
        let mut f = DailyFilter::default();
        if let Some(w) = r#where {
            w.apply(&mut f);
        }
        f.order = order_of(order_by, order_direction);
        f.first_skip(first, skip);
        let pool = ctx.data::<PgPool>()?;
        Ok(daily_points(pool, &f)
            .await
            .map_err(|e| async_graphql::Error::new(e.to_string()))?
            .into_iter()
            .map(QueryDailyDataPoint)
            .collect())
    }

    async fn indexer(&self, id: String) -> Indexer {
        Indexer {
            id: id.to_lowercase(),
        }
    }

    async fn subgraphDeployment(&self, id: String) -> SubgraphDeployment {
        SubgraphDeployment { id }
    }

    /// Not in the oracle's schema. Added so a consumer can ask how current this feed is without
    /// a second HTTP call — the question a stale subgraph silently answers wrong.
    #[graphql(name = "_foghornStatus")]
    async fn _foghornStatus(&self, ctx: &Context<'_>) -> async_graphql::Result<FoghornStatus> {
        let pool = ctx.data::<PgPool>()?;
        let (bucket, computed): (
            Option<chrono::DateTime<chrono::Utc>>,
            Option<chrono::DateTime<chrono::Utc>>,
        ) = sqlx::query_as("SELECT max(bucket_start), max(computed_at) FROM foghorn_qos")
            .fetch_one(pool)
            .await
            .map_err(|e| async_graphql::Error::new(e.to_string()))?;
        Ok(FoghornStatus { bucket, computed })
    }
}

pub struct FoghornStatus {
    bucket: Option<chrono::DateTime<chrono::Utc>>,
    computed: Option<chrono::DateTime<chrono::Utc>>,
}

#[Object]
impl FoghornStatus {
    #[graphql(name = "gateway_id")]
    async fn gateway_id(&self) -> &str {
        "lodestar"
    }
    #[graphql(name = "last_bucket")]
    async fn last_bucket(&self) -> Option<String> {
        self.bucket.map(|t| t.to_rfc3339())
    }
    #[graphql(name = "last_computed")]
    async fn last_computed(&self) -> Option<String> {
        self.computed.map(|t| t.to_rfc3339())
    }
    #[graphql(name = "age_seconds")]
    async fn age_seconds(&self) -> Option<i64> {
        self.bucket.map(|t| (chrono::Utc::now() - t).num_seconds())
    }
    /// The caveat, in-band. `query_count` counts probes Foghorn dispatched, not organic traffic.
    #[graphql(name = "query_count_means")]
    async fn query_count_means(&self) -> &str {
        "probes dispatched by Foghorn, NOT organic gateway traffic"
    }
}

async fn fetch(
    ctx: &Context<'_>,
    f: DailyFilter,
) -> async_graphql::Result<Vec<AllocationDailyDataPoint>> {
    let pool = ctx.data::<PgPool>()?;
    Ok(daily_points(pool, &f)
        .await
        .map_err(|e| async_graphql::Error::new(e.to_string()))?
        .into_iter()
        .map(AllocationDailyDataPoint)
        .collect())
}
