use axum::{routing::get, Router};
use foghorn_core::{
    config::load_config,
    db::{create_pool, run_migrations},
};
use tower_http::cors::{Any, CorsLayer};
use tracing::info;

mod graphql;
mod qos;
mod routes;

#[derive(Clone)]
pub struct AppState {
    pub pool: sqlx::PgPool,
    /// How often the probe scheduler runs, from the same config the probe binary reads.
    ///
    /// Staleness thresholds are derived from this rather than hardcoded. The page previously
    /// declared the feed "lagging" after 15 minutes while the box was configured to probe hourly,
    /// so it condemned its own feed for ~45 minutes of every hour. A cadence-relative threshold
    /// cannot drift out of step with the deployment the way a guessed constant does.
    pub probe_interval_secs: u64,
    /// The oracle-compatible GraphQL schema. Held in state rather than built per request because
    /// schema construction walks every resolver.
    pub schema: graphql::QosSchema,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::from_default_env()
                .add_directive("foghorn_api=info".parse()?)
                .add_directive("tower_http=info".parse()?),
        )
        .init();

    info!("Foghorn API starting");

    let config = load_config()?;
    let pool = create_pool(&config.database_url).await?;
    run_migrations(&pool).await?;

    let state = AppState {
        schema: graphql::schema(pool.clone()),
        probe_interval_secs: config.probe_interval_secs,
        pool,
    };

    let cors = CorsLayer::new()
        .allow_origin(Any)
        .allow_methods(Any)
        .allow_headers(Any);

    let app = Router::new()
        .route("/v1/health", get(routes::health))
        .route("/v1/stats", get(routes::stats))
        .route("/v1/feed", get(routes::feed))
        .route("/v1/probe/:probe_id", get(routes::probe_detail))
        .route("/v1/indexer/:address/quality", get(routes::indexer_quality))
        .route("/v1/indexer/:address/freshness", get(routes::indexer_freshness))
        .route("/v1/deployments", get(routes::deployments))
        .route("/v1/deployment/:deployment_id/quality", get(routes::deployment_quality))
        // ── Judgement layer ──
        .route("/v1/indexers", get(routes::indexers))
        .route("/v1/indexer/:address/scorecard", get(routes::indexer_scorecard))
        .route("/v1/needs-attention", get(routes::needs_attention))
        .route("/v1/verdicts", get(routes::verdicts))
        .route("/v1/sybil", get(routes::sybil_clusters))
        .route("/v1/nondeterministic", get(routes::nondeterministic))
        .route("/v1/deployment/:deployment_id/qos", get(routes::deployment_qos))
        .route("/v1/indexer/:address/allocations-qos", get(routes::indexer_allocations_qos))
        // ── Foghorn QoS: measured here, in the oracle's shape ──
        .route("/v1/qos/status", get(routes::qos_status))
        .route("/v1/qos/buckets", get(routes::qos_buckets))
        .route("/v1/qos/compare", get(routes::qos_compare))
        .route("/v1/qos/canonical", get(routes::qos_canonical))
        // Oracle-compatible GraphQL. POST is the endpoint a consumer repoints at us; GET serves
        // a playground so "does this really answer my existing query?" is answerable in a browser
        // before anyone edits a config.
        .route(
            "/v1/qos/graphql",
            get(routes::graphql_playground).post(routes::graphql_handler),
        )
        .layer(cors)
        .with_state(state);

    let addr = format!("{}:{}", config.api_host, config.api_port);
    info!("Listening on {}", addr);
    let listener = tokio::net::TcpListener::bind(&addr).await?;
    axum::serve(listener, app).await?;

    Ok(())
}
