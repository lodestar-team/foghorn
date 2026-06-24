use axum::{routing::get, Router};
use foghorn_core::{
    config::load_config,
    db::{create_pool, run_migrations},
};
use tower_http::cors::{Any, CorsLayer};
use tracing::info;

mod routes;

#[derive(Clone)]
pub struct AppState {
    pub pool: sqlx::PgPool,
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

    let state = AppState { pool };

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
        .layer(cors)
        .with_state(state);

    let addr = format!("{}:{}", config.api_host, config.api_port);
    info!("Listening on {}", addr);
    let listener = tokio::net::TcpListener::bind(&addr).await?;
    axum::serve(listener, app).await?;

    Ok(())
}
