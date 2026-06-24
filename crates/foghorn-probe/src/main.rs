use foghorn_core::{
    config::load_config,
    db::{create_pool, run_migrations},
};
use tracing::info;

mod autodiscover;
mod cluster;
mod discovery;
mod executor;
mod ingest;
mod lodestar;
mod resolver;
mod scheduler;
mod scorer;
mod status;
mod sybil;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::from_default_env()
                .add_directive("foghorn_probe=info".parse()?)
                .add_directive("reqwest=warn".parse()?),
        )
        .init();

    info!("Foghorn probe service starting");

    let config = load_config()?;
    let pool = create_pool(&config.database_url).await?;
    run_migrations(&pool).await?;

    info!("Database connected and migrations applied");

    // Lodestar ingest loop — roster / QoS / REO into indexer_profile.
    if let Some(lodestar) = config.lodestar.clone() {
        let pool = pool.clone();
        let api_key = config.gateway.as_ref().map(|g| g.api_key.clone());
        tokio::spawn(async move { ingest::run_ingest_loop(lodestar, api_key, pool).await });
    } else {
        info!("No [lodestar] config — roster/QoS ingest disabled");
    }

    // Direct /status health probing (unauthenticated, no TAP).
    {
        let status_cfg = config.status_probe.clone();
        let pool = pool.clone();
        tokio::spawn(async move { status::run_status_loop(status_cfg, pool).await });
    }

    // Scoring loop — grades, verdicts, attention, sybil clusters.
    {
        let scoring = config.scoring.clone();
        let api_key = config.gateway.as_ref().map(|g| g.api_key.clone());
        let pool = pool.clone();
        tokio::spawn(async move { scorer::run_score_loop(scoring, api_key, pool).await });
    }

    scheduler::run_probe_scheduler(config, pool).await?;

    Ok(())
}
