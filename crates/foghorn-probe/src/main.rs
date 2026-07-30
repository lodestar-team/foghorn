use foghorn_core::{
    config::load_config,
    db::{create_pool, run_migrations},
};
use tracing::info;

mod alerter;
mod autodiscover;
mod cluster;
mod dataedge;
mod discovery;
mod executor;
mod ingest;
mod lodestar;
mod mirror;
mod qos;
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

    // Full mirror of the canonical oracle. This is the clone: every number it ever published,
    // held by Lodestar, served in its own schema, queryable without an API key. It cannot invent
    // data for a window the publisher never produced — but the history stays served and the freeze
    // stays visible.
    {
        let mirror_cfg = config.oracle_mirror.clone();
        let api_key = config.gateway.as_ref().map(|g| g.api_key.clone());
        let pool = pool.clone();
        tokio::spawn(async move { mirror::run_mirror_loop(mirror_cfg, api_key, pool).await });
    }

    // Canonical oracle publisher liveness, straight from Gnosis. No API key, no subgraph, so
    // this keeps working precisely when the oracle's own pipeline does not.
    {
        let de_cfg = config.data_edge.clone();
        let pool = pool.clone();
        tokio::spawn(async move { dataedge::run_dataedge_loop(de_cfg, pool).await });
    }

    // QoS rollup — Foghorn's OWN observations into the oracle's schema, so the QoS surface
    // survives Edge & Node's pipeline being down. Pure SQL over stored observations: no extra
    // network traffic, no new dependency, nothing to stall.
    {
        let qos_cfg = config.qos_rollup.clone();
        let pool = pool.clone();
        tokio::spawn(async move { qos::run_qos_rollup_loop(qos_cfg, pool).await });
    }

    // Scoring loop — grades, verdicts, attention, sybil clusters.
    {
        let scoring = config.scoring.clone();
        let api_key = config.gateway.as_ref().map(|g| g.api_key.clone());
        let pool = pool.clone();
        tokio::spawn(async move { scorer::run_score_loop(scoring, api_key, pool).await });
    }

    // Discord alerting — push new critical needs-attention items to #foghorn-alerts.
    if let Some(webhook) = config.alert_webhook.clone().filter(|w| !w.is_empty()) {
        let pool = pool.clone();
        // Cloned per task: each loop owns its own handles rather than sharing one across
        // spawns, which the borrow checker will not allow anyway.
        let roster_hook = webhook.clone();
        let roster_pool = pool.clone();
        tokio::spawn(async move { alerter::run_alert_loop(roster_hook, roster_pool).await });
        // Separate loop: oracle liveness needs minutes, the roster digest needs hours.
        tokio::spawn(async move { alerter::run_oracle_watch_loop(webhook, pool).await });
    } else {
        info!("No alert_webhook configured — Discord alerting disabled");
    }

    scheduler::run_probe_scheduler(config, pool).await?;

    Ok(())
}
