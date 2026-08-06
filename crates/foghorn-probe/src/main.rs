use foghorn_core::{
    config::load_config,
    db::{create_pool, run_migrations},
};
use tracing::{info, warn};

mod alerter;
mod allocations;
mod autodiscover;
mod cluster;
mod dataedge;
mod discovery;
mod escrow;
mod executor;
mod ingest;
mod nest;
mod lodestar;
mod peer;
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

    // Edge & Node's oracle, watched as a peer. We no longer hold or serve a copy of it: there is no
    // canonical oracle, only two independent ones, and republishing theirs made Lodestar a
    // dependency of their pipeline for nothing. What this loop reports is whether the feed we
    // compare ourselves against is current — a subgraph at chain tip with no indexing errors can
    // still be rejecting every message, which is exactly what happened on 2026-07-01.
    if config.peer_oracle.enabled {
        let peer_cfg = config.peer_oracle.clone();
        let api_key = config.gateway.as_ref().map(|g| g.api_key.clone());
        let pool = pool.clone();
        tokio::spawn(async move { peer::run_peer_watch_loop(peer_cfg, api_key, pool).await });
    }

    // Canonical oracle publisher liveness, straight from Gnosis. No API key, no subgraph, so
    // this keeps working precisely when the oracle's own pipeline does not.
    {
        let de_cfg = config.data_edge.clone();
        let pool = pool.clone();
        tokio::spawn(async move { dataedge::run_dataedge_loop(de_cfg, pool).await });
    }

    // Paying indexers directly needs two things kept fresh: which allocation to bill, and which
    // indexers we hold escrow with. Both are read from their sources rather than assumed.
    if config.tap.enabled {
        {
            let api_key = config.gateway.as_ref().map(|g| g.api_key.clone());
            let secs = config.tap.allocation_sync_secs;
            let pool = pool.clone();
            // Our own nest over Horizon, when configured. It supplies the allocation set and the
            // indexer endpoints from Arbitrum directly, which is the last thing paid probing needs
            // from Edge & Node's gateway.
            let nest = if config.nest.enabled && !config.nest.url.is_empty() {
                let auth = match (&config.nest.username, &config.nest.password) {
                    (Some(u), Some(p)) => Some((u.clone(), p.clone())),
                    _ => None,
                };
                match nest::NestClient::new(&config.nest.url, auth) {
                    Ok(c) => {
                        info!(url = %config.nest.url, "Allocation set will be read from our own nest");
                        Some(c)
                    }
                    Err(e) => {
                        warn!(error = %e, "Nest client could not be built — staying on the gateway");
                        None
                    }
                }
            } else {
                None
            };
            // Chain tip, read from the same RPC the escrow sync uses. The nest is only trusted when
            // it is caught up, and "caught up" is meaningless without an independent tip.
            let rpc = config
                .rpc_urls
                .get("arbitrum-one")
                .cloned()
                .unwrap_or_else(|| "https://arb1.arbitrum.io/rpc".to_string());
            let tip_cache: std::sync::Arc<std::sync::atomic::AtomicU64> =
                std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0));
            {
                let tip_cache = tip_cache.clone();
                let rpc = rpc.clone();
                tokio::spawn(async move { allocations::run_chain_tip_loop(rpc, tip_cache).await });
            }
            let tip_reader = {
                let tip_cache = tip_cache.clone();
                std::sync::Arc::new(move || {
                    match tip_cache.load(std::sync::atomic::Ordering::Relaxed) {
                        0 => None,
                        n => Some(n),
                    }
                }) as std::sync::Arc<dyn Fn() -> Option<u64> + Send + Sync>
            };
            tokio::spawn(async move {
                allocations::run_allocation_sync_loop(api_key, secs, nest, tip_reader, pool).await
            });
        }
        {
            let tap = config.tap.clone();
            // Arbitrum One, where escrow lives. Falls back to the public endpoint.
            let rpc = config
                .rpc_urls
                .get("arbitrum-one")
                .cloned()
                .unwrap_or_else(|| "https://arb1.arbitrum.io/rpc".to_string());
            let pool = pool.clone();
            tokio::spawn(async move { escrow::run_escrow_sync_loop(tap, rpc, pool).await });
        }
    } else {
        info!("TAP disabled — probes will keep dispatching through the gateway, so success rate stays an upper bound");
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
