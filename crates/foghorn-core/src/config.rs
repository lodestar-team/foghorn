use serde::Deserialize;
use std::collections::HashMap;

#[derive(Debug, Deserialize, Clone, Default)]
pub struct GatewayConfig {
    pub api_key: String,
    #[serde(default = "default_gateway_url")]
    pub url: String,
    #[serde(default = "default_probe_count")]
    pub probe_count: u32,
}

fn default_gateway_url() -> String {
    "https://gateway.thegraph.com/api".to_string()
}

fn default_probe_count() -> u32 {
    8
}

#[derive(Debug, Deserialize, Clone)]
pub struct LodestarConfig {
    /// Base URL of the Lodestar dashboard API, e.g. "https://www.lodestar-dashboard.com".
    pub base_url: String,
    /// Optional bearer token, if the deployment gates the API.
    #[serde(default)]
    pub api_key: Option<String>,
    /// How often to re-ingest the roster + QoS (seconds).
    #[serde(default = "default_ingest_interval")]
    pub ingest_interval_secs: u64,
}

impl Default for LodestarConfig {
    fn default() -> Self {
        Self {
            base_url: "https://www.lodestar-dashboard.com".to_string(),
            api_key: None,
            ingest_interval_secs: default_ingest_interval(),
        }
    }
}

fn default_ingest_interval() -> u64 {
    3600
}

/// Weights + thresholds for the composite grade. Tunable without a recompile so
/// the network-quality bar can be tightened over time (per the community's intent).
#[derive(Debug, Deserialize, Clone)]
#[serde(default)]
pub struct ScoringConfig {
    /// Rolling windows (days) to score over.
    pub windows: Vec<i32>,
    /// Seconds between scoring runs.
    pub interval_secs: u64,
    // Sub-score weights (need not sum to 1; normalised internally).
    pub w_correctness: f64,
    pub w_availability: f64,
    pub w_freshness: f64,
    pub w_coverage: f64,
    pub w_value: f64,
    // Grade thresholds on the 0..100 composite.
    pub grade_a: f64,
    pub grade_b: f64,
    pub grade_c: f64,
    pub grade_d: f64,
    // Thresholds for verdicts / attention.
    pub low_coverage_subgraphs: i32, // < this many query-producing subgraphs => low-coverage
    pub leech_min_stake_grt: f64,    // high stake ...
    pub leech_max_queries: i64,      // ... but <= this many queries => leech
    pub bad_data_min_faults: i64,    // min minority-divergence faults for serving-bad-data
    pub bad_data_min_rate: f64,      // and min fault rate (0..1)
    pub no_data_min_error_rate: f64, // error/timeout rate (0..1) over recent probes => serving-no-data
    pub behind_lag_blocks: i64,      // chainhead lag (blocks) considered "behind"
    pub qos_min_queries: i64,        // min QoS query volume before QoS-based verdicts apply
    pub sybil_grade_penalty: f64,    // composite multiplier removed at full sybil confidence (0..1)
    pub serving_grade_penalty: f64,  // composite multiplier removed when ALL measured deployments error (0..1)
    pub serving_min_deployments: i64, // min materially-queried deployments before the serving penalty applies
    pub serving_broken_count_ref: i64, // erroring-deployment count at which the absolute-count penalty saturates
}

impl Default for ScoringConfig {
    fn default() -> Self {
        Self {
            windows: vec![7, 30],
            interval_secs: 900,
            w_correctness: 0.35,
            w_availability: 0.25,
            w_freshness: 0.20,
            w_coverage: 0.10,
            w_value: 0.10,
            grade_a: 90.0,
            grade_b: 75.0,
            grade_c: 60.0,
            grade_d: 40.0,
            low_coverage_subgraphs: 20,
            leech_min_stake_grt: 1_000_000.0,
            leech_max_queries: 100,
            bad_data_min_faults: 3,
            bad_data_min_rate: 0.10,
            no_data_min_error_rate: 0.50,
            // QoS blocks-behind is a single cross-chain AVERAGE per indexer, so a
            // tens-of-thousands figure is usually one lagging subgraph dragging the
            // mean, not a stuck indexer. We can't decompose it from the ingested
            // aggregate, so the bar is set to "egregiously stuck" (≈hundreds of
            // thousands+) — the genuinely-down indexers run into the millions.
            // Moderate lag still feeds the freshness sub-score; it just doesn't
            // raise a loud verdict against reputable operators.
            behind_lag_blocks: 500_000,
            qos_min_queries: 500,
            // A 90%-confidence swarm member loses ~54% of its composite (e.g. A97 → ~D).
            sybil_grade_penalty: 0.6,
            // An indexer serving errors across most of the deployments it actually
            // receives traffic on can't be an A: the penalty scales with the fraction
            // of materially-queried deployments that are erroring (success < 50%).
            // At ~half its deployments broken (e.g. ellipfra: 26/~50), composite drops
            // ~35% — A99 → ~C. Gated by serving_min_deployments to ignore one-off noise.
            serving_grade_penalty: 0.7,
            serving_min_deployments: 3,
            // Erroring on ~20 deployments saturates the absolute-count term, capping
            // a big-but-mostly-healthy operator at ~C/D (e.g. ellipfra: 26 broken of
            // ~600 → out of A). The broadly-dead (datanexus/pinax: ~all broken) still
            // hit F via the fraction term.
            serving_broken_count_ref: 20,
        }
    }
}

#[derive(Debug, Deserialize, Clone)]
#[serde(default)]
pub struct StatusProbeConfig {
    pub enabled: bool,
    pub interval_secs: u64,
    pub concurrency: usize,
    pub timeout_secs: u64,
}

impl Default for StatusProbeConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            interval_secs: 600,
            concurrency: 8,
            timeout_secs: 10,
        }
    }
}

/// Rolling Foghorn's own observations into QoS, in the oracle's schema.
///
/// `bucket_secs` defaults to the oracle's 5-minute cadence so the two feeds are directly
/// comparable. `interval_secs` is deliberately far shorter than E&N's ~30-minute watermark:
/// their delay exists because organic gateway traffic arrives late over Kafka, whereas a probe
/// result is complete the moment the probe returns. There is nothing to wait for.
///
/// `lookback_secs` recomputes a trailing window rather than only the current bucket, so a
/// late-landing observation or a restart mid-bucket converges instead of leaving a permanent
/// hole. That is the specific failure we watched E&N's pipeline produce on 2026-07-29.
#[derive(Debug, Deserialize, Clone)]
#[serde(default)]
pub struct QosRollupConfig {
    pub enabled: bool,
    pub interval_secs: u64,
    pub bucket_secs: u64,
    pub lookback_secs: u64,
    /// Stamped onto every row as the oracle's `gateway_id`. The reference schema puts this on
    /// every data point, so publishing as a distinct gateway is the format's own design, not a
    /// fork of it. Never leave this as another party's id.
    pub gateway_id: String,
    /// Stamped onto every row as the oracle's `chain_id`.
    pub chain_id: String,
}

impl Default for QosRollupConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            interval_secs: 60,
            bucket_secs: 300,
            lookback_secs: 3600,
            gateway_id: "lodestar".to_string(),
            chain_id: "arbitrum-one".to_string(),
        }
    }
}

/// Polling the canonical oracle's DataEdge on Gnosis for PUBLISHER liveness.
///
/// Exists because every other view Foghorn has of that oracle arrives through its subgraph, which
/// makes freshness a measure of our own ingest clock rather than of whether it published. Defaults
/// point at the live QoS DataEdge and Gnosis Blockscout, which needs no API key.
#[derive(Debug, Deserialize, Clone)]
#[serde(default)]
pub struct DataEdgeConfig {
    pub enabled: bool,
    /// The publisher posts every 5 minutes; polling faster only burns explorer quota.
    pub interval_secs: u64,
    pub address: String,
    pub explorer_base: String,
    pub timeout_secs: u64,
}

impl Default for DataEdgeConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            interval_secs: 120,
            address: "0x5b4293b4c0f36cb5d4448950830bc777759b6c4f".to_string(),
            explorer_base: "https://gnosis.blockscout.com".to_string(),
            timeout_secs: 25,
        }
    }
}

/// Mirroring the canonical QoS oracle's subgraph in full.
///
/// The oracle's own subgraph is the only surviving source for its historical metrics: they cannot be
/// recomputed (private gateway telemetry) and cannot be fetched from the chain (the DataEdge carries
/// only CIDs, and those payloads are unreachable from public IPFS). Reaching it needs the gateway,
/// hence `[gateway].api_key`.
#[derive(Debug, Deserialize, Clone)]
#[serde(default)]
pub struct OracleMirrorConfig {
    pub enabled: bool,
    pub interval_secs: u64,
    pub subgraph_id: String,
    pub gateway_base: String,
    /// Trailing days of daily entities to re-pull each cycle. The newest days are still being
    /// written, so a one-shot sync would freeze partial values forever.
    pub window_days: u32,
    /// Separate, shorter window for the 5-minute `AllocationDataPoint` entity — by far the highest
    /// row count, so it gets its own budget rather than consuming the whole request allowance.
    pub point_window_days: u32,
    /// Keyset pages per entity per cycle. Hitting this cap is logged as INCOMPLETE rather than
    /// passing silently for complete.
    pub max_pages: u32,
    pub timeout_secs: u64,
}

impl Default for OracleMirrorConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            interval_secs: 900,
            // The live Gateway QoS Oracle subgraph, same id ingest.rs already reads.
            subgraph_id: "Dtr9rETvwokot4BSXaD5tECanXfqfJKcvHuaaEgPDD2D".to_string(),
            gateway_base: "https://gateway-arbitrum.network.thegraph.com/api".to_string(),
            window_days: 7,
            point_window_days: 2,
            max_pages: 40,
            timeout_secs: 30,
        }
    }
}

#[derive(Debug, Deserialize, Clone)]
#[serde(default)]
pub struct FoghornConfig {
    pub database_url: String,
    pub network_subgraph_url: String,
    pub rpc_urls: HashMap<String, String>,
    pub reorg_threshold: u64,
    pub max_qps_per_indexer: f64,
    pub probe_interval_secs: u64,
    pub freshness_interval_secs: u64,
    pub api_port: u16,
    pub api_host: String,
    pub test_sets_dir: String,
    pub opted_in_indexers: Vec<IndexerConfig>,
    pub cors_origins: Vec<String>,
    /// Max deployments to auto-discover + probe for correctness (0 = disabled,
    /// curated test-sets only). Broadens correctness coverage across the roster.
    pub auto_discover_limit: usize,
    pub gateway: Option<GatewayConfig>,
    pub lodestar: Option<LodestarConfig>,
    pub scoring: ScoringConfig,
    pub status_probe: StatusProbeConfig,
    pub qos_rollup: QosRollupConfig,
    pub data_edge: DataEdgeConfig,
    pub oracle_mirror: OracleMirrorConfig,
    /// Discord webhook URL for #foghorn-alerts. When set, new critical
    /// needs-attention items are pushed to Discord. Empty = alerting disabled.
    pub alert_webhook: Option<String>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct IndexerConfig {
    pub address: String,
    pub url: String,
    pub auth_token: Option<String>,
    pub stake_grt: Option<String>,
}

impl Default for FoghornConfig {
    fn default() -> Self {
        Self {
            database_url: "postgres://dispatch:dispatch@drpc-postgres-1:5432/foghorn".to_string(),
            network_subgraph_url: String::new(),
            rpc_urls: HashMap::new(),
            reorg_threshold: 12,
            max_qps_per_indexer: 0.2,
            probe_interval_secs: 300,
            freshness_interval_secs: 30,
            api_port: 8080,
            api_host: "0.0.0.0".to_string(),
            test_sets_dir: "./test-sets".to_string(),
            opted_in_indexers: vec![],
            cors_origins: vec!["*".to_string()],
            auto_discover_limit: 12,
            gateway: None,
            lodestar: None,
            scoring: ScoringConfig::default(),
            status_probe: StatusProbeConfig::default(),
            qos_rollup: QosRollupConfig::default(),
            data_edge: DataEdgeConfig::default(),
            oracle_mirror: OracleMirrorConfig::default(),
            alert_webhook: None,
        }
    }
}

pub fn load_config() -> anyhow::Result<FoghornConfig> {
    let cfg = config::Config::builder()
        .add_source(config::File::with_name("config").required(false))
        .add_source(
            config::Environment::with_prefix("FOGHORN")
                .separator("__")
                .try_parsing(true),
        )
        .build()?;

    Ok(cfg.try_deserialize::<FoghornConfig>().unwrap_or_else(|e| {
        tracing::warn!(error = %e, "Config deserialization failed, falling back to defaults");
        FoghornConfig::default()
    }))
}
