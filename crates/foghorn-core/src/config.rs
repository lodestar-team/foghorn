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
            behind_lag_blocks: 50,
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
    pub gateway: Option<GatewayConfig>,
    pub lodestar: Option<LodestarConfig>,
    pub scoring: ScoringConfig,
    pub status_probe: StatusProbeConfig,
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
            gateway: None,
            lodestar: None,
            scoring: ScoringConfig::default(),
            status_probe: StatusProbeConfig::default(),
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
