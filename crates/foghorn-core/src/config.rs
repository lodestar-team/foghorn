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
    /// Dead. `value` scored query volume from the canonical oracle — a demand signal probing
    /// cannot reproduce, from a feed that can be silently stale. Kept so existing config files
    /// still parse; changing it has no effect.
    #[deprecated(note = "value was removed from the composite; this weight is ignored")]
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
    /// Probes on one (indexer, deployment) before its serving health counts.
    ///
    /// Replaces the old `query_count >= 100` gate, which was tuned for the canonical oracle's real
    /// gateway traffic. Probes arrive on `probe_interval_secs` (3600 in production), so 100 would
    /// mean four days of uninterrupted probing per deployment and almost nothing would ever qualify.
    pub serving_min_probes: i64,
}

impl Default for ScoringConfig {
    fn default() -> Self {
        Self {
            windows: vec![7, 30],
            interval_secs: 900,
            // Renormalised over the four components we can actually measure, preserving their
            // relative emphasis after `value` (0.10) was removed. Correctness leads because it is
            // the one signal no gateway telemetry can produce at all.
            w_correctness: 0.40,
            w_availability: 0.30,
            w_freshness: 0.20,
            w_coverage: 0.10,
            #[allow(deprecated)]
            w_value: 0.0,
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
            // Ten probes is enough that a single timeout cannot push a deployment under 50%, and
            // low enough to qualify within a day of hourly probing.
            serving_min_probes: 10,
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
    /// How far below chainhead probes are pinned. Chainhead at probe time is
    /// `probe.block_number + this`, which is the reference blocks-behind is measured against. Must
    /// match `reorg_threshold`, which is what the scheduler pins with.
    pub chainhead_offset: u64,
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
            chainhead_offset: 12,
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
    /// Fetch each newly-seen payload from IPFS immediately, while it is still being provided.
    pub capture_payloads: bool,
    /// Tried in order, first success wins.
    pub ipfs_gateways: Vec<String>,
}

impl Default for DataEdgeConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            interval_secs: 120,
            address: "0x5b4293b4c0f36cb5d4448950830bc777759b6c4f".to_string(),
            explorer_base: "https://gnosis.blockscout.com".to_string(),
            timeout_secs: 25,
            capture_payloads: true,
            ipfs_gateways: vec![
                "https://ipfs.io/ipfs".to_string(),
                "https://dweb.link/ipfs".to_string(),
                "https://gateway.pinata.cloud/ipfs".to_string(),
            ],
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
    /// Window for the 5-minute `AllocationDataPoint` entity, in HOURS. One row per indexer ×
    /// deployment × 288 buckets a day means a day-granular window is millions of rows, far past any
    /// page budget, so this is deliberately the smallest unit that makes sense.
    pub point_window_hours: u32,
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
            point_window_hours: 24,
            max_pages: 40,
            timeout_secs: 30,
        }
    }
}

/// Paying indexers directly, instead of routing probes through Edge & Node's gateway.
///
/// This is what makes the measurement honest. Probes dispatched through a gateway are routed to
/// indexers it already believes are healthy, so failures it avoids are invisible and any success
/// rate computed from them is an upper bound rather than a measurement. Paying directly means we
/// choose who answers.
///
/// Requires, on-chain and in this order: the signer authorised on GraphTallyCollector, and escrow
/// deposited for each (payer, collector, receiver) tuple. Escrow is PER INDEXER, so coverage costs
/// locked capital rather than fees.
#[derive(Debug, Deserialize, Clone)]
#[serde(default)]
pub struct TapConfig {
    pub enabled: bool,
    /// The prober's signing key. Signs receipts and nothing else — it holds no funds and cannot
    /// move any, which is why it is the key that belongs on a server. Set via
    /// `FOGHORN__TAP__SIGNER_KEY`; never commit it.
    pub signer_key: Option<String>,
    /// The escrow account that authorised the signer. Recovered from the signature by the indexer,
    /// so it must match what was funded.
    pub payer: String,
    /// GraphTallyCollector — the EIP-712 verifying contract.
    pub verifier: String,
    /// SubgraphService. Checked by the indexer's DataServiceCheck.
    pub data_service: String,
    /// PaymentsEscrow, for reading balances.
    pub escrow: String,
    /// Value per receipt, in the escrow token's smallest unit. Must clear the indexer's cost model:
    /// `MinimumValue` rejects an underpriced receipt, which reads as a refusal to serve.
    pub receipt_value: u128,
    /// How often to re-read escrow balances on-chain.
    pub escrow_sync_secs: u64,
    /// How often to refresh active allocations from the network subgraph.
    pub allocation_sync_secs: u64,
    /// Indexers never to probe, whatever their allocations say — retiring operators, opt-outs.
    /// Probing them produces failures that describe our target list rather than their health.
    pub excluded_indexers: Vec<String>,
}

impl Default for TapConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            signer_key: None,
            payer: String::new(),
            // Arbitrum One. GraphTallyCollector cross-checked on-chain against
            // SubgraphService.getGraphTallyCollector().
            verifier: "0x8f69F5C07477Ac46FBc491B1E6D91E2bb0111A9e".to_string(),
            data_service: "0xb2Bb92d0DE618878E438b55D5846cfecD9301105".to_string(),
            escrow: "0xf6Fcc27aAf1fcD8B254498c9794451d82afC673E".to_string(),
            // ~0.001 GRT. Comfortably above observed cost models (~0.00073 GRT/query network-wide)
            // without overpaying by an order of magnitude.
            receipt_value: 1_000_000_000_000_000u128,
            escrow_sync_secs: 900,
            allocation_sync_secs: 1800,
            excluded_indexers: Vec::new(),
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
    pub tap: TapConfig,
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
            tap: TapConfig::default(),
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
