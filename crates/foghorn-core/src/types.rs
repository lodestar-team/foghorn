use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Probe {
    pub id: Uuid,
    pub deployment_id: String,
    pub block_hash: String,
    pub block_number: i64,
    pub query_hash: String,
    pub query_category: String,
    pub query_text: String,
    pub dispatched_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Observation {
    pub probe_id: Uuid,
    pub indexer_address: String,
    pub response_hash: Option<String>,
    pub latency_ms: Option<i32>,
    pub meta_block_number: Option<i64>,
    pub meta_block_hash: Option<String>,
    pub http_status: Option<i32>,
    pub error_class: Option<String>,
    pub stake_weight: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Divergence {
    pub probe_id: Uuid,
    pub cluster_count: i32,
    pub diff_patches: serde_json::Value,
    pub largest_by_count_hash: String,
    pub largest_by_count_size: i32,
    pub largest_by_stake_hash: String,
    pub largest_by_stake_weight: f64,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FreshnessSample {
    pub id: i64,
    pub indexer_address: String,
    pub deployment_id: String,
    pub sampled_at: DateTime<Utc>,
    pub meta_block_number: i64,
    pub meta_block_hash: String,
    pub chainhead_lag_blocks: i32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusterSummary {
    pub hash: String,
    pub member_count: usize,
    pub stake_weight: f64,
    pub members: Vec<String>,
    pub is_largest_by_count: bool,
    pub is_largest_by_stake: bool,
}

// Test set types — loaded from YAML
#[derive(Debug, Clone, Deserialize)]
pub struct TestSet {
    pub deployment: TestSetDeployment,
    pub queries: Vec<TestQuery>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct TestSetDeployment {
    pub id: String,
    pub ipfs_hash: String,
    pub network: String,
    pub description: String,
    /// Subgraph ID used with The Graph gateway (base58 format, e.g. "J55C6V...")
    #[serde(default)]
    pub gateway_subgraph_id: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct TestQuery {
    pub category: String,
    pub template: String,
    #[serde(default)]
    pub entity_ids: Vec<String>,
}

// ── Judgement layer ───────────────────────────────────────────────────────────

/// Ingested context for one indexer (Lodestar enriched roster + QoS aggregate),
/// keyed by the real indexer address.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct IndexerProfile {
    pub indexer_address: String,
    pub ens_name: Option<String>,
    pub url: Option<String>,
    pub created_at: Option<i64>,
    pub self_stake_grt: Option<f64>,
    pub delegated_grt: Option<f64>,
    pub allocated_grt: Option<f64>,
    pub allocation_count: Option<i32>,
    pub query_fees_collected_grt: Option<f64>,
    pub reo_status: Option<String>,
    pub reo_source: Option<String>,
    pub lodestar_score: Option<f64>,
    pub lodestar_grade: Option<String>,
    pub qos_query_count: Option<i64>,
    pub qos_success_rate: Option<f64>,
    pub qos_latency_ms: Option<f64>,
    pub qos_blocks_behind: Option<f64>,
}

/// One direct `/status` health sample for an (indexer, deployment).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StatusSample {
    pub indexer_address: String,
    pub deployment_id: String,
    pub sampled_at: DateTime<Utc>,
    pub synced: Option<bool>,
    pub health: Option<String>,
    pub chain_head_block: Option<i64>,
    pub latest_block: Option<i64>,
    pub lag_blocks: Option<i64>,
    pub fatal_error: Option<String>,
    pub probe_error: Option<String>,
}

/// Severity tier shared by verdicts and attention items.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Severity {
    Critical,
    High,
    Medium,
    Low,
}

impl Severity {
    pub fn as_str(&self) -> &'static str {
        match self {
            Severity::Critical => "critical",
            Severity::High => "high",
            Severity::Medium => "medium",
            Severity::Low => "low",
        }
    }
}

/// A computed composite score for an indexer over a rolling window.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexerScore {
    pub indexer_address: String,
    pub window_days: i32,
    pub composite: f64,
    pub grade: String,
    /// False = inactive/unrated (no queries, allocations, or probes) — shown as "NR".
    pub rated: bool,
    pub correctness_score: Option<f64>,
    pub availability_score: Option<f64>,
    pub freshness_score: Option<f64>,
    pub coverage_score: Option<f64>,
    pub value_score: Option<f64>,
    pub sybil_flag: bool,
    pub sybil_cluster_id: Option<String>,
    pub probe_count: i32,
    pub reasons: Vec<String>,
    pub sub_scores: serde_json::Value,
}

/// An actionable verdict — Foghorn naming names.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Verdict {
    pub indexer_address: String,
    pub kind: String,
    pub severity: Severity,
    pub title: String,
    pub evidence: serde_json::Value,
    pub window_days: Option<i32>,
}

/// An entry in the "needs attention" triage surface — current, high-confidence,
/// "fix this right now" problems (serving no data / serving bad data / behind chainhead).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AttentionItem {
    pub indexer_address: String,
    pub kind: String,
    pub deployment_id: String,
    pub severity: Severity,
    pub urgency: f64,
    pub title: String,
    pub detail: serde_json::Value,
}

/// A detected operator-swarm cluster.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SybilCluster {
    pub cluster_id: String,
    pub confidence: f64,
    pub member_count: i32,
    pub members: Vec<String>,
    pub signals: serde_json::Value,
}
