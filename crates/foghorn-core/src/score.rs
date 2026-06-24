//! The judgement core — pure, I/O-free, unit-tested.
//!
//! Foghorn fuses its own correctness signal (block-pinned divergence probing —
//! the one thing the QoS oracle can't see) with Lodestar-sourced QoS / stake /
//! REO data and direct `/status` health, then emits:
//!   - a composite 0..100 network-quality score + A..F grade,
//!   - actionable verdicts (naming names), and
//!   - "needs attention" items (current, high-confidence, fix-this-now problems).
//!
//! Every sub-score is 0..100, higher = better. Missing signals are simply
//! omitted from the weighted mean (weights renormalise over what's present), so
//! an indexer with no probe coverage is still graded on QoS/coverage/value.

use crate::config::ScoringConfig;
use crate::types::{AttentionItem, IndexerScore, Severity, Verdict};
use serde_json::json;

/// Everything the scorer assembles from the DB for one (indexer, window).
#[derive(Debug, Clone, Default)]
pub struct ScoreInputs {
    pub indexer_address: String,
    pub window_days: i32,

    // ── Foghorn-native probe signals (the correctness edge) over the window ──
    pub probes_answered: i64,
    pub correctness_faults: i64, // probes where this indexer was the minority (wrong) cluster
    pub error_observations: i64,
    pub total_observations: i64,

    // ── Recent tail (last few rounds) — drives urgent verdicts/attention ──
    pub recent_observations: i64,
    pub recent_errors: i64,
    pub recent_faults: i64,

    // ── Lodestar profile / QoS ──
    pub self_stake_grt: Option<f64>,
    pub allocation_count: Option<i32>,
    pub qos_success_rate: Option<f64>, // 0..100
    pub qos_blocks_behind: Option<f64>,
    pub qos_query_count: Option<i64>,
    pub reo_status: Option<String>,
    pub ens_name: Option<String>,

    // NOTE: direct /status probing is collected (status_sample) but NOT used for
    // verdicts — firewalled endpoints and cross-chain/syncing deployments make it
    // an unreliable judge. Freshness/availability/no-data are driven by the
    // QoS oracle (query-derived) and Foghorn's own probes instead.

    // ── Sybil ──
    pub sybil_cluster_id: Option<String>,
    pub sybil_confidence: Option<f64>,
}

/// The full result of judging one (indexer, window).
#[derive(Debug, Clone)]
pub struct ScoreOutcome {
    pub score: IndexerScore,
    pub verdicts: Vec<Verdict>,
    pub attention: Vec<AttentionItem>,
}

/// Confidence at/above which a sybil cluster earns a public verdict.
pub const SYBIL_VERDICT_CONFIDENCE: f64 = 0.6;

fn clamp01(x: f64) -> f64 {
    x.max(0.0).min(1.0)
}

fn grade_for(composite: f64, cfg: &ScoringConfig) -> &'static str {
    if composite >= cfg.grade_a {
        "A"
    } else if composite >= cfg.grade_b {
        "B"
    } else if composite >= cfg.grade_c {
        "C"
    } else if composite >= cfg.grade_d {
        "D"
    } else {
        "F"
    }
}

// ── Individual sub-scores (None = no signal) ──────────────────────────────────

fn correctness_score(i: &ScoreInputs) -> Option<f64> {
    if i.probes_answered <= 0 {
        return None;
    }
    let fault_rate = i.correctness_faults as f64 / i.probes_answered as f64;
    Some(100.0 * (1.0 - clamp01(fault_rate)))
}

fn availability_score(i: &ScoreInputs) -> Option<f64> {
    let mut parts: Vec<f64> = Vec::new();
    if i.total_observations > 0 {
        let err_rate = i.error_observations as f64 / i.total_observations as f64;
        parts.push(100.0 * (1.0 - clamp01(err_rate)));
    }
    if let Some(q) = i.qos_success_rate {
        parts.push(q.max(0.0).min(100.0));
    }
    if parts.is_empty() {
        None
    } else {
        Some(parts.iter().sum::<f64>() / parts.len() as f64)
    }
}

fn lag_to_score(lag: f64, behind_blocks: i64) -> f64 {
    // Full marks at 0 lag; linear to 0 at 4× the "behind" threshold.
    let max_lag = (behind_blocks.max(1) as f64) * 4.0;
    100.0 * (1.0 - clamp01(lag / max_lag))
}

fn freshness_score(i: &ScoreInputs, cfg: &ScoringConfig) -> Option<f64> {
    // Driven by the QoS oracle's measured blocks-behind (query-derived, reliable),
    // not /status latestBlock (nonsensical for syncing / cross-chain deployments).
    let bb = i.qos_blocks_behind?;
    Some(lag_to_score(bb.max(0.0), cfg.behind_lag_blocks))
}

fn coverage_score(i: &ScoreInputs, cfg: &ScoringConfig) -> Option<f64> {
    let count = i.allocation_count? as f64;
    // 50 at the threshold, 100 at 2× the threshold.
    let target = (cfg.low_coverage_subgraphs.max(1) as f64) * 2.0;
    Some(100.0 * clamp01(count / target))
}

fn value_score(i: &ScoreInputs, cfg: &ScoringConfig) -> Option<f64> {
    let queries = i.qos_query_count?;
    // A heavily-staked indexer serving almost nothing is a leech: zero value.
    if let Some(stake) = i.self_stake_grt {
        if stake >= cfg.leech_min_stake_grt && queries <= cfg.leech_max_queries {
            return Some(0.0);
        }
    }
    // Otherwise reward query volume on a log curve (10k queries ≈ full marks).
    let reference = 10_000.0_f64.ln_1p();
    Some(100.0 * clamp01((queries.max(0) as f64).ln_1p() / reference))
}

/// Is there enough signal to actually judge this indexer? An indexer with no
/// query volume, no allocations, and no Foghorn probe coverage is *inactive*,
/// not *bad* — grading it F-0 would conflate "doing nothing" with "doing harm".
/// A high-stake idle indexer is the exception: that's a leech, and is rated.
fn is_rated(i: &ScoreInputs, cfg: &ScoringConfig) -> bool {
    i.qos_query_count.map(|q| q > 0).unwrap_or(false)
        || i.probes_answered > 0
        || i.allocation_count.map(|n| n > 0).unwrap_or(false)
        || is_leech(i, cfg)
}

/// Compute the full judgement for one (indexer, window). Pure.
pub fn judge(i: &ScoreInputs, cfg: &ScoringConfig) -> ScoreOutcome {
    if !is_rated(i, cfg) {
        // Unrated: surface as "NR", not a damning F-0, and emit no verdicts.
        let score = IndexerScore {
            indexer_address: i.indexer_address.clone(),
            window_days: i.window_days,
            composite: 0.0,
            grade: "NR".to_string(),
            rated: false,
            correctness_score: None,
            availability_score: None,
            freshness_score: None,
            coverage_score: None,
            value_score: None,
            sybil_flag: false,
            sybil_cluster_id: None,
            probe_count: 0,
            reasons: vec!["inactive — no queries, allocations, or probe coverage".to_string()],
            sub_scores: json!({
                "correctness": null, "availability": null, "freshness": null,
                "coverage": null, "value": null
            }),
        };
        return ScoreOutcome { score, verdicts: vec![], attention: vec![] };
    }

    let correctness = correctness_score(i);
    let availability = availability_score(i);
    let freshness = freshness_score(i, cfg);
    let coverage = coverage_score(i, cfg);
    let value = value_score(i, cfg);

    // Weighted mean over present sub-scores.
    let weighted: [(Option<f64>, f64); 5] = [
        (correctness, cfg.w_correctness),
        (availability, cfg.w_availability),
        (freshness, cfg.w_freshness),
        (coverage, cfg.w_coverage),
        (value, cfg.w_value),
    ];
    let mut num = 0.0;
    let mut den = 0.0;
    for (v, w) in weighted.iter() {
        if let Some(v) = v {
            num += v * w;
            den += w;
        }
    }
    let raw_composite = if den > 0.0 { num / den } else { 0.0 };

    let sybil_flag = i
        .sybil_confidence
        .map(|c| c >= SYBIL_VERDICT_CONFIDENCE)
        .unwrap_or(false);
    // Swarm membership bites the grade: a confirmed operator-swarm member is a
    // network-health problem regardless of how cleanly it serves data. The
    // penalty scales with detection confidence.
    let composite = if sybil_flag {
        raw_composite * (1.0 - i.sybil_confidence.unwrap_or(0.0) * cfg.sybil_grade_penalty)
    } else {
        raw_composite
    };
    let grade = grade_for(composite, cfg).to_string();

    let reasons = build_reasons(i, cfg, correctness, availability, freshness, coverage, value);
    let sub_scores = json!({
        "correctness": correctness,
        "availability": availability,
        "freshness": freshness,
        "coverage": coverage,
        "value": value,
    });

    let score = IndexerScore {
        indexer_address: i.indexer_address.clone(),
        window_days: i.window_days,
        composite,
        grade,
        rated: true,
        correctness_score: correctness,
        availability_score: availability,
        freshness_score: freshness,
        coverage_score: coverage,
        value_score: value,
        sybil_flag,
        sybil_cluster_id: if sybil_flag {
            i.sybil_cluster_id.clone()
        } else {
            None
        },
        probe_count: i.probes_answered as i32,
        reasons,
        sub_scores,
    };

    ScoreOutcome {
        verdicts: derive_verdicts(i, cfg, composite),
        attention: derive_attention(i, cfg),
        score,
    }
}

fn build_reasons(
    i: &ScoreInputs,
    cfg: &ScoringConfig,
    correctness: Option<f64>,
    availability: Option<f64>,
    _freshness: Option<f64>,
    coverage: Option<f64>,
    value: Option<f64>,
) -> Vec<String> {
    let mut r = Vec::new();
    if let Some(_c) = correctness {
        if i.correctness_faults > 0 {
            r.push(format!(
                "served minority (divergent) data on {}/{} probes",
                i.correctness_faults, i.probes_answered
            ));
        } else if i.probes_answered > 0 {
            r.push(format!("in consensus on all {} probes", i.probes_answered));
        }
    } else {
        r.push("no Foghorn probe coverage in window".to_string());
    }
    if let (Some(_a), Some(q)) = (availability, i.qos_success_rate) {
        r.push(format!("QoS success rate {:.0}%", q));
    }
    if i.total_observations > 0 && i.error_observations > 0 {
        r.push(format!(
            "{}/{} probe responses errored",
            i.error_observations, i.total_observations
        ));
    }
    if let Some(bb) = i.qos_blocks_behind {
        if bb > cfg.behind_lag_blocks as f64 {
            r.push(format!("behind chainhead (~{:.0} blocks, QoS)", bb));
        }
    }
    if qos_failing(i, cfg) {
        r.push(format!(
            "low QoS success rate {:.0}% over {} queries",
            i.qos_success_rate.unwrap_or(0.0),
            i.qos_query_count.unwrap_or(0)
        ));
    }
    if let (Some(_cov), Some(n)) = (coverage, i.allocation_count) {
        if n < cfg.low_coverage_subgraphs {
            r.push(format!(
                "narrow coverage: {} subgraphs (< {})",
                n, cfg.low_coverage_subgraphs
            ));
        }
    }
    if value == Some(0.0) {
        r.push(format!(
            "high stake ({:.0} GRT) but only {} queries served — leech",
            i.self_stake_grt.unwrap_or(0.0),
            i.qos_query_count.unwrap_or(0)
        ));
    }
    if i.ens_name.is_none() {
        r.push("anonymous (no ENS name)".to_string());
    }
    if i.sybil_confidence.map(|c| c >= SYBIL_VERDICT_CONFIDENCE).unwrap_or(false) {
        r.push(format!(
            "member of probable operator swarm {} ({:.0}% confidence)",
            i.sybil_cluster_id.as_deref().unwrap_or("?"),
            i.sybil_confidence.unwrap_or(0.0) * 100.0
        ));
    }
    r
}

// ── Verdicts ──────────────────────────────────────────────────────────────────

fn fault_rate(i: &ScoreInputs) -> f64 {
    if i.probes_answered > 0 {
        i.correctness_faults as f64 / i.probes_answered as f64
    } else {
        0.0
    }
}

fn recent_error_rate(i: &ScoreInputs) -> f64 {
    if i.recent_observations > 0 {
        i.recent_errors as f64 / i.recent_observations as f64
    } else {
        0.0
    }
}

fn is_serving_bad_data(i: &ScoreInputs, cfg: &ScoringConfig) -> bool {
    i.correctness_faults >= cfg.bad_data_min_faults && fault_rate(i) >= cfg.bad_data_min_rate
}

fn qos_failing(i: &ScoreInputs, cfg: &ScoringConfig) -> bool {
    // A meaningfully-queried indexer whose served success rate is poor — the
    // "400s" the network sees. Requires real volume to avoid flagging idle indexers.
    matches!(
        (i.qos_success_rate, i.qos_query_count),
        (Some(sr), Some(q)) if q >= cfg.qos_min_queries && sr < (1.0 - cfg.no_data_min_error_rate) * 100.0
    )
}

fn is_serving_no_data(i: &ScoreInputs, cfg: &ScoringConfig) -> bool {
    // Genuinely failing served queries (QoS), or Foghorn's own probes erroring.
    qos_failing(i, cfg)
        || (i.recent_observations >= 3 && recent_error_rate(i) >= cfg.no_data_min_error_rate)
}

fn is_behind(i: &ScoreInputs, cfg: &ScoringConfig) -> bool {
    i.qos_blocks_behind.map(|b| b > cfg.behind_lag_blocks as f64).unwrap_or(false)
}

fn is_leech(i: &ScoreInputs, cfg: &ScoringConfig) -> bool {
    matches!(
        (i.self_stake_grt, i.qos_query_count),
        (Some(s), Some(q)) if s >= cfg.leech_min_stake_grt && q <= cfg.leech_max_queries
    )
}

fn derive_verdicts(i: &ScoreInputs, cfg: &ScoringConfig, composite: f64) -> Vec<Verdict> {
    let mut v = Vec::new();
    let mk = |kind: &str, sev: Severity, title: String, evidence: serde_json::Value| Verdict {
        indexer_address: i.indexer_address.clone(),
        kind: kind.to_string(),
        severity: sev,
        title,
        evidence,
        window_days: Some(i.window_days),
    };

    if is_serving_bad_data(i, cfg) {
        v.push(mk(
            "serving-bad-data",
            Severity::Critical,
            format!(
                "Serving divergent data on {:.0}% of probes",
                fault_rate(i) * 100.0
            ),
            json!({ "faults": i.correctness_faults, "probes": i.probes_answered, "fault_rate": fault_rate(i) }),
        ));
        // Sustained, severe correctness faults => worth a formal POI dispute.
        if i.correctness_faults >= cfg.bad_data_min_faults * 2 && fault_rate(i) >= cfg.bad_data_min_rate * 2.0 {
            v.push(mk(
                "dispute-candidate",
                Severity::Critical,
                "Sustained correctness faults — POI dispute candidate".to_string(),
                json!({ "faults": i.correctness_faults, "fault_rate": fault_rate(i) }),
            ));
        }
    }

    if is_serving_no_data(i, cfg) {
        v.push(mk(
            "serving-no-data",
            Severity::Critical,
            "Serving errors / no data".to_string(),
            json!({
                "qos_success_rate": i.qos_success_rate,
                "qos_query_count": i.qos_query_count,
                "recent_error_rate": recent_error_rate(i),
                "recent_observations": i.recent_observations,
            }),
        ));
    }

    if is_behind(i, cfg) {
        v.push(mk(
            "behind-chainhead",
            Severity::High,
            "Behind chainhead".to_string(),
            json!({ "qos_blocks_behind": i.qos_blocks_behind }),
        ));
    }

    if let Some(n) = i.allocation_count {
        if n < cfg.low_coverage_subgraphs {
            v.push(mk(
                "low-coverage",
                Severity::Medium,
                format!("Narrow coverage: {} subgraphs", n),
                json!({ "allocation_count": n, "threshold": cfg.low_coverage_subgraphs }),
            ));
        }
    }

    if is_leech(i, cfg) {
        v.push(mk(
            "leech",
            Severity::High,
            "High stake, negligible queries served".to_string(),
            json!({ "self_stake_grt": i.self_stake_grt, "query_count": i.qos_query_count }),
        ));
    }

    // The thread's core ask: REO-eligible yet failing the quality bar. Name the
    // actual failing condition(s) rather than just composite-vs-threshold.
    if i.reo_status.as_deref() == Some("eligible") {
        let mut failing: Vec<&str> = Vec::new();
        if composite < cfg.grade_d {
            failing.push("composite below D grade");
        }
        if is_serving_bad_data(i, cfg) {
            failing.push("serving bad data");
        }
        if is_serving_no_data(i, cfg) {
            failing.push("serving no data");
        }
        if is_leech(i, cfg) {
            failing.push("leech (high stake, negligible queries)");
        }
        if !failing.is_empty() {
            v.push(mk(
                "reo-ineligible-candidate",
                Severity::High,
                format!("REO-eligible but failing: {}", failing.join(", ")),
                json!({ "failing": failing, "composite": composite, "grade_d_threshold": cfg.grade_d }),
            ));
        }
    }

    if i.sybil_confidence.map(|c| c >= SYBIL_VERDICT_CONFIDENCE).unwrap_or(false) {
        v.push(mk(
            "sybil-swarm-member",
            Severity::High,
            "Probable operator-swarm member".to_string(),
            json!({ "cluster_id": i.sybil_cluster_id, "confidence": i.sybil_confidence }),
        ));
    }

    v
}

// ── Needs-attention triage (current, high-confidence "fix now") ───────────────

fn derive_attention(i: &ScoreInputs, cfg: &ScoringConfig) -> Vec<AttentionItem> {
    let mut a = Vec::new();

    if is_serving_no_data(i, cfg) {
        a.push(AttentionItem {
            indexer_address: i.indexer_address.clone(),
            kind: "serving-no-data".to_string(),
            deployment_id: String::new(),
            severity: Severity::Critical,
            urgency: 100.0 + (100.0 - i.qos_success_rate.unwrap_or(100.0)).max(0.0),
            title: "Serving errors / no data".to_string(),
            detail: json!({
                "qos_success_rate": i.qos_success_rate,
                "qos_query_count": i.qos_query_count,
                "recent_errors": i.recent_errors,
                "recent_observations": i.recent_observations,
            }),
        });
    }

    if is_serving_bad_data(i, cfg) {
        a.push(AttentionItem {
            indexer_address: i.indexer_address.clone(),
            kind: "serving-bad-data".to_string(),
            deployment_id: String::new(),
            severity: Severity::Critical,
            urgency: 90.0 + (i.correctness_faults.min(100) as f64),
            title: "Serving divergent (likely wrong) data".to_string(),
            detail: json!({ "faults": i.correctness_faults, "probes": i.probes_answered }),
        });
    }

    if is_behind(i, cfg) {
        let lag = i.qos_blocks_behind.unwrap_or(0.0).max(0.0);
        a.push(AttentionItem {
            indexer_address: i.indexer_address.clone(),
            kind: "behind-chainhead".to_string(),
            deployment_id: String::new(),
            severity: Severity::High,
            urgency: 50.0 + lag.min(1000.0) / 20.0,
            title: format!("Behind chainhead (~{:.0} blocks)", lag),
            detail: json!({ "qos_blocks_behind": i.qos_blocks_behind }),
        });
    }

    a
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cfg() -> ScoringConfig {
        ScoringConfig::default()
    }

    fn healthy() -> ScoreInputs {
        ScoreInputs {
            indexer_address: "0xgood".to_string(),
            window_days: 7,
            probes_answered: 50,
            correctness_faults: 0,
            error_observations: 0,
            total_observations: 50,
            self_stake_grt: Some(500_000.0),
            allocation_count: Some(60),
            qos_success_rate: Some(99.0),
            qos_blocks_behind: Some(1.0),
            qos_query_count: Some(50_000),
            reo_status: Some("eligible".to_string()),
            ens_name: Some("good.eth".to_string()),
            ..Default::default()
        }
    }

    #[test]
    fn healthy_indexer_grades_well_and_no_verdicts() {
        let out = judge(&healthy(), &cfg());
        assert!(out.score.composite >= 90.0, "composite={}", out.score.composite);
        assert_eq!(out.score.grade, "A");
        assert!(out.verdicts.is_empty(), "verdicts={:?}", out.verdicts);
        assert!(out.attention.is_empty());
    }

    #[test]
    fn serving_bad_data_flags_and_lands_in_attention() {
        let mut i = healthy();
        i.correctness_faults = 20; // 40% of 50 probes diverged
        let out = judge(&i, &cfg());
        assert!(out.verdicts.iter().any(|v| v.kind == "serving-bad-data"));
        assert!(out.verdicts.iter().any(|v| v.kind == "dispute-candidate"));
        assert!(out.attention.iter().any(|a| a.kind == "serving-bad-data"));
        assert!(out.score.correctness_score.unwrap() < 70.0);
    }

    #[test]
    fn low_qos_success_is_serving_no_data() {
        let mut i = healthy();
        i.qos_success_rate = Some(20.0); // 80% of served queries error
        i.qos_query_count = Some(5000); // with real volume
        let out = judge(&i, &cfg());
        assert!(out.verdicts.iter().any(|v| v.kind == "serving-no-data"));
        assert!(out.attention.iter().any(|a| a.kind == "serving-no-data" && a.urgency >= 100.0));
    }

    #[test]
    fn deterministic_subgraph_fault_does_not_flag_indexer() {
        // A healthy indexer with good QoS and no Foghorn probe errors must NOT be
        // flagged serving-no-data — a failed deployment elsewhere is a broken
        // subgraph (identical across indexers), not this indexer's fault.
        let i = healthy();
        let out = judge(&i, &cfg());
        assert!(!out.verdicts.iter().any(|v| v.kind == "serving-no-data"));
        assert!(out.attention.is_empty());
    }

    #[test]
    fn low_volume_failures_do_not_flag() {
        // Poor success rate but negligible volume → not flagged (idle, not broken).
        let mut i = healthy();
        i.qos_success_rate = Some(10.0);
        i.qos_query_count = Some(20);
        let out = judge(&i, &cfg());
        assert!(!out.verdicts.iter().any(|v| v.kind == "serving-no-data"));
    }

    #[test]
    fn high_stake_low_queries_is_leech_and_reo_candidate() {
        let mut i = healthy();
        i.self_stake_grt = Some(100_000_000.0); // 100M, the swarm pattern
        i.qos_query_count = Some(5);
        let out = judge(&i, &cfg());
        assert_eq!(out.score.value_score, Some(0.0));
        assert!(out.verdicts.iter().any(|v| v.kind == "leech"));
        // eligible + leech => should be flagged as REO-ineligible candidate
        assert!(out.verdicts.iter().any(|v| v.kind == "reo-ineligible-candidate"));
    }

    #[test]
    fn narrow_coverage_flagged() {
        let mut i = healthy();
        i.allocation_count = Some(3);
        let out = judge(&i, &cfg());
        assert!(out.verdicts.iter().any(|v| v.kind == "low-coverage"));
    }

    #[test]
    fn behind_chainhead_attention() {
        let mut i = healthy();
        i.qos_blocks_behind = Some(1_600_000.0); // egregiously stuck (> 500k threshold)
        let out = judge(&i, &cfg());
        assert!(out.verdicts.iter().any(|v| v.kind == "behind-chainhead"));
        assert!(out.attention.iter().any(|a| a.kind == "behind-chainhead"));
        assert!(out.score.freshness_score.unwrap() < 50.0);
    }

    #[test]
    fn moderate_lag_does_not_flag_behind() {
        let mut i = healthy();
        i.qos_blocks_behind = Some(6_000.0); // fast-chain noise, not stuck
        let out = judge(&i, &cfg());
        assert!(!out.verdicts.iter().any(|v| v.kind == "behind-chainhead"));
    }

    #[test]
    fn inactive_indexer_is_unrated_not_f() {
        let i = ScoreInputs {
            indexer_address: "0xidle".to_string(),
            window_days: 7,
            ..Default::default()
        };
        let out = judge(&i, &cfg());
        assert!(!out.score.rated);
        assert_eq!(out.score.grade, "NR");
        assert!(out.verdicts.is_empty());
        assert!(out.attention.is_empty());
    }

    #[test]
    fn high_stake_idle_is_rated_leech() {
        let i = ScoreInputs {
            indexer_address: "0xleech".to_string(),
            window_days: 7,
            self_stake_grt: Some(5_000_000.0),
            qos_query_count: Some(0),
            ..Default::default()
        };
        let out = judge(&i, &cfg());
        assert!(out.score.rated);
        assert!(out.verdicts.iter().any(|v| v.kind == "leech"));
    }

    #[test]
    fn no_probe_coverage_still_grades_on_other_signals() {
        let mut i = healthy();
        i.probes_answered = 0;
        i.total_observations = 0;
        let out = judge(&i, &cfg());
        assert!(out.score.correctness_score.is_none());
        assert!(out.score.composite > 0.0);
    }

    #[test]
    fn sybil_member_flagged_above_confidence() {
        let mut i = healthy();
        i.sybil_cluster_id = Some("swarm-7".to_string());
        i.sybil_confidence = Some(0.8);
        let out = judge(&i, &cfg());
        assert!(out.score.sybil_flag);
        assert!(out.verdicts.iter().any(|v| v.kind == "sybil-swarm-member"));
        // Swarm membership must bite the grade: composite drops vs the clean baseline.
        let clean = judge(&healthy(), &cfg()).score.composite;
        assert!(out.score.composite < clean - 20.0, "sybil should drop composite: {} vs {}", out.score.composite, clean);

        i.sybil_confidence = Some(0.3); // below gate
        let out = judge(&i, &cfg());
        assert!(!out.score.sybil_flag);
        assert!(!out.verdicts.iter().any(|v| v.kind == "sybil-swarm-member"));
    }
}
