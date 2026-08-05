//! The judgement core — pure, I/O-free, unit-tested.
//!
//! Scored on what Foghorn measures: block-pinned divergence probing (correctness — the one thing
//! the QoS oracle cannot see), observed errors and per-deployment serving health (availability),
//! measured chainhead lag (freshness), and on-chain allocation count (coverage). Emits:
//!   - a composite 0..100 network-quality score + A..F grade,
//!   - actionable verdicts (naming names), and
//!   - "needs attention" items (current, high-confidence, fix-this-now problems).
//!
//! The canonical QoS oracle is no longer an input to any sub-score. It was, until it emerged that
//! its subgraph had published nothing since 2026-07-01 while answering queries exactly as a live
//! one does: a grade that is part live measurement and part month-old memory cannot tell you which
//! part is which, and neither can the operator being graded. Its figures are still ingested,
//! served and compared against — just never silently blended into a judgement.
//!
//! Query volume was dropped as a component rather than left at zero. Volume is *demand* — which
//! indexers a gateway chose to route to — and no amount of probing reproduces it. Scoring
//! operators on a number we cannot measure was rewarding and punishing them for our blind spot.
//!
//! Every sub-score is 0..100, higher = better. Missing signals are omitted from the weighted mean
//! (weights renormalise over what's present) rather than defaulted, because a zero is a verdict and
//! an absent measurement is not.

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
    /// The canonical oracle's chainhead lag. Carried for display and comparison ONLY; no sub-score,
    /// verdict or attention item reads it. See `freshness_score` and `is_behind`.
    pub qos_blocks_behind: Option<f64>,
    /// Chainhead lag Foghorn measured itself, from `foghorn_qos`. Preferred over
    /// `qos_blocks_behind` (the canonical oracle's figure) because that feed can be — and on
    /// 2026-07-01 was — a month stale while looking perfectly healthy.
    pub measured_blocks_behind: Option<f64>,
    pub qos_query_count: Option<i64>,
    pub reo_status: Option<String>,
    pub ens_name: Option<String>,

    // ── Per-deployment serving health (oracle allocation QoS) ──
    // The indexer-wide qos_success_rate is query-weighted, so a handful of dead
    // deployments vanish under a healthy bulk. These count deployments the indexer
    // actually receives material traffic on, and how many are erroring (success
    // < 50%), so broad serving failure can bite the grade the query-weighted mean misses.
    pub qos_deployments_measured: i64,
    pub qos_deployments_erroring: i64,

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

/// Fraction (0..1) of an indexer's materially-queried deployments that are
/// serving errors. None when there isn't enough deployment coverage to judge.
/// This is the honest "what share of your served deployments work" measure that
/// feeds the availability sub-score.
fn serving_broken_fraction(i: &ScoreInputs, cfg: &ScoringConfig) -> Option<f64> {
    if i.qos_deployments_measured < cfg.serving_min_deployments {
        return None;
    }
    Some(clamp01(
        i.qos_deployments_erroring as f64 / i.qos_deployments_measured as f64,
    ))
}

/// Severity (0..1) driving the composite grade penalty. The max of two signals:
///   • fraction — a totally-broken indexer (errors on ~all deployments) → full hit,
///   • absolute count — erroring on *many* deployments caps you out of A even when
///     proportionally small (a big operator neglecting 26 subgraphs is still 26
///     broken subgraphs). The count term is capped at half so a mostly-healthy
///     giant lands at C/D, not F (which is reserved for the broadly-dead).
fn serving_broken_severity(i: &ScoreInputs, cfg: &ScoringConfig) -> Option<f64> {
    let frac = serving_broken_fraction(i, cfg)?;
    let count_factor =
        clamp01(i.qos_deployments_erroring as f64 / cfg.serving_broken_count_ref.max(1) as f64);
    Some(frac.max(0.5 * count_factor))
}

fn availability_score(i: &ScoreInputs, cfg: &ScoringConfig) -> Option<f64> {
    let mut parts: Vec<f64> = Vec::new();
    if i.total_observations > 0 {
        let err_rate = i.error_observations as f64 / i.total_observations as f64;
        parts.push(100.0 * (1.0 - clamp01(err_rate)));
    }
    // The canonical oracle's success rate is deliberately NOT mixed in here any more. It measures
    // a different population (real gateway traffic) on a different clock, and when it goes stale it
    // does so invisibly — averaging it with live observations produced a score that was part
    // measurement and part month-old memory, with no way to tell which.
    // Per-deployment serving health — surfaces broad failure the query-weighted
    // mean hides (an indexer can be 99% by volume yet erroring on half its subgraphs).
    if let Some(broken) = serving_broken_fraction(i, cfg) {
        parts.push(100.0 * (1.0 - broken));
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
    // Our own measurement, or nothing. The canonical oracle's `qos_blocks_behind` used to serve as
    // a fallback, which sounds harmless and is not: an indexer we have never probed then scored on
    // a chainhead lag last written on 2026-07-01, and the resulting grade was indistinguishable
    // from one earned this hour. Returning None costs that indexer its freshness component and
    // says so, which is the honest answer to "how far behind are they?" when we have not looked.
    //
    // Still not /status latestBlock, which is nonsensical for syncing or cross-chain deployments.
    let bb = i.measured_blocks_behind?;
    Some(lag_to_score(bb.max(0.0), cfg.behind_lag_blocks))
}

fn coverage_score(i: &ScoreInputs, cfg: &ScoringConfig) -> Option<f64> {
    let count = i.allocation_count? as f64;
    // 50 at the threshold, 100 at 2× the threshold.
    let target = (cfg.low_coverage_subgraphs.max(1) as f64) * 2.0;
    Some(100.0 * clamp01(count / target))
}


/// Is there enough signal to actually judge this indexer?
///
/// Two separate questions live here, and conflating them was a bug. *Active* — does this operator
/// do anything at all — is answered by allocations or query volume. *Judgeable* — have we measured
/// anything about the quality of what they do — needs at least one of correctness, availability or
/// freshness, all of which come from our own probes.
///
/// Coverage alone is not a judgement. It counts allocations: how many subgraphs an indexer signed
/// up to serve, not whether any of them are served correctly, quickly or at all. Rating on coverage
/// alone handed a flat A-100 to fifteen indexers we had never once probed, on a page whose whole
/// claim is that it shows measurements — the same failure as reading an absent number as a healthy
/// one, arrived at from the opposite direction.
///
/// An indexer that is active but unmeasured comes back NR: not damning, just honest.
/// A high-stake idle indexer is the exception: that's a leech, and is rated on that basis alone.
fn is_rated(i: &ScoreInputs, cfg: &ScoringConfig) -> bool {
    if is_leech(i, cfg) {
        return true;
    }
    let active = i.probes_answered > 0
        || i.qos_query_count.map(|q| q > 0).unwrap_or(false)
        || i.allocation_count.map(|n| n > 0).unwrap_or(false);
    let measured = correctness_score(i).is_some()
        || availability_score(i, cfg).is_some()
        || freshness_score(i, cfg).is_some();
    active && measured
}

/// Why an indexer came back NR. "Idle" and "unmeasured" are opposite problems and the operator
/// reading this needs to know which one applies to them.
fn unrated_reason(i: &ScoreInputs, cfg: &ScoringConfig) -> String {
    let active = i.probes_answered > 0
        || i.qos_query_count.map(|q| q > 0).unwrap_or(false)
        || i.allocation_count.map(|n| n > 0).unwrap_or(false);
    if active {
        let _ = cfg;
        "not rated — active, but Lodestar has not measured this indexer in the window".to_string()
    } else {
        "inactive — no queries, allocations, or probe coverage".to_string()
    }
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
            reasons: vec![unrated_reason(i, cfg)],
            sub_scores: json!({
                "correctness": null, "availability": null, "freshness": null,
                "coverage": null, "value": null
            }),
        };
        return ScoreOutcome { score, verdicts: vec![], attention: vec![] };
    }

    let correctness = correctness_score(i);
    let availability = availability_score(i, cfg);
    let freshness = freshness_score(i, cfg);
    let coverage = coverage_score(i, cfg);

    // `value` used to sit here, scoring query volume from the canonical oracle. It is gone: query
    // volume is DEMAND, a fact about which indexers a gateway chose to route to, and no amount of
    // probing reproduces it. Scoring an operator on a number we cannot measure — and which was in
    // practice a month stale — was rewarding or punishing them for our blind spot.
    //
    // Weighted mean over present sub-scores. Weights are renormalised by the denominator, so a
    // missing sub-score reduces the divisor rather than counting as zero.
    let weighted: [(Option<f64>, f64); 4] = [
        (correctness, cfg.w_correctness),
        (availability, cfg.w_availability),
        (freshness, cfg.w_freshness),
        (coverage, cfg.w_coverage),
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
    let mut composite = if sybil_flag {
        raw_composite * (1.0 - i.sybil_confidence.unwrap_or(0.0) * cfg.sybil_grade_penalty)
    } else {
        raw_composite
    };
    // Broad serving failure bites the grade too: an indexer erroring across most of
    // the deployments it's actually queried on — or across many in absolute terms —
    // can't be an A, even if its query-weighted success rate looks fine.
    if let Some(severity) = serving_broken_severity(i, cfg) {
        composite *= 1.0 - severity * cfg.serving_grade_penalty;
    }
    let grade = grade_for(composite, cfg).to_string();

    let reasons = build_reasons(i, cfg, correctness, availability, freshness, coverage);
    let sub_scores = json!({
        "correctness": correctness,
        "availability": availability,
        "freshness": freshness,
        "coverage": coverage,
        // Retained as an explicit null so consumers can tell the component was dropped rather
        // than silently omitted.
        "value": serde_json::Value::Null,
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
        // Always None now. The column and the API field remain so consumers see an explicit
        // "not measured" rather than a field that silently disappeared.
        value_score: None,
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
    if let Some(bb) = i.measured_blocks_behind {
        if bb > cfg.behind_lag_blocks as f64 {
            r.push(format!("behind chainhead (~{:.0} blocks, measured)", bb));
        }
    }
    if qos_failing(i, cfg) {
        r.push(format!(
            "low QoS success rate {:.0}% over {} queries",
            i.qos_success_rate.unwrap_or(0.0),
            i.qos_query_count.unwrap_or(0)
        ));
    }
    if let Some(broken) = serving_broken_fraction(i, cfg) {
        if broken > 0.0 {
            r.push(format!(
                "serving errors on {}/{} materially-queried deployments",
                i.qos_deployments_erroring, i.qos_deployments_measured
            ));
        }
    }
    if let (Some(_cov), Some(n)) = (coverage, i.allocation_count) {
        if n < cfg.low_coverage_subgraphs {
            r.push(format!(
                "narrow coverage: {} subgraphs (< {})",
                n, cfg.low_coverage_subgraphs
            ));
        }
    }
    // The "leech" reason lived here: high stake, few queries served. It is gone from the score
    // because it is a judgement about DEMAND — how much traffic a gateway chose to send — which
    // probing cannot measure and which came from a feed that can be silently a month stale.
    // Accusing an operator of leeching on that basis is not a claim this score can support.
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

/// Behind chainhead, on our own reading.
///
/// This fires a High verdict and a needs-attention item that names the operator on a public
/// dashboard. It ran on `qos_blocks_behind` — the canonical oracle's figure — until that feed
/// stopped publishing on 2026-07-01 and kept serving its last values as though they were current.
/// An accusation is only as fresh as its evidence, so the evidence is now ours or there is none.
fn is_behind(i: &ScoreInputs, cfg: &ScoringConfig) -> bool {
    i.measured_blocks_behind.map(|b| b > cfg.behind_lag_blocks as f64).unwrap_or(false)
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
            json!({ "measured_blocks_behind": i.measured_blocks_behind }),
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
        let lag = i.measured_blocks_behind.unwrap_or(0.0).max(0.0);
        a.push(AttentionItem {
            indexer_address: i.indexer_address.clone(),
            kind: "behind-chainhead".to_string(),
            deployment_id: String::new(),
            severity: Severity::High,
            urgency: 50.0 + lag.min(1000.0) / 20.0,
            title: format!("Behind chainhead (~{:.0} blocks)", lag),
            detail: json!({ "measured_blocks_behind": i.measured_blocks_behind }),
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
        // The `value` sub-score is gone: query volume is demand, which probing cannot measure and
        // which came from a feed that can be silently a month stale. It stays in the response as an
        // explicit null so consumers can see it was dropped rather than omitted.
        assert_eq!(out.score.value_score, None);
        // The leech VERDICT still fires. It reads the canonical query count directly rather than
        // via the composite, and is a separate judgement with its own thresholds.
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
        i.measured_blocks_behind = Some(1_600_000.0); // egregiously stuck (> 500k threshold)
        let out = judge(&i, &cfg());
        assert!(out.verdicts.iter().any(|v| v.kind == "behind-chainhead"));
        assert!(out.attention.iter().any(|a| a.kind == "behind-chainhead"));
        assert!(out.score.freshness_score.unwrap() < 50.0);
    }

    #[test]
    fn moderate_lag_does_not_flag_behind() {
        let mut i = healthy();
        i.measured_blocks_behind = Some(6_000.0); // fast-chain noise, not stuck
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

    /// An indexer we have never measured must not be graded on its allocation count alone.
    ///
    /// This asserted the opposite until 2026-08-05, and the live consequence was fifteen indexers
    /// carrying a flat A-100 on the public board whose sole basis was "has allocations" — no probe
    /// ever sent, no response ever seen. Coverage says what an operator signed up to serve, never
    /// whether they serve it.
    #[test]
    fn coverage_alone_is_not_a_grade() {
        let mut i = healthy();
        i.probes_answered = 0;
        i.total_observations = 0;
        i.qos_deployments_measured = 0;
        i.qos_deployments_erroring = 0;
        i.measured_blocks_behind = None;
        let out = judge(&i, &cfg());
        assert!(!out.score.rated, "unmeasured indexer must not be rated");
        assert_eq!(out.score.grade, "NR");
        assert!(out.verdicts.is_empty());
        assert!(out.score.reasons[0].contains("not measured"), "{:?}", out.score.reasons);
    }

    /// The canonical oracle's chainhead figure must not resurrect a grade on its own.
    #[test]
    fn stale_oracle_lag_does_not_stand_in_for_measurement() {
        let mut i = healthy();
        i.probes_answered = 0;
        i.total_observations = 0;
        i.qos_deployments_measured = 0;
        i.qos_deployments_erroring = 0;
        i.measured_blocks_behind = None;
        i.qos_blocks_behind = Some(1.0); // pristine, and a month old
        let out = judge(&i, &cfg());
        assert!(out.score.freshness_score.is_none());
        assert!(!out.score.rated);
    }

    /// Probe coverage without a freshness reading still grades — on what was measured.
    #[test]
    fn partial_measurement_still_grades() {
        let mut i = healthy();
        i.measured_blocks_behind = None;
        let out = judge(&i, &cfg());
        assert!(out.score.rated);
        assert!(out.score.freshness_score.is_none());
        assert!(out.score.composite > 0.0);
    }

    #[test]
    fn broad_serving_failure_bites_the_grade() {
        // An indexer with a healthy query-weighted success rate but erroring across
        // most of its materially-queried deployments must not stay an A.
        let clean = judge(&healthy(), &cfg()).score.composite;
        let mut i = healthy();
        i.qos_deployments_measured = 50;
        i.qos_deployments_erroring = 26; // ~half its deployments broken
        let out = judge(&i, &cfg());
        assert!(out.score.composite < clean - 20.0, "broad failure should drop composite: {} vs {}", out.score.composite, clean);
        assert!(out.score.availability_score.unwrap() < 90.0, "availability should reflect it: {:?}", out.score.availability_score);
        assert!(out.score.reasons.iter().any(|r| r.contains("materially-queried deployments")));
    }

    #[test]
    fn many_broken_deployments_caps_a_big_healthy_indexer() {
        // The ellipfra case: 26 erroring of ~600 measured. Proportionally tiny, but
        // 26 broken subgraphs must knock it out of A via the absolute-count floor.
        let mut i = healthy();
        i.qos_deployments_measured = 600;
        i.qos_deployments_erroring = 26;
        let out = judge(&i, &cfg());
        assert_ne!(out.score.grade, "A", "26 broken deployments must not stay A: composite={}", out.score.composite);
    }

    #[test]
    fn one_off_deployment_error_does_not_bite() {
        // A single erroring deployment below the min-deployments gate is noise, not
        // a broadly-broken indexer — the grade should be unaffected.
        let clean = judge(&healthy(), &cfg()).score.composite;
        let mut i = healthy();
        i.qos_deployments_measured = 2;
        i.qos_deployments_erroring = 1;
        let out = judge(&i, &cfg());
        assert!((out.score.composite - clean).abs() < 0.01, "below gate should not change grade: {} vs {}", out.score.composite, clean);
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
