//! Scoring loop. After ingest/probe/status data lands, this assembles
//! `ScoreInputs` per (indexer, window) from Postgres, runs the pure
//! `foghorn_core::score::judge`, and upserts grades, verdicts, attention items,
//! and sybil clusters. Verdicts/attention are derived from the primary (longest)
//! window — they describe the indexer's current standing.

use crate::sybil;
use anyhow::Result;
use chrono::Utc;
use foghorn_core::config::ScoringConfig;
use foghorn_core::score::{judge, ScoreInputs};
use foghorn_core::types::{AttentionItem, IndexerScore, Severity, Verdict};
use sqlx::{PgPool, Row};
use std::collections::{HashMap, HashSet};
use std::time::Duration;
use tracing::{info, warn};

#[derive(Default, Clone)]
struct ProbeAgg {
    probes_answered: i64,
    faults: i64,
    errors: i64,
    total: i64,
}

#[derive(Default, Clone)]
struct Profile {
    ens_name: Option<String>,
    self_stake_grt: Option<f64>,
    allocation_count: Option<i32>,
    qos_success_rate: Option<f64>,
    qos_blocks_behind: Option<f64>,
    qos_query_count: Option<i64>,
    reo_status: Option<String>,
}

pub async fn run_score_loop(cfg: ScoringConfig, api_key: Option<String>, pool: PgPool) {
    info!(windows = ?cfg.windows, interval = cfg.interval_secs, "Scoring loop starting");
    loop {
        match run_score_once(&cfg, api_key.as_deref(), &pool).await {
            Ok(n) => info!(scored = n, "Scoring cycle complete"),
            Err(e) => warn!(error = %e, "Scoring cycle failed"),
        }
        tokio::time::sleep(Duration::from_secs(cfg.interval_secs)).await;
    }
}

pub async fn run_score_once(cfg: &ScoringConfig, api_key: Option<&str>, pool: &PgPool) -> Result<usize> {
    let run_start = Utc::now();

    let sybil_map = sybil::detect_and_store(pool, api_key).await.unwrap_or_else(|e| {
        warn!(error = %e, "Sybil detection failed");
        HashMap::new()
    });
    // Flag chronically non-deterministic deployments before scoring so they can
    // be excluded from correctness faulting.
    if let Err(e) = detect_nondeterministic(pool).await {
        warn!(error = %e, "Non-deterministic deployment detection failed");
    }

    let profiles = load_profiles(pool).await?;
    let alloc_health = load_alloc_health(pool, cfg).await?;
    let measured_lag = load_measured_lag(pool).await?;
    let recent = load_probe_agg(pool, "6 hours").await?;

    let mut windows = cfg.windows.clone();
    if windows.is_empty() {
        windows.push(30);
    }
    windows.sort_unstable();
    let primary = *windows.iter().max().unwrap();

    let mut scored = 0usize;
    for window in &windows {
        let agg = load_probe_agg(pool, &format!("{} days", window)).await?;

        let mut keys: HashSet<&String> = HashSet::new();
        keys.extend(profiles.keys());
        keys.extend(agg.keys());

        for addr in keys {
            let inputs = assemble(addr, *window, &agg, &recent, &profiles, &alloc_health, &sybil_map, &measured_lag);
            let outcome = judge(&inputs, cfg);
            upsert_score(pool, &outcome.score).await?;
            scored += 1;

            if *window == primary {
                for v in &outcome.verdicts {
                    upsert_verdict(pool, v).await?;
                }
                for a in &outcome.attention {
                    upsert_attention(pool, a).await?;
                }
            }
        }
    }

    // Per-(indexer, deployment) issues from the oracle's allocation QoS — catches
    // "synced but serving errors on subgraph X" and genuine per-deployment lag,
    // allocation-scoped (never flags an indexer merely syncing a deployment).
    match detect_allocation_issues(pool, cfg).await {
        Ok(n) => info!(items = n, "Per-deployment QoS check complete"),
        Err(e) => warn!(error = %e, "Per-deployment QoS check failed"),
    }

    // Drop verdicts/attention not re-emitted this run (condition cleared).
    sqlx::query("DELETE FROM verdict WHERE last_seen < $1")
        .bind(run_start)
        .execute(pool)
        .await?;
    sqlx::query("DELETE FROM attention_item WHERE last_seen < $1")
        .bind(run_start)
        .execute(pool)
        .await?;

    Ok(scored)
}

#[allow(clippy::too_many_arguments)]
fn assemble(
    addr: &str,
    window: i32,
    agg: &HashMap<String, ProbeAgg>,
    recent: &HashMap<String, ProbeAgg>,
    profiles: &HashMap<String, Profile>,
    alloc_health: &HashMap<String, (i64, i64)>,
    sybil_map: &HashMap<String, (String, f64)>,
    // Chainhead lag Foghorn measured itself. Separate from `profiles`, which carries the canonical
    // oracle's figures, so the two provenances cannot be mixed up at the call site.
    measured_lag: &HashMap<String, f64>,
) -> ScoreInputs {
    let a = agg.get(addr).cloned().unwrap_or_default();
    let r = recent.get(addr).cloned().unwrap_or_default();
    let p = profiles.get(addr).cloned().unwrap_or_default();
    let (qos_deployments_measured, qos_deployments_erroring) =
        alloc_health.get(addr).copied().unwrap_or((0, 0));
    let (sybil_cluster_id, sybil_confidence) = match sybil_map.get(addr) {
        Some((id, conf)) => (Some(id.clone()), Some(*conf)),
        None => (None, None),
    };

    ScoreInputs {
        indexer_address: addr.to_string(),
        window_days: window,
        probes_answered: a.probes_answered,
        correctness_faults: a.faults,
        error_observations: a.errors,
        total_observations: a.total,
        recent_observations: r.total,
        recent_errors: r.errors,
        recent_faults: r.faults,
        self_stake_grt: p.self_stake_grt,
        allocation_count: p.allocation_count,
        qos_success_rate: p.qos_success_rate,
        qos_blocks_behind: p.qos_blocks_behind,
        measured_blocks_behind: measured_lag.get(addr).copied(),
        qos_query_count: p.qos_query_count,
        reo_status: p.reo_status,
        ens_name: p.ens_name,
        qos_deployments_measured,
        qos_deployments_erroring,
        sybil_cluster_id,
        sybil_confidence,
    }
}

// ── Loaders ───────────────────────────────────────────────────────────────────

async fn load_probe_agg(pool: &PgPool, interval: &str) -> Result<HashMap<String, ProbeAgg>> {
    // Aggregate Foghorn probe outcomes by REAL indexer address (resolved through
    // allocation_map from the recovered allocation signing key). A "fault" =
    // this indexer's response differed from the majority cluster on a divergent
    // probe — i.e. it served minority (likely wrong) data.
    let rows = sqlx::query(
        r#"WITH responders AS (
               SELECT probe_id, COUNT(*) FILTER (WHERE response_hash IS NOT NULL) AS n
               FROM observation GROUP BY probe_id
           )
           -- See qos.rs: a gateway observation carries the allocation signing key, a paid one
           -- carries the indexer itself. Resolving both through allocation_map dropped every paid
           -- observation, so grades were computed from gateway data only while the page reported
           -- direct coverage.
           SELECT COALESCE(am.indexer_address, o.indexer_address) AS addr,
                  COUNT(*)::bigint AS total,
                  COUNT(*) FILTER (WHERE o.error_class IS NOT NULL OR o.response_hash IS NULL)::bigint AS errors,
                  COUNT(DISTINCT o.probe_id) FILTER (WHERE o.response_hash IS NOT NULL) AS probes_answered,
                  -- A fault counts only when a CLEAR MAJORITY agreed and this indexer
                  -- deviated from it. A no-majority scatter (e.g. a subgraph with
                  -- non-deterministic BigDecimal aggregates) is real divergence but
                  -- not one indexer's fault, so it is not penalised here.
                  COUNT(DISTINCT o.probe_id) FILTER (
                      WHERE d.probe_id IS NOT NULL
                        AND o.response_hash IS NOT NULL
                        AND o.response_hash <> d.largest_by_count_hash
                        AND d.largest_by_count_size * 2 > r.n
                        AND p.deployment_id NOT IN (SELECT deployment_id FROM nondeterministic_deployment)
                  ) AS faults
           FROM observation o
           JOIN probe p ON p.id = o.probe_id
           LEFT JOIN divergence d ON d.probe_id = o.probe_id
           LEFT JOIN responders r ON r.probe_id = o.probe_id
           LEFT JOIN allocation_map am ON am.allocation_key = o.indexer_address
                AND am.indexer_address IS NOT NULL
           WHERE p.dispatched_at > NOW() - $1::interval
             AND (am.indexer_address IS NOT NULL OR o.dispatch_mode = 'paid')
             -- Payment refusals describe our escrow, not the operator. Left in, they would drive
             -- the availability sub-score — and therefore the public grade — straight down for
             -- every indexer whose tap-agent has not yet seen our deposit. See qos.rs for the full
             -- reasoning; the two must agree or the grade and the feed would tell different stories.
             AND (o.error_class IS NULL OR o.error_class NOT LIKE 'payment\_%')
           GROUP BY am.indexer_address"#,
    )
    .bind(interval)
    .fetch_all(pool)
    .await?;

    let mut map = HashMap::new();
    for row in rows {
        map.insert(
            row.get::<String, _>("addr").to_lowercase(),
            ProbeAgg {
                probes_answered: row.get::<i64, _>("probes_answered"),
                faults: row.get::<i64, _>("faults"),
                errors: row.get::<i64, _>("errors"),
                total: row.get::<i64, _>("total"),
            },
        );
    }
    Ok(map)
}

/// Flag deployments that diverge persistently (every round, across blocks) as
/// non-deterministic — the subgraph's mappings, not the indexers, are at fault.
async fn detect_nondeterministic(pool: &PgPool) -> Result<()> {
    let rows = sqlx::query(
        r#"SELECT p.deployment_id,
                  COUNT(DISTINCT p.id)::int AS total,
                  COUNT(DISTINCT d.probe_id)::int AS divergent
           FROM probe p
           LEFT JOIN divergence d ON d.probe_id = p.id
           WHERE p.dispatched_at > NOW() - INTERVAL '7 days'
           GROUP BY p.deployment_id
           HAVING COUNT(DISTINCT d.probe_id) >= 3
              AND COUNT(DISTINCT d.probe_id)::float8 / NULLIF(COUNT(DISTINCT p.id), 0) >= 0.5"#,
    )
    .fetch_all(pool)
    .await?;

    let mut ids: Vec<String> = Vec::new();
    for row in &rows {
        let dep: String = row.get("deployment_id");
        let total: i32 = row.get("total");
        let divergent: i32 = row.get("divergent");
        let rate = divergent as f64 / (total.max(1) as f64);
        let sample = sample_divergent_fields(pool, &dep).await.unwrap_or_default();
        sqlx::query(
            r#"INSERT INTO nondeterministic_deployment
                 (deployment_id, divergent_probes, total_probes, divergence_rate, sample_fields, last_seen)
               VALUES ($1,$2,$3,$4,$5, NOW())
               ON CONFLICT (deployment_id) DO UPDATE SET
                 divergent_probes = EXCLUDED.divergent_probes,
                 total_probes = EXCLUDED.total_probes,
                 divergence_rate = EXCLUDED.divergence_rate,
                 sample_fields = EXCLUDED.sample_fields,
                 last_seen = NOW()"#,
        )
        .bind(&dep)
        .bind(divergent)
        .bind(total)
        .bind(rate)
        .bind(serde_json::to_value(&sample)?)
        .execute(pool)
        .await?;
        ids.push(dep);
    }

    // Drop deployments that no longer qualify (divergence cleared up).
    sqlx::query("DELETE FROM nondeterministic_deployment WHERE deployment_id <> ALL($1)")
        .bind(&ids)
        .execute(pool)
        .await?;
    Ok(())
}

/// Per-deployment lag margin (blocks) an indexer may trail the freshest serving
/// peer before it's flagged "behind on this deployment".
const PER_DEPLOYMENT_LAG_MARGIN: f64 = 50_000.0;

/// Flag per-(indexer, deployment) problems from the oracle's allocation QoS:
///   • serving-errors-deployment — low success rate with real volume (the
///     "synced but 400ing on subgraph X" case the per-indexer average misses);
///   • behind-deployment — lagging the freshest *serving* peer on the same
///     deployment (allocation-scoped, so syncing-not-allocated indexers don't
///     false-positive, and same-deployment = same-chain so blocks compare).
/// Both are needs-attention items (fixable per-deployment issues), not grade hits.
/// When an indexer has this many affected deployments, roll them into a single
/// indexer-level item (it's an indexer-wide outage, not N subgraph issues).
const ROLLUP_THRESHOLD: usize = 5;

async fn detect_allocation_issues(pool: &PgPool, cfg: &ScoringConfig) -> Result<usize> {
    let mut n = 0usize;

    // 1. Serving errors on specific deployments (low success, enough probes to mean something).
    //
    // From `foghorn_qos`, not the canonical oracle's `allocation_qos`. These items name an operator
    // and a subgraph on the public dashboard; sourcing them from a feed frozen since 2026-07-01
    // would keep accusing indexers of outages that may have ended a month ago, with the item's
    // timestamp claiming otherwise.
    let errs = sqlx::query(
        r#"SELECT addr AS indexer_address, deployment_id,
                  ok::float8 / probes::float8 AS success_rate,
                  probes AS query_count
           FROM (
               SELECT LOWER(indexer_address) AS addr, deployment_id,
                      SUM(query_count) AS probes, SUM(num_indexer_200_responses) AS ok
               FROM foghorn_qos
               WHERE bucket_start >= NOW() - ($1 || ' days')::interval
               GROUP BY 1, 2
           ) d
           WHERE probes >= $2 AND ok::float8 / probes::float8 < 0.5
           ORDER BY addr, probes DESC"#,
    )
    .bind(cfg.windows.iter().max().copied().unwrap_or(30))
    .bind(cfg.serving_min_probes)
    .fetch_all(pool)
    .await?;
    let mut by_indexer: HashMap<String, Vec<(String, f64, i64)>> = HashMap::new();
    for row in &errs {
        by_indexer
            .entry(row.get("indexer_address"))
            .or_default()
            .push((row.get("deployment_id"), row.get("success_rate"), row.get("query_count")));
    }
    for (indexer, deps) in by_indexer {
        if deps.len() >= ROLLUP_THRESHOLD {
            // Indexer-wide outage → one item. Store the FULL deployment list so the
            // dashboard can expand it (not just 3 examples).
            let total_q: i64 = deps.iter().map(|(_, _, q)| q).sum();
            let deployments: Vec<&str> = deps.iter().map(|(d, _, _)| d.as_str()).collect();
            let item = AttentionItem {
                indexer_address: indexer,
                kind: "serving-errors".to_string(),
                deployment_id: String::new(),
                severity: Severity::Critical,
                urgency: 100.0 + (deps.len() as f64).min(50.0),
                title: format!("Serving errors across {} deployments (indexer-wide)", deps.len()),
                detail: serde_json::json!({
                    "deployment_count": deps.len(),
                    "total_probes": total_q,
                    "deployments": deployments,
                }),
            };
            upsert_attention(pool, &item).await?;
            n += 1;
        } else {
            for (dep, sr, qc) in &deps {
                let item = AttentionItem {
                    indexer_address: indexer.clone(),
                    kind: "serving-errors-deployment".to_string(),
                    deployment_id: dep.clone(),
                    severity: Severity::Critical,
                    urgency: 95.0 + (1.0 - sr) * 5.0,
                    title: format!("Serving errors on a deployment ({:.0}% success over {} probes)", sr * 100.0, qc),
                    detail: serde_json::json!({ "success_rate": sr, "probe_count": qc }),
                };
                upsert_attention(pool, &item).await?;
                n += 1;
            }
        }
    }

    // 2. Behind the freshest serving peer on specific deployments.
    //
    // The peer floor is computed over the same window and the same feed, so "behind the freshest
    // serving peer" compares indexers measured at the same time by the same prober. Mixing a live
    // reading against a month-old floor would manufacture lag out of nothing but the clock.
    let lags = sqlx::query(
        r#"WITH recent AS (
               SELECT LOWER(indexer_address) AS addr, deployment_id,
                      AVG(avg_indexer_blocks_behind) AS blocks_behind,
                      SUM(query_count) AS probes
               FROM foghorn_qos
               WHERE bucket_start >= NOW() - ($2 || ' days')::interval
                 AND avg_indexer_blocks_behind IS NOT NULL
               GROUP BY 1, 2
           ),
           baseline AS (
               SELECT deployment_id, MIN(blocks_behind) AS floor, COUNT(*) AS peers
               FROM recent WHERE probes >= $3 GROUP BY deployment_id
           )
           SELECT a.addr AS indexer_address, a.deployment_id, a.blocks_behind, b.floor, b.peers
           FROM recent a
           JOIN baseline b ON b.deployment_id = a.deployment_id
           WHERE b.peers >= 3
             AND a.probes >= $3
             AND a.blocks_behind > b.floor + $1
             AND a.blocks_behind > $1
           ORDER BY a.addr, a.blocks_behind DESC"#,
    )
    .bind(PER_DEPLOYMENT_LAG_MARGIN)
    .bind(cfg.windows.iter().max().copied().unwrap_or(30))
    .bind(cfg.serving_min_probes)
    .fetch_all(pool)
    .await?;
    let mut lag_by_indexer: HashMap<String, Vec<(String, f64, f64)>> = HashMap::new();
    for row in &lags {
        lag_by_indexer
            .entry(row.get("indexer_address"))
            .or_default()
            .push((row.get("deployment_id"), row.get("blocks_behind"), row.get("floor")));
    }
    for (indexer, deps) in lag_by_indexer {
        if deps.len() >= ROLLUP_THRESHOLD {
            let worst = deps.iter().map(|(_, b, _)| *b).fold(0.0_f64, f64::max);
            let deployments: Vec<&str> = deps.iter().map(|(d, _, _)| d.as_str()).collect();
            let item = AttentionItem {
                indexer_address: indexer,
                kind: "behind-deployments".to_string(),
                deployment_id: String::new(),
                severity: Severity::High,
                urgency: 65.0 + (deps.len() as f64).min(30.0),
                title: format!("Behind on {} deployments (worst ~{:.0} blocks)", deps.len(), worst),
                detail: serde_json::json!({ "deployment_count": deps.len(), "worst_blocks_behind": worst, "deployments": deployments }),
            };
            upsert_attention(pool, &item).await?;
            n += 1;
        } else {
            for (dep, bb, floor) in &deps {
                let item = AttentionItem {
                    indexer_address: indexer.clone(),
                    kind: "behind-deployment".to_string(),
                    deployment_id: dep.clone(),
                    severity: Severity::High,
                    urgency: 60.0 + (bb / 100_000.0).min(35.0),
                    title: format!("Behind on a deployment (~{:.0} blocks; freshest serving peer at {:.0})", bb, floor),
                    detail: serde_json::json!({ "blocks_behind": bb, "peer_floor": floor }),
                };
                upsert_attention(pool, &item).await?;
                n += 1;
            }
        }
    }
    Ok(n)
}

/// Distinct trailing field names from a deployment's recent divergence diffs
/// (e.g. ".../totalVolumeUSD" → "totalVolumeUSD") — shows devs what's unstable.
async fn sample_divergent_fields(pool: &PgPool, deployment_id: &str) -> Result<Vec<String>> {
    let rows = sqlx::query(
        r#"SELECT DISTINCT regexp_replace(elem->>'path', '^.*/', '') AS field
           FROM divergence d
           JOIN probe p ON p.id = d.probe_id
           CROSS JOIN LATERAL jsonb_array_elements(d.diff_patches) elem
           WHERE p.deployment_id = $1
             AND p.dispatched_at > NOW() - INTERVAL '7 days'
           LIMIT 12"#,
    )
    .bind(deployment_id)
    .fetch_all(pool)
    .await?;
    Ok(rows
        .iter()
        .filter_map(|r| r.get::<Option<String>, _>("field"))
        .filter(|s| !s.is_empty())
        .collect())
}

async fn load_profiles(pool: &PgPool) -> Result<HashMap<String, Profile>> {
    let rows = sqlx::query(
        r#"SELECT indexer_address, ens_name, self_stake_grt, allocation_count,
                  qos_success_rate, qos_blocks_behind, qos_query_count, reo_status
           FROM indexer_profile"#,
    )
    .fetch_all(pool)
    .await?;
    let mut map = HashMap::new();
    for row in rows {
        map.insert(
            row.get::<String, _>("indexer_address").to_lowercase(),
            Profile {
                ens_name: row.get("ens_name"),
                self_stake_grt: row.get("self_stake_grt"),
                allocation_count: row.get("allocation_count"),
                qos_success_rate: row.get("qos_success_rate"),
                qos_blocks_behind: row.get("qos_blocks_behind"),
                qos_query_count: row.get("qos_query_count"),
                reo_status: row.get("reo_status"),
            },
        );
    }
    Ok(map)
}

/// Per-indexer deployment-serving health from the oracle's allocation QoS:
/// (materially-queried deployments, erroring deployments). "Erroring" mirrors
/// detect_allocation_issues, so the grade penalty and the needs-attention items agree on what
/// "broken" means.
///
/// Read from `foghorn_qos` — our own probes — rather than `allocation_qos`, which holds the
/// canonical oracle's numbers. That feed has published nothing since 2026-07-01 while continuing
/// to answer queries normally, so every grade computed from it was scoring a month-old snapshot
/// and had no way to say so. A per-deployment penalty is an accusation; making one from data that
/// old, against operators who may have fixed the problem weeks ago, is the same failure as reading
/// an absent measurement as a healthy one.
async fn load_alloc_health(pool: &PgPool, cfg: &ScoringConfig) -> Result<HashMap<String, (i64, i64)>> {
    let rows = sqlx::query(
        r#"WITH per_dep AS (
               SELECT LOWER(indexer_address) AS addr,
                      deployment_id,
                      SUM(query_count)               AS probes,
                      SUM(num_indexer_200_responses) AS ok
               FROM foghorn_qos
               WHERE bucket_start >= NOW() - ($1 || ' days')::interval
               GROUP BY 1, 2
           )
           SELECT addr,
                  COUNT(*) FILTER (WHERE probes >= $2)::bigint AS measured,
                  COUNT(*) FILTER (WHERE probes >= $2 AND ok::float8 / probes::float8 < 0.5)::bigint AS erroring
           FROM per_dep
           GROUP BY addr"#,
    )
    .bind(cfg.windows.iter().max().copied().unwrap_or(30))
    .bind(cfg.serving_min_probes)
    .fetch_all(pool)
    .await?;
    let mut map = HashMap::new();
    for row in rows {
        map.insert(
            row.get::<String, _>("addr"),
            (row.get::<i64, _>("measured"), row.get::<i64, _>("erroring")),
        );
    }
    Ok(map)
}

// ── Upserts ───────────────────────────────────────────────────────────────────

async fn upsert_score(pool: &PgPool, s: &IndexerScore) -> Result<()> {
    sqlx::query(
        r#"INSERT INTO indexer_score
             (indexer_address, window_days, computed_at, composite, grade, rated,
              correctness_score, availability_score, freshness_score, coverage_score,
              value_score, sybil_flag, sybil_cluster_id, probe_count, reasons, sub_scores)
           VALUES ($1,$2, NOW(),$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15)
           ON CONFLICT (indexer_address, window_days) DO UPDATE SET
             computed_at = NOW(),
             composite = EXCLUDED.composite,
             grade = EXCLUDED.grade,
             rated = EXCLUDED.rated,
             correctness_score = EXCLUDED.correctness_score,
             availability_score = EXCLUDED.availability_score,
             freshness_score = EXCLUDED.freshness_score,
             coverage_score = EXCLUDED.coverage_score,
             value_score = EXCLUDED.value_score,
             sybil_flag = EXCLUDED.sybil_flag,
             sybil_cluster_id = EXCLUDED.sybil_cluster_id,
             probe_count = EXCLUDED.probe_count,
             reasons = EXCLUDED.reasons,
             sub_scores = EXCLUDED.sub_scores"#,
    )
    .bind(&s.indexer_address)
    .bind(s.window_days)
    .bind(s.composite)
    .bind(&s.grade)
    .bind(s.rated)
    .bind(s.correctness_score)
    .bind(s.availability_score)
    .bind(s.freshness_score)
    .bind(s.coverage_score)
    .bind(s.value_score)
    .bind(s.sybil_flag)
    .bind(&s.sybil_cluster_id)
    .bind(s.probe_count)
    .bind(serde_json::to_value(&s.reasons)?)
    .bind(&s.sub_scores)
    .execute(pool)
    .await?;
    Ok(())
}

async fn upsert_verdict(pool: &PgPool, v: &Verdict) -> Result<()> {
    sqlx::query(
        r#"INSERT INTO verdict
             (indexer_address, kind, severity, title, evidence, window_days, first_seen, last_seen, status)
           VALUES ($1,$2,$3,$4,$5,$6, NOW(), NOW(), 'open')
           ON CONFLICT (indexer_address, kind) DO UPDATE SET
             severity = EXCLUDED.severity,
             title = EXCLUDED.title,
             evidence = EXCLUDED.evidence,
             window_days = EXCLUDED.window_days,
             last_seen = NOW(),
             status = 'open'"#,
    )
    .bind(&v.indexer_address)
    .bind(&v.kind)
    .bind(v.severity.as_str())
    .bind(&v.title)
    .bind(&v.evidence)
    .bind(v.window_days)
    .execute(pool)
    .await?;
    Ok(())
}

async fn upsert_attention(pool: &PgPool, a: &AttentionItem) -> Result<()> {
    sqlx::query(
        r#"INSERT INTO attention_item
             (indexer_address, kind, deployment_id, severity, urgency, title, detail, first_seen, last_seen)
           VALUES ($1,$2,$3,$4,$5,$6,$7, NOW(), NOW())
           ON CONFLICT (indexer_address, kind, deployment_id) DO UPDATE SET
             severity = EXCLUDED.severity,
             urgency = EXCLUDED.urgency,
             title = EXCLUDED.title,
             detail = EXCLUDED.detail,
             last_seen = NOW()"#,
    )
    .bind(&a.indexer_address)
    .bind(&a.kind)
    .bind(&a.deployment_id)
    .bind(a.severity.as_str())
    .bind(a.urgency)
    .bind(&a.title)
    .bind(&a.detail)
    .execute(pool)
    .await?;
    Ok(())
}

/// Chainhead lag Foghorn measured itself, per indexer, over the recent window.
///
/// Read from `foghorn_qos` rather than `indexer_profile.qos_blocks_behind`, which carries the
/// canonical oracle's number. That distinction is the whole point: the oracle's figure was frozen
/// at 2026-07-01 for over a month while reporting itself healthy, so a freshness score built on it
/// was scoring month-old state and could not tell.
async fn load_measured_lag(pool: &PgPool) -> Result<HashMap<String, f64>> {
    let rows = sqlx::query(
        r#"SELECT indexer_address, avg(avg_indexer_blocks_behind) AS lag
           FROM foghorn_qos
           WHERE bucket_start >= NOW() - interval '24 hours'
             AND avg_indexer_blocks_behind IS NOT NULL
           GROUP BY 1"#,
    )
    .fetch_all(pool)
    .await?;
    Ok(rows
        .into_iter()
        .filter_map(|r| {
            let addr: String = r.get("indexer_address");
            r.get::<Option<f64>, _>("lag").map(|l| (addr, l))
        })
        .collect())
}
