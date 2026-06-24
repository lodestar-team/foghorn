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
use foghorn_core::types::{AttentionItem, IndexerScore, Verdict};
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
    let profiles = load_profiles(pool).await?;
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
            let inputs = assemble(addr, *window, &agg, &recent, &profiles, &sybil_map);
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
    sybil_map: &HashMap<String, (String, f64)>,
) -> ScoreInputs {
    let a = agg.get(addr).cloned().unwrap_or_default();
    let r = recent.get(addr).cloned().unwrap_or_default();
    let p = profiles.get(addr).cloned().unwrap_or_default();
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
        qos_query_count: p.qos_query_count,
        reo_status: p.reo_status,
        ens_name: p.ens_name,
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
        r#"SELECT am.indexer_address AS addr,
                  COUNT(*)::bigint AS total,
                  COUNT(*) FILTER (WHERE o.error_class IS NOT NULL OR o.response_hash IS NULL)::bigint AS errors,
                  COUNT(DISTINCT o.probe_id) FILTER (WHERE o.response_hash IS NOT NULL) AS probes_answered,
                  COUNT(DISTINCT o.probe_id) FILTER (
                      WHERE d.probe_id IS NOT NULL
                        AND o.response_hash IS NOT NULL
                        AND o.response_hash <> d.largest_by_count_hash
                  ) AS faults
           FROM observation o
           JOIN probe p ON p.id = o.probe_id
           LEFT JOIN divergence d ON d.probe_id = o.probe_id
           JOIN allocation_map am ON am.allocation_key = o.indexer_address
                AND am.indexer_address IS NOT NULL
           WHERE p.dispatched_at > NOW() - $1::interval
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
