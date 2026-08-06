use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    response::Json,
};
use serde::Deserialize;
use serde_json::{json, Value};
use sqlx::Row;
use uuid::Uuid;

use crate::AppState;

// ── Health ──────────────────────────────────────────────────────────────────

pub async fn health() -> Json<Value> {
    Json(json!({ "status": "ok", "version": env!("CARGO_PKG_VERSION") }))
}

// ── Stats ───────────────────────────────────────────────────────────────────

pub async fn stats(State(state): State<AppState>) -> Result<Json<Value>, StatusCode> {
    let db = &state.pool;

    let total_probes: i64 = sqlx::query("SELECT COUNT(*) FROM probe")
        .fetch_one(db)
        .await
        .map(|r| r.get::<i64, _>(0))
        .unwrap_or(0);

    let total_divergences: i64 = sqlx::query("SELECT COUNT(*) FROM divergence WHERE cluster_count > 1")
        .fetch_one(db)
        .await
        .map(|r| r.get::<i64, _>(0))
        .unwrap_or(0);

    let opted_in_indexers: i64 =
        sqlx::query("SELECT COUNT(DISTINCT indexer_address) FROM observation")
            .fetch_one(db)
            .await
            .map(|r| r.get::<i64, _>(0))
            .unwrap_or(0);

    let deployments_covered: i64 =
        sqlx::query("SELECT COUNT(DISTINCT deployment_id) FROM probe")
            .fetch_one(db)
            .await
            .map(|r| r.get::<i64, _>(0))
            .unwrap_or(0);

    let probes_24h: i64 =
        sqlx::query("SELECT COUNT(*) FROM probe WHERE dispatched_at > NOW() - INTERVAL '24 hours'")
            .fetch_one(db)
            .await
            .map(|r| r.get::<i64, _>(0))
            .unwrap_or(0);

    let divergences_24h: i64 = sqlx::query(
        "SELECT COUNT(*) FROM divergence WHERE cluster_count > 1 AND created_at > NOW() - INTERVAL '24 hours'",
    )
    .fetch_one(db)
    .await
    .map(|r| r.get::<i64, _>(0))
    .unwrap_or(0);

    let divergence_rate_24h = if probes_24h > 0 {
        divergences_24h as f64 / probes_24h as f64
    } else {
        0.0
    };

    Ok(Json(json!({
        "total_probes": total_probes,
        "total_divergences": total_divergences,
        "opted_in_indexers": opted_in_indexers,
        "deployments_covered": deployments_covered,
        "divergence_rate_24h": divergence_rate_24h,
        "probes_24h": probes_24h,
        "divergences_24h": divergences_24h,
    })))
}

// ── Feed ────────────────────────────────────────────────────────────────────

#[derive(Deserialize)]
pub struct FeedParams {
    pub limit: Option<i64>,
    pub offset: Option<i64>,
    pub deployment_id: Option<String>,
    pub indexer: Option<String>,
}

pub async fn feed(
    State(state): State<AppState>,
    Query(params): Query<FeedParams>,
) -> Result<Json<Value>, StatusCode> {
    let limit = params.limit.unwrap_or(50).min(200);
    let offset = params.offset.unwrap_or(0);

    // Build query with optional filters
    let rows = if let Some(ref deployment_id) = params.deployment_id {
        sqlx::query(
            r#"SELECT p.id, p.deployment_id, p.block_number, p.block_hash, p.query_category,
                      p.dispatched_at, d.cluster_count, d.diff_patches,
                      COUNT(o.indexer_address)::int as indexer_count
               FROM divergence d
               JOIN probe p ON p.id = d.probe_id
               LEFT JOIN observation o ON o.probe_id = p.id
               -- Only actual disagreements. A row exists for every corroborated probe now,
               -- including unanimous ones, and this feed names indexers publicly.
               WHERE d.cluster_count > 1
                 AND p.deployment_id = $1
               GROUP BY p.id, p.deployment_id, p.block_number, p.block_hash,
                        p.query_category, p.dispatched_at, d.cluster_count, d.diff_patches, d.created_at
               ORDER BY d.created_at DESC
               LIMIT $2 OFFSET $3"#,
        )
        .bind(deployment_id)
        .bind(limit)
        .bind(offset)
        .fetch_all(&state.pool)
        .await
    } else {
        sqlx::query(
            r#"SELECT p.id, p.deployment_id, p.block_number, p.block_hash, p.query_category,
                      p.dispatched_at, d.cluster_count, d.diff_patches,
                      COUNT(o.indexer_address)::int as indexer_count
               FROM divergence d
               JOIN probe p ON p.id = d.probe_id
               LEFT JOIN observation o ON o.probe_id = p.id
               -- Only actual disagreements; see the filtered variant above.
               WHERE d.cluster_count > 1
               GROUP BY p.id, p.deployment_id, p.block_number, p.block_hash,
                        p.query_category, p.dispatched_at, d.cluster_count, d.diff_patches, d.created_at
               ORDER BY d.created_at DESC
               LIMIT $1 OFFSET $2"#,
        )
        .bind(limit)
        .bind(offset)
        .fetch_all(&state.pool)
        .await
    }
    .map_err(|e| {
        tracing::error!(error = %e, "feed query failed");
        StatusCode::INTERNAL_SERVER_ERROR
    })?;

    let events: Vec<Value> = rows
        .iter()
        .map(|r| {
            let probe_id: Uuid = r.get("id");
            let diff_patches: Value = r.get("diff_patches");
            let diff_count = diff_patches.as_array().map(|a| a.len()).unwrap_or(0);
            json!({
                "probe_id": probe_id.to_string(),
                "deployment_id": r.get::<String, _>("deployment_id"),
                "block_number": r.get::<i64, _>("block_number"),
                "block_hash": r.get::<String, _>("block_hash"),
                "query_category": r.get::<String, _>("query_category"),
                "dispatched_at": r.get::<chrono::DateTime<chrono::Utc>, _>("dispatched_at"),
                "cluster_count": r.get::<i32, _>("cluster_count"),
                "indexer_count": r.get::<i32, _>("indexer_count"),
                "diff_patch_count": diff_count,
            })
        })
        .collect();

    Ok(Json(json!({ "events": events, "count": events.len() })))
}

// ── Probe detail ─────────────────────────────────────────────────────────────

pub async fn probe_detail(
    State(state): State<AppState>,
    Path(probe_id): Path<Uuid>,
) -> Result<Json<Value>, StatusCode> {
    let probe_row = sqlx::query(
        "SELECT id, deployment_id, block_hash, block_number, query_category, query_text, dispatched_at
         FROM probe WHERE id = $1",
    )
    .bind(probe_id)
    .fetch_optional(&state.pool)
    .await
    .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
    .ok_or(StatusCode::NOT_FOUND)?;

    let obs_rows = sqlx::query(
        "SELECT indexer_address, response_hash, latency_ms, meta_block_number, meta_block_hash,
                http_status, error_class, stake_weight
         FROM observation WHERE probe_id = $1 ORDER BY indexer_address",
    )
    .bind(probe_id)
    .fetch_all(&state.pool)
    .await
    .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let div_row = sqlx::query(
        "SELECT cluster_count, diff_patches, largest_by_count_hash, largest_by_count_size,
                largest_by_stake_hash, largest_by_stake_weight
         FROM divergence WHERE probe_id = $1",
    )
    .bind(probe_id)
    .fetch_optional(&state.pool)
    .await
    .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let probe_id_str: Uuid = probe_row.get("id");

    Ok(Json(json!({
        "probe": {
            "id": probe_id_str.to_string(),
            "deployment_id": probe_row.get::<String, _>("deployment_id"),
            "block_hash": probe_row.get::<String, _>("block_hash"),
            "block_number": probe_row.get::<i64, _>("block_number"),
            "query_category": probe_row.get::<String, _>("query_category"),
            "query_text": probe_row.get::<String, _>("query_text"),
            "dispatched_at": probe_row.get::<chrono::DateTime<chrono::Utc>, _>("dispatched_at"),
        },
        "observations": obs_rows.iter().map(|o| json!({
            "indexer_address": o.get::<String, _>("indexer_address"),
            "response_hash": o.get::<Option<String>, _>("response_hash"),
            "latency_ms": o.get::<Option<i32>, _>("latency_ms"),
            "meta_block_number": o.get::<Option<i64>, _>("meta_block_number"),
            "meta_block_hash": o.get::<Option<String>, _>("meta_block_hash"),
            "http_status": o.get::<Option<i32>, _>("http_status"),
            "error_class": o.get::<Option<String>, _>("error_class"),
            "stake_weight": o.get::<f64, _>("stake_weight"),
        })).collect::<Vec<_>>(),
        "divergence": div_row.as_ref().map(|d| json!({
            "cluster_count": d.get::<i32, _>("cluster_count"),
            "diff_patches": d.get::<Value, _>("diff_patches"),
            "largest_by_count": {
                "hash": d.get::<String, _>("largest_by_count_hash"),
                "size": d.get::<i32, _>("largest_by_count_size"),
            },
            "largest_by_stake": {
                "hash": d.get::<String, _>("largest_by_stake_hash"),
                "weight": d.get::<f64, _>("largest_by_stake_weight"),
            },
        })),
    })))
}

// ── Indexer quality ──────────────────────────────────────────────────────────

#[derive(Deserialize)]
pub struct QualityParams {
    pub days: Option<i32>,
}

pub async fn indexer_quality(
    State(state): State<AppState>,
    Path(address): Path<String>,
    Query(params): Query<QualityParams>,
) -> Result<Json<Value>, StatusCode> {
    let days = params.days.unwrap_or(30);
    let address = address.to_lowercase();
    let interval = format!("{} days", days);

    let summary = sqlx::query(
        r#"SELECT
             COUNT(DISTINCT o.probe_id) as total_probes,
             COUNT(DISTINCT CASE WHEN d.cluster_count > 1 THEN o.probe_id END) as divergent_probes,
             ROUND(AVG(o.latency_ms))::int as avg_latency_ms,
             PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY o.latency_ms) as p50_latency,
             PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY o.latency_ms) as p95_latency
           FROM observation o
           JOIN probe p ON p.id = o.probe_id
           LEFT JOIN divergence d ON d.probe_id = o.probe_id
           WHERE o.indexer_address = $1
             AND p.dispatched_at > NOW() - $2::interval
             AND o.response_hash IS NOT NULL"#,
    )
    .bind(&address)
    .bind(&interval)
    .fetch_one(&state.pool)
    .await
    .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let by_deployment = sqlx::query(
        r#"SELECT
             p.deployment_id,
             COUNT(DISTINCT o.probe_id) as total_probes,
             COUNT(DISTINCT CASE WHEN d.cluster_count > 1 THEN o.probe_id END) as divergent_probes
           FROM observation o
           JOIN probe p ON p.id = o.probe_id
           LEFT JOIN divergence d ON d.probe_id = o.probe_id
           WHERE o.indexer_address = $1
             AND p.dispatched_at > NOW() - $2::interval
           GROUP BY p.deployment_id"#,
    )
    .bind(&address)
    .bind(&interval)
    .fetch_all(&state.pool)
    .await
    .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let recent_probes = sqlx::query(
        r#"SELECT p.id, p.deployment_id, p.query_category, p.dispatched_at,
                  o.response_hash, d.probe_id as divergence_probe_id
           FROM observation o
           JOIN probe p ON p.id = o.probe_id
           LEFT JOIN divergence d ON d.probe_id = o.probe_id
           WHERE o.indexer_address = $1
           ORDER BY p.dispatched_at DESC
           LIMIT 20"#,
    )
    .bind(&address)
    .fetch_all(&state.pool)
    .await
    .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let total_probes: i64 = summary.get("total_probes");
    let divergent_probes: i64 = summary.get("divergent_probes");
    let divergence_rate = if total_probes > 0 {
        divergent_probes as f64 / total_probes as f64
    } else {
        0.0
    };

    Ok(Json(json!({
        "indexer_address": address,
        "days": days,
        "total_probes": total_probes,
        "divergent_probes": divergent_probes,
        "divergence_rate": divergence_rate,
        "avg_latency_ms": summary.get::<Option<i32>, _>("avg_latency_ms"),
        "p50_latency_ms": summary.get::<Option<f64>, _>("p50_latency"),
        "p95_latency_ms": summary.get::<Option<f64>, _>("p95_latency"),
        "by_deployment": by_deployment.iter().map(|r| {
            let tp: i64 = r.get("total_probes");
            let dp: i64 = r.get("divergent_probes");
            json!({
                "deployment_id": r.get::<String, _>("deployment_id"),
                "total_probes": tp,
                "divergent_probes": dp,
                "divergence_rate": if tp > 0 { dp as f64 / tp as f64 } else { 0.0 },
            })
        }).collect::<Vec<_>>(),
        "recent_probes": recent_probes.iter().map(|r| {
            let pid: Uuid = r.get("id");
            json!({
                "probe_id": pid.to_string(),
                "deployment_id": r.get::<String, _>("deployment_id"),
                "query_category": r.get::<String, _>("query_category"),
                "dispatched_at": r.get::<chrono::DateTime<chrono::Utc>, _>("dispatched_at"),
                "response_hash": r.get::<Option<String>, _>("response_hash"),
                "divergent": r.get::<Option<Uuid>, _>("divergence_probe_id").is_some(),
            })
        }).collect::<Vec<_>>(),
    })))
}

// ── Indexer freshness ────────────────────────────────────────────────────────

pub async fn indexer_freshness(
    State(state): State<AppState>,
    Path(address): Path<String>,
) -> Result<Json<Value>, StatusCode> {
    let address = address.to_lowercase();

    let samples = sqlx::query(
        r#"SELECT deployment_id, chainhead_lag_blocks, sampled_at
           FROM freshness_sample
           WHERE indexer_address = $1
             AND sampled_at > NOW() - INTERVAL '24 hours'
           ORDER BY sampled_at DESC
           LIMIT 500"#,
    )
    .bind(&address)
    .fetch_all(&state.pool)
    .await
    .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    Ok(Json(json!({
        "indexer_address": address,
        "samples": samples.iter().map(|s| json!({
            "deployment_id": s.get::<String, _>("deployment_id"),
            "chainhead_lag_blocks": s.get::<i32, _>("chainhead_lag_blocks"),
            "sampled_at": s.get::<chrono::DateTime<chrono::Utc>, _>("sampled_at"),
        })).collect::<Vec<_>>(),
    })))
}

// ── Deployments list ─────────────────────────────────────────────────────────

pub async fn deployments(State(state): State<AppState>) -> Result<Json<Value>, StatusCode> {
    let rows = sqlx::query(
        r#"SELECT
             p.deployment_id,
             COUNT(DISTINCT p.id) as total_probes,
             ROUND(AVG(CASE WHEN o.response_hash IS NOT NULL THEN o.latency_ms END))::int as avg_latency_ms,
             PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY CASE WHEN o.response_hash IS NOT NULL THEN o.latency_ms END) as p50_latency_ms,
             PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY CASE WHEN o.response_hash IS NOT NULL THEN o.latency_ms END) as p95_latency_ms,
             MAX(p.dispatched_at) as last_probe_at,
             COUNT(DISTINCT CASE WHEN o.response_hash IS NOT NULL THEN o.indexer_address END) as unique_indexers
           FROM probe p
           LEFT JOIN observation o ON o.probe_id = p.id
           WHERE p.dispatched_at > NOW() - INTERVAL '7 days'
           GROUP BY p.deployment_id
           ORDER BY total_probes DESC"#,
    )
    .fetch_all(&state.pool)
    .await
    .map_err(|e| {
        tracing::error!(error = %e, "deployments query failed");
        StatusCode::INTERNAL_SERVER_ERROR
    })?;

    let list: Vec<Value> = rows
        .iter()
        .map(|r| {
            json!({
                "deployment_id": r.get::<String, _>("deployment_id"),
                "total_probes": r.get::<i64, _>("total_probes"),
                "avg_latency_ms": r.get::<Option<i32>, _>("avg_latency_ms"),
                "p50_latency_ms": r.get::<Option<f64>, _>("p50_latency_ms").map(|v| v.round() as i64),
                "p95_latency_ms": r.get::<Option<f64>, _>("p95_latency_ms").map(|v| v.round() as i64),
                "last_probe_at": r.get::<Option<chrono::DateTime<chrono::Utc>>, _>("last_probe_at"),
                "unique_indexers": r.get::<i64, _>("unique_indexers"),
            })
        })
        .collect();

    Ok(Json(json!({ "deployments": list })))
}

// ── Deployment quality ───────────────────────────────────────────────────────

pub async fn deployment_quality(
    State(state): State<AppState>,
    Path(deployment_id): Path<String>,
    Query(params): Query<QualityParams>,
) -> Result<Json<Value>, StatusCode> {
    let days = params.days.unwrap_or(7);
    let interval = format!("{} days", days);

    let by_indexer = sqlx::query(
        r#"SELECT
             o.indexer_address,
             am.indexer_address as resolved_indexer,
             am.indexer_url,
             COUNT(DISTINCT o.probe_id) as total_probes,
             COUNT(DISTINCT CASE WHEN d.cluster_count > 1 THEN o.probe_id END) as divergent_probes,
             ROUND(AVG(o.latency_ms))::int as avg_latency_ms,
             MAX(p.dispatched_at) as last_seen
           FROM observation o
           JOIN probe p ON p.id = o.probe_id
           LEFT JOIN divergence d ON d.probe_id = o.probe_id
           LEFT JOIN allocation_map am ON am.allocation_key = o.indexer_address
           WHERE p.deployment_id = $1
             AND p.dispatched_at > NOW() - $2::interval
           GROUP BY o.indexer_address, am.indexer_address, am.indexer_url
           ORDER BY total_probes DESC"#,
    )
    .bind(&deployment_id)
    .bind(&interval)
    .fetch_all(&state.pool)
    .await
    .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let recent_divergences = sqlx::query(
        r#"SELECT p.id, p.block_number, p.query_category, p.dispatched_at, d.cluster_count
           FROM divergence d
           JOIN probe p ON p.id = d.probe_id
           WHERE p.deployment_id = $1
           ORDER BY p.dispatched_at DESC
           LIMIT 10"#,
    )
    .bind(&deployment_id)
    .fetch_all(&state.pool)
    .await
    .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    Ok(Json(json!({
        "deployment_id": deployment_id,
        "days": days,
        "indexers": by_indexer.iter().map(|r| {
            let tp: i64 = r.get("total_probes");
            let dp: i64 = r.get("divergent_probes");
            json!({
                "indexer_address": r.get::<String, _>("indexer_address"),
                "resolved_indexer": r.get::<Option<String>, _>("resolved_indexer"),
                "indexer_url": r.get::<Option<String>, _>("indexer_url"),
                "total_probes": tp,
                "divergent_probes": dp,
                "divergence_rate": if tp > 0 { dp as f64 / tp as f64 } else { 0.0 },
                "avg_latency_ms": r.get::<Option<i32>, _>("avg_latency_ms"),
                "last_seen": r.get::<Option<chrono::DateTime<chrono::Utc>>, _>("last_seen"),
            })
        }).collect::<Vec<_>>(),
        "recent_divergences": recent_divergences.iter().map(|r| {
            let pid: Uuid = r.get("id");
            json!({
                "probe_id": pid.to_string(),
                "block_number": r.get::<i64, _>("block_number"),
                "query_category": r.get::<String, _>("query_category"),
                "dispatched_at": r.get::<chrono::DateTime<chrono::Utc>, _>("dispatched_at"),
                "cluster_count": r.get::<i32, _>("cluster_count"),
            })
        }).collect::<Vec<_>>(),
    })))
}

// ── Judgement layer ───────────────────────────────────────────────────────────

#[derive(Deserialize)]
pub struct IndexersParams {
    pub window: Option<i32>,
    pub limit: Option<i64>,
    pub offset: Option<i64>,
    pub order: Option<String>, // "asc" | "desc" (default desc = best first)
}

/// Ranked leaderboard: composite grade + sub-scores + verdict/attention flags.
pub async fn indexers(
    State(state): State<AppState>,
    Query(params): Query<IndexersParams>,
) -> Result<Json<Value>, StatusCode> {
    let window = params.window.unwrap_or(30);
    let limit = params.limit.unwrap_or(100).min(500);
    let offset = params.offset.unwrap_or(0);
    let asc = params.order.as_deref() == Some("asc");

    let sql = format!(
        r#"SELECT s.indexer_address, s.composite, s.grade, s.rated, s.correctness_score,
                  s.availability_score, s.freshness_score, s.coverage_score, s.value_score,
                  s.sybil_flag, s.sybil_cluster_id, s.probe_count, s.reasons,
                  p.ens_name, p.self_stake_grt, p.allocation_count, p.reo_status, p.qos_query_count,
                  (SELECT COUNT(*) FROM verdict v WHERE v.indexer_address = s.indexer_address)::int AS verdict_count,
                  EXISTS(SELECT 1 FROM attention_item a WHERE a.indexer_address = s.indexer_address) AS needs_attention
           FROM indexer_score s
           LEFT JOIN indexer_profile p ON p.indexer_address = s.indexer_address
           WHERE s.window_days = $1
           ORDER BY s.rated DESC, s.composite {} NULLS LAST
           LIMIT $2 OFFSET $3"#,
        if asc { "ASC" } else { "DESC" }
    );

    let rows = sqlx::query(&sql)
        .bind(window)
        .bind(limit)
        .bind(offset)
        .fetch_all(&state.pool)
        .await
        .map_err(|e| {
            tracing::error!(error = %e, "indexers query failed");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    let list: Vec<Value> = rows.iter().map(indexer_row_json).collect();
    Ok(Json(json!({ "window_days": window, "indexers": list, "count": list.len() })))
}

fn indexer_row_json(r: &sqlx::postgres::PgRow) -> Value {
    json!({
        "indexer_address": r.get::<String, _>("indexer_address"),
        "ens_name": r.get::<Option<String>, _>("ens_name"),
        "composite": r.get::<f64, _>("composite"),
        "grade": r.get::<String, _>("grade"),
        "rated": r.get::<bool, _>("rated"),
        "sub_scores": {
            "correctness": r.get::<Option<f64>, _>("correctness_score"),
            "availability": r.get::<Option<f64>, _>("availability_score"),
            "freshness": r.get::<Option<f64>, _>("freshness_score"),
            "coverage": r.get::<Option<f64>, _>("coverage_score"),
            "value": r.get::<Option<f64>, _>("value_score"),
        },
        "self_stake_grt": r.get::<Option<f64>, _>("self_stake_grt"),
        "allocation_count": r.get::<Option<i32>, _>("allocation_count"),
        "reo_status": r.get::<Option<String>, _>("reo_status"),
        "qos_query_count": r.get::<Option<i64>, _>("qos_query_count"),
        "probe_count": r.get::<i32, _>("probe_count"),
        "sybil_flag": r.get::<bool, _>("sybil_flag"),
        "sybil_cluster_id": r.get::<Option<String>, _>("sybil_cluster_id"),
        "verdict_count": r.get::<i32, _>("verdict_count"),
        "needs_attention": r.get::<bool, _>("needs_attention"),
        "reasons": r.get::<Value, _>("reasons"),
    })
}

/// Full scorecard for one indexer: all windows, verdicts, attention, sybil, profile.
pub async fn indexer_scorecard(
    State(state): State<AppState>,
    Path(address): Path<String>,
) -> Result<Json<Value>, StatusCode> {
    let address = address.to_lowercase();

    let scores = sqlx::query(
        r#"SELECT window_days, composite, grade, rated, correctness_score, availability_score,
                  freshness_score, coverage_score, value_score, sybil_flag, sybil_cluster_id,
                  probe_count, reasons, sub_scores, computed_at
           FROM indexer_score WHERE indexer_address = $1 ORDER BY window_days"#,
    )
    .bind(&address)
    .fetch_all(&state.pool)
    .await
    .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    if scores.is_empty() {
        return Err(StatusCode::NOT_FOUND);
    }

    let profile = sqlx::query(
        r#"SELECT ens_name, url, created_at, self_stake_grt, delegated_grt, allocation_count,
                  query_fees_collected_grt, reo_status, reo_source, lodestar_score, lodestar_grade,
                  qos_query_count, qos_success_rate, qos_latency_ms, qos_blocks_behind
           FROM indexer_profile WHERE indexer_address = $1"#,
    )
    .bind(&address)
    .fetch_optional(&state.pool)
    .await
    .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let verdicts = sqlx::query(
        "SELECT kind, severity, title, evidence, window_days, first_seen, last_seen
         FROM verdict WHERE indexer_address = $1 ORDER BY last_seen DESC",
    )
    .bind(&address)
    .fetch_all(&state.pool)
    .await
    .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let attention = sqlx::query(
        "SELECT kind, deployment_id, severity, urgency, title, detail, first_seen, last_seen
         FROM attention_item WHERE indexer_address = $1 ORDER BY urgency DESC",
    )
    .bind(&address)
    .fetch_all(&state.pool)
    .await
    .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let sybil = sqlx::query(
        r#"SELECT c.cluster_id, c.confidence, c.member_count, c.members, c.signals
           FROM sybil_cluster c
           WHERE c.members @> to_jsonb($1::text)"#,
    )
    .bind(&address)
    .fetch_optional(&state.pool)
    .await
    .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    Ok(Json(json!({
        "indexer_address": address,
        "profile": profile.as_ref().map(|p| json!({
            "ens_name": p.get::<Option<String>, _>("ens_name"),
            "url": p.get::<Option<String>, _>("url"),
            "created_at": p.get::<Option<i64>, _>("created_at"),
            "self_stake_grt": p.get::<Option<f64>, _>("self_stake_grt"),
            "delegated_grt": p.get::<Option<f64>, _>("delegated_grt"),
            "allocation_count": p.get::<Option<i32>, _>("allocation_count"),
            "query_fees_collected_grt": p.get::<Option<f64>, _>("query_fees_collected_grt"),
            "reo_status": p.get::<Option<String>, _>("reo_status"),
            "reo_source": p.get::<Option<String>, _>("reo_source"),
            "lodestar_score": p.get::<Option<f64>, _>("lodestar_score"),
            "lodestar_grade": p.get::<Option<String>, _>("lodestar_grade"),
            "qos": {
                "query_count": p.get::<Option<i64>, _>("qos_query_count"),
                "success_rate": p.get::<Option<f64>, _>("qos_success_rate"),
                "latency_ms": p.get::<Option<f64>, _>("qos_latency_ms"),
                "blocks_behind": p.get::<Option<f64>, _>("qos_blocks_behind"),
            },
        })),
        "scores": scores.iter().map(|s| json!({
            "window_days": s.get::<i32, _>("window_days"),
            "composite": s.get::<f64, _>("composite"),
            "grade": s.get::<String, _>("grade"),
            "rated": s.get::<bool, _>("rated"),
            "sub_scores": s.get::<Value, _>("sub_scores"),
            "probe_count": s.get::<i32, _>("probe_count"),
            "sybil_flag": s.get::<bool, _>("sybil_flag"),
            "reasons": s.get::<Value, _>("reasons"),
            "computed_at": s.get::<chrono::DateTime<chrono::Utc>, _>("computed_at"),
        })).collect::<Vec<_>>(),
        "verdicts": verdicts.iter().map(|v| json!({
            "kind": v.get::<String, _>("kind"),
            "severity": v.get::<String, _>("severity"),
            "title": v.get::<String, _>("title"),
            "evidence": v.get::<Value, _>("evidence"),
            "window_days": v.get::<Option<i32>, _>("window_days"),
            "first_seen": v.get::<chrono::DateTime<chrono::Utc>, _>("first_seen"),
            "last_seen": v.get::<chrono::DateTime<chrono::Utc>, _>("last_seen"),
        })).collect::<Vec<_>>(),
        "needs_attention": attention.iter().map(|a| json!({
            "kind": a.get::<String, _>("kind"),
            "deployment_id": a.get::<String, _>("deployment_id"),
            "severity": a.get::<String, _>("severity"),
            "urgency": a.get::<f64, _>("urgency"),
            "title": a.get::<String, _>("title"),
            "detail": a.get::<Value, _>("detail"),
            "first_seen": a.get::<chrono::DateTime<chrono::Utc>, _>("first_seen"),
            "last_seen": a.get::<chrono::DateTime<chrono::Utc>, _>("last_seen"),
        })).collect::<Vec<_>>(),
        "sybil_cluster": sybil.as_ref().map(|c| json!({
            "cluster_id": c.get::<String, _>("cluster_id"),
            "confidence": c.get::<f64, _>("confidence"),
            "member_count": c.get::<i32, _>("member_count"),
            "members": c.get::<Value, _>("members"),
            "signals": c.get::<Value, _>("signals"),
        })),
    })))
}

#[derive(Deserialize)]
pub struct AttentionParams {
    pub limit: Option<i64>,
    pub kind: Option<String>,
}

/// The "needs attention" triage surface — indexers serving bad/no data right now.
pub async fn needs_attention(
    State(state): State<AppState>,
    Query(params): Query<AttentionParams>,
) -> Result<Json<Value>, StatusCode> {
    let limit = params.limit.unwrap_or(100).min(500);

    let rows = if let Some(ref kind) = params.kind {
        sqlx::query(
            r#"SELECT a.indexer_address, a.kind, a.deployment_id, a.severity, a.urgency,
                      a.title, a.detail, a.first_seen, a.last_seen,
                      p.ens_name, p.self_stake_grt, p.reo_status
               FROM attention_item a
               LEFT JOIN indexer_profile p ON p.indexer_address = a.indexer_address
               WHERE a.kind = $1
               ORDER BY a.urgency DESC, a.last_seen DESC
               LIMIT $2"#,
        )
        .bind(kind)
        .bind(limit)
        .fetch_all(&state.pool)
        .await
    } else {
        sqlx::query(
            r#"SELECT a.indexer_address, a.kind, a.deployment_id, a.severity, a.urgency,
                      a.title, a.detail, a.first_seen, a.last_seen,
                      p.ens_name, p.self_stake_grt, p.reo_status
               FROM attention_item a
               LEFT JOIN indexer_profile p ON p.indexer_address = a.indexer_address
               ORDER BY a.urgency DESC, a.last_seen DESC
               LIMIT $1"#,
        )
        .bind(limit)
        .fetch_all(&state.pool)
        .await
    }
    .map_err(|e| {
        tracing::error!(error = %e, "needs_attention query failed");
        StatusCode::INTERNAL_SERVER_ERROR
    })?;

    let items: Vec<Value> = rows.iter().map(|a| json!({
        "indexer_address": a.get::<String, _>("indexer_address"),
        "ens_name": a.get::<Option<String>, _>("ens_name"),
        "self_stake_grt": a.get::<Option<f64>, _>("self_stake_grt"),
        "reo_status": a.get::<Option<String>, _>("reo_status"),
        "kind": a.get::<String, _>("kind"),
        "deployment_id": a.get::<String, _>("deployment_id"),
        "severity": a.get::<String, _>("severity"),
        "urgency": a.get::<f64, _>("urgency"),
        "title": a.get::<String, _>("title"),
        "detail": a.get::<Value, _>("detail"),
        "first_seen": a.get::<chrono::DateTime<chrono::Utc>, _>("first_seen"),
        "last_seen": a.get::<chrono::DateTime<chrono::Utc>, _>("last_seen"),
    })).collect();

    Ok(Json(json!({ "items": items, "count": items.len() })))
}

#[derive(Deserialize)]
pub struct VerdictsParams {
    pub limit: Option<i64>,
    pub kind: Option<String>,
    pub severity: Option<String>,
}

/// Feed of actionable verdicts across all indexers.
pub async fn verdicts(
    State(state): State<AppState>,
    Query(params): Query<VerdictsParams>,
) -> Result<Json<Value>, StatusCode> {
    let limit = params.limit.unwrap_or(100).min(500);

    // Optional kind/severity filters via COALESCE-style match (NULL = no filter).
    let rows = sqlx::query(
        r#"SELECT v.indexer_address, v.kind, v.severity, v.title, v.evidence,
                  v.window_days, v.first_seen, v.last_seen, p.ens_name
           FROM verdict v
           LEFT JOIN indexer_profile p ON p.indexer_address = v.indexer_address
           WHERE ($1::text IS NULL OR v.kind = $1)
             AND ($2::text IS NULL OR v.severity = $2)
           ORDER BY
             CASE v.severity WHEN 'critical' THEN 0 WHEN 'high' THEN 1 WHEN 'medium' THEN 2 ELSE 3 END,
             v.last_seen DESC
           LIMIT $3"#,
    )
    .bind(&params.kind)
    .bind(&params.severity)
    .bind(limit)
    .fetch_all(&state.pool)
    .await
    .map_err(|e| {
        tracing::error!(error = %e, "verdicts query failed");
        StatusCode::INTERNAL_SERVER_ERROR
    })?;

    let items: Vec<Value> = rows.iter().map(|v| json!({
        "indexer_address": v.get::<String, _>("indexer_address"),
        "ens_name": v.get::<Option<String>, _>("ens_name"),
        "kind": v.get::<String, _>("kind"),
        "severity": v.get::<String, _>("severity"),
        "title": v.get::<String, _>("title"),
        "evidence": v.get::<Value, _>("evidence"),
        "window_days": v.get::<Option<i32>, _>("window_days"),
        "first_seen": v.get::<chrono::DateTime<chrono::Utc>, _>("first_seen"),
        "last_seen": v.get::<chrono::DateTime<chrono::Utc>, _>("last_seen"),
    })).collect();

    Ok(Json(json!({ "verdicts": items, "count": items.len() })))
}

/// Per-indexer query success/lag for a single deployment (from the oracle's
/// allocation QoS) — so a subgraph page can show who's *serving* it, not just
/// who's synced. Catches "synced but 400ing on this subgraph".
pub async fn deployment_qos(
    State(state): State<AppState>,
    Path(deployment_id): Path<String>,
) -> Result<Json<Value>, StatusCode> {
    let rows = sqlx::query(
        r#"SELECT indexer_address, success_rate, blocks_behind, query_count
           FROM allocation_qos WHERE deployment_id = $1
           ORDER BY query_count DESC"#,
    )
    .bind(&deployment_id)
    .fetch_all(&state.pool)
    .await
    .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let indexers: Vec<Value> = rows.iter().map(|r| json!({
        "indexer_address": r.get::<String, _>("indexer_address"),
        "success_rate": r.get::<Option<f64>, _>("success_rate"),
        "blocks_behind": r.get::<Option<f64>, _>("blocks_behind"),
        "query_count": r.get::<Option<i64>, _>("query_count"),
    })).collect();

    // Foghorn's own measurements alongside the oracle's. A failure to read them must not take
    // the oracle view down with it — the entire point of this feed is that one source being
    // unavailable does not blank the page.
    let measured = measured_block(
        &state.pool,
        crate::qos::DailyFilter::for_deployment(&deployment_id),
    )
    .await;

    Ok(Json(json!({
        "deployment_id": deployment_id,
        // Legacy key, oracle-fed, unchanged for existing dashboard consumers.
        "indexers": indexers,
        "oracle": { "source": "edgeandnode-qos-oracle", "indexers": indexers },
        "measured": measured,
    })))
}

/// The `measured` half of a QoS response: Foghorn's own daily rollup plus its provenance.
///
/// Returns an `error` block rather than propagating, so a fault in the new feed can never take
/// down a response that would otherwise have served the oracle view perfectly well.
async fn measured_block(pool: &sqlx::PgPool, filter: crate::qos::DailyFilter) -> Value {
    match crate::qos::daily_points(pool, &filter).await {
        Ok(rows) => {
            let gateway_id = rows
                .first()
                .and_then(|r| r.get::<Option<String>, _>("gateway_id"));
            let points: Vec<Value> = rows.iter().map(crate::qos::row_to_oracle_json).collect();
            // 24h mix: this endpoint serves daily rollups, and the caveat should describe how the
            // data was actually gathered rather than restate a constant.
            let mix = crate::qos::dispatch_mix(pool, 24).await;
            let mut block = crate::qos::measured_provenance(gateway_id.as_deref(), Some(mix));
            block["allocationDailyDataPoints"] = json!(points);
            block
        }
        Err(e) => {
            tracing::warn!(error = %e, "measured QoS rollup query failed");
            json!({ "source": "foghorn", "error": "unavailable" })
        }
    }
}

/// Per-deployment query success/lag for one INDEXER (all its allocations) —
/// for the Active Allocations table on the indexer profile.
pub async fn indexer_allocations_qos(
    State(state): State<AppState>,
    Path(address): Path<String>,
) -> Result<Json<Value>, StatusCode> {
    let rows = sqlx::query(
        r#"SELECT deployment_id, success_rate, blocks_behind, query_count
           FROM allocation_qos WHERE indexer_address = $1
           ORDER BY query_count DESC"#,
    )
    .bind(address.to_lowercase())
    .fetch_all(&state.pool)
    .await
    .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let deployments: Vec<Value> = rows.iter().map(|r| json!({
        "deployment_id": r.get::<String, _>("deployment_id"),
        "success_rate": r.get::<Option<f64>, _>("success_rate"),
        "blocks_behind": r.get::<Option<f64>, _>("blocks_behind"),
        "query_count": r.get::<Option<i64>, _>("query_count"),
    })).collect();

    let measured = measured_block(
        &state.pool,
        crate::qos::DailyFilter::for_indexer(&address),
    )
    .await;

    Ok(Json(json!({
        "indexer_address": address.to_lowercase(),
        // Legacy key, oracle-fed, unchanged for existing dashboard consumers.
        "deployments": deployments,
        "oracle": { "source": "edgeandnode-qos-oracle", "deployments": deployments },
        "measured": measured,
    })))
}

/// Execute an oracle-compatible GraphQL query.
///
/// Deliberately not using `async-graphql-axum`: that crate's current release requires axum 0.8,
/// and upgrading would rewrite every `:param` route in this file for a wrapper that is two lines
/// of glue. `async_graphql::Request`/`Response` are already Deserialize/Serialize, which is the
/// entire integration.
pub async fn graphql_handler(
    State(state): State<AppState>,
    Json(req): Json<async_graphql::Request>,
) -> Json<async_graphql::Response> {
    Json(state.schema.execute(req).await)
}

/// GraphQL playground, so a consumer can paste its existing oracle query and see it answered.
pub async fn graphql_playground() -> axum::response::Html<String> {
    axum::response::Html(async_graphql::http::playground_source(
        async_graphql::http::GraphQLPlaygroundConfig::new("/v1/qos/graphql"),
    ))
}

/// Side-by-side freshness of both QoS oracles — the headline panel.
///
/// This is the whole argument in one response: how old is each feed. On 2026-07-29 Edge & Node's
/// went 35 hours without publishing while the Lodestar Oracle kept measuring, and a consumer had no
/// way to tell because a stale subgraph answers exactly like a fresh one. Serving each source's age
/// next to its data makes "is this current?" answerable without trusting anybody — including us.
///
/// Neither feed is canonical. Lodestar publishes what it measures; Edge & Node publish what they
/// measure; they measure different populations by different means. This endpoint reports the state
/// of both and ranks neither.
pub async fn qos_status(State(state): State<AppState>) -> Result<Json<Value>, StatusCode> {
    // The oracle's age is when its PUBLISHER last posted to the DataEdge on Gnosis — NOT
    // `max(allocation_qos.updated_at)`, which this endpoint originally used and which records
    // when Foghorn ingested. That version reported the oracle as 187 seconds old while it had
    // been dead for 37 hours, reproducing exactly the failure this page exists to expose. Our own
    // ingest clock cannot distinguish a fresh feed from a stale one; only the chain can.
    let (oracle_posted, oracle_bucket, oracle_lag): (
        Option<chrono::DateTime<chrono::Utc>>,
        Option<chrono::DateTime<chrono::Utc>>,
        Option<i32>,
    ) = sqlx::query_as(
        "SELECT max(posted_at),
                (SELECT bucket_ts   FROM oracle_message ORDER BY posted_at DESC LIMIT 1),
                (SELECT lag_seconds FROM oracle_message ORDER BY posted_at DESC LIMIT 1)
         FROM oracle_message",
    )
    .fetch_one(&state.pool)
    .await
    .unwrap_or((None, None, None));

    // Kept separately and clearly labelled: useful for spotting a broken ingest, useless for
    // judging the oracle.
    let ingested_at: Option<chrono::DateTime<chrono::Utc>> =
        sqlx::query_scalar("SELECT max(updated_at) FROM allocation_qos")
            .fetch_one(&state.pool)
            .await
            .unwrap_or(None);

    // How old the peer's newest DATA is — a different question from when its publisher last posted,
    // and the one that caught a 35-day hole. Read from `day_number` (their day index) rather than
    // our `updated_at`, which only says when we last re-fetched the same stale rows.
    // `day_start` is the peer's OWN timestamp for that day, stored at ingest. `day_number` is their
    // private day index and shares no epoch with unix time — deriving a date from it reported a
    // 35-day-old feed as 51 years old.
    let (peer_newest_day, peer_newest_day_start): (Option<i32>, Option<i64>) =
        sqlx::query_as("SELECT max(day_number), max(day_start) FROM allocation_qos")
            .fetch_one(&state.pool)
            .await
            .unwrap_or((None, None));
    // Age from the END of their newest day: a feed that published an hour ago is not a day stale.
    let peer_data_age_seconds = peer_newest_day_start
        .filter(|d| *d > 0)
        .map(|d| (chrono::Utc::now().timestamp() - (d + 86_400)).max(0));

    // Whether the deployment we read is actually accepting the publisher's messages. A subgraph can
    // sit at chain tip, report no indexing errors, and reject every post with "not a valid
    // submitter" — which is what the deployment usually called canonical has done since 2026-07-01.
    let peer_subgraph: Option<(
        Option<i64>,
        Option<bool>,
        Option<chrono::DateTime<chrono::Utc>>,
        Option<bool>,
        Option<String>,
    )> = sqlx::query_as(
        "SELECT indexed_block, has_indexing_errors, newest_message_at, newest_message_valid,
                newest_message_error
         FROM oracle_subgraph_health WHERE id = TRUE",
    )
    .fetch_optional(&state.pool)
    .await
    .unwrap_or(None);

    let (bucket, computed): (
        Option<chrono::DateTime<chrono::Utc>>,
        Option<chrono::DateTime<chrono::Utc>>,
    ) = sqlx::query_as("SELECT max(bucket_start), max(computed_at) FROM foghorn_qos")
        .fetch_one(&state.pool)
        .await
        .unwrap_or((None, None));

    // Freshness is time since the last MEASUREMENT, not since the current bucket's lower edge.
    //
    // Using `bucket_start` made a perfectly healthy feed look minutes stale by construction — a
    // 5-minute bucket is already 5 minutes "old" the instant before it closes — and after a restart
    // it read as 17 minutes while probes were landing 9 seconds earlier. Using `computed_at` would
    // be the opposite error: the rollup runs on a timer and would report freshness even if probing
    // had stopped entirely, which is precisely the "absent renders as healthy" failure this page
    // exists to complain about. The last probe that actually produced an observation is the only
    // measure that goes stale when, and only when, measurement genuinely stops.
    let last_measured: Option<chrono::DateTime<chrono::Utc>> = sqlx::query_scalar(
        "SELECT max(p.dispatched_at) FROM probe p
         WHERE EXISTS (SELECT 1 FROM observation o WHERE o.probe_id = p.id)",
    )
    .fetch_one(&state.pool)
    .await
    .unwrap_or(None);

    let now = chrono::Utc::now();
    let age = |t: Option<chrono::DateTime<chrono::Utc>>| {
        t.map(|t| (now - t).num_seconds()).map(Value::from).unwrap_or(Value::Null)
    };

    // Paid dispatch, reported as a fact about US.
    //
    // Payment refusals are excluded from the feed and the grades, because they describe our escrow
    // rather than an indexer. Excluded is not the same as hidden: dropping them without saying so
    // would leave a reader unable to tell "we measure 40 indexers directly" from "we tried and 38
    // turned our money away", which are very different claims about how good this oracle is.
    let (paid_ok, paid_denied, paid_refused): (i64, i64, i64) = sqlx::query_as(
        r#"SELECT
               COUNT(*) FILTER (WHERE o.error_class IS NULL AND o.response_hash IS NOT NULL)::bigint,
               COUNT(*) FILTER (WHERE o.error_class = 'payment_denylisted')::bigint,
               COUNT(*) FILTER (WHERE o.error_class = 'payment_refused')::bigint
           FROM observation o
           JOIN probe p ON p.id = o.probe_id
           WHERE p.dispatched_at >= NOW() - interval '24 hours'
             AND (o.error_class LIKE 'payment\_%' OR o.dispatch_mode = 'paid')"#,
    )
    .fetch_one(&state.pool)
    .await
    .unwrap_or((0, 0, 0));

    Ok(Json(json!({
        "checked_at": now.to_rfc3339(),
        "paid_dispatch": {
            "window_hours": 24,
            "served": paid_ok,
            "refused_denylisted": paid_denied,
            "refused_unfunded": paid_refused,
            "note": "Direct-to-indexer probes, paid with TAP receipts, where WE choose who answers \
                     rather than a gateway choosing for us. Refusals are OUR problem — an indexer's \
                     tap-agent has not yet observed our escrow deposit — and are excluded from every \
                     measurement and grade rather than counted against the operator.",
        },
        "sources": [
            {
                "source": "edgeandnode-qos-oracle",
                "last_update": oracle_posted.map(|t| t.to_rfc3339()),
                "age_seconds": age(oracle_posted),
                "measured_from": "DataEdge transactions on Gnosis — the publisher itself",
                // The bucket the last post described, and how far behind it was running. Lag is
                // the leading indicator: it went 30.3 → 47.7 minutes about 17 minutes before the
                // 2026-07-29 outage, while liveness still looked fine.
                "last_bucket_published": oracle_bucket.map(|t| t.to_rfc3339()),
                "publish_lag_seconds": oracle_lag,
                // Explicitly NOT the oracle's age. Ours.
                "lodestar_last_ingested_at": ingested_at.map(|t| t.to_rfc3339()),
                // Their newest published DATA, which is not the same question as whether their
                // publisher is alive, and can be over a month older. Read for comparison only —
                // Lodestar does not mirror or serve this feed.
                "data": {
                    "newest_day_number": peer_newest_day,
                    "newest_day_start": peer_newest_day_start,
                    "age_seconds": peer_data_age_seconds,
                },
                "subgraph": peer_subgraph.map(|(block, errs, msg_at, valid, err)| json!({
                    "indexed_block": block,
                    "has_indexing_errors": errs,
                    "newest_message_at": msg_at.map(|t| t.to_rfc3339()),
                    "newest_message_accepted": valid,
                    "rejection_reason": err,
                    "note": "A deployment can be at chain tip with no indexing errors and still turn \
                             every message away. If `newest_message_accepted` is false, its data \
                             stopped at that point however healthy it otherwise looks.",
                })),
                "note": "read from the chain it publishes to, so a stalled publisher cannot look fresh",
            },
            {
                "source": "lodestar-oracle",
                "gateway_id": "lodestar",
                // Age is time since the last probe that produced an observation — see the comment
                // above. `last_bucket` and `last_computed` are context, not the freshness measure.
                "age_seconds": age(last_measured),
                "last_measured": last_measured.map(|t| t.to_rfc3339()),
                // The cadence this feed is actually configured to run at, so a consumer can judge
                // staleness relative to it instead of against an assumed number. Probing hourly
                // means an age of 50 minutes is normal, not a fault.
                "expected_interval_seconds": state.probe_interval_secs,
                "last_bucket": bucket.map(|t| t.to_rfc3339()),
                "last_computed": computed.map(|t| t.to_rfc3339()),
                "note": "measured by Lodestar; no external publisher in the path",
            },
        ],
    })))
}

#[derive(Debug, Deserialize)]
pub struct QosBucketParams {
    pub indexer: Option<String>,
    pub deployment: Option<String>,
    pub hours: Option<i32>,
    pub limit: Option<i64>,
}

/// Agreement between the Lodestar Oracle and Edge & Node's.
///
/// This is the honesty check: if the two feeds disagree wildly, ours needs explaining before
/// anyone should rely on it. It is also the only place a *third* fact shows up — an indexer
/// returning fast, well-formed garbage scores a perfect 200-rate in the oracle and fails
/// Foghorn's correctness clustering. Those rows are the interesting output, not noise.
///
/// ## What is deliberately NOT compared
///
/// `query_count`. The oracle counts organic queries a gateway routed; Foghorn counts probes it
/// chose to send. A difference there is a category error, not a disagreement, so reporting a
/// delta on it would manufacture alarm out of nothing.
///
/// ## Why disagreement is not automatically our error
///
/// The two have different vantage points. The oracle sees real user traffic through one gateway;
/// Foghorn sees synthetic block-pinned probes from one location. Different query mixes, different
/// geography, wildly different sample sizes. `allocation_qos` also holds a trailing multi-day
/// window rather than per-day rows, so the comparison window is matched to it rather than to a
/// day boundary.
pub async fn qos_compare(
    State(state): State<AppState>,
    Query(params): Query<QosCompareParams>,
) -> Result<Json<Value>, StatusCode> {
    // Default matches ingest.rs's ALLOC_WINDOW_DAYS, so like is compared with like.
    let days: i32 = params.days.unwrap_or(3).clamp(1, 30);
    // A pair below this many probes is reported but excluded from the aggregate error, because a
    // success rate over three probes is not evidence of anything.
    let min_probes: i64 = params.min_probes.unwrap_or(20).max(1);

    let rows = sqlx::query(
        r#"
        WITH mine AS (
            SELECT
                indexer_address,
                deployment_id,
                sum(query_count)::bigint               AS probes,
                sum(num_indexer_200_responses)::double precision
                    / NULLIF(sum(query_count), 0)::double precision AS success_rate,
                avg(avg_indexer_blocks_behind)         AS blocks_behind,
                sum(comparable_count)::bigint          AS comparable,
                sum(divergent_count)::bigint           AS divergent
            FROM foghorn_qos
            WHERE bucket_start >= NOW() - make_interval(days => $1)
            GROUP BY 1, 2
        )
        SELECT
            m.indexer_address,
            m.deployment_id,
            m.probes,
            m.success_rate                AS mine_success_rate,
            m.blocks_behind               AS mine_blocks_behind,
            CASE WHEN m.comparable > 0
                 THEN 1.0 - (m.divergent::double precision / m.comparable::double precision)
                 ELSE NULL
            END                           AS mine_correctness_rate,
            m.comparable                  AS mine_comparable,
            o.success_rate                AS oracle_success_rate,
            o.blocks_behind               AS oracle_blocks_behind,
            o.query_count                 AS oracle_query_count
        FROM mine m
        -- INNER JOIN on purpose: a pair only one side has says nothing about agreement. Coverage
        -- gaps are a separate question, answered by the counts below rather than by null deltas.
        JOIN allocation_qos o
          ON o.indexer_address = m.indexer_address
         AND o.deployment_id   = m.deployment_id
        ORDER BY abs(COALESCE(m.success_rate, 0) - COALESCE(o.success_rate, 0)) DESC
        LIMIT 2000
        "#,
    )
    .bind(days)
    .fetch_all(&state.pool)
    .await
    .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let mut pairs: Vec<Value> = Vec::with_capacity(rows.len());
    let mut abs_err_sum = 0.0f64;
    let mut abs_err_n = 0u64;
    let mut disagree_10 = 0u64;
    // Indexers the oracle scores well but Foghorn caught serving wrong data. The reason this
    // whole comparison is worth publishing.
    let mut oracle_blind = 0u64;

    for r in &rows {
        let probes: i64 = r.get("probes");
        let mine_sr: Option<f64> = r.get("mine_success_rate");
        let oracle_sr: Option<f64> = r.get("oracle_success_rate");
        let mine_correctness: Option<f64> = r.get("mine_correctness_rate");

        let delta = match (mine_sr, oracle_sr) {
            (Some(a), Some(b)) => Some(a - b),
            _ => None,
        };

        let counted = probes >= min_probes && delta.is_some();
        if let (true, Some(d)) = (counted, delta) {
            abs_err_sum += d.abs();
            abs_err_n += 1;
            if d.abs() >= 0.10 {
                disagree_10 += 1;
            }
        }

        // "The oracle is happy and we are not": high 200-rate over there, wrong data over here.
        //
        // Gated on the SAME evidence threshold as the aggregate. Publishing this without one named
        // an indexer as serving wrong data on 7 probes, while the panel beside it declared that
        // fewer than 20 probes is not evidence. A public accusation deserves at least the standard
        // applied to a summary statistic.
        let comparable: Option<i64> = r.try_get("mine_comparable").ok().flatten();
        let blind = counted
            && comparable.unwrap_or(0) >= min_probes
            && oracle_sr.unwrap_or(0.0) >= 0.99
            && mine_correctness.is_some_and(|c| c < 1.0);
        if blind {
            oracle_blind += 1;
        }

        pairs.push(json!({
            "indexer_address": r.get::<String, _>("indexer_address"),
            "deployment_id": r.get::<String, _>("deployment_id"),
            "probes": probes,
            "counted_in_aggregate": counted,
            "foghorn": {
                "success_rate": mine_sr,
                "blocks_behind": r.get::<Option<f64>, _>("mine_blocks_behind"),
                "correctness_rate": mine_correctness,
                // The evidence behind the ratio. A correctness figure over one or two comparisons
                // is not a finding, and hiding the denominator invites it being read as one.
                "comparable_responses": comparable,
            },
            "oracle": {
                "success_rate": oracle_sr,
                "blocks_behind": r.get::<Option<f64>, _>("oracle_blocks_behind"),
                // Present for context only. Not comparable with our probe count.
                "query_count": r.get::<Option<i64>, _>("oracle_query_count"),
            },
            "success_rate_delta": delta,
            "oracle_blind_spot": blind,
        }));
    }

    let overlap = rows.len() as u64;
    let mine_total: i64 = sqlx::query_scalar(
        "SELECT count(DISTINCT (indexer_address, deployment_id)) FROM foghorn_qos
         WHERE bucket_start >= NOW() - make_interval(days => $1)",
    )
    .bind(days)
    .fetch_one(&state.pool)
    .await
    .unwrap_or(0);
    let oracle_total: i64 = sqlx::query_scalar("SELECT count(*) FROM allocation_qos")
        .fetch_one(&state.pool)
        .await
        .unwrap_or(0);

    Ok(Json(json!({
        "window_days": days,
        "min_probes_for_aggregate": min_probes,
        "coverage": {
            "overlapping_pairs": overlap,
            "foghorn_pairs": mine_total,
            "oracle_pairs": oracle_total,
            "note": "Coverage differs because the oracle sees whatever its gateway routed while \
                     Foghorn sees whatever it probed. Neither is a subset of the other.",
        },
        "agreement": {
            "pairs_in_aggregate": abs_err_n,
            "mean_absolute_success_rate_error": if abs_err_n > 0 {
                Some(abs_err_sum / abs_err_n as f64)
            } else { None },
            "pairs_disagreeing_over_10pct": disagree_10,
            "oracle_blind_spots": oracle_blind,
            "oracle_blind_spot_means": "oracle success rate >= 99% while Foghorn measured \
                                        incorrect data — served fast, well-formed, and wrong",
        },
        "not_compared": {
            "query_count": "probes dispatched vs organic queries routed — a category error, not a \
                            disagreement",
        },
        "pairs": pairs,
    })))
}

#[derive(Debug, Deserialize)]
pub struct QosCompareParams {
    pub days: Option<i32>,
    pub min_probes: Option<i64>,
}

/// Bucket-resolution measured QoS — the only place latency percentiles are served.
///
/// Percentiles do not recombine, so the daily rollup deliberately omits them (see
/// `crate::qos`). Here they are as computed, at the resolution they were computed at.
pub async fn qos_buckets(
    State(state): State<AppState>,
    Query(params): Query<QosBucketParams>,
) -> Result<Json<Value>, StatusCode> {
    let indexer = params.indexer.map(|s| s.to_lowercase());
    let deployment = params.deployment;
    // Clamped rather than rejected: a caller asking for a decade of buckets gets a month, not a
    // 400. The row cap is what actually protects the database.
    let hours: i32 = params.hours.unwrap_or(24).clamp(1, 720);
    let limit: i64 = params.limit.unwrap_or(500).clamp(1, 5000);

    let rows = sqlx::query(
        r#"SELECT indexer_address, deployment_id, bucket_start, bucket_secs, gateway_id, chain_id,
                  query_count, num_indexer_200_responses, proportion_indexer_200_responses,
                  avg_indexer_latency_ms, max_indexer_latency_ms, stdev_indexer_latency_ms,
                  latency_p50_ms, latency_p95_ms, latency_p99_ms,
                  avg_indexer_blocks_behind, max_indexer_blocks_behind,
                  comparable_count, divergent_count, correctness_rate
           FROM foghorn_qos
           WHERE ($1::text IS NULL OR indexer_address = $1)
             AND ($2::text IS NULL OR deployment_id   = $2)
             AND bucket_start >= NOW() - make_interval(hours => $3)
           ORDER BY bucket_start DESC
           LIMIT $4"#,
    )
    .bind(&indexer)
    .bind(&deployment)
    .bind(hours)
    .bind(limit)
    .fetch_all(&state.pool)
    .await
    .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let buckets: Vec<Value> = rows.iter().map(|r| json!({
        "indexer_wallet": r.get::<String, _>("indexer_address"),
        "subgraph_deployment_ipfs_hash": r.get::<String, _>("deployment_id"),
        "bucket_start": r.get::<chrono::DateTime<chrono::Utc>, _>("bucket_start").to_rfc3339(),
        "bucket_secs": r.get::<i32, _>("bucket_secs"),
        "gateway_id": r.get::<Option<String>, _>("gateway_id"),
        "chain_id": r.get::<Option<String>, _>("chain_id"),
        "query_count": r.get::<i64, _>("query_count"),
        "num_indexer_200_responses": r.get::<i64, _>("num_indexer_200_responses"),
        "proportion_indexer_200_responses": r.get::<f64, _>("proportion_indexer_200_responses"),
        "avg_indexer_latency_ms": r.get::<Option<f64>, _>("avg_indexer_latency_ms"),
        "max_indexer_latency_ms": r.get::<Option<f64>, _>("max_indexer_latency_ms"),
        "stdev_indexer_latency_ms": r.get::<Option<f64>, _>("stdev_indexer_latency_ms"),
        "latency_p50_ms": r.get::<Option<i32>, _>("latency_p50_ms"),
        "latency_p95_ms": r.get::<Option<i32>, _>("latency_p95_ms"),
        "latency_p99_ms": r.get::<Option<i32>, _>("latency_p99_ms"),
        "avg_indexer_blocks_behind": r.get::<Option<f64>, _>("avg_indexer_blocks_behind"),
        "max_indexer_blocks_behind": r.get::<Option<f64>, _>("max_indexer_blocks_behind"),
        "comparable_count": r.get::<i64, _>("comparable_count"),
        "divergent_count": r.get::<i64, _>("divergent_count"),
        "correctness_rate": r.get::<Option<f64>, _>("correctness_rate"),
    })).collect();

    // Mix measured over the SAME window the buckets cover, so the caveat and the data agree.
    let mix = crate::qos::dispatch_mix(&state.pool, hours).await;
    let mut out = crate::qos::measured_provenance(None, Some(mix));
    out["window_hours"] = json!(hours);
    out["buckets"] = json!(buckets);
    Ok(Json(out))
}

/// Deployments flagged as non-deterministic (diverge every round — subgraph's fault).
pub async fn nondeterministic(State(state): State<AppState>) -> Result<Json<Value>, StatusCode> {
    let rows = sqlx::query(
        r#"SELECT deployment_id, divergent_probes, total_probes, divergence_rate,
                  sample_fields, first_seen, last_seen
           FROM nondeterministic_deployment
           ORDER BY divergence_rate DESC, divergent_probes DESC"#,
    )
    .fetch_all(&state.pool)
    .await
    .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let items: Vec<Value> = rows.iter().map(|r| json!({
        "deployment_id": r.get::<String, _>("deployment_id"),
        "divergent_probes": r.get::<i32, _>("divergent_probes"),
        "total_probes": r.get::<i32, _>("total_probes"),
        "divergence_rate": r.get::<f64, _>("divergence_rate"),
        "sample_fields": r.get::<Value, _>("sample_fields"),
        "first_seen": r.get::<chrono::DateTime<chrono::Utc>, _>("first_seen"),
        "last_seen": r.get::<chrono::DateTime<chrono::Utc>, _>("last_seen"),
    })).collect();

    Ok(Json(json!({ "deployments": items, "count": items.len() })))
}

/// Detected operator-swarm clusters.
pub async fn sybil_clusters(State(state): State<AppState>) -> Result<Json<Value>, StatusCode> {
    let rows = sqlx::query(
        r#"SELECT cluster_id, confidence, member_count, members, signals, detected_at
           FROM sybil_cluster ORDER BY confidence DESC, member_count DESC"#,
    )
    .fetch_all(&state.pool)
    .await
    .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let clusters: Vec<Value> = rows.iter().map(|c| json!({
        "cluster_id": c.get::<String, _>("cluster_id"),
        "confidence": c.get::<f64, _>("confidence"),
        "member_count": c.get::<i32, _>("member_count"),
        "members": c.get::<Value, _>("members"),
        "signals": c.get::<Value, _>("signals"),
        "detected_at": c.get::<chrono::DateTime<chrono::Utc>, _>("detected_at"),
    })).collect();

    Ok(Json(json!({ "clusters": clusters, "count": clusters.len() })))
}
