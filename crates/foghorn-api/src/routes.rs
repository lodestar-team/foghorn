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

    let total_divergences: i64 = sqlx::query("SELECT COUNT(*) FROM divergence")
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
        "SELECT COUNT(*) FROM divergence WHERE created_at > NOW() - INTERVAL '24 hours'",
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
               WHERE p.deployment_id = $1
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
             COUNT(DISTINCT CASE WHEN d.probe_id IS NOT NULL THEN o.probe_id END) as divergent_probes,
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
             COUNT(DISTINCT CASE WHEN d.probe_id IS NOT NULL THEN o.probe_id END) as divergent_probes
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
             COUNT(DISTINCT CASE WHEN d.probe_id IS NOT NULL THEN o.probe_id END) as divergent_probes,
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
