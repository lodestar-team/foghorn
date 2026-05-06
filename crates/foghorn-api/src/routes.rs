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
             COUNT(DISTINCT o.probe_id) as total_probes,
             COUNT(DISTINCT CASE WHEN d.probe_id IS NOT NULL THEN o.probe_id END) as divergent_probes,
             ROUND(AVG(o.latency_ms))::int as avg_latency_ms,
             MAX(p.dispatched_at) as last_seen
           FROM observation o
           JOIN probe p ON p.id = o.probe_id
           LEFT JOIN divergence d ON d.probe_id = o.probe_id
           WHERE p.deployment_id = $1
             AND p.dispatched_at > NOW() - $2::interval
           GROUP BY o.indexer_address
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
