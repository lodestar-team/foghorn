//! Ingesting realised query fees from our own nest.
//!
//! `QueryFeesCollected` fires when an indexer actually collects for queries it served. That makes it
//! the one economic signal this oracle can report without anybody self-reporting it — and the half
//! GRC-009 said active probing cannot produce. A probe knows what a query cost us; it can never know
//! what the network paid an indexer for serving real users.
//!
//! Kept strictly apart from `foghorn_qos`. See `migrations/022_chain_query_fees.sql` for why: our
//! buckets count probes, so writing network settlement into their fee fields would credit our
//! synthetic traffic with money an indexer earned from somebody else.

use anyhow::Result;
use sqlx::PgPool;
use std::time::Duration;
use tracing::{info, warn};

use crate::nest::NestClient;

/// Rows per pass. Settlement history is large (63k events and counting), so the first few passes
/// walk it in chunks rather than trying to hold it all at once.
const PAGE: i64 = 5_000;

pub async fn run_fee_ingest_loop(nest: NestClient, interval_secs: u64, pool: PgPool) {
    info!(interval = interval_secs, "Chain query-fee ingest starting");
    loop {
        match ingest_once(&nest, &pool).await {
            Ok(0) => {}
            Ok(n) => info!(rows = n, "Chain query fees ingested"),
            Err(e) => warn!(error = %e, "Chain query-fee ingest failed"),
        }
        tokio::time::sleep(Duration::from_secs(interval_secs)).await;
    }
}

/// One incremental pass. Returns rows written.
pub async fn ingest_once(nest: &NestClient, pool: &PgPool) -> Result<usize> {
    // Resume from the highest block already stored. The primary key is (block_number, log_index),
    // so a re-run overlapping the boundary is harmless rather than duplicating.
    let since: i64 = sqlx::query_scalar("SELECT COALESCE(MAX(block_number), 0) FROM chain_query_fees")
        .fetch_one(pool)
        .await
        .unwrap_or(0);

    let fees = nest.query_fees(since, PAGE).await?;
    if fees.is_empty() {
        return Ok(0);
    }

    let mut written = 0usize;
    let mut tx = pool.begin().await?;
    for f in &fees {
        // `to_timestamp` rather than a Rust conversion: block_timestamp is unix seconds and letting
        // Postgres own the conversion keeps the column's meaning in one place.
        sqlx::query(
            r#"INSERT INTO chain_query_fees
                   (indexer_address, deployment_id, allocation_id, payer,
                    tokens_collected, tokens_curators, block_number, block_timestamp, log_index)
               VALUES ($1, $2, $3, $4, CAST($5 AS numeric), CAST($6 AS numeric), $7, to_timestamp($8), $9)
               ON CONFLICT (block_number, log_index) DO NOTHING"#,
        )
        .bind(&f.indexer)
        .bind(&f.deployment_id)
        .bind(&f.allocation_id.to_lowercase())
        .bind(&f.payer.to_lowercase())
        .bind(&f.tokens_collected)
        .bind(f.tokens_curators.as_deref())
        .bind(f.block_number)
        .bind(f.block_timestamp)
        .bind(f.log_index)
        .execute(&mut *tx)
        .await?;
        written += 1;
    }
    tx.commit().await?;
    Ok(written)
}
