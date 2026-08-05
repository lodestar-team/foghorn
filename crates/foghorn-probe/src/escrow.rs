//! Which indexers we can actually pay.
//!
//! Escrow is keyed on (payer, collector, receiver) on-chain, so paying an indexer we have not
//! funded is a guaranteed refusal. Discovering that per query would cost traffic and, worse, record
//! a failure that describes our funding rather than the indexer's health — the same category error
//! as reading probe volume as demand.
//!
//! Read directly over JSON-RPC rather than through a subgraph: this is one `eth_call` per indexer
//! against a value we must not be wrong about, and putting an indexing layer in front of it would
//! reintroduce exactly the staleness this project exists to catch.

use anyhow::{Context, Result};
use foghorn_core::config::TapConfig;
use serde_json::json;
use sqlx::PgPool;
use std::time::Duration;
use tracing::{info, warn};

/// `getBalance(address,address,address)` — verified with `cast sig`.
const GET_BALANCE_SELECTOR: &str = "d6bd603c";

pub async fn run_escrow_sync_loop(cfg: TapConfig, rpc_url: String, pool: PgPool) {
    if !cfg.enabled {
        info!("TAP disabled — escrow sync not starting");
        return;
    }
    info!(interval = cfg.escrow_sync_secs, "Escrow sync starting");
    loop {
        match sync_once(&cfg, &rpc_url, &pool).await {
            Ok((funded, total)) => info!(funded, checked = total, "Escrow balances refreshed"),
            Err(e) => warn!(error = %e, "Escrow sync failed"),
        }
        tokio::time::sleep(Duration::from_secs(cfg.escrow_sync_secs)).await;
    }
}

/// Refresh escrow balances for every indexer we currently hold an active allocation for.
///
/// Returns (indexers with a positive balance, indexers checked).
pub async fn sync_once(cfg: &TapConfig, rpc_url: &str, pool: &PgPool) -> Result<(usize, usize)> {
    let indexers: Vec<String> = sqlx::query_scalar(
        "SELECT DISTINCT indexer_address FROM active_allocation WHERE indexer_url IS NOT NULL",
    )
    .fetch_all(pool)
    .await?;

    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(20))
        .build()?;

    let mut funded = 0usize;
    for indexer in &indexers {
        // Excluded operators are skipped before we spend a call on them. Their escrow may well be
        // funded — p2p's is — but we are never going to probe them, so the balance is irrelevant
        // and recording it would put them back in the payable set.
        if cfg
            .excluded_indexers
            .iter()
            .any(|e| e.eq_ignore_ascii_case(indexer))
        {
            continue;
        }

        let balance = match read_balance(&client, rpc_url, cfg, indexer).await {
            Ok(b) => b,
            Err(e) => {
                warn!(indexer = %indexer, error = %e, "escrow balance read failed");
                continue;
            }
        };
        if balance > 0.0 {
            funded += 1;
        }
        sqlx::query(
            r#"INSERT INTO tap_escrow (indexer_address, balance_wei, checked_at)
               VALUES ($1, CAST($2 AS numeric), NOW())
               ON CONFLICT (indexer_address) DO UPDATE SET
                   balance_wei = EXCLUDED.balance_wei,
                   checked_at  = NOW()"#,
        )
        .bind(indexer)
        .bind(format!("{balance:.0}"))
        .execute(pool)
        .await?;
    }

    Ok((funded, indexers.len()))
}

/// One `eth_call` to PaymentsEscrow.getBalance(payer, collector, receiver).
///
/// Returned as f64 only because it feeds a `> 0` decision and a NUMERIC column; nothing here does
/// arithmetic on it, so the precision loss at 1e18 scale is immaterial.
async fn read_balance(
    client: &reqwest::Client,
    rpc_url: &str,
    cfg: &TapConfig,
    receiver: &str,
) -> Result<f64> {
    let pad = |a: &str| format!("{:0>64}", a.trim_start_matches("0x").to_lowercase());
    let data = format!(
        "0x{}{}{}{}",
        GET_BALANCE_SELECTOR,
        pad(&cfg.payer),
        pad(&cfg.verifier),
        pad(receiver)
    );

    let body = json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "eth_call",
        "params": [{ "to": cfg.escrow, "data": data }, "latest"]
    });

    let v: serde_json::Value = client
        .post(rpc_url)
        .json(&body)
        .send()
        .await
        .context("escrow eth_call failed")?
        .json()
        .await
        .context("escrow rpc returned unparseable JSON")?;

    if let Some(err) = v.get("error") {
        anyhow::bail!("escrow eth_call error: {err}");
    }
    let hex = v
        .get("result")
        .and_then(|r| r.as_str())
        .context("escrow eth_call returned no result")?
        .trim_start_matches("0x");

    // A zero-length result means the call hit an address with no code, which is a configuration
    // error rather than an empty account, and must not be reported as a zero balance.
    if hex.is_empty() {
        anyhow::bail!("escrow returned empty data — check the escrow address");
    }
    Ok(u128::from_str_radix(hex, 16).unwrap_or(0) as f64)
}
