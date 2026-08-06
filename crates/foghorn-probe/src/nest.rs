//! Reading the allocation set and indexer endpoints from our own nuthatch nest.
//!
//! Until now `allocations.rs` fetched both from the network subgraph **through Edge & Node's
//! gateway, with their API key**. That is the critical path for paid probing: no allocation refresh
//! means no `collection_id` to bill, which means direct probing stops. An oracle whose independence
//! rests on someone else's API key is not independent, and GRC-009 claims otherwise on our behalf.
//!
//! Everything that path provided is on Arbitrum as events, and `nightswatchhq/horizon-nest` indexes
//! them: `AllocationCreated/Resized/Closed` on the SubgraphService for the allocation set, and
//! `ServiceProviderRegistered` for the endpoints.
//!
//! ## Why this does not simply replace the gateway path
//!
//! A nest that is still backfilling answers queries perfectly well and returns a *partial*
//! allocation set. Swapping that in wholesale would silently shrink the probe target list — every
//! indexer the nest has not reached yet would look like it has no allocations, and would quietly
//! stop being measured. That is the same failure this whole project exists to catch, so
//! [`NestClient::allocations`] refuses to answer at all until the nest says it is current.

use anyhow::{bail, Context, Result};
use foghorn_core::deployment::normalise_deployment_id;
use serde::Deserialize;
use std::time::Duration;

/// How far behind chain tip the nest may be and still be trusted for a full replacement.
///
/// Arbitrum blocks are sub-second, so a few thousand blocks is seconds of lag, not staleness. The
/// point of the check is to reject a nest that is mid-backfill (tens of millions of blocks behind),
/// not to demand it be exactly at tip.
const MAX_LAG_BLOCKS: u64 = 10_000;

pub struct NestClient {
    http: reqwest::Client,
    base_url: String,
    basic_auth: Option<(String, String)>,
}

/// Every nest response carries this. `sealed_through` is the block the nest has actually committed,
/// which is a different question from whether the process is running — the distinction this oracle
/// keeps insisting on, handed to us for free by the indexer.
#[derive(Debug, Clone, Deserialize)]
pub struct Provenance {
    pub sealed_through: Option<u64>,
    #[serde(default)]
    pub as_of: Option<u64>,
}

#[derive(Debug, Deserialize)]
struct SqlResponse<T> {
    rows: Vec<T>,
    provenance: Provenance,
    #[serde(default)]
    truncated: bool,
}

#[derive(Debug, Clone, Deserialize)]
pub struct NestAllocation {
    #[serde(rename = "allocationId")]
    pub allocation_id: String,
    pub indexer: String,
    /// As the chain stores it: a bytes32 id, NOT the `Qm…` IPFS hash. Normalised on the way out of
    /// [`NestClient::allocations`] - see the note there, this one bites silently.
    #[serde(rename = "subgraphDeploymentId")]
    pub deployment_id: String,
    /// A big decimal, arriving as a JSON **string**. Declaring it `f64` made every response
    /// "unparseable JSON" and sent the sync quietly back to the gateway, which is a fallback doing
    /// its job and hiding a bug while it does.
    #[serde(default)]
    pub tokens: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
struct NestEndpointRow {
    indexer: String,
    data: String,
}

impl NestClient {
    pub fn new(base_url: &str, basic_auth: Option<(String, String)>) -> Result<Self> {
        Ok(Self {
            http: reqwest::Client::builder()
                .timeout(Duration::from_secs(60))
                .build()?,
            base_url: base_url.trim_end_matches('/').to_string(),
            basic_auth,
        })
    }

    async fn sql<T: for<'de> Deserialize<'de>>(&self, query: &str) -> Result<(Vec<T>, Provenance)> {
        let mut req = self
            .http
            .get(format!("{}/sql", self.base_url))
            .query(&[("q", query)]);
        if let Some((u, p)) = &self.basic_auth {
            req = req.basic_auth(u, Some(p));
        }
        let resp = req.send().await.context("nest request failed")?;
        if !resp.status().is_success() {
            bail!("nest returned HTTP {}", resp.status());
        }
        let body: SqlResponse<T> = resp.json().await.context("nest returned unparseable JSON")?;
        // Truncation would silently shorten the allocation set, which is precisely the failure this
        // module is written to avoid. Refuse rather than return a short list that looks complete.
        if body.truncated {
            bail!("nest truncated the result — raise the row limit rather than trusting this");
        }
        Ok((body.rows, body.provenance))
    }

    /// Chain tip as the nest sees it, versus what it has actually sealed.
    pub async fn lag_blocks(&self, chain_tip: u64) -> Result<u64> {
        let (_rows, prov) = self.sql::<serde_json::Value>("SELECT 1").await?;
        let sealed = prov
            .sealed_through
            .context("nest reported no sealed_through — cannot judge how current it is")?;
        Ok(chain_tip.saturating_sub(sealed))
    }

    /// The active allocation set, or an error if the nest is not current enough to be trusted.
    ///
    /// Deliberately all-or-nothing. A partial answer here does not degrade the oracle gracefully —
    /// it removes indexers from the probe set without anything looking wrong.
    pub async fn allocations(&self, chain_tip: u64) -> Result<Vec<NestAllocation>> {
        let lag = self.lag_blocks(chain_tip).await?;
        if lag > MAX_LAG_BLOCKS {
            bail!(
                "nest is {lag} blocks behind tip (limit {MAX_LAG_BLOCKS}) — still backfilling, so \
                 its allocation set is partial and using it would silently shrink the probe target \
                 list"
            );
        }
        let (rows, _) = self
            .sql::<NestAllocation>(
                "SELECT \"allocationId\", \"indexer\", \"subgraphDeploymentId\", tokens \
                 FROM allocations WHERE status = 'active'",
            )
            .await?;
        if rows.is_empty() {
            bail!("nest returned no active allocations — refusing to clear the table");
        }

        // Normalise deployment ids to their `Qm…` form.
        //
        // The chain stores bytes32; everything else in Foghorn - `foghorn_qos`, the test-sets, the
        // served `subgraph_deployment_ipfs_hash` field - speaks IPFS hashes. Leaving these as hex
        // would mean `payable_targets_for_deployment` matched nothing for every deployment, so paid
        // probing would find no targets and simply stop, with no error anywhere. The same two forms
        // in one column already cost us a day; they do not get a second go.
        let rows = rows
            .into_iter()
            .map(|mut a| {
                a.deployment_id = normalise_deployment_id(&a.deployment_id);
                a
            })
            .collect();
        Ok(rows)
    }

    /// Indexer service endpoints, decoded from their on-chain registrations.
    ///
    /// Returns (indexer, url) for every indexer whose latest registration decodes. A registration
    /// that does not decode is skipped with a warning rather than failing the batch: one malformed
    /// blob should not cost us every other operator's endpoint.
    pub async fn endpoints(&self) -> Result<Vec<(String, String)>> {
        let (rows, _) = self
            .sql::<NestEndpointRow>("SELECT indexer, data FROM service_endpoints")
            .await?;
        let mut out = Vec::with_capacity(rows.len());
        for r in rows {
            match decode_service_url(&r.data) {
                Some(url) if !url.is_empty() => out.push((r.indexer.to_lowercase(), url)),
                _ => tracing::warn!(indexer = %r.indexer, "service registration did not decode"),
            }
        }
        Ok(out)
    }
}

/// Pull the service URL out of a `ServiceProviderRegistered` payload.
///
/// The blob is `abi.encode(string url, string geohash, …)`: word 0 is the byte offset of `url`,
/// word 1 the offset of `geohash`; at each offset sits a length word followed by UTF-8 bytes.
/// Verified against live registrations rather than inferred from the interface — see the tests.
///
/// Returns None rather than panicking on anything malformed. This decodes third-party data that
/// decides where we send paid queries, so a bad length word must not become a slice out of bounds.
pub fn decode_service_url(data_hex: &str) -> Option<String> {
    let hex = data_hex.strip_prefix("0x").unwrap_or(data_hex);
    let bytes = hex::decode(hex).ok()?;

    let word = |at: usize| -> Option<usize> {
        let w = bytes.get(at..at + 32)?;
        // Offsets and lengths beyond usize are nonsense here; treat the high 24 bytes as a
        // must-be-zero guard so a hostile value cannot wrap into a plausible index.
        if w[..24].iter().any(|b| *b != 0) {
            return None;
        }
        let mut buf = [0u8; 8];
        buf.copy_from_slice(&w[24..32]);
        Some(u64::from_be_bytes(buf) as usize)
    };

    let url_off = word(0)?;
    let url_len = word(url_off)?;
    let start = url_off.checked_add(32)?;
    let end = start.checked_add(url_len)?;
    let raw = bytes.get(start..end)?;
    Some(String::from_utf8_lossy(raw).trim().to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A real payload, captured verbatim from the running nest on 2026-08-06.
    ///
    /// Deliberately NOT one this test encoded itself. An earlier version of this test built the
    /// payload using the same layout assumptions the decoder uses, so it proved only that the
    /// function reverses itself - it would have passed just as happily with the layout wrong.
    /// The hex below is what indexer 0x0874e792... actually published on Arbitrum, and the
    /// expected URL is what they actually serve from.
    const LIVE_REGISTRATION: &str = "0x000000000000000000000000000000000000000000000000000000000000006000000000000000000000000000000000000000000000000000000000000000c00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000002168747470733a2f2f696e6465782e7765623376616c696461746f722e696e666f2f0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000097673333667747730670000000000000000000000000000000000000000000000";

    #[test]
    fn decodes_a_real_registration() {
        assert_eq!(
            decode_service_url(LIVE_REGISTRATION).as_deref(),
            Some("https://index.web3validator.info/")
        );
    }

    /// Malformed input must return None, never panic. This decodes data an arbitrary operator put
    /// on chain, and it chooses where we send paid queries.
    #[test]
    fn refuses_malformed_payloads_without_panicking() {
        assert_eq!(decode_service_url(""), None);
        assert_eq!(decode_service_url("0x"), None);
        assert_eq!(decode_service_url("0xzzzz"), None);
        // Offset past the end of the blob.
        let mut b = vec![0u8; 64];
        b[31] = 0xff;
        assert_eq!(decode_service_url(&hex::encode(&b)), None);
        // A length word claiming more bytes than exist.
        let mut b = vec![0u8; 96];
        b[31] = 64; // url at byte 64
        b[64 + 31] = 0xff; // length 255, but only 0 bytes follow
        assert_eq!(decode_service_url(&hex::encode(&b)), None);
    }

    /// A length whose high bytes are set must be rejected, not truncated into something plausible.
    #[test]
    fn rejects_oversized_words_rather_than_wrapping() {
        let mut b = vec![0u8; 96];
        b[0] = 0x01; // url offset with a high byte set
        assert_eq!(decode_service_url(&hex::encode(&b)), None);
    }

}
