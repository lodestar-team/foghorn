use foghorn_core::normalize::normalize_and_hash;
use k256::ecdsa::{RecoveryId, Signature, VerifyingKey};
use sha3::{Digest, Keccak256};
use std::time::{Duration, Instant};
use tracing::{debug, warn};

// Pre-computed EIP-712 constants for The Graph attestation scheme.
// Domain: name="Graph Protocol", version="0", chainId=42161 (Arbitrum),
//         verifyingContract=DisputeManager (0x2fe023a575449acb698648ed21276293fa176f96)
const RECEIPT_TYPEHASH: [u8; 32] =
    hex_bytes("32dd026408194a0d7e54cc66a2ab6c856efc55cfcd4dd258fde5b1a55222baa6");
// EIP-712 domain separator for The Graph attestations on Arbitrum One:
//   name="Graph Protocol", version="0", chainId=42161,
//   verifyingContract=DisputeManager (0x2fe023a575449acb698648ed21276293fa176f96),
//   salt=0xa070ffb1cd7409649bf77822cce74495468e06dbfaef09556838bf188679b9c2.
// Empirically verified to recover live allocation signers (see recovery_regression).
const DOMAIN_SEPARATOR: [u8; 32] =
    hex_bytes("334534ebf2bc5646180e51fb57f74e967114b2abe197d4aed526f908494a87ce");

const fn hex_bytes<const N: usize>(s: &str) -> [u8; N] {
    let s = s.as_bytes();
    let mut out = [0u8; N];
    let mut i = 0;
    while i < N {
        let hi = hex_nibble(s[i * 2]);
        let lo = hex_nibble(s[i * 2 + 1]);
        out[i] = (hi << 4) | lo;
        i += 1;
    }
    out
}

const fn hex_nibble(b: u8) -> u8 {
    match b {
        b'0'..=b'9' => b - b'0',
        b'a'..=b'f' => b - b'a' + 10,
        b'A'..=b'F' => b - b'A' + 10,
        _ => 0,
    }
}

pub struct ProbeRequest {
    pub indexer_address: String,
    pub indexer_url: String,
    pub deployment_ipfs_hash: String,
    pub query: String,
    pub block_hash: String,
    pub auth_token: Option<String>,
    pub stake_weight: f64,
}

pub struct GatewayProbeRequest {
    pub gateway_url: String,
    pub api_key: String,
    pub subgraph_id: String,
    pub _deployment_id: String,
    pub query: String,
    pub block_hash: String,
}

pub struct RawObservation {
    pub indexer_address: String,
    pub response_hash: Option<String>,
    pub raw_response: Option<String>,
    pub latency_ms: i32,
    pub meta_block_number: Option<i64>,
    pub meta_block_hash: Option<String>,
    pub http_status: Option<i32>,
    pub error_class: Option<String>,
    pub stake_weight: f64,
}

/// Execute a probe via The Graph gateway. The response hash is taken from
/// the `graph-attestation` header (`responseCID`), and the indexer address
/// is recovered from the EIP-712 signature in that header.
pub async fn execute_gateway_probe(req: GatewayProbeRequest) -> RawObservation {
    let url = format!(
        "{}/{}/subgraphs/id/{}",
        req.gateway_url.trim_end_matches('/'),
        req.api_key,
        req.subgraph_id,
    );

    let Ok(client) = reqwest::Client::builder()
        .timeout(Duration::from_secs(30))
        .build()
    else {
        return gateway_error_observation("client_build");
    };

    let body = serde_json::json!({
        "query": req.query,
        "variables": { "block": { "hash": req.block_hash } }
    });

    let start = Instant::now();

    let resp = match client.post(&url).json(&body).send().await {
        Err(e) => {
            warn!(error = %e, "Gateway probe network error");
            return gateway_error_observation("network_error");
        }
        Ok(r) => r,
    };

    let http_status = resp.status().as_u16() as i32;
    let latency_ms = start.elapsed().as_millis() as i32;
    let attestation_header = resp
        .headers()
        .get("graph-attestation")
        .and_then(|v| v.to_str().ok())
        .map(str::to_string);

    if !resp.status().is_success() {
        return RawObservation {
            indexer_address: "gateway-error".to_string(),
            response_hash: None,
            raw_response: None,
            latency_ms,
            meta_block_number: None,
            meta_block_hash: None,
            http_status: Some(http_status),
            error_class: Some("http_error".to_string()),
            stake_weight: 1.0,
        };
    }

    let body_text = match resp.text().await {
        Err(e) => {
            warn!(error = %e, "Gateway probe body read error");
            return gateway_error_observation("body_error");
        }
        Ok(t) => t,
    };

    let parsed: Option<serde_json::Value> = serde_json::from_str(&body_text).ok();

    let error_class = parsed.as_ref().and_then(|v| {
        if v.get("errors").is_some() {
            Some("graphql_error".to_string())
        } else {
            None
        }
    });

    let (meta_block_number, meta_block_hash) = parsed
        .as_ref()
        .map(extract_meta)
        .unwrap_or((None, None));

    // Recover indexer address from attestation; use JCS hash for content clustering.
    let indexer_address = if let Some(attest_str) = &attestation_header {
        parse_attestation_address(attest_str)
    } else {
        "gateway-no-attestation".to_string()
    };
    let response_hash = if error_class.is_none() {
        normalize_and_hash(&body_text).ok()
    } else {
        None
    };

    debug!(
        indexer = %indexer_address,
        hash = ?response_hash,
        latency_ms,
        "Gateway probe complete"
    );

    RawObservation {
        indexer_address,
        response_hash,
        raw_response: Some(body_text),
        latency_ms,
        meta_block_number,
        meta_block_hash,
        http_status: Some(http_status),
        error_class,
        stake_weight: 1.0,
    }
}

/// Parse the `graph-attestation` JSON header and recover the signing address.
/// The address is the allocation-specific key (unique per indexer allocation).
fn parse_attestation_address(header: &str) -> String {
    let Ok(v) = serde_json::from_str::<serde_json::Value>(header) else {
        return "gateway-bad-attestation".to_string();
    };
    try_recover_signer(&v).unwrap_or_else(|| "gateway-unresolved".to_string())
}

fn try_recover_signer(v: &serde_json::Value) -> Option<String> {
    let parse_hex32 = |key: &str| -> Option<[u8; 32]> {
        let s = v[key].as_str()?.trim_start_matches("0x");
        if s.len() != 64 {
            return None;
        }
        let bytes = hex::decode(s).ok()?;
        bytes.try_into().ok()
    };

    let request_cid = parse_hex32("requestCID")?;
    let response_cid = parse_hex32("responseCID")?;
    let deployment_id = parse_hex32("subgraphDeploymentID")?;
    let r_bytes = parse_hex32("r")?;
    let s_bytes = parse_hex32("s")?;
    let v_val = v["v"].as_u64()? as u8;

    // Build EIP-712 hash: keccak256(0x1901 || DOMAIN_SEPARATOR || receipt_hash)
    // receipt_hash = keccak256(abi.encode(RECEIPT_TYPEHASH, requestCID, responseCID, deploymentID))
    let mut receipt_encoded = [0u8; 128];
    receipt_encoded[0..32].copy_from_slice(&RECEIPT_TYPEHASH);
    receipt_encoded[32..64].copy_from_slice(&request_cid);
    receipt_encoded[64..96].copy_from_slice(&response_cid);
    receipt_encoded[96..128].copy_from_slice(&deployment_id);
    let receipt_hash = keccak256(&receipt_encoded);

    let mut preimage = [0u8; 66];
    preimage[0] = 0x19;
    preimage[1] = 0x01;
    preimage[2..34].copy_from_slice(&DOMAIN_SEPARATOR);
    preimage[34..66].copy_from_slice(&receipt_hash);
    let msg_hash = keccak256(&preimage);

    let sig = Signature::from_scalars(r_bytes, s_bytes).ok()?;
    let recovery_id = RecoveryId::try_from(v_val).ok()?;
    let vk = VerifyingKey::recover_from_prehash(&msg_hash, &sig, recovery_id).ok()?;

    let uncompressed = vk.to_encoded_point(false);
    let pubkey_bytes = &uncompressed.as_bytes()[1..]; // skip 0x04
    let hash = keccak256(pubkey_bytes);
    Some(format!("0x{}", hex::encode(&hash[12..])))
}

fn keccak256(input: &[u8]) -> [u8; 32] {
    let mut h = Keccak256::new();
    h.update(input);
    h.finalize().into()
}

// ── Direct indexer probe (legacy free-query mode) ────────────────────────────

pub async fn execute_probe(req: ProbeRequest) -> RawObservation {
    let url = format!(
        "{}/subgraphs/id/{}",
        req.indexer_url.trim_end_matches('/'),
        req.deployment_ipfs_hash
    );

    let Ok(client) = reqwest::Client::builder()
        .timeout(Duration::from_secs(30))
        .build()
    else {
        return error_observation(req.indexer_address, req.stake_weight, None, "client_build");
    };

    let mut builder = client.post(&url);
    if let Some(ref token) = req.auth_token {
        builder = builder.bearer_auth(token);
    }

    let body = serde_json::json!({
        "query": req.query,
        "variables": { "block": { "hash": req.block_hash } }
    });

    let start = Instant::now();

    let resp = match builder.json(&body).send().await {
        Err(e) => {
            warn!(indexer = %req.indexer_address, error = %e, "Probe network error");
            return error_observation(
                req.indexer_address,
                req.stake_weight,
                Some(start.elapsed().as_millis() as i32),
                "network_error",
            );
        }
        Ok(r) => r,
    };

    let http_status = resp.status().as_u16() as i32;
    let latency_ms = start.elapsed().as_millis() as i32;

    if !resp.status().is_success() {
        warn!(indexer = %req.indexer_address, status = http_status, "Probe HTTP error");
        return RawObservation {
            indexer_address: req.indexer_address,
            response_hash: None,
            raw_response: None,
            latency_ms,
            meta_block_number: None,
            meta_block_hash: None,
            http_status: Some(http_status),
            error_class: Some("http_error".to_string()),
            stake_weight: req.stake_weight,
        };
    }

    let body_text = match resp.text().await {
        Err(e) => {
            warn!(indexer = %req.indexer_address, error = %e, "Probe body read error");
            return RawObservation {
                indexer_address: req.indexer_address,
                response_hash: None,
                raw_response: None,
                latency_ms,
                meta_block_number: None,
                meta_block_hash: None,
                http_status: Some(http_status),
                error_class: Some("body_error".to_string()),
                stake_weight: req.stake_weight,
            };
        }
        Ok(t) => t,
    };

    let parsed: Option<serde_json::Value> = serde_json::from_str(&body_text).ok();

    let error_class = parsed.as_ref().and_then(|v| {
        if v.get("errors").is_some() {
            Some("graphql_error".to_string())
        } else {
            None
        }
    }).or_else(|| {
        if parsed.is_none() { Some("invalid_json".to_string()) } else { None }
    });

    let (meta_block_number, meta_block_hash) = parsed
        .as_ref()
        .map(extract_meta)
        .unwrap_or((None, None));

    let response_hash = if error_class.is_none() {
        normalize_and_hash(&body_text).ok()
    } else {
        None
    };

    debug!(
        indexer = %req.indexer_address,
        hash = ?response_hash,
        latency_ms,
        "Probe complete"
    );

    RawObservation {
        indexer_address: req.indexer_address,
        response_hash,
        raw_response: Some(body_text),
        latency_ms,
        meta_block_number,
        meta_block_hash,
        http_status: Some(http_status),
        error_class,
        stake_weight: req.stake_weight,
    }
}

fn gateway_error_observation(class: &str) -> RawObservation {
    RawObservation {
        indexer_address: "gateway-error".to_string(),
        response_hash: None,
        raw_response: None,
        latency_ms: 0,
        meta_block_number: None,
        meta_block_hash: None,
        http_status: None,
        error_class: Some(class.to_string()),
        stake_weight: 1.0,
    }
}

fn error_observation(addr: String, stake_weight: f64, latency_ms: Option<i32>, class: &str) -> RawObservation {
    RawObservation {
        indexer_address: addr,
        response_hash: None,
        raw_response: None,
        latency_ms: latency_ms.unwrap_or(0),
        meta_block_number: None,
        meta_block_hash: None,
        http_status: None,
        error_class: Some(class.to_string()),
        stake_weight,
    }
}

#[cfg(test)]
mod horizon_domain_probe {
    //! One-off empirical solver: given a LIVE attestation and the set of active
    //! allocations on its deployment, find which EIP-712 domain recovers a real
    //! allocation signer. Run: `cargo test -p foghorn-probe horizon -- --nocapture`.
    use super::keccak256;
    use k256::ecdsa::{RecoveryId, Signature, VerifyingKey};

    fn h32(s: &str) -> [u8; 32] {
        hex::decode(s.trim_start_matches("0x")).unwrap().try_into().unwrap()
    }
    fn a20(s: &str) -> [u8; 20] {
        hex::decode(s.trim_start_matches("0x")).unwrap().try_into().unwrap()
    }

    fn domain_separator(name: &str, version: &str, chain_id: u64, vc: &[u8; 20], salt: Option<[u8; 32]>) -> [u8; 32] {
        let type_str = if salt.is_some() {
            "EIP712Domain(string name,string version,uint256 chainId,address verifyingContract,bytes32 salt)"
        } else {
            "EIP712Domain(string name,string version,uint256 chainId,address verifyingContract)"
        };
        let mut enc = Vec::new();
        enc.extend_from_slice(&keccak256(type_str.as_bytes()));
        enc.extend_from_slice(&keccak256(name.as_bytes()));
        enc.extend_from_slice(&keccak256(version.as_bytes()));
        let mut cid = [0u8; 32];
        cid[24..32].copy_from_slice(&chain_id.to_be_bytes());
        enc.extend_from_slice(&cid);
        let mut v = [0u8; 32];
        v[12..32].copy_from_slice(vc);
        enc.extend_from_slice(&v);
        if let Some(s) = salt {
            enc.extend_from_slice(&s);
        }
        keccak256(&enc)
    }

    fn recover(ds: &[u8; 32], req: &[u8; 32], resp: &[u8; 32], dep: &[u8; 32], r: &[u8; 32], s: &[u8; 32], v: u8) -> Option<String> {
        let receipt_th = keccak256(b"Receipt(bytes32 requestCID,bytes32 responseCID,bytes32 subgraphDeploymentID)");
        let mut renc = Vec::new();
        renc.extend_from_slice(&receipt_th);
        renc.extend_from_slice(req);
        renc.extend_from_slice(resp);
        renc.extend_from_slice(dep);
        let receipt_hash = keccak256(&renc);
        let mut pre = vec![0x19u8, 0x01u8];
        pre.extend_from_slice(ds);
        pre.extend_from_slice(&receipt_hash);
        let msg = keccak256(&pre);
        let sig = Signature::from_scalars(*r, *s).ok()?;
        let rid = RecoveryId::try_from(v).ok()?;
        let vk = VerifyingKey::recover_from_prehash(&msg, &sig, rid).ok()?;
        let unc = vk.to_encoded_point(false);
        let h = keccak256(&unc.as_bytes()[1..]);
        Some(format!("0x{}", hex::encode(&h[12..])))
    }

    #[test]
    #[ignore] // run explicitly with --ignored when refreshing the live attestation
    fn solve_domain() {
        let req = h32("0787aef480048d1bbde1521fe285f7a263d292d908ddedea9cd1c261a99b10b3");
        let resp = h32("f553725f0a15dfea344ba76d509645d1b3f8e1fffe422657d9871c5416e72b91");
        let dep = h32("45c636b73728d75a77b84c782e2a44624a294c1414326e59f12d60e0a6e58f51");
        let r = h32("ca9344be7b872656954e7263afd14bb0808fab58687a824f77c28a8105ebcc59");
        let s = h32("0483b0bedae626d2dc08099cc2c8b9abf7b495c019fb598f24b9535a5bd18b96");
        let salt = h32("a070ffb1cd7409649bf77822cce74495468e06dbfaef09556838bf188679b9c2");

        let allocs: Vec<String> = [
            "0x0d8c40d3ca2992fe31cfd3e60efd5821f1fa970a", "0x180ccc426c7f5c9cbbd0ae8942d0a6a27ef4cc06",
            "0x1edb6ff654aabd9993939c12be421dae340258e8", "0x3926e271bfd75f028661b34514fe051772cdb35e",
            "0x4ec436742f68f96279727e727756c73872626e70", "0x5ad0e84f7f128c54936e0ae05c132634be237665",
            "0x6194517789e7318a934982775b489304da085c40", "0x65327b219de01791256c5efa198f6fc9bd0e02ce",
            "0x65c6cd8e6f7082824bf5a6ca09d222d0c990ab2e", "0x6f242077cb684ade830de875280c0967180bc035",
            "0x82940f7121f3f1383f5762ff4594da4e6521a74b", "0x82a46ac2af962e09243a8f57cc0fd979f130c706",
            "0x8b1f19fefa405a9302ba6da263aa8e574b29ad01", "0x938e45c0522a9f3814354447e26e39af6b8859dc",
            "0xa9adae02f4aea15bc37117dc3859cf4f386b61b6", "0xaa20f278e1bb31c2dcacbc02a1a260f7b01a76d7",
            "0xb7355eb38de912edb11ceaf75b75c474c596a290", "0xba525fc0e00e0317d501ae82be897b4de9eb7bf9",
            "0xbaa1f960a5abba78206b354651d5e8d655691925", "0xcbd933038d39934150c4bd4c1407a6cd350a7a6c",
            "0xce5f6cd124ead2e6fa2489a2944fabf9c4499fc0", "0xd25d397bea3c6471652f3050da7586ddc0e5ebb0",
            "0xd3dcf5eec879c3e41e61229881f3fe1d3cb7d74f", "0xd59cf0777ab4a92dcb9d9a0b59a57e0d525f4ff6",
            "0xd7ce93af23ccbeaa0eb3cbde7f1fee9133e36fdc",
        ].iter().map(|x| x.to_string()).collect();

        let vcs = [
            ("HorizonDM", "2FE023a575449AcB698648eD21276293Fa176f96"),
            ("SubgraphService", "b2Bb92d0DE618878E438b55D5846cfecD9301105"),
            ("LegacyDM", "0Ab2B043138352413Bb02e67E626a70320E3BD46"),
        ];
        let mut found = false;
        for (label, vc) in vcs {
            let vca = a20(vc);
            for name in ["Graph Protocol"] {
                for version in ["0", "1", "2"] {
                    for salted in [true, false] {
                        for v in [1u8, 0u8] {
                            let ds = domain_separator(name, version, 42161, &vca, salted.then_some(salt));
                            if let Some(signer) = recover(&ds, &req, &resp, &dep, &r, &s, v) {
                                if allocs.contains(&signer) {
                                    println!(
                                        "MATCH vc={label} name={name:?} version={version:?} salted={salted} v={v}\n  signer={signer}\n  domain_separator=0x{}",
                                        hex::encode(ds)
                                    );
                                    found = true;
                                }
                            }
                        }
                    }
                }
            }
        }
        assert!(found, "no domain combination recovered a known allocation");
    }
}

#[cfg(test)]
mod recovery_regression {
    use super::*;

    #[test]
    fn receipt_typehash_is_canonical() {
        let expected = keccak256(b"Receipt(bytes32 requestCID,bytes32 responseCID,bytes32 subgraphDeploymentID)");
        assert_eq!(RECEIPT_TYPEHASH, expected, "RECEIPT_TYPEHASH drifted from canonical");
    }

    #[test]
    fn recovers_known_allocation_signer() {
        // A real Arbitrum attestation; signer is a known active allocation.
        let att = serde_json::json!({
            "requestCID": "0x0787aef480048d1bbde1521fe285f7a263d292d908ddedea9cd1c261a99b10b3",
            "responseCID": "0xf553725f0a15dfea344ba76d509645d1b3f8e1fffe422657d9871c5416e72b91",
            "subgraphDeploymentID": "0x45c636b73728d75a77b84c782e2a44624a294c1414326e59f12d60e0a6e58f51",
            "r": "0xca9344be7b872656954e7263afd14bb0808fab58687a824f77c28a8105ebcc59",
            "s": "0x0483b0bedae626d2dc08099cc2c8b9abf7b495c019fb598f24b9535a5bd18b96",
            "v": 1
        });
        assert_eq!(
            try_recover_signer(&att).as_deref(),
            Some("0x6f242077cb684ade830de875280c0967180bc035")
        );
    }
}

fn extract_meta(value: &serde_json::Value) -> (Option<i64>, Option<String>) {
    let block = value.pointer("/data/_meta/block");
    match block {
        Some(b) => (
            b["number"].as_i64(),
            b["hash"].as_str().map(str::to_string),
        ),
        None => (None, None),
    }
}
