//! One canonical form for a subgraph deployment id.
//!
//! A deployment has two equivalent identities on The Graph: the IPFS hash (`Qm…`, a CIDv0) and the
//! bytes32 id used on-chain and in graph-node's `_meta`. They carry the same 32-byte digest — a
//! CIDv0 is `base58(0x12 0x20 || digest)` — so converting between them is deterministic and lossless.
//!
//! Foghorn accepted both and stored whichever arrived. Auto-discovered deployments came in as `Qm…`,
//! the four hand-written test-sets as bytes32, and both landed in the same `deployment_id` column,
//! which the QoS schema serves under a field named `subgraph_deployment_ipfs_hash`. That is a broken
//! promise in the one place we most insist on keeping it: the whole "repoint your existing oracle
//! query at us" claim rests on the field meaning what its name says. A consumer filtering on a
//! `Qm…` hash silently got no rows for those four deployments — not an error, just an absence, which
//! is the failure mode this project exists to complain about.
//!
//! Everything is normalised to `Qm…` on the way in.

/// The multihash prefix of every CIDv0: sha2-256 (0x12), 32 bytes long (0x20).
const CIDV0_PREFIX: [u8; 2] = [0x12, 0x20];

/// Normalise a deployment id to its IPFS `Qm…` form.
///
/// Accepts a bytes32 hex string (with or without `0x`) or an already-normalised `Qm…` hash.
/// Anything else is returned unchanged: this runs on config and on data from other services, and
/// silently rewriting an id we do not recognise would be worse than passing it through.
pub fn normalise_deployment_id(id: &str) -> String {
    let trimmed = id.trim();
    let hex_body = trimmed.strip_prefix("0x").unwrap_or(trimmed);

    // 32 bytes, hex-encoded. Anything else is not a bytes32 id.
    if hex_body.len() != 64 || !hex_body.chars().all(|c| c.is_ascii_hexdigit()) {
        return trimmed.to_string();
    }
    let Ok(digest) = hex::decode(hex_body) else {
        return trimmed.to_string();
    };

    let mut multihash = Vec::with_capacity(34);
    multihash.extend_from_slice(&CIDV0_PREFIX);
    multihash.extend_from_slice(&digest);
    bs58::encode(multihash).into_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Checked against ids we did not produce.
    ///
    /// The expected values are not this function's own output written back as a test. The
    /// graph-network-arbitrum hash is the id Edge & Node's oracle independently reports for that
    /// deployment, and the ens/premia hashes are what Foghorn's own auto-discovery pulled from the
    /// network subgraph — which is how the duplicate-id bug surfaced: the same two deployments were
    /// already being probed under their Qm form while the test-sets used bytes32.
    #[test]
    fn converts_known_bytes32_ids_to_their_ipfs_hashes() {
        let cases = [
            // graph-network-arbitrum, corroborated by allocation_qos (Edge & Node's ids).
            (
                "0x45c636b73728d75a77b84c782e2a44624a294c1414326e59f12d60e0a6e58f51",
                "QmT329Bej8AwSLahmgnmi6fdYkj3rorYAcCes45gDv9aJ4",
            ),
            // ens-ethereum, corroborated by autodiscover probing it as QmcE8Rp… concurrently.
            (
                "0xce57e4bc7b885a6255edd3e9d1617bb8819559f3903b84c18bb5db31afe17d06",
                "QmcE8RpWtsiN5hkJKdfCXGfTDoTgPEjMbQwnjLPfThT7kZ",
            ),
            // premia-arbitrum, likewise probed as QmdHQVHi… at the same time.
            (
                "0xde0a7b5368f846f7d863d9f64949b688ad9818243151d488b4c6b206145b9ea3",
                "QmdHQVHirs3yPygcgo3HNttXaFCS4pnoGiMx3aKXr192En",
            ),
            // aave-v2-ethereum.
            (
                "0xe7b79e8051d136a6ab0ffd6016c7b7fd96dc63e220fe4071021844f36796398b",
                "QmdwBHGxokamYsLfMVk6fXfry3Ss9emEiTy6wptd1ecysG",
            ),
        ];
        for (hex, expected) in cases {
            assert_eq!(normalise_deployment_id(hex), expected, "for {hex}");
        }
    }

    #[test]
    fn accepts_hex_with_and_without_prefix() {
        let with = normalise_deployment_id(
            "0x45c636b73728d75a77b84c782e2a44624a294c1414326e59f12d60e0a6e58f51",
        );
        let without = normalise_deployment_id(
            "45c636b73728d75a77b84c782e2a44624a294c1414326e59f12d60e0a6e58f51",
        );
        assert_eq!(with, without);
    }

    #[test]
    fn leaves_an_ipfs_hash_alone() {
        let qm = "QmTsWCWrFDCCXqPXeCriXRvfCMbSBQhqRWQwLqBvhqbnQ4";
        assert_eq!(normalise_deployment_id(qm), qm);
    }

    /// Idempotent, because it runs at more than one boundary and must not corrupt on a second pass.
    #[test]
    fn is_idempotent() {
        let once = normalise_deployment_id(
            "0xe7b79e8051d136a6ab0ffd6016c7b7fd96dc63e220fe4071021844f36796398b",
        );
        assert_eq!(normalise_deployment_id(&once), once);
    }

    /// Unrecognised input passes through rather than being mangled into a plausible-looking id.
    #[test]
    fn passes_through_what_it_does_not_recognise() {
        assert_eq!(normalise_deployment_id(""), "");
        assert_eq!(normalise_deployment_id("0xdeadbeef"), "0xdeadbeef");
        assert_eq!(normalise_deployment_id("not-an-id"), "not-an-id");
        // 64 chars but not hex — must not be read as bytes32.
        let not_hex = "z".repeat(64);
        assert_eq!(normalise_deployment_id(&not_hex), not_hex);
    }
}
