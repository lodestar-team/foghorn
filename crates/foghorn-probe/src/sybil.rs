//! Sybil / operator-swarm detection.
//!
//! The pattern the indexer community flags by hand (Discord, 2026-06): a single
//! operator running many *anonymous* identities, registered within days of each
//! other, each with near-identical (large) self-stake, crowding the same
//! subgraphs. We reconstruct that from the ingested roster.
//!
//! Deliberately conservative — a real shared-infra indexer is not a swarm. We
//! require all of: no ENS, tight creation-time clustering, near-identical stake,
//! and ≥3 members. Confidence scales with how tightly those hold, and only
//! clusters at/above `SYBIL_VERDICT_CONFIDENCE` earn a public verdict.

use anyhow::Result;
use foghorn_core::types::SybilCluster;
use sha2::{Digest, Sha256};
use sqlx::{PgPool, Row};
use std::collections::HashMap;

const CREATED_WINDOW_SECS: i64 = 14 * 86_400; // same operator spins identities up within a fortnight
const STAKE_REL_TOL: f64 = 0.25; // self-stakes within 25% of each other
const HIGH_STAKE_GRT: f64 = 50_000_000.0; // anonymous nine-figure stake is itself a swarm tell
const MIN_STAKE_GRT: f64 = 1_000_000.0; // ignore dust; swarms crowd rewards, so they stake big
const MIN_MEMBERS: usize = 3;

#[derive(Clone)]
struct Candidate {
    address: String,
    created_at: i64,
    stake: f64,
}

/// Run detection over the current roster, persist clusters, and return a map
/// from indexer address → (cluster_id, confidence) for the scorer to consume.
pub async fn detect_and_store(pool: &PgPool) -> Result<HashMap<String, (String, f64)>> {
    let rows = sqlx::query(
        r#"SELECT indexer_address, created_at, self_stake_grt
           FROM indexer_profile
           WHERE ens_name IS NULL
             AND created_at IS NOT NULL
             AND self_stake_grt IS NOT NULL
             AND self_stake_grt >= 1000000"#,
    )
    .fetch_all(pool)
    .await?;

    let candidates: Vec<Candidate> = rows
        .iter()
        .map(|r| Candidate {
            address: r.get::<String, _>("indexer_address").to_lowercase(),
            created_at: r.get::<i64, _>("created_at"),
            stake: r.get::<f64, _>("self_stake_grt"),
        })
        .collect();

    let clusters = cluster(&candidates);

    let mut map = HashMap::new();
    for c in &clusters {
        for m in &c.members {
            map.insert(m.clone(), (c.cluster_id.clone(), c.confidence));
        }
        sqlx::query(
            r#"INSERT INTO sybil_cluster (cluster_id, confidence, member_count, members, signals, detected_at)
               VALUES ($1,$2,$3,$4,$5, NOW())
               ON CONFLICT (cluster_id) DO UPDATE SET
                 confidence = EXCLUDED.confidence,
                 member_count = EXCLUDED.member_count,
                 members = EXCLUDED.members,
                 signals = EXCLUDED.signals,
                 detected_at = NOW()"#,
        )
        .bind(&c.cluster_id)
        .bind(c.confidence)
        .bind(c.member_count)
        .bind(serde_json::to_value(&c.members)?)
        .bind(&c.signals)
        .execute(pool)
        .await?;
    }
    Ok(map)
}

/// Pure clustering — union-find over the "same operator" adjacency relation.
fn cluster(candidates: &[Candidate]) -> Vec<SybilCluster> {
    let n = candidates.len();
    let mut parent: Vec<usize> = (0..n).collect();

    fn find(parent: &mut [usize], x: usize) -> usize {
        let mut x = x;
        while parent[x] != x {
            parent[x] = parent[parent[x]];
            x = parent[x];
        }
        x
    }

    for i in 0..n {
        for j in (i + 1)..n {
            let a = &candidates[i];
            let b = &candidates[j];
            let close_time = (a.created_at - b.created_at).abs() <= CREATED_WINDOW_SECS;
            let similar_stake =
                (a.stake - b.stake).abs() / a.stake.max(b.stake) <= STAKE_REL_TOL;
            // Two anonymous identities created close together are "same operator"
            // suspects if their stakes are similar OR both are nine-figure whales
            // (anonymous + ~100M each + same-fortnight is the exact swarm pattern).
            let both_whales = a.stake >= HIGH_STAKE_GRT && b.stake >= HIGH_STAKE_GRT;
            if close_time && (similar_stake || both_whales) {
                let ra = find(&mut parent, i);
                let rb = find(&mut parent, j);
                if ra != rb {
                    parent[ra] = rb;
                }
            }
        }
    }

    let mut groups: HashMap<usize, Vec<usize>> = HashMap::new();
    for i in 0..n {
        let r = find(&mut parent, i);
        groups.entry(r).or_default().push(i);
    }

    let mut out = Vec::new();
    for (_root, idxs) in groups {
        if idxs.len() < MIN_MEMBERS {
            continue;
        }
        let members: Vec<Candidate> = idxs.iter().map(|&i| candidates[i].clone()).collect();
        out.push(build_cluster(&members));
    }
    out
}

fn build_cluster(members: &[Candidate]) -> SybilCluster {
    let mut addrs: Vec<String> = members.iter().map(|m| m.address.clone()).collect();
    addrs.sort();

    let created_min = members.iter().map(|m| m.created_at).min().unwrap_or(0);
    let created_max = members.iter().map(|m| m.created_at).max().unwrap_or(0);
    let spread_days = (created_max - created_min) as f64 / 86_400.0;
    let stake_min = members.iter().map(|m| m.stake).fold(f64::INFINITY, f64::min);
    let stake_max = members.iter().map(|m| m.stake).fold(0.0_f64, f64::max);
    let stake_rel = if stake_max > 0.0 {
        (stake_max - stake_min) / stake_max
    } else {
        0.0
    };

    let mut confidence = 0.5;
    confidence += ((members.len() as f64 - MIN_MEMBERS as f64) * 0.05).min(0.2);
    if spread_days <= 1.0 {
        confidence += 0.2;
    } else if spread_days <= 2.0 {
        confidence += 0.1;
    }
    if stake_rel <= 0.03 {
        confidence += 0.2;
    } else if stake_rel <= 0.10 {
        confidence += 0.1;
    }
    // Anonymous nine-figure stake across the whole cluster is highly suspicious.
    if stake_min >= HIGH_STAKE_GRT {
        confidence += 0.2;
    }
    confidence = confidence.min(1.0);

    let cluster_id = {
        let mut h = Sha256::new();
        h.update(addrs.join(",").as_bytes());
        format!("swarm-{}", &hex::encode(h.finalize())[..10])
    };

    let signals = serde_json::json!({
        "anonymous": true,
        "member_count": members.len(),
        "created_spread_days": spread_days,
        "self_stake_min_grt": stake_min,
        "self_stake_max_grt": stake_max,
        "self_stake_rel_spread": stake_rel,
    });

    SybilCluster {
        cluster_id,
        confidence,
        member_count: members.len() as i32,
        members: addrs,
        signals,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn c(addr: &str, day: i64, stake: f64) -> Candidate {
        Candidate { address: addr.to_string(), created_at: day * 86_400, stake }
    }

    #[test]
    fn flags_tight_anonymous_swarm() {
        // 3 anon indexers, created same day, ~100M GRT each — the thread's pattern.
        let cands = vec![
            c("0xa", 1000, 100_000_000.0),
            c("0xb", 1000, 100_000_000.0),
            c("0xc", 1000, 100_100_000.0),
        ];
        let clusters = cluster(&cands);
        assert_eq!(clusters.len(), 1);
        assert_eq!(clusters[0].member_count, 3);
        assert!(clusters[0].confidence >= 0.6, "conf={}", clusters[0].confidence);
    }

    #[test]
    fn flags_anonymous_whale_swarm_with_stake_spread() {
        // The real case: anonymous, created days apart, ~100-124M each (>25% would
        // miss, but 18% spread is within tolerance and they're whales).
        let cands = vec![
            c("0xa", 1000, 101_000_000.0),
            c("0xb", 1003, 123_000_000.0),
            c("0xc", 1005, 110_000_000.0),
        ];
        let clusters = cluster(&cands);
        assert_eq!(clusters.len(), 1, "{:?}", clusters);
        assert_eq!(clusters[0].member_count, 3);
        assert!(clusters[0].confidence >= 0.6, "conf={}", clusters[0].confidence);
    }

    #[test]
    fn does_not_flag_pairs_or_dissimilar() {
        // only 2 members -> below MIN_MEMBERS
        let cands = vec![c("0xa", 1000, 1.0), c("0xb", 1000, 1.0)];
        assert!(cluster(&cands).is_empty());

        // 3 members but wildly different stake & creation -> no cluster
        let cands = vec![
            c("0xa", 1000, 1_000.0),
            c("0xb", 1050, 50_000_000.0),
            c("0xc", 2000, 3.0),
        ];
        assert!(cluster(&cands).is_empty());
    }

    #[test]
    fn cluster_id_is_deterministic_and_order_independent() {
        let a = build_cluster(&[c("0xc", 1, 1.0), c("0xa", 1, 1.0), c("0xb", 1, 1.0)]);
        let b = build_cluster(&[c("0xa", 1, 1.0), c("0xb", 1, 1.0), c("0xc", 1, 1.0)]);
        assert_eq!(a.cluster_id, b.cluster_id);
    }
}
