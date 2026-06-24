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
use serde_json::json;
use sha2::{Digest, Sha256};
use sqlx::{PgPool, Row};
use std::collections::{HashMap, HashSet};
use std::time::Duration;

const CREATED_WINDOW_SECS: i64 = 14 * 86_400; // same operator spins identities up within a fortnight
const STAKE_REL_TOL: f64 = 0.25; // self-stakes within 25% of each other
const HIGH_STAKE_GRT: f64 = 50_000_000.0; // anonymous nine-figure stake is itself a swarm tell
#[allow(dead_code)]
const MIN_STAKE_GRT: f64 = 1_000_000.0; // ignore dust; swarms crowd rewards, so they stake big
const MIN_MEMBERS: usize = 3;
// Crowding link: catches staggered-creation identities the time window misses.
// Two anonymous whales whose allocation footprints overlap heavily are the same
// operator crowding the same subgraphs.
const CROWD_OVERLAP: f64 = 0.8; // overlap coefficient |A∩B| / min(|A|,|B|)
const CROWD_MIN_ALLOCS: usize = 5; // both must hold enough allocations to be meaningful

const NETWORK_SUBGRAPH_ID: &str = "DZz4kDTdmzWLWsV373w2bSmoar3umKKH9y82SUKr5qmp";
const NETWORK_SUBGRAPH_BASE: &str = "https://gateway-arbitrum.network.thegraph.com/api";

#[derive(Clone)]
struct Candidate {
    address: String,
    created_at: i64,
    stake: f64,
    allocations: HashSet<String>,
}

/// Run detection over the current roster, persist clusters, and return a map
/// from indexer address → (cluster_id, confidence) for the scorer to consume.
pub async fn detect_and_store(pool: &PgPool, api_key: Option<&str>) -> Result<HashMap<String, (String, f64)>> {
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

    let mut candidates: Vec<Candidate> = rows
        .iter()
        .map(|r| Candidate {
            address: r.get::<String, _>("indexer_address").to_lowercase(),
            created_at: r.get::<i64, _>("created_at"),
            stake: r.get::<f64, _>("self_stake_grt"),
            allocations: HashSet::new(),
        })
        .collect();

    // Enrich with active allocation footprints so we can link staggered-creation
    // identities that crowd the same subgraphs. Best-effort: on failure, fall
    // back to creation-time + stake linking only.
    if let Some(key) = api_key {
        let addrs: Vec<String> = candidates.iter().map(|c| c.address.clone()).collect();
        match fetch_allocation_footprints(key, &addrs).await {
            Ok(footprints) => {
                for c in candidates.iter_mut() {
                    if let Some(set) = footprints.get(&c.address) {
                        c.allocations = set.clone();
                    }
                }
            }
            Err(e) => tracing::warn!(error = %e, "Sybil allocation-footprint fetch failed"),
        }
    }

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
            // Crowding link — same operator allocating to the same subgraphs,
            // regardless of when each identity was created.
            let crowd = a.allocations.len() >= CROWD_MIN_ALLOCS
                && b.allocations.len() >= CROWD_MIN_ALLOCS
                && overlap_coefficient(&a.allocations, &b.allocations) >= CROWD_OVERLAP;
            if (close_time && (similar_stake || both_whales)) || crowd {
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

/// |A ∩ B| / min(|A|, |B|) — high when one footprint is largely contained in the other.
fn overlap_coefficient(a: &HashSet<String>, b: &HashSet<String>) -> f64 {
    let min = a.len().min(b.len());
    if min == 0 {
        return 0.0;
    }
    let inter = a.intersection(b).count();
    inter as f64 / min as f64
}

/// Mean pairwise overlap coefficient across a cluster's members (crowding strength).
fn mean_overlap(members: &[Candidate]) -> f64 {
    let mut sum = 0.0;
    let mut pairs = 0;
    for i in 0..members.len() {
        for j in (i + 1)..members.len() {
            sum += overlap_coefficient(&members[i].allocations, &members[j].allocations);
            pairs += 1;
        }
    }
    if pairs == 0 {
        0.0
    } else {
        sum / pairs as f64
    }
}

/// Fetch active allocation deployment sets for the given indexers from the
/// network subgraph, keyed by lowercase indexer address.
async fn fetch_allocation_footprints(api_key: &str, addresses: &[String]) -> Result<HashMap<String, HashSet<String>>> {
    let mut map: HashMap<String, HashSet<String>> = HashMap::new();
    if addresses.is_empty() {
        return Ok(map);
    }
    let url = format!("{}/{}/subgraphs/id/{}", NETWORK_SUBGRAPH_BASE, api_key, NETWORK_SUBGRAPH_ID);
    let client = reqwest::Client::builder().timeout(Duration::from_secs(20)).build()?;
    let ids: Vec<String> = addresses.iter().map(|a| a.to_lowercase()).collect();

    let mut last_id = String::new();
    for _ in 0..6 {
        let q = json!({
            "query": format!(
                r#"{{ allocations(first: 1000, orderBy: id, orderDirection: asc, where: {{ indexer_in: {:?}, status: Active, id_gt: "{}" }}) {{ id indexer {{ id }} subgraphDeployment {{ id }} }} }}"#,
                ids, last_id
            )
        });
        let resp = client.post(&url).json(&q).send().await?;
        let v: serde_json::Value = resp.json().await?;
        let allocs = match v.pointer("/data/allocations").and_then(|x| x.as_array()) {
            Some(a) if !a.is_empty() => a.clone(),
            _ => break,
        };
        for a in &allocs {
            let indexer = a.pointer("/indexer/id").and_then(|x| x.as_str()).unwrap_or_default().to_lowercase();
            let dep = a.pointer("/subgraphDeployment/id").and_then(|x| x.as_str()).unwrap_or_default().to_string();
            if !indexer.is_empty() && !dep.is_empty() {
                map.entry(indexer).or_default().insert(dep);
            }
        }
        if allocs.len() < 1000 {
            break;
        }
        last_id = allocs.last().and_then(|a| a["id"].as_str()).unwrap_or_default().to_string();
        if last_id.is_empty() {
            break;
        }
    }
    Ok(map)
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
    // Crowding the same subgraphs is strong corroboration the identities are one operator.
    let overlap = mean_overlap(members);
    if overlap >= CROWD_OVERLAP {
        confidence += 0.2;
    } else if overlap >= 0.5 {
        confidence += 0.1;
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
        "allocation_overlap": overlap,
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
        Candidate { address: addr.to_string(), created_at: day * 86_400, stake, allocations: HashSet::new() }
    }

    fn ca(addr: &str, day: i64, stake: f64, deps: &[&str]) -> Candidate {
        Candidate {
            address: addr.to_string(),
            created_at: day * 86_400,
            stake,
            allocations: deps.iter().map(|s| s.to_string()).collect(),
        }
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
    fn crowding_links_staggered_creation_identities() {
        // Created far apart (time window misses them) and stake too spread for the
        // 25% rule — but they crowd the SAME subgraphs → linked by overlap.
        let deps = ["d1", "d2", "d3", "d4", "d5", "d6"];
        let cands = vec![
            ca("0xa", 1000, 5_000_000.0, &deps),
            ca("0xb", 1200, 40_000_000.0, &deps),
            ca("0xc", 1400, 2_000_000.0, &deps),
        ];
        let clusters = cluster(&cands);
        assert_eq!(clusters.len(), 1, "{:?}", clusters);
        assert_eq!(clusters[0].member_count, 3);
        assert!(clusters[0].confidence >= 0.6, "conf={}", clusters[0].confidence);
    }

    #[test]
    fn distinct_footprints_do_not_crowd_link() {
        // Far apart in time, spread stake, and DIFFERENT subgraphs → no link.
        let cands = vec![
            ca("0xa", 1000, 5_000_000.0, &["d1", "d2", "d3", "d4", "d5"]),
            ca("0xb", 1200, 40_000_000.0, &["e1", "e2", "e3", "e4", "e5"]),
            ca("0xc", 1400, 2_000_000.0, &["f1", "f2", "f3", "f4", "f5"]),
        ];
        assert!(cluster(&cands).is_empty());
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
