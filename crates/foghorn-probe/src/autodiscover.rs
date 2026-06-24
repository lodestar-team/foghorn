//! Dynamic deployment discovery + schema-introspection query generation.
//!
//! Hand-curated test-sets only cover a couple of deployments, so correctness
//! reaches only the few indexers serving them. This module broadens coverage:
//! it finds the most-staked (most-indexed) deployments on networks we can
//! block-pin, introspects each subgraph's schema, and auto-generates a
//! deterministic block-pinned probe query — turning each into a `TestSet` the
//! scheduler probes exactly like a curated one.
//!
//! Generation is defensive: a deployment whose query can't be generated or
//! doesn't validate is simply skipped (logged), never crashing discovery.

use crate::discovery::get_safe_block;
use foghorn_core::config::FoghornConfig;
use foghorn_core::types::{TestQuery, TestSet, TestSetDeployment};
use serde_json::{json, Value};
use std::time::Duration;
use tracing::{info, warn};

const NETWORK_SUBGRAPH_ID: &str = "DZz4kDTdmzWLWsV373w2bSmoar3umKKH9y82SUKr5qmp";
const NETWORK_SUBGRAPH_BASE: &str = "https://gateway-arbitrum.network.thegraph.com/api";
const MAX_FIELDS: usize = 8;
/// Block lag used only for validating a generated query (older = more likely indexed).
const VALIDATE_LAG: u64 = 3000;

/// Discover deployments and build auto-probe TestSets. Best-effort.
pub async fn discover_test_sets(cfg: &FoghornConfig, limit: usize) -> Vec<TestSet> {
    let Some(gw) = &cfg.gateway else {
        return vec![];
    };
    let client = match reqwest::Client::builder().timeout(Duration::from_secs(20)).build() {
        Ok(c) => c,
        Err(_) => return vec![],
    };

    let candidates = match discover_candidates(&client, &gw.api_key, limit * 3).await {
        Ok(c) => c,
        Err(e) => {
            warn!(error = %e, "Auto-discovery query failed");
            return vec![];
        }
    };

    let mut out = Vec::new();
    for cand in candidates {
        if out.len() >= limit {
            break;
        }
        // Only networks we can block-pin.
        let Some(rpc) = cfg.rpc_urls.get(&cand.network) else {
            continue;
        };
        let dep_url = format!(
            "{}/{}/deployments/id/{}",
            gw.url.trim_end_matches('/'),
            gw.api_key,
            cand.ipfs_hash
        );
        let Some((field, fields)) = build_query_fields(&client, &dep_url).await else {
            continue;
        };
        let template = render_template(&field, &fields);

        // Validate against an older, near-certainly-indexed block.
        if let Ok((_, block_hash)) = get_safe_block(rpc, VALIDATE_LAG).await {
            if !validate_query(&client, &dep_url, &template, &block_hash).await {
                continue;
            }
        }

        info!(deployment = %cand.ipfs_hash, network = %cand.network, entity = %field, "Auto-probe deployment");
        out.push(TestSet {
            deployment: TestSetDeployment {
                id: cand.ipfs_hash.clone(),
                ipfs_hash: cand.ipfs_hash.clone(),
                network: cand.network.clone(),
                description: format!("auto:{} ({})", field, cand.network),
                gateway_subgraph_id: None, // probe by deployment ipfs hash
            },
            queries: vec![TestQuery {
                category: "Q_auto".to_string(),
                template,
                entity_ids: vec![],
            }],
        });
    }
    info!(count = out.len(), "Auto-discovered probe deployments");
    out
}

struct Candidate {
    ipfs_hash: String,
    network: String,
}

async fn discover_candidates(client: &reqwest::Client, api_key: &str, first: usize) -> anyhow::Result<Vec<Candidate>> {
    // Most-staked deployments = most actively indexed = most indexers to cover.
    let url = format!("{}/{}/subgraphs/id/{}", NETWORK_SUBGRAPH_BASE, api_key, NETWORK_SUBGRAPH_ID);
    let q = json!({
        "query": format!(
            "{{ subgraphDeployments(first: {first}, orderBy: stakedTokens, orderDirection: desc, where: {{ stakedTokens_gt: \"0\" }}) {{ ipfsHash manifest {{ network }} }} }}"
        )
    });
    let resp = client.post(&url).json(&q).send().await?;
    let v: Value = resp.json().await?;
    let arr = v.pointer("/data/subgraphDeployments").and_then(|x| x.as_array()).cloned().unwrap_or_default();
    let mut out = Vec::new();
    for d in arr {
        let ipfs = d["ipfsHash"].as_str().unwrap_or_default().to_string();
        let net = d.pointer("/manifest/network").and_then(|x| x.as_str()).unwrap_or_default().to_string();
        if ipfs.starts_with("Qm") && !net.is_empty() {
            out.push(Candidate { ipfs_hash: ipfs, network: net });
        }
    }
    Ok(out)
}

/// Introspect the schema and pick a plural entity field + its scalar fields.
async fn build_query_fields(client: &reqwest::Client, dep_url: &str) -> Option<(String, Vec<String>)> {
    let schema_q = json!({"query": "{ __schema { queryType { fields { name args { name } type { kind name ofType { kind name ofType { kind name ofType { kind name } } } } } } } }"});
    let v: Value = client.post(dep_url).json(&schema_q).send().await.ok()?.json().await.ok()?;
    let fields = v.pointer("/data/__schema/queryType/fields")?.as_array()?;

    // Find a plural entity field: has `first` + `block` args, type → LIST → OBJECT(T).
    let (field_name, entity) = fields.iter().find_map(|f| {
        let name = f["name"].as_str()?;
        if name.starts_with('_') {
            return None;
        }
        let args: Vec<&str> = f["args"].as_array()?.iter().filter_map(|a| a["name"].as_str()).collect();
        if !args.contains(&"first") || !args.contains(&"block") {
            return None;
        }
        let entity = list_element_object(&f["type"])?;
        Some((name.to_string(), entity))
    })?;

    // Introspect the entity type's scalar/enum fields.
    let type_q = json!({"query": format!("{{ __type(name: \"{entity}\") {{ fields {{ name type {{ kind name ofType {{ kind name }} }} }} }} }}")});
    let tv: Value = client.post(dep_url).json(&type_q).send().await.ok()?.json().await.ok()?;
    let tfields = tv.pointer("/data/__type/fields")?.as_array()?;

    let mut selected = Vec::new();
    for tf in tfields {
        let Some(name) = tf["name"].as_str() else { continue };
        if is_scalar_or_enum(&tf["type"]) {
            selected.push(name.to_string());
        }
        if selected.len() >= MAX_FIELDS {
            break;
        }
    }
    if !selected.iter().any(|s| s == "id") {
        // ensure id is present and first
        selected.insert(0, "id".to_string());
        selected.truncate(MAX_FIELDS);
    }
    if selected.is_empty() {
        return None;
    }
    Some((field_name, selected))
}

/// Walk NON_NULL/LIST wrappers; return the inner OBJECT type name if this is a list.
fn list_element_object(t: &Value) -> Option<String> {
    let mut cur = t;
    let mut saw_list = false;
    for _ in 0..4 {
        let kind = cur["kind"].as_str().unwrap_or("");
        if kind == "LIST" {
            saw_list = true;
        }
        if kind == "OBJECT" {
            if saw_list {
                return cur["name"].as_str().map(str::to_string);
            }
            return None;
        }
        cur = &cur["ofType"];
        if cur.is_null() {
            break;
        }
    }
    None
}

/// True if the type (unwrapping NON_NULL) is a SCALAR or ENUM.
fn is_scalar_or_enum(t: &Value) -> bool {
    let kind = t["kind"].as_str().unwrap_or("");
    match kind {
        "SCALAR" | "ENUM" => true,
        "NON_NULL" => {
            let inner = t["ofType"]["kind"].as_str().unwrap_or("");
            inner == "SCALAR" || inner == "ENUM"
        }
        _ => false,
    }
}

fn render_template(field: &str, fields: &[String]) -> String {
    format!(
        "{{ {}(first: 4, orderBy: id, orderDirection: asc, block: {{ hash: \"$block_hash\" }}) {{ {} }} }}",
        field,
        fields.join(" ")
    )
}

/// A query is valid if the gateway returns a non-null `data` object (schema-valid),
/// even if some indexers were unavailable for the validation block.
async fn validate_query(client: &reqwest::Client, dep_url: &str, template: &str, block_hash: &str) -> bool {
    let query = template.replace("$block_hash", block_hash);
    let body = json!({ "query": query });
    match client.post(dep_url).json(&body).send().await {
        Ok(r) => match r.json::<Value>().await {
            Ok(v) => v.get("data").map(|d| d.is_object()).unwrap_or(false),
            Err(_) => false,
        },
        Err(_) => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn list_element_object_unwraps_non_null_list() {
        // NON_NULL -> LIST -> NON_NULL -> OBJECT(Token)
        let t = json!({
            "kind": "NON_NULL", "name": null,
            "ofType": { "kind": "LIST", "name": null,
                "ofType": { "kind": "NON_NULL", "name": null,
                    "ofType": { "kind": "OBJECT", "name": "Token" } } }
        });
        assert_eq!(list_element_object(&t).as_deref(), Some("Token"));
    }

    #[test]
    fn singular_object_is_not_a_list() {
        let t = json!({ "kind": "OBJECT", "name": "Token", "ofType": null });
        assert_eq!(list_element_object(&t), None);
    }

    #[test]
    fn scalar_detection() {
        assert!(is_scalar_or_enum(&json!({"kind":"SCALAR","name":"String"})));
        assert!(is_scalar_or_enum(&json!({"kind":"NON_NULL","ofType":{"kind":"SCALAR","name":"Bytes"}})));
        assert!(is_scalar_or_enum(&json!({"kind":"ENUM","name":"OrderDirection"})));
        assert!(!is_scalar_or_enum(&json!({"kind":"OBJECT","name":"Token"})));
        assert!(!is_scalar_or_enum(&json!({"kind":"LIST","name":null})));
    }

    #[test]
    fn template_has_block_placeholder() {
        let t = render_template("tokens", &["id".into(), "symbol".into()]);
        assert!(t.contains("$block_hash"));
        assert!(t.contains("tokens(first: 4"));
        assert!(t.contains("id symbol"));
    }
}
