# Foghorn

Deterministic query-quality observation for [The Graph Protocol](https://thegraph.com). Sends
block-pinned GraphQL probes, canonicalises responses with JCS (RFC 8785), hashes them with
SHA-256, clusters by hash, and computes RFC 6902 JSON-Patch diffs when clusters diverge.

No scoring. No verdicts. Just neutral observations.

---

## What is actually implemented

- **Block-pinned probing** — every probe query is pinned to `block: { hash: $H }` at
  chainhead − 12 blocks, fetched from a public Arbitrum RPC.
- **Gateway-based dispatch** — queries go through The Graph gateway (API key required).
  8 requests per probe round; the gateway load-balances across different indexers.
- **EIP-712 attestation parsing** — each gateway response carries a `graph-attestation`
  header. Foghorn parses the ECDSA signature and does ecrecover against the DisputeManager
  domain to produce a stable per-allocation identifier. This is the allocation-specific
  signing key, **not** the human-readable indexer name (`graphops.eth` etc.) — mapping
  that back would require a separate operator-key registry which is not yet built.
- **JCS normalisation + SHA-256** — volatile fields stripped, keys sorted, then hashed.
  This is what the cluster comparison runs on, not the raw response bytes.
- **Cluster analysis** — observations grouped by JCS hash. `cluster_count > 1` = divergence.
- **RFC 6902 diff** — JSON-Patch diff between the two largest cluster representatives,
  stored with the divergence record.
- **Postgres storage** — `probe`, `observation`, `divergence`, `freshness_sample` tables.
  sqlx 0.8 with runtime queries (no compile-time macro magic, no `SQLX_OFFLINE` needed).
- **REST API** (axum 0.7):
  - `GET /v1/health`
  - `GET /v1/stats`
  - `GET /v1/feed?limit=50&deployment_id=...`
  - `GET /v1/probe/:uuid`
  - `GET /v1/indexer/:address/quality?days=30`
  - `GET /v1/indexer/:address/freshness`
  - `GET /v1/deployment/:id/quality?days=7`
- **Docker multi-stage build** — single `Dockerfile` with separate `probe` and `api` targets;
  `docker-compose.yml` for deployment.
- **Lodestar integration** — the companion dashboard
  ([graph-dashboard](https://github.com/cargopete/graph-dashboard)) has:
  - an API proxy route (`/api/foghorn/[...path]`)
  - a divergence feed page (`/foghorn`)
  - a probe detail page with diff viewer (`/foghorn/probe/:id`)
  - an indexer quality widget embedded in the indexer profile page

---

## What is not implemented (yet)

- **Direct indexer probing via TAP** — the RFC design assumed indexers would whitelist
  Foghorn for free probes via `--free-query-auth-token`. In practice, every modern
  indexer uses TAP (Timeline Aggregation Protocol) for payment and won't accept
  unauthenticated queries. Implementing a full TAP payer (GRT escrow setup, EIP-712
  receipt signing, on-chain approval transactions) is scoped for a future milestone.
- **Human-readable indexer attribution** — because TAP is not implemented, queries go
  through the gateway. The `indexer_address` stored is the ecrecover'd allocation-key
  address. To map this to `graphops.eth` you would need a maintained operator-key → indexer
  registry, which is not built.
- **Freshness monitor** — the `freshness` crate is a stub. It polls `_meta` but deployment
  enumeration from the network subgraph is not wired in.
- **IPFS daily bundle pinning** — observations are stored in Postgres only.
- **Indexer auto-discovery** — there is no network-subgraph crawl to discover opted-in
  indexers automatically. For direct mode (if TAP were implemented), indexers would be
  listed in `config.toml`.
- **Stake-weighted clustering** — the `stake_weight` field exists in the schema and is
  stored, but the current clustering treats all observations equally.

---

## Architecture

```
┌─────────────────┐         ┌──────────────────────────┐
│  foghorn-probe  │─────────▶  The Graph Gateway        │
│  (scheduler)    │  8 req/  │  (routes to indexers)    │
│                 │  round   │  graph-attestation header │
└────────┬────────┘          └──────────────────────────┘
         │ JCS hash + ecrecover
         ▼
    ┌─────────┐    ┌────────────────┐
    │ Postgres │◀───│  foghorn-api   │──▶ Lodestar dashboard
    └─────────┘    │  (axum REST)   │
                   └────────────────┘
```

---

## Running it

Copy and fill in the config:

```toml
# config.toml  (excluded from git)
database_url       = "postgres://user:pass@host:5432/foghorn"
test_sets_dir      = "./test-sets"
reorg_threshold    = 12
probe_interval_secs = 43200   # twice a day

[rpc_urls]
"arbitrum-one" = "https://arb1.arbitrum.io/rpc"

[gateway]
api_key     = "your-graph-api-key"
url         = "https://gateway.thegraph.com/api"
probe_count = 8
```

Then:

```bash
docker compose build
docker compose up -d
```

The API listens on port 8080 inside the container (mapped to 8082 in the provided
`docker-compose.yml`).

---

## Test sets

YAML files in `test-sets/` describe what to probe. Each file maps to one subgraph deployment
and lists query templates. The active one probes
[Premia Finance on Arbitrum One](https://thegraph.com/explorer/subgraphs/J55C6VRkMbVH6foEDR1qHtcRArMCqpCneEwLdfuGu2En)
because it has multiple allocating indexers and we observed live divergence in the wild on
the first probe run.

`gateway_subgraph_id` in the YAML is the base58 subgraph ID used with the gateway URL.

---

## EIP-712 attestation recovery

The Graph gateway returns a `graph-attestation` header with every response:

```json
{
  "requestCID": "0x...",
  "responseCID": "0x...",
  "subgraphDeploymentID": "0x...",
  "r": "0x...", "s": "0x...", "v": 0
}
```

Foghorn recovers the signer using the DisputeManager domain on Arbitrum One
(`0x2fe023a575449acb698648ed21276293fa176f96`) and uses it as a stable allocation-level
identifier. The recovered address changes when an indexer closes and reopens an allocation.

---

## Disclaimer

Foghorn is an independent open-source project by the [Lodestar](https://lodestar-dashboard.com)
team. It is **not affiliated with, endorsed by, or associated with The Graph Foundation,
Edge & Node, or any other core Graph Protocol organisation.** It consumes The Graph's
public gateway and network subgraph as any third-party application would.

---

## RFC

Based on the Foghorn RFC v0.3 (community design doc, not an official Foundation document).
M1–M3 implemented. M4 (TAP direct probing) pending.
