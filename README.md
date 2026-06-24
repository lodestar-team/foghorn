# Foghorn

A network-quality **judge** for [The Graph Protocol](https://thegraph.com). Foghorn grades
indexers, names names, and surfaces the freeloaders.

It earns the right to judge with a signal nobody else has: **correctness**. The QoS oracle
knows an indexer was *fast* and returned *200s* — it cannot tell you the indexer returned the
*right data*. Foghorn sends block-pinned GraphQL probes, canonicalises responses with JCS
(RFC 8785), hashes with SHA-256, and clusters by hash. An indexer that lands in the minority
cluster served confident, well-formed **garbage** — and Foghorn catches it.

That correctness signal is then fused with QoS / stake / REO data (from the
[Lodestar](https://lodestar-dashboard.com) API) and direct, unauthenticated `/status` health
probing into:

- a **composite A–F network-quality grade** per indexer,
- **actionable verdicts** — `serving-bad-data`, `serving-no-data`, `behind-chainhead`,
  `low-coverage`, `leech`, `reo-ineligible-candidate`, `dispute-candidate`,
  `sybil-swarm-member` — each with evidence,
- a **"Needs Attention" triage surface** — indexers *definitely* serving bad or no data right
  now, ranked by urgency, the "fix this today" list, and
- **sybil-swarm detection** — anonymous identities, registered within days of each other, with
  near-identical large self-stake, crowding the same subgraphs: one operator wearing many hats.

> Foghorn used to be a neutral observer ("no scoring, no verdicts"). It now passes judgement.
> Methodology and thresholds are documented below and fully configurable — when you name
> names, you show your working.

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
  An indexer in a minority cluster on a divergent probe is recorded as a **correctness fault**.
- **RFC 6902 diff** — JSON-Patch diff between the two largest cluster representatives,
  stored with the divergence record.
- **Lodestar ingest** — roster, QoS (success rate / latency / blocks-behind / query count),
  REO eligibility, stake and ENS names are pulled from the Lodestar API into `indexer_profile`.
- **Direct `/status` probing** — Foghorn polls each indexer's unauthenticated status endpoint
  (no TAP) for per-deployment sync state, chainhead lag, and fatal errors → `status_sample`.
- **Scoring engine** — a pure, unit-tested core (`foghorn-core::score`) fuses correctness,
  availability, freshness, coverage and value sub-scores into a composite 0–100 + A–F grade,
  derives verdicts, and builds the needs-attention list. Weights/thresholds are config-driven.
- **Sybil-swarm detection** — conservative union-find clustering over the roster (anonymity +
  creation-time proximity + near-identical self-stake + ≥3 members), with confidence scoring.
- **Postgres storage** — v1/v2 tables plus `indexer_profile`, `status_sample`, `indexer_score`,
  `verdict`, `attention_item`, `sybil_cluster`. sqlx 0.8 runtime queries (no `SQLX_OFFLINE`).
- **REST API** (axum 0.7):
  - `GET /v1/health` · `GET /v1/stats` · `GET /v1/feed` · `GET /v1/probe/:uuid`
  - `GET /v1/indexer/:address/quality?days=30` · `GET /v1/indexer/:address/freshness`
  - `GET /v1/deployments` · `GET /v1/deployment/:id/quality?days=7`
  - `GET /v1/indexers?window=30&order=desc` — ranked leaderboard with grades + sub-scores
  - `GET /v1/indexer/:address/scorecard` — full breakdown: scores, verdicts, attention, sybil
  - `GET /v1/needs-attention?kind=...` — urgency-ranked triage list
  - `GET /v1/verdicts?kind=...&severity=...` — actionable verdict feed
  - `GET /v1/sybil` — detected operator-swarm clusters
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
- **IPFS daily bundle pinning** — observations are stored in Postgres only.
- **On-chain dispute submission** — Foghorn flags `dispute-candidate` indexers; filing the
  actual POI dispute is left to a human.
- **Per-deployment crowding in sybil scoring** — the detector uses roster metadata (anonymity,
  creation time, stake). Behavioural fingerprinting (identical response hashes / latency across
  probes) is a planned confidence booster, not yet wired in.
- **Indexer auto-discovery** — there is no network-subgraph crawl to discover opted-in
  indexers automatically. For direct mode (if TAP were implemented), indexers would be
  listed in `config.toml`.
- **Stake-weighted clustering** — the `stake_weight` field exists in the schema and is
  stored, but the current clustering treats all observations equally.

---

## Architecture

```
 gateway probes (divergence) ──┐
 direct /status probes ────────┤──▶ Postgres ──▶ scoring engine ──▶ grades + verdicts
 Lodestar API (roster/QoS/REO) ┘                  (foghorn-core::score)   + attention + sybil
                                                          │
                                  foghorn-api (axum) ◀────┘
                                  /v1/indexers · /scorecard · /needs-attention
                                  /verdicts · /sybil          │
                                                              ▼
                                                   Lodestar dashboard UI
```

The probe binary runs four loops: the divergence **scheduler** (the correctness source), the
Lodestar **ingest** loop, the direct **status** loop, and the **scorer** that fuses it all.

---

## Judgement methodology

Each indexer gets a composite 0–100 score per rolling window (default 7d & 30d), a weighted mean
over whichever sub-scores have data (weights renormalise over what's present):

| Sub-score | Source | Weight |
|---|---|--:|
| **Correctness** | Foghorn minority-divergence rate (native — the unique signal) | 0.35 |
| **Availability** | probe error rate + Lodestar QoS success rate | 0.25 |
| **Freshness** | `/status` chainhead lag, `synced`, fatal errors; QoS blocks-behind | 0.20 |
| **Coverage** | breadth of subgraphs served | 0.10 |
| **Value** | stake-to-query ratio (leech detection) | 0.10 |

Grade boundaries (A ≥ 90, B ≥ 75, C ≥ 60, D ≥ 40, else F), all weights, and every verdict
threshold live in `[scoring]` in `config.toml` — tighten the bar without recompiling. Verdicts
and the needs-attention surface are derived from the same signals; see `score.rs` for the exact
rules (and its unit tests for worked examples).

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
