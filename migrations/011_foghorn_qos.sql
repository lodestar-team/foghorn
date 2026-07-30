-- Foghorn schema v11 — QoS Foghorn MEASURED ITSELF, in the oracle's own shape.
--
-- `allocation_qos` (v6) is ingested from Edge & Node's QoS oracle, which makes every QoS
-- field Foghorn serves downstream of a ten-link private pipeline (gateway → Kafka →
-- Materialize → DBT → cron → IPFS → Defender → Gnosis → subgraph → gateway). On 2026-07-29
-- one link died mid-bucket and the feed went dark for 35+ hours while Foghorn kept quoting
-- the stale figures. This table is the same information derived from observations Foghorn
-- already collects, so the surface stays live when theirs does not.
--
-- ## Why the column names are theirs
--
-- The reference oracle subgraph (juanmardefago/gateway-qos-oracle-example-subgraph) puts
-- `gateway_id` and `chain_id` on every data point. The format was designed for MULTIPLE
-- gateways publishing into one schema. So this is not a fork or a rival format: it is a
-- second `gateway_id` in the schema E&N already defined. Columns therefore carry the
-- oracle's exact names (`proportion_indexer_200_responses`, `avg_indexer_blocks_behind`, …)
-- so the serving layer is a passthrough and existing consumers — indexer-tools, Lodestar's
-- own ingest — change a URL, not their queries.
--
-- ## Where our numbers mean something different, and how we say so
--
-- `query_count` is PROBES DISPATCHED, not organic traffic. The oracle counts what a gateway
-- actually routed; Foghorn counts what it chose to measure. A low count here is a statement
-- about Foghorn's cadence and never about an indexer's popularity. `gateway_id` is what keeps
-- that honest: our rows are tagged as ours, so nobody can silently read probe volume as
-- market demand. Anything serving this table repeats the caveat.
--
-- ## Two columns the oracle structurally cannot have
--
--   * `correctness_rate` — the oracle knows an indexer answered fast with a 200; it cannot
--     know the answer was RIGHT. Foghorn clusters JCS-canonicalised response hashes, so
--     confident garbage is measurable. This is the signal that earns Foghorn the right to
--     publish QoS at all.
--   * `latency_p50/p95/p99_ms` — the oracle publishes avg/max. An average hides precisely the
--     tail that makes a gateway route away from you, and a max is one bad minute forever.
--     We keep avg/max for compatibility and add percentiles for truth.
--
-- Native resolution is the bucket, default 5 minutes to match the oracle's cadence. The daily
-- rollup (their `AllocationDailyDataPoint`) is derived at query time rather than stored:
-- storing both would let them drift apart, and a QoS feed that disagrees with itself is worse
-- than one that is briefly absent.
CREATE TABLE IF NOT EXISTS foghorn_qos (
    -- ── Identity ────────────────────────────────────────────────────────────────
    indexer_address   TEXT NOT NULL,          -- oracle: indexer_wallet
    deployment_id     TEXT NOT NULL,          -- oracle: subgraph_deployment_ipfs_hash
    bucket_start      TIMESTAMPTZ NOT NULL,   -- inclusive lower edge; floor(dispatched_at / bucket_secs)
    bucket_secs       INT NOT NULL,           -- bucket width; part of the key so changing cadence
                                              -- adds a series rather than corrupting the old one
    indexer_url       TEXT,                   -- oracle: indexer_url (from allocation_map)
    chain_id          TEXT,                   -- oracle: chain_id
    gateway_id        TEXT NOT NULL,          -- oracle: gateway_id. Ours, always. See above.

    -- ── Volume and success (oracle names) ───────────────────────────────────────
    query_count                       BIGINT NOT NULL,  -- probes dispatched, see caveat above
    num_indexer_200_responses         BIGINT NOT NULL,
    proportion_indexer_200_responses  DOUBLE PRECISION NOT NULL,

    -- ── Latency (oracle names) ──────────────────────────────────────────────────
    -- Successful probes only: including errors would let a fast 500 flatter an indexer, and
    -- the failure is already counted in proportion_indexer_200_responses. Counting it twice,
    -- once as a failure and once as excellent latency, would be perverse.
    avg_indexer_latency_ms    DOUBLE PRECISION,
    max_indexer_latency_ms    DOUBLE PRECISION,
    stdev_indexer_latency_ms  DOUBLE PRECISION,

    -- ── Latency (Foghorn additions) ─────────────────────────────────────────────
    latency_p50_ms  INT,
    latency_p95_ms  INT,
    latency_p99_ms  INT,

    -- ── Freshness (oracle names) ────────────────────────────────────────────────
    -- From freshness_sample, which already resolves chainhead lag against a public Arbitrum
    -- RPC. NULL when no freshness sample landed in the bucket — absent, not zero.
    avg_indexer_blocks_behind  DOUBLE PRECISION,
    max_indexer_blocks_behind  DOUBLE PRECISION,

    -- ── Fees (oracle names) ─────────────────────────────────────────────────────
    -- NULL until probes are TAP-paid through Lodestar's own gateway. At that point these are
    -- fees FOGHORN PAID, which is a real number but not the organic revenue the oracle's
    -- equivalent fields describe. Left NULL rather than 0 so "not measured" cannot be read
    -- as "free".
    avg_query_fee    DOUBLE PRECISION,
    max_query_fee    DOUBLE PRECISION,
    total_query_fees DOUBLE PRECISION,

    -- ── Correctness (Foghorn additions) ─────────────────────────────────────────
    -- `divergent_count` counts responses disagreeing with the stake-weighted majority cluster
    -- for the same probe. `correctness_rate` is NULL rather than 1.0 when nothing in the
    -- bucket was comparable, so "we did not check" can never read as "verified correct".
    comparable_count  BIGINT NOT NULL DEFAULT 0,
    divergent_count   BIGINT NOT NULL DEFAULT 0,
    correctness_rate  DOUBLE PRECISION,

    computed_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (indexer_address, deployment_id, bucket_start, bucket_secs)
);

-- Serving paths: per-deployment leaderboards, per-indexer history, and the recent-window
-- scan the daily-rollup compat layer runs.
CREATE INDEX IF NOT EXISTS foghorn_qos_deployment
    ON foghorn_qos (deployment_id, bucket_start DESC);
CREATE INDEX IF NOT EXISTS foghorn_qos_indexer
    ON foghorn_qos (indexer_address, bucket_start DESC);
CREATE INDEX IF NOT EXISTS foghorn_qos_bucket_start
    ON foghorn_qos (bucket_start DESC);
