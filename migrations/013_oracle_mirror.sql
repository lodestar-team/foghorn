-- Foghorn schema v13 — a full mirror of the canonical Gateway QoS Oracle.
--
-- ## Why a mirror and not a reimplementation
--
-- The oracle's metrics are produced from Edge & Node's private gateway telemetry, so they cannot be
-- recomputed by anyone else. They are also not recoverable from the chain: the DataEdge carries
-- only `{topic, ipfs_hash, timestamp}`, and the pinned payloads are unreachable from public IPFS —
-- eight gateways, four request forms, all `504 no providers found for the CID`. The single place
-- the historical numbers survive is the oracle's own subgraph, which materialises every field.
--
-- So this mirrors that subgraph, completely and in its own field names, into tables Lodestar owns.
-- When the publisher stalls — 37 hours on 2026-07-29, with no announcement — consumers keep a
-- queryable, API-key-free copy of everything ever published, and `oracle_message` (v12) tells them
-- exactly how stale it is instead of a stale subgraph answering like a fresh one.
--
-- ## What a mirror cannot do, stated so nobody claims otherwise
--
-- It cannot invent data for a window the publisher never produced. During an outage the mirror is
-- as frozen as the source. What it changes is that the freeze is *visible*, the history stays
-- *served*, and neither depends on the gateway path being healthy.
--
-- ## Types
--
-- The oracle's `BigDecimal` fields become NUMERIC, not double precision: they arrive as decimal
-- strings and NUMERIC round-trips them exactly. Query fees in particular are small enough that
-- float error is a real risk. `BigInt` day boundaries become BIGINT epoch seconds, as published.
--
-- Primary keys are the oracle's own entity ids, so re-ingesting is idempotent and an entity that
-- gets corrected upstream converges here rather than duplicating.

-- Per (indexer, deployment, day) — the entity almost every consumer actually queries.
CREATE TABLE IF NOT EXISTS oracle_allocation_daily (
    id                                TEXT PRIMARY KEY,
    day_number                        INT  NOT NULL,
    day_start                         BIGINT,
    day_end                           BIGINT,
    data_point_count                  BIGINT,
    indexer_wallet                    TEXT NOT NULL,
    indexer_url                       TEXT,
    subgraph_deployment_ipfs_hash     TEXT NOT NULL,
    avg_indexer_blocks_behind         NUMERIC,
    avg_indexer_latency_ms            NUMERIC,
    avg_query_fee                     NUMERIC,
    max_indexer_blocks_behind         NUMERIC,
    max_indexer_latency_ms            NUMERIC,
    max_query_fee                     NUMERIC,
    num_indexer_200_responses         NUMERIC,
    proportion_indexer_200_responses  NUMERIC,
    query_count                       NUMERIC,
    total_query_fees                  NUMERIC,
    start_epoch                       NUMERIC,
    end_epoch                         NUMERIC,
    chain_id                          TEXT,
    gateway_id                        TEXT,
    -- When Foghorn copied it. Never confused with when the oracle published it: that is
    -- `oracle_message.posted_at`, read from Gnosis. Conflating the two is the exact bug that made
    -- a 37-hour-dead oracle report an age of 187 seconds.
    synced_at                         TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS oracle_alloc_daily_indexer
    ON oracle_allocation_daily (indexer_wallet, day_number DESC);
CREATE INDEX IF NOT EXISTS oracle_alloc_daily_deployment
    ON oracle_allocation_daily (subgraph_deployment_ipfs_hash, day_number DESC);
CREATE INDEX IF NOT EXISTS oracle_alloc_daily_day
    ON oracle_allocation_daily (day_number DESC);

-- Gateway-wide per-indexer daily totals.
CREATE TABLE IF NOT EXISTS oracle_indexer_daily (
    id                                TEXT PRIMARY KEY,
    day_number                        INT  NOT NULL,
    day_start                         BIGINT,
    day_end                           BIGINT,
    data_point_count                  BIGINT,
    indexer_wallet                    TEXT NOT NULL,
    indexer_url                       TEXT,
    subgraph_deployment_ipfs_hash     TEXT,
    avg_indexer_blocks_behind         NUMERIC,
    avg_indexer_latency_ms            NUMERIC,
    avg_query_fee                     NUMERIC,
    max_indexer_blocks_behind         NUMERIC,
    max_indexer_latency_ms            NUMERIC,
    max_query_fee                     NUMERIC,
    num_indexer_200_responses         NUMERIC,
    proportion_indexer_200_responses  NUMERIC,
    query_count                       NUMERIC,
    total_query_fees                  NUMERIC,
    start_epoch                       NUMERIC,
    end_epoch                         NUMERIC,
    chain_id                          TEXT,
    gateway_id                        TEXT,
    synced_at                         TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS oracle_indexer_daily_indexer
    ON oracle_indexer_daily (indexer_wallet, day_number DESC);

-- Gateway-wide per-deployment daily totals. This is the served-share denominator: an indexer's
-- query_count over this deployment total is the "share of traffic I actually got" figure that no
-- probe-based feed can ever produce.
CREATE TABLE IF NOT EXISTS oracle_query_daily (
    id                             TEXT PRIMARY KEY,
    day_number                     INT  NOT NULL,
    day_start                      BIGINT,
    day_end                        BIGINT,
    data_point_count               BIGINT,
    subgraph_deployment_ipfs_hash  TEXT NOT NULL,
    avg_gateway_latency_ms         NUMERIC,
    max_gateway_latency_ms         NUMERIC,
    avg_query_fee                  NUMERIC,
    max_query_fee                  NUMERIC,
    gateway_query_success_rate     NUMERIC,
    user_attributed_error_rate     NUMERIC,
    most_recent_query_ts           NUMERIC,
    query_count                    NUMERIC,
    total_query_fees               NUMERIC,
    start_epoch                    NUMERIC,
    end_epoch                      NUMERIC,
    chain_id                       TEXT,
    gateway_id                     TEXT,
    synced_at                      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS oracle_query_daily_deployment
    ON oracle_query_daily (subgraph_deployment_ipfs_hash, day_number DESC);

-- The 5-MINUTE granularity. `AllocationDataPoint` is one row per (indexer, deployment) per publish
-- bucket, and it carries `stdev_indexer_latency_ms`, which the daily rollups drop entirely. Nothing
-- in the ecosystem surfaces this today, and it is the resolution at which the 2026-07-29 failure
-- was diagnosable at all.
CREATE TABLE IF NOT EXISTS oracle_allocation_point (
    id                                TEXT PRIMARY KEY,
    day_number                        INT  NOT NULL,
    day_start                         BIGINT,
    day_end                           BIGINT,
    indexer_wallet                    TEXT NOT NULL,
    indexer_url                       TEXT,
    subgraph_deployment_ipfs_hash     TEXT NOT NULL,
    avg_indexer_blocks_behind         NUMERIC,
    avg_indexer_latency_ms            NUMERIC,
    stdev_indexer_latency_ms          NUMERIC,
    avg_query_fee                     NUMERIC,
    max_indexer_blocks_behind         NUMERIC,
    max_indexer_latency_ms            NUMERIC,
    max_query_fee                     NUMERIC,
    num_indexer_200_responses         NUMERIC,
    proportion_indexer_200_responses  NUMERIC,
    query_count                       NUMERIC,
    total_query_fees                  NUMERIC,
    start_epoch                       NUMERIC,
    end_epoch                         NUMERIC,
    chain_id                          TEXT,
    gateway_id                        TEXT,
    synced_at                         TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS oracle_alloc_point_indexer
    ON oracle_allocation_point (indexer_wallet, day_number DESC);
CREATE INDEX IF NOT EXISTS oracle_alloc_point_deployment
    ON oracle_allocation_point (subgraph_deployment_ipfs_hash, day_number DESC);

-- Raw payloads captured from IPFS at post time.
--
-- Separate from the subgraph mirror because it has a different trust story: this is the publisher's
-- own bytes, fetched from the CID it committed to on-chain, so it needs nobody's subgraph and
-- nobody's indexing to be believed. It only works going forward — those CIDs stop being served
-- once providers drop them, which is why every historical payload is already unreachable.
CREATE TABLE IF NOT EXISTS oracle_payload (
    ipfs_hash   TEXT PRIMARY KEY,
    topic       TEXT NOT NULL,
    bucket_ts   TIMESTAMPTZ NOT NULL,
    -- The payload verbatim. Stored unparsed so a future decoder change cannot lose the original.
    raw         TEXT NOT NULL,
    bytes       INT  NOT NULL,
    fetched_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    -- Which gateway served it, for diagnosing availability decay over time.
    via         TEXT
);
CREATE INDEX IF NOT EXISTS oracle_payload_bucket ON oracle_payload (bucket_ts DESC);
