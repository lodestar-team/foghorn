-- Foghorn schema v3 — judgement layer.
-- Foghorn now grades and judges indexers. These tables hold ingested context
-- (roster/QoS/REO from Lodestar), direct /status health samples, and the
-- computed output: composite grades, actionable verdicts, sybil clusters, and
-- the "needs attention" triage surface.
--
-- Additive only. Nothing from v1/v2 is dropped. freshness_sample is retained
-- for back-compat but the probe stops writing to it (status_sample supersedes).

-- ── Ingested indexer profile (Lodestar enriched roster + QoS aggregate) ───────
-- Keyed by the REAL indexer address (resolved via allocation_map).
CREATE TABLE IF NOT EXISTS indexer_profile (
    indexer_address           TEXT PRIMARY KEY,   -- lowercase hex
    ens_name                  TEXT,               -- NULL = anonymous (a sybil signal)
    url                       TEXT,
    created_at                BIGINT,             -- unix seconds (indexer registration)
    self_stake_grt            DOUBLE PRECISION,
    delegated_grt             DOUBLE PRECISION,
    allocated_grt             DOUBLE PRECISION,
    allocation_count          INT,                -- ~= distinct subgraphs served (coverage proxy)
    query_fees_collected_grt  DOUBLE PRECISION,
    reo_status                TEXT,               -- 'eligible' | 'ineligible' | 'unknown'
    reo_source                TEXT,               -- 'oracle' | 'heuristic'
    lodestar_score            DOUBLE PRECISION,   -- Lodestar's delegator-lensed score (reference)
    lodestar_grade            TEXT,
    qos_query_count           BIGINT,             -- summed over the QoS window
    qos_success_rate          DOUBLE PRECISION,   -- 0..100, mean over window (200-response proportion)
    qos_latency_ms            DOUBLE PRECISION,   -- mean over window
    qos_blocks_behind         DOUBLE PRECISION,   -- mean over window
    ingested_at               TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS indexer_profile_created_at ON indexer_profile (created_at);
CREATE INDEX IF NOT EXISTS indexer_profile_self_stake ON indexer_profile (self_stake_grt DESC);

-- ── Direct /status health samples (unauthenticated indexer status endpoint) ───
CREATE TABLE IF NOT EXISTS status_sample (
    id                  BIGSERIAL PRIMARY KEY,
    indexer_address     TEXT NOT NULL,            -- lowercase hex
    deployment_id       TEXT NOT NULL,            -- ipfs hash (Qm...) reported by the indexer
    sampled_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    synced              BOOLEAN,
    health              TEXT,                      -- 'healthy' | 'unhealthy' | 'failed'
    chain_head_block    BIGINT,
    latest_block        BIGINT,
    lag_blocks          BIGINT,                    -- chain_head - latest
    fatal_error         TEXT,                      -- NULL = none
    probe_error         TEXT                       -- set when the /status request itself failed
);
CREATE INDEX IF NOT EXISTS status_sample_indexer_time
    ON status_sample (indexer_address, sampled_at DESC);
CREATE INDEX IF NOT EXISTS status_sample_deployment
    ON status_sample (deployment_id, sampled_at DESC);

-- ── Computed composite score per indexer per rolling window ───────────────────
CREATE TABLE IF NOT EXISTS indexer_score (
    indexer_address     TEXT NOT NULL,
    window_days         INT  NOT NULL,
    computed_at         TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    composite           DOUBLE PRECISION NOT NULL,   -- 0..100
    grade               TEXT NOT NULL,               -- 'A'..'F'
    correctness_score   DOUBLE PRECISION,            -- 0..100 (NULL = no probe coverage)
    availability_score  DOUBLE PRECISION,
    freshness_score     DOUBLE PRECISION,
    coverage_score      DOUBLE PRECISION,
    value_score         DOUBLE PRECISION,
    sybil_flag          BOOLEAN NOT NULL DEFAULT FALSE,
    sybil_cluster_id    TEXT,
    probe_count         INT NOT NULL DEFAULT 0,       -- # foghorn probes this indexer answered
    reasons             JSONB NOT NULL DEFAULT '[]',  -- human-readable evidence strings
    sub_scores          JSONB NOT NULL DEFAULT '{}',
    PRIMARY KEY (indexer_address, window_days)
);
CREATE INDEX IF NOT EXISTS indexer_score_grade ON indexer_score (window_days, composite DESC);

-- ── Sybil / operator-swarm clusters ───────────────────────────────────────────
CREATE TABLE IF NOT EXISTS sybil_cluster (
    cluster_id          TEXT PRIMARY KEY,            -- deterministic id (e.g. hash of sorted members)
    confidence          DOUBLE PRECISION NOT NULL,   -- 0..1
    member_count        INT NOT NULL,
    members             JSONB NOT NULL,              -- ["0x..", ...]
    signals             JSONB NOT NULL,              -- which heuristics fired + values
    detected_at         TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- ── Actionable verdicts (current state, with first/last-seen tracking) ────────
CREATE TABLE IF NOT EXISTS verdict (
    indexer_address     TEXT NOT NULL,
    kind                TEXT NOT NULL,   -- serving-bad-data | serving-no-data | behind-chainhead
                                         -- | low-coverage | leech | reo-ineligible-candidate
                                         -- | dispute-candidate | sybil-swarm-member
    severity            TEXT NOT NULL,   -- 'critical' | 'high' | 'medium' | 'low'
    title               TEXT NOT NULL,
    evidence            JSONB NOT NULL DEFAULT '{}',
    window_days         INT,
    first_seen          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_seen           TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    status              TEXT NOT NULL DEFAULT 'open',
    PRIMARY KEY (indexer_address, kind)
);
CREATE INDEX IF NOT EXISTS verdict_kind     ON verdict (kind, last_seen DESC);
CREATE INDEX IF NOT EXISTS verdict_severity ON verdict (severity, last_seen DESC);

-- ── "Needs Attention" triage surface ──────────────────────────────────────────
-- Distinct from the leaderboard: only indexers DEFINITELY serving bad/no data
-- right now, ranked by urgency. deployment_id '' = indexer-wide (not per-deployment).
CREATE TABLE IF NOT EXISTS attention_item (
    indexer_address     TEXT NOT NULL,
    kind                TEXT NOT NULL,   -- serving-no-data | serving-bad-data | behind-chainhead
    deployment_id       TEXT NOT NULL DEFAULT '',
    severity            TEXT NOT NULL,
    urgency             DOUBLE PRECISION NOT NULL DEFAULT 0,  -- ranking key (higher = worse)
    title               TEXT NOT NULL,
    detail              JSONB NOT NULL DEFAULT '{}',          -- evidence: probe ids, status, samples
    first_seen          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_seen           TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (indexer_address, kind, deployment_id)
);
CREATE INDEX IF NOT EXISTS attention_urgency ON attention_item (urgency DESC, last_seen DESC);
