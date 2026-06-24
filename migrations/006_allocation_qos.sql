-- Foghorn schema v6 — per-(indexer, deployment) QoS from the oracle.
-- The QoS oracle's AllocationDailyDataPoint exposes success rate, blocks-behind
-- and query volume PER allocation (indexer × deployment) — the granularity that
-- catches "synced but serving 400s on subgraph X", which the per-indexer average
-- and /status sync state both miss. Allocation-scoped, so it never flags an
-- indexer merely syncing a deployment it doesn't serve.
CREATE TABLE IF NOT EXISTS allocation_qos (
    indexer_address  TEXT NOT NULL,
    deployment_id    TEXT NOT NULL,          -- IPFS hash (Qm…)
    day_number       INT,
    success_rate     DOUBLE PRECISION,       -- 0..1 (proportion_indexer_200_responses)
    blocks_behind    DOUBLE PRECISION,
    query_count      BIGINT,
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (indexer_address, deployment_id)
);
CREATE INDEX IF NOT EXISTS allocation_qos_deployment ON allocation_qos (deployment_id);
