-- Foghorn schema v5 — non-deterministic subgraph detection.
-- A deployment that diverges every probe round (across blocks) isn't an indexer
-- problem — its mappings are non-deterministic (e.g. BigDecimal float math), so
-- honest indexers legitimately disagree. We flag the DEPLOYMENT, exclude it from
-- correctness faulting, and surface it as a finding for subgraph developers.
CREATE TABLE IF NOT EXISTS nondeterministic_deployment (
    deployment_id     TEXT PRIMARY KEY,
    divergent_probes  INT NOT NULL,
    total_probes      INT NOT NULL,
    divergence_rate   DOUBLE PRECISION NOT NULL,   -- 0..1
    sample_fields     JSONB NOT NULL DEFAULT '[]', -- field names observed diverging
    first_seen        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_seen         TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
