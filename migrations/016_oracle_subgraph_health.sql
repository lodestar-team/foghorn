-- Foghorn schema v16 — health of the canonical oracle's SUBGRAPH, as distinct from its publisher.
--
-- ## Why this is a separate thing to watch
--
-- v12 (`oracle_message`) answers "is the publisher posting?" by reading Gnosis. That turned out to
-- be only half the question. On 2026-08-04 the publisher was posting normally and the subgraph was
-- at chain tip with `hasIndexingErrors: false`, yet it had produced no new data since 2026-07-01 —
-- 34 days — because it rejected every message:
--
--     valid: false
--     errorMessage: "0x8cbbe43f…d0ce is not a valid submitter."
--
-- The on-chain layer indexed fine; the IPFS-derived layer accepted nothing. Every consumer read
-- July data believing it current, and no existing signal showed it: the publisher looked alive, the
-- subgraph looked synced, the mirror looked populated.
--
-- So a feed has THREE independent liveness questions, and answering two of them is how a month-long
-- outage hides:
--   1. is the publisher posting?            → oracle_message (v12)
--   2. is the subgraph accepting the posts? → this table
--   3. how old is the data we actually hold? → derived from oracle_allocation_daily
CREATE TABLE IF NOT EXISTS oracle_subgraph_health (
    -- Singleton: one current view, history not needed to answer "is it broken right now".
    id                    BOOLEAN PRIMARY KEY DEFAULT TRUE,
    -- How far the serving indexer has indexed. Being at tip is what makes the failure deceptive.
    indexed_block         BIGINT,
    has_indexing_errors   BOOLEAN,
    -- Newest message the subgraph has seen on-chain, and whether it ACCEPTED it. The gap between
    -- this timestamp and `newest_valid_day_start` is the size of the silent hole.
    newest_message_at     TIMESTAMPTZ,
    newest_message_valid  BOOLEAN,
    -- Verbatim rejection reason. Kept as text because it names the offending address, which is what
    -- makes the diagnosis actionable rather than merely alarming.
    newest_message_error  TEXT,
    -- Newest day the subgraph has actually materialised data for.
    newest_valid_day      INT,
    newest_valid_day_start TIMESTAMPTZ,
    checked_at            TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT oracle_subgraph_health_singleton CHECK (id)
);
