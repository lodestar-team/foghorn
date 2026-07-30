-- Foghorn schema v14 — keyset cursors for the oracle mirror.
--
-- ## The bug this fixes
--
-- Every mirror cycle restarted pagination at `id_gt: ''`. For the daily entities that is harmless:
-- 39,673 rows fit inside the page budget, so a cycle reaches the end. For the 5-minute
-- `AllocationDataPoint` — one row per indexer × deployment × 288 buckets a day, millions of rows —
-- it meant re-reading the same lowest-id 5,000 rows forever. The table looked populated and never
-- advanced past an arbitrary slice.
--
-- Storing the cursor lets each cycle resume where the last one stopped, so the mirror walks forward
-- across cycles instead of running on the spot.
--
-- ## Why `complete` matters
--
-- When a walk reaches the end of an entity, the cursor must reset so later cycles pick up newly
-- published rows (whose ids may sort *below* the cursor). Without that flag a completed entity would
-- freeze permanently — the same bug one level up.
CREATE TABLE IF NOT EXISTS mirror_cursor (
    entity      TEXT PRIMARY KEY,
    -- The `id` of the last row successfully upserted. Empty string means "start from the beginning".
    last_id     TEXT NOT NULL DEFAULT '',
    -- Set when a walk ran out of rows; the next cycle starts over to catch new publications.
    complete    BOOLEAN NOT NULL DEFAULT FALSE,
    -- Rows upserted on the most recent walk, for spotting an entity that has quietly stopped moving.
    last_rows   BIGINT NOT NULL DEFAULT 0,
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
