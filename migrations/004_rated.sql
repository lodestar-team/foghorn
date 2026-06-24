-- Foghorn schema v4 — distinguish "unrated/inactive" indexers from graded ones.
-- An indexer with no query volume, no allocations, and no probe coverage is
-- inactive, not bad — it should read "NR", not F-0. Existing rows default to
-- rated=true; the scorer overwrites on its next cycle.
ALTER TABLE indexer_score ADD COLUMN IF NOT EXISTS rated BOOLEAN NOT NULL DEFAULT TRUE;
