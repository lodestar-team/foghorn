-- Foghorn schema v9 — alerter posts the FULL current failure set, not deltas.
-- last_fingerprint stores the signature of the last-posted failure roster so the
-- alerter reposts the complete list whenever it changes (an indexer appears,
-- clears, or its failure summary shifts) — never a partial delta that would read
-- as "everyone else recovered". The alerted_at column (v7) is now unused.
ALTER TABLE alerter_state ADD COLUMN IF NOT EXISTS last_fingerprint TEXT;
