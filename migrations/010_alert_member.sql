-- Foghorn schema v10 — alerter roster hysteresis.
-- Per-indexer debounce so a flickering indexer (its single erroring deployment's
-- traffic dipping past the threshold) or a one-cycle severity flip doesn't fire a
-- Discord post. An indexer must hold a new state (critical / high / absent) for a
-- few consecutive cycles before it moves the trigger fingerprint. The posted
-- message still shows the full *current* roster — only the WHEN-to-post is debounced.
--   stable_state    — the state currently committed to the trigger fingerprint
--   candidate_state — the new state being observed but not yet confirmed
--   streak          — consecutive cycles candidate_state has held
CREATE TABLE IF NOT EXISTS alert_member (
    indexer_address TEXT PRIMARY KEY,
    stable_state    TEXT NOT NULL,                 -- 'C' | 'H' | 'absent'
    candidate_state TEXT NOT NULL,
    streak          INT  NOT NULL DEFAULT 0,
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
