-- Foghorn schema v15 — small key/value flags for one-shot alert state.
--
-- `alerter_state` (v8) is a deliberate SINGLETON (`id BOOLEAN PRIMARY KEY` with a CHECK), holding
-- one `last_post` timestamp for the failure-roster heartbeat. It cannot store per-alert state, so
-- edge-triggered alerts need somewhere else to remember whether they have already fired.
--
-- Used by the canonical-oracle stall alert: post once when the publisher goes quiet, once when it
-- returns, and nothing in between. Without persisted state a continuing 37-hour outage would
-- re-post on every poll cycle, which trains people to mute the channel.
CREATE TABLE IF NOT EXISTS alerter_flag (
    key        TEXT PRIMARY KEY,
    value      TEXT NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
