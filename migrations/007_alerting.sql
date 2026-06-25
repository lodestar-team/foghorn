-- Foghorn schema v7 — alerting state.
-- alerted_at marks when a needs-attention item was pushed to Discord, so the
-- alerter posts each new issue exactly once. Re-emitted items keep their
-- alerted_at (no re-alert); a recurrence after the item cleared is a fresh row
-- (alerted_at NULL) and alerts again.
ALTER TABLE attention_item ADD COLUMN IF NOT EXISTS alerted_at TIMESTAMPTZ;
