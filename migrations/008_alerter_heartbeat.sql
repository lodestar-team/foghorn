-- Foghorn schema v8 — alerter liveness state.
-- Tracks the last time the alerter posted ANYTHING to Discord (a real alert or a
-- daily heartbeat). When a cycle finds no new issues, the alerter still posts a
-- "still on watch" heartbeat if it's been >24h since the last post — proof of
-- life on quiet days. A real alert bumps last_post too, so the heartbeat only
-- fires after a genuine 24h lull. Singleton row enforced via a CHECK on id.
CREATE TABLE IF NOT EXISTS alerter_state (
    id        BOOLEAN PRIMARY KEY DEFAULT TRUE,
    last_post TIMESTAMPTZ,
    CONSTRAINT alerter_state_singleton CHECK (id)
);
INSERT INTO alerter_state (id, last_post) VALUES (TRUE, NULL)
ON CONFLICT (id) DO NOTHING;
