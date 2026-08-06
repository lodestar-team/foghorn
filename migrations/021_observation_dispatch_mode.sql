-- How each observation was dispatched.
--
-- Paid and gateway probes now run against the same probe, and they are not the same measurement.
-- A gateway observation is selection-biased: the gateway routes to indexers it already believes are
-- healthy, so the failures it avoids are invisible and any success rate computed from it is an
-- upper bound. A paid observation is unbiased, because we choose the indexer.
--
-- Without this column both land in one undifferentiated pile and the page has to make a single
-- claim about bias that is true of some rows and false of others. Recording provenance per
-- observation lets each be described accurately, and lets the biased share be reported honestly as
-- it shrinks.
--
-- Existing rows are backfilled to 'gateway': every observation before this migration came through
-- the gateway, since paid dispatch had never been switched on.
ALTER TABLE observation ADD COLUMN IF NOT EXISTS dispatch_mode TEXT NOT NULL DEFAULT 'gateway';

CREATE INDEX IF NOT EXISTS observation_dispatch_mode ON observation (dispatch_mode);
