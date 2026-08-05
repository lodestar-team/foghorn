-- Store the peer feed's own day timestamp rather than deriving it.
--
-- The status endpoint needs to say how old Edge & Node's newest data is. It had only `day_number`,
-- which is the oracle's private day index — NOT days since the unix epoch. Treating it as the latter
-- reported their 35-day-old feed as 51 years old, which is at least an honest kind of wrong, but the
-- fix is not a magic offset constant: the subgraph publishes `dayStart` on every data point, so we
-- read the timestamp from the source and do no arithmetic at all.

ALTER TABLE allocation_qos ADD COLUMN IF NOT EXISTS day_start BIGINT;
