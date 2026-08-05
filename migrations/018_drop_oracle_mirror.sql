-- Foghorn no longer mirrors Edge & Node's QoS oracle.
--
-- There is no canonical QoS oracle. There is the Lodestar Oracle, which publishes what it measures,
-- and Edge & Node's, which publishes what it measures, and neither is authoritative over the other.
-- Holding a full copy of theirs and serving it under our name bought a dependency on their pipeline
-- and nothing we could not get by measuring the thing directly.
--
-- Their numbers are still READ, for one purpose: comparing ours against a second opinion. That runs
-- off `allocation_qos` (a small recent window, via ingest.rs) and does not need any of this.
--
-- Dropped rather than left in place. 17,031 rows of oracle_query_daily frozen at 2026-07-01 sitting
-- in the schema is exactly the kind of thing that gets joined against by accident in six months and
-- read as current — the failure this project exists to catch, left lying about as a trap.

DROP TABLE IF EXISTS oracle_allocation_point;
DROP TABLE IF EXISTS oracle_query_daily;
DROP TABLE IF EXISTS oracle_indexer_daily;
DROP TABLE IF EXISTS oracle_allocation_daily;
DROP TABLE IF EXISTS mirror_cursor;

-- oracle_subgraph_health SURVIVES. It is not mirrored data: it records whether the feed we compare
-- against is accepting the publisher's messages at all, which is the one thing about their pipeline
-- we are better placed to report than they are.
