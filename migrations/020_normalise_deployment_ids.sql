-- One canonical form for a deployment id: the IPFS `Qm…` hash.
--
-- Four hand-written test-sets carried bytes32 ids while auto-discovery produced `Qm…`, and both
-- landed in the same `deployment_id` column — which the QoS schema serves under a field named
-- `subgraph_deployment_ipfs_hash`. Two separate faults came out of that:
--
--   1. The served id was wrong for those four deployments. A consumer filtering on a `Qm` hash got
--      no rows back: not an error, just an absence, which is the failure mode this project exists
--      to complain about.
--   2. `ens-ethereum` and `premia-arbitrum` were ALSO being auto-discovered under their `Qm` form,
--      so both were probed twice every round under two names. Double the traffic at those indexers,
--      and each deployment's rollup split across two rows that each looked like half a deployment.
--
-- The code fix normalises at both entry points and drops discovered deployments already covered by
-- a curated test-set, so no new split rows can appear. This repairs the ones already stored.
--
-- The mapping is hardcoded rather than computed. Postgres has no base58, a plpgsql implementation
-- would be a lot of untested code to trust with a rewrite, and after the code fix these four are a
-- closed set. Each was verified three ways before being written here: the Rust implementation in
-- `foghorn_core::deployment`, an independent base58 implementation, and — for
-- graph-network-arbitrum — the id Edge & Node's own oracle reports for that deployment.

CREATE TEMP TABLE deployment_id_fix (hex TEXT PRIMARY KEY, ipfs TEXT NOT NULL) ON COMMIT DROP;
INSERT INTO deployment_id_fix (hex, ipfs) VALUES
    ('0x45c636b73728d75a77b84c782e2a44624a294c1414326e59f12d60e0a6e58f51',
     'QmT329Bej8AwSLahmgnmi6fdYkj3rorYAcCes45gDv9aJ4'),  -- graph-network-arbitrum
    ('0xe7b79e8051d136a6ab0ffd6016c7b7fd96dc63e220fe4071021844f36796398b',
     'QmdwBHGxokamYsLfMVk6fXfry3Ss9emEiTy6wptd1ecysG'),  -- aave-v2-ethereum
    ('0xce57e4bc7b885a6255edd3e9d1617bb8819559f3903b84c18bb5db31afe17d06',
     'QmcE8RpWtsiN5hkJKdfCXGfTDoTgPEjMbQwnjLPfThT7kZ'),  -- ens-ethereum
    ('0xde0a7b5368f846f7d863d9f64949b688ad9818243151d488b4c6b206145b9ea3',
     'QmdHQVHirs3yPygcgo3HNttXaFCS4pnoGiMx3aKXr192En');  -- premia-arbitrum

-- ── Raw measurements ─────────────────────────────────────────────────────────
--
-- `probe` is the source of truth: `foghorn_qos` is a pure recompute over it, so correcting this
-- corrects every future rollup pass for free. Keyed on a uuid, so no collisions are possible.
UPDATE probe p SET deployment_id = f.ipfs
FROM deployment_id_fix f WHERE p.deployment_id = f.hex;

UPDATE freshness_sample s SET deployment_id = f.ipfs
FROM deployment_id_fix f WHERE s.deployment_id = f.hex;

UPDATE status_sample s SET deployment_id = f.ipfs
FROM deployment_id_fix f WHERE s.deployment_id = f.hex;

-- ── Derived rows that can simply be renamed ──────────────────────────────────
--
-- Only where no `Qm` row already occupies the same key. The rest are merged below.
UPDATE foghorn_qos q SET deployment_id = f.ipfs
FROM deployment_id_fix f
WHERE q.deployment_id = f.hex
  AND NOT EXISTS (
      SELECT 1 FROM foghorn_qos o
      WHERE o.indexer_address = q.indexer_address
        AND o.deployment_id   = f.ipfs
        AND o.bucket_start    = q.bucket_start
        AND o.bucket_secs     = q.bucket_secs
  );

-- ── Derived rows that collide: two half-measurements of one deployment ───────
--
-- Counts sum. Averages are re-weighted by what they were averages OVER — latency over successful
-- probes, blocks-behind over all probes — because a plain mean of two means silently reweights
-- whichever bucket had fewer probes up to parity.
--
-- Percentiles are set to NULL. p50/p95/p99 cannot be recombined from two summaries without the
-- underlying samples; picking one side's value, or averaging them, would produce a number that
-- looks precise and means nothing. NULL says "not available for this bucket", which is true. The
-- next rollup pass recomputes these buckets from the corrected `probe` rows anyway and will restore
-- real percentiles for anything still inside the lookback window; this only matters for older ones.
WITH merged AS (
    SELECT
        q.indexer_address,
        f.ipfs AS deployment_id,
        q.bucket_start,
        q.bucket_secs,
        SUM(q.query_count)               AS query_count,
        SUM(q.num_indexer_200_responses) AS ok,
        SUM(q.comparable_count)          AS comparable_count,
        SUM(q.divergent_count)           AS divergent_count,
        -- Weighted by successes: latency is only measured on probes that succeeded.
        SUM(q.avg_indexer_latency_ms * q.num_indexer_200_responses)
            / NULLIF(SUM(q.num_indexer_200_responses), 0)      AS avg_latency,
        MAX(q.max_indexer_latency_ms)                          AS max_latency,
        -- Weighted by probes: blocks-behind is recorded per probe, successful or not.
        SUM(q.avg_indexer_blocks_behind * q.query_count)
            / NULLIF(SUM(q.query_count), 0)                    AS avg_behind,
        MAX(q.max_indexer_blocks_behind)                       AS max_behind
    FROM foghorn_qos q
    JOIN deployment_id_fix f ON f.hex = q.deployment_id
    GROUP BY 1, 2, 3, 4
)
UPDATE foghorn_qos t SET
    query_count                      = t.query_count + m.query_count,
    num_indexer_200_responses        = t.num_indexer_200_responses + m.ok,
    proportion_indexer_200_responses =
        (t.num_indexer_200_responses + m.ok)::float8
        / NULLIF(t.query_count + m.query_count, 0)::float8,
    avg_indexer_latency_ms =
        CASE WHEN (t.num_indexer_200_responses + m.ok) > 0
             THEN (COALESCE(t.avg_indexer_latency_ms, 0) * t.num_indexer_200_responses
                   + COALESCE(m.avg_latency, 0) * m.ok)
                  / (t.num_indexer_200_responses + m.ok)
             ELSE NULL END,
    max_indexer_latency_ms   = GREATEST(t.max_indexer_latency_ms, m.max_latency),
    -- Not recombinable from summaries. See above.
    stdev_indexer_latency_ms = NULL,
    latency_p50_ms           = NULL,
    latency_p95_ms           = NULL,
    latency_p99_ms           = NULL,
    avg_indexer_blocks_behind =
        CASE WHEN (t.query_count + m.query_count) > 0
             THEN (COALESCE(t.avg_indexer_blocks_behind, 0) * t.query_count
                   + COALESCE(m.avg_behind, 0) * m.query_count)
                  / (t.query_count + m.query_count)
             ELSE NULL END,
    max_indexer_blocks_behind = GREATEST(t.max_indexer_blocks_behind, m.max_behind),
    comparable_count = t.comparable_count + m.comparable_count,
    divergent_count  = t.divergent_count + m.divergent_count,
    -- NULL when nothing was comparable, never 1.0. "We did not check" must not read as "correct".
    correctness_rate =
        CASE WHEN (t.comparable_count + m.comparable_count) > 0
             THEN 1.0 - (t.divergent_count + m.divergent_count)::float8
                        / (t.comparable_count + m.comparable_count)::float8
             ELSE NULL END,
    computed_at = NOW()
FROM merged m
WHERE t.indexer_address = m.indexer_address
  AND t.deployment_id   = m.deployment_id
  AND t.bucket_start    = m.bucket_start
  AND t.bucket_secs     = m.bucket_secs;

-- The hex rows have now been folded into their `Qm` counterparts.
DELETE FROM foghorn_qos q USING deployment_id_fix f WHERE q.deployment_id = f.hex;

-- ── Everything else keyed on a deployment ────────────────────────────────────
--
-- Attention items are (indexer, kind, deployment) keyed, so a rename can collide with an item
-- already raised under the `Qm` id. The duplicate is dropped rather than merged: these are current
-- findings regenerated on every scoring pass, not history.
DELETE FROM attention_item a USING deployment_id_fix f
WHERE a.deployment_id = f.hex
  AND EXISTS (
      SELECT 1 FROM attention_item o
      WHERE o.indexer_address = a.indexer_address
        AND o.kind            = a.kind
        AND o.deployment_id   = f.ipfs
  );
UPDATE attention_item a SET deployment_id = f.ipfs
FROM deployment_id_fix f WHERE a.deployment_id = f.hex;

DELETE FROM nondeterministic_deployment n USING deployment_id_fix f
WHERE n.deployment_id = f.hex
  AND EXISTS (SELECT 1 FROM nondeterministic_deployment o WHERE o.deployment_id = f.ipfs);
UPDATE nondeterministic_deployment n SET deployment_id = f.ipfs
FROM deployment_id_fix f WHERE n.deployment_id = f.hex;
