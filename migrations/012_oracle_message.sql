-- Foghorn schema v12 — the canonical oracle's PUBLISHER liveness, read from Gnosis directly.
--
-- ## The bug this exists to fix
--
-- `qos/status` originally reported the oracle's freshness as `max(allocation_qos.updated_at)`.
-- That column records when FOGHORN INGESTED, not when the oracle PUBLISHED. On 2026-07-30, with
-- the oracle 37 hours dead, the endpoint cheerfully reported its age as 187 seconds — reproducing
-- the exact failure it was built to expose. A stale feed answers exactly like a fresh one, and
-- measuring our own ingest clock cannot tell the difference.
--
-- The only honest source for "did the publisher publish" is the chain it publishes to. Each row
-- here is one `DataEdge` transaction on Gnosis, decoded from calldata that is plain ASCII JSON:
-- `{"topic": …, "hash": <IPFS CID>, "timestamp": <bucket epoch>}`. No API key, no subgraph, no
-- gateway — the shortest possible path to the truth.
--
-- ## Why `lag_seconds` is the column that matters
--
-- Liveness is a lagging indicator: by the time nothing has posted for an hour, the damage is
-- done. Lag (post time minus the bucket the data describes) is a LEADING one. Before the
-- 2026-07-29 outage it sat at a metronomic 30.3 minutes for hours, then jumped to 47.7 minutes
-- roughly 17 minutes before the publisher died. Anything watching lag had warning; anything
-- watching liveness alone found out hours later.
CREATE TABLE IF NOT EXISTS oracle_message (
    tx_hash       TEXT PRIMARY KEY,
    -- e.g. gateway_indexer_attempt_qos_5_minutes_prod_v3. Two topics are posted per bucket, and
    -- the publisher died BETWEEN them on 2026-07-29, so per-topic completeness is observable
    -- here and nowhere else.
    topic         TEXT NOT NULL,
    -- The pinned payload's CID. Recorded even though the content is not retrievable from public
    -- IPFS gateways ("no providers found"), because the CID is still the join key to the
    -- oracle's own subgraph entities.
    ipfs_hash     TEXT NOT NULL,
    -- The 5-minute bucket the payload describes.
    bucket_ts     TIMESTAMPTZ NOT NULL,
    -- When the transaction landed on Gnosis.
    posted_at     TIMESTAMPTZ NOT NULL,
    block_number  BIGINT NOT NULL,
    -- posted_at - bucket_ts. Stored rather than derived so it can be indexed and trended.
    lag_seconds   INT NOT NULL,
    seen_at       TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- "When did it last publish" and "what is the lag trend" are the only two read patterns.
CREATE INDEX IF NOT EXISTS oracle_message_posted_at ON oracle_message (posted_at DESC);
CREATE INDEX IF NOT EXISTS oracle_message_bucket    ON oracle_message (bucket_ts DESC);
CREATE INDEX IF NOT EXISTS oracle_message_topic     ON oracle_message (topic, bucket_ts DESC);
