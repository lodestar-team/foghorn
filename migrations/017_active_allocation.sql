-- Foghorn schema v17 — active allocations, so a probe knows what to bill.
--
-- Paying an indexer directly needs the ALLOCATION being served, because the TAP receipt carries
-- `collection_id` (the allocation address, left-padded to 32 bytes) and the indexer's
-- `AllocationEligible` check rejects anything that is not currently active.
--
-- `allocation_map` (v2) cannot answer this. Its `allocation_key` is the ecrecovered attestation
-- SIGNER — an allocation-specific key, not the allocation id — which is the same confusion that
-- previously had QoS rows keyed on signing keys instead of indexers.
--
-- Refreshed from the network subgraph. Rows are deleted when an allocation closes rather than
-- flagged, because a closed allocation is not merely stale: billing against it is refused outright,
-- and keeping it would produce probes that fail for a reason unrelated to the indexer's health.
CREATE TABLE IF NOT EXISTS active_allocation (
    allocation_id    TEXT PRIMARY KEY,          -- the address the receipt bills
    indexer_address  TEXT NOT NULL,             -- receipt `service_provider`
    deployment_id    TEXT NOT NULL,             -- IPFS hash (Qm…)
    indexer_url      TEXT,                      -- where to send the query
    allocated_tokens NUMERIC,                   -- for prioritising coverage when funds are finite
    refreshed_at     TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- The probe path looks up by (indexer, deployment); the escrow question is per indexer.
CREATE INDEX IF NOT EXISTS active_allocation_pair
    ON active_allocation (indexer_address, deployment_id);
CREATE INDEX IF NOT EXISTS active_allocation_indexer
    ON active_allocation (indexer_address);
CREATE INDEX IF NOT EXISTS active_allocation_deployment
    ON active_allocation (deployment_id);

-- Which indexers we have escrow for. Escrow is keyed on (payer, collector, receiver) on-chain, so
-- probing an indexer we have not funded is a guaranteed 402 — pointless traffic and a misleading
-- failure. Recording it here lets the scheduler skip them rather than discover it per query.
CREATE TABLE IF NOT EXISTS tap_escrow (
    indexer_address  TEXT PRIMARY KEY,
    balance_wei      NUMERIC,
    -- NULL until first checked. Distinguishes "never looked" from "looked and found nothing",
    -- which is the distinction this codebase keeps having to relearn.
    checked_at       TIMESTAMPTZ
);
