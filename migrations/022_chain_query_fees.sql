-- Realised query fees, from Arbitrum settlement rather than anyone's self-report.
--
-- `QueryFeesCollected` on the SubgraphService fires when an indexer actually collects for served
-- queries. It is the half of quality-of-service that active probing cannot produce: a probe knows
-- what a query cost US, never what the network paid an indexer for serving real users.
--
-- Deliberately NOT folded into `foghorn_qos.avg_query_fee`. That field means "fee per query in this
-- bucket", and our buckets count probes — so filling it with network-wide settlement would say
-- that our synthetic traffic earned an indexer money it earned from someone else. The oracle-schema
-- fee fields stay null, which is true, and this table answers the different question honestly.
CREATE TABLE IF NOT EXISTS chain_query_fees (
    indexer_address  TEXT   NOT NULL,
    deployment_id    TEXT   NOT NULL,   -- normalised to the Qm form, like everything else here
    allocation_id    TEXT   NOT NULL,
    payer            TEXT   NOT NULL,
    -- Settlement is lumpy and periodic (a RAV redemption covers many queries over a period), so
    -- these are event totals, never a per-query rate. Anything dividing by a query count would be
    -- inventing a denominator we do not have.
    tokens_collected NUMERIC NOT NULL,
    tokens_curators  NUMERIC,
    block_number     BIGINT NOT NULL,
    block_timestamp  TIMESTAMPTZ NOT NULL,
    log_index        BIGINT NOT NULL,
    PRIMARY KEY (block_number, log_index)
);

CREATE INDEX IF NOT EXISTS chain_query_fees_indexer ON chain_query_fees (indexer_address, block_timestamp DESC);
CREATE INDEX IF NOT EXISTS chain_query_fees_deployment ON chain_query_fees (deployment_id, block_timestamp DESC);
CREATE INDEX IF NOT EXISTS chain_query_fees_ts ON chain_query_fees (block_timestamp DESC);
