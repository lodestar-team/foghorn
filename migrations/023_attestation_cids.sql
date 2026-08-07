-- The attestation's own hashes, kept alongside our clustering hash.
--
-- We already cluster responses by JCS-canonicalised SHA-256, which is the better DETECTOR: it sees
-- through incidental byte differences and catches genuine semantic divergence. What it cannot do is
-- prove anything to anyone else.
--
-- An indexer's attestation signs three fields with its allocation key: requestCID, responseCID and
-- subgraphDeploymentID, where the CIDs are keccak256 over the RAW request and response bytes. The
-- DisputeManager's conflict test is exactly:
--
--     requestCID == requestCID && subgraphDeploymentID == subgraphDeploymentID
--       && responseCID != responseCID
--
-- So two indexers that answered the identical probe and signed different responseCIDs constitute a
-- conflicting-attestation dispute that a fisherman can file with a ZERO deposit, and which can end
-- in the indexer being slashed. That is a categorically stronger claim than "our hashes differed",
-- and we were decoding these fields to verify the signature and then discarding them.
--
-- Both are stored because they answer different questions. `response_hash` says whether the DATA
-- disagreed. These say whether the disagreement is PROVABLE.
ALTER TABLE observation ADD COLUMN IF NOT EXISTS request_cid  TEXT;
ALTER TABLE observation ADD COLUMN IF NOT EXISTS response_cid TEXT;
-- The attestation itself, so a conflict can be handed to a fisherman without re-probing. Without
-- the signature the CIDs are just numbers we are asserting.
ALTER TABLE observation ADD COLUMN IF NOT EXISTS attestation  JSONB;

CREATE INDEX IF NOT EXISTS observation_response_cid ON observation (response_cid) WHERE response_cid IS NOT NULL;
