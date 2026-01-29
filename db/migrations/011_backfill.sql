-- Migration 011 Backfill Script
-- This script populates the denormalized provenance tracking tables with historical data
-- Run this AFTER migration 011.sql has been applied
-- For large datasets (>1M tokens), consider running in batches with progress tracking

BEGIN;

-- 1. Backfill tokens.last_provenance_timestamp
-- Finds the most recent provenance event timestamp for each token
UPDATE tokens t
SET last_provenance_timestamp = subq.last_timestamp
FROM (
    SELECT DISTINCT ON (token_id)
        token_id,
        timestamp as last_timestamp
    FROM provenance_events
    ORDER BY token_id, timestamp DESC, (raw->>'tx_index')::bigint DESC
) subq
WHERE t.id = subq.token_id;

-- 2. Backfill token_ownership_provenance
-- Finds the most recent provenance event for each token-owner pair
-- Only tracks to_address (recipients/owners), not from_address (senders)
INSERT INTO token_ownership_provenance (
    token_id, 
    owner_address, 
    last_timestamp,
    last_tx_index,
    last_event_type
)
SELECT DISTINCT ON (token_id, to_address)
    token_id,
    to_address as owner_address,
    timestamp as last_timestamp,
    (raw->>'tx_index')::bigint as last_tx_index,
    event_type as last_event_type
FROM provenance_events
WHERE to_address IS NOT NULL
ORDER BY token_id, to_address, timestamp DESC, (raw->>'tx_index')::bigint DESC
ON CONFLICT (token_id, owner_address) DO NOTHING;

COMMIT;