-- Migration 010: Add performance indexes for bulk query operations
-- This migration adds composite indexes optimized for LATERAL join queries
-- These indexes significantly improve performance of GetTokenOwnersBulk and GetTokenProvenanceEventsBulk

BEGIN;

-- Add index for balances bulk queries
-- This index enables efficient index seeks in LATERAL joins when fetching first N balances per token
-- ordered by id ASC. Covers the query pattern: WHERE token_id = X ORDER BY id ASC LIMIT N
CREATE INDEX IF NOT EXISTS idx_balances_token_id_id 
    ON balances (token_id, id ASC);

-- Add index for provenance events bulk queries
-- This index enables efficient index seeks in LATERAL joins when fetching first N events per token
-- ordered by timestamp DESC with tx_index as tiebreaker
-- Covers the query pattern: WHERE token_id = X ORDER BY timestamp DESC, (raw->>'tx_index')::bigint DESC LIMIT N
CREATE INDEX IF NOT EXISTS idx_provenance_events_token_id_timestamp_desc 
    ON provenance_events (token_id, timestamp DESC, ((raw->>'tx_index')::bigint) DESC);

COMMIT;
