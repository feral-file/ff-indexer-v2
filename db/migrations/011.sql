-- Migration 011: Optimize GetTokensByFilter query performance
-- Adds denormalized provenance tracking for fast owner-filtered queries

BEGIN;

-- 1. Add columns to tokens table for global latest provenance tracking
ALTER TABLE tokens 
ADD COLUMN IF NOT EXISTS last_provenance_timestamp TIMESTAMPTZ;

COMMENT ON COLUMN tokens.last_provenance_timestamp IS 'Cached timestamp of the most recent provenance event for this token. Maintained by application code on provenance event insert.';

-- 2. Create token_ownership_provenance table for owner-specific provenance tracking
-- This table tracks the most recent provenance event for each token owner (to_address only)
CREATE TABLE IF NOT EXISTS token_ownership_provenance (
    id BIGSERIAL PRIMARY KEY,
    token_id BIGINT NOT NULL REFERENCES tokens(id) ON DELETE CASCADE,
    owner_address TEXT NOT NULL,
    last_timestamp TIMESTAMPTZ NOT NULL,
    last_tx_index BIGINT NOT NULL,
    last_event_type event_type NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    
    -- Unique constraint: one record per token-owner pair
    CONSTRAINT uq_token_ownership_provenance_token_owner UNIQUE(token_id, owner_address)
);

COMMENT ON TABLE token_ownership_provenance IS 'Tracks the most recent provenance event per token-owner pair. Enables fast owner-filtered queries without LATERAL JOINs. Only tracks to_address (recipients/owners), not from_address (senders). Maintained by application code with monotonic timestamp enforcement in UPSERT WHERE clause.';
COMMENT ON COLUMN token_ownership_provenance.owner_address IS 'Address that received the token (to_address from provenance event)';
COMMENT ON COLUMN token_ownership_provenance.last_timestamp IS 'Timestamp of most recent provenance event where this address was the recipient (to_address)';
COMMENT ON COLUMN token_ownership_provenance.last_tx_index IS 'Transaction index of most recent provenance event for tiebreaking when timestamps are equal';

-- 3. Create indexes for optimal query performance

-- Index 1: For owner-filtered token queries (GetTokensByFilter with owner filter)
-- Query: WHERE owner_address IN (...) AND token_id IN (...) ORDER BY token_id, last_timestamp DESC, last_tx_index DESC, id DESC
CREATE INDEX IF NOT EXISTS idx_token_ownership_prov_owner_token_timestamp 
    ON token_ownership_provenance (
        owner_address, 
        token_id,
        last_timestamp DESC, 
        last_tx_index DESC,
        id DESC
    );

-- Index 2: For LATERAL JOIN queries (GetTokenOwnerProvenancesBulk without owner filter)
-- Query: WHERE token_id = $1 ORDER BY token_id, last_timestamp DESC, last_tx_index DESC, id DESC LIMIT 20
CREATE INDEX IF NOT EXISTS idx_token_ownership_prov_token_timestamp 
    ON token_ownership_provenance (
        token_id,
        last_timestamp DESC,
        last_tx_index DESC,
        id DESC
    );

-- 4. Add updated_at trigger to token_ownership_provenance
CREATE TRIGGER update_token_ownership_provenance_updated_at
    BEFORE UPDATE ON token_ownership_provenance
    FOR EACH ROW 
    EXECUTE FUNCTION update_updated_at_column();

COMMIT;
