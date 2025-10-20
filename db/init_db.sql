-- Feral File Indexer Database Schema
-- This script initializes the complete database schema for the FF Indexer v2
-- Supports Ethereum (ERC-721, ERC-1155) and Tezos (FA2) token indexing

-- Create ENUM types for better type safety and performance
CREATE TYPE token_standard AS ENUM ('erc721', 'erc1155', 'fa2');
CREATE TYPE blockchain_chain AS ENUM ('eip155:1', 'eip155:11155111', 'tezos:mainnet', 'tezos:narwhal');
CREATE TYPE enrichment_level AS ENUM ('none', 'vendor', 'full');
CREATE TYPE vendor_type AS ENUM ('opensea', 'artblocks', 'onchain', 'objkt');
CREATE TYPE media_role AS ENUM ('image', 'animation', 'poster');
CREATE TYPE media_status AS ENUM ('pending', 'ready', 'failed');
CREATE TYPE subject_type AS ENUM ('token', 'owner', 'balance', 'metadata', 'media');
CREATE TYPE event_type AS ENUM ('mint', 'transfer', 'burn', 'metadata_update');

-- ============================================================================
-- CORE TABLES
-- ============================================================================

-- Tokens table - Primary entity for tracking tokens across all blockchains
CREATE TABLE tokens (
    id BIGSERIAL PRIMARY KEY,
    token_cid TEXT NOT NULL UNIQUE,             -- canonical id: eip155:1/erc721:0x.../1234
    chain blockchain_chain NOT NULL,           -- eip155:1 or tezos:mainnet
    standard token_standard NOT NULL,          -- erc721, erc1155, fa2
    contract_address TEXT NOT NULL,
    token_number TEXT NOT NULL,
    current_owner TEXT,
    burned BOOLEAN NOT NULL DEFAULT FALSE,
    last_activity_time TIMESTAMPTZ NOT NULL DEFAULT now(),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (chain, contract_address, token_number)
);

-- Balances table - Tracks ownership quantities for multi-edition tokens (ERC1155, FA2)
CREATE TABLE balances (
    id BIGSERIAL PRIMARY KEY,
    token_id BIGINT NOT NULL REFERENCES tokens (id) ON DELETE CASCADE,
    owner_address TEXT NOT NULL,
    quantity NUMERIC(78,0) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (token_id, owner_address)
);

-- Token Metadata table - Stores original and enriched metadata for tokens
CREATE TABLE token_metadata (
    token_id BIGINT PRIMARY KEY REFERENCES tokens (id) ON DELETE CASCADE,
    origin_json JSONB,
    latest_json JSONB,
    latest_hash TEXT,
    enrichment_level enrichment_level NOT NULL DEFAULT 'none',
    last_refreshed_at TIMESTAMPTZ,
    image_url TEXT,
    animation_url TEXT,
    name TEXT,
    artists TEXT[],
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Enrichment Sources table - Tracks metadata fetch attempts from various vendors
CREATE TABLE enrichment_sources (
    id BIGSERIAL PRIMARY KEY,
    token_id BIGINT NOT NULL REFERENCES tokens (id) ON DELETE CASCADE,
    vendor vendor_type NOT NULL,           -- 'opensea','artblocks','onchain','objkt'
    source_url TEXT,
    etag TEXT,
    last_status INTEGER,
    last_error TEXT,
    last_fetched_at TIMESTAMPTZ,
    last_hash TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (token_id, vendor)
);

-- Media Assets table - Manages media files with different storage providers integration
CREATE TABLE media_assets (
    id BIGSERIAL PRIMARY KEY,
    token_id BIGINT NOT NULL REFERENCES tokens (id) ON DELETE CASCADE,
    role media_role NOT NULL,               -- 'image','animation','poster'
    source_url TEXT,
    content_hash TEXT,
    cf_image_id TEXT,
    cf_variant_map JSONB,                   -- {"thumb":..., "fit":...}
    status media_status NOT NULL DEFAULT 'pending',
    last_checked_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (token_id, role)
);

-- Changes Journal table - Audit log for tracking all changes to indexed data
CREATE TABLE changes_journal (
    "cursor" BIGSERIAL PRIMARY KEY,
    token_id BIGINT NOT NULL REFERENCES tokens (id) ON DELETE CASCADE,
    subject_type subject_type NOT NULL,     -- token, owner, balance, metadata, media
    subject_id TEXT NOT NULL,               -- polymorphic ref: provenance_event_id, balance_id, media_asset_id, etc.
    changed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    meta JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Provenance Events table - Optional audit trail of blockchain events
CREATE TABLE provenance_events (
    id BIGSERIAL PRIMARY KEY,
    token_id BIGINT NOT NULL REFERENCES tokens (id) ON DELETE CASCADE,
    chain TEXT NOT NULL,
    event_type event_type NOT NULL,         -- mint, transfer, burn, metadata_update
    from_address TEXT,
    to_address TEXT,
    quantity NUMERIC(78,0),
    tx_hash TEXT,
    block_number BIGINT,
    block_hash TEXT,
    timestamp TIMESTAMPTZ NOT NULL,
    raw JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Watched Addresses table - For owner-based indexing
CREATE TABLE watched_addresses (
    chain          TEXT        NOT NULL,
    address        TEXT        NOT NULL,
    watching       BOOLEAN     NOT NULL DEFAULT TRUE,
    added_by       TEXT        NOT NULL,  -- system/user/tenant/source of watch request
    reason         TEXT,                  -- e.g. "owner_index_request" | "manual"
    last_queried_at TIMESTAMPTZ,          -- when API last queried this address
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (chain, address)
);

-- Key-Value Store table - For configuration and state management
CREATE TABLE key_value_store (
    "key"       TEXT PRIMARY KEY,
    value     TEXT NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- ============================================================================
-- INDEXES FOR PERFORMANCE
-- ============================================================================

-- Tokens table indexes
CREATE INDEX idx_tokens_chain_contract_number ON tokens (chain, contract_address, token_number);
CREATE INDEX idx_tokens_current_owner ON tokens (current_owner) WHERE current_owner IS NOT NULL;
CREATE INDEX idx_tokens_burned ON tokens (burned) WHERE burned;
CREATE INDEX idx_tokens_last_activity_time ON tokens (last_activity_time);
CREATE INDEX idx_tokens_created_at ON tokens (created_at);

-- Balances table indexes
CREATE INDEX idx_balances_token_owner ON balances (token_id, owner_address);
CREATE INDEX idx_balances_owner_address ON balances (owner_address);
CREATE INDEX idx_balances_updated_at ON balances (updated_at);

-- Token Metadata table indexes
CREATE INDEX idx_token_metadata_enrichment_level ON token_metadata (enrichment_level);
CREATE INDEX idx_token_metadata_last_refreshed_at ON token_metadata (last_refreshed_at);
CREATE INDEX idx_token_metadata_name ON token_metadata (name) WHERE name IS NOT NULL;
CREATE INDEX idx_token_metadata_artists ON token_metadata USING GIN (artists) WHERE CARDINALITY(artists) > 0;
CREATE INDEX idx_token_metadata_image_url ON token_metadata (image_url) WHERE image_url IS NOT NULL;

-- Enrichment Sources table indexes
CREATE INDEX idx_enrichment_sources_token_vendor ON enrichment_sources (token_id, vendor);
CREATE INDEX idx_enrichment_sources_vendor ON enrichment_sources (vendor);
CREATE INDEX idx_enrichment_sources_last_fetched_at ON enrichment_sources (last_fetched_at);
CREATE INDEX idx_enrichment_sources_last_status ON enrichment_sources (last_status);

-- Media Assets table indexes
CREATE INDEX idx_media_assets_token_role ON media_assets (token_id, role);
CREATE INDEX idx_media_assets_status ON media_assets (status);
CREATE INDEX idx_media_assets_cf_image_id ON media_assets (cf_image_id) WHERE cf_image_id IS NOT NULL;
CREATE INDEX idx_media_assets_created_at ON media_assets (created_at);

-- Changes Journal table indexes
CREATE INDEX idx_changes_journal_token_id ON changes_journal (token_id);
CREATE INDEX idx_changes_journal_subject_type_id ON changes_journal (subject_type, subject_id);
CREATE INDEX idx_changes_journal_changed_at ON changes_journal (changed_at);
CREATE INDEX idx_changes_journal_cursor ON changes_journal ("cursor");

-- Provenance Events table indexes
CREATE INDEX idx_provenance_events_token_id ON provenance_events (token_id);
CREATE INDEX idx_provenance_events_chain ON provenance_events (chain);
CREATE INDEX idx_provenance_events_event_type ON provenance_events (event_type);
CREATE INDEX idx_provenance_events_timestamp ON provenance_events (timestamp);
CREATE INDEX idx_provenance_events_tx_hash ON provenance_events (tx_hash) WHERE tx_hash IS NOT NULL;
CREATE INDEX idx_provenance_events_block_number ON provenance_events (block_number) WHERE block_number IS NOT NULL;

-- Watched Addresses table indexes
CREATE INDEX idx_watched_addresses_watching ON watched_addresses (watching, chain, address);
CREATE INDEX idx_watched_addresses_chain ON watched_addresses (chain);
CREATE INDEX idx_watched_addresses_last_queried_at ON watched_addresses (last_queried_at);

-- Key-Value Store table indexes
CREATE INDEX idx_key_value_store_updated_at ON key_value_store (updated_at);

-- ============================================================================
-- JSONB INDEXES FOR COMPLEX QUERIES
-- ============================================================================

-- JSONB indexes for token metadata
CREATE INDEX idx_token_metadata_origin_json_gin ON token_metadata USING GIN (origin_json);
CREATE INDEX idx_token_metadata_latest_json_gin ON token_metadata USING GIN (latest_json);

-- JSONB indexes for media assets
CREATE INDEX idx_media_assets_cf_variant_map_gin ON media_assets USING GIN (cf_variant_map);

-- JSONB indexes for changes journal
CREATE INDEX idx_changes_journal_meta_gin ON changes_journal USING GIN (meta);

-- JSONB indexes for provenance events
CREATE INDEX idx_provenance_events_raw_gin ON provenance_events USING GIN (raw);

-- ============================================================================
-- TRIGGERS FOR AUTOMATIC UPDATES
-- ============================================================================

-- Function to update updated_at timestamp
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = now();
    RETURN NEW;
END;
$$ language 'plpgsql';

-- Apply updated_at trigger to tokens
CREATE TRIGGER update_tokens_updated_at
    BEFORE UPDATE ON tokens
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

-- Apply updated_at trigger to balances
CREATE TRIGGER update_balances_updated_at
    BEFORE UPDATE ON balances
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

-- Apply updated_at trigger to token_metadata
CREATE TRIGGER update_token_metadata_updated_at
    BEFORE UPDATE ON token_metadata
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

-- Apply updated_at trigger to enrichment_sources
CREATE TRIGGER update_enrichment_sources_updated_at
    BEFORE UPDATE ON enrichment_sources
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

-- Apply updated_at trigger to media_assets
CREATE TRIGGER update_media_assets_updated_at
    BEFORE UPDATE ON media_assets
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

-- Apply updated_at trigger to changes_journal
CREATE TRIGGER update_changes_journal_updated_at
    BEFORE UPDATE ON changes_journal
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

-- Apply updated_at trigger to provenance_events
CREATE TRIGGER update_provenance_events_updated_at
    BEFORE UPDATE ON provenance_events
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

-- Apply updated_at trigger to watched_addresses
CREATE TRIGGER update_watched_addresses_updated_at 
    BEFORE UPDATE ON watched_addresses 
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

-- Apply updated_at trigger to key_value_store
CREATE TRIGGER update_key_value_store_updated_at 
    BEFORE UPDATE ON key_value_store 
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

-- ============================================================================
-- CONSTRAINTS AND VALIDATIONS
-- ============================================================================

-- ENUM types provide automatic validation, no additional CHECK constraints needed

-- Function to ensure tokens are only updated with newer or equal last_activity_time
CREATE OR REPLACE FUNCTION check_last_activity_time()
RETURNS TRIGGER AS $$
BEGIN
    -- If last_activity_time is being updated to an older time, reject the update
    IF NEW.last_activity_time < OLD.last_activity_time THEN
        RAISE EXCEPTION 'Cannot update token with older last_activity_time. Current: %, Attempted: %',
            OLD.last_activity_time, NEW.last_activity_time
            USING ERRCODE = '23514', -- check_violation
                  HINT = 'Token updates must have last_activity_time >= current value';
    END IF;
    
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Trigger to enforce last_activity_time progression on tokens table
CREATE TRIGGER enforce_last_activity_time_progression
    BEFORE UPDATE ON tokens
    FOR EACH ROW
    WHEN (OLD.last_activity_time IS DISTINCT FROM NEW.last_activity_time)
    EXECUTE FUNCTION check_last_activity_time();

COMMENT ON FUNCTION check_last_activity_time() IS 
    'Ensures tokens can only be updated with newer or equal last_activity_time values to maintain temporal consistency';

-- ============================================================================
-- INITIAL DATA
-- ============================================================================

-- Insert initial configuration values
INSERT INTO key_value_store (key, value) VALUES 
    ('ethereum_mainnet_cursor', '0'),
    ('ethereum_sepolia_cursor', '0'),
    ('tezos_mainnet_cursor', '0'),
    ('tezos_narwhal_cursor', '0'),
    ('indexer_version', '2.0.0')
ON CONFLICT (key) DO NOTHING;

-- ============================================================================
-- COMMENTS FOR DOCUMENTATION
-- ============================================================================

COMMENT ON TABLE tokens IS 'Primary entity for tracking tokens across all supported blockchains';
COMMENT ON TABLE balances IS 'Tracks ownership quantities for multi-edition tokens (ERC1155, FA2)';
COMMENT ON TABLE token_metadata IS 'Stores original and enriched metadata for tokens';
COMMENT ON TABLE enrichment_sources IS 'Tracks metadata fetch attempts from various vendors';
COMMENT ON TABLE media_assets IS 'Manages media files with different storage providers integration';
COMMENT ON TABLE changes_journal IS 'Audit log for tracking all changes to indexed data. token_id always points to affected token. subject_id is polymorphic: provenance_event_id (token/owner), balance_id (balance), token_id (metadata), media_asset_id (media)';
COMMENT ON TABLE provenance_events IS 'Optional audit trail of blockchain events';
COMMENT ON TABLE watched_addresses IS 'For owner-based indexing functionality';
COMMENT ON TABLE key_value_store IS 'For configuration and state management';