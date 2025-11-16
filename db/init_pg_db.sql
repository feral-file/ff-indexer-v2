-- Feral File Indexer Database Schema
-- This script initializes the complete database schema for the FF Indexer v2
-- Supports Ethereum (ERC-721, ERC-1155) and Tezos (FA2) token indexing

-- Create ENUM types for better type safety and performance
CREATE TYPE token_standard AS ENUM ('erc721', 'erc1155', 'fa2');
CREATE TYPE blockchain_chain AS ENUM ('eip155:1', 'eip155:11155111', 'tezos:mainnet', 'tezos:ghostnet');
CREATE TYPE enrichment_level AS ENUM ('none', 'vendor');
CREATE TYPE vendor_type AS ENUM ('artblocks', 'fxhash', 'foundation', 'superrare', 'feralfile', 'objkt');
CREATE TYPE storage_provider AS ENUM ('self_hosted', 'cloudflare', 's3');
CREATE TYPE subject_type AS ENUM ('token', 'owner', 'balance', 'metadata', 'enrich_source', 'media_asset');
CREATE TYPE event_type AS ENUM ('mint', 'transfer', 'burn', 'metadata_update');

-- ============================================================================
-- CORE TABLES
-- ============================================================================

-- Tokens table - Primary entity for tracking tokens across all blockchains
CREATE TABLE tokens (
    id BIGSERIAL PRIMARY KEY,
    token_cid TEXT NOT NULL UNIQUE,             -- canonical id: eip155:1:erc721:0x...:1234
    chain blockchain_chain NOT NULL,           -- eip155:1 or tezos:mainnet
    standard token_standard NOT NULL,          -- erc721, erc1155, fa2
    contract_address TEXT NOT NULL,
    token_number TEXT NOT NULL,
    current_owner TEXT,
    burned BOOLEAN NOT NULL DEFAULT FALSE,
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
    description TEXT,
    artists JSONB,
    publisher JSONB,
    mime_type TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Enrichment Sources table - Stores enriched metadata from vendor APIs
CREATE TABLE enrichment_sources (
    token_id BIGINT PRIMARY KEY REFERENCES tokens (id) ON DELETE CASCADE,
    vendor vendor_type NOT NULL,           -- 'artblocks', 'fxhash', 'foundation', 'superrare', 'feralfile'
    vendor_json JSONB,                     -- raw response from vendor API
    vendor_hash TEXT,                      -- hash of vendor_json to detect changes
    image_url TEXT,                        -- normalized image URL from vendor
    animation_url TEXT,                    -- normalized animation URL from vendor
    name TEXT,                             -- normalized name from vendor
    description TEXT,                      -- normalized description from vendor
    artists JSONB,                         -- normalized artists array from vendor
    mime_type TEXT,                        -- MIME type of the artwork (detected from animation_url or image_url)
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Media Assets table - Reference mapping between original URLs and provider URLs
CREATE TABLE media_assets (
    id BIGSERIAL PRIMARY KEY,
    
    -- Original source
    source_url TEXT NOT NULL,               -- Original URL where media was found
    mime_type TEXT,                         -- image/jpeg, video/mp4, etc.
    file_size_bytes BIGINT,
    
    -- Storage provider info
    provider storage_provider NOT NULL,
    provider_asset_id TEXT,                 -- provider-specific ID (cf_image_id, s3 key, etc.)
    provider_metadata JSONB,                -- provider-specific data (e.g., cloudflare account info)
    
    -- Variant URLs (actual URLs, not URIs)
    variant_urls JSONB NOT NULL,            -- {"thumbnail": "https://...", "medium": "https://...", "original": "https://..."}
    
    -- Timestamps
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    
    -- Unique constraints: one entry per provider per asset, one entry per source URL per provider
    UNIQUE (provider, provider_asset_id),
    UNIQUE (source_url, provider)
);

-- Changes Journal table - Audit log for tracking all changes to indexed data
CREATE TABLE changes_journal (
    id BIGSERIAL PRIMARY KEY,
    subject_type subject_type NOT NULL,     -- token, owner, balance, metadata, enrich_source, media_asset
    subject_id TEXT NOT NULL,               -- polymorphic ref: provenance_event_id for token/owner/balance; token_id for metadata/enrich_source; media_asset_id for media_asset
    changed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    meta JSONB,                             -- ProvenanceChangeMeta for token/owner/balance; MetadataChangeMeta for metadata; EnrichmentSourceChangeMeta for enrich_source; MediaAssetChangeMeta for media_asset
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (subject_type, subject_id, changed_at)
);

-- Provenance Events table - Optional audit trail of blockchain events
CREATE TABLE provenance_events (
    id BIGSERIAL PRIMARY KEY,
    token_id BIGINT NOT NULL REFERENCES tokens (id) ON DELETE CASCADE,
    chain blockchain_chain NOT NULL,
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

-- Unique constraint to prevent duplicate events
-- Combines chain, tx_hash, token_id, addresses, and event_type
-- This allows multiple events in the same transaction for different tokens or different address pairs
-- Note: NULL values in from_address/to_address are treated as distinct values in unique constraints
ALTER TABLE provenance_events
ADD CONSTRAINT provenance_events_unique UNIQUE (
    chain,
    tx_hash,
    token_id,
    from_address,
    to_address,
    event_type
);

-- Token Ownership Periods table - Tracks when addresses owned tokens with quantity > 0
-- Used for efficiently querying ownership history and metadata changes during ownership
CREATE TABLE token_ownership_periods (
    id BIGSERIAL PRIMARY KEY,
    token_id BIGINT NOT NULL REFERENCES tokens (id) ON DELETE CASCADE,
    owner_address TEXT NOT NULL,
    acquired_at TIMESTAMPTZ NOT NULL,       -- When the address acquired the token (first transfer in)
    released_at TIMESTAMPTZ,                -- When the address released the token (balance became 0); NULL means still owns it
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Create partial unique index to ensure only one active ownership period per token-owner combination
CREATE UNIQUE INDEX unique_active_token_owner ON token_ownership_periods (token_id, owner_address) WHERE (released_at IS NULL);

-- Watched Addresses table - For owner-based indexing
CREATE TABLE watched_addresses (
    chain          TEXT        NOT NULL,
    address        TEXT        NOT NULL,
    watching       BOOLEAN     NOT NULL DEFAULT TRUE,
    last_queried_at TIMESTAMPTZ,          -- when API last queried this address
    last_successful_indexing_blk_range JSONB, -- {"eip155:1": {"min_block": 123, "max_block": 456}}
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
CREATE INDEX idx_tokens_created_at ON tokens (created_at);

-- Balances table indexes
CREATE INDEX idx_balances_token_owner ON balances (token_id, owner_address);
CREATE INDEX idx_balances_owner_address ON balances (owner_address);
CREATE INDEX idx_balances_updated_at ON balances (updated_at);

-- Token Metadata table indexes
CREATE INDEX idx_token_metadata_enrichment_level ON token_metadata (enrichment_level);
CREATE INDEX idx_token_metadata_last_refreshed_at ON token_metadata (last_refreshed_at);
CREATE INDEX idx_token_metadata_artists ON token_metadata USING GIN (artists) WHERE artists IS NOT NULL AND jsonb_array_length(artists) > 0;
CREATE INDEX idx_token_metadata_publisher ON token_metadata USING GIN (publisher) WHERE publisher IS NOT NULL AND jsonb_typeof(publisher) = 'object';

-- Enrichment Sources table indexes
CREATE INDEX idx_enrichment_sources_vendor ON enrichment_sources (vendor);
CREATE INDEX idx_enrichment_sources_vendor_hash ON enrichment_sources (vendor_hash) WHERE vendor_hash IS NOT NULL;
CREATE INDEX idx_enrichment_sources_artists ON enrichment_sources USING GIN (artists) WHERE artists IS NOT NULL;

-- Media Assets table indexes
CREATE INDEX idx_media_assets_source_url ON media_assets (source_url);
CREATE INDEX idx_media_assets_provider ON media_assets (provider);
CREATE INDEX idx_media_assets_provider_asset_id ON media_assets (provider, provider_asset_id);
CREATE INDEX idx_media_assets_created_at ON media_assets (created_at);

-- Changes Journal table indexes
CREATE INDEX idx_changes_journal_subject ON changes_journal (subject_type, subject_id);
CREATE INDEX idx_changes_journal_changed_at ON changes_journal (changed_at);
CREATE INDEX idx_changes_journal_subject_type ON changes_journal (subject_type);
CREATE INDEX idx_changes_journal_subject_id ON changes_journal (subject_id);
CREATE INDEX idx_changes_journal_subject_type_changed_at_id ON changes_journal (subject_type, changed_at, id);
CREATE INDEX idx_changes_journal_changed_at_id ON changes_journal (changed_at, id);

-- Provenance Events table indexes
CREATE INDEX idx_provenance_events_token_id ON provenance_events (token_id);
CREATE INDEX idx_provenance_events_chain ON provenance_events (chain);
CREATE INDEX idx_provenance_events_event_type ON provenance_events (event_type);
CREATE INDEX idx_provenance_events_timestamp ON provenance_events (timestamp);
CREATE INDEX idx_provenance_events_tx_hash ON provenance_events (tx_hash) WHERE tx_hash IS NOT NULL;
CREATE INDEX idx_provenance_events_block_number ON provenance_events (block_number) WHERE block_number IS NOT NULL;
CREATE INDEX idx_provenance_events_raw ON provenance_events USING GIN (raw);
CREATE INDEX idx_provenance_events_from_address ON provenance_events (from_address);
CREATE INDEX idx_provenance_events_to_address ON provenance_events (to_address);
CREATE INDEX idx_provenance_events_token_id_from_address_timestamp ON provenance_events (token_id, from_address, timestamp);
CREATE INDEX idx_provenance_events_token_id_to_address_timestamp ON provenance_events (token_id, to_address, timestamp);
CREATE INDEX idx_provenance_events_id_text ON provenance_events (CAST(id AS TEXT));

-- Token Ownership Periods table indexes
CREATE INDEX idx_token_ownership_token_owner_periods ON token_ownership_periods (token_id, owner_address, acquired_at, released_at);
CREATE INDEX idx_token_ownership_owner_periods ON token_ownership_periods (owner_address, acquired_at, released_at);
CREATE INDEX idx_token_ownership_token_id_text_periods ON token_ownership_periods (CAST(token_id AS TEXT), owner_address, acquired_at, released_at);
CREATE INDEX idx_token_ownership_current_owners ON token_ownership_periods (token_id, owner_address) WHERE released_at IS NULL;

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

-- JSONB indexes for enrichment sources
CREATE INDEX idx_enrichment_sources_vendor_json_gin ON enrichment_sources USING GIN (vendor_json);

-- JSONB indexes for media assets
CREATE INDEX idx_media_assets_variant_urls_gin ON media_assets USING GIN (variant_urls);
CREATE INDEX idx_media_assets_provider_metadata_gin ON media_assets USING GIN (provider_metadata) WHERE provider_metadata IS NOT NULL;

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

-- Apply updated_at trigger to token_ownership_periods
CREATE TRIGGER update_token_ownership_periods_updated_at 
    BEFORE UPDATE ON token_ownership_periods 
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

-- ============================================================================
-- INITIAL DATA
-- ============================================================================

-- Insert initial configuration values
INSERT INTO key_value_store (key, value) VALUES 
    ('ethereum_mainnet_cursor', '0'),
    ('ethereum_sepolia_cursor', '0'),
    ('tezos_mainnet_cursor', '0'),
    ('tezos_ghostnet_cursor', '0'),
    ('indexer_version', '2.0.0')
ON CONFLICT (key) DO NOTHING;

-- ============================================================================
-- COMMENTS FOR DOCUMENTATION
-- ============================================================================

COMMENT ON TABLE tokens IS 'Primary entity for tracking tokens across all supported blockchains';
COMMENT ON TABLE balances IS 'Tracks ownership quantities for multi-edition tokens (ERC1155, FA2)';
COMMENT ON TABLE token_metadata IS 'Stores original and enriched metadata for tokens';
COMMENT ON TABLE enrichment_sources IS 'Stores enriched metadata from vendor APIs (Art Blocks, fxhash, Foundation, SuperRare, Feral File) with both raw and normalized data';
COMMENT ON TABLE media_assets IS 'Reference mapping between original URLs and provider-hosted URLs with variants. Acts as a generic media reference tracker for any uploaded media across different storage providers';
COMMENT ON TABLE changes_journal IS 'Audit log for tracking all changes to indexed data. subject_id is polymorphic: provenance_event_id (token/owner/balance), token_id (metadata/enrich_source), media_asset_id (media_asset). Token association is resolved through subject_id based on subject_type';
COMMENT ON TABLE provenance_events IS 'Optional audit trail of blockchain events';
COMMENT ON TABLE token_ownership_periods IS 'Tracks when addresses owned tokens with quantity > 0. Used for efficiently querying ownership history and metadata changes during ownership. Automatically maintained by application logic on provenance event creation';
COMMENT ON TABLE watched_addresses IS 'For owner-based indexing functionality';
COMMENT ON TABLE key_value_store IS 'For configuration and state management';