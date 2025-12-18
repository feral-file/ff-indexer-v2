# Database Schema

This document describes the database schema for FF-Indexer v2, including all tables, relationships, and migration notes.

## Overview

The database uses PostgreSQL 15+ with the following design principles:
- Normalized schema with foreign key relationships
- JSONB columns for flexible metadata storage
- Indexes optimized for common query patterns
- Audit logging via changes_journal table
- Support for multiple blockchains and token standards

## Core Tables

The database includes the following main tables:

- `tokens` - Primary token entity across all blockchains
- `token_metadata` - NFT metadata (name, description, media, attributes, etc.)
- `enrichment_sources` - Additional data sources enriching token information
- `balances` - Current token ownership balances (multi-edition tokens)
- `token_ownership_periods` - Historical ownership periods for efficient address-based queries
- `provenance_events` - Historical provenance events (mint, transfer, burn, etc.)
- `media_assets` - Media files associated with tokens (images, videos, etc.)
- `changes_journal` - Change tracking for all entities
- `watched_addresses` - Addresses being monitored for indexing
- `key_value_store` - Configuration and state management

### tokens

Primary entity for tracking tokens across all supported blockchains.

| Column | Type | Description |
|--------|------|-------------|
| id | BIGSERIAL | Primary key |
| token_cid | TEXT | Canonical token ID (format: `chain:standard:contract:tokenNumber`) |
| chain | blockchain_chain | Blockchain identifier (eip155:1, tezos:mainnet, etc.) |
| standard | token_standard | Token standard (erc721, erc1155, fa2) |
| contract_address | TEXT | Smart contract address |
| token_number | TEXT | Token ID within contract |
| current_owner | TEXT | Current owner address (NULL for multi-owner tokens) |
| burned | BOOLEAN | Whether token has been burned |
| created_at | TIMESTAMPTZ | Record creation timestamp |
| updated_at | TIMESTAMPTZ | Last update timestamp |

**Indexes**:
- `idx_tokens_chain_contract_number` on (chain, contract_address, token_number)
- `idx_tokens_current_owner` on (current_owner) WHERE current_owner IS NOT NULL
- `idx_tokens_burned` on (burned) WHERE burned
- `idx_tokens_created_at` on (created_at)

**Unique Constraints**:
- `token_cid` (unique)
- `(chain, contract_address, token_number)` (unique)

### token_metadata

Stores original and enriched metadata for tokens.

| Column | Type | Description |
|--------|------|-------------|
| token_id | BIGINT | Foreign key to tokens.id |
| origin_json | JSONB | Original metadata from blockchain/IPFS |
| latest_json | JSONB | Latest metadata from blockchain/IPFS |
| latest_hash | TEXT | Hash of latest_json for change detection |
| enrichment_level | enrichment_level | Enrichment status (none, vendor) |
| last_refreshed_at | TIMESTAMPTZ | Last metadata refresh timestamp |
| image_url | TEXT | Normalized image URL |
| animation_url | TEXT | Normalized animation/video URL |
| name | TEXT | Token name |
| description | TEXT | Token description |
| artists | JSONB | Array of artist information |
| publisher | JSONB | Publisher information |
| mime_type | TEXT | MIME type of media |
| created_at | TIMESTAMPTZ | Record creation timestamp |
| updated_at | TIMESTAMPTZ | Last update timestamp |

**Indexes**:
- `idx_token_metadata_enrichment_level` on (enrichment_level)
- `idx_token_metadata_last_refreshed_at` on (last_refreshed_at)
- `idx_token_metadata_artists` GIN on (artists) WHERE artists IS NOT NULL
- `idx_token_metadata_publisher` GIN on (publisher) WHERE publisher IS NOT NULL
- `idx_token_metadata_origin_json_gin` GIN on (origin_json)
- `idx_token_metadata_latest_json_gin` GIN on (latest_json)

**Relationships**:
- One-to-one with `tokens`

### enrichment_sources

Stores enriched metadata from vendor APIs (Art Blocks, fxhash, Foundation, SuperRare, Feral File, Objkt, OpenSea).

| Column | Type | Description |
|--------|------|-------------|
| token_id | BIGINT | Foreign key to tokens.id |
| vendor | vendor_type | Vendor name (artblocks, fxhash, foundation, superrare, feralfile, objkt, opensea) |
| vendor_json | JSONB | Raw API response |
| vendor_hash | TEXT | Hash of vendor_json for change detection |
| image_url | TEXT | Normalized image URL from vendor |
| animation_url | TEXT | Normalized animation URL from vendor |
| name | TEXT | Normalized name from vendor |
| description | TEXT | Normalized description from vendor |
| artists | JSONB | Normalized artists array |
| mime_type | TEXT | MIME type detected from URLs |
| created_at | TIMESTAMPTZ | Record creation timestamp |
| updated_at | TIMESTAMPTZ | Last update timestamp |

**Indexes**:
- `idx_enrichment_sources_vendor` on (vendor)
- `idx_enrichment_sources_vendor_hash` on (vendor_hash) WHERE vendor_hash IS NOT NULL
- `idx_enrichment_sources_artists` GIN on (artists) WHERE artists IS NOT NULL
- `idx_enrichment_sources_vendor_json_gin` GIN on (vendor_json)

**Relationships**:
- One-to-one with `tokens`

### balances

Tracks ownership quantities for multi-edition tokens (ERC1155, FA2).

| Column | Type | Description |
|--------|------|-------------|
| id | BIGSERIAL | Primary key |
| token_id | BIGINT | Foreign key to tokens.id |
| owner_address | TEXT | Owner's blockchain address |
| quantity | NUMERIC(78,0) | Number of tokens owned |
| created_at | TIMESTAMPTZ | Record creation timestamp |
| updated_at | TIMESTAMPTZ | Last update timestamp |

**Indexes**:
- `idx_balances_token_owner` on (token_id, owner_address)
- `idx_balances_owner_address` on (owner_address)
- `idx_balances_updated_at` on (updated_at)

**Unique Constraints**:
- `(token_id, owner_address)` (unique)

**Relationships**:
- Many-to-one with `tokens`

---

### token_ownership_periods

Tracks historical ownership periods for tokens to efficiently query metadata and media changes that occurred during an address's ownership. This table pre-computes ownership time windows to optimize the `GetChanges` query when filtering by addresses.

**Purpose:**
- Enables fast address-based filtering for metadata/media changes
- Avoids expensive subqueries with `provenance_events` table
- Supports both single-owner (ERC721) and multi-edition (ERC1155/FA2) tokens

**Schema:**

| Column | Type | Description |
|--------|------|-------------|
| `id` | BIGSERIAL | Primary key |
| `token_id` | BIGINT | Reference to token (FK, CASCADE DELETE) |
| `owner_address` | TEXT | Address that owned the token |
| `acquired_at` | TIMESTAMPTZ | When the address first received the token |
| `released_at` | TIMESTAMPTZ | When the address's balance became 0 (NULL if still owns) |
| `created_at` | TIMESTAMPTZ | Record creation timestamp |
| `updated_at` | TIMESTAMPTZ | Last update timestamp (auto-updated by trigger) |

**Indexes:**
- `unique_active_token_owner` (partial unique) on `(token_id, owner_address) WHERE released_at IS NULL`
- `idx_token_ownership_token_owner_periods` on `(token_id, owner_address, acquired_at, released_at)`
- `idx_token_ownership_owner_periods` on `(owner_address, acquired_at, released_at)`
- `idx_token_ownership_token_id_text_periods` on `(CAST(token_id AS TEXT), owner_address, acquired_at, released_at)` - for text-based joins
- `idx_token_ownership_current_owners` (partial) on `(token_id, owner_address) WHERE released_at IS NULL`

**Relationships**:
- Many-to-one with `tokens` (CASCADE DELETE)

---

### media_assets

Reference mapping between original URLs and provider-hosted URLs with variants.

| Column | Type | Description |
|--------|------|-------------|
| id | BIGSERIAL | Primary key |
| source_url | TEXT | Original URL where media was found |
| mime_type | TEXT | MIME type (image/jpeg, video/mp4, etc.) |
| file_size_bytes | BIGINT | File size in bytes |
| provider | storage_provider | Storage provider (self_hosted, cloudflare, s3) |
| provider_asset_id | TEXT | Provider-specific ID (cf_image_id, s3 key, etc.) |
| provider_metadata | JSONB | Provider-specific metadata |
| variant_urls | JSONB | URL variants (thumbnail, medium, original) |
| created_at | TIMESTAMPTZ | Record creation timestamp |
| updated_at | TIMESTAMPTZ | Last update timestamp |

**Indexes**:
- `idx_media_assets_source_url` on (source_url)
- `idx_media_assets_provider` on (provider)
- `idx_media_assets_provider_asset_id` on (provider, provider_asset_id)
- `idx_media_assets_created_at` on (created_at)
- `idx_media_assets_variant_urls_gin` GIN on (variant_urls)
- `idx_media_assets_provider_metadata_gin` GIN on (provider_metadata) WHERE provider_metadata IS NOT NULL

**Unique Constraints**:
- `(provider, provider_asset_id)` (unique)
- `(source_url, provider)` (unique)

### provenance_events

Optional audit trail of blockchain events.

| Column | Type | Description |
|--------|------|-------------|
| id | BIGSERIAL | Primary key |
| token_id | BIGINT | Foreign key to tokens.id |
| chain | blockchain_chain | Blockchain identifier |
| event_type | event_type | Event type (mint, transfer, burn, metadata_update) |
| from_address | TEXT | Sender address (NULL for mints) |
| to_address | TEXT | Recipient address (NULL for burns) |
| quantity | NUMERIC(78,0) | Token quantity |
| tx_hash | TEXT | Transaction hash |
| block_number | BIGINT | Block number |
| block_hash | TEXT | Block hash |
| timestamp | TIMESTAMPTZ | Blockchain timestamp |
| raw | JSONB | Complete raw event data |
| created_at | TIMESTAMPTZ | Record creation timestamp |
| updated_at | TIMESTAMPTZ | Last update timestamp |

**Indexes**:
- `idx_provenance_events_token_id` on (token_id)
- `idx_provenance_events_chain` on (chain)
- `idx_provenance_events_event_type` on (event_type)
- `idx_provenance_events_timestamp` on (timestamp)
- `idx_provenance_events_tx_hash` on (tx_hash) WHERE tx_hash IS NOT NULL
- `idx_provenance_events_block_number` on (block_number) WHERE block_number IS NOT NULL
- `idx_provenance_events_raw` GIN on (raw)

**Unique Constraints**:
- `(chain, tx_hash, token_id, from_address, to_address, event_type)` - Allows multiple events in the same transaction for different tokens or address pairs

**Relationships**:
- Many-to-one with `tokens`

### changes_journal

Audit log for tracking all changes to indexed data.

| Column | Type | Description |
|--------|------|-------------|
| id | BIGSERIAL | Primary key |
| subject_type | subject_type | Type of change (token, owner, balance, metadata, enrich_source, media_asset) |
| subject_id | TEXT | Polymorphic reference (provenance_event_id for token/owner/balance; token_id for metadata/enrich_source; media_asset_id for media_asset) |
| changed_at | TIMESTAMPTZ | Change timestamp |
| meta | JSONB | Change metadata (ProvenanceChangeMeta, MetadataChangeMeta, EnrichmentSourceChangeMeta, or MediaAssetChangeMeta) |
| created_at | TIMESTAMPTZ | Record creation timestamp |
| updated_at | TIMESTAMPTZ | Last update timestamp |

**Note**: Token association is resolved through `subject_id` based on `subject_type`. For provenance changes (token/owner/balance), the token is found via the `provenance_events` table. For metadata/enrich_source changes, `subject_id` directly contains the token_id. Media asset changes don't have a direct token association.

**Indexes**:
- `idx_changes_journal_subject` on (subject_type, subject_id)
- `idx_changes_journal_changed_at` on (changed_at)
- `idx_changes_journal_subject_type` on (subject_type)
- `idx_changes_journal_meta_gin` GIN on (meta)

**Unique Constraints**:
- `(subject_type, subject_id, changed_at)` (unique)

**Relationships**:
- Indirect relationship to `tokens` through `subject_id` (resolved based on `subject_type`)

### watched_addresses

For owner-based indexing functionality.

| Column | Type | Description |
|--------|------|-------------|
| chain | TEXT | Blockchain identifier |
| address | TEXT | Wallet address to watch |
| watching | BOOLEAN | Whether currently watching |
| last_queried_at | TIMESTAMPTZ | Last API query timestamp |
| last_successful_indexing_blk_range | JSONB | Last successful block range per chain |
| created_at | TIMESTAMPTZ | Record creation timestamp |
| updated_at | TIMESTAMPTZ | Last update timestamp |

**Indexes**:
- `idx_watched_addresses_watching` on (watching, chain, address)
- `idx_watched_addresses_chain` on (chain)
- `idx_watched_addresses_last_queried_at` on (last_queried_at)

**Primary Key**:
- `(chain, address)`

### key_value_store

Configuration and state management (cursors, version, etc.).

| Column | Type | Description |
|--------|------|-------------|
| key | TEXT | Key name (primary key) |
| value | TEXT | Value (stored as text) |
| updated_at | TIMESTAMPTZ | Last update timestamp |
| created_at | TIMESTAMPTZ | Record creation timestamp |

**Indexes**:
- `idx_key_value_store_updated_at` on (updated_at)

**Common Keys**:
- `ethereum_mainnet_cursor` - Last processed block for Ethereum mainnet
- `ethereum_sepolia_cursor` - Last processed block for Ethereum Sepolia
- `tezos_mainnet_cursor` - Last processed level for Tezos mainnet
- `tezos_ghostnet_cursor` - Last processed level for Tezos ghostnet
- `indexer_version` - Current indexer version

## ENUM Types

### token_standard
- `erc721` - ERC-721 standard
- `erc1155` - ERC-1155 standard
- `fa2` - Tezos FA2 standard

### blockchain_chain
- `eip155:1` - Ethereum mainnet
- `eip155:11155111` - Ethereum Sepolia testnet
- `tezos:mainnet` - Tezos mainnet
- `tezos:ghostnet` - Tezos ghostnet testnet

### enrichment_level
- `none` - No enrichment applied
- `vendor` - Enriched from vendor API

### vendor_type
- `artblocks` - Art Blocks
- `fxhash` - fxhash
- `foundation` - Foundation
- `superrare` - SuperRare
- `feralfile` - Feral File
- `objkt` - Objkt
- `opensea` - OpenSea

### storage_provider
- `self_hosted` - Self-hosted storage
- `cloudflare` - Cloudflare Images/Stream
- `s3` - Amazon S3

### subject_type
- `token` - Token changes
- `owner` - Owner changes
- `balance` - Balance changes
- `metadata` - Metadata changes
- `enrich_source` - Enrichment source changes
- `media_asset` - Media asset changes

### event_type
- `mint` - Token mint event
- `transfer` - Token transfer event
- `burn` - Token burn event
- `metadata_update` - Metadata update event

## Relationships

```
tokens (1)
  ├── (1) token_metadata
  ├── (N) balances
  ├── (N) provenance_events
  └── (N) enrichment_sources

changes_journal (standalone, references other entities via subject_id)

media_assets (standalone)

watched_addresses (standalone)

key_value_store (standalone)
```

**Note on changes_journal relationships**: The changes_journal table doesn't have a direct foreign key to tokens. Instead, it uses a polymorphic pattern where the token association is resolved through `subject_id` based on `subject_type`:
- For `token`/`owner`/`balance`: Join via `provenance_events.id = subject_id`, then get token
- For `metadata`/`enrich_source`: `subject_id` IS the token_id
- For `media_asset`: No direct token association

## Triggers

All tables with `updated_at` columns have triggers that automatically update the timestamp on row updates:

- `update_tokens_updated_at`
- `update_balances_updated_at`
- `update_token_metadata_updated_at`
- `update_enrichment_sources_updated_at`
- `update_media_assets_updated_at`
- `update_changes_journal_updated_at`
- `update_provenance_events_updated_at`
- `update_watched_addresses_updated_at`
- `update_key_value_store_updated_at`
- `update_token_ownership_periods_updated_at`

## Migrations

### Initial Schema (init_pg_db.sql)

**Migration Notes**:
- All ENUM types are created first
- Tables are created in dependency order
- Indexes are created after tables
- Triggers are created last
- Initial data is inserted into `key_value_store`

### Future Migrations

Migrations should be placed in `db/migrations/` directory with sequential numbering:
- `001.sql` - Add the `token_ownership_periods` table, migrate the existing data.
- `002.sql` - Future changes
- etc.

**Migration Guidelines**:
1. Always test migrations on a copy of production data
2. Use transactions for atomic migrations
3. Add indexes concurrently for large tables
4. Document breaking changes
5. Provide rollback scripts if possible

## Indexing Strategy

### Primary Indexes

- **Composite indexes** for common query patterns (chain + contract + token)
- **Partial indexes** for filtered queries (WHERE burned = true, WHERE current_owner IS NOT NULL)
- **GIN indexes** for JSONB columns used in queries

### Query Patterns

**Common Queries**:
1. Get token by TokenCID → `token_cid` unique index
2. Get tokens by owner → `idx_tokens_current_owner` or `idx_balances_owner_address`
3. Get metadata → `token_id` foreign key
4. Get provenance events → `idx_provenance_events_token_id`
5. Search metadata → GIN indexes on JSONB columns

## Performance Considerations

1. **JSONB Columns**: Use GIN indexes for JSONB queries, but avoid frequent JSONB updates
2. **Foreign Keys**: All foreign keys have CASCADE delete for data integrity
3. **Unique Constraints**: Prefer unique indexes over unique constraints for better performance
4. **Partial Indexes**: Use WHERE clauses to reduce index size
5. **Connection Pooling**: Use connection pooling in application layer

## Data Retention

- **Provenance Events**: Optional, can be disabled for high-volume contracts
- **Changes Journal**: Audit log, consider archiving old records
- **Media Assets**: Keep indefinitely for reference
- **Tokens**: Keep all indexed tokens

