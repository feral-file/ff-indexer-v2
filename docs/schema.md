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
- `token_media_health` - Health status of token media URLs
- `changes_journal` - Change tracking for all entities
- `watched_addresses` - Addresses being monitored for indexing
- `address_indexing_jobs` - Address-level indexing job status tracking
- `webhook_clients` - Registered webhook clients for event notifications
- `webhook_deliveries` - Audit log of webhook delivery attempts
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
| image_url_hash | TEXT | MD5 hash of image_url for efficient indexing |
| animation_url | TEXT | Normalized animation/video URL |
| animation_url_hash | TEXT | MD5 hash of animation_url for efficient indexing |
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
- `idx_token_metadata_image_url_hash` on (image_url_hash) WHERE image_url IS NOT NULL
- `idx_token_metadata_animation_url_hash` on (animation_url_hash) WHERE animation_url IS NOT NULL

**Relationships**:
- One-to-one with `tokens`

**Note**: MD5 hash columns enable efficient indexing of URLs without size limitations, as hash values are fixed-length (32 characters). This resolves index size constraints that can occur with variable-length URLs.

### enrichment_sources

Stores enriched metadata from vendor APIs (Art Blocks, fxhash, Foundation, SuperRare, Feral File, Objkt, OpenSea).

| Column | Type | Description |
|--------|------|-------------|
| token_id | BIGINT | Foreign key to tokens.id |
| vendor | vendor_type | Vendor name (artblocks, fxhash, foundation, superrare, feralfile, objkt, opensea) |
| vendor_json | JSONB | Raw API response |
| vendor_hash | TEXT | Hash of vendor_json for change detection |
| image_url | TEXT | Normalized image URL from vendor |
| image_url_hash | TEXT | MD5 hash of image_url for efficient indexing |
| animation_url | TEXT | Normalized animation URL from vendor |
| animation_url_hash | TEXT | MD5 hash of animation_url for efficient indexing |
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
- `idx_enrichment_sources_image_url_hash` on (image_url_hash) WHERE image_url IS NOT NULL
- `idx_enrichment_sources_animation_url_hash` on (animation_url_hash) WHERE animation_url IS NOT NULL

**Relationships**:
- One-to-one with `tokens`

**Note**: MD5 hash columns enable efficient indexing of URLs without size limitations, as hash values are fixed-length (32 characters). This resolves index size constraints that can occur with variable-length URLs.

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

### token_media_health

Tracks health check status for media URLs associated with tokens. The sweeper service continuously monitors these URLs and updates their status.

| Column | Type | Description |
|--------|------|-------------|
| id | BIGSERIAL | Primary key |
| token_id | BIGINT | Foreign key to tokens.id |
| media_url | TEXT | URL being checked for health |
| media_url_hash | TEXT | MD5 hash of media_url for efficient indexing |
| media_source | TEXT | Source of URL (metadata_image, metadata_animation, enrichment_image, enrichment_animation) |
| health_status | media_health_status | Health status (unknown, healthy, broken, checking) |
| last_checked_at | TIMESTAMPTZ | Last health check timestamp |
| last_error | TEXT | Error message from last failed check (NULL if healthy) |
| created_at | TIMESTAMPTZ | Record creation timestamp |
| updated_at | TIMESTAMPTZ | Last update timestamp |

**Indexes**:
- `idx_token_media_health_token_id` on (token_id)
- `idx_token_media_health_url_hash` on (media_url_hash)
- `idx_token_media_health_last_checked` on (last_checked_at)
- `idx_token_media_health_token_status_source` on (token_id, health_status, media_source)

**Unique Constraints**:
- `(token_id, media_url, media_source)` (unique)

**Relationships**:
- Many-to-one with `tokens`

**Purpose**:
- Enables API clients to filter out tokens with broken media URLs
- Tracks health of both metadata and enrichment source URLs
- Supports alternative gateway discovery for IPFS/Arweave/OnChFS
- Animation URLs have precedence over image URLs for filtering

**Note**: The `media_url_hash` column uses MD5 hashing to enable efficient URL lookups without index size limitations. This is particularly important for long URLs that would otherwise exceed PostgreSQL's B-tree index size limits.

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
- `idx_provenance_events_from_address` on (from_address)
- `idx_provenance_events_to_address` on (to_address)
- `idx_provenance_events_token_id_from_address_timestamp` on (token_id, from_address, timestamp)
- `idx_provenance_events_token_id_to_address_timestamp` on (token_id, to_address, timestamp)
- `idx_provenance_events_id_text` on (CAST(id AS TEXT))
- `idx_provenance_events_raw_gin` GIN on (raw)

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
- `idx_changes_journal_subject_id` on (subject_id)
- `idx_changes_journal_subject_type_changed_at_id` on (subject_type, changed_at, id)
- `idx_changes_journal_changed_at_id` on (changed_at, id)
- `idx_changes_journal_meta_gin` GIN on (meta)

**Unique Constraints**:
- `(subject_type, subject_id, changed_at)` (unique)

**Relationships**:
- Indirect relationship to `tokens` through `subject_id` (resolved based on `subject_type`)

### watched_addresses

For owner-based indexing functionality with budgeted indexing mode support.

| Column | Type | Description |
|--------|------|-------------|
| chain | TEXT | Blockchain identifier |
| address | TEXT | Wallet address to watch |
| watching | BOOLEAN | Whether currently watching |
| last_queried_at | TIMESTAMPTZ | Last API query timestamp |
| last_successful_indexing_blk_range | JSONB | Last successful block range per chain (format: `{"eip155:1": {"min_block": 123, "max_block": 456}}`) |
| daily_token_quota | INTEGER | Maximum tokens per 24-hour period (default: 1000) |
| quota_reset_at | TIMESTAMPTZ | When current 24-hour quota period ends (rolling window) |
| tokens_indexed_today | INTEGER | Number of tokens indexed in current quota period (default: 0) |
| created_at | TIMESTAMPTZ | Record creation timestamp |
| updated_at | TIMESTAMPTZ | Last update timestamp |

**Indexes**:
- `idx_watched_addresses_watching` on (watching, chain, address)
- `idx_watched_addresses_chain` on (chain)
- `idx_watched_addresses_last_queried_at` on (last_queried_at)

**Primary Key**:
- `(chain, address)`

**Note**: The budgeted indexing mode fields (`daily_token_quota`, `quota_reset_at`, `tokens_indexed_today`) are used to throttle token indexing to prevent excessive API calls or quota usage.

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

### address_indexing_jobs

Tracks address-level indexing job status independent of Temporal workflows. Decouples job status from Temporal for easier querying via REST/GraphQL APIs.

| Column | Type | Description |
|--------|------|-------------|
| id | BIGSERIAL | Primary key |
| address | TEXT | Blockchain address being indexed |
| chain | blockchain_chain | Blockchain identifier |
| status | indexing_job_status | Job status (running, paused, failed, completed, canceled) |
| workflow_id | TEXT | Temporal workflow ID for correlation |
| workflow_run_id | TEXT | Temporal run ID (may be null initially) |
| tokens_processed | INTEGER | Number of tokens processed (default: 0) |
| current_min_block | BIGINT | Current minimum block indexed |
| current_max_block | BIGINT | Current maximum block indexed |
| started_at | TIMESTAMPTZ | When the job started |
| paused_at | TIMESTAMPTZ | When the job was paused (quota exhausted) |
| completed_at | TIMESTAMPTZ | When the job completed successfully |
| failed_at | TIMESTAMPTZ | When the job failed |
| canceled_at | TIMESTAMPTZ | When the job was canceled |
| created_at | TIMESTAMPTZ | Record creation timestamp |
| updated_at | TIMESTAMPTZ | Last update timestamp |

**Indexes**:
- `idx_address_indexing_jobs_workflow_id` (unique) on (workflow_id) - For querying job by workflow ID
- `idx_address_indexing_jobs_address_chain_created` on (address, chain, created_at DESC) - For querying jobs by address
- `idx_address_indexing_jobs_status_created` on (status, created_at DESC) - For querying jobs by status

**Use Cases**:
- Query job status via REST API (`GET /api/v1/indexing/jobs/{workflow_id}`)
- Query job status via GraphQL (`indexingJob(workflow_id)`)
- Track progress of owner-based indexing workflows
- Monitor paused jobs due to quota exhaustion
- Identify failed indexing jobs for retry

**Note**: This table is automatically updated by the `IndexTezosTokenOwner` and `IndexEthereumTokenOwner` workflows. Job records are created when the workflow starts (status: `running`) and updated as the workflow progresses.

### webhook_clients

Registered webhook clients for event notifications with HTTPS endpoints and event filtering.

| Column | Type | Description |
|--------|------|-------------|
| id | BIGSERIAL | Primary key |
| client_id | VARCHAR(36) | UUID for client identification (unique) |
| webhook_url | TEXT | Client's webhook endpoint (HTTPS only) |
| webhook_secret | TEXT | HMAC signing secret for signature verification |
| event_filters | JSONB | Array of event types or wildcard `["*"]` |
| is_active | BOOLEAN | Whether client is active (default: true) |
| retry_max_attempts | INTEGER | Maximum retry attempts (default: 5) |
| created_at | TIMESTAMPTZ | Record creation timestamp |
| updated_at | TIMESTAMPTZ | Last update timestamp |

**Indexes**:
- `idx_webhook_clients_active` on (is_active) WHERE is_active = true

**Unique Constraints**:
- `client_id` (unique)

**Constraints**:
- `webhook_url_http_https` CHECK constraint - Enforces HTTP/HTTPS URLs only

**Use Cases**:
- Register webhook clients to receive real-time event notifications
- Filter events by type (indexing events, ownership events, or wildcard)
- Configure retry behavior for failed deliveries

### webhook_deliveries

Audit log of webhook delivery attempts with status tracking and response details.

| Column | Type | Description |
|--------|------|-------------|
| id | BIGSERIAL | Primary key |
| client_id | VARCHAR(36) | Foreign key to webhook_clients.client_id (CASCADE DELETE) |
| event_id | VARCHAR(255) | Unique event ID (ULID for time-sortable) |
| event_type | VARCHAR(50) | Event type (e.g., "token.indexing.queryable") |
| payload | JSONB | Full event payload |
| workflow_id | VARCHAR(255) | Temporal workflow ID for tracking |
| workflow_run_id | VARCHAR(255) | Temporal run ID |
| delivery_status | webhook_delivery_status | Delivery status (pending, success, failed) |
| attempts | INTEGER | Number of delivery attempts (default: 0) |
| last_attempt_at | TIMESTAMPTZ | Timestamp of last attempt |
| response_status | INTEGER | HTTP status code from webhook endpoint |
| response_body | TEXT | Response body (limited to 4KB) |
| error_message | TEXT | Error message if delivery failed |
| created_at | TIMESTAMPTZ | Record creation timestamp |
| updated_at | TIMESTAMPTZ | Last update timestamp |

**Indexes**:
- `idx_webhook_deliveries_status` on (delivery_status)
- `idx_webhook_deliveries_event_id` on (event_id)
- `idx_webhook_deliveries_client` on (client_id, created_at DESC)

**Relationships**:
- Many-to-one with `webhook_clients` (CASCADE DELETE)

**Use Cases**:
- Audit webhook delivery attempts
- Track failed deliveries for debugging
- Monitor webhook client health and reliability

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

### media_health_status
- `unknown` - Not yet checked
- `checking` - Check in progress
- `healthy` - URL is accessible
- `broken` - URL is not accessible

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

### indexing_job_status
- `running` - Workflow is currently running
- `paused` - Workflow paused (quota exhausted, will resume after quota reset)
- `failed` - Workflow failed with an error
- `completed` - Workflow completed successfully
- `canceled` - Workflow was canceled

### webhook_delivery_status
- `pending` - Delivery pending or in progress
- `success` - Successfully delivered to webhook endpoint
- `failed` - Failed to deliver after all retry attempts

## Relationships

```
tokens (1)
  ├── (1) token_metadata
  ├── (N) balances
  ├── (N) provenance_events
  ├── (N) token_ownership_periods
  ├── (N) enrichment_sources
  └── (N) token_media_health

webhook_clients (1)
  └── (N) webhook_deliveries

changes_journal (standalone, references other entities via subject_id)

media_assets (standalone)

watched_addresses (standalone)

key_value_store (standalone)

address_indexing_jobs (standalone, references workflows via workflow_id)
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
- `update_webhook_clients_updated_at`
- `update_webhook_deliveries_updated_at`
- `update_address_indexing_jobs_updated_at`
- `update_token_media_health_updated_at`

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
- **MD5 hash indexes** for variable-length URLs to avoid size limitations

### URL Indexing Optimization

To handle potentially long URLs without exceeding PostgreSQL's B-tree index size limits, we use MD5 hashing:

- **Problem**: URLs can be very long (especially data URIs and complex IPFS/Arweave URLs), which can exceed PostgreSQL's index size limits
- **Solution**: Store MD5 hashes of URLs in separate columns and index those instead
- **Implementation**:
  - `token_metadata`: `image_url_hash`, `animation_url_hash`
  - `enrichment_sources`: `image_url_hash`, `animation_url_hash`
  - `token_media_health`: `media_url_hash`
- **Benefits**:
  - Fixed-length hash values (32 characters) eliminate size constraints
  - Fast lookups using hash-based indexes
  - Original URLs preserved for display and data integrity
  - Automatic hash population in application layer

### Query Patterns

**Common Queries**:
1. Get token by TokenCID → `token_cid` unique index
2. Get tokens by owner → `idx_tokens_current_owner` or `idx_balances_owner_address`
3. Get metadata → `token_id` foreign key
4. Get provenance events → `idx_provenance_events_token_id`
5. Search metadata → GIN indexes on JSONB columns
6. Find tokens by media URL → `idx_token_media_health_url_hash` using MD5 hash

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

