# Database Schema

This document describes the database schema for FF-Indexer v2, including all tables, relationships, and migration notes.

## Overview

The database uses PostgreSQL 15+ with the following design principles:
- Normalized schema with foreign key relationships
- JSONB columns for flexible metadata storage
- Indexes optimized for common query patterns
- Support for multiple blockchains and token standards

## Core Tables

The database includes the following main tables:

- `tokens` - Primary token entity across all blockchains
- `token_metadata` - NFT metadata (name, description, media, attributes, etc.)
- `enrichment_sources` - Additional data sources enriching token information
- `balances` - Current token ownership balances (multi-edition tokens)
- `token_events` - Unified log for collection sync (`acquired` / `released` / metadata / viewability); ownership rows with `metadata.tx_hash` are unique per `(token_id, owner_address, event_type, tx_hash)` via `token_events_ownership_unique`
- `provenance_events` - Historical provenance events (mint, transfer, burn, etc.)
- `media_assets` - Media files associated with tokens (images, videos, etc.)
- `token_media_health` - Health status of token media URLs
- `releases` - Cross-vendor release abstraction (Feral File series, Art Blocks projects) with stable internal id (migration 018)
- `release_members` - Ordered token membership within a release; `mint_number` is authoritative and 1-based (migration 018)
- `watched_addresses` - Addresses being monitored for indexing
- `jobs` - Postgres-backed durable job queue (token and media work units)
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
| is_viewable | BOOLEAN | Whether token has accessible media URLs (for filtering unviewable tokens) |
| last_provenance_timestamp | TIMESTAMPTZ | Cached timestamp of most recent provenance event (denormalized for query performance) |
| version | BIGINT | Incremented on user-visible changes (ownership, metadata, enrichment, viewability, burn status); used for scoped state sync |
| created_at | TIMESTAMPTZ | Record creation timestamp |
| updated_at | TIMESTAMPTZ | Last update timestamp |

**Indexes**:
- `idx_tokens_chain_contract_number` on (chain, contract_address, token_number)
- `idx_tokens_current_owner` on (current_owner) WHERE current_owner IS NOT NULL
- `idx_tokens_burned` on (burned) WHERE burned
- `idx_tokens_created_at` on (created_at)
- `idx_tokens_last_prov_timestamp_id` on (last_provenance_timestamp DESC NULLS LAST, id DESC) - for sorting by latest activity
- `idx_tokens_viewable_last_prov_timestamp` on (is_viewable, last_provenance_timestamp DESC NULLS LAST, id DESC) - for filtered sorting

**Unique Constraints**:
- `token_cid` (unique)
- `(chain, contract_address, token_number)` (unique)

**Query Sorting**:
The tokens query supports three sort options:
- `sort_by=created_at` - Sort by token creation timestamp
- `sort_by=latest_provenance` (default) - Sort by latest provenance event:
  - When `owners` filter is provided: Sorts by latest provenance event for those specific owners (via join with `token_ownership_provenance`)
  - Without `owners` filter: Uses denormalized `last_provenance_timestamp` field for efficient sorting
- `sort_by=mint_number` - Sort by authoritative 1-based mint order within a release (via join with `release_members`). **Requires** `release_id` filter; the API returns a validation error if `mint_number` is requested without `release_id`. Uses `release_members_release_id_mint_number_idx` on `(release_id, mint_number)` for ordered member listing.

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
- `(token_id, media_url_hash, media_source)` (unique) - Uses hash for efficiency while maintaining uniqueness

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

---

### token_ownership_provenance

Materialized view tracking the most recent provenance event per token-owner pair. Optimizes owner-filtered token queries by avoiding expensive LATERAL JOINs.

**Purpose:**
- Enables fast sorting by latest activity when filtering tokens by owner
- Pre-computes latest provenance timestamp per token-owner pair
- Only tracks recipients (to_address), not senders (from_address)
- Used for `sort_by=latest_provenance` with owner filter

| Column | Type | Description |
|--------|------|-------------|
| id | BIGSERIAL | Primary key |
| token_id | BIGINT | Foreign key to tokens.id (CASCADE DELETE) |
| owner_address | TEXT | Address that received the token (to_address from provenance event) |
| last_timestamp | TIMESTAMPTZ | Timestamp of most recent provenance event where this address was recipient |
| last_tx_index | BIGINT | Transaction index for tiebreaking when timestamps are equal |
| last_event_type | event_type | Type of the most recent provenance event |
| created_at | TIMESTAMPTZ | Record creation timestamp |
| updated_at | TIMESTAMPTZ | Last update timestamp |

**Indexes**:
- `idx_token_ownership_prov_owner_token_timestamp` on (owner_address, token_id, last_timestamp DESC, last_tx_index DESC, id DESC)
- `idx_token_ownership_prov_token_timestamp` on (token_id, last_timestamp DESC, last_tx_index DESC, id DESC)

**Unique Constraints**:
- `(token_id, owner_address)` (unique) - One record per token-owner pair

**Relationships**:
- Many-to-one with `tokens` (CASCADE DELETE)

**Maintenance**:
- Maintained by application code via UPSERT operations
- Monotonic timestamp enforcement in UPSERT WHERE clause prevents out-of-order updates

---

### releases

Cross-vendor release abstraction that gives Feral File series and Art Blocks projects a stable internal id with mint-ordered members. Added in migration 018.

| Column | Type | Description |
|--------|------|-------------|
| id | BIGSERIAL | Stable internal release identifier (primary key) |
| vendor | vendor_type | Source platform (`artblocks`, `feralfile`) |
| vendor_release_id | TEXT | External release key: FF seriesID UUID or AB `{chainID}-{contract}-{projectID}` (chain-qualified to prevent cross-chain collisions) |
| name | TEXT | Human-readable release title (e.g. "Fidenza by Tyler Hobbs"); populated from vendor enrichment |
| total_mints | BIGINT | Declared max edition size from vendor (AB max_invocations, FF series.settings.maxArtwork); nullable when unknown |
| created_at | TIMESTAMPTZ | Record creation timestamp |
| updated_at | TIMESTAMPTZ | Last update timestamp (bumped on every upsert via trigger) |

**Unique Constraints**:
- `(vendor, vendor_release_id)` (unique) — the upsert conflict target

**Triggers**:
- `update_releases_updated_at` — automatically bumps `updated_at` on row update

**Note**: `UpsertRelease` uses `INSERT ... ON CONFLICT (vendor, vendor_release_id) DO UPDATE SET updated_at = now()` (and `name`/`total_mints` when provided) RETURNING id to be safe under concurrent token workers. `release_members` has no `updated_at`; membership rows are immutable once written (overwritten in full by the ON CONFLICT path on `token_id`).

---

### release_members

Ordered token membership within a release. Each token belongs to at most one release; `mint_number` is the authoritative 1-based edition order within the release. Added in migration 018.

| Column | Type | Description |
|--------|------|-------------|
| id | BIGSERIAL | Internal row identifier (primary key) |
| release_id | BIGINT | Foreign key to `releases.id` (CASCADE delete) |
| token_id | BIGINT | Foreign key to `tokens.id` (CASCADE delete) |
| mint_number | BIGINT | Authoritative 1-based mint/edition order within the release (`CHECK (mint_number > 0)`) |
| created_at | TIMESTAMPTZ | Record creation timestamp |

**Unique Constraints**:
- `(token_id)` (unique) — a token belongs to at most one release; conflict target for `UpsertReleaseMember`
- `(release_id, token_id)` (unique)
- `(release_id, mint_number)` (unique)

**Indexes**:
- `release_members_release_id_mint_number_idx` on `(release_id, mint_number)` — supports ordered member listing

**Relationships**:
- Many-to-one with `releases` (FK `release_id`)
- Many-to-one with `tokens` (FK `token_id`)

**Note**: `release_members` is intentionally `created_at`-only (no `updated_at`, no trigger). When a token's membership changes, the existing row is replaced via `ON CONFLICT (token_id) DO UPDATE`.

---

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
- `ethereum_mainnet_cursor` - Last **fully flushed** Ethereum mainnet block number at the block boundary (see ingestion runner in [`docs/architecture.md`](architecture.md#chain-ingestion)); combined with `start_block` override and monotonic flush semantics
- `ethereum_sepolia_cursor` - Last **fully flushed** Ethereum Sepolia block number (same semantics as mainnet)
- `tezos_mainnet_cursor` - Last **fully flushed** Tezos mainnet **level** at the block boundary (same runner semantics; Tezos subscriber emits ordered levels toward the runner)
- `tezos_ghostnet_cursor` - Last **fully flushed** Tezos ghostnet level
- `indexer_version` - Current indexer version

**Ingestion note**: These values are authoritative for “how far ingestion has committed” after jobs are enqueued for that block/level. They do not move backward due to late subscriber delivery; the runner skips buffers strictly below the in-memory floor loaded on first flush (see architecture doc).

### jobs

Durable work queue: one row per unit of background work. Logical queues (e.g. `token_index`, `media_index`) map to **worker pools** in `cmd/ff-indexer`; the handler name is stored in `kind` and resolved by an in-process registry.

| Column | Type | Description |
|--------|------|-------------|
| id | BIGSERIAL | Primary key; returned to clients as `job_id` from trigger/status APIs |
| queue | TEXT | Queue name (e.g. `token_index`, `media_index`) |
| kind | TEXT | Registered handler name (e.g. `IndexTokenMint`, `IndexMediaWorkflow`) |
| payload | JSONB | Handler arguments (wire format: JSON array of values) |
| status | job_status | `pending` \| `running` \| `succeeded` \| `failed` \| `canceled` |
| unique_key | TEXT | When set, deduplicates in-flight work (see partial unique index below) |
| run_after | TIMESTAMPTZ | Do not run before this time (scheduling, quota resume, operator reschedule) |
| last_error | TEXT | Set when a handler fails (terminal for `failed` status) |
| cancel_requested | BOOLEAN | Worker observes this and cancels the handler context |
| created_at | TIMESTAMPTZ | Row creation time |
| updated_at | TIMESTAMPTZ | Last update time |
| started_at | TIMESTAMPTZ | When a worker claimed the job |
| finished_at | TIMESTAMPTZ | When the job reached a terminal status |

**Indexes**:

- `jobs_unique_key_active` — **UNIQUE** partial index on `(queue, kind, unique_key)` **WHERE** `status IN ('pending','running')` **AND** `unique_key IS NOT NULL`. Enforces at most one *active* job per deduplication key; a new enqueue with the same key after success/failure can insert a new row.
- `jobs_poll` — on `(queue, run_after)` **WHERE** `status = 'pending'`, to support efficient polling of ready work.

**Claim semantics (implementation)**:

Workers **claim** work in PostgreSQL using a transaction that selects candidate rows and updates them to `running` with **`SELECT … FOR UPDATE SKIP LOCKED`**. `SKIP LOCKED` lets concurrent transactions skip rows already locked by another session so multiple workers (or poller threads) do not block each other on the same job row. (At most one **process** is expected to *poll* a given `queue` name in default deployments, backed by a **per-queue advisory lock**; the SQL still uses `SKIP LOCKED` for safe claim batches.)

**State machine (v1)**:

- **`pending` → `running`**: on successful claim by a worker.
- **`running` → `succeeded`**: handler returns with no error.
- **`running` → `failed`**: handler returns an error (other than a controlled reschedule sentinel). `last_error` is populated. **There is no automatic retry**: operators must re-enqueue or fix upstream and enqueue again.
- **`running` → `pending`**: handler requests reschedule (e.g. quota not yet reset) with a new `run_after` time.
- **Startup / crash recovery**: rows left `running` (e.g. after process death) are swept back to **`pending`** with cleared `started_at` so they can be claimed again. This is a deliberate trade-off: a handler that was mid-flight may run twice; idempotent handlers and dedup keys mitigate duplicates.
- **Cancel**: `cancel_requested` is set; the worker cancels the handler context and drives the row to **`canceled`**.

**Relationships**:

- `address_indexing_jobs.job_id` → `jobs.id` (required for address indexing work units)

### address_indexing_jobs

Tracks address-level indexing job status for owner-based indexing. Each row is keyed to the postgres-backed job queue via `job_id` (`jobs.id`), which is the same id returned by enqueue/trigger APIs and used in `GET /api/v1/indexing/jobs/{job_id}` and GraphQL.

| Column | Type | Description |
|--------|------|-------------|
| id | BIGSERIAL | Primary key |
| address | TEXT | Blockchain address being indexed |
| chain | blockchain_chain | Blockchain identifier |
| status | indexing_job_status | Job status (running, paused, failed, completed, canceled) |
| job_id | BIGINT | Foreign key to `jobs.id` (queue work unit); `NOT NULL`, `ON DELETE CASCADE` |
| workflow_id | TEXT | Deprecated. Legacy correlation id (Temporal UUID/string or decimal `jobs.id` string). Prefer `job_id`; APIs resolve polling by this value for older clients. |
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
- `idx_address_indexing_job_workflow_id` (unique partial) on `workflow_id` where status is `running` or `paused` — at most one active job per deprecated workflow id
- `idx_address_indexing_jobs_address_chain_active` (unique partial) on (address, chain) where status is `running` or `paused` — at most one active job per address+chain
- `idx_address_indexing_jobs_address_chain_created` on (address, chain, created_at DESC) - For querying jobs by address
- `idx_address_indexing_jobs_status_created` on (status, created_at DESC) - For querying jobs by status

**Use Cases**:
- Query job status via REST API (`GET /api/v1/indexing/jobs/{job_id}`) and GraphQL using the queue `jobs.id` as `job_id`
- Track progress of owner-based indexing
- Monitor paused jobs due to quota exhaustion
- Identify failed indexing jobs for operator follow-up or manual re-enqueue

**Note**: This table is updated by the in-process owner indexing steps (`IndexTezosTokenOwner` and `IndexEthereumTokenOwner`). Job records are created when work starts (status: `running`) and updated as indexing progresses.

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
- `retry_max_attempts` is stored for API compatibility; see `docs/api_design.md` for current delivery behavior (v1: no automatic re-drive of failed deliveries from this value alone)

### webhook_deliveries

Audit log of webhook delivery attempts with status tracking and response details.

| Column | Type | Description |
|--------|------|-------------|
| id | BIGSERIAL | Primary key |
| client_id | VARCHAR(36) | Foreign key to webhook_clients.client_id (CASCADE DELETE) |
| event_id | VARCHAR(255) | Unique event ID (ULID for time-sortable) |
| event_type | VARCHAR(50) | Event type (e.g., "token.indexing.queryable") |
| payload | JSONB | Full event payload |
| workflow_id | VARCHAR(255) | Correlation id for the delivery (legacy column name; new work typically stores the queue job id or equivalent) |
| workflow_run_id | VARCHAR(255) | Optional second correlation id (legacy) |
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

### job_status
- `pending` - Eligible to be claimed when `run_after` has passed
- `running` - Claimed by a worker
- `succeeded` - Handler completed successfully
- `failed` - Handler failed; see `last_error` (no automatic retry in v1)
- `canceled` - Canceled (e.g. operator or `cancel_requested`)

### event_type
- `mint` - Token mint event
- `transfer` - Token transfer event
- `burn` - Token burn event
- `metadata_update` - Metadata update event

### indexing_job_status
- `running` - Address indexing is currently running
- `paused` - Paused (quota exhausted; work may resume after quota reset via rescheduled `jobs` row)
- `failed` - Failed with an error
- `completed` - Completed successfully
- `canceled` - Canceled

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
  ├── (N) token_events
  ├── (N) enrichment_sources
  ├── (N) token_media_health
  └── (0..1) release_members → releases

releases (1)
  └── (N) release_members

webhook_clients (1)
  └── (N) webhook_deliveries

media_assets (standalone)

watched_addresses (standalone)

key_value_store (standalone)

address_indexing_jobs (standalone, references `jobs` via `job_id`)

jobs (standalone; referenced by `address_indexing_jobs.job_id` and by application logic for all async work)
```

## Triggers

All tables with `updated_at` columns have triggers that automatically update the timestamp on row updates:

- `update_tokens_updated_at`
- `update_balances_updated_at`
- `update_token_metadata_updated_at`
- `update_enrichment_sources_updated_at`
- `update_media_assets_updated_at`
- `update_provenance_events_updated_at`
- `update_watched_addresses_updated_at`
- `update_key_value_store_updated_at`
- `update_webhook_clients_updated_at`
- `update_webhook_deliveries_updated_at`
- `update_address_indexing_jobs_updated_at`
- `update_token_media_health_updated_at`
- `update_jobs_updated_at`
- `update_releases_updated_at` — added in migration 018 (`releases` table only; `release_members` has no `updated_at` by design)

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
- `001.sql` - Historical: introduced `token_ownership_periods` (removed in `015.sql`).
- `018.sql` - Adds `releases` and `release_members` tables for cross-vendor release abstraction with mint-ordered members (including `CHECK (mint_number > 0)`), plus the `update_releases_updated_at` trigger.

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
- **Media Assets**: Keep indefinitely for reference
- **Tokens**: Keep all indexed tokens

