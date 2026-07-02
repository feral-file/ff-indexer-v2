# Development Guide

This guide covers local development setup, seed data, and useful scripts for FF-Indexer v2.

## Local Development Stack

### Infrastructure Services

The development stack uses Docker Compose for infrastructure services:

- **PostgreSQL** (port 5432) - Main database and `jobs` queue for background work

### Starting Infrastructure

Start only infrastructure services:
```bash
make dev
```

Or start everything:
```bash
make quickstart
```

### Infrastructure Access

**PostgreSQL**:
```bash
psql -h localhost -U postgres -d ff_indexer
# Password: postgres (default)
```

### Configuration

The system supports **dual configuration**: YAML config files and environment variables. You can use either or both together.

**Configuration Priority** (highest to lowest):
1. Environment variables (with `FF_INDEXER_` prefix)
2. `.env.local` files
3. YAML config files

#### Option 1: YAML Config Files

The binary loads a single config (defaults search `cmd/ff-indexer/`, current directory, and `config/`):

```bash
cp cmd/ff-indexer/config.yaml.sample config/config.yaml
# Edit config/config.yaml with your settings
```

**Config file location**:
- Default search paths include `config/config.yaml`, the repo root, and `cmd/ff-indexer/`
- Override with `-config /path/to/config.yaml`

#### Option 2: Environment Variables

Environment variables use the `FF_INDEXER_` prefix and map to nested config keys:
- `FF_INDEXER_DATABASE_HOST` → `database.host`
- `FF_INDEXER_ETHEREUM_RPC_URL` → `ethereum.rpc_url`
- `FF_INDEXER_JOBS_TOKEN_QUEUE` → `jobs.token_queue`

Dots in config keys become underscores in env vars.

**Outbound SSRF protection** (`security.ssrf_protection` in YAML; shared by every HTTP client built via `NewHTTPClientWithSSRF` in this binary — including the **media health sweeper**, **token-indexing worker (worker core)** and its metadata/URI fetches, **Tezos chain ingestion** HTTP usage, and **media worker** outbound downloads when CGO and media are enabled):

- **`enabled`** — When `true` (default), those HTTP clients validate each URL (and redirect hop) before connecting for attacker-influenced or stored source URLs (media, metadata, gateway checks, etc.).
- **`max_redirects`** — Maximum **redirect hops** after the initial request (default `3` when unset in YAML). `0` forbids redirects. With `3`, the client may follow up to three `3xx` responses after the first GET/HEAD.
- **`block_multicast`** — Refuse multicast ranges when `true` (default `false`).
- **`allowlist.domains`** — **Hostnames only** (not IP literals — those belong in `allowlist.ips`; IPv4/IPv6 strings here are rejected at startup). Entries bypass hostname/DNS/IP checks (subdomain suffix matching applies). Each entry must include **at least one dot** (e.g. `cdn.example.com`); bare suffixes like `com` are rejected. Trust DNS for anything under those names.
- **`allowlist.ips`** — Literal IPs that bypass IP-range blocking only (IPv4-mapped literals such as `::ffff:192.168.x.x` match an IPv4 entry on the list).

Examples:

- `FF_INDEXER_SECURITY_SSRF_PROTECTION_ENABLED=false`
- `FF_INDEXER_SECURITY_SSRF_PROTECTION_MAX_REDIRECTS=5`

**Environment variable files** (loaded in order, later files override earlier):
1. `config/.env` - Base configuration (version controlled)
2. `config/.env.local` - Local overrides (git ignored)
3. `config/.env.ff-indexer.local` - Optional overrides for the binary (git ignored)

#### Required Configuration

Secrets settings (can be in YAML config or environment variables):
```bash
# Database
FF_INDEXER_DATABASE_USER=YOUR_DB_USER
FF_INDEXER_DATABASE_PASSWORD=YOUR_DB_PASSWORD
FF_INDEXER_DATABASE_DBNAME=ff_indexer

# Ethereum (for ff-indexer chain ingestion and token worker)
FF_INDEXER_ETHEREUM_RPC_URL=YOUR_ETHEREUM_RPC_URL
FF_INDEXER_ETHEREUM_WEBSOCKET_URL=YOUR_ETHEREUM_WEBSOCKET_URL

# Cloudflare (only required when FF_INDEXER_MEDIA_ENABLED=true)
FF_INDEXER_CLOUDFLARE_ACCOUNT_ID=YOUR_ACCOUNT_ID
FF_INDEXER_CLOUDFLARE_API_TOKEN=YOUR_API_TOKEN
FF_INDEXER_MEDIA_ENABLED=false
# Opt-in Cloudflare Stream uploads for video/*. Default false: skip videos; image/SVG unchanged.
FF_INDEXER_VIDEO_PROCESSING_ENABLED=false

# API authentication
FF_INDEXER_AUTH_JWT_PUBLIC_KEY=YOUR_JWT_PUBKEY_PEM
FF_INDEXER_AUTH_API_KEYS=YOUR_AUTH_API_KEYS
```

**Example**: Mixing YAML and environment variables
- Use `config.yaml` for most settings (version controlled)
- Use `config/.env.local` for sensitive values (passwords, API keys)
- Use environment variables for container/CI overrides

**Environment variable examples**:
```bash
# Database
export FF_INDEXER_DATABASE_HOST=localhost
export FF_INDEXER_DATABASE_USER=postgres
export FF_INDEXER_DATABASE_PASSWORD=postgres
export FF_INDEXER_DATABASE_DBNAME=ff_indexer

# Ethereum
export FF_INDEXER_ETHEREUM_RPC_URL=https://mainnet.infura.io/v3/YOUR_KEY
export FF_INDEXER_ETHEREUM_WEBSOCKET_URL=wss://mainnet.infura.io/ws/v3/YOUR_KEY
export FF_INDEXER_ETHEREUM_CHAIN_ID=eip155:1

# Job queue (names for token_index / media_index workers)
export FF_INDEXER_JOBS_TOKEN_QUEUE=token_index
export FF_INDEXER_JOBS_MEDIA_QUEUE=media_index
```

## Running Locally

After starting infrastructure, run the binary:

```bash
go run ./cmd/ff-indexer -config config/config.yaml
```

- **Without CGO** (`CGO_ENABLED=0`): the media job worker is not started; other subsystems run.
- **With CGO** and libvips (see [README](README.md) / Docker image): full media pipeline including the `media_index` queue worker when `FF_INDEXER_MEDIA_ENABLED=true`. Set `FF_INDEXER_VIDEO_PROCESSING_ENABLED=true` only if you want `video/*` assets ingested to Cloudflare Stream; when unset or `false`, videos are skipped (no upload, no `media_assets` row) while images and SVG handling stay the same.
- **With `FF_INDEXER_MEDIA_ENABLED=false`**: the media worker is intentionally disabled even in CGO/full builds.

Media worker concurrency and poll settings use `FF_INDEXER_JOBS_MEDIA_WORKER_*` (see [config/.env](config/.env)).

### Data URI Media Processing

When metadata contains data URIs, the media worker decodes and transforms them server-side before upload:

- Data URIs are validated in probe and processed through the standard media pipeline.
- Media assets are indexed by `source_url_hash` (MD5) to avoid oversized index entries:
  - `source_url` stores the raw URL (including data URIs) for consistency.
- API expansions resolve media assets by hashing incoming URLs for lookup.

HTTP API (same process): http://localhost:8081 (port from `server.port` in config).

## Database Setup

### Initial Schema

The database schema is automatically created when the PostgreSQL container starts using `db/init_pg_db.sql`.

To manually initialize:
```bash
psql -h localhost -U postgres -d ff_indexer -f db/init_pg_db.sql
```

### Migrations

Migrations are stored in `db/migrations/`. Apply migrations:
```bash
psql -h localhost -U postgres -d ff_indexer -f db/migrations/001.sql
```

**⚠️ CRITICAL: Migration ordering for deployments**

Some migrations introduce database constraints that application code depends on (e.g., unique indexes with `ON CONFLICT` clauses). **You MUST run migrations before deploying new application code.**

**Required deployment sequence:**

1. **Stop or pause** traffic to the application (optional for blue-green deployments)
2. **Run migrations** on all database instances
3. **Wait for migration completion** across all replicas/shards
4. **Verify migrations** succeeded (check indexes/constraints exist)
5. **Deploy** the new application version
6. **Resume** traffic

**Migration 017 (token_events uniqueness) - REQUIRED:**

This migration adds the `token_events_ownership_unique` partial index that application code depends on for idempotent ownership event insertion.

**⚠️ CRITICAL: Traffic must be paused for migration 017**

Unlike most migrations, **you MUST pause or stop write traffic** before running migration 017. This is required because:

1. **Race condition risk**: If writes continue between dedup (`017_dedup.sql`) and unique index creation (`017.sql`), new duplicates can be introduced, causing the unique index creation to fail.
2. **Write blocking**: `CREATE UNIQUE INDEX` (non-concurrent) in `017.sql` blocks writes during index build on large tables.

**Deployment sequence for migration 017:**

1. **STOP write traffic** (pause indexer workers, drain queues, or use maintenance mode)
2. Run `017_dedup.sql` if `token_events` is large or has known duplicates
3. Run `017.sql` immediately after dedup completes
4. Verify the index exists:
   ```bash
   psql -h localhost -U postgres -d ff_indexer -c "\d token_events_ownership_unique"
   # Or in SQL:
   SELECT indexname, indexdef FROM pg_indexes
   WHERE tablename = 'token_events' AND indexname = 'token_events_ownership_unique';
   ```
5. Deploy new application version
6. **RESUME write traffic**

**Large databases (recommended when `token_events` has many rows or known duplicates):**

Run the batched dedup script **before** `017.sql`. It deletes duplicate `acquired`/`released` rows in batches (default 50,000 per batch) and `COMMIT`s between batches so one huge `DELETE` does not hold locks for the full table scan.

```bash
# 1. Batched dedup (no wrapping BEGIN; commits per batch)
psql -h localhost -U postgres -d ff_indexer -f db/migrations/017_dedup.sql

# 2. Unique index (transactional)
psql -h localhost -U postgres -d ff_indexer -f db/migrations/017.sql
```

**IMPORTANT:** `017_dedup.sql` performs its own transaction control (`COMMIT` per batch). Migration runners that auto-wrap files in `BEGIN`/`COMMIT` transactions must disable that wrapping for this file, or the procedure will fail.

**Small databases or fresh installs:**

Fresh installs from `init_pg_db.sql` can run `017.sql` directly if there are no duplicate ownership rows. Traffic pause is still required due to write blocking during index build.

### Migration 018: releases and release_members

Migration `018.sql` adds the cross-vendor release abstraction (`releases`, `release_members`) used for mint-ordered series/project membership. The `release_members.mint_number` column includes `CHECK (mint_number > 0)` to enforce the 1-based contract at the database level. Fresh installs pick this up from `db/init_pg_db.sql` automatically.

- **If migration 018 has NOT run (tables missing):** Every token read (`GET /tokens`, `GET /tokens/:cid`, GraphQL token queries) fails because the app unconditionally queries `release_members` to populate `release_id` and `mint_number`. The error you will see on each call is:
  ```
  ERROR: relation "release_members" does not exist (SQLSTATE 42P01)
  ```
  Migration 018 must run **before** deploying this app version, not only before backfill or re-enrichment. This is not limited to release write paths.
- **If migration 018 ran partially (tables exist but constraint missing):** `UpsertRelease` fails with:
  ```
  ERROR: there is no unique or exclusion constraint matching the ON CONFLICT specification (SQLSTATE 42P10)
  ```
- **There is NO silent fallback:** The application explicitly returns errors rather than silently skipping release membership.

**Migration verification:**

```bash
# Verify releases and release_members tables exist
psql -h localhost -U postgres -d ff_indexer -c "\d releases"
psql -h localhost -U postgres -d ff_indexer -c "\d release_members"
```

**For production deployments:**
- Use blue-green deployment strategy to avoid downtime
- Run migrations on blue environment, verify, then switch traffic
- Or schedule maintenance window for migration + deployment

### Migration 019: re-enrich existing tokens to populate release membership

Migration `019_reindex.sql` inserts `IndexTokenMetadata` jobs for all tokens previously enriched by Art Blocks, Feral File, fxhash, and objkt. The running worker processes these jobs to re-fetch vendor data and populate `releases` and `release_members`.

**Why reindex rather than derive from stored vendor JSON:**

Stored `vendor_json` from before this release is incomplete for every vendor and cannot produce correct release rows without hitting vendor APIs:

| Vendor | Gap in pre-existing stored JSON |
|--------|--------------------------------|
| Art Blocks | `max_invocations` was not fetched; `total_mints` would be absent |
| Feral File | `index` and `seriesID` were not stored; `mint_number` cannot be derived |
| fxhash | Tokens were stored as `vendor=objkt` with no `generative_token`/`iteration`; `vendor_release_id` cannot be derived |
| objkt | `fa.collection_type` was not fetched; custom collections cannot be identified |

Reindexing runs the full enrichment pipeline — vendor API calls are governed by the configured rate limiters (2 RPS for fxhash/objkt, no separate limit for Feral File/Art Blocks) and the existing token worker concurrency.

**What happens after migration 019 runs:**

1. Jobs are inserted into the `token_index` queue with `status=pending`.
2. The token worker picks them up and calls `EnhanceTokenMetadata` for each token.
3. The enhancer re-fetches data from the vendor API, stores a complete `vendor_json`, and upserts `releases` + `release_members` directly.
4. New tokens indexed after this release get releases written automatically at enrichment time — no additional action needed.

**Idempotency:** The migration uses `ON CONFLICT ... DO NOTHING` on the partial unique index `jobs_unique_key_active`. Re-running the migration skips tokens that already have a pending or running metadata job. Tokens whose jobs completed or failed can be re-triggered via `POST /api/v1/tokens/index`.

**Fresh installs:** `019_reindex.sql` produces no rows on a fresh database (no `enrichment_sources` rows exist yet). `db/init_pg_db.sql` does not need updating.

**Verify progress after deployment:**

```sql
-- Jobs inserted by migration 019 (all statuses)
SELECT status, COUNT(*)
FROM jobs
WHERE kind = 'IndexTokenMetadata'
GROUP BY status;

-- Tokens still without release membership (expected to shrink as worker runs)
SELECT es.vendor, COUNT(*)
FROM enrichment_sources es
LEFT JOIN release_members rm ON rm.token_id = es.token_id
WHERE es.vendor IN ('artblocks', 'feralfile', 'fxhash', 'objkt')
  AND rm.id IS NULL
GROUP BY es.vendor;

-- Release membership after enrichment completes
SELECT vendor, COUNT(*) FROM releases GROUP BY vendor;
SELECT COUNT(*) FROM release_members;
```

### Reset Database

To reset the database (WARNING: deletes all data):
```bash
# Stop services
make down

# Remove volumes
docker volume rm docker_postgres_data

# Start fresh
make dev
```

## Scripts

### Database Utilities

**Check database connection**:
```bash
psql -h localhost -U postgres -d ff_indexer -c "SELECT version();"
```

**View tables**:
```bash
psql -h localhost -U postgres -d ff_indexer -c "\dt"
```

**View indexes**:
```bash
psql -h localhost -U postgres -d ff_indexer -c "\di"
```

**Check block cursors**:
```bash
psql -h localhost -U postgres -d ff_indexer -c "SELECT * FROM key_value_store WHERE key LIKE '%cursor%';"
```

## Job queue (manual checks)

Work is stored in the **`jobs`** table (`queue`, `kind`, `status`, `payload`, `unique_key`, `run_after`, …). See [`docs/schema.md`](../docs/schema.md#jobs) for the full state machine and indexes.

**No automatic retry (v1).** A handler error sets **`failed`** and **`last_error`**. The service does not apply exponential backoff or re-drive failed rows automatically. Webhook deliveries are also single-shot for delivery semantics; `webhook_clients.retry_max_attempts` is retained in the schema/API for compatibility but is not used to retry delivery. Operators **re-enqueue** work (e.g. new API trigger or ingestion event) or fix configuration/upstreams, then watch new jobs succeed.

**Claiming and scaling.** At runtime, workers **poll** for `pending` jobs ready by `run_after` and **claim** them inside a database transaction with **`SELECT … FOR UPDATE SKIP LOCKED`**, so different sessions can claim different rows without waiting on each other’s row locks. A **per-queue advisory lock** (`pg_try_advisory_lock` on a hash of the queue name) ensures only one process in the default model **polls** a given `queue` name; do not start multiple competing pollers for the same queue name without a deliberate operational plan.

**Inspect recent jobs**:
```sql
SELECT id, queue, kind, status, run_after, created_at, last_error FROM jobs ORDER BY id DESC LIMIT 20;
```

**Pending work ready to run** (illustrative):
```sql
SELECT id, queue, kind, run_after
FROM jobs
WHERE status = 'pending' AND run_after <= now()
ORDER BY run_after, id
LIMIT 20;
```

**HTTP status of a job** (after an API trigger returns `job_id`):
```bash
curl -s "http://localhost:8081/api/v1/jobs/123"
```

## Debugging

### Logs

View service logs:
```bash
# All services
make logs

# Specific service
make logs-app
make logs-infra
```

### Database Debugging

**Check recent tokens**:
```sql
SELECT * FROM tokens ORDER BY created_at DESC LIMIT 10;
```

**Check recent metadata**:
```sql
SELECT t.token_cid, tm.name, tm.enrichment_level, tm.last_refreshed_at
FROM tokens t
JOIN token_metadata tm ON t.id = tm.token_id
ORDER BY tm.last_refreshed_at DESC
LIMIT 10;
```

**Check recent provenance events**:
```sql
SELECT * FROM provenance_events ORDER BY timestamp DESC LIMIT 10;
```

### Common Issues

**Database connection errors**:
- Check PostgreSQL is running: `docker ps`
- Verify connection string in config
- Check firewall/network settings

**Chain ingestion not receiving events**:
- Verify WebSocket connection to blockchain RPC (Ethereum) or TzKT WebSocket (Tezos)
- **Tezos long downtime**: On restart, the Tezos subscriber attaches SignalR first (live batches buffer while REST runs), REST-backfills from the persisted cursor (`fromLevel`) through a post-subscribe chain head snapshot, then starts live processing. Logs include **“TzKT subscribe starting: SignalR attach, REST backfill, then live processing”** and **“TzKT REST backfill complete, starting live stream processing”**. If backfill fails, subscription aborts and the cursor stays at the last committed level. Remaining edge cases (open partial block on shutdown, timeout flush, buffer overflow) are documented in [`docs/architecture.md`](docs/architecture.md#accepted-durability-gaps-rare--edge-triggered).
- Check block / level cursor in the `key_value_store` table (see [`docs/schema.md`](docs/schema.md#key_value_store))
- If logs show **“Dropping block older than cursor”**, the runner discarded a buffer below the current checkpoint (often after very late Tezos deliveries or a `start_block`/`start_level` subscription behind the stored cursor). Same-height late buffers are still processed; rewind/backfill needs a deliberate cursor reset — see [`docs/architecture.md`](docs/architecture.md#chain-ingestion).
- Verify contract addresses are correct
- Check for blacklisted contracts

## Performance Testing

### Load Testing

Use the API to trigger indexing:

```bash
# Index tokens by CIDs (open, no authentication required)
curl -X POST http://localhost:8081/api/v1/tokens/index \
  -H "Content-Type: application/json" \
  -d '{"token_cids": ["eip155:1:erc721:0x1234567890123456789012345678901234567890:1"]}'

# Index multiple tokens by CIDs
curl -X POST http://localhost:8081/api/v1/tokens/index \
  -H "Content-Type: application/json" \
  -d '{"token_cids": ["eip155:1:erc721:0x123...:1", "eip155:1:erc721:0x123...:2"]}'

# Index tokens by owner addresses (requires authentication)
curl -X POST http://localhost:8081/api/v1/tokens/addresses/index \
  -H "Authorization: ApiKey test-api-key" \
  -H "Content-Type: application/json" \
  -d '{"addresses": ["0xowner123", "tz1abc123"]}'
```

### Monitoring

**Job queue**:
- Query `jobs` in PostgreSQL (see "Job queue (manual checks)" above) for `status` and `last_error`.

**Database Performance**:
```sql
-- As superuser
CREATE EXTENSION IF NOT EXISTS pg_stat_statements;
```

Then add the following line to postgresql.conf:
```
shared_preload_libraries = 'pg_stat_statements'
```

```sql
-- Check slow queries
SELECT query, calls, total_exec_time, mean_exec_time
FROM pg_stat_statements
ORDER BY mean_exec_time DESC
LIMIT 10;

-- Check table sizes
SELECT 
  schemaname,
  tablename,
  pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) AS size
FROM pg_tables
WHERE schemaname = 'public'
ORDER BY pg_total_relation_size(schemaname||'.'||tablename) DESC;
```

## Cleanup

### Stop Services

```bash
# Stop all services
make down

# Stop but keep containers
make stop
```

### Clean Volumes

```bash
# Remove all data (WARNING: deletes everything)
make clean
```

### Clean Images

```bash
# Remove built Docker images
make clean-images
```

## Testing

### Canonical Verification

Use this command before handing off a substantive change:

```bash
make check
```

It runs the `check` target in the `Makefile`: format imports (`goimports`), verify `gofmt -s` formatting (same as CI’s go fmt check), full-repo local lint (`golangci-lint` with CGO enabled), then `CGO_ENABLED=1` `go test -cover ./...` (same package set CI exercises, including `cmd/ff-indexer`).

To fix formatting issues before running checks:

```bash
make imports   # goimports (import order and grouping)
make fmt       # gofmt -s -w (simplifications enforced in CI)
```

The lint profile is opinionated (complexity, length, doc expectations). For CI’s exact commands and package filters, see `.github/workflows/test.yaml` and `.github/workflows/lint.yaml`.

Optional lightweight verification (CGO-disabled binary and stub media path) is **not** part of `make check` or CI. Run `make test-lightweight-build` when you change code that must remain compatible with the default lightweight Docker deployment.

Some packages need PostgreSQL or Docker (for example `internal/store` may use `TEST_DB_*` against a local DB or testcontainers when `TEST_DB_HOST` is unset). Start infrastructure with `make up-infra` when tests require Postgres, and set `TEST_DB_*` if you use an external database instead of the default container path.

For non-trivial changed functions, use the doc comment to capture the reason, trade-offs, and constraints behind the implementation so later contributors do not reopen already-rejected paths by accident.

Coverage policy is non-regression versus the base branch. If a change must lower coverage, document the reason in the PR description and call out the gap for reviewers.

```bash
# Run all media-related tests (requires CGO)
CGO_ENABLED=1 go test ./internal/media/... -v

# Narrow to specific packages
CGO_ENABLED=1 go test ./internal/media/processor -v
CGO_ENABLED=1 go test ./internal/media/transformer -v
CGO_ENABLED=1 go test ./internal/media/rasterizer -v
```

Note: media tests require CGO; make sure `CGO_ENABLED=1` is set in your environment.

## Tips

1. **Inspect the `jobs` table** and application logs for handler failures (v1 does not auto-retry failed jobs).
2. **Use database transactions** when testing data changes.
3. **Monitor logs** in real-time with `make logs`.
4. **Use GraphQL Playground** at http://localhost:8081/graphql for API testing.
5. **Keep infrastructure running** and restart only application services during development.

## Next Steps

- Read [Architecture](docs/architecture.md) for system design details
- Read [Schema](docs/schema.md) for database structure
- Read [AGENTS](AGENTS.md) for the repo contract and review loop
- Read [Contributing](CONTRIBUTING.md) for PR guidelines
