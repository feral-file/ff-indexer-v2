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

# Ethereum (Infura or Chainstack; both URLs must point at the same provider)
export FF_INDEXER_ETHEREUM_RPC_URL=https://mainnet.infura.io/v3/YOUR_KEY            # Chainstack: https://ethereum-mainnet.core.chainstack.com/YOUR_KEY
export FF_INDEXER_ETHEREUM_WEBSOCKET_URL=wss://mainnet.infura.io/ws/v3/YOUR_KEY     # Chainstack: wss://ethereum-mainnet.core.chainstack.com/YOUR_KEY
export FF_INDEXER_ETHEREUM_CHAIN_ID=eip155:1
# Optional: self-hosted log warehouse (ff-eth-logs) for historical eth_getLogs; empty = vendor only
export FF_INDEXER_ETHEREUM_LOG_WAREHOUSE_URL=http://localhost:8545

# Job queue (names for token_index / media_index workers)
export FF_INDEXER_JOBS_TOKEN_QUEUE=token_index
export FF_INDEXER_JOBS_MEDIA_QUEUE=media_index
```

### Ethereum RPC provider notes

The Ethereum code path is provider-agnostic, but two providers are known and their limits differ in ways that matter for configuration:

| | Infura (Team) | Chainstack (Growth) |
|---|---|---|
| Billing | credits, **daily** quota, hard 402 stop | request units, **monthly** quota, $15/1M overage |
| `eth_getLogs` | 255 credits; span cap `toBlock-fromBlock ≤ 10000` (verified live) | 1 RU, **2 RU when `fromBlock` is ≥127 blocks behind the tip** (every history walk); span cap accepts `toBlock-fromBlock ≤ 10100` (verified live 2026-08-26; docs say "10,000 blocks"), so `FF_INDEXER_ETHEREUM_GETLOGS_SPAN_CAP=10000` fits both providers; a 10k-block owner-scan window answered in ~0.5 s |
| WebSocket | `eth_subscribe logs` billed 300 credits/block | **every pushed notification is 1 RU** — the reason chain ingestion is head-driven pull (`newHeads` + per-block `eth_getLogs`), see `docs/architecture.md` |
| Rate limit | 40k credits/s | 250 requests/s (Growth); peak concurrent `eth_getLogs` ≈ `token_worker.concurrency × scan_window_concurrency × 3` |
| Archive state | included | included on paid plans, **but the node must be deployed in Archive mode** — `GetContractDeployer` binary-searches `eth_getCode` at historical blocks and a Full node answers `missing trie node` (the Global Node endpoint used for the soak served `eth_getCode` at block 5,000,000) |
| Over-range error | `range N exceeds limit of 10000` | `Block range limit exceeded. See more details at https://docs.chainstack.com/docs/limits#evm-range-limits` (`-32602`, verified live) |
| Load behaviour (measured 2026-08-26, same script, sequential) | owner-shaped 10k-block `eth_getLogs`: idle 1.6–1.8 s; queues linearly with concurrency (p50 4 s at 32–64, 8 s at 128) with sporadic `503 service temporarily unavailable`; **at 256 concurrent 201/256 fail with 503 plus a 429** — the production peak below is beyond what Infura serves; unfiltered topic-only 10k scans stay ≤ 4 s even at 32 concurrent | owner-shaped: idle 0.2 s, p50 ≤ 0.4 s to 128, **256/256 ok at p50 1.2 s and 360/360 ok at p50 2.2 s (max 4.8 s)** with `eth_blockNumber` unaffected and immediate recovery — i.e. the production peak `token_worker.concurrency × scan_window_concurrency × 3` = 360 in flight is served (one transient held/closed connection was seen once at 128; it did not recur at 256 or 360 and the adapter retries it as EOF). **Unfiltered topic-only 10k scans serialize** (~0.6 s each: p50 4 / 8 / 22 s at 8 / 16 / 32 concurrent) and stall trivial calls while they drain (`eth_blockNumber` up to 5.5 s, 17 s to recover); abandoned requests keep running server-side. Production never issues that shape (catch-up uses 10-block windows; adapter queries are address/owner-scoped) — keep it that way |

Both phrasings (and drpc's `query returns too many logs`) are recognised by `helpers.IsBlockRangeCapError` / `IsTooManyResultsError`, which is what lets pagination halve instead of aborting a walk. `TestE2E_OverRangeGetLogsIsClassifiedAndPaginated` (`-tags e2elive`, `E2E_ETH_WS=wss://...`) issues an over-cap request through the production client, asserts the classifier recognises the rejection, and paginates the same range to completion — run it against any new provider before cut-over, and add any new limit message to the classifier with a test.

### Ethereum log warehouse (ff-eth-logs)

`ethereum.log_warehouse_url` points the indexer at a [ff-eth-logs](https://github.com/feral-file/ff-eth-logs) warehouse (JSON-RPC over HTTP, private network, no auth). With it set, the historical part of every `eth_getLogs` is one warehouse query and only the blocks above the warehouse head reach the vendor; without it nothing changes. The routing, its fall-through policy and what the warehouse answers are described in `docs/architecture.md` ("Ethereum log warehouse routing").

| Key | Default | Meaning |
|---|---|---|
| `ethereum.log_warehouse_url` | `""` (off) | http(s) endpoint of the warehouse; validated at load, chain id and capability probe checked at startup (mismatch / failed probe is fatal, unreachable is a WARN and re-verified on first use) |
| `ethereum.log_warehouse_timeout` | `120s` | per-request deadline, one attempt, no retry — on expiry the query falls through to the vendor |
| `ethereum.log_warehouse_scan_window_blocks` | `1000000` | owner-scan window over the warehouse-covered range (above the head: `getlogs_span_cap + 1`) |

Locally, `make quickstart` in the ff-eth-logs repo starts a **tail-only** warehouse on `http://localhost:8545` (it serves only the blocks it has followed since it started). The indexer **refuses** such a warehouse: on mainnet it probes block 3,919,706 for the CryptoPunks internal `Transfer` before routing anything (`docs/architecture.md`, "Nothing is routed through an unverified warehouse"), and a warehouse whose coverage starts above that block fails the probe at startup. To exercise routing locally, load the backfill first (`make backfill` in ff-eth-logs). The routing itself is visible in the logs: `Log warehouse verified` (info), `Log range served by the warehouse` (debug), `Log warehouse unavailable for range, falling through to the vendor` (warn). Sizing note for the warehouse side: each owner-scan window issues the three merged owner queries at once, so the warehouse sees up to `token_worker.concurrency × scan_window_concurrency × 3` concurrent queries against its PostgreSQL pool.

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

**Migration 025 + the EVM credit guard — required ordering:**

The Ethereum credit guard (`ethereum.full_provenance_disabled`, with
`ethereum.getlogs_span_cap` and `ethereum.getlogs_call_budget`) has a lifecycle
that spans config and database. Order matters in both directions:

*Enabling the guard:*

1. Run `025.sql` (adds `tokens.provenance_deferred_at`) on all instances
2. Deploy application code that knows the guard
3. Enable the guard in deploy config and restart workers

Enabling the guard before 025 is applied makes every guarded skip fail on the
missing column — jobs error instead of deferring.

*Lifting the guard (recovery):*

1. Disable `ethereum.full_provenance_disabled` in deploy config and restart workers
2. Size the backfill burst first — each deferred token replays full history
   (~0.7–1.3M Infura credits at a 10k span cap; with `ethereum.log_warehouse_url`
   set the replay is one warehouse query per token and the burst is warehouse
   load, not vendor spend):
   `SELECT count(*) FROM tokens WHERE provenance_deferred_at IS NOT NULL;`
3. Run the backfill with the deployment's token queue (required parameter):
   `psql ... -v queue=<jobs.token_queue> -f db/migrations/025_backfill.sql`
4. Verify convergence: the count from step 2 shrinks toward zero as jobs
   complete (successful full provenance clears the marker). Re-running the
   backfill is safe and picks up only the remainder.

Running the backfill while the guard is still enabled is harmless but useless:
every job re-skips and re-marks; re-run the file after the guard is off.

**Migration 028 (render-probe counter reset) — exact-once, with the indexer stopped:**

`028.sql` zeroes every `media_render_probes.consecutive_failures` accumulated
while render timeouts still counted toward the gate (pre ff-indexer-v2#142). It is
pure data with no code dependency, so the default ordering does not apply; what does
apply is that a probe in flight across the UPDATE writes its pre-reset counter back
afterwards, whichever executor version runs it, and the new executor would then read a
legacy stall count as blank evidence. Sequence:

1. Stop the indexer — every media/probe worker, old or new, including in-flight
   `RenderMediaProbe` jobs (no worker may be running during step 2)
2. Run `028.sql` once; confirm it reports `COMMIT`
3. Start the #142 image

Do **not** re-run the file after cutover: with the new executor live, a non-zero
counter is a genuine first blank awaiting its confirmation, and a second run would
erase that evidence.

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

### Migration 018_reindex: re-enrich existing tokens to populate release membership

Migration `018_reindex.sql` inserts `IndexTokenMetadata` jobs for all tokens previously enriched by Art Blocks, Feral File, fxhash, and objkt. The running worker processes these jobs to re-fetch vendor data and populate `releases` and `release_members`.

**Why reindex rather than derive from stored vendor JSON:**

Stored `vendor_json` from before this release is incomplete for every vendor and cannot produce correct release rows without hitting vendor APIs:

| Vendor | Gap in pre-existing stored JSON |
|--------|--------------------------------|
| Art Blocks | `max_invocations` was not fetched; `total_mints` would be absent |
| Feral File | `index` and `seriesID` were not stored; `mint_number` cannot be derived |
| fxhash | Tokens were stored as `vendor=objkt` with no `generative_token`/`iteration`; `vendor_release_id` cannot be derived |
| objkt | `fa.collection_type` was not fetched; custom collections cannot be identified |

Reindexing runs the full enrichment pipeline — vendor API calls are governed by the configured rate limiters (2 RPS for fxhash/objkt, no separate limit for Feral File/Art Blocks) and the existing token worker concurrency.

**What happens after migration 018_reindex runs:**

1. Jobs are inserted into the `token_index` queue with `status=pending`.
2. The token worker picks them up and calls `EnhanceTokenMetadata` for each token.
3. The enhancer re-fetches data from the vendor API, stores a complete `vendor_json`, and upserts `releases` + `release_members` directly.
4. New tokens indexed after this release get releases written automatically at enrichment time — no additional action needed.

**Idempotency:** The migration uses `ON CONFLICT ... DO NOTHING` on the partial unique index `jobs_unique_key_active`. Re-running the migration skips tokens that already have a pending or running metadata job. Tokens whose jobs completed or failed can be re-triggered via `POST /api/v1/tokens/index`.

**Fresh installs:** `018_reindex.sql` produces no rows on a fresh database (no `enrichment_sources` rows exist yet). `db/init_pg_db.sql` does not need updating.

**Verify progress after deployment:**

```sql
-- Jobs inserted by migration 018_reindex (all statuses)
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

### Migration 019_reindex: re-enrich OpenSea tokens and backfill release slugs

Migration `019_reindex.sql` has two parts:

**Part 1 — Re-enrich all OpenSea tokens**

Inserts one `IndexTokenMetadata` job per token with `vendor = 'opensea'` in `enrichment_sources`. OpenSea was excluded from `018_reindex` because full OpenSea release support was added later. Re-enrichment populates two new fields on the release row:

- `name` — collection name from `GetCollection`
- `total_mints` — collection `total_supply` from `GetCollection`
- `vendor_release_slug` — the collection slug (equals `vendor_release_id` for OpenSea)

**Part 2 — Slug backfill for all other vendor releases**

For every non-OpenSea release where `vendor_release_slug IS NULL`, inserts one `IndexTokenMetadata` job for the first release member (lowest `mint_number`). The token is queued unconditionally — filtering to un-enriched tokens only would miss releases whose members were enriched before the slug column existed.

Enriching a single token is enough to call the vendor API and upsert `vendor_release_slug` on the release row.

**What happens after migration 019_reindex runs:**

1. Jobs are inserted into the `token_index` queue with `status=pending`.
2. The token worker picks them up and calls `EnhanceTokenMetadata` for each token.
3. For OpenSea tokens, the enhancer calls `GetCollection` (cached per slug per worker process) and upserts `name`, `total_mints`, and `vendor_release_slug` on the release row.
4. For non-OpenSea tokens, normal enrichment upserts the slug via `UpsertRelease`.

**Idempotency:** Uses `ON CONFLICT ... DO NOTHING` on the partial unique index `jobs_unique_key_active`. Safe to re-run.

**Fresh installs:** Both INSERTs produce no rows. `db/init_pg_db.sql` does not need updating.

**Verify progress after deployment:**

```sql
-- Releases still missing a slug (expected to shrink as worker runs)
SELECT vendor, COUNT(*)
FROM releases
WHERE vendor_release_slug IS NULL
GROUP BY vendor;

-- OpenSea releases: name and total_mints should be populated after Part 1
SELECT vendor_release_id, name, total_mints, vendor_release_slug
FROM releases
WHERE vendor = 'opensea';

-- Jobs inserted by migration 019_reindex (all statuses)
SELECT status, COUNT(*)
FROM jobs
WHERE kind = 'IndexTokenMetadata'
GROUP BY status;
```

### Migration 021_reindex: give pre-existing tokens a first moderation verdict

Migration `021.sql` creates `token_moderation_verdicts` but leaves it empty, and neither writer discovers tokens on its own — the enricher writes a verdict only while indexing a token, and the moderation sweeper's queue query reads `FROM token_moderation_verdicts` (an inner join, no discovery pass). On a database that already holds tokens the feature would therefore cover nothing until each token happened to be re-indexed for an unrelated reason, leaving already-moderated spam rendering indefinitely.

`021_reindex.sql` inserts one `IndexTokenMetadata` job per token with `vendor IN ('opensea', 'objkt')` in `enrichment_sources`. Those are the only moderating vendors, matching `schema.ModerationSourceForVendor` — other vendors get no verdict at indexing time either, so re-enriching them would spend vendor quota to write nothing.

fxhash is deliberately not covered. `enhanceFxhash` falls back to `enhanceObjkt` when the fxhash API has no gentk for a token, and those rows are stored as `vendor='objkt'`, so that subset is picked up; tokens fxhash does index are stored as `vendor='fxhash'` and are skipped. This matches the enricher's behavior and is the intended coverage gap for curated surfaces — see the accepted-gaps list in `docs/token_moderation.md`.

**Why reindex rather than derive from stored vendor JSON:** `enrichment_sources.vendor_json` often does contain objkt's `flag` and OpenSea's `is_disabled` today, but it is a snapshot of whatever fields the enricher kept at the time it ran — accumulated across every version of that code — not a guarantee that any given field exists on every historical row. Migration `018_reindex` hit exactly this and documents four fields missing from older rows.

**⚠️ Ordering: run this AFTER deploying the new application code** — the reverse of the usual rule above, which still applies to `021.sql` itself. This file enqueues work rather than changing schema: a worker running pre-021 code would claim these jobs, run the old enricher, write no verdict row, and mark them succeeded. The jobs leave the active set and the backfill silently does nothing.

Recovery if that happens: re-run `021_reindex.sql` only. `jobs_unique_key_active` guards only *active* jobs, so finished ones do not block re-insertion. Do not re-run `021.sql` — it aborts at `CREATE TYPE moderation_status` (the first statement of step 1), leaving the database untouched because that step runs in a transaction.

**What happens after migration 021_reindex runs:**

1. Jobs are inserted into the `token_index` queue with `status=pending`.
2. The token worker picks them up and calls `EnhanceTokenMetadata` for each token.
3. The enhancer reads the vendor's moderation field (objkt `flag`, OpenSea `is_disabled`) and writes a `token_moderation_verdicts` row, which recomputes `tokens.moderation_status`.
4. Each new row lands at `now + initial_recheck_interval`, after which the moderation sweeper keeps it fresh.

**Volume:** one job per already-enriched opensea/objkt token, and step 4 means the sweeper's first pass after the queue drains is a burst against the same vendor rate limiter the enricher uses (OpenSea ~4 rps, objkt ~2 rps, shared process-wide). Size it before running:

```sql
SELECT vendor, COUNT(*)
FROM enrichment_sources
WHERE vendor IN ('opensea', 'objkt')
GROUP BY vendor;
```

See `docs/token_moderation.md` for mitigations (longer `initial_recheck_interval`, or pacing the INSERT in batches) if the count is large.

**Idempotency:** Uses `ON CONFLICT ... DO NOTHING` on the partial unique index `jobs_unique_key_active`. Safe to re-run.

**Fresh installs:** Produces no rows. `db/init_pg_db.sql` does not need updating.

**Verify progress after deployment:**

```sql
-- Verdict rows written so far (expected to grow toward the sizing count above)
SELECT source, COUNT(*)
FROM token_moderation_verdicts
GROUP BY source;

-- Tokens the backfill has flagged
SELECT COUNT(*) FROM tokens WHERE moderation_status <> 'none';

-- Jobs inserted by migration 021_reindex (all statuses)
SELECT status, COUNT(*)
FROM jobs
WHERE kind = 'IndexTokenMetadata'
GROUP BY status;
```

### Migration 024_reindex: heal FA2 tokens stored with the unsigned-fxhash placeholder

TzKT's resolved-metadata cache can permanently serve a gentk's mint-time "[WAITING TO BE SIGNED]" snapshot. The resolver now detects the placeholder and re-resolves from the contract's `token_metadata` big map, but that path only runs inside a future `IndexTokenMetadata` execution — rows already in `token_metadata` are served from PostgreSQL and stay stale. `024_reindex.sql` enqueues that execution for every affected stored row, matched by placeholder name/description or the placeholder page's CID in `animation_url` (scoped to `standard = 'fa2'`).

**⚠️ Ordering: run this AFTER deploying the new application code** — same reversal as `021_reindex`, same reason: a worker running the previous code would re-fetch the same stale TzKT cache, re-store the placeholder, and mark the jobs succeeded. Recovery: re-run `024_reindex.sql` only (finished jobs do not block re-insertion).

**Idempotency:** `ON CONFLICT ... DO NOTHING` on `jobs_unique_key_active`. Safe to re-run. Genuinely unsigned gentks re-store the placeholder and would be re-enqueued by a later re-run — correct, since they are still unsigned.

**Fresh installs:** Produces no rows. `db/init_pg_db.sql` does not need updating.

**Verify after the queue drains** — the on-chain reproducer must no longer point at the placeholder page:

```sql
SELECT tm.name, tm.animation_url
FROM token_metadata tm JOIN tokens t ON t.id = tm.token_id
WHERE t.token_cid = 'tezos:mainnet:fa2:KT1KEa8z6vWXDJrVqtMrAeDVzsvxat3kHaCE:324719';
-- animation_url must NOT contain QmdGV3UqJqX4v5x9nFcDYeekCEAm3SDXUG5SHdjKQKn4Pe
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

**No automatic retry (v1) — except crash recovery.** A handler error sets **`failed`** and **`last_error`**. The service does not apply exponential backoff or re-drive failed rows automatically. Webhook deliveries are also single-shot for delivery semantics; `webhook_clients.retry_max_attempts` is retained in the schema/API for compatibility but is not used to retry delivery. Operators **re-enqueue** work (e.g. new API trigger or ingestion event) or fix configuration/upstreams, then watch new jobs succeed.

**Bounded crash-loop recovery.** Jobs left `running` after a process crash are swept by `SweepOrphanedJobs` on worker startup. The `jobs.attempt_count` column (incremented on each claim, reset to 0 on clean reschedule) determines the outcome:

- `attempt_count < max_attempts` → reset to `pending` for retry (default: up to 3 attempts total).
- `attempt_count >= max_attempts` → permanently `failed` with `last_error = "crash-loop terminated: ..."`.

This breaks infinite loops caused by CGO/Rust panics (e.g. resvg SIGABRT on certain SVG filter primitives) that kill the process before the job can be marked failed. `max_attempts` is operator-tunable via `FF_INDEXER_JOBS_TOKEN_WORKER_MAX_ATTEMPTS` and `FF_INDEXER_JOBS_MEDIA_WORKER_MAX_ATTEMPTS` (default 3).

**Claim backpressure.** Workers claim at most `min(concurrency - in_use, batch_size)` jobs per poll tick. This keeps DB `running` counts aligned with actual executing goroutines and prevents inflating observable queue health metrics.

See [`docs/schema.md`](../docs/schema.md#jobs) for the full state machine and `attempt_count` semantics.

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

### Test categories

Tests are split by the `integration` build tag:

**Unit tests** (default, no build tag):
- All `*_test.go` files without `//go:build integration`
- Run with `make test` or `CGO_ENABLED=1 go test ./...`
- Excludes `internal/store` tests and vendor `*_integration_test.go` files

**Integration tests** (`//go:build integration`):
- `internal/store` — requires PostgreSQL (testcontainers when Docker is available and `TEST_DB_HOST` is unset, or `TEST_DB_*` for an external DB)
- `internal/providers/vendors/*/client_integration_test.go` — call live vendor APIs (network required)

**Important:** `go test -tags=integration ./...` is **additive** — it runs unit tests **and** integration-tagged tests. It is not integration-only.

### Canonical Verification

Use this command before handing off a substantive change:

```bash
make check
```

It runs: format imports (`goimports`), verify `gofmt -s` formatting, full-repo local lint (`golangci-lint` with CGO enabled), unit tests (`make test`), then the full suite with integration tests (`make test-integration`).

**Store integration tests require Docker or `TEST_DB_*`** — the store harness spins up a PostgreSQL testcontainer when `TEST_DB_HOST` is unset. Ensure Docker is running before invoking `make check`, or point at an external database:

```bash
export TEST_DB_HOST=localhost TEST_DB_PORT=5432
export TEST_DB_USER=postgres TEST_DB_PASSWORD=postgres TEST_DB_NAME=test_db
make test-integration
```

To fix formatting issues before running checks:

```bash
make imports   # goimports (import order and grouping)
make fmt       # gofmt -s -w (simplifications enforced in CI)
```

### Running tests

```bash
# Unit tests only (fast, no external dependencies)
make test

# Full suite: unit + integration (matches CI; needs Docker or TEST_DB_* for store tests)
make test-integration

# Same command CI uses (with PostgreSQL service and TEST_DB_* set)
CGO_ENABLED=1 go test -tags=integration -cover ./...

# Store integration tests only
CGO_ENABLED=1 go test -tags=integration -v ./internal/store/...

# Vendor API integration tests only (live external APIs)
go test -tags=integration ./internal/providers/vendors/fxhash/...
go test -tags=integration ./internal/providers/vendors/objkt/...
go test -tags=integration ./internal/providers/vendors/artblocks/...
go test -tags=integration ./internal/providers/vendors/feralfile/...

# All media-related tests (requires CGO)
CGO_ENABLED=1 go test ./internal/media/... -v
```

The lint profile is opinionated (complexity, length, doc expectations). For CI's exact commands and package filters, see `.github/workflows/test.yaml` and `.github/workflows/lint.yaml`. CI runs the full test suite with `-tags=integration` against a PostgreSQL service (see `TEST_DB_*` env vars in that workflow).

Optional lightweight verification (CGO-disabled binary and stub media path) is **not** part of `make check` or CI. Run `make test-lightweight-build` when you change code that must remain compatible with the default lightweight Docker deployment.

For non-trivial changed functions, use the doc comment to capture the reason, trade-offs, and constraints behind the implementation so later contributors do not reopen already-rejected paths by accident.

Coverage policy is non-regression versus the base branch. If a change must lower coverage, document the reason in the PR description and call out the gap for reviewers.

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
