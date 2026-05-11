# Architecture

This document describes the system architecture, components, and data flow of FF-Indexer v2.

## Overview

FF-Indexer v2 indexes NFT data from multiple blockchain networks. The ingestion path is direct:

1. Chain ingestion subscribes to Ethereum and Tezos events
2. Each ingestion runner enqueues normalized events in an in-memory flush queue
3. The ingestion runner filters each queued event, enqueues a matching **job** on the PostgreSQL-backed queue for that event, and only then advances the durable cursor in PostgreSQL
4. In-process **job workers** poll the `jobs` table, run registered handlers (token and media workflows), and persist results in PostgreSQL

**Deployment model**: A single OS process (`cmd/ff-indexer`) runs the HTTP API, chain ingestion, two logical **job worker pools** (token queue and, when enabled, media queue), and the media health sweeper concurrently. Durable orchestration state lives in **PostgreSQL** (`jobs` and the rest of the schema). Outbound vendor and TzKT traffic is rate limited in-process.

## System Components

### Infrastructure Services

1. **PostgreSQL** — Primary database for indexed data, cursor state, and the **`jobs` queue** (durable work items, status, deduplication keys)

### Application Subsystems

1. **Chain ingestion** — Subscribes to blockchain events, owns the ordered flush queue, enqueues jobs, and persists durable cursor progress
2. **Worker core** — Polls the `token_index` job queue and runs token- and webhook-related handlers
3. **Worker media** — Polls the `media_index` job queue and runs media pipeline handlers (CGO / full image when enabled)
4. **API server** — Provides REST and GraphQL APIs
5. **Sweeper** — Monitors media URL health and can enqueue jobs (e.g. webhook notify). Media health HTTP checks apply **SSRF controls** by default. The **worker core** (token queue) and **media worker** use the same SSRF-protected HTTP client for outbound metadata/media fetches when `security.ssrf_protection.enabled` is true (media worker requires CGO when enabled; configure under `security.ssrf_protection`; see `docs/constraints.md`).

## Chain Ingestion

**Purpose**: Monitor blockchain networks for token mints, transfers, burns, and metadata updates, then flush those events into job execution with explicit durable progress.

**Responsibilities**:

- Connect to blockchain RPC or indexer endpoints
- Subscribe to relevant event feeds
- Parse and normalize events into `domain.BlockchainEvent`
- Enqueue each normalized event into the ordered flush queue
- Apply backpressure when the in-memory dispatch queue is saturated

Durable progress moves only inside the ingestion runner after the queued event has flushed successfully (job enqueued and cursor rules satisfied).

### Durable checkpoint (block cursor)

Progress is stored per chain in PostgreSQL (`key_value_store` cursor keys; see [`docs/schema.md`](schema.md#key_value_store)). After a **block-boundary flush** resolves every queued event in that buffered block (filters, job enqueues where applicable), the runner advances the durable cursor to that Ethereum **block number** or Tezos **level**.

### Monotonic guard and late arrivals

The runner keeps an in-memory cursor floor, loaded from the database **once on the first flush** after `Run`. When flushing a buffered block:

- If its height is **strictly below** that floor, the runner **skips** the block (no job resolution, no cursor regression), logs a warning, and continues. This handles very old **late** arrivals—for example after Tezos level buffering, timeout flushes, or seal pruning—without rewinding the checkpoint.
- If its height **equals** the floor, the buffer is still processed so legitimate **same-level** late events can run through idempotent job deduplication.
- If its height is **above** the floor, processing proceeds and the cursor advances after a successful flush.

Upstream subscribers aim for ascending order; the guard is the durability backstop when reality diverges.

### Subscription start override (`start_block` / `start_level`)

Ethereum `start_block` and Tezos `start_level` (wired into the same ingestion starting height) are **not** “only when the DB cursor is empty”. When either value is **non‑zero**, it **unconditionally** selects where `SubscribeEvents` begins, **independent of** the persisted cursor.

If the configured start is **behind** the persisted cursor, live subscription may replay old heights; the **monotonic guard drops** those buffers until the stream is past the checkpoint. To **intentionally rewind or backfill** from an earlier height, operators must align intent by adjusting or clearing the stored cursor in `key_value_store` (and setting the desired start), as documented in [`docs/constraints.md`](constraints.md).

### Tezos ingestion specifics

Tezos chain ingestion uses **TzKT HTTP + WebSocket**, normalizes FA2-relevant activity into `domain.BlockchainEvent`, and applies **level-based buffering and cross-feed coordination** in `internal/providers/tezos` so outbound events to the runner stay in **strictly ascending level order** under normal conditions. Remaining edge cases (very late old levels) still rely on the ingestion runner’s monotonic guard above.

#### TzKT WebSocket and resume gap (crash recovery)

The ingestion runner passes **`fromLevel`** into `EventSource.SubscribeEvents` (typically **persisted cursor + 1** when `start_level` is zero). **Ethereum** WebSocket providers typically honor a **from block** semantics in their subscription API.

**TzKT SignalR does not:** published hub methods **`SubscribeToTokenTransfers`** and **`SubscribeToBigMaps`** accept only **filter** arguments (e.g. `account`, `contract`, `tokenId` for transfers; `path`, `contract`, `tags`, `ptr` for bigmaps). They do **not** take a **starting level** or historical replay window. After connect, you receive **new** events only. The **“State”** message on each channel reports the hub’s last processed level for that subscription; it does not fill the gap between your **process cursor** and **connect-time head**.

**Operational consequence:** if the indexer stops while Tezos advances, restarting only opens a **live** stream. Levels **already baked into the persisted cursor remain correct** for “no double-apply”; anything **between** `fromLevel` and the levels that existed at reconnect **may never be streamed** unless another path indexes it.

**Mitigations:**

1. **Short outages** — Often acceptable if tip moved little and watch-list breadth is narrow; optionally trigger **manual token/index paths** via API for anything critical.
2. **Future implementation** — **`TODO(tezos-resume)`:** before subscribing, **page TzKT REST** (e.g. `GET /v1/tokens/transfers`, bigmap/update endpoints with **level bounds** per [TzKT REST docs](https://api.tzkt.io/)), convert through the existing `TzKTClient` parsers, feed the runner’s handler in ascending level order until caught up **with overlap or idempotent jobs**, then attach WebSocket **without resetting the cursor**.
3. **Contract-scoped ingestion** — If subscriptions are later narrowed per contract/account, REST backfill queries must apply the **same filters** as the SignalR invokes to avoid ingest drift.

Official WebSocket parameter reference: TzKT **“SubscribeToTokenTransfers”** / **“SubscribeToBigMaps”** sections in the bundled API explorer (same content as hosted OpenAPI/HTML docs for `api.tzkt.io`).

**Current routing** (by job `kind` on the token queue):

- `mint` -> `IndexTokenMint`
- `transfer` -> `IndexTokenTransfer`
- `burn` -> `IndexTokenBurn`
- `metadata_update` -> `IndexMetadataUpdate`
- `metadata_update_range` -> ignored explicitly until range handling is redesigned

## Worker core

**Purpose**: Execute **handlers** registered for the token queue—token indexing, metadata resolution, provenance, owner sweeps, and webhook delivery.

Representative `kind` values:

- `IndexTokenMint`
- `IndexTokenTransfer`
- `IndexTokenBurn`
- `IndexMetadataUpdate`
- `IndexTokenMetadata`
- `IndexTokenProvenances`
- `IndexTokenOwner`
- `DeliverWebhook` / `NotifyWebhookClients` (as applicable)

Cross-queue work (for example, resolving media for a token) is modeled by **enqueuing** a separate job on the `media_index` queue rather than in-process handoff.

## Data Flow

### Event Ingestion

```text
Blockchain (Ethereum / Tezos)
    ↓
Chain ingestion
    ↓
Ordered flush queue
    ↓
jobs row (token queue)
    ↓
Worker core handlers
    ↓
PostgreSQL
```

### Metadata Resolution

```text
Token event
    ↓
IndexToken* handler
    ↓
FetchTokenMetadata (executor)
    ↓
Metadata resolver and vendor enrichment
    ↓
UpsertTokenMetadata (executor)
    ↓
PostgreSQL
```

### Media Processing

```text
Token metadata
    ↓
IndexMediaWorkflow job (media_index queue)
    ↓
IndexMediaFile (executor)
    ↓
Cloudflare Images / Stream
    ↓
PostgreSQL media asset records
```

## Scaling Notes

The default deployment target is still a single full `ff-indexer` replica. Because each chain ingestion runner owns its own in-process queue and durable cursor stream, running multiple identical full replicas will duplicate work unless operators intentionally partition chains or ingestion responsibility.

The HTTP API and job workers are **in-process** by default. Running **a second** process that polls the same queue is intentionally discouraged at the data layer: one worker hold per queue uses a **Postgres advisory lock** so only one poller “owns” a given queue name; additional replicas would exit the worker cleanly or not poll. Scale-out patterns (if ever needed) would partition **queue names** or separate deployment roles rather than N identical pollers on the same queue.

Stateless read replicas or split API-only deployments are separate operational choices; the job queue’s correctness assumes a **single active poller per logical queue** unless the deployment model is extended deliberately.
