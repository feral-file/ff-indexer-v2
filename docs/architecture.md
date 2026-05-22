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

### Accepted durability gaps (rare / edge-triggered)

These are intentional trade-offs, not silent bugs. Operators should size **`block_flush_timeout`** and monitor Tezos feed health when correctness is critical.

1. **Shutdown without flushing the in-flight block** — On process stop, the runner **does not** flush the current partial block; the durable cursor stays at the last successful block-boundary flush. Events already accepted into RAM for the unfinished height are **not** replayed from TzKT on restart (REST backfill covers committed cursor+1 through head at subscribe; the open partial block is still lost). Ethereum subscriptions may still overlap earlier heights depending on provider semantics, but **do not** rely on that for durability.
2. **Timeout flush then restart** — After **`block_flush_timeout`**, the runner may flush height **N** and advance the cursor to **N**. On restart, REST backfill resumes from **N+1** through head, then the live socket attaches. Any events for **N** that had not been delivered **before** that flush **cannot** be recovered (distinct from same-process late arrival, where same-height buffers can still run while the process stays up).
3. **Tezos level-buffer overflow** — When the per-level buffer exceeds its cap, the subscriber **force-flushes** the lowest incomplete level and marks it emitted. Later transfers or bigmap rows for that **same level** are **dropped** (logged). Recovery requires REST backfill / reindex if the missed data matters.
4. **Tezos level timeout wake** — While an **incomplete** level blocks emission, the subscriber arms **`clock.After`** until **`firstSeen + level timeout`** for that level (same pattern as the runner’s **`block_flush_timeout`** / `flushTimerC`). When the deadline fires or a new SignalR batch arrives, the buffer is drained again. **No periodic ticker** runs when there is nothing to flush.

### Subscription start override (`start_block` / `start_level`)

Ethereum `start_block` and Tezos `start_level` (wired into the same ingestion starting height) are **not** “only when the DB cursor is empty”. When either value is **non‑zero**, it **unconditionally** selects where `SubscribeEvents` begins, **independent of** the persisted cursor.

If the configured start is **behind** the persisted cursor, live subscription may replay old heights; the **monotonic guard drops** those buffers until the stream is past the checkpoint. To **intentionally rewind or backfill** from an earlier height, operators must align intent by adjusting or clearing the stored cursor in `key_value_store` (and setting the desired start), as documented in [`docs/constraints.md`](constraints.md).

### Tezos ingestion specifics

Tezos chain ingestion uses **TzKT HTTP + WebSocket**, normalizes FA2-relevant activity into `domain.BlockchainEvent`, and applies **level-based buffering and cross-feed coordination** in `internal/providers/tezos` so outbound events to the runner stay in **strictly ascending level order** under normal conditions. Remaining edge cases (very late old levels, overflow force-flush, deadline-based level timeout) still rely on the ingestion runner’s monotonic guard above and the **accepted durability gaps** in the previous section.

#### TzKT WebSocket and REST resume (crash recovery)

The ingestion runner passes **`fromLevel`** into `EventSource.SubscribeEvents` (typically **persisted cursor + 1** when `start_level` is zero). **Ethereum** WebSocket providers typically honor a **from block** semantics in their subscription API.

**TzKT SignalR does not:** published hub methods **`SubscribeToTokenTransfers`** and **`SubscribeToBigMaps`** accept only **filter** arguments (e.g. `account`, `contract`, `tokenId` for transfers; `path`, `contract`, `tags`, `ptr` for bigmaps). They do **not** take a **starting level** or historical replay window. After connect, you receive **new** events only.

**Resume behavior:** On subscribe, the Tezos provider (1) **attaches SignalR first** so live FA2/metadata batches buffer in `streamCh` while historic work runs, (2) snapshots chain head **after both hub subscriptions are confirmed** using **uncached fetch** to ensure the boundary captures the true connection time (not a stale cached value), (3) **pages TzKT REST** (`GET /v1/tokens/transfers` and `GET /v1/bigmaps/updates` with **`level.ge` / `level.le`** bounds) from **`fromLevel`** through that head, emitting in **ascending level order** (transfers then bigmaps per level), and (4) starts `processStream` to drain `streamCh` for levels **above** the REST boundary (plus any boundary overlap). Duplicate boundary levels are tolerated via runner monotonic guards and idempotent job keys. Parse failures during backfill (or live processing) cause the subscription to fail and retry from the persisted cursor, preventing silent data loss.

**Operational notes:**

1. **Large gaps** — Backfill latency scales with outage length and TzKT rate limits; progress is logged at page boundaries and levels are emitted incrementally as pages arrive (bounded memory). Live events during backfill accumulate in `streamCh` until step (4). If the buffer fills before backfill completes, subscribe fails with **`ErrLiveStreamBufferOverflow`** and the runner reconnects (REST replay from the persisted cursor).
2. **Handoff boundary** — REST covers `[fromLevel, headAfterSubscribe]`. Live processing covers levels delivered to `streamCh` after subscribe (typically `headAfterSubscribe+1` onward, with possible boundary overlap).
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
