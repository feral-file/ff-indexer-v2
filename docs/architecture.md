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
5. **Sweeper** — Monitors media URL health and can enqueue jobs (e.g. webhook notify)

## Chain Ingestion

**Purpose**: Monitor blockchain networks for token mints, transfers, burns, and metadata updates, then flush those events into job execution with explicit durable progress.

**Responsibilities**:

- Connect to blockchain RPC or indexer endpoints
- Subscribe to relevant event feeds
- Parse and normalize events into `domain.BlockchainEvent`
- Enqueue each normalized event into the ordered flush queue
- Apply backpressure when the in-memory dispatch queue is saturated

Durable progress moves only inside the ingestion runner after the queued event has flushed successfully (job enqueued and cursor rules satisfied).

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
