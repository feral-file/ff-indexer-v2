# Architecture

This document describes the system architecture, components, and data flow of FF-Indexer v2.

## Overview

FF-Indexer v2 indexes NFT data from multiple blockchain networks. The ingestion path is direct:

1. Chain ingestion subscribes to Ethereum and Tezos events
2. Each ingestion runner enqueues normalized events in an in-memory flush queue
3. The ingestion runner filters each queued event, starts the matching Temporal workflow, and only then advances the durable cursor in PostgreSQL
4. Temporal workers execute indexing logic and persist results in PostgreSQL

**Deployment model**: A single OS process (`cmd/ff-indexer`) runs the HTTP API, chain ingestion, Temporal workers, and the media health sweeper concurrently. External infrastructure remains **PostgreSQL** and **Temporal**. Outbound vendor and TzKT traffic is rate limited in-process.

## System Components

### Infrastructure Services

1. **PostgreSQL** - Primary database for indexed data and cursor state
2. **Temporal** - Workflow orchestration engine

### Application Subsystems

1. **Chain ingestion** - Subscribes to blockchain events, owns the ordered flush queue, starts workflows, and persists durable cursor progress
2. **Worker core** - Executes token indexing workflows
3. **Worker media** - Processes and uploads media files
4. **API server** - Provides REST and GraphQL APIs
5. **Sweeper** - Monitors media URL health

## Chain Ingestion

**Purpose**: Monitor blockchain networks for token mints, transfers, burns, and metadata updates, then flush those events into workflow execution with explicit durable progress.

**Responsibilities**:

- Connect to blockchain RPC or indexer endpoints
- Subscribe to relevant event feeds
- Parse and normalize events into `domain.BlockchainEvent`
- Enqueue each normalized event into the ordered flush queue
- Apply backpressure when the in-memory dispatch queue is saturated

Durable progress moves only inside the ingestion runner after the queued event has flushed successfully.

**Current routing**:

- `mint` -> `IndexTokenMint`
- `transfer` -> `IndexTokenTransfer`
- `burn` -> `IndexTokenBurn`
- `metadata_update` -> `IndexMetadataUpdate`
- `metadata_update_range` -> ignored explicitly until range handling is redesigned

## Worker Core

**Purpose**: Execute Temporal workflows for token indexing, metadata resolution, and provenance tracking.

Representative workflows:

- `IndexTokenMint`
- `IndexTokenTransfer`
- `IndexTokenBurn`
- `IndexMetadataUpdate`
- `IndexTokenMetadata`
- `IndexTokenProvenances`
- `IndexTokenOwner`

## Data Flow

### Event Ingestion

```text
Blockchain (Ethereum / Tezos)
    ↓
Chain ingestion
    ↓
Ordered flush queue
    ↓
Temporal workflow
    ↓
Worker core activities
    ↓
PostgreSQL
```

### Metadata Resolution

```text
Token event
    ↓
IndexToken* workflow
    ↓
FetchTokenMetadata activity
    ↓
Metadata resolver and vendor enrichment
    ↓
UpsertTokenMetadata activity
    ↓
PostgreSQL
```

### Media Processing

```text
Token metadata
    ↓
IndexMediaWorkflow
    ↓
IndexMediaFile activity
    ↓
Cloudflare Images / Stream
    ↓
PostgreSQL media asset records
```

## Scaling Notes

The default deployment target is still a single full `ff-indexer` replica. Because each chain ingestion runner owns its own in-process queue and durable cursor stream, running multiple identical full replicas will duplicate work unless operators intentionally partition chains or ingestion responsibility.

Stateless API processes and Temporal workers can still scale independently when deployed separately from chain ingestion.
