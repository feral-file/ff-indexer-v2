# Architecture

This document describes the system architecture, components, data flow, and communication patterns of FF-Indexer v2.

## Overview

FF-Indexer v2 is a distributed system that indexes NFT data from multiple blockchain networks. It uses an event-driven architecture with Temporal for workflow orchestration and NATS JetStream for event streaming.

## System Components

### Infrastructure Services

1. **PostgreSQL** - Primary database for all indexed data
2. **Temporal** - Workflow orchestration engine
3. **NATS JetStream** - Event streaming and message queue

### Application Services

1. **Event Emitters** - Subscribe to blockchain events and publish to NATS
2. **Event Bridge** - Consumes events from NATS and triggers Temporal workflows
3. **Worker Core** - Executes Temporal workflows for token indexing
4. **Worker Media** - Processes and uploads media files
5. **API Server** - Provides REST and GraphQL APIs

## Component Details

### Event Emitters

**Purpose**: Monitor blockchain networks and emit events for token mints, transfers, burns, and metadata updates.

**Components**:
- `ethereum-event-emitter`: Subscribes to Ethereum WebSocket events
- `tezos-event-emitter`: Subscribes to Tezos events via TzKT WebSocket

**Responsibilities**:
- Connect to blockchain RPC endpoints
- Subscribe to relevant contract events
- Parse and normalize events into `BlockchainEvent` format
- Publish events to NATS JetStream stream `BLOCKCHAIN_EVENTS`
- Maintain block cursor in database for resumability

**Output**: Publishes events to NATS subjects:
- `events.ethereum.mint`
- `events.ethereum.transfer`
- `events.ethereum.burn`
- `events.tezos.mint`
- etc.

### Event Bridge

**Purpose**: Acts as a bridge between NATS events and Temporal workflows, filtering and routing events appropriately.

**Responsibilities**:
- Consume events from NATS JetStream stream
- Filter events based on blacklist registry
- Check if tokens are already indexed or addresses are watched
- Route events to appropriate Temporal workflows based on event type
- Handle message acknowledgment and retries

**Workflow Routing**:
- `mint` → `IndexTokenMint` workflow
- `transfer` → `IndexTokenTransfer` workflow
- `burn` → `IndexTokenBurn` workflow
- `metadata_update` → `IndexMetadataUpdate` workflow

### Worker Core

**Purpose**: Executes Temporal workflows for indexing tokens, resolving metadata, and tracking provenance.

**Workflows**:
- `IndexTokenMint` - Processes mint events
- `IndexTokenTransfer` - Processes transfer events
- `IndexTokenBurn` - Processes burn events
- `IndexTokenMetadata` - Resolves and enriches metadata
- `IndexTokenProvenances` - Indexes full provenance history
- `IndexTokenFromEvent` - Full token indexing from event
- `IndexToken` - Index token by TokenCID
- `IndexTokens` - Batch token indexing
- `IndexTokenOwners` - Owner-based indexing

**Activities**:
- `CreateTokenMint` - Create token record from mint event
- `FetchTokenMetadata` - Fetch metadata from blockchain
- `UpsertTokenMetadata` - Store metadata in database
- `EnhanceTokenMetadata` - Enrich metadata from vendor APIs
- `UpdateTokenTransfer` - Update ownership from transfer
- `IndexTokenWithMinimalProvenancesByBlockchainEvent` - Quick token indexing
- `IndexTokenWithFullProvenancesByTokenCID` - Full provenance indexing

### Worker Media

**Purpose**: Processes media files (images, videos) and uploads them to Cloudflare.

**Workflows**:
- `IndexMediaWorkflow` - Process single media file
- `IndexMultipleMediaWorkflow` - Batch media processing

**Activities**:
- `IndexMediaFile` - Download, process, and upload media

### API Server

**Purpose**: Provides HTTP APIs (REST and GraphQL) for querying indexed data.

**Endpoints**:
- REST API: `/api/v1/*`
- GraphQL API: `/graphql` (query and mutation)
- Health check: `/health`

**Features**:
- JWT authentication for mutations
- API key authentication
- CORS support
- Blacklist validation for indexing requests

**GraphQL Auto-Detection**:

The GraphQL API automatically detects which fields to expand based on your query structure, eliminating the need for explicit `expands` parameters (which are required for REST APIs).

**How it works**:
1. When you query for nested fields (e.g., `owners`, `provenance_events`, `enrichment_source`), the system automatically detects these fields from your GraphQL query
2. The appropriate data is fetched and populated without needing to specify `expands: ["owners", "provenance_events"]`
3. This provides a more natural GraphQL experience where you simply request the fields you need

**Example**:

```graphql
# Auto-detects that owners and metadata are needed
query {
  token(cid: "tez-KT1...") {
    token_cid
    metadata {
      name
      image_url
    }
    owners {
      items {
        owner_address
        quantity
      }
    }
  }
}
```

**Backward Compatibility**:
- The `expands` parameter is still supported for backward compatibility but is now deprecated
- Manual expansions specified via `expands` are merged with auto-detected expansions
- REST API still requires explicit `expands` query parameter as it cannot auto-detect field selections

**Field to Expansion Mapping**:
- `owners` → `ExpansionOwners`
- `provenance_events` → `ExpansionProvenanceEvents`
- `enrichment_source` → `ExpansionEnrichmentSource`
- `metadata_media_assets` → `ExpansionMetadataMediaAsset`
- `enrichment_source_media_assets` → `ExpansionEnrichmentSourceMediaAsset`
- `subject` (in changes) → `ExpansionSubject`

## Data Flow

### Event-Driven Indexing Flow

```
Blockchain (Ethereum/Tezos)
    ↓
Event Emitter (WebSocket subscription)
    ↓
NATS JetStream (BLOCKCHAIN_EVENTS stream)
    ↓
Event Bridge (filtering and routing)
    ↓
Temporal Workflow (IndexTokenMint/Transfer/Burn)
    ↓
Worker Core Activities (database operations)
    ↓
PostgreSQL (tokens, metadata, provenance)
```

### Metadata Resolution Flow

```
Token Mint Event
    ↓
IndexTokenMint Workflow
    ↓
IndexTokenMetadata Workflow (child)
    ↓
FetchTokenMetadata Activity
    ├─→ Blockchain RPC (tokenURI)
    ├─→ URI Resolver (IPFS/Arweave/ONCHFS/HTTP)
    └─→ Metadata Resolver (normalization)
    ↓
EnhanceTokenMetadata Activity
    ├─→ Art Blocks API (if applicable)
    ├─→ fxhash API (if applicable)
    └─→ Other vendor APIs
    ↓
UpsertTokenMetadata Activity
    ↓
PostgreSQL (token_metadata table)
```

### Media Processing Flow

```
Token Metadata (image_url, animation_url)
    ↓
IndexMediaWorkflow (triggered by worker-core)
    ↓
IndexMediaFile Activity
    ├─→ Download from source URL
    ├─→ Process (validate, stream pipeline)
    ├─→ Upload to Cloudflare Images/Stream
    └─→ Store media_asset record
    ↓
PostgreSQL (media_assets table)
```

## Component Communication

### Event Streaming (NATS JetStream)

**Stream**: `BLOCKCHAIN_EVENTS`
- **Subjects**: `events.*.>` (wildcard matching)
- **Storage**: File-based persistence
- **Retention**: 7 days
- **Replicas**: 1 (configurable)

**Publishers**:
- Event Emitters (ethereum-event-emitter, tezos-event-emitter)

**Consumers**:
- Event Bridge (consumer: `event-bridge`)

### Workflow Orchestration (Temporal)

**Task Queues**:
- `token-indexing` - Core indexing workflows (worker-core)
- `media-indexing` - Media processing workflows (worker-media)

**Workflow Execution**:
- Event Bridge triggers workflows via Temporal client
- Workers execute workflows and activities
- Activities perform database operations, API calls, etc.

### Database (PostgreSQL)

**Shared Connection**:
- All services connect to the same PostgreSQL instance
- Connection pooling per service
- GORM for ORM operations

**Key Tables**:
- `tokens` - Token records
- `token_metadata` - Metadata JSON and normalized fields
- `enrichment_sources` - Vendor API responses
- `media_assets` - Media file references
- `provenance_events` - Blockchain event history
- `balances` - Multi-token ownership (ERC1155, FA2)
- `changes_journal` - Audit log
- `watched_addresses` - Owner-based indexing config
- `key_value_store` - Configuration and cursors

## Communication Diagram

```
┌─────────────────┐
│   Blockchain    │
│  (Ethereum/     │
│    Tezos)       │
└────────┬────────┘
         │ WebSocket
         ↓
┌─────────────────┐
│ Event Emitters  │──────┐
│ (Ethereum/      │      │
│  Tezos)         │      │
└─────────────────┘      │
                         │ NATS JetStream
                         │ (BLOCKCHAIN_EVENTS)
                         ↓
┌─────────────────┐      │
│  Event Bridge   │◄─────┘
│                 │
└────────┬────────┘
         │ Temporal Client
         ↓
┌─────────────────┐
│   Temporal      │
│   (Workflows)   │
└────────┬────────┘
         │
    ┌────┴────┐
    ↓         ↓
┌─────────┐ ┌──────────┐
│ Worker  │ │  Worker  │
│  Core   │ │  Media   │
└────┬────┘ └─────┬─────┘
     │           │
     └─────┬─────┘
           │
           ↓
    ┌──────────────┐
    │  PostgreSQL  │
    └──────────────┘
           ↑
           │
    ┌──────┴──────┐
    │             │
┌─────────┐ ┌──────────┐
│  REST   │ │ GraphQL  │
│   API   │ │   API    │
└─────────┘ └──────────┘
```

## Schema Diagram

```
tokens (1) ────┬── (1) token_metadata
               │
               ├── (N) balances
               │
               ├── (N) provenance_events
               │
               ├── (N) enrichment_sources
               │
               └── (N) changes_journal

media_assets (standalone table)

watched_addresses (standalone table)

key_value_store (standalone table)
```

## Scaling Considerations

### Horizontal Scaling

- **Event Emitters**: Can run multiple instances per chain (different start blocks)
- **Event Bridge**: Can run multiple instances (NATS consumer groups)
- **Worker Core**: Can run multiple instances (Temporal task queue)
- **Worker Media**: Can run multiple instances (Temporal task queue)
- **API Server**: Can run multiple instances (stateless)

### Vertical Scaling

- **PostgreSQL**: Connection pooling, query optimization, indexes
- **Temporal**: Separate workflow and activity workers
- **NATS**: Stream replication for high availability

## Error Handling

- **Event Emitters**: Retry on connection failures, save cursor on errors
- **Event Bridge**: NAK messages on processing errors (retry up to MaxDeliver)
- **Temporal Workflows**: Retry policies per activity, non-retryable errors
- **Workers**: Temporal handles retries, dead letter queues

## Monitoring

- **Logging**: Structured logging with zap, Sentry integration
- **Metrics**: Temporal workflow metrics, NATS monitoring
- **Health Checks**: Service health endpoints, database connectivity

## Security

- **Authentication**: JWT for API mutations, API keys
- **Blacklist**: Contract address filtering
- **CORS**: Configurable origins
- **Input Validation**: Schema validation for all inputs

## Configuration

The system supports flexible configuration through YAML files and environment variables.

### Configuration Sources

1. **YAML Config Files**: Each service can use a `config.yaml` file in its `cmd/{service}/` directory
2. **Environment Variables**: Variables with `FF_INDEXER_` prefix override config file values
3. **Environment Files**: `.env` files in `config/` directory (loaded automatically)

### Configuration Priority

1. Environment variables (highest priority)
2. `.env.local` files
3. YAML config files (base configuration)

### Configuration Mapping

Environment variables map to nested YAML keys:
- `FF_INDEXER_DATABASE_HOST` → `database.host`
- `FF_INDEXER_ETHEREUM_RPC_URL` → `ethereum.rpc_url`
- `FF_INDEXER_TEMPORAL_HOST_PORT` → `temporal.host_port`

Dots in YAML keys become underscores in environment variables.

### Example Configuration

**YAML config** (`cmd/api/config.yaml`):
```yaml
database:
  host: localhost
  port: 5432
  user: postgres

server:
  port: 8081
```

**Environment variables** (override YAML):
```bash
export FF_INDEXER_DATABASE_HOST=production-db.example.com
export FF_INDEXER_SERVER_PORT=8080
```

**Result**: Database host and server port use environment variable values, other settings use YAML defaults.

See [DEVELOPMENT.md](../DEVELOPMENT.md) for detailed configuration examples.


