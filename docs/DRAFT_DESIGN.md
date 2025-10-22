# Feral File Indexer Knowledge Base

## 1. Requirements

### Overview

The Feral File Indexer is a **token indexing service** that provides unified indexing and query capabilities for tokens across multiple blockchains and standards.

### Functional Requirements

* **Supported standards**: Ethereum ERC-721, ERC-1155, and Tezos FA2.
* **Capabilities**:

  * Index token **ownership**, **provenance**, **metadata**, and **media** assets.
  * Support indexing by **individual token ID** and by **owner address**.
  * Support **real-time indexing** using blockchain event watchers.
  * Allow **manual indexing triggers** via RESTful API.
  * Provide both **RESTful** and **GraphQL** query interfaces.
* **Data Sources**:

  * **Blockchain** (Infura, TzKT): Source of truth for ownership and provenance.
  * **Vendor APIs** (OpenSea, ArtBlocks): Used *only* for metadata enrichment.

### Non-functional Requirements

* High reliability and idempotency.
* Durable event orchestration and replay support.
* Horizontal scalability on Kubernetes/EKS.
* Secure public APIs (JWT-based authentication).
* Use interface/abstraction approach for module injection for testability, unit test using gomock.
* Each component should include the test file to cover the unit tests.

---

## 2. Technology Stack

| Layer             | Component                       | Purpose                                                      |
| ----------------- | ------------------------------- | ------------------------------------------------------------ |
| **Language**      | Golang 1.24+                    | Core implementation                                          |
| **Database**      | PostgreSQL (JSONB support)      | Storage for tokens, metadata, media, provenance, and changes |
| **Orchestration** | Temporal/Cadence                | Durable event orchestration and workflow management          |
| **Messaging**     | NATS JetStream                  | Reliable queue for event delivery and retries                |
| **Blockchain**    | Infura (Ethereum), TzKT (Tezos) | Real-time ownership and provenance data                      |
| **Vendors**       | OpenSea, ArtBlocks              | Optional metadata enrichment                                 |
| **API**           | RESTful + GraphQL               | Query and control interface                                  |
| **Infra**         | Kubernetes/EKS                  | Deployment and scaling                                       |
| **CDN**           | Cloudflare Images/Stream        | Image/video caching and streaming                            |
| **Testing**       | Gomock, k6                      | Unit/integration/load testing                                |

---

## 3. Database Schema

Simplified from the original design — excludes `contracts` and `chain_cursors` tables.

### 3.1 Tokens

```sql
CREATE TABLE tokens (
  id BIGSERIAL PRIMARY KEY,
  token_cid TEXT NOT NULL UNIQUE,             -- canonical id: eip155:1:erc721:0x...:1234
  chain TEXT NOT NULL,                        -- eip155:1 or tezos:mainnet
  standard TEXT NOT NULL,                     -- erc721, erc1155, fa2
  contract_address TEXT NOT NULL,
  token_number TEXT NOT NULL,
  current_owner TEXT,
  burned BOOLEAN NOT NULL DEFAULT FALSE,
  last_activity_time TIMESTAMPTZ NOT NULL DEFAULT now(),
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (chain, contract_address, token_number)
);
```

### 3.2 Balances

```sql
CREATE TABLE balances (
  id BIGSERIAL PRIMARY KEY,
  token_id BIGINT NOT NULL REFERENCES tokens (id) ON DELETE CASCADE,
  owner_address TEXT NOT NULL,
  quantity NUMERIC(78,0) NOT NULL,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (token_id, owner_address)
);
```

### 3.3 Token Metadata

```sql
CREATE TABLE token_metadata (
  token_id BIGINT PRIMARY KEY REFERENCES tokens (id) ON DELETE CASCADE,
  origin_json JSONB,
  latest_json JSONB,
  latest_hash TEXT,
  enrichment_level TEXT NOT NULL DEFAULT 'none',
  last_refreshed_at TIMESTAMPTZ,
  image_url TEXT,
  animation_url TEXT,
  name TEXT,
  artist TEXT
);
```

### 3.4 Enrichment Sources

```sql
CREATE TABLE enrichment_sources (
  id BIGSERIAL PRIMARY KEY,
  token_id BIGINT NOT NULL REFERENCES tokens (id) ON DELETE CASCADE,
  vendor TEXT NOT NULL,                   -- 'opensea','artblocks','onchain'
  source_url TEXT,
  etag TEXT,
  last_status INTEGER,
  last_error TEXT,
  last_fetched_at TIMESTAMPTZ,
  last_hash TEXT,
  UNIQUE (token_id, vendor)
);
```

### 3.5 Media Assets

```sql
CREATE TABLE media_assets (
  id BIGSERIAL PRIMARY KEY,
  token_id BIGINT NOT NULL REFERENCES tokens (id) ON DELETE CASCADE,
  role TEXT NOT NULL,                     -- 'image','animation','poster'
  source_url TEXT,
  content_hash TEXT,
  cf_image_id TEXT,
  cf_variant_map JSONB,                   -- {"thumb":..., "fit":...}
  status TEXT NOT NULL DEFAULT 'pending',
  last_checked_at TIMESTAMPTZ,
  created_at TIMESTAMPTZ DEFAULT now(),
  UNIQUE (token_id, role)
);
```

### 3.6 Changes Journal

```sql
CREATE TABLE changes_journal (
  cursor BIGSERIAL PRIMARY KEY,
  subject_type TEXT NOT NULL,             -- token, owner, balance, metadata, media
  subject_id TEXT NOT NULL,               -- token_cid
  changed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  meta JSONB
);
```

### 3.7 Provenance Events (optional)

```sql
CREATE TABLE provenance_events (
  id BIGSERIAL PRIMARY KEY,
  token_id BIGINT NOT NULL REFERENCES tokens (id) ON DELETE CASCADE,
  chain TEXT NOT NULL,
  event_type TEXT NOT NULL,               -- mint, transfer, burn, metadata_update
  from_address TEXT,
  to_address TEXT,
  quantity NUMERIC(78,0),
  tx_hash TEXT,
  block_number BIGINT,
  block_hash TEXT,
  timestamp TIMESTAMPTZ NOT NULL,
  raw JSONB,
  created_at TIMESTAMPTZ DEFAULT now()
);
```

### 3.8 Watched addresses
```sql
-- Watched addresses (for owner-based indexing)
CREATE TABLE watched_addresses (
  chain          TEXT        NOT NULL,
  address        TEXT        NOT NULL,
  watching       BOOLEAN     NOT NULL DEFAULT TRUE,
  added_by       TEXT        NOT NULL,  -- system/user/tenant/source of watch request
  reason         TEXT,                  -- e.g. "owner_index_request" | "manual"
  last_queried_at TIMESTAMPTZ,          -- when API last queried this address
  created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (chain, address)
);

CREATE INDEX idx_watched_addresses_watching
  ON watched_addresses (watching, chain, address);

```

---

## 4. Services

### 4.1 Blockchain Event Watchers

* **ethereum-event-emitter**

  * Connects to Infura WebSocket (eth_subscribe) and HTTP for backfill.
  * Listens for ERC721/1155 Transfer events.
  * Listens for EIP-4906 events for metadata update on ERC-721 tokens. For ERC-1155 token, listen standard built-in event. 
  * Finality depth: 12 blocks.
* **tezos-event-emitter**

  * Connects to TzKT WebSocket + HTTP.
  * Listens for FA2 transfer operations.
  * Listens on the Bitmaps change event for the metadata update.
  * Finality depth: 3 blocks.

Both emitters:

* Normalize events → standard JSON event objects.
* Publish to NATS JetStream subjects (e.g., `chain.eth.events.transfer`, `chain.tezos.events.fa2_transfer`).

### 4.2 Event Bridge

* JetStream consumer (explicit ACK).
* Coalesces events by token key for ~1s.
* Invokes Temporal using `SignalWithStart(IndexTokenWorkflow)`.
* ACK only after Temporal accepts the signal.
* NAK or TERM on failure, parked DLQ for poison messages.
* Maintain an in-memory cache for watched addresses to determine to drop event or forward to workers.
* Update the watched address by handle the NATS event `watchlist.delta`

### 4.3 Worker Core

* Temporal worker that runs workflows and activities for token indexing:

  * **IndexTokenWorkflow**: per-token, idempotent, long-lived, handles new events.
  * **Activities**: ownership upsert, provenance update, metadata enrich.
* Activities have retry policies; workflows never hard-fail.
* Writes to `tokens`, `balances`, `token_metadata` and `changes_journal`.

### 4.4 Worker Media
* Temporal worker that runs workflows and activities for token media indexing:
  * ***IndexTokenMedia**: per-token, idempotent, long-lived.
  * **Activities**: download media, upload to storage provider, cache, write to `token_media`

### 4.5 Metadata Enrichment

* Pull original token metadata directly from blockchain (tokenURI).
* Enrich using vendor APIs (OpenSea, ArtBlocks) with TTL + ETag checks.
* Update only when hash changes to prevent redundant writes.

### 4.6 RESTful & GraphQL API

* **Endpoints:**

  * `GET /api/v1/tokens/:id`
  * `GET /api/v1/tokens?owners=`
  * `GET /api/v1/changes?since=`
  * `POST /api/v1/tokens` (trigger indexing)
* **Auth:** JWT bearer tokens.
* **GraphQL:** gqlgen implementation with DataLoader and persisted queries.

---

## 5. Project Structure

```
ff-indexer/
├── cmd/
│   ├── api/                 # REST + GraphQL server (JWT auth)
│   ├── worker-core/         # Temporal worker (token indexing, enrichment)
│   ├── worker-media/        # Temporal worker (media indexing)
│   ├── ethereum-event-emitter/
│   ├── tezos-event-emitter/
│   ├── event-bridge/        # NATS consumer → Temporal signaler
│   └── sweeper/             # Periodic catch-up (resignal stale tokens)
│
├── internal/
│   ├── api/                 # REST + GraphQL handlers, resolvers, middleware
│   ├── config/              # env parsing + validation
│   ├── domain/              # pure business logic (folding, token events)
│   ├── store/pg/            # Postgres repositories (tokens, metadata, media)
│   ├── providers/           # blockchain, vendors, cloudflare, jetstream, temporal
│   ├── workflows/           # Temporal workflows + activities
│   └── log/                 # unified structured logger
│
├── db/migrations/           # SQL migrations (aligned with schema above)
├── api/openapi/             # REST schema
├── api/graphql/             # GraphQL schema + gqlgen config
├── tools/                   # dev helpers, scripts, dockerfiles
├── go.mod
└── Makefile
```

---

This knowledge base should guide an LLM or developer to understand the architectural layout, data model, and flow of the **Feral File Indexer**, enabling accurate implementation or extension of the system.
