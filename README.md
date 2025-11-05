# FF-Indexer v2

[![Tests](https://github.com/feral-file/ff-indexer-v2/actions/workflows/test.yaml/badge.svg)](https://github.com/feral-file/ff-indexer-v2/actions/workflows/test.yaml)
[![Lint](https://github.com/feral-file/ff-indexer-v2/actions/workflows/lint.yaml/badge.svg)](https://github.com/feral-file/ff-indexer-v2/actions/workflows/lint.yaml)
[![codecov](https://codecov.io/github/feral-file/ff-indexer-v2/graph/badge.svg?token=8S74fEIBNs)](https://codecov.io/github/feral-file/ff-indexer-v2)

A comprehensive NFT indexing system that tracks, processes, and provides access to NFT data across Ethereum and Tezos blockchains.

## Purpose

FF-Indexer v2 is a production-ready indexing service designed to capture and index NFT data from multiple blockchain networks. It supports:

- **Ethereum**: ERC-721 and ERC-1155 tokens
- **Tezos**: FA2 tokens
- **Real-time indexing** of blockchain events (mints, transfers, burns, metadata updates)
- **Metadata resolution** from IPFS, Arweave, ONCHFS, and HTTP sources
- **Metadata enrichment** from vendor APIs (Art Blocks, fxhash, Foundation, SuperRare, Feral File)
- **Media processing** with Cloudflare Images and Stream
- **Provenance tracking** with full blockchain event history
- **Owner-based indexing** for wallet-based queries

The system is built with scalability and reliability in mind, using Temporal for workflow orchestration, NATS JetStream for event streaming, and PostgreSQL for persistent storage.

## Quick Start

### Docker Compose (Recommended)

The easiest way to run the full stack:

```bash
# Clone the repository
git clone https://github.com/feral-file/ff-indexer-v2.git
cd ff-indexer-v2

# Setup environment files
make setup

# Configure your settings (choose one or both):
# Option 1: Edit config/.env.local with your credentials
# Option 2: Copy config.yaml.sample files and customize
#   cp cmd/api/config.yaml.sample cmd/api/config.yaml
#   # Edit cmd/api/config.yaml, cmd/worker-core/config.yaml, etc.

# Build and start all services
make quickstart
```

**Configuration**: The system supports both YAML config files and environment variables. Environment variables (with `FF_INDEXER_` prefix) override config file values. See [DEVELOPMENT.md](DEVELOPMENT.md) for details.

This will start:
- PostgreSQL (port 5432)
- Temporal server (ports 7233-7235) and UI (port 8080)
- NATS JetStream (ports 4222, 8222)
- All application services (event emitters, workers, API)

The API will be available at `http://localhost:8081`

### Local Development

For local development, you can run infrastructure in Docker and services locally:

```bash
# Start only infrastructure (PostgreSQL, Temporal, NATS)
make dev

# Run services locally with Go
cd cmd/ethereum-event-emitter && go run main.go
cd cmd/tezos-event-emitter && go run main.go
cd cmd/event-bridge && go run main.go
cd cmd/worker-core && go run main.go
cd cmd/worker-media && go run main.go
cd cmd/api && go run main.go
```

See [DEVELOPMENT.md](DEVELOPMENT.md) for detailed local development setup.

## Documentation

- **[Architecture](docs/architecture.md)** - System design, components, and data flow diagrams
- **[Database Schema](docs/schema.md)** - Complete database schema and migration notes
- **[Development Guide](DEVELOPMENT.md)** - Local development setup, seed data, and scripts
- **[Contributing Guide](CONTRIBUTING.md)** - Setup, linting, testing, and PR process

## Components

- **Event Emitters** (`ethereum-event-emitter`, `tezos-event-emitter`) - Subscribe to blockchain events and publish to NATS
- **Event Bridge** (`event-bridge`) - Consumes events from NATS and triggers Temporal workflows
- **Worker Core** (`worker-core`) - Executes Temporal workflows for token indexing, metadata resolution, and enrichment
- **Worker Media** (`worker-media`) - Processes and uploads media files to Cloudflare
- **API Server** (`api`) - Provides REST and GraphQL APIs for querying indexed data

## Requirements

- Go 1.24+
- Docker and Docker Compose
- PostgreSQL 15+
- Access to Ethereum RPC endpoints
- Cloudflare account (for media processing)

## License

See [LICENSE](LICENSE) for details.
