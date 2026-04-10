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
- **Metadata enrichment** from vendor APIs (Art Blocks, fxhash, Foundation, SuperRare, Feral File, Objkt, OpenSea)
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
# Option 2: Copy the sample config and customize
#   cp cmd/ff-indexer/config.yaml.sample config/config.yaml

# Build and start all services
make quickstart
```

**Configuration**: The system supports both YAML config files and environment variables. Environment variables (with `FF_INDEXER_` prefix) override config file values. See [DEVELOPMENT.md](DEVELOPMENT.md) for details.

This will start:
- PostgreSQL (port 5432)
- Temporal server (ports 7233-7235) and UI (port 8080)
- NATS JetStream (ports 4222, 8222)
- Redis
- **ff-indexer** — single container running the HTTP API, chain emitters, NATS event bridge, Temporal workers (token + media when built with CGO), and media health sweeper

The API will be available at `http://localhost:8081`

### Local Development

For local development, you can run infrastructure in Docker and the application locally:

```bash
# Start only infrastructure (PostgreSQL, Temporal, NATS, Redis)
make dev

# Run the binary (CGO optional; without CGO the media worker is disabled)
go run ./cmd/ff-indexer -config cmd/ff-indexer/config.yaml
```

See [DEVELOPMENT.md](DEVELOPMENT.md) for detailed local development setup.

## Documentation

- **[Architecture](docs/architecture.md)** - System design, components, and data flow diagrams
- **[Database Schema](docs/schema.md)** - Complete database schema and migration notes
- **[Development Guide](DEVELOPMENT.md)** - Local development setup, seed data, and scripts
- **[Contributing Guide](CONTRIBUTING.md)** - Setup, linting, testing, and PR process
- **[Roadmap](roadmap.md)** - Planned features and future improvements

## Components

All of the following run inside the **`ff-indexer`** process (goroutines) by default:

- **Event emitters** — Ethereum and Tezos chain listeners publishing to NATS
- **Event bridge** — NATS consumer that starts Temporal workflows
- **Worker core** — Temporal worker on `token-indexing`
- **Worker media** — Temporal worker on `media-indexing` (requires CGO / full Docker image)
- **API server** — REST and GraphQL
- **Sweeper** — Media URL health checks

## Requirements

- Go 1.24+
- Docker and Docker Compose
- PostgreSQL 18+
- Temporal 1.29.0+
- Redis 8.2.2+
- NATs Jetstream 2.12.1+
- Access to Ethereum RPC endpoints

## License

See [LICENSE](LICENSE) for details.
