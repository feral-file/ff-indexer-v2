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

The system is built with reliability and operational clarity in mind, using a PostgreSQL-backed `jobs` queue for background work and the same database for all durable state, while chain ingestion runs in-process.

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
- (Optional in compose) Temporal and UI if still included in the stack for other use
- **ff-indexer** - single container running the HTTP API, chain ingestion, job workers (token by default, media when built with CGO), and media health sweeper

The API will be available at `http://localhost:8081`

### Local Development

For local development, you can run infrastructure in Docker and the application locally:

```bash
# Start only infrastructure (PostgreSQL, etc.)
make dev

# Run the binary (CGO optional; without CGO the media worker is disabled)
go run ./cmd/ff-indexer -config config/config.yaml
```

- Lightweight mode: `CGO_ENABLED=0`, media worker stub only.
- Full media mode: `CGO_ENABLED=1`, `FF_INDEXER_MEDIA_ENABLED=true`, and Cloudflare media config.

See [DEVELOPMENT.md](DEVELOPMENT.md) for detailed local development setup.

## Documentation

- **[Agent Guide](AGENTS.md)** - Repository workflow, canonical verification, and PR/review contract for agents and contributors
- **[Architecture](docs/architecture.md)** - System design, components, and data flow diagrams
- **[Database Schema](docs/schema.md)** - Complete database schema and migration notes
- **[Development Guide](DEVELOPMENT.md)** - Local development setup, seed data, and scripts
- **[Contributing Guide](CONTRIBUTING.md)** - Setup, linting, testing, and PR process
- **[Roadmap](roadmap.md)** - Planned features and future improvements

## Components

All of the following run inside the **`ff-indexer`** process (goroutines) by default:

- **Chain ingestion** - Ethereum and Tezos event subscriptions plus ordered in-memory flush queues that enqueue jobs and advance durable cursors
- **Worker core** — polls the `token_index` job queue
- **Worker media** — polls the `media_index` job queue (requires CGO / full Docker image and is disabled by default unless `FF_INDEXER_MEDIA_ENABLED=true`)
- **API server** — REST and GraphQL
- **Sweeper** — Media URL health checks

## Requirements

- Go 1.25.0+
- Docker and Docker Compose
- PostgreSQL 18+
- Access to Ethereum RPC endpoints

## License

See [LICENSE](LICENSE) for details.
