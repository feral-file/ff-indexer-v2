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
cd cmd/sweeper && go run main.go
```

See [DEVELOPMENT.md](DEVELOPMENT.md) for detailed local development setup.

## Verification

Use the repo-wide CI-shaped verification path before handing off substantive changes:

```bash
make post-implementation-check
```

This target runs `tools/scripts/post_implementation_check.sh`, which is the same non-mutating verification path used by the GitHub Actions Test workflow. It checks changed Go files for formatting and strict lint issues, then runs the CI test package set with coverage output and generated-file filtering.

`make post-implementation-check` expects PostgreSQL test dependencies at `localhost:5432` by default, matching CI. Start them locally with `make dev`, or override `TEST_DB_HOST`, `TEST_DB_PORT`, `TEST_DB_USER`, `TEST_DB_PASSWORD`, and `TEST_DB_NAME`.

Use `make check` only when you intentionally want the broader local maintenance pass. It runs `imports`, `lint-local`, and `test`; `imports` rewrites Go imports, so it is not the non-mutating CI-shaped verification command.

## GitHub Actions

This repo has these primary GitHub Actions workflows:

- **Test** (`.github/workflows/test.yaml`) - sets up Go 1.24.13, libvips, and PostgreSQL, then runs `make post-implementation-check` and uploads coverage to Codecov.
- **Lint** (`.github/workflows/lint.yaml`) - runs repo-wide `golangci-lint` v2.5.0 and a `gofmt` check with Go 1.24.13.
- **Build Images** (`.github/workflows/build-images.yaml`) - builds and publishes container images.
- **Secret Scan** (`.github/workflows/secret-scan.yaml`) - scans for leaked secrets.

## Documentation

- **[Agent Guide](AGENTS.md)** - Repository workflow, canonical verification, and PR/review contract for agents and contributors
- **[Architecture](docs/architecture.md)** - System design, components, and data flow diagrams
- **[Database Schema](docs/schema.md)** - Complete database schema and migration notes
- **[Development Guide](DEVELOPMENT.md)** - Local development setup, seed data, and scripts
- **[Contributing Guide](CONTRIBUTING.md)** - Setup, linting, testing, and PR process
- **[Roadmap](roadmap.md)** - Planned features and future improvements

## Components

- **Event Emitters** (`ethereum-event-emitter`, `tezos-event-emitter`) - Subscribe to blockchain events and publish to NATS
- **Event Bridge** (`event-bridge`) - Consumes events from NATS and triggers Temporal workflows
- **Worker Core** (`worker-core`) - Executes Temporal workflows for token indexing, metadata resolution, and enrichment
- **Worker Media** (`worker-media`) - Processes and uploads media files to Cloudflare
- **API Server** (`api`) - Provides REST and GraphQL APIs for querying indexed data
- **Sweeper** (`sweeper`) - Continuously monitors media URL health and updates status

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
