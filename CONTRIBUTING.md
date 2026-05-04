# Contributing

Thank you for your interest in contributing to FF-Indexer v2! This document outlines the setup process, development workflow, and PR guidelines.

## Setup

### Prerequisites

- Go 1.25.0 or later
- Docker and Docker Compose
- PostgreSQL 18+ (for local development)
- Access to Ethereum and/or Tezos RPC endpoints
- Git

### Initial Setup

1. **Fork and clone the repository**:
   ```bash
   git clone https://github.com/feral-file/ff-indexer-v2.git
   cd ff-indexer-v2
   ```

2. **Configure your environment** (choose one or both):

   **Option A: Environment Variables**:
   ```bash
   make setup
   ```
   This creates `config/.env` and `config/.env.local` from templates.
   Edit `config/.env.local` with your local settings.

   **Option B: YAML Config File**:
   ```bash
   cp cmd/ff-indexer/config.yaml.sample config/config.yaml
   ```
   Edit `config/config.yaml` with your settings.

3. **Required configuration** (in env vars or YAML):
   - Database credentials (PostgreSQL; the app also uses the **`jobs` table** as its work queue)
   - Ethereum RPC (and Tezos indexer/RPC as needed for your chains)
   - Cloudflare credentials (only when `FF_INDEXER_MEDIA_ENABLED=true`)
   - API authentication (JWT public key or API keys)

   **Note**: Environment variables (with `FF_INDEXER_` prefix) override YAML config values. See [DEVELOPMENT.md](DEVELOPMENT.md) for configuration details.

4. **Start infrastructure**:
   ```bash
   make dev
   ```
   This brings up Docker Compose from [`tools/docker/Docker-compose.yaml`](tools/docker/Docker-compose.yaml) (PostgreSQL for the app database and `jobs` queue, plus `ff-indexer` when using `make up` / `make quickstart`).

5. **Verify setup**:
   - PostgreSQL: `psql -h localhost -U postgres -d ff_indexer`
   - After `go run ./cmd/ff-indexer`, API: `http://localhost:8081` (port from config)

## Development Workflow

### Running Locally

After starting infrastructure with `make dev`, run the binary:

```bash
go run ./cmd/ff-indexer -config config/config.yaml
```

### Code Structure

- `cmd/ff-indexer` - Single application entrypoint (HTTP API, chain ingestion, postgres-backed job workers, sweeper)
- `internal/` - Internal packages (not exported)
  - `adapter/` - External service adapters
  - `api/` - API handlers (REST, GraphQL)
  - `bridge/` - Event bridge logic
  - `config/` - Configuration loading
  - `domain/` - Domain models and types
  - `emitter/` - Event emitter logic
  - `metadata/` - Metadata resolution and enrichment
  - `providers/` - Blockchain and external service providers
  - `registry/` - Blacklist and publisher registries
  - `store/` - Database layer
  - `workflows/` - In-process workflow handlers invoked by the job worker (orchestration via `jobs` table)
  - `uri/` - URI resolution (IPFS, Arweave, HTTP)
- `db/` - Database migrations and schema
- `tools/` - `docker/` (Compose and images), `registry/` (publisher/blacklist JSON fixtures). There is no `tools/benchmark` CLI.
- `docs/` - Documentation

### Testing

The canonical pre-review verification command is:

```bash
make check
```

This runs import formatting, full-repo `golangci-lint` (with CGO), a lightweight `CGO_ENABLED=0` build and `cmd/ff-indexer` tests, then `CGO_ENABLED=1` `go test -cover ./...`. See the `check` target in the `Makefile` for the exact dependency chain.

The lint profile checks cyclomatic and cognitive complexity, function and file length, and doc quality.

Coverage policy is non-regression versus the base branch. If coverage drops, explain why in the PR body and call out any follow-up work.

For narrower debugging loops, you can still run tests directly:

Run tests:
```bash
go test ./...
```

Run tests with coverage:
```bash
go test -cover ./...
```

Run tests for a specific package:
```bash
go test ./internal/store/...
```

### Linting

`make check` is the authoritative lint-and-test gate for substantive changes.

For narrower maintenance work, this project also uses standard Go tooling:

```bash
# Format code
go fmt ./...

# Run vet
go vet ./...

# Check for common issues
docker run --rm -v $(pwd):/app -w /app golangci/golangci-lint:v2.1.6 golangci-lint run --verbose
```

### Building

Build the default lightweight image:
```bash
make build
```

Build the full image with media processing:
```bash
make build-full
```

## Pull Request Process

### Before Submitting

1. **Create a feature branch**:
   ```bash
   git checkout -b feature/your-feature-name
   ```

2. **Make your changes**:
   - Follow Go conventions and style
   - Prefer extracting helpers or simplifying control flow before accepting a large function
   - Add tests for new functionality
   - Update documentation as needed
   - Add doc comments to changed Go functions
   - For non-trivial changed functions, use the doc comment to record the reason, trade-offs, and constraints behind the implementation
   - Run `make check`

3. **Commit your changes**:
   - Use clear, descriptive commit messages
   - Reference issue numbers if applicable
   - Follow conventional commits format

4. **Push to your fork**:
   ```bash
   git push origin feature/your-feature-name
   ```

### PR Guidelines

1. **Title**: Clear, descriptive title
2. **Description**: 
   - What changes were made
   - Why the changes were needed
   - How to test the changes
   - Any breaking changes

3. **Link to Issue**: Reference related issues

4. **Checklist**: 
   - [ ] `make check` passes locally, or the blocker is documented
   - [ ] Code is formatted
   - [ ] Documentation updated
   - [ ] No breaking changes (or documented)

### PR Template

When creating a PR, use the template at [.github/PULL_REQUEST_TEMPLATE.md](.github/PULL_REQUEST_TEMPLATE.md). Fill out all relevant sections:

- **Problem**: What is changing
- **Why It Matters**: Why the work should land now
- **Acceptance Checks**: 1-3 concrete checks reviewers can use
- **Human Owner**: Who owns the outcome
- **How The Agent Will Be Used**: What the agent did for implementation, review, or follow-up
- **PR or Deploy Link**: The relevant PR, deploy, or release reference

### Code Review

- All PRs require at least one approval
- Address review comments promptly
- Keep PRs focused and reasonably sized
- Rebase on main if needed before merging
- Review the full diff before requesting review and again after addressing feedback
- Rerun `make check` after each substantive review update

## Coding Standards

### Go Style

- Follow [Effective Go](https://go.dev/doc/effective_go) guidelines
- Use `gofmt` for formatting
- Prefer explicit error handling
- Use meaningful variable names
- Keep functions small and focused

### Error Handling

- Always handle errors explicitly
- Use `fmt.Errorf` with `%w` for error wrapping
- Return errors, don't log and ignore
- Return wrapped errors from handlers; the v1 job worker does not distinguish retryable types (failed jobs stay `failed` with `last_error`)

### Logging

- Use structured logging with `zap`
- Use appropriate log levels (Debug, Info, Warn, Error)
- Include context in log messages
- Use `logger.InfoCtx` for context-aware logging

### Testing

- Write unit tests for new functions
- Use table-driven tests when appropriate
- Mock external dependencies
- Test error cases

### Database

- Use transactions for multi-step operations
- Handle connection errors gracefully
- Use prepared statements for queries
- Validate inputs before database operations

## Documentation

- Update relevant documentation when making changes
- Add comments for exported functions
- Document complex algorithms or business logic
- Update architecture docs for significant changes

## Questions?

- Open an issue for bugs or feature requests
- Check existing issues and discussions
- Review the codebase and documentation

Thank you for contributing!
