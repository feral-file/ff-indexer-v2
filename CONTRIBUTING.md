# Contributing

Thank you for your interest in contributing to FF-Indexer v2! This document outlines the setup process, development workflow, and PR guidelines.

## Setup

### Prerequisites

- Go 1.24 or later
- Docker and Docker Compose
- PostgreSQL 15+ (for local development)
- Access to Ethereum and/or Tezos RPC endpoints
- Git

### Initial Setup

1. **Fork and clone the repository**:
   ```bash
   git clone https://github.com/feral-file/ff-indexer-v2.git
   cd ff-indexer-v2
   ```

2. **Create environment files**:
   ```bash
   make setup
   ```
   This creates `config/.env` and `config/.env.local` from templates.

3. **Configure environment variables**:
   Edit `config/.env.local` with your local settings:
   - Database credentials
   - NATS URL
   - Temporal connection
   - Ethereum/Tezos RPC endpoints
   - Cloudflare credentials (for media worker)

4. **Start infrastructure**:
   ```bash
   make dev
   ```
   This starts PostgreSQL, Temporal, and NATS in Docker.

5. **Verify setup**:
   - PostgreSQL: `psql -h localhost -U postgres -d ff_indexer`
   - Temporal UI: `http://localhost:8080`
   - NATS: `http://localhost:8222`

## Development Workflow

### Running Services Locally

After starting infrastructure with `make dev`, run services locally:

```bash
# Terminal 1: Ethereum Event Emitter
cd cmd/ethereum-event-emitter && go run main.go

# Terminal 2: Tezos Event Emitter
cd cmd/tezos-event-emitter && go run main.go

# Terminal 3: Event Bridge
cd cmd/event-bridge && go run main.go

# Terminal 4: Worker Core
cd cmd/worker-core && go run main.go

# Terminal 5: Worker Media
cd cmd/worker-media && go run main.go

# Terminal 6: API Server
cd cmd/api && go run main.go
```

### Code Structure

- `cmd/` - Application entry points
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
  - `workflows/` - Temporal workflows and activities
  - `uri/` - URI resolution (IPFS, Arweave, HTTP)
- `db/` - Database migrations and schema
- `tools/` - Development tools and scripts
- `docs/` - Documentation

### Testing

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

This project uses standard Go tooling. Check your code:

```bash
# Format code
go fmt ./...

# Run vet
go vet ./...

# Check for common issues
docker run --rm -v $(pwd):/app -w /app golangci/golangci-lint:v2.1.6 golangci-lint run --verbose
```

### Building

Build all services:
```bash
make build-all
```

Build a specific service:
```bash
make build-api
make build-worker-core
```

## Pull Request Process

### Before Submitting

1. **Create a feature branch**:
   ```bash
   git checkout -b feature/your-feature-name
   ```

2. **Make your changes**:
   - Follow Go conventions and style
   - Add tests for new functionality
   - Update documentation as needed
   - Ensure all tests pass

3. **Commit your changes**:
   - Use clear, descriptive commit messages
   - Reference issue numbers if applicable
   - Follow conventional commits format when possible

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
   - [ ] Tests pass locally
   - [ ] Code is formatted
   - [ ] Documentation updated
   - [ ] No breaking changes (or documented)

### PR Template

When creating a PR, use the template at [.github/PULL_REQUEST_TEMPLATE.md](.github/PULL_REQUEST_TEMPLATE.md). Fill out all relevant sections:

- **Description**: What and why
- **Type of Change**: Bug fix, feature, refactor, etc.
- **Testing**: How to verify changes
- **Checklist**: Pre-submission checklist

### Code Review

- All PRs require at least one approval
- Address review comments promptly
- Keep PRs focused and reasonably sized
- Rebase on main if needed before merging

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
- Use `temporal.NewNonRetryableApplicationError` for non-retryable errors in workflows

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

