# Development Guide

This guide covers local development setup, seed data, and useful scripts for FF-Indexer v2.

## Local Development Stack

### Infrastructure Services

The development stack uses Docker Compose for infrastructure services:

- **PostgreSQL** (port 5432) - Main database
- **Temporal** (ports 7233-7235) - Workflow orchestration
- **Temporal UI** (port 8080) - Temporal web interface
- **NATS JetStream** (ports 4222, 8222) - Event streaming

### Starting Infrastructure

Start only infrastructure services:
```bash
make dev
```

Or start everything:
```bash
make quickstart
```

### Infrastructure Access

**PostgreSQL**:
```bash
psql -h localhost -U postgres -d ff_indexer
# Password: postgres (default)
```

**Temporal UI**:
- URL: http://localhost:8080
- View workflows, executions, and history

**NATS Monitoring**:
- URL: http://localhost:8222
- View streams, consumers, and messages

### Configuration

The system supports **dual configuration**: YAML config files and environment variables. You can use either or both together.

**Configuration Priority** (highest to lowest):
1. Environment variables (with `FF_INDEXER_` prefix)
2. `.env.local` files
3. YAML config files (`config.yaml` in each `cmd/*/` directory)

#### Option 1: YAML Config Files

Each service can use a `config.yaml` file in its directory:

```bash
# Copy sample configs
cp cmd/api/config.yaml.sample cmd/api/config.yaml
cp cmd/worker-core/config.yaml.sample cmd/worker-core/config.yaml
cp cmd/worker-media/config.yaml.sample cmd/worker-media/config.yaml
cp cmd/event-bridge/config.yaml.sample cmd/event-bridge/config.yaml
cp cmd/ethereum-event-emitter/config.yaml.sample cmd/ethereum-event-emitter/config.yaml
cp cmd/tezos-event-emitter/config.yaml.sample cmd/tezos-event-emitter/config.yaml

# Edit config files with your settings
```

**Config file location**:
- Default: `cmd/{service}/config.yaml` (relative to service directory)
- Can be overridden with `--config` flag: `go run main.go --config /path/to/config.yaml`

#### Option 2: Environment Variables

Environment variables use the `FF_INDEXER_` prefix and map to nested config keys:
- `FF_INDEXER_DATABASE_HOST` → `database.host`
- `FF_INDEXER_ETHEREUM_RPC_URL` → `ethereum.rpc_url`
- `FF_INDEXER_TEMPORAL_HOST_PORT` → `temporal.host_port`

Dots in config keys become underscores in env vars.

**Environment variable files** (loaded in order, later files override earlier):
1. `config/.env` - Base configuration (version controlled)
2. `config/.env.local` - Local overrides (git ignored)
3. `config/.env.{service}.local` - Service-specific overrides (git ignored)

#### Required Configuration

Secrets settings (can be in YAML config or environment variables):
```bash
# Database
FF_INDEXER_DATABASE_USER=YOUR_DB_USER
FF_INDEXER_DATABASE_PASSWORD=YOUR_DB_PASSWORD
FF_INDEXER_DATABASE_DBNAME=ff_indexer

# Ethereum (for ethereum-event-emitter and worker-core)
FF_INDEXER_ETHEREUM_RPC_URL=YOUR_ETHEREUM_RPC_URL
FF_INDEXER_ETHEREUM_WEBSOCKET_URL=YOUR_ETHEREUM_WEBSOCKET_URL

# Cloudflare (for worker-media)
FF_INDEXER_CLOUDFLARE_ACCOUNT_ID=YOUR_ACCOUNT_ID
FF_INDEXER_CLOUDFLARE_API_TOKEN=YOUR_API_TOKEN

# API authentication
FF_INDEXER_AUTH_JWT_PUBLIC_KEY=YOUR_JWT_PUBKEY_PEM
FF_INDEXER_AUTH_API_KEYS=YOUR_AUTH_API_KEYS
```

**Example**: Mixing YAML and environment variables
- Use `config.yaml` for most settings (version controlled)
- Use `config/.env.local` for sensitive values (passwords, API keys)
- Use environment variables for container/CI overrides

**Environment variable examples**:
```bash
# Database
export FF_INDEXER_DATABASE_HOST=localhost
export FF_INDEXER_DATABASE_USER=postgres
export FF_INDEXER_DATABASE_PASSWORD=postgres
export FF_INDEXER_DATABASE_DBNAME=ff_indexer

# Ethereum
export FF_INDEXER_ETHEREUM_RPC_URL=https://mainnet.infura.io/v3/YOUR_KEY
export FF_INDEXER_ETHEREUM_WEBSOCKET_URL=wss://mainnet.infura.io/ws/v3/YOUR_KEY
export FF_INDEXER_ETHEREUM_CHAIN_ID=eip155:1

# Temporal
export FF_INDEXER_TEMPORAL_HOST_PORT=localhost:7233
export FF_INDEXER_TEMPORAL_NAMESPACE=default
export FF_INDEXER_TEMPORAL_TASK_QUEUE=token-indexing
```

## Running Services Locally

After starting infrastructure, run services with Go:

### Ethereum Event Emitter
```bash
cd cmd/ethereum-event-emitter
go run main.go
```

### Tezos Event Emitter
```bash
cd cmd/tezos-event-emitter
go run main.go
```

### Event Bridge
```bash
cd cmd/event-bridge
go run main.go
```

### Worker Core
```bash
cd cmd/worker-core
go run main.go
```

### Worker Media
```bash
cd cmd/worker-media
go run main.go
```

### API Server
```bash
cd cmd/api
go run main.go
```
API available at: http://localhost:8081

## Database Setup

### Initial Schema

The database schema is automatically created when the PostgreSQL container starts using `db/init_db.sql`.

To manually initialize:
```bash
psql -h localhost -U postgres -d ff_indexer -f db/init_db.sql
```

### Migrations

Migrations are stored in `db/migrations/`. Apply migrations:
```bash
psql -h localhost -U postgres -d ff_indexer -f db/migrations/001.sql
```

### Reset Database

To reset the database (WARNING: deletes all data):
```bash
# Stop services
make down

# Remove volumes
docker volume rm docker_postgres_data

# Start fresh
make dev
```

## Scripts

### NATS Stream Setup

Setup NATS stream manually:
```bash
./tools/scripts/setup_nats.sh
```

Or using Docker:
```bash
make up
# The nats-setup container runs automatically
```

### Database Utilities

**Check database connection**:
```bash
psql -h localhost -U postgres -d ff_indexer -c "SELECT version();"
```

**View tables**:
```bash
psql -h localhost -U postgres -d ff_indexer -c "\dt"
```

**View indexes**:
```bash
psql -h localhost -U postgres -d ff_indexer -c "\di"
```

**Check block cursors**:
```bash
psql -h localhost -U postgres -d ff_indexer -c "SELECT * FROM key_value_store WHERE key LIKE '%cursor%';"
```

## Testing Workflows

### Manual Workflow Execution

Use Temporal CLI to trigger workflows:

```bash
# Install Temporal CLI
# https://docs.temporal.io/cli

# Start a workflow
temporal workflow start \
  --task-queue token-indexing \
  --type IndexToken \
  --workflow-id test-index-token-1 \
  --input '{"token_cid": "eip155:1:erc721:0x1234567890123456789012345678901234567890:1"}'

# Check workflow status
temporal workflow describe --workflow-id test-index-token-1
```

### View Workflows in Temporal UI

1. Open http://localhost:8080
2. Navigate to Workflows
3. Search for your workflow ID
4. View execution history and details

## Debugging

### Logs

View service logs:
```bash
# All services
make logs

# Specific service
make logs-api
make logs-workers
make logs-emitters
```

### Database Debugging

**Check recent tokens**:
```sql
SELECT * FROM tokens ORDER BY created_at DESC LIMIT 10;
```

**Check recent metadata**:
```sql
SELECT t.token_cid, tm.name, tm.enrichment_level, tm.last_refreshed_at
FROM tokens t
JOIN token_metadata tm ON t.id = tm.token_id
ORDER BY tm.last_refreshed_at DESC
LIMIT 10;
```

**Check recent provenance events**:
```sql
SELECT * FROM provenance_events ORDER BY timestamp DESC LIMIT 10;
```

**Check NATS messages**:
```bash
# View stream info
nats stream info BLOCKCHAIN_EVENTS

# View consumer info
nats consumer info BLOCKCHAIN_EVENTS event-bridge
```

### Common Issues

**Database connection errors**:
- Check PostgreSQL is running: `docker ps`
- Verify connection string in config
- Check firewall/network settings

**Temporal connection errors**:
- Verify Temporal is running: `docker ps`
- Check Temporal UI: http://localhost:8080
- Verify namespace and task queue names

**NATS connection errors**:
- Check NATS is running: `docker ps`
- Verify stream exists: `nats stream info BLOCKCHAIN_EVENTS`
- Check NATS monitoring: http://localhost:8222

**Event emitter not receiving events**:
- Verify WebSocket connection to blockchain RPC
- Check block cursor in database
- Verify contract addresses are correct
- Check for blacklisted contracts

## Performance Testing

### Load Testing

Use the API to trigger indexing:

```bash
# Index a single token
curl -X POST http://localhost:8081/api/v1/tokens/index \
  -H "Authorization: ApiKey test-api-key" \
  -H "Content-Type: application/json" \
  -d '{"token_cid": "eip155:1:erc721:0x1234567890123456789012345678901234567890:1"}'

# Index multiple tokens
curl -X POST http://localhost:8081/api/v1/tokens/index-batch \
  -H "Authorization: ApiKey test-api-key" \
  -H "Content-Type: application/json" \
  -d '{"token_cids": ["eip155:1:erc721:0x123...:1", "eip155:1:erc721:0x123...:2"]}'
```

### Monitoring

**Temporal Metrics**:
- View workflows and activities in Temporal UI
- Check execution times and failure rates

**Database Performance**:
```sql
-- As superuser
CREATE EXTENSION IF NOT EXISTS pg_stat_statements;
```

Then add the following line to postgresql.conf:
```
shared_preload_libraries = 'pg_stat_statements'
```

```sql
-- Check slow queries
SELECT query, calls, total_exec_time, mean_exec_time
FROM pg_stat_statements
ORDER BY mean_exec_time DESC
LIMIT 10;

-- Check table sizes
SELECT 
  schemaname,
  tablename,
  pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) AS size
FROM pg_tables
WHERE schemaname = 'public'
ORDER BY pg_total_relation_size(schemaname||'.'||tablename) DESC;
```

## Cleanup

### Stop Services

```bash
# Stop all services
make down

# Stop but keep containers
make stop
```

### Clean Volumes

```bash
# Remove all data (WARNING: deletes everything)
make clean
```

### Clean Images

```bash
# Remove built Docker images
make clean-images
```

## Tips

1. **Use Temporal UI** for workflow debugging and monitoring
2. **Check NATS monitoring** for event flow issues
3. **Use database transactions** when testing data changes
4. **Monitor logs** in real-time with `make logs`
5. **Use GraphQL Playground** at http://localhost:8081/graphql for API testing
6. **Keep infrastructure running** and restart only application services during development

## Next Steps

- Read [Architecture](docs/architecture.md) for system design details
- Read [Schema](docs/schema.md) for database structure
- Read [Contributing](CONTRIBUTING.md) for PR guidelines

