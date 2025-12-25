# Temporal Workflow Benchmark Tool

A CLI tool for benchmarking and monitoring Temporal workflow executions, specifically designed for the token indexing workflows with support for nested child workflows.

## Quick Start

```bash
# 1. Build the tool
cd tools/benchmark
make build

# 2. Run it (replace with your actual workflow ID)
./temporal-benchmark -workflow-id index-token-ethereum-mainnet-erc721-0xYOUR_CONTRACT-123

# The tool will:
# - Connect to Temporal
# - Collect workflow statistics recursively
# - Poll in the background until all workflows complete
# - Display final benchmark results

# Or use the Makefile
make run WORKFLOW_ID=your-workflow-id
```

## Features

- **Background Polling**: Continuously polls workflow status until all workflows (including children) complete
- **Recursive Child Workflow Discovery**: Automatically discovers all child workflows at any depth level
- **Child Workflow Breakdown**: Groups child workflows by type and provides detailed statistics
- **Progress Indicator**: Shows real-time progress while collecting data
- **Comprehensive Metrics**:
  - Total execution time
  - Child workflow counts (total, passed, failed, running)
  - First start and last end times for each workflow group
  - Elapsed time for each workflow type group
  - Success rate percentages
  - Average execution rate (workflows/second)
- **Graceful Shutdown**: Displays final results when interrupted (Ctrl+C)
- **Status Indicators**: Visual indicators for workflow status (✅ completed, ❌ failed, 🟡 running, etc.)
- **Configuration File Support**: Save your Temporal connection settings
- **Zero Dependencies**: Uses only Temporal SDK, no external tools required

## Installation

```bash
cd tools/benchmark
go build -o temporal-benchmark
```

## Usage

### Basic Usage

```bash
./temporal-benchmark -workflow-id <workflow-id>
```

### With Markdown Output

```bash
./temporal-benchmark -workflow-id <workflow-id> -output report.md
```

### With All Options

```bash
./temporal-benchmark \
  -temporal-host localhost:7233 \
  -namespace ff-indexer \
  -workflow-id index-token-xyz \
  -run-id abc123 \
  -output benchmark-report.md \
  -concurrency 5 \
  -max-depth 3 \
  -max-workflows 1000 \
  -page-size 1000 \
  -query-timeout 60
```

### For Large Workflow Trees

For workflows with many nested children (like batch indexing workflows):

```bash
# Use more workers for faster collection
./temporal-benchmark -workflow-id large-workflow -concurrency 10

# Limit depth to prevent excessive recursion
./temporal-benchmark -workflow-id large-workflow -max-depth 2

# Limit total workflows collected
./temporal-benchmark -workflow-id large-workflow -max-workflows 500

# Increase timeout for slow queries
./temporal-benchmark -workflow-id large-workflow -query-timeout 60

# Combine all strategies for maximum performance
./temporal-benchmark -workflow-id large-workflow \
  -concurrency 10 \
  -max-depth 2 \
  -max-workflows 500 \
  -query-timeout 60

# Or unlimited (be careful!)
./temporal-benchmark -workflow-id large-workflow \
  -concurrency 10 \
  -max-depth 0 \
  -max-workflows 0
```

### Debug Mode

```bash
./temporal-benchmark -workflow-id index-token-xyz -debug
```

## CLI Options

| Flag | Default | Description |
|------|---------|-------------|
| `-temporal-host` | `localhost:7233` | Temporal host address |
| `-namespace` | `ff-indexer` | Temporal namespace |
| `-workflow-id` | (required) | Workflow ID to monitor |
| `-run-id` | (optional) | Specific run ID to monitor |
| `-config` | (optional) | Path to config file |
| `-output` | (optional) | Output markdown file path |
| `-debug` | `false` | Enable debug logging |
| `-max-depth` | `3` | Maximum depth to traverse (0 = unlimited) |
| `-max-workflows` | `1000` | Maximum workflows to collect (0 = unlimited) |
| `-query-timeout` | `30` | Timeout for each Temporal query in seconds |
| `-page-size` | `1000` | Page size for Temporal queries (max: 1000) |
| `-concurrency` | `5` | Number of concurrent workers (max: 10) |

## Configuration File

You can use a configuration file to avoid typing the same options repeatedly:

```bash
# Copy the example config
cp config.example.json my-config.json

# Edit your config
vim my-config.json

# Use it
./temporal-benchmark -config my-config.json -workflow-id your-workflow-id
```

Example `config.json`:

```json
{
  "temporal_host": "temporal.example.com:7233",
  "namespace": "production"
}
```

**Note**: Command-line flags take precedence over config file values.

## Markdown Output

The tool can generate a markdown report that's optimized for both human reading and LLM processing:

```bash
./temporal-benchmark -workflow-id xyz -output report.md
```

The markdown file includes:
- **Human-readable tables**: Main workflow info, child workflow breakdown
- **Visual indicators**: Emoji status indicators
- **Structured data**: JSON metadata section at the end for LLM parsing

## Performance Optimization

The tool includes several optimizations for handling large workflow trees:

1. **Concurrent Processing**: Uses worker pool pattern with goroutines
   ```bash
   -concurrency 10  # Default: 5 workers, max: 10
   ```
   - Processes multiple workflows in parallel
   - Thread-safe with mutex-protected shared state
   - Dramatically faster for large trees (10x+ speedup)

2. **Page Size**: Uses 1000 items per page (Temporal's maximum) for faster queries
   ```bash
   -page-size 1000  # Default, maximizes query efficiency
   ```

3. **Depth Limiting**: Prevents excessive recursion
   ```bash
   -max-depth 3  # Only go 3 levels deep
   ```

4. **Workflow Cap**: Limits total workflows collected
   ```bash
   -max-workflows 1000  # Stop after 1000 workflows
   ```

5. **Query Timeout**: Configurable timeout per query
   ```bash
   -query-timeout 60  # 60 seconds per query
   ```

## Development

To modify the tool:

1. Edit `main.go`
2. Rebuild: `go build -o temporal-benchmark`
3. Test with a running workflow

## License

Same as ff-indexer-v2 project.

