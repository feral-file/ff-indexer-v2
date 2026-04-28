# Roadmap

This document outlines the planned features and improvements for FF-Indexer v2. The items are ordered by priority and represent our vision for expanding the platform's capabilities.

## Lighter Version for FF1 Deployment

### Overview

The current FF-Indexer v2 **production** shape is a **single `ff-indexer` process** (HTTP API, chain ingestion, postgres-backed **`jobs` workers**, sweeper) with **PostgreSQL** as the system of record. Optional **Cloudflare** media and **CGO** add weight. For deployment on FF1 (resource limited), we need a variant that runs efficiently with minimal resources while keeping core indexing.

### Goals

Create a streamlined version of the indexer optimized for FF1 that:

- **Small Device Compatibility**: Designed to run efficiently on FF1 hardware with limited resources
- **Minimal Dependencies**: Fewer moving parts (e.g. no Cloudflare, smaller optional stacks)
- **Self-Hosted Media**: Process and serve media files locally without external cloud services
- **GraphQL-First API**: Provide GraphQL as the primary and only API interface
- **Core Functionality**: Maintain essential indexing capabilities while removing resource-intensive features

### Proposed Architecture

The FF1 lightweight version will make significant product/architecture changes on top of the existing **single-process + `jobs` queue** model:

1. **Remove Cloudflare Integration**: Eliminate dependency on Cloudflare Images and Stream services where FF1 cannot use them
2. **Local Media Processing**: Process media files (images, videos) locally and store them on the device's filesystem
3. **Tighter resource profile**: Optional SQLite (or smaller Postgres), reduced enrichment defaults, and stricter limits as needed for the device class

### Features to Include

- **Core Indexing**: Token mint, transfer, and burn event processing
- **Basic Metadata Resolution**: IPFS, Arweave, and HTTP URI resolution
- **GraphQL API**: Complete GraphQL API for querying indexed tokens, metadata, and provenance
- **Local Media Storage**: Download, process, and serve media files from local filesystem
- **Database**: PostgreSQL (or SQLite for even lighter deployments) for data persistence
- **Event-driven indexing**: Chain ingestion enqueues **`jobs`** (no separate message broker required)

### Features to Remove or Replace

- **Cloudflare Services**: Removed entirely for FF1; media processed and served locally
- **Advanced Enrichment**: Optional; can be disabled to reduce resource usage
- **Split deployment roles**: Full tree already runs workers in-process; FF1 avoids extra operational surfaces beyond the device

### Implementation Strategy

1. **Lightweight Alternatives**: 
   - Keep postgres-backed **`jobs`** (or evaluate SQLite / smaller DB for FF1)
   - Use local filesystem for media storage instead of Cloudflare

2. **Configuration Modes**: 
   - Add "ff1" deployment mode that automatically disables heavy dependencies
   - Feature flags to enable/disable optional components (enrichment, media processing)

3. **Modular Design**: 
   - Conditional compilation or runtime initialization based on deployment mode
   - Separate build targets for full vs. FF1 versions if needed

4. **Resource Optimization**:
   - Optimize database queries for smaller datasets
   - Implement efficient media processing pipelines
   - Add resource usage monitoring and limits

5. **Documentation**: 
   - Provide FF1-specific deployment guide
   - Document resource requirements and optimization tips

### Benefits

- **FF1 Compatibility**: Designed specifically for small device deployment with resource constraints
- **Self-Sufficiency**: No dependency on external cloud services (Cloudflare)
- **Simplified Operations**: Single service deployment, easier to manage and monitor
- **Lower Resource Usage**: Significantly reduced memory and CPU footprint
- **GraphQL Focus**: Modern, flexible API interface for all operations

---

## Future Considerations

While not yet prioritized, we are also considering:

- Real-time WebSocket subscriptions for API clients
- GraphQL subscriptions for live updates
- Advanced query capabilities and indexing optimizations

---

## Contributing

If you're interested in contributing to any of these roadmap items, please see our [Contributing Guide](CONTRIBUTING.md) for details on how to get started.

