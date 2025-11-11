# Roadmap

This document outlines the planned features and improvements for FF-Indexer v2. The items are ordered by priority and represent our vision for expanding the platform's capabilities.

## Metadata Enrichment from Additional Platforms

### Overview

Currently, FF-Indexer v2 supports metadata enrichment from Art Blocks, providing enhanced metadata including project information, artist details, and normalized fields. We plan to extend this enrichment capability to additional NFT platforms to provide comprehensive metadata coverage across the ecosystem.

### Planned Platforms

#### fxhash

fxhash is a generative art platform on Tezos that hosts a large collection of generative NFT projects. Enrichment from fxhash will provide:

- Project metadata and curation information
- Artist profiles and attribution
- Generative script information
- Platform-specific metadata fields

#### objkt

objkt is a major NFT marketplace on Tezos that aggregates tokens from various platforms. Enrichment from objkt will provide:

- Collection metadata
- Creator and artist information
- Platform-specific metadata fields

#### Feral File

Feral File is a curated Digital Art platform that hosts exhibitions and collections. Enrichment from Feral File will provide:

- Exhibition and collection context
- Curator information
- Artist profiles and statements
- Platform-specific metadata enhancements

### Technical Approach

The enrichment system follows a consistent pattern:

1. **Client Provider**: Each platform will have a dedicated client in `internal/providers/vendors/` that handles API communication
2. **Enhancement Logic**: Platform-specific enhancement functions in `internal/metadata/enhancer.go` that normalize vendor-specific data
3. **Storage**: Enriched metadata is stored in the `enrichment_sources` table with vendor-specific JSON for auditing
4. **Integration**: The enhancement is automatically triggered during the `IndexTokenMetadata` workflow when a matching publisher is detected

### Benefits

- **Comprehensive Coverage**: Support for major NFT platforms across Ethereum and Tezos
- **Data Quality**: Normalized and enriched metadata from authoritative sources
- **Audit Trail**: Raw vendor JSON stored for reprocessing and verification
- **Extensibility**: Easy to add new platforms following the established pattern

---

## Lighter Version for FF1 Deployment

### Overview

The current FF-Indexer v2 architecture is designed for full-scale production deployments with multiple services, Temporal workflows, NATS JetStream, and comprehensive media processing. However, for deployment on FF1 (resource limited), we need a lightweight version that can run efficiently with minimal resource requirements while maintaining core indexing functionality.

### Goals

Create a streamlined version of the indexer optimized for FF1 that:

- **Small Device Compatibility**: Designed to run efficiently on FF1 hardware with limited resources
- **Minimal Dependencies**: Replace heavy dependencies (Temporal, NATS) with lightweight alternatives
- **Self-Hosted Media**: Process and serve media files locally without external cloud services
- **GraphQL-First API**: Provide GraphQL as the primary and only API interface
- **Core Functionality**: Maintain essential indexing capabilities while removing resource-intensive features

### Proposed Architecture

The FF1 lightweight version will make significant architectural changes:

1. **Replace Temporal Workflows**: Use direct processing or lightweight in-memory task queues instead of Temporal orchestration
2. **Replace NATS JetStream**: Use direct database writes or lightweight message passing instead of NATS event streaming
3. **Remove Cloudflare Integration**: Eliminate dependency on Cloudflare Images and Stream services
4. **Local Media Processing**: Process media files (images, videos) locally and store them on the device's filesystem
5. **GraphQL-Only API**: Remove REST API endpoints, focusing solely on GraphQL for all queries and operations
6. **Unified Service**: Consolidate event emission, processing, and API into a single service to reduce overhead

### Features to Include

- **Core Indexing**: Token mint, transfer, and burn event processing
- **Basic Metadata Resolution**: IPFS, Arweave, and HTTP URI resolution
- **GraphQL API**: Complete GraphQL API for querying indexed tokens, metadata, and provenance
- **Local Media Storage**: Download, process, and serve media files from local filesystem
- **Database**: PostgreSQL (or SQLite for even lighter deployments) for data persistence
- **Direct Event Processing**: Blockchain event processing without intermediate message queues

### Features to Remove or Replace

- **Temporal Workflows**: Replaced with direct processing or lightweight task queues
- **NATS JetStream**: Replaced with direct database writes or in-memory queues
- **Cloudflare Services**: Removed entirely; media processed and served locally
- **REST API**: Removed; GraphQL is the only API interface
- **Advanced Enrichment**: Optional; can be disabled to reduce resource usage
- **Separate Worker Services**: Consolidated into a single unified service

### Implementation Strategy

1. **Lightweight Alternatives**: 
   - Replace Temporal with simple goroutine-based processing or lightweight job queues
   - Replace NATS with direct database writes or in-memory channels
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

