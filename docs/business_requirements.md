# Business Requirements

Business-level requirements for **FF Indexer v2**.

## Problem statement

Feral File’s products depend on **reliable, queryable truth** about digital art on chain: what exists, who holds it, how it changed over time, and whether it is **fit to display** in the home. Raw chain and fragmented vendor data are not enough for a calm, dependable **Gold Path** (from intent to playback). The indexer closes that gap by **ingesting, enriching, and serving** NFT state for Ethereum and Tezos so apps, devices, and partners can build consistent experiences without each re-implementing indexing, metadata resolution, and media handling.

## Primary users and consumers

- **Feral File apps and backends** — Personal **Collection** flows (wallet-address-driven stewardship data), playback-adjacent queries, and sync-style consumption of token state and events.
- **The Digital Art System** — Surfaces that need **viewability**, media, and provenance-aware behavior aligned with **DP-1** and product specs.
- **Partners and internal services** — Read/query APIs, selective **indexing and refresh triggers**, and **webhooks** for event-driven integrations.

## Core jobs to be done

1. **Index and refresh** — Track tokens across supported chains and standards; support **triggered** re-index and **metadata refresh** so displayed state can catch up with chain and vendor reality.
2. **Answer “what does the user steward?”** — Support **address-centric** indexing and job visibility so a person’s owned/stewarded works can populate **My Collection** semantics (stewardship is **legibility**, not access gating — art remains public by default in product strategy).
3. **Serve dependable read APIs** — Provide **token list and detail** views with **controlled expansions** (metadata, enrichment, display merge rules, media assets, provenance, owners) so clients avoid N+1 chaos and can size payloads.
4. **Expose change over time** — Provide **provenance** and **collection sync** style feeds so clients can incrementally align with acquisitions, releases, metadata/enrichment updates, and viewability changes.
5. **Integrate safely** — Offer **authenticated** operations where abuse or cost risk is higher (e.g. broad address indexing) and **documented webhook** registration and delivery with verifiable signatures.

## In-scope capabilities

Aligned with the current system description and architecture band **Ownership & Identity** (smart contracts, **token indexer**, passkey/login ecosystem; **address indexing for Collection claims**):

- **Multi-chain NFT indexing** — Ethereum (**ERC-721**, **ERC-1155**) and Tezos (**FA2**); chain identifiers consistent with CAIP-2-style usage in APIs.
- **Real-time or event-driven indexing** — Processing mints, transfers, burns, metadata updates as described in the README.
- **Metadata resolution** — From **IPFS, Arweave, ONCHFS, HTTP** as applicable.
- **Metadata enrichment** — From agreed vendor APIs (e.g. Art Blocks, fxhash, Foundation, SuperRare, Feral File, Objkt, OpenSea) as implemented in the service.
- **Media processing** — Integration with **Cloudflare Images and Stream** (and health-oriented follow-up such as sweeper behavior described in the README).
- **Provenance history** — Queryable event history supporting trust and UI timelines.
- **Dual API access** — **REST** and **GraphQL** exposing the same domain with documented equivalence.
- **Workflow-backed operations** — **Temporal**-tracked triggers and status for long-running work; **NATS JetStream** and workers as the execution substrate (per README architecture).
- **Webhooks** — Registered clients receive typed token events for integrations that need push instead of poll.

## Out-of-scope capabilities (for this component)

Unless explicitly added elsewhere in this repo’s specs:

- **Replacing DP-1** or the device **runtime** — The indexer feeds data; it is not the display protocol or FF OS.
- **Exhibition CMS or marketplace** — Those are separate bands (catalog, storefront); this service should expose stable data contracts, not absorb editorial commerce logic.
- **End-user identity UX** — Passkey onboarding and device pairing live in other systems; this service honors **auth schemes** for its own endpoints as documented.
- **Chains or standards not supported** by the deployed configuration — Adding them is a **product/engineering** decision with schema and ops impact.

## Success criteria

- **Reliability as a feature** — Meets bar implied by company principles: **reliability before novelty**; indexing and APIs should be predictable enough that apps can treat them as **infrastructure**, not a fragile experiment.
- **Correctness and clarity** — Token identity (**CID**), ownership, burn state, and **viewability** are consistent across list/detail paths and expansions; errors are **actionable** (stable codes, clear messages).
- **Support for Collection and sync UX** — Address indexing, job visibility, and **checkpointed collection sync** enable clients to build **incremental** personal library experiences without gaps, consistent with app/data patterns in reference specs.
- **Operational sustainability** — Stack choices in the README (PostgreSQL, Temporal, NATS, workers) support **scaling and recovery**; health and workflow surfaces support operators and integrators.
- **Trust alignment** — Fits **Ownership & Identity** expectations: verifiable stewardship data and integration hooks (webhooks, signatures) without weakening the **vendor-neutral, portable** trust posture described in architecture docs.

## Operational priorities

- **Production readiness** — Changes should preserve debuggability and explicit failure modes (no silent divergence between “indexed” and “shown”).
- **API contract hygiene** — OpenAPI and GraphQL stay aligned with shipped behavior so generated clients and partner integrations remain trustworthy.
- **Performance discipline** — Expensive counts and joins stay behind **optional** parameters or separate paths where the API already establishes that pattern.
- **Security proportionality** — **Authenticated** triggers for high-breadth operations; **API-key-only** paths where JWT would be the wrong trust model (e.g. webhook registration as documented).

## Known product and dependency risks

- **Third-party and vendor APIs** — Enrichment and media pipelines depend on external availability and terms; degradation should remain **observable** and **explainable** to downstream clients.
- **Chain and RPC quality** — Indexing completeness and latency depend on RPC providers and network conditions.
- **Public vs authenticated triggers** — Open trigger endpoints trade accessibility for abuse and cost risk; requirements may tighten as product and gates evolve.
- **Ecosystem evolution** — Strategy emphasizes **DP-1** and multi-surface playback; indexer schema and events may need **additive** evolution as Orbit-level trust and collection features mature (see trust architecture roadmap themes).
