# Constraints

Hard and soft constraints for **FF Indexer v2**. Agents and engineers should treat these as guardrails when changing behavior, schema, or operations. Detailed API rules live in [`docs/api_design.md`](api_design.md); business intent in [`docs/business_requirements.md`](business_requirements.md).

## Product constraints

- **Domain boundary** — This service is **indexing and data delivery** for on-chain NFTs (and related metadata/media), not exhibition CMS, marketplace checkout, or device runtime (FF OS / DP-1 playback). It **feeds** apps and partners; it does not replace their UX or protocol responsibilities.
- **Supported networks and standards** — In scope: **Ethereum** (ERC-721, ERC-1155) and **Tezos** (FA2), at the chains enumerated in OpenAPI (e.g. mainnet and agreed testnets). Adding chains or standards requires **emitters**, **schema**, **API surface**, and **operational** updates together.
- **Stewardship vs access** — APIs expose **ownership and collection-oriented** data for product flows (e.g. “My Collection”). They must **not** be designed to imply that owning a token **gates** public discovery of the work; product strategy treats art as **public by default** with stewardship as **legibility**.
- **Truth model** — The indexer reflects **observed chain state**, **resolved metadata**, **vendor enrichment**, and **media health** as implemented. Clients should assume **eventual consistency**: triggers and workflows complete asynchronously; reads may lag writes briefly depending on load and pipeline stage.
- **Viewability** — **`viewable` / `is_viewable`** encode “fit for display” heuristics (metadata, enrichment, accessible media URLs, sweeper outcomes). It is a **product-oriented** flag, not a legal or on-chain guarantee.

## Data and consistency constraints

- **System of record** — **PostgreSQL** holds authoritative indexed state for tokens, balances, provenance, metadata, enrichment, media assets, jobs, and webhook configuration. **NATS JetStream** and **Temporal** carry **commands and workflow state**, not the long-term source of truth for token rows.
- **Canonical identity** — **`token_cid`** is the stable external identifier; **`(chain, contract_address, token_number)`** is unique in the database. Any API or client that bypasses CID rules risks collisions or rejected writes.
- **Migrations** — Schema changes go through **`db/migrations/`** and stay mirrored in **`db/init_pg_db.sql`** (and fixtures when tests depend on them). Do not leave drift between migration history and fresh installs.
- **JSONB and evolution** — Metadata and vendor payloads use **JSONB** for flexibility; **breaking** interpretation of stored JSON without migration or dual-read logic is out of bounds for production-safe releases.
- **Collection sync cursor** — **Checkpoint (`timestamp`, `event_id`)** semantics for collection sync are **stability constraints**: clients rely on **gap-free pagination** and ordering guarantees documented in OpenAPI; changing tuple logic without a versioned API is a breaking change.

## API and compatibility constraints

- **Contract sources** — REST: **`api/openapi/openapi.yaml`**. GraphQL: **`api/graphql/schema.graphql`**. Shipped behavior must match these; **`docs/api_design.md`** summarizes conventions (paths, errors, auth, pagination, expansions).
- **Dual surface** — **REST and GraphQL** should stay **semantically aligned** for the same operations. GraphQL field selection replaces REST `expand` but must not introduce contradictory field meaning.
- **Error envelope** — Use the shared **`APIError`** shape and **stable `code` values** for client-parseable failures; avoid silent HTTP 200 with error bodies for new endpoints.
- **Authentication policy** — **JWT (Bearer)** and **API key (`ApiKey`)** are the supported schemes where documented. **Webhook client creation** is **API-key-only** by design; do not broaden JWT there without an explicit product/security decision.
- **Pagination caps** — Respect documented **limits** (e.g. list `limit` max **255**, nested owner/provenance caps on list vs detail). Raising caps can have **DB and egress** impact and should be treated as a scale decision.
- **Breaking vs additive** — Prefer **additive** JSON fields and optional parameters. **Removing** or **renaming** public fields, tightening validation on existing parameters, or changing checkpoint or webhook signing rules are **compatibility events** and need explicit versioning or migration notes.

## Performance and scale constraints

- **Expensive reads** — Optional aggregates (e.g. **total token counts** on indexing jobs) stay **off by default** where the API already uses flags. New heavy queries should follow the same pattern.
- **Expansion cost** — **`expand`** / nested GraphQL fields can multiply work (metadata, enrichment, **media_assets**, provenance). Defaults should favor **safe** response sizes; clients that need large graphs must paginate at the **single-token** level where supported.
- **Batch triggers** — OpenAPI documents **maximum batch sizes** for trigger endpoints; enforcement protects workers and vendor rate limits. Do not raise limits without assessing **Temporal concurrency**, **RPC**, and **downstream quotas**.
- **Offset pagination** — List endpoints use **limit/offset**. Very deep offsets can be expensive on PostgreSQL; **cursor-style** alternatives (like collection sync) exist where offset is unsuitable.

## Operational constraints

- **Critical dependencies** — A functioning indexing pipeline requires **PostgreSQL**, **Temporal**, **NATS JetStream**, **Redis** (per current stack), **Ethereum (and Tezos) RPC** (and Tezos indexer connectivity as deployed), and **running workers** (core, media, emitters, bridge, sweeper as applicable). Missing any of these degrades specific paths predictably rather than “half working” without visibility.
- **Workflow observability** — Long-running work is **Temporal workflow–backed**; operators and integrators use **workflow and job APIs** for status. New async features should expose **trackable IDs** and documented polling patterns.
- **Configuration** — Runtime config uses **YAML plus `FF_INDEXER_*` environment overrides** as documented in **DEVELOPMENT.md**. Secrets (RPC URLs, API keys, Cloudflare, webhook signing) must not be committed.
- **Blacklist / abuse** — Indexing requests are subject to **blacklist validation** (see architecture). Changes to filtering rules affect **fair use and cost**; document operator impact when adjusting.
- **Health scope** — **`GET /health`** is **liveness** for the API process. Deep dependency checks belong behind an explicit **readiness** contract if introduced, not silently folded into `/health` without agreement.

## Security and compliance constraints

- **Transport** — Production **webhook URLs** are expected to be **HTTPS** (per OpenAPI validation). Webhook **HMAC** signatures must remain **verifiable** with documented header and payload canonicalization.
- **Least privilege** — Prefer **narrow API keys** and documented scopes as the auth model evolves; **address indexing** stays **authenticated** to reduce anonymous abuse of expensive sweeps.
- **No silent trust** — Do not treat indexer output as **cryptographic proof** of ownership for high-risk actions in other systems without independent verification where product/trust architecture requires it.
- **PII** — The system handles **wallet addresses** and **public on-chain / metadata** content. Treat logs and exports as **sensitive operational data**; avoid logging full vendor payloads or secrets at default levels.

## Deployment constraints

- **Runtime versions** — Target versions in **README** (Go, PostgreSQL, Temporal, Redis, NATS) are **supported baselines** for development and CI. Upgrades should be validated with **`make check`** and integration tests.
- **Single-process application** — The default deployment runs **`ff-indexer`** as one OS process (API, emitters, bridge, Temporal workers, sweeper). An “API only” build **does not index** unless emitters, bridge, and workers are also running (e.g., full binary or external workers).
- **Cloudflare** — **Worker media** integration assumes **Cloudflare Images/Stream** (or configured equivalents) for processed media URLs; swapping providers is a **cross-cutting** change (workers, config, URL shapes).
- **Observability** — Production operation expects **logs and metrics** suitable for diagnosing **stuck workflows**, **NATS backlog**, and **RPC errors**. New components should emit **actionable** errors rather than swallowing failures.

## Non-goals

- **Sole authority for rights or licensing** — The indexer does not replace **legal** or **DP-1** enforcement on devices; it supplies **state and signals** (e.g. viewability, provenance).
- **Guaranteed completeness of third-party data** — Vendor APIs, gateways (IPFS/Arweave/ONCHFS), and RPC providers can **fail or rate-limit**; the system prioritizes **clear degradation** over fake completeness.
- **Unbounded synchronous indexing in the API** — Trigger endpoints **accept** work and return **202** (or equivalent) with workflow identifiers; they do not block until the chain is fully scanned.
- **Per-tenant arbitrary SQL or unbounded ad hoc analytics** — The API is **curated query surfaces**; arbitrary query languages or report-style dumps are out of scope unless added as a first-class, documented product.
