# API Design Rules

Rules for designing and evolving the FF Indexer v2 API surface. **Normative contracts** live in `api/openapi/openapi.yaml` (REST) and `api/graphql/schema.graphql` (GraphQL). This document captures conventions, compatibility expectations, and practices inferred from those sources.

## API surface overview

- **REST:** OpenAPI **3.1** under the **`/api/v1`** prefix, plus **`GET /health`** for process liveness.
- **GraphQL:** Serves the same domain; schema comments call out **REST equivalents** for each operation. Prefer keeping REST and GraphQL behavior aligned when adding features.
- **Documentation:** Extend OpenAPI paths, components, tags, and examples when adding REST endpoints; extend the GraphQL schema with equivalent comments and types.

### Resource groups (OpenAPI tags)

| Tag | Role |
| --- | --- |
| `tokens` | Token reads, expansions, and indexing triggers |
| `collections` | Owner collection sync (checkpointed event stream) |
| `workflows` | Async workflow / job correlation (e.g. `workflow_id` from API triggers; backed by `jobs` where applicable) |
| `indexing` | Address indexing job status |
| `webhooks` | Webhook client registration (outbound delivery is documented under `webhooks` in OpenAPI) |
| `health` | Service health |

## Naming and resource conventions

- **Paths:** Plural nouns and clear sub-resources (e.g. `/api/v1/tokens`, `/api/v1/tokens/{cid}`, `/api/v1/collection/{address}/sync`).
- **JSON property names:** **`snake_case`** (e.g. `token_cid`, `workflow_id`, `created_at`). GraphQL field names follow the same convention to stay aligned with REST payloads.
- **Identifiers:**
  - **Token CID:** Canonical string `{chain}:{standard}:{contract_address}:{token_number}` (see OpenAPI patterns and descriptions). Document any new chain or standard in both specs.
  - **Chains:** Use **CAIP-2** strings (e.g. `eip155:1`, `tezos:mainnet`) as enumerated where possible; keep OpenAPI `Chain` enum and GraphQL `String` filters in sync conceptually.
- **Query parameter names:** Use **`snake_case`**. For nested options on a single resource, use **dot-separated** names (e.g. `owners.limit`, `provenance_events.order`) rather than inventing new nesting styles.

## HTTP semantics and status codes

- **Success:** Use the narrowest success code: **`200`** (read), **`201`** (create), **`202`** (accepted async work such as indexing triggers).
- **Client errors:**
  - **`400`** — Malformed identifiers, bad checkpoints, or invalid path/query semantics.
  - **`401`** — Missing or invalid authentication (where required).
  - **`404`** — Missing resource (token, workflow, job) when applicable.
  - **`422`** — Valid transport but failed validation (body/query rules, business validation); use the standard error envelope.
- **Server errors:** **`500`** for unexpected failures; prefer stable `code` values over free-form prose for clients.

Reserve **`403`** / **`forbidden`** in the error `code` enum for future use if authorization expands beyond today’s “authenticated or not” model.

## Error semantics

All error responses that follow the shared contract should use the **`APIError`** shape:

- **`code`** (required) — Machine-oriented enum: `bad_request`, `not_found`, `validation_failed`, `unauthorized`, `forbidden`, `internal_error`, `database_error`, `service_error`.
- **`message`** (required) — Human-readable summary.
- **`details`** (optional) — Extra context (e.g. which field failed).

Rules:

- Prefer **stable `code` values** and actionable **`message`** text; put specifics in **`details`** when it helps operators without breaking clients that key off `code`.
- Document **representative examples** in OpenAPI for non-obvious validation (as with webhook and metadata trigger endpoints).

## Authentication

Two schemes are supported (see `components.securitySchemes` in OpenAPI):

1. **JWT:** `Authorization: Bearer <token>` — RSA algorithms (**RS256 / RS384 / RS512**). Used where OpenAPI attaches `BearerAuth`.
2. **API key:** `Authorization: ApiKey <key>` — Used where OpenAPI attaches `ApiKeyAuth`.

Design rules:

- Declare **`security`** on every route that requires auth; do not rely on prose alone.
- **Least surprise:** **`POST /api/v1/webhooks/clients` accepts API key only** (not JWT). If a new endpoint requires a single scheme, state it explicitly in the description and security list.
- **GraphQL:** Mirror REST policy — authenticated mutations must match REST (e.g. address indexing requires the same credentials as `POST /api/v1/tokens/addresses/index`).

## Pagination, filtering, and sorting

### List tokens (`GET /api/v1/tokens`)

- **Pagination:** `limit` (default **20**, max **255**), `offset` (default **0**).
- **Filters:** Repeatable query parameters; **AND** across different filter types, **OR** within the same parameter. Document new filters with the same semantics.
  - **`release_id`:** Filter tokens to members of a specific release (internal integer id). When combined with `sort_by=mint_number`, results are ordered by their authoritative mint position within the release.
  - **`mint_from` / `mint_to`:** 1-based mint number range filter (inclusive). Both parameters require `release_id` to be set; supplying either without `release_id` returns a **`422`** validation error. `mint_from` must be `≥ 1`; `mint_to` must be `≥ mint_from`. Omitting both returns all members. The primary use case is polling `IndexRelease` job progress: page through `GET /api/v1/tokens?release_id=X&mint_from=1&mint_to=N&include_unviewable=true` with offset-based pagination and accumulate the count until a partial page is returned — when the accumulated count equals `mint_to - mint_from + 1` (minus any known-skipped tokens), all child jobs have completed. `TriggerIndexingResponse` returns only a `job_id`; the expected count must be computed by the caller from the requested mint range. (`paging.total` is deprecated and always returns 0; do not rely on it for termination.)
- **Sorting:** `sort_by` (`created_at` | `latest_provenance` | `mint_number`), `sort_order` (`asc` | `desc`). When `owner` is present, `latest_provenance` follows documented owner-scoped behavior (see OpenAPI). `mint_number` **requires** `release_id` to be set; the API returns a validation error if `mint_number` is requested without a `release_id` filter.
- **Invalid sort parameters (behavior change, release abstraction / #93):** Unrecognized `sort_by` or `sort_order` values return **`422`** with a validation error. Previously the list endpoint silently rewrote invalid values to `latest_provenance` / `desc`; that masking was removed so validation matches the OpenAPI enum contract and the release member endpoint (which already rejected invalid `sort_order`). Clients that sent typos and depended on silent defaults must send valid enum values.
- **`include_unviewable`:** Default **`false`**; changing defaults is a **compatibility** decision.
- **GraphQL:** `tokens(release_id, mint_from, mint_to, limit, offset, ...)` exposes the same mint range filter; the same validation rules apply.

### List releases (`GET /api/v1/releases`)

- Returns a paginated list of release metadata rows filtered by **`ids`**, **`vendor`**, **`vendor_release_id`**, and/or **`vendor_release_slug`**. At least one filter is required; when multiple are provided they are combined with AND semantics.
- **`ids`:** One or more internal release IDs (positive integers). Repeat the parameter for multiple values: `?ids=1&ids=2`. All IDs must be positive; zero is rejected.
- **`vendor`:** `artblocks`, `feralfile`, `fxhash`, or `objkt` (all vendors that populate the `releases` table).
- **`vendor_release_id`:** External release key as stored during indexing (FF series UUID, AB `{chainID}-{contract}-{projectID}`, fxhash generative token numeric id, or objkt custom-collection KT1 contract address). May be used without `vendor`; results can span vendors in theory but are typically 0–1 rows.
- **`vendor_release_slug`:** Human-readable URL slug from the vendor's website (e.g. `industrial-park` for fxhash, `fidenza-by-tyler-hobbs` for Art Blocks, `data-pilgrims-01-769` for Feral File; for objkt this equals the KT1 contract address). Nullable; only rows where `vendor_release_slug` is non-null and matches are returned.
- **Response:** `items` array of release metadata (`id`, `vendor`, `vendor_release_id`, optional `vendor_release_slug`, optional `name`, optional `total_mints`). **No `members`** — use `GET /api/v1/releases/{id}` or `GET /api/v1/tokens?release_id=...` for member tokens.
- **Pagination:** `limit` (default **20**, max **255**), `offset` (default **0**); empty match set returns **`200`** with `"items": []`.
- **GraphQL:** `releases(ids, vendor, vendor_release_id, vendor_release_slug, limit, offset)` exposes the same contract via `ReleaseList`.

### Release endpoint (`GET /api/v1/releases/{id}`)

- Returns a release by internal id (integer) with its complete, mint-ordered member token list.
- **Release metadata:** Optional read-only `name` (human-readable title, e.g. `"Fidenza"`), `total_mints` (declared max edition size from vendor: AB `max_invocations`, FF `series.settings.maxArtwork`), and `vendor_release_slug` (URL slug from the vendor's website, populated during enrichment). All are nullable when vendor data is unavailable; `vendor_release_slug` is also null for older rows not yet re-enriched.
- **Membership completeness:** All members are returned regardless of `is_viewable` state so the list is stable across viewability changes (tokens may be temporarily unviewable during media processing). Callers needing only publicly visible members should use `GET /api/v1/tokens?release_id=...` with the default `include_unviewable=false`.
- **Pagination:** `limit` / `offset` on member list; `sort_order` (`asc` | `desc`). Sort is always by `mint_number` (not configurable here).
- **GraphQL:** `release(id)` query exposes the same release with a `members` field that follows the same membership-completeness contract.

### Single token (`GET /api/v1/tokens/{cid}`)

- **Nested pagination** for `owners` and `provenance_events` via `owners.limit` / `owners.offset` / `provenance_events.*` (defaults and maxima per OpenAPI).
- **List vs detail:** List responses cap embedded owners/provenance rows per token; **full pagination belongs on the single-token endpoint** (and must stay documented).

### Collection sync (`GET /api/v1/collection/{address}/sync`)

- **Cursor:** Checkpoint **`timestamp` + `event_id`** (pair them in API design; document RFC3339 for timestamps).
- **Paging:** `limit` optional (uint8-style bound in spec); **`next_checkpoint`** and **`server_time`** semantics must stay stable for clients doing incremental sync.

### GraphQL alignment

- Expose the same limits and defaults where types allow (`Uint8` for small limits, etc.).
- **`TokenList.total` deprecation:** Prefer documenting pagination continuation in terms of **`offset`** and payload shape rather than reintroducing reliance on deprecated fields for new clients.

## Expansions and field selection

### REST `expand`

- Comma-separated list (OpenAPI `style: form`, `explode: false` for array serialization).
- Allowed values are **closed enums** per endpoint; adding a value is a **minor** API change if optional.
- **`media_asset` / `media_assets`:** Behavior is **compositional** — it depends on which source expansions are requested (`metadata`, `enrichment_source`, `display`). New combinations must be documented to avoid surprising empty results.

### GraphQL

- Expansions are **inferred from the selection set**; keep parity with REST expansions and document any intentional differences in schema comments.

### Release indexing (`POST /api/v1/releases/index`)

Triggers asynchronous, full-collection indexing for a vendor release within a 1-based mint number range. The response arrives immediately (HTTP **202**) with a `job_id`; the actual token indexing runs in the background via the jobs queue.

**Request body:**

| Field | Type | Required | Notes |
| --- | --- | --- | --- |
| `vendor` | string | yes | `artblocks`, `feralfile`, `fxhash`, or `objkt` |
| `vendor_release_id` | string | one of | Vendor-specific release key (see list releases docs for format per vendor). Mutually exclusive with `vendor_release_slug`. |
| `vendor_release_slug` | string | one of | Human-readable URL slug from the vendor's website (e.g. `industrial-park`). The workflow resolves the slug to a canonical `vendor_release_id` via the vendor's API before CID derivation. Mutually exclusive with `vendor_release_id`. For `objkt`, slug equals the KT1 contract address (no resolution API call needed). |
| `mint_from` | int64 | no | First mint number to index (1-based, inclusive). Defaults to **1**. |
| `mint_to` | int64 | yes | Last mint number to index (1-based, inclusive). |

**Validation:**
- Exactly one of `vendor_release_id` or `vendor_release_slug` must be supplied.
- `mint_from ≥ 1` (when supplied)
- `mint_to ≥ mint_from`
- `mint_to − mint_from + 1 ≤ 100` (max range per request; see `MAX_RELEASE_MINT_RANGE`). Clients must batch larger collections themselves by issuing multiple non-overlapping requests.

**Response (`202`):** `TriggerIndexingResponse` with `job_id`. Use `GET /api/v1/jobs/{job_id}` to check whether Phase 1 (CID derivation + child-job enqueueing) succeeded. After the `job_id` reaches a terminal state, track per-token completion via `GET /api/v1/tokens?release_id=X&mint_from=1&mint_to=N&include_unviewable=true` with offset-based pagination (see List tokens documentation above).

**Per-vendor CID derivation strategy:**
- `artblocks` — fully deterministic; zero API calls; `vendor_release_id` is `"{chainID}-{contract}-{projectID}"`; slug resolution calls the AB GraphQL API once.
- `objkt` — fully deterministic; zero API calls; `vendor_release_id` is the KT1 FA2 contract address; slug equals the KT1 address (no extra call).
- `fxhash` — requires an internal API call to map iteration numbers to on-chain token IDs; slug resolution calls the fxhash GraphQL API once.
- `feralfile` — requires an internal API call; artworks still on the Bitmark chain (not yet swapped to EVM/Tezos) are skipped and counted as `tokens_skipped`; slug resolution calls the FF series list API once.

**Auth:** Open (no API key required), same as `POST /api/v1/tokens/index`. Max range cap of 100 tokens per call prevents large queue floods; clients batch larger collections themselves.

**GraphQL:** `triggerReleaseIndexing(vendor, vendorReleaseId, vendorReleaseSlug, mintFrom, mintTo)` — same behavior; exactly one of `vendorReleaseId` / `vendorReleaseSlug` must be supplied.

## Async operations and job tracking

- **Token and metadata trigger responses** return **`job_id`** plus **deprecated** **`workflow_id`** (decimal string of `job_id`) and **`run_id`** (always null) on `TriggerIndexingResult` / OpenAPI `TriggerIndexingResponse`; new clients should use **`job_id`** with `jobStatus` / `GET /api/v1/jobs/{job_id}`.
- **Address indexing** returns **per-address jobs** with **`job_id`** and **deprecated** **`workflow_id`** (stored opaque correlation id: for new queue-backed rows this is typically the decimal string of `job_id`; pre-migration rows may retain a legacy Temporal-style string). New integrations should use **`job_id`**. Deprecated **`workflow_id`** is persisted and echoed in the API for backward compatibility.
- **Status endpoints:** `GET /api/v1/jobs/{job_id}` (queue job lifecycle), GraphQL `jobStatus(job_id)`, **deprecated** GraphQL `workflowStatus(workflow_id, run_id)` (same as `jobStatus` when `workflow_id` is the decimal string of `jobs.id`; optional `run_id` is ignored), and `GET /api/v1/indexing/jobs/{job_id}` (address indexing detail: progress, blocks, optional counts) are the supported ways to poll progress. Legacy REST `GET /api/v1/workflows/{workflow_id}` behaves like `GET /api/v1/jobs/{job_id}` (jobs table only). GraphQL `indexingJob` may still resolve by stored address-indexing `workflow_id` when not using `job_id`. Address indexing GET/trigger responses mirror **`job_id`** plus deprecated **`workflow_id`** as above.
- **Optional expensive fields:** Use explicit query flags (e.g. `include_total_indexed`) for costly aggregates; defaults should favor low latency.

## Webhooks

- **Registration:** Document HTTPS requirements, filter rules, retry bounds, and **generated secrets** in OpenAPI.
- **Delivery:** Document headers (`X-Webhook-Signature`, `X-Webhook-Event-ID`, etc.), signature string format, and **2xx** acknowledgment expectations. Changes to signing or headers are **breaking** for integrators.

## Compatibility expectations

- **Prefix:** New externally visible REST routes belong under **`/api/v1/`** unless you introduce a new major version (`/api/v2/`) with a migration story.
- **Additive changes:** New optional query fields, new enum values for optional expansions, and new read-only JSON properties are preferred over breaking renames.
- **Breaking changes:** Removing or renaming paths, changing required fields, changing error codes for the same condition, or tightening validation without a version bump requires explicit stakeholder sign-off and release notes.
- **Deprecation:** Use GraphQL `@deprecated` with **reason**; in REST, prefer `deprecated: true` in OpenAPI where tooling supports it, and keep old behavior until a published removal window ends.

## GraphQL schema evolution

- **Mirror REST** for new operations (document the equivalent path in comments).
- **Scalars** (`Time`, `JSON`, `Uint64`, `Uint8`) encode constraints that differ from JSON number rules; document overflow or precision limits for clients.
- **Nullability:** Prefer nullable arguments only when “omit” vs “null” must differ; otherwise keep defaults explicit as in the current schema.

## Observability and operator-facing requirements

- **Health:** `GET /health` remains a simple **liveness** JSON payload (`status`, `service`); avoid overloading it with deep dependency checks without a separate readiness contract.
- **OpenAPI** should remain accurate enough to generate clients and **AI/tooling** surfaces (see org guidance on discoverable OpenAPI in reference docs).
- **Descriptions** in OpenAPI and schema comments are the right place for **semantic** guarantees (ordering, filter logic, expansion composition), not just repeating parameter names.

## Checklist for new endpoints

1. Add or update **`api/openapi/openapi.yaml`** (path, operationId, tags, parameters, schemas, responses, security, examples).
2. Add or update **`api/graphql/schema.graphql`** with equivalent operations and REST equivalence comments where applicable.
3. Align **error model**, **auth**, **pagination/filter** semantics, and **naming** with this document.
4. Regenerate or update any derived artifacts per `AGENTS.md` (e.g. gqlgen after GraphQL changes).
