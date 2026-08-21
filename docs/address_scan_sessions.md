# Ethereum owner-scan sessions (checkpointed discovery)

Design note for the checkpointed, window-major Ethereum owner scan. Companion to
`docs/indexing_flows.md` §3 (owner sweep) and `docs/ethereum_contract_adapters.md`
§6.3 (merged owner-scan queries).

## 1. Problem

Discovering every token an address holds requires walking the chain's owner-scoped
logs. On a span-capped provider (Infura: `eth_getLogs` limited to a 10k-block span),
a full-history mainnet scan is ~2,500 calls per merged query — ~7,500 calls total
after query merging. The pre-session implementation ran that walk as one atomic
in-memory operation, which failed two ways:

- **Any mid-scan failure discarded everything.** One RPC error after retries, a pod
  restart, or a deploy threw away thousands of already-paid calls; the next attempt
  restarted from block zero.
- **Daily-quota pauses forced full re-scans.** Budgeted mode indexed `quota` tokens
  per day, but the discovered token list was never persisted — only a block-range
  watermark advanced. Every quota resume re-scanned the entire un-indexed remainder,
  so an address holding 10× the daily quota paid ~10 nearly-full scans.

## 2. Design

Invert the loop from query-major (each query walks the whole range; atomicity = the
whole scan) to **window-major**: one cursor walks the range in provider-cap-sized
windows, each window fetches the three merged owner queries, and progress commits
to Postgres per window. The unit of loss on any failure is bounded by the
fetch-ahead window — at most `scan_window_concurrency` fetched-but-uncommitted
windows (× 3 calls each; one window at concurrency 1) — not the scan. What matters
is that the bound is small and operator-configurable, not that it is exactly one.

### Session lifecycle

One session per `(chain, address)` (enforced by a unique constraint), moving through
two statuses and then deletion:

1. **`scanning`** — the window loop. Per window: fetch merged owner logs
   (`FetchOwnerLogsWindow`), then in ONE transaction insert the raw logs into
   `address_scan_logs` (idempotent on the log identity PK) and advance
   `cursor_block`. Resume after any interruption continues from `cursor_block`.
2. Replay — when the cursor passes `to_block`, load all staged logs, run the
   receipt repairs and the unified ownership replay (`DiscoverOwnedTokensFromLogs`,
   full discovery — no per-day limit), and in ONE transaction persist the derived
   token list to `address_scan_tokens`, **delete the staged logs** (pure
   intermediate state, re-derivable, and the bulkiest rows), and set status
   **`replayed`**.
3. **`replayed`** — indexing consumes the persisted token list in block-aligned
   chunks under the existing daily-quota machinery, marking each chunk's tokens
   `indexed_at` after success. A quota pause reschedules the job; resume continues
   from the un-indexed tokens with **zero further RPC**.
4. Completion — when no un-indexed tokens remain, the scanned range merges into the
   address's block-range watermark (`watched_addresses.last_successful_indexing_blk_range`)
   and the session row is **deleted** (cascade removes its token rows).

The watermark only advances at completion: it now strictly means "fully indexed
range", instead of being juggled per chunk mid-scan.

### Range selection

`IndexEthereumTokenOwner` loops: resume the active session if one exists; otherwise
derive the next range from the watermark — the backward gap
`[sweep_start, stored_min-1]` first, then the forward gap `[stored_max+1, latest]` —
and create a session for it. No gaps → done. First run is simply the backward case
with an empty watermark: `[sweep_start, latest]`.

### Window size

`ethereum.getlogs_span_cap + 1` blocks when the cap is configured (every window is
exactly one accepted call per query). When no cap is configured (self-hosted node),
a 1M-block window keeps checkpoints frequent while the pagination helper's adaptive
halving handles any too-many-results rejections inside the window.

### Window concurrency

The scan is purely RPC-latency-bound: each window is one provider round-trip
(~0.9s measured against Infura) and windows are independent of each other, so a
sequential loop spends ~2,000 round-trips back to back (~32 minutes for a mainnet
history). `ethereum.scan_window_concurrency` (default 2) fetches that many windows
at once, dividing wall-clock by roughly that factor at **identical total credit
cost** — only the request rate rises.

Sizing it is deliberately an **operations decision per RPC vendor**, not a binary
default: credit-metered, flat-rate, and self-hosted providers all want different
values, and the indexer does not encode any vendor's limits. Reason from the full
fan-out — every window issues the **three** merged owner-topic queries at once and
every token worker may run a scan, so
`peak concurrent eth_getLogs = token_worker.concurrency × scan_window_concurrency × 3`
(30 at binary defaults; a 30-worker deployment at 4 is 360). Throttling (429) is
retried with backoff and the checkpoint resumes the walk, so over-sizing degrades
to slower rather than broken — but sustained 429s exhaust the per-call retry
budget, so size from the vendor's real limit rather than upward from symptoms.

Persistence stays strictly sequential. The cursor is a contiguous-prefix marker, so
a reorder buffer holds windows that finish early until every earlier window has
committed: persisting window N+1 before window N would let a crash between the two
leave a gap that resume silently skips. Fetching has no side effects, so on any
failure the group context cancels in-flight fetches and fetched-but-uncommitted
windows are simply dropped and re-fetched on resume.

### Reorg safety: the scan head lags the chain head

A session's `to_block` is `latest − ethereum.scan_head_lag_blocks` (default 64,
two PoS epochs ≈ the Ethereum finality horizon, ~13 min), never `latest` itself.

The checkpoint design makes this necessary rather than optional. Staged logs are
replayed later and the watermark then marks the whole scanned range as done. If
that range reached into blocks still subject to reorg, a reorged block would do two
bad things at once: its logs would replay as real ownership events, and the
watermark would record its canonical replacement as already scanned — permanently
skipping it. Stopping short of head so that **no checkpointed block can reorg**
removes the failure mode outright, which is cheaper and more robust than validating
persisted block hashes on resume and rewinding the affected suffix.

The margin is vendor-neutral by design: it uses only block numbers, not
provider-specific `safe`/`finalized` tags, so it behaves identically against any
RPC vendor or a self-hosted node. `0` disables it (pre-checkpoint behavior; not
recommended). `12` (one epoch, "safe") is a defensible lower setting when freshness
matters more than reorg safety.

### Consistency semantics

The persisted token list is a snapshot as of `to_block`, which is the scan head
above — so a token acquired inside the lag window surfaces on the **next** forward
sweep, a delay of at most the lag (~13 min at the default). Otherwise these are the
semantics the watermark always had. Blacklist filtering happens at replay time; a
token blacklisted after replay is still skipped by the per-token indexing path.

## 3. Schema

Three tables (migration `027.sql`, mirrored in `db/init_pg_db.sql`):

- `address_scan_sessions` — `(chain, address)` unique; `from_block`, `to_block`,
  `cursor_block` (next un-fetched block), `status` (`scanning` | `replayed`).
- `address_scan_logs` — staged raw logs; PK `(session_id, block_number, tx_hash,
  log_index)` makes window re-fetch after a crash idempotent
  (`ON CONFLICT DO NOTHING`); `ON DELETE CASCADE` from sessions.
- `address_scan_tokens` — the durable discovery result; PK
  `(session_id, token_cid)`; `indexed_at` NULL = pending; partial index on pending
  rows for the resume query.

Sizing: queries are owner-scoped, so even a heavy collector stages thousands to
tens of thousands of log rows (megabytes) — deleted at replay. Token rows are
smaller still and live only until the session completes.

### Stale sessions

There is deliberately no background janitor (simplicity at ~3k users). A session
leaks only if its address is never indexed again, and its cost is bounded: logs are
deleted at replay, so a stuck `scanning` session holds at most one scan's staged
logs, and a stuck `replayed` session holds only token rows. Re-triggering the
address resumes and eventually completes the session. Revisit if the
watched-address population grows orders of magnitude.

## 4. What changed at each layer

- **Ethereum client**: `GetTokenCIDsByOwnerAndBlockRange` (fetch+replay+limit in
  one call) is replaced by `FetchOwnerLogsWindow` (merged-query fetch for one
  window) and `DiscoverOwnedTokensFromLogs` (repairs + dedupe + full replay).
  The per-day `limit` / effective-range truncation machinery is gone: discovery is
  always full; the daily quota paces only indexing.
- **Store**: session/log/token CRUD with the two transactional composites
  (`AppendScanLogsAdvanceCursor`, `FinishScanReplay`).
- **Workflow**: `IndexEthereumTokenOwner` drives the session loop described above.
  Tezos owner indexing is unchanged.
