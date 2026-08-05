# Spam Token Filtering

Unsolicited airdrop scams ("Visit `<domain>` to claim rewards" phishing lures) land in
every real wallet and are then rendered full-size by display surfaces (ff-app, FF1
walls). Vendors already moderate these — OpenSea exposes `is_disabled` on its NFT API,
objkt exposes `flag` (`banned`) — and this design records those verdicts per token,
filters flagged tokens out of read paths by default, and keeps the verdicts fresh.

## Two-tier model

```
enricher ─────────┐  (first verdict at indexing time)
spam sweeper ─────┼──► token_spam_verdicts ──► recompute ──► tokens.is_spam ──► read paths
future FF system ─┘  (one row per token+source)  (one tx)     (materialized)     (filter is_spam=false)
```

1. **`token_spam_verdicts`** is the source of truth: one row per (token, source),
   where a source is a moderating vendor (`opensea`, `objkt`) or Feral File's own
   moderation system (`feralfile`). Sources never overwrite each other — OpenSea
   clearing its flag cannot clear a Feral File pin.
2. **`tokens.is_spam`** is the materialized combination, recomputed transactionally on
   every verdict write (`UpsertTokenSpamVerdict`). Read paths filter on this single
   column and never join the verdicts table; `include_spam=true` opts back in.

**Precedence rule**: a `feralfile` row wins outright in both directions — `true` pins
spam against vendor reversals, `false` whitelists against vendor flags. Otherwise the
combined verdict is the OR of vendor verdicts. Token-level recompute reads *all* rows
for the token inside the same transaction, serialized by a `FOR UPDATE` lock on the
tokens row (uniform lock ordering across writers; prevents two concurrent writers
from materializing values computed from row sets missing each other's write).

**Tri-state everywhere**: absence of a row means "no opinion", which is deliberately
distinct from a clean verdict. Vendors without a moderation signal (ArtBlocks, fxhash,
Foundation, SuperRare, the feralfile *enrichment* vendor) never create rows
(`schema.SpamSourceForVendor` returns ok=false). A vendor API error is not a verdict
either: the failure path (`RecordTokenSpamCheckFailure`) advances only scheduling
state, never `verdict` or `last_checked_at`.

**Tag-not-drop**: unlike the write-time contract blacklist (which drops events so
blacklisted tokens are never stored and cannot be un-hidden), flagged tokens stay
fully indexed — the verdict is reversible and clients are notified of flips via
broadcast `spam_status_changed` token events (payload carries `token_cid` because
sync clients cannot resolve ids through the very query that filters spam out).

**Fail-open**: no signal means the token stays visible. Hiding a user's real asset by
mistake is worse than letting spam through until the next sweep.

## Writers

| Writer | Source | When |
|--------|--------|------|
| Enricher (`EnhanceTokenMetadata`) | `opensea` / `objkt` | Every enrichment where the vendor publishes a moderation signal; schedules the first sweep at now + `store.DefaultSpamRecheckInterval` (fresh signal → fresh schedule) |
| Spam verdict sweeper (`internal/sweeper/spam_verdict.go`) | `opensea` / `objkt` | Rows due per `next_check_at`, per-source queues |
| Future FF moderation system | `feralfile` | **Reserved — no writer exists yet.** User reports, operator decisions, or any FF-side pipeline slots in as just another `UpsertTokenSpamVerdict` caller with no schema change. `feralfile` rows carry `next_check_at = NULL` so the sweeper never touches them |

## Sweeper scheduling

The enricher only sees a vendor's verdict at indexing time — which is exactly when
moderation has usually not happened yet (takedowns land hours to days after an
airdrop) — and appealed takedowns get reversed. The sweeper re-asks vendors so both
late flags and reversals converge:

- **Clean token**: previous interval (derived as `next_check_at − last_checked_at`
  from the row itself, no interval column) doubled, clamped to
  [`initial_recheck_interval` (24h), `max_recheck_interval` (720h)].
- **Clean token with prior failures**: restarts at `initial_recheck_interval`. The
  derivation above is only meaningful when `next_check_at` was last set by a
  *successful* check — the failure path advances it while freezing
  `last_checked_at`, so after an outage the difference measures the failure backoff.
  Doubling that would demote a clean token to the 30-day cadence because a vendor
  had a bad hour.
- **Flagged token**: fixed `max_recheck_interval` — appeals are rare, poll slowly.
- **Check failure**: `failure_backoff_initial` (1h) doubled per consecutive failure;
  once `max_consecutive_failures` (5) is reached the row pins at
  `max_recheck_interval` — permanently missing tokens stop burning API quota but
  never leave the queue for good.
- **OpenSea `ErrNoAPIKey`**: source-wide condition, not a per-row failure — rows are
  left untouched (they stay due until a key is configured) and the source reports
  itself idle for the cycle. That last part matters: untouched rows stay due, so
  counting them as work would keep the cycle from sleeping, and `ErrNoAPIKey` is
  returned before the HTTP request and the rate limiter, leaving nothing to throttle
  the respin.

Per-source queues (`GetTokenSpamVerdictsDueForCheck`) with a partial index
`(source, next_check_at) WHERE next_check_at IS NOT NULL` keep one vendor's API quota
from starving another's. The sweeper's vendor clients share the process-wide rate
limiter with the enricher, so the per-provider budget holds across subsystems.

Config: `spam_sweeper.*` (see `config/.env`); `initial_recheck_interval` must stay in
step with `store.DefaultSpamRecheckInterval`.

## Coverage policy (accepted gaps)

- Verdict rows exist only after a **first successful vendor signal**. Tokens routed
  to no-signal vendors are intentionally never swept: those vendors are curated
  surfaces, and burning OpenSea quota on ArtBlocks mints buys nothing.
- A token whose first enrichment errored has no row and stays outside the sweep
  queue until something re-triggers its metadata indexing. Accepted for now; the
  alternative (seeding rows at token creation) costs quota proportional to the whole
  token table.
- OpenSea coverage is Ethereum-only (the client hardcodes the `ethereum` chain
  segment); objkt covers Tezos.

## Out of scope (future work)

- **User-report ingestion from ff-app** (a `token_spam_reports` table feeding the FF
  moderation system, with report-triggered `next_check_at` bumps rate-limited to
  protect vendor quota).
- **Admin/operator API** for writing `feralfile` verdicts (set *and* clear — clearing
  means deleting the row, since absence ≠ false).
- **Contract-level verdicts** (one vendor collection call covering thousands of
  airdropped tokens). Adding them later is a same-shape `contract_spam_verdicts`
  table plus one branch in the recompute — no changes to the token-level tables.
