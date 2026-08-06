-- Migration 021: Spam filtering — per-source verdicts plus a materialized flag on tokens.
--
-- Unsolicited airdrop scams (e.g. "Visit <domain> to claim rewards" phishing lures) land in
-- every real wallet and are then rendered full-size by display surfaces (ff-app, FF1 walls).
-- Vendors already moderate these: OpenSea exposes `is_disabled` on its NFT API and objkt
-- exposes `flag` ("banned"). The enricher fetches those responses today but discards the
-- moderation verdict at unmarshal.
--
-- Two-tier design:
--   1. `token_spam_verdicts` is the source of truth: one row per (token, source), where a
--      source is a moderating vendor ('opensea', 'objkt') or Feral File's own future
--      moderation system ('feralfile' — reserved now so it slots in as just another writer,
--      no schema change). Sources never overwrite each other.
--   2. `tokens.is_spam` is the materialized combined verdict, recomputed transactionally on
--      every verdict write: a feralfile row wins outright in both directions (true pins
--      spam, false whitelists against vendors), otherwise OR of the vendor verdicts. Read
--      paths filter on this single column and never join the verdicts table.
--
-- Unlike the write-time contract blacklist (which drops events so blacklisted tokens are
-- never stored and cannot be un-hidden), is_spam is tag-not-drop: the token stays fully
-- indexed and read paths filter it by default, with an opt-in to include flagged tokens.
--
-- Default false: no signal means the token stays visible (fail-open) — hiding a user's real
-- asset by mistake is worse than letting spam through until the next sweep.
--
-- LOCKING: run the steps as written. Step 1 is a metadata-only ADD COLUMN plus brand-new
-- objects (no table rewrite on PG11+) and is safe under normal traffic. Step 2 deliberately
-- splits the CHECK constraint into ADD ... NOT VALID (brief ACCESS EXCLUSIVE, no scan)
-- followed by VALIDATE in its own transaction (SHARE UPDATE EXCLUSIVE, does not block reads
-- or writes) — a plain ADD CONSTRAINT would validate every row of token_events, which holds
-- millions of broadcast events, under ACCESS EXCLUSIVE and stall the sync API for every FF1
-- and mobile client.
--
-- No index is created on tokens.is_spam: the only predicate in the read path is
-- `is_spam = false`, which matches nearly every row, so a partial index on the true side
-- could never serve it and a full index would not beat the existing filter. The spam
-- sweeper's work queue is served by the partial index on token_spam_verdicts instead.
--
-- Step 3 backfills pre-existing tokens: token_spam_verdicts starts empty, and both the
-- enricher (indexing time) and the sweeper (GetTokenSpamVerdictsDueForCheck, which reads
-- FROM token_spam_verdicts) only ever touch rows that already exist. Without a backfill,
-- every token indexed before this migration — including ones a vendor has already flagged —
-- stays invisible to the sweeper forever; nothing re-asks the vendor on its own. This
-- mirrors migration 018_reindex's fix for the same class of gap (new per-token data needed
-- from vendors, old rows can't have it). Like that migration, it does not derive the verdict
-- from stored enrichment_sources.vendor_json — vendor_json is a snapshot of whatever fields
-- the enricher chose to keep at the time, not a contract that `flag`/`is_disabled` was
-- captured for every historical row (018's own comment documents exactly this failure mode
-- for other fields). It re-asks the vendor for real via the existing job queue instead.

-- Step 1: materialized column, verdict source table, and sweep-queue index (safe under load)
BEGIN;

ALTER TABLE tokens
    ADD COLUMN is_spam BOOLEAN NOT NULL DEFAULT false;

COMMENT ON COLUMN tokens.is_spam IS
    'Materialized combined spam verdict, recomputed from token_spam_verdicts on every '
    'verdict write: a feralfile row wins outright, otherwise OR of vendor verdicts. Read '
    'paths exclude flagged tokens unless include_spam is requested. Tag-not-drop: the row '
    'stays fully indexed for reversibility.';

CREATE TYPE spam_source AS ENUM ('opensea', 'objkt', 'feralfile');

CREATE TABLE token_spam_verdicts (
    token_id BIGINT NOT NULL REFERENCES tokens (id) ON DELETE CASCADE,
    source spam_source NOT NULL,
    verdict BOOLEAN NOT NULL,
    detail JSONB,                                        -- raw moderation fields only ({"is_disabled":true} / {"flag":"banned"}); full payload lives in enrichment_sources.vendor_json
    last_checked_at TIMESTAMPTZ NOT NULL DEFAULT now(),  -- last time the source CONFIRMED the stored verdict; failed checks do not touch it (an error is not a verdict)
    next_check_at TIMESTAMPTZ,                           -- sweep due time; NULL = never swept (feralfile rows)
    consecutive_failures INT NOT NULL DEFAULT 0,         -- sweeper error backoff state; reset to 0 on every successful check
    last_error TEXT,                                     -- error message from the last failed check (NULL after a successful one)
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (token_id, source)
);

COMMENT ON TABLE token_spam_verdicts IS
    'Source of truth for spam moderation: one row per (token, source). Rows exist only '
    'after a source has actually published a verdict — absence means "no opinion", which '
    'is deliberately distinct from a clean verdict (tri-state). tokens.is_spam is the '
    'materialized combination.';

-- The spam sweeper's work queue: per-source so one vendor's API quota cannot starve
-- another's, partial so feralfile rows (next_check_at IS NULL) never occupy the index.
CREATE INDEX idx_token_spam_verdicts_due
    ON token_spam_verdicts (source, next_check_at)
    WHERE next_check_at IS NOT NULL;

CREATE TRIGGER update_token_spam_verdicts_updated_at
    BEFORE UPDATE ON token_spam_verdicts
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

COMMIT;

-- Step 2a: widen the event_type CHECK constraint (from migration 014) without scanning.
-- NOT VALID skips validation of existing rows; they already satisfy the widened predicate
-- because it only adds a value.
BEGIN;

ALTER TABLE token_events DROP CONSTRAINT token_events_event_type_check;
ALTER TABLE token_events ADD CONSTRAINT token_events_event_type_check CHECK (event_type IN (
    'acquired',
    'released',
    'metadata_updated',
    'enrichment_updated',
    'viewability_changed',
    'spam_status_changed'
)) NOT VALID;

COMMIT;

-- Step 2b: validate in a separate transaction. Takes SHARE UPDATE EXCLUSIVE, so concurrent
-- reads and writes to token_events continue while the scan runs.
ALTER TABLE token_events VALIDATE CONSTRAINT token_events_event_type_check;

-- Step 3: enqueue one IndexTokenMetadata job per token already enriched by a moderating
-- vendor, so the running worker re-fetches real vendor data and the enricher writes the
-- first token_spam_verdicts row for it (see the migration header for why this re-asks the
-- vendor instead of trusting stored vendor_json).
--
-- Idempotency: the partial unique index jobs_unique_key_active enforces at most one active
-- (pending or running) job per (queue, kind, unique_key), so this migration is safe to run
-- again — it only skips tokens that already have one of these jobs in flight, not ones
-- whose job already finished.
--
-- Fresh installs have no enrichment_sources rows, so this INSERT produces no rows. No
-- changes to db/init_pg_db.sql are required (same as migration 018_reindex).
INSERT INTO jobs (queue, kind, payload, status, unique_key)
SELECT
    'token_index'                          AS queue,
    'IndexTokenMetadata'                   AS kind,
    jsonb_build_array(t.token_cid, null)   AS payload,
    'pending'                              AS status,
    'index-metadata-' || t.token_cid       AS unique_key
FROM enrichment_sources es
JOIN tokens t ON t.id = es.token_id
WHERE es.vendor IN ('opensea', 'objkt')
ON CONFLICT (queue, kind, unique_key)
    WHERE status IN ('pending', 'running') AND unique_key IS NOT NULL
DO NOTHING;
