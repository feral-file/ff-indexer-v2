-- Migration 021: Token moderation — per-source verdicts plus a materialized status on tokens.
--
-- Unsolicited airdrop scams (e.g. "Visit <domain> to claim rewards" phishing lures) land in
-- every real wallet and are then rendered full-size by display surfaces (ff-app, FF1 walls).
-- Vendors already moderate these: OpenSea exposes `is_disabled` on its NFT API and objkt
-- exposes `flag` ("banned"). The enricher fetches those responses today but discards the
-- moderation verdict at unmarshal.
--
-- Two-tier design:
--   1. `token_moderation_verdicts` is the source of truth: one row per (token, source), where a
--      source is a moderating vendor ('opensea', 'objkt') or Feral File's own future
--      moderation system ('feralfile' — reserved now so it slots in as just another writer,
--      no schema change). Sources never overwrite each other.
--   2. `tokens.moderation_status` is the materialized combined verdict, recomputed transactionally
--      on every verdict write: a feralfile row wins outright in both directions (a non-none
--      status pins the token, 'none' whitelists it against vendors), otherwise the most severe
--      vendor verdict. Read paths filter on this single column and never join the verdicts table.
--
-- Both columns use the `moderation_status` enum rather than a boolean. Spam is the only
-- verdict today, but moderation is open-ended (nsfw, copyright takedowns, operator
-- decisions): as an enum, a new verdict kind is one added value, whereas a boolean would
-- force another column on tokens and another verdict table per kind.
--
-- Unlike the write-time contract blacklist (which drops events so blacklisted tokens are
-- never stored and cannot be un-hidden), moderation_status is tag-not-drop: the token stays
-- fully indexed and read paths filter it by default, with an opt-in to include them.
--
-- Default 'none': no signal means the token stays visible (fail-open) — hiding a user's real
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
-- No index is created on tokens.moderation_status: the only predicate in the read path is
-- `moderation_status = 'none'`, which matches nearly every row, so a partial index on the
-- moderated side could never serve it and a full index would not beat the existing filter.
-- The moderation sweeper's work queue is served by the partial index on
-- token_moderation_verdicts instead.
--
-- Pre-existing tokens are NOT covered by this file. token_moderation_verdicts starts empty, and
-- both writers only ever touch rows that already exist: the enricher writes at indexing
-- time, and the sweeper's GetTokenModerationVerdictsDueForCheck reads FROM token_moderation_verdicts
-- (an inner join, no discovery pass). Every token indexed before this release therefore
-- stays invisible to moderation filtering until something re-enriches it. Migration
-- 021_reindex.sql closes that gap and MUST be run separately, AFTER the new application
-- code is deployed — see the ordering note in its header.

-- Step 1: materialized column, verdict source table, and sweep-queue index (safe under load)
BEGIN;

CREATE TYPE moderation_status AS ENUM ('none', 'spam');

ALTER TABLE tokens
    ADD COLUMN moderation_status moderation_status NOT NULL DEFAULT 'none';

COMMENT ON COLUMN tokens.moderation_status IS
    'Materialized combined moderation verdict, recomputed from token_moderation_verdicts on '
    'every verdict write: a feralfile row wins outright, otherwise the most severe vendor '
    'verdict. An enum so new verdict kinds need no schema change; only none/spam exist today. '
    'Read paths exclude moderated tokens unless include_moderated is requested. Tag-not-drop: '
    'the row stays fully indexed for reversibility.';

CREATE TYPE moderation_source AS ENUM ('opensea', 'objkt', 'feralfile');

CREATE TABLE token_moderation_verdicts (
    token_id BIGINT NOT NULL REFERENCES tokens (id) ON DELETE CASCADE,
    source moderation_source NOT NULL,
    verdict moderation_status NOT NULL,                  -- this source's decision; an enum so new verdict kinds do not need another column or table
    detail JSONB,                                        -- raw moderation fields only ({"is_disabled":true} / {"flag":"banned"}); full payload lives in enrichment_sources.vendor_json
    last_checked_at TIMESTAMPTZ NOT NULL DEFAULT now(),  -- last time the source CONFIRMED the stored verdict; failed checks do not touch it (an error is not a verdict)
    next_check_at TIMESTAMPTZ,                           -- sweep due time; NULL = never swept (feralfile rows)
    consecutive_failures INT NOT NULL DEFAULT 0,         -- sweeper error backoff state; reset to 0 on every successful check
    last_error TEXT,                                     -- error message from the last failed check (NULL after a successful one)
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (token_id, source)
);

COMMENT ON TABLE token_moderation_verdicts IS
    'Source of truth for token moderation: one row per (token, source). Rows exist only '
    'after a source has actually published a verdict — absence means "no opinion", which '
    'is deliberately distinct from a "none" verdict. tokens.moderation_status is the '
    'materialized combination.';

-- The moderation sweeper's work queue: per-source so one vendor's API quota cannot starve
-- another's, partial so feralfile rows (next_check_at IS NULL) never occupy the index.
CREATE INDEX idx_token_moderation_verdicts_due
    ON token_moderation_verdicts (source, next_check_at)
    WHERE next_check_at IS NOT NULL;

CREATE TRIGGER update_token_moderation_verdicts_updated_at
    BEFORE UPDATE ON token_moderation_verdicts
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
    'moderation_status_changed'
)) NOT VALID;

COMMIT;

-- Step 2b: validate in a separate transaction. Takes SHARE UPDATE EXCLUSIVE, so concurrent
-- reads and writes to token_events continue while the scan runs.
ALTER TABLE token_events VALIDATE CONSTRAINT token_events_event_type_check;
