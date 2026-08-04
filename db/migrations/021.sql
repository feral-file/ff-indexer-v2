-- Migration 021: Add is_spam to tokens for vendor-flagged spam filtering.
--
-- Unsolicited airdrop scams (e.g. "Visit <domain> to claim rewards" phishing lures) land in
-- every real wallet and are then rendered full-size by display surfaces (ff-app, FF1 walls).
-- Vendors already moderate these: OpenSea exposes `is_disabled` on its NFT API and objkt
-- exposes `flag` ("banned"). The enricher fetches those responses today but discards the
-- moderation verdict at unmarshal.
--
-- This column records that verdict per token. Unlike the write-time contract blacklist
-- (which drops events so blacklisted tokens are never stored and cannot be un-hidden),
-- is_spam is tag-not-drop: the token stays fully indexed and read paths filter it by
-- default, with an opt-in to include flagged tokens.
--
-- Default false: no vendor signal means the token stays visible (fail-open) — hiding a
-- user's real asset by mistake is worse than letting spam through until the next sweep.
--
-- LOCKING: run steps 1 and 2 as written. Step 1 is a metadata-only ADD COLUMN (no table
-- rewrite on PG11+) and is safe under normal traffic. Step 2 deliberately splits the CHECK
-- constraint into ADD ... NOT VALID (brief ACCESS EXCLUSIVE, no scan) followed by VALIDATE
-- in its own transaction (SHARE UPDATE EXCLUSIVE, does not block reads or writes) — a plain
-- ADD CONSTRAINT would validate every row of token_events, which holds millions of broadcast
-- events, under ACCESS EXCLUSIVE and stall the sync API for every FF1 and mobile client.
--
-- No index is created on is_spam: the only predicate in the read path is `is_spam = false`,
-- which matches nearly every row, so a partial index on the true side could never serve it
-- and a full index would not beat the existing filter. Revisit when the periodic spam
-- sweeper lands and starts querying the flagged set directly.

-- Step 1: column (safe under load)
BEGIN;

ALTER TABLE tokens
    ADD COLUMN is_spam BOOLEAN NOT NULL DEFAULT false;

COMMENT ON COLUMN tokens.is_spam IS
    'Vendor moderation verdict: true when OpenSea reports is_disabled or objkt reports '
    'flag=banned for this token. Read paths exclude flagged tokens unless include_spam '
    'is requested. Tag-not-drop: the row stays fully indexed for reversibility.';

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
