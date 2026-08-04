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

BEGIN;

ALTER TABLE tokens
    ADD COLUMN is_spam BOOLEAN NOT NULL DEFAULT false;

COMMENT ON COLUMN tokens.is_spam IS
    'Vendor moderation verdict: true when OpenSea reports is_disabled or objkt reports '
    'flag=banned for this token. Read paths exclude flagged tokens unless include_spam '
    'is requested. Tag-not-drop: the row stays fully indexed for reversibility.';

-- Partial index: the flagged set is tiny relative to the table, and the default read
-- path predicate is `is_spam = false`; a partial index on the true side keeps the
-- anti-join cheap without indexing the entire table.
CREATE INDEX idx_tokens_is_spam ON tokens (id) WHERE is_spam;

-- token_events.event_type is guarded by a CHECK constraint (migration 014); widen it to
-- accept the broadcast event emitted when a token's spam verdict changes, so collection
-- sync clients can drop or restore the token.
ALTER TABLE token_events DROP CONSTRAINT token_events_event_type_check;
ALTER TABLE token_events ADD CONSTRAINT token_events_event_type_check CHECK (event_type IN (
    'acquired',
    'released',
    'metadata_updated',
    'enrichment_updated',
    'viewability_changed',
    'spam_status_changed'
));

COMMIT;
