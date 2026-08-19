-- Migration 025: deferred-provenance marker (tokens.provenance_deferred_at).
--
-- The EVM credit guard (ethereum.full_provenance_disabled, PR #127) skips per-token
-- history replay because one token's genesis-to-head log walk costs ~0.9M Infura
-- credits under the 10k block-span cap. A skipped IndexTokenProvenances job returns
-- success, so without a persisted marker the skipped tokens would be unidentifiable
-- once the guard lifts and their provenance permanently incomplete unless some
-- unrelated trigger happened to reindex them.
--
-- provenance_deferred_at records the skip: set whenever a guard bypasses provenance
-- work (the IndexTokenProvenances workflow gate, and the ERC-1155 owner-path
-- balance-only shortcut), cleared when full provenance indexing succeeds.
-- db/migrations/025_backfill.sql — run AFTER the guard is disabled — enqueues
-- IndexTokenProvenances for every marked token.
--
-- The partial index keeps the backlog query cheap; the predicate set is small (only
-- tokens indexed while the guard was active).

BEGIN;

ALTER TABLE tokens ADD COLUMN provenance_deferred_at TIMESTAMPTZ;

CREATE INDEX idx_tokens_provenance_deferred ON tokens (provenance_deferred_at) WHERE provenance_deferred_at IS NOT NULL;

COMMIT;
