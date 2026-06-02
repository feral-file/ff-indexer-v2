-- Migration 017: Enforce per-transfer uniqueness on ownership token_events
-- See https://github.com/feral-file/ff-indexer-v2/issues/46
--
-- ⚠️ CRITICAL: This migration MUST be applied BEFORE deploying application code that depends on it.
-- The application uses ON CONFLICT with token_events_ownership_unique and will fail at runtime
-- if this index does not exist. There is no fallback behavior.
--
-- ⚠️ CRITICAL: You MUST pause or stop write traffic before running this migration.
-- Reasons:
--   1. CREATE UNIQUE INDEX (non-concurrent) blocks writes during index build on large tables.
--   2. If writes continue between 017_dedup.sql and this file, new duplicates can cause index creation to fail.
--
-- Deployment sequence:
--   1. STOP write traffic (pause indexer workers, drain queues)
--   2. If token_events is large or has known duplicates, run 017_dedup.sql first (batched deletes)
--   3. Run this file (creates token_events_ownership_unique)
--   4. Verify the index exists (see DEVELOPMENT.md)
--   5. Deploy the new application version
--   6. RESUME write traffic
--
-- Deduplication is NOT inlined here: see 017_dedup.sql for a batched, commit-per-batch backfill.

BEGIN;

CREATE UNIQUE INDEX token_events_ownership_unique
ON token_events (token_id, owner_address, event_type, (metadata->>'tx_hash'))
WHERE event_type IN ('acquired', 'released')
  AND owner_address IS NOT NULL
  AND (metadata->>'tx_hash') IS NOT NULL;

COMMENT ON INDEX token_events_ownership_unique IS 'One acquired/released token_event per token-owner-transfer (tx_hash in metadata)';

COMMIT;
