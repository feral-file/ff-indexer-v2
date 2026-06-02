-- Migration 017: Deduplicate ownership token_events and enforce per-transfer uniqueness
-- See https://github.com/feral-file/ff-indexer-v2/issues/46
--
-- ⚠️ CRITICAL: This migration MUST be applied BEFORE deploying application code that depends on it.
-- The application uses ON CONFLICT with token_events_ownership_unique and will fail at runtime
-- if this index does not exist. There is no fallback behavior.
--
-- Deployment sequence:
--   1. Run this migration on all database instances
--   2. Verify the index exists (see DEVELOPMENT.md for verification commands)
--   3. Deploy the new application version

BEGIN;

-- Remove duplicate acquired/released rows that share token, owner, event type, and tx_hash.
-- Keep the earliest row (lowest id) as the canonical record.
DELETE FROM token_events te
WHERE te.event_type IN ('acquired', 'released')
  AND te.owner_address IS NOT NULL
  AND te.metadata->>'tx_hash' IS NOT NULL
  AND te.id NOT IN (
      SELECT MIN(id)
      FROM token_events
      WHERE event_type IN ('acquired', 'released')
        AND owner_address IS NOT NULL
        AND metadata->>'tx_hash' IS NOT NULL
      GROUP BY token_id, owner_address, event_type, metadata->>'tx_hash'
  );

CREATE UNIQUE INDEX token_events_ownership_unique
ON token_events (token_id, owner_address, event_type, (metadata->>'tx_hash'))
WHERE event_type IN ('acquired', 'released')
  AND owner_address IS NOT NULL
  AND (metadata->>'tx_hash') IS NOT NULL;

COMMENT ON INDEX token_events_ownership_unique IS 'One acquired/released token_event per token-owner-transfer (tx_hash in metadata)';

COMMIT;
