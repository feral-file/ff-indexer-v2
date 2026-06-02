-- Migration 017 dedup (run BEFORE 017.sql on large databases)
-- See https://github.com/feral-file/ff-indexer-v2/issues/46
--
-- Removes duplicate acquired/released token_events, keeping the row with the lowest id
-- per (token_id, owner_address, event_type, metadata.tx_hash).
--
-- ⚠️ CRITICAL: You MUST pause or stop write traffic before running this migration.
-- If writes continue between this dedup and 017.sql's unique index creation, new duplicates
-- can be introduced, causing the unique index creation to fail.
--
-- Why a separate script:
--   - The NOT IN (SELECT MIN(id) ... GROUP BY) pattern does not scale to millions of rows.
--   - This script deletes in batches and COMMITs between batches to limit lock duration and WAL.
--
-- How to run (must be OUTSIDE an explicit BEGIN transaction):
--   1. STOP write traffic (pause indexer workers, drain queues)
--   2. psql ... -f db/migrations/017_dedup.sql
--   3. psql ... -f db/migrations/017.sql (immediately after)
--   4. Deploy new application version
--   5. RESUME write traffic
--
-- IMPORTANT: This script does its own transaction control (COMMIT per batch).
-- Migration runners that auto-wrap files in BEGIN/COMMIT must disable that wrapping for this file.
--
-- Safe to re-run: later batches delete zero rows and the procedure exits.

-- Supporting index for the duplicate-finding join (dropped at the end of this script).
-- CONCURRENTLY avoids blocking writes during index build on large tables.
--
-- Drop invalid index from prior interrupted concurrent build (reruns must not skip creation).
DROP INDEX CONCURRENTLY IF EXISTS idx_token_events_ownership_dedup_tmp;

CREATE INDEX CONCURRENTLY idx_token_events_ownership_dedup_tmp
ON token_events (token_id, owner_address, event_type, (metadata->>'tx_hash'), id)
WHERE event_type IN ('acquired', 'released')
  AND owner_address IS NOT NULL
  AND (metadata->>'tx_hash') IS NOT NULL;

CREATE OR REPLACE PROCEDURE migration_017_dedup_token_events(p_batch_size bigint DEFAULT 50000)
LANGUAGE plpgsql
AS $proc$
DECLARE
    v_deleted bigint;
BEGIN
    IF p_batch_size < 1 THEN
        RAISE EXCEPTION 'p_batch_size must be >= 1';
    END IF;

    LOOP
        -- Delete rows that have an older sibling with the same ownership key (keep MIN(id)).
        -- DISTINCT prevents fanout when a newer row joins multiple older rows.
        WITH dup_ids AS (
            SELECT DISTINCT newer.id
            FROM token_events AS newer
            INNER JOIN token_events AS older
                ON older.token_id = newer.token_id
               AND older.owner_address = newer.owner_address
               AND older.event_type = newer.event_type
               AND older.metadata->>'tx_hash' = newer.metadata->>'tx_hash'
               AND older.id < newer.id
            WHERE newer.event_type IN ('acquired', 'released')
              AND newer.owner_address IS NOT NULL
              AND newer.metadata->>'tx_hash' IS NOT NULL
            LIMIT p_batch_size
        )
        DELETE FROM token_events AS t
        USING dup_ids AS d
        WHERE t.id = d.id;

        GET DIAGNOSTICS v_deleted = ROW_COUNT;
        RAISE NOTICE 'migration_017_dedup: deleted % duplicate ownership token_events', v_deleted;
        COMMIT;
        EXIT WHEN v_deleted = 0;
    END LOOP;
END;
$proc$;

CALL migration_017_dedup_token_events();

DROP PROCEDURE migration_017_dedup_token_events;

-- CONCURRENTLY avoids blocking writes during index drop.
DROP INDEX CONCURRENTLY IF EXISTS idx_token_events_ownership_dedup_tmp;
