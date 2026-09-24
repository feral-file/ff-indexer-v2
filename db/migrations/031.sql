-- Migration 031: index token_events (token_id, event_type) for the ownership-event
-- replay delete in UpsertToken.
--
-- UpsertToken rebuilds a token's provenance and first runs
--   DELETE FROM token_events WHERE token_id = $1 AND event_type IN ('acquired','released')
-- No index serves that predicate: idx_token_events_token_created is partial on
-- owner_address IS NULL (ownership events always carry an owner, so it never applies),
-- and token_events_ownership_unique is partial on tx_hash IS NOT NULL, which the delete
-- does not assert. The planner therefore sequentially scans the whole table for every
-- token indexed. Measured on prod-01 on 2026-09-24: 5.07M rows, ~10 s per delete, up to
-- 65 concurrent deletes from the 30 IndexTokens workers, Postgres pinned at 3.5 of 4
-- cores, IndexTokens jobs of 20 tokens taking ~6 minutes each. pg_stat_user_tables
-- showed 244k sequential scans reading 1.02 trillion rows over the table's life.
--
-- A plain (token_id, event_type) btree is chosen over a partial index on the two
-- ownership event types: 80% of rows are ownership events, so a partial index would
-- save little space, and a plain index also serves the per-owner ownership lookups
-- that filter on token_id and event_type first.
--
-- LOCKING: CONCURRENTLY takes no exclusive lock and is safe under traffic. Like
-- 017_dedup.sql and 030_drop_unused_indexes.sql it must run in autocommit mode
-- (no BEGIN/COMMIT wrapper); IF NOT EXISTS makes it re-runnable. Apply before or
-- after deploying application code; nothing in the code depends on it.

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_token_events_token_event
    ON token_events (token_id, event_type);
