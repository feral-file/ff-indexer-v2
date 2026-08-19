-- Migration 025_backfill: enqueue full provenance for tokens skipped by the EVM
-- credit guard (tokens.provenance_deferred_at IS NOT NULL).
--
-- ⚠️ Run this ONLY AFTER disabling the guard (ethereum.full_provenance_disabled=false
-- in deploy config) and restarting workers. With the guard still on, every enqueued
-- job re-skips, re-marks the token deferred, and reports success — the backfill would
-- silently do nothing (safe, but wasted; re-run this file after the guard is off).
--
-- Volume / credit sizing
-- ----------------------
-- Each backfilled token replays its full event history. With the #127 efficiency
-- fixes (span cap 10000 configured, merged legs) a genesis-to-head ERC-721 token walk
-- is ~2,600 eth_getLogs calls ≈ 0.66M Infura credits; ERC-1155 costs up to ~2x that.
-- Size the burst against the daily credit quota before running:
--
--   SELECT count(*), min(provenance_deferred_at), max(provenance_deferred_at)
--   FROM tokens WHERE provenance_deferred_at IS NOT NULL;
--
-- If count x ~1M credits exceeds the remaining daily quota, run in slices by
-- deferral time (append: AND provenance_deferred_at < '<cutoff>') across days.
--
-- Idempotency
-- -----------
-- The partial unique index jobs_unique_key_active enforces at most one active
-- (pending or running) job per (queue, kind, unique_key); ON CONFLICT skips tokens
-- that already have an active provenance job, so this file is safe to re-run.
-- Successful full provenance clears provenance_deferred_at, so re-runs shrink to the
-- not-yet-processed remainder.
--
-- Queue parameter (REQUIRED)
-- --------------------------
-- Jobs must land on the deployment's configured jobs.token_queue or workers
-- never pick them up. The queue is a required psql variable — running this file
-- without it fails loudly instead of inserting into the wrong queue:
--
--   psql ... -v queue=token_index -f db/migrations/025_backfill.sql
--
-- Use the exact value of jobs.token_queue from the deployment config
-- (default: token_index).
--
-- Fresh installs
-- --------------
-- Fresh installs have no deferred tokens; this INSERT produces no rows.

BEGIN;

INSERT INTO jobs (queue, kind, payload, status, unique_key)
SELECT
    :'queue'                                         AS queue,
    'IndexTokenProvenances'                          AS kind,
    jsonb_build_array(t.token_cid, null)             AS payload,
    'pending'                                        AS status,
    'index-provenance-' || t.token_cid               AS unique_key
FROM tokens t
WHERE t.provenance_deferred_at IS NOT NULL
ON CONFLICT (queue, kind, unique_key)
    WHERE status IN ('pending', 'running') AND unique_key IS NOT NULL
DO NOTHING;

COMMIT;
