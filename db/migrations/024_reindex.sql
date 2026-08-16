BEGIN;

-- Migration 024_reindex: Re-enqueue IndexTokenMetadata for FA2 tokens stored with the
-- unsigned-fxhash placeholder, so already-indexed tokens heal.
--
-- Context
-- -------
-- TzKT's `/v1/tokens[].metadata` is a resolved-metadata cache and can permanently serve
-- a mint-time snapshot when TzKT misses a later `token_metadata` big map update. For
-- fxhash gentks that snapshot is the "[WAITING TO BE SIGNED]" placeholder, whose
-- artifactUri is a static IPFS HTML page — so the stored `animation_url` renders the
-- placeholder forever. The resolver now detects the placeholder and re-resolves from the
-- contract's `token_metadata` big map, but that code only runs inside a future
-- IndexTokenMetadata execution; rows already in `token_metadata` are served from
-- PostgreSQL and never touch the resolver again. This migration enqueues that execution
-- for every affected stored row.
--
-- Affected-row predicate
-- ----------------------
-- Three markers, OR-ed because no single one is complete across enricher versions:
--
--   * `name` still "[WAITING TO BE SIGNED]" — the normalized on-chain name. The fxhash
--     enricher fixes the display name in enrichment_sources, not here, so this usually
--     survives; but match the others too in case older code paths wrote differently.
--   * `description` containing the placeholder's signer text.
--   * `animation_url` carrying the placeholder page's CID
--     (QmdGV3UqJqX4v5x9nFcDYeekCEAm3SDXUG5SHdjKQKn4Pe) — matches whether the URL was
--     stored as ipfs:// or resolved through any gateway. This is the marker that
--     actually breaks the frame, so it is the load-bearing one.
--
-- Scoped to standard = 'fa2': the stale-cache path exists only in the TzKT resolve
-- branch. The predicate is a sequential scan over token_metadata; acceptable for a
-- one-off migration.
--
-- ⚠️ Deployment ordering: run this AFTER the new application code is live
-- -----------------------------------------------------------------------
-- Same reversal as 021_reindex, for the same reason: this file enqueues work rather
-- than changing schema. A worker running the previous code would re-fetch the same
-- stale TzKT cache, re-store the placeholder, and mark the job succeeded — the backfill
-- would silently do nothing. Recovery if that happens: re-run THIS FILE only
-- (jobs_unique_key_active guards only active jobs, so finished ones do not block
-- re-insertion).
--
-- Verification
-- ------------
-- The verified on-chain reproducer is KT1KEa8z6vWXDJrVqtMrAeDVzsvxat3kHaCE token
-- 324719. After the queue drains, this must return an animation_url NOT containing
-- QmdGV3UqJqX4v5x9nFcDYeekCEAm3SDXUG5SHdjKQKn4Pe:
--
--   SELECT tm.name, tm.animation_url
--   FROM token_metadata tm JOIN tokens t ON t.id = tm.token_id
--   WHERE t.token_cid = 'tezos:mainnet:fa2:KT1KEa8z6vWXDJrVqtMrAeDVzsvxat3kHaCE:324719';
--
-- Volume / sizing
-- ---------------
-- Size the queue burst before running (each job re-fetches TzKT + the metadata URI):
--
--   SELECT count(*)
--   FROM token_metadata tm JOIN tokens t ON t.id = tm.token_id
--   WHERE t.standard = 'fa2'
--     AND (lower(btrim(tm.name)) = '[waiting to be signed]'
--          OR lower(tm.description) LIKE '%waiting to be signed by fxhash signer%'
--          OR tm.animation_url LIKE '%QmdGV3UqJqX4v5x9nFcDYeekCEAm3SDXUG5SHdjKQKn4Pe%');
--
-- Idempotency
-- -----------
-- The partial unique index jobs_unique_key_active enforces at most one active (pending
-- or running) job per (queue, kind, unique_key); ON CONFLICT skips tokens that already
-- have an active metadata job, so this migration is safe to re-run. Tokens whose big map
-- still holds the placeholder (genuinely unsigned gentks) re-store the placeholder and
-- would be re-enqueued by a future re-run; that is correct — they are still unsigned.
--
-- Fresh installs
-- --------------
-- Fresh installs have no token_metadata rows, so this INSERT produces no rows. No
-- changes to db/init_pg_db.sql are required.

INSERT INTO jobs (queue, kind, payload, status, unique_key)
SELECT
    'token_index'                                    AS queue,
    'IndexTokenMetadata'                             AS kind,
    jsonb_build_array(t.token_cid, null)             AS payload,
    'pending'                                        AS status,
    'index-metadata-' || t.token_cid                 AS unique_key
FROM token_metadata tm
JOIN tokens t ON t.id = tm.token_id
WHERE t.standard = 'fa2'
  AND (
    lower(btrim(tm.name)) = '[waiting to be signed]'
    OR lower(tm.description) LIKE '%waiting to be signed by fxhash signer%'
    OR tm.animation_url LIKE '%QmdGV3UqJqX4v5x9nFcDYeekCEAm3SDXUG5SHdjKQKn4Pe%'
  )
ON CONFLICT (queue, kind, unique_key)
    WHERE status IN ('pending', 'running') AND unique_key IS NOT NULL
DO NOTHING;

COMMIT;
