BEGIN;

-- Migration 019: Enqueue IndexTokenMetadata jobs for tokens that need release membership.
--
-- Context
-- -------
-- Migration 018 added releases and release_members. New tokens are written at enrichment
-- time by the updated enhancer. Pre-existing tokens must be re-enriched so the enhancer
-- can re-fetch vendor data and populate those tables.
--
-- Why reindex rather than derive from stored vendor_json
-- ------------------------------------------------------
-- Stored vendor_json from before this release is incomplete for every vendor:
--   Art Blocks  - max_invocations was not fetched; total_mints would be absent
--   Feral File  - index and seriesID were not stored; mint_number cannot be derived
--   fxhash      - tokens were stored as vendor=objkt with no generative_token/iteration;
--                 vendor_release_id cannot be derived and the vendor bucket is wrong
--   objkt       - fa.collection_type was not fetched; custom collections cannot be identified
--
-- Approach
-- --------
-- Insert one IndexTokenMetadata job per previously-enriched token. The running worker
-- claims jobs from the token_index queue, calls EnhanceTokenMetadata (rate-limited vendor
-- API calls), re-stores a complete vendor_json, and upserts release + release_member rows.
--
-- Idempotency
-- -----------
-- The partial unique index jobs_unique_key_active enforces at most one active
-- (pending or running) job per (queue, kind, unique_key). The ON CONFLICT clause
-- below skips any token that already has an active metadata job so this migration
-- is safe to run again if needed.
--
-- Fresh installs
-- --------------
-- Fresh installs have no enrichment_sources rows, so this INSERT produces no rows.
-- No changes to db/init_pg_db.sql are required.

INSERT INTO jobs (queue, kind, payload, status, unique_key)
SELECT
    'token_index'                                    AS queue,
    'IndexTokenMetadata'                             AS kind,
    jsonb_build_array(t.token_cid, null)             AS payload,
    'pending'                                        AS status,
    'index-metadata-' || t.token_cid                AS unique_key
FROM enrichment_sources es
JOIN tokens t ON t.id = es.token_id
WHERE es.vendor IN ('artblocks', 'feralfile', 'fxhash', 'objkt')
ON CONFLICT (queue, kind, unique_key)
    WHERE status IN ('pending', 'running') AND unique_key IS NOT NULL
DO NOTHING;

COMMIT;
