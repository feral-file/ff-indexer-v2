BEGIN;

-- Migration 019_reindex: Enqueue IndexTokenMetadata jobs for OpenSea tokens and
-- slug backfill for all vendor releases.
--
-- Context
-- -------
-- OpenSea was not included in 018_reindex because OpenSea release enrichment
-- (release metadata via GetCollection) was added after that migration. Two new
-- capabilities require re-enrichment of all previously-enriched OpenSea tokens:
--
--   1. Release name and total_mints: enhanceOpenSea now calls GetCollection and
--      upserts Release.Name and Release.TotalMints on the release row (previously
--      both were nil, leaving releases with name=null and total_mints=null).
--
--   2. Release slug backfill: the enhancer now sets Release.Slug via GetCollection
--      so UpsertRelease populates vendor_release_slug. For OpenSea, the slug is
--      always equal to vendor_release_id (the collection slug), but the column was
--      not written by older code paths.
--
-- Note: IndexRelease does NOT support opensea as a vendor. OpenSea tokens enter the
-- system exclusively via on-chain event indexing; release rows are created during
-- OpenSea enrichment when a positive mint_number is successfully derived from the
-- token identifier.
--
-- Part 1: Re-enrich all enriched OpenSea tokens
-- -----------------------------------------------
-- Enqueues one IndexTokenMetadata job per token that already has an OpenSea
-- enrichment_sources row. Re-enrichment re-fetches NFT and collection data,
-- populates Release.Name/TotalMints/Slug, and upserts the release row.
--
-- Part 2: Slug backfill for all non-OpenSea releases
-- ---------------------------------------------------
-- For every non-OpenSea release that still has vendor_release_slug IS NULL, enqueue
-- the first member token (lowest mint_number) for re-enrichment. Enriching even a
-- single token is sufficient to upsert vendor_release_slug on the release row.
--
-- OpenSea is excluded from Part 2: Part 1 already handles all OpenSea enriched
-- tokens. The first member is queued unconditionally (regardless of whether it
-- already has an enrichment_sources entry). Filtering to un-enriched tokens only
-- would miss releases whose members were enriched before the slug column existed,
-- leaving the slug column still null despite enrichment_sources rows being present.
--
-- Idempotency
-- -----------
-- The partial unique index jobs_unique_key_active enforces at most one active
-- (pending or running) job per (queue, kind, unique_key). The ON CONFLICT clause
-- skips any token that already has an active metadata job, so this migration is
-- safe to re-run.
--
-- Fresh installs
-- --------------
-- Fresh installs have no enrichment_sources or release_members rows, so both
-- INSERTs produce no rows. No changes to db/init_pg_db.sql are required.

-- Part 1: re-enrich all OpenSea-enriched tokens.
INSERT INTO jobs (queue, kind, payload, status, unique_key)
SELECT
    'token_index'                                    AS queue,
    'IndexTokenMetadata'                             AS kind,
    jsonb_build_array(t.token_cid, null)             AS payload,
    'pending'                                        AS status,
    'index-metadata-' || t.token_cid                AS unique_key
FROM enrichment_sources es
JOIN tokens t ON t.id = es.token_id
WHERE es.vendor = 'opensea'
ON CONFLICT (queue, kind, unique_key)
    WHERE status IN ('pending', 'running') AND unique_key IS NOT NULL
DO NOTHING;

-- Part 2: for every non-OpenSea release without a slug, queue one token
-- (the first by mint_number) regardless of enrichment status.
INSERT INTO jobs (queue, kind, payload, status, unique_key)
SELECT
    'token_index'                                    AS queue,
    'IndexTokenMetadata'                             AS kind,
    jsonb_build_array(t.token_cid, null)             AS payload,
    'pending'                                        AS status,
    'index-metadata-' || t.token_cid                AS unique_key
FROM releases r
JOIN LATERAL (
    SELECT rm.token_id
    FROM release_members rm
    WHERE rm.release_id = r.id
    ORDER BY rm.mint_number
    LIMIT 1
) first_member ON TRUE
JOIN tokens t ON t.id = first_member.token_id
WHERE r.vendor != 'opensea'
  AND r.vendor_release_slug IS NULL
ON CONFLICT (queue, kind, unique_key)
    WHERE status IN ('pending', 'running') AND unique_key IS NOT NULL
DO NOTHING;

COMMIT;
