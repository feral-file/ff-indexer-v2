-- Migration 032: move Feral File artwork URLs to the new CDN origin.
--
-- Context
-- -------
-- Feral File moved artwork files from https://cdn.feralfileassets.com/ to
-- https://cdn.artworks.feralfile.io/ with unchanged paths. The old hostname stopped
-- resolving around 2026-09-23. From then on the media health sweeper has been flipping
-- rows to broken (failure_reason = 'dns') on its 24h recheck cadence, which hides the
-- tokens. The new CDN does not serve a directory index, so a directory-style URL
-- ("…/<ts>/?edition_number=…") must also gain "index.html".
--
-- The rewrite is types.MigrateFeralFileCDN (internal/types/feralfile_cdn.go), mirrored
-- below by migration_032_ff_cdn_move(). Both must produce byte-identical strings because
-- every *_url_hash column is md5 of the exact URL.
--
-- What this file does, mirroring the store's own write paths
-- ----------------------------------------------------------
--   * media_render_probes: move url + hash (the PK) in place. Verdicts that are not
--     rendered_ok become due now. consecutive_failures resets when the old host's
--     outage caused the failure.
--   * media_assets: move source_url + hash. The Cloudflare copies stay valid and the
--     API finds assets by md5(display URL), so no re-upload is needed.
--   * Then per batch of tokens, in one transaction each:
--       - enrichment_sources: rewrite image/animation URL + hash, and insert
--         token_events 'enrichment_updated' {"vendor", "changed_fields"}, like
--         UpsertEnrichmentSource.
--       - token_metadata: the same, with 'metadata_updated' {"changed_fields"}, like
--         UpsertTokenMetadata.
--       - token_media_health: move url + hash in place, like UpdateMediaURLAndPropagate.
--         The API serves media_url from these rows, so it has to change in the same
--         transaction as the event: a client that refetches on the event then sees the
--         new URL. Rows broken by the old host's DNS failure become 'unknown' and are
--         backdated so the sweeper re-probes them first. Other statuses are kept.
--   Raw payloads (vendor_json, origin_json, latest_json) are left verbatim, as in code.
--
-- Deploy ordering (REQUIRED)
-- --------------------------
-- Run this ONLY AFTER the application version containing types.MigrateFeralFileCDN is
-- live. Older code rebuilds old-host URLs from the Feral File API and on-chain metadata
-- and would revert these rows (emitting another round of events).
--
-- Running
-- -------
-- This file does its own transaction control (COMMIT per batch). It must run OUTSIDE an
-- explicit BEGIN, in one psql session (the work list is a temp table):
--
--   psql ... -v ON_ERROR_STOP=1 -f db/migrations/032_feralfile_cdn_move.sql
--
-- Workers can keep running. Rare races are self-healing:
--   * An enrichment upsert that read the old row before a batch commits may fail its
--     health-row insert on the unique key, then retry and find nothing to change.
--   * A render probe in flight may write back one orphan row under the old hash.
--
-- Sizing (2026-09-25): ~29.6k enrichment rows, ~2.3k token_metadata rows, ~50k health
-- rows, 18.4k render probe rows, 16.6k media assets. Expect ~32k token_events. The
-- sweeper then re-probes ~1.2k 'unknown' rows first.
--
--   SELECT count(*) FROM enrichment_sources
--   WHERE image_url LIKE '%cdn.feralfileassets.com/%' OR animation_url LIKE '%cdn.feralfileassets.com/%';
--
-- Idempotency
-- -----------
-- Safe to re-run. Every statement selects only rows still on the old host, so a re-run
-- touches nothing already moved and emits no duplicate events.
--
-- Fresh installs: no-op.

\set ON_ERROR_STOP on

-- Rewrite helper; must match types.MigrateFeralFileCDN exactly.
CREATE OR REPLACE FUNCTION migration_032_ff_cdn_move(u text) RETURNS text
LANGUAGE sql IMMUTABLE PARALLEL SAFE AS $$
    SELECT CASE
        WHEN r LIKE 'https://cdn.artworks.feralfile.io/%'
            THEN regexp_replace(r, '^(https://cdn\.artworks\.feralfile\.io/[^?#]*/)([?#]|$)', '\1index.html\2')
        ELSE r
    END
    FROM (SELECT replace(u, 'https://cdn.feralfileassets.com/', 'https://cdn.artworks.feralfile.io/') AS r) s
$$;

-- Self-check against the Go test vectors before touching any data.
DO $$
DECLARE
    v_case record;
BEGIN
    FOR v_case IN SELECT * FROM (VALUES
        ('https://cdn.feralfileassets.com/previews/a/1/preview.mp4', 'https://cdn.artworks.feralfile.io/previews/a/1/preview.mp4'),
        ('https://cdn.feralfileassets.com/thumbnails/a/1', 'https://cdn.artworks.feralfile.io/thumbnails/a/1'),
        ('https://cdn.feralfileassets.com/previews/a/1/?edition_number=3&blockchain=bitmark', 'https://cdn.artworks.feralfile.io/previews/a/1/index.html?edition_number=3&blockchain=bitmark'),
        ('https://cdn.feralfileassets.com/previews/a/1/_unique-previews/0/', 'https://cdn.artworks.feralfile.io/previews/a/1/_unique-previews/0/index.html'),
        ('https://cdn.feralfileassets.com/previews/a/1/#x', 'https://cdn.artworks.feralfile.io/previews/a/1/index.html#x'),
        ('https://cdn.feralfileassets.com/previews/a/1/nft.html?next=/', 'https://cdn.artworks.feralfile.io/previews/a/1/nft.html?next=/'),
        ('https://cdn.feralfileassets.com/previews/a/1/index.html?clip_directory_url=https://cdn.feralfileassets.com/misc/clips', 'https://cdn.artworks.feralfile.io/previews/a/1/index.html?clip_directory_url=https://cdn.artworks.feralfile.io/misc/clips'),
        ('https://cdn.artworks.feralfile.io/previews/a/1/index.html?e=1', 'https://cdn.artworks.feralfile.io/previews/a/1/index.html?e=1'),
        ('https://cdn.feralfileassets.com/', 'https://cdn.artworks.feralfile.io/'),
        ('https://example.com/previews/a/', 'https://example.com/previews/a/')
    ) AS t(input, expected) LOOP
        IF migration_032_ff_cdn_move(v_case.input) IS DISTINCT FROM v_case.expected THEN
            RAISE EXCEPTION 'migration_032: rewrite mismatch for %: got %, want %',
                v_case.input, migration_032_ff_cdn_move(v_case.input), v_case.expected;
        END IF;
    END LOOP;
END $$;

-- 1. Render probes (keyed by URL hash, independent of tokens).
DELETE FROM media_render_probes p
WHERE p.media_url LIKE '%cdn.feralfileassets.com/%'
  AND EXISTS (SELECT 1 FROM media_render_probes n
              WHERE n.media_url_hash = md5(migration_032_ff_cdn_move(p.media_url)));

UPDATE media_render_probes
SET media_url            = migration_032_ff_cdn_move(media_url),
    media_url_hash       = md5(migration_032_ff_cdn_move(media_url)),
    next_check_at        = CASE WHEN verdict <> 'rendered_ok' THEN now() ELSE next_check_at END,
    consecutive_failures = CASE WHEN last_error ~ '(feralfileassets|resolution failed)' THEN 0
                                ELSE consecutive_failures END
WHERE media_url LIKE '%cdn.feralfileassets.com/%';

-- 2. Cloudflare media assets. A row whose new URL already has an asset keeps its old URL;
--    it is unreachable either way and a newer asset exists.
UPDATE media_assets a
SET source_url      = migration_032_ff_cdn_move(a.source_url),
    source_url_hash = md5(migration_032_ff_cdn_move(a.source_url))
WHERE a.source_url LIKE '%cdn.feralfileassets.com/%'
  AND NOT EXISTS (SELECT 1 FROM media_assets n
                  WHERE n.provider = a.provider
                    AND n.source_url_hash = md5(migration_032_ff_cdn_move(a.source_url)));

-- 3. Work list of affected tokens (one sequential scan per table).
DROP TABLE IF EXISTS pg_temp.migration_032_tokens;
CREATE TEMP TABLE migration_032_tokens AS
    SELECT token_id FROM enrichment_sources
    WHERE image_url LIKE '%cdn.feralfileassets.com/%' OR animation_url LIKE '%cdn.feralfileassets.com/%'
    UNION
    SELECT token_id FROM token_metadata
    WHERE image_url LIKE '%cdn.feralfileassets.com/%' OR animation_url LIKE '%cdn.feralfileassets.com/%'
    UNION
    SELECT token_id FROM token_media_health
    WHERE media_url LIKE '%cdn.feralfileassets.com/%';
CREATE UNIQUE INDEX ON migration_032_tokens (token_id);
ANALYZE migration_032_tokens;

CREATE OR REPLACE PROCEDURE migration_032_move_tokens(p_batch_size int DEFAULT 500)
LANGUAGE plpgsql AS $$
DECLARE
    v_last     bigint := 0;
    v_ids      bigint[];
    v_enriched bigint;
    v_meta     bigint;
    v_health   bigint;
    v_total_e  bigint := 0;
    v_total_m  bigint := 0;
    v_total_h  bigint := 0;
BEGIN
    IF p_batch_size < 1 THEN
        RAISE EXCEPTION 'p_batch_size must be >= 1';
    END IF;

    LOOP
        SELECT array_agg(token_id ORDER BY token_id) INTO v_ids
        FROM (SELECT token_id FROM migration_032_tokens
              WHERE token_id > v_last ORDER BY token_id LIMIT p_batch_size) s;
        EXIT WHEN v_ids IS NULL;
        v_last := v_ids[array_length(v_ids, 1)];

        -- Enrichment: rewrite + one 'enrichment_updated' event per changed token.
        WITH src AS (
            SELECT token_id, vendor, image_url AS oi, animation_url AS oa,
                   migration_032_ff_cdn_move(image_url) AS ni,
                   migration_032_ff_cdn_move(animation_url) AS na
            FROM enrichment_sources
            WHERE token_id = ANY (v_ids)
              AND (image_url LIKE '%cdn.feralfileassets.com/%' OR animation_url LIKE '%cdn.feralfileassets.com/%')
            FOR UPDATE
        ), upd AS (
            UPDATE enrichment_sources e
            SET image_url          = s.ni,
                image_url_hash     = CASE WHEN s.ni IS NULL OR s.ni = '' THEN NULL ELSE md5(s.ni) END,
                animation_url      = s.na,
                animation_url_hash = CASE WHEN s.na IS NULL OR s.na = '' THEN NULL ELSE md5(s.na) END
            FROM src s
            WHERE e.token_id = s.token_id
              AND (s.oi IS DISTINCT FROM s.ni OR s.oa IS DISTINCT FROM s.na)
            RETURNING e.token_id
        )
        INSERT INTO token_events (token_id, event_type, owner_address, occurred_at, metadata, created_at)
        SELECT s.token_id, 'enrichment_updated', NULL, clock_timestamp(),
               jsonb_build_object(
                   'vendor', s.vendor::text,
                   'changed_fields', to_jsonb(array_remove(ARRAY[
                       CASE WHEN s.oi IS DISTINCT FROM s.ni THEN 'image_url' END,
                       CASE WHEN s.oa IS DISTINCT FROM s.na THEN 'animation_url' END], NULL))),
               clock_timestamp()
        FROM src s JOIN upd u USING (token_id);
        GET DIAGNOSTICS v_enriched = ROW_COUNT;

        -- Token metadata: rewrite + one 'metadata_updated' event per changed token.
        WITH src AS (
            SELECT token_id, image_url AS oi, animation_url AS oa,
                   migration_032_ff_cdn_move(image_url) AS ni,
                   migration_032_ff_cdn_move(animation_url) AS na
            FROM token_metadata
            WHERE token_id = ANY (v_ids)
              AND (image_url LIKE '%cdn.feralfileassets.com/%' OR animation_url LIKE '%cdn.feralfileassets.com/%')
            FOR UPDATE
        ), upd AS (
            UPDATE token_metadata m
            SET image_url          = s.ni,
                image_url_hash     = CASE WHEN s.ni IS NULL OR s.ni = '' THEN NULL ELSE md5(s.ni) END,
                animation_url      = s.na,
                animation_url_hash = CASE WHEN s.na IS NULL OR s.na = '' THEN NULL ELSE md5(s.na) END
            FROM src s
            WHERE m.token_id = s.token_id
              AND (s.oi IS DISTINCT FROM s.ni OR s.oa IS DISTINCT FROM s.na)
            RETURNING m.token_id
        )
        INSERT INTO token_events (token_id, event_type, owner_address, occurred_at, metadata, created_at)
        SELECT s.token_id, 'metadata_updated', NULL, clock_timestamp(),
               jsonb_build_object(
                   'changed_fields', to_jsonb(array_remove(ARRAY[
                       CASE WHEN s.oi IS DISTINCT FROM s.ni THEN 'image_url' END,
                       CASE WHEN s.oa IS DISTINCT FROM s.na THEN 'animation_url' END], NULL))),
               clock_timestamp()
        FROM src s JOIN upd u USING (token_id);
        GET DIAGNOSTICS v_meta = ROW_COUNT;

        -- Media health: drop an old row whose moved twin already exists (unique key),
        -- then move the rest in place.
        DELETE FROM token_media_health h
        WHERE h.token_id = ANY (v_ids)
          AND h.media_url LIKE '%cdn.feralfileassets.com/%'
          AND EXISTS (SELECT 1 FROM token_media_health n
                      WHERE n.token_id = h.token_id
                        AND n.media_source = h.media_source
                        AND n.media_url_hash = md5(migration_032_ff_cdn_move(h.media_url)));

        UPDATE token_media_health h
        SET media_url      = migration_032_ff_cdn_move(h.media_url),
            media_url_hash = md5(migration_032_ff_cdn_move(h.media_url)),
            health_status  = CASE WHEN d.dns_outage THEN 'unknown'::media_health_status ELSE h.health_status END,
            failure_reason = CASE WHEN d.dns_outage THEN NULL ELSE h.failure_reason END,
            last_error     = CASE WHEN d.dns_outage THEN NULL ELSE h.last_error END,
            last_checked_at = CASE WHEN d.dns_outage THEN 'epoch'::timestamptz ELSE h.last_checked_at END
        FROM (SELECT id,
                     (health_status = 'broken' AND failure_reason = 'dns'
                      AND last_error LIKE '%cdn.feralfileassets.com%') AS dns_outage
              FROM token_media_health
              WHERE token_id = ANY (v_ids) AND media_url LIKE '%cdn.feralfileassets.com/%') d
        WHERE h.id = d.id;
        GET DIAGNOSTICS v_health = ROW_COUNT;

        v_total_e := v_total_e + v_enriched;
        v_total_m := v_total_m + v_meta;
        v_total_h := v_total_h + v_health;
        RAISE NOTICE 'migration_032: through token_id % | enrichment % | metadata % | health % (totals % / % / %)',
            v_last, v_enriched, v_meta, v_health, v_total_e, v_total_m, v_total_h;
        COMMIT;
    END LOOP;
END $$;

CALL migration_032_move_tokens();

DROP PROCEDURE migration_032_move_tokens(int);
DROP TABLE migration_032_tokens;

-- Verification: every count should be 0.
SELECT 'enrichment_sources' AS tbl, count(*) AS old_host_rows FROM enrichment_sources
    WHERE image_url LIKE '%cdn.feralfileassets.com/%' OR animation_url LIKE '%cdn.feralfileassets.com/%'
UNION ALL SELECT 'token_metadata', count(*) FROM token_metadata
    WHERE image_url LIKE '%cdn.feralfileassets.com/%' OR animation_url LIKE '%cdn.feralfileassets.com/%'
UNION ALL SELECT 'token_media_health', count(*) FROM token_media_health
    WHERE media_url LIKE '%cdn.feralfileassets.com/%'
UNION ALL SELECT 'media_render_probes', count(*) FROM media_render_probes
    WHERE media_url LIKE '%cdn.feralfileassets.com/%'
UNION ALL SELECT 'media_assets (conflicts left on purpose)', count(*) FROM media_assets
    WHERE source_url LIKE '%cdn.feralfileassets.com/%';

DROP FUNCTION migration_032_ff_cdn_move(text);
