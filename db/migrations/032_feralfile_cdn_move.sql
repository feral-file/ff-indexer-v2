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
-- Several old URLs can converge on one new URL (".../x/" and ".../x/index.html"), and
-- post-deploy indexing may already have created the new-URL row. Every step keeps exactly
-- one row per target before moving, so no unique key can abort the run. None of these
-- collisions existed in production on 2026-09-25.
--
--   * media_render_probes: move url + hash (the PK) in place. Among converging rows an
--     existing new-host row wins, then a gated row, then the latest capture. Verdicts
--     that are not rendered_ok become due now. consecutive_failures resets when the old
--     host's outage caused the failure. A gate that lands on a new-host URL is applied to
--     that URL's existing health rows, as AcquireRenderGate does.
--   * media_assets: move source_url + hash, one old row per (provider, target). The
--     Cloudflare copies stay valid and the API finds assets by md5(display URL), so no
--     re-upload is needed.
--   * Then per batch of 100 tokens, in one transaction each:
--       - enrichment_sources: rewrite image/animation URL + hash, and insert
--         token_events 'enrichment_updated' {"vendor", "changed_fields"}, like
--         UpsertEnrichmentSource.
--       - token_metadata: the same, with 'metadata_updated' {"changed_fields"}, like
--         UpsertTokenMetadata.
--       - token_media_health: move url + hash in place, like UpdateMediaURLAndPropagate.
--         The API serves media_url from these rows, so it has to change in the same
--         transaction as the event: a client that refetches on the event then sees the
--         new URL. Rows broken by the old host's DNS failure become 'unknown' and are
--         backdated so the sweeper re-probes them first, and so are rows held by a
--         render gate on the old URL (a gate belongs to its URL). Other statuses are kept.
--         The batch holds pg_advisory_xact_lock on every target URL hash (lockURLGate),
--         then applies any active render gate on those URLs to the moved rows, with
--         AcquireRenderGate's predicate and values, as syncSingleMediaURL does for a new
--         URL. A URL whose rows it gates becomes due for a re-render, so the application
--         recomputes viewability for its tokens on that probe.
--   Raw payloads (vendor_json, origin_json, latest_json) are left verbatim, as in code.
--
-- Deploy ordering (REQUIRED)
-- --------------------------
-- Run this ONLY AFTER the application version containing types.MigrateFeralFileCDN is
-- live and no process on the previous version remains. Older code rebuilds old-host URLs
-- from the Feral File API and on-chain metadata and would revert these rows (emitting
-- another round of events). This is an exception to the default "migrations before
-- deploy" sequence; see "Migration 032" in DEVELOPMENT.md.
--
-- Running
-- -------
-- This file does its own transaction control (COMMIT per batch). It must run OUTSIDE an
-- explicit BEGIN, in one psql session (the work list is a temp table):
--
--   psql ... -v ON_ERROR_STOP=1 -f db/migrations/032_feralfile_cdn_move.sql
--
-- Workers can keep running. Races resolve without pausing them:
--   * An enrichment upsert that read the old row before a batch commits may fail its
--     health-row insert on the unique key, then retry and find nothing to change.
--   * A render job that started against an old URL may re-create its old-host probe row
--     and gate the old URL's health rows mid-run. The batch releases those rows when it
--     moves them and re-applies only the new URL's gate. The final render-probe pass
--     deletes the old-host probe rows. A job finishing after the run writes an old-host
--     probe row that no health row references, so it is never scheduled again.
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

-- Render-gate reason for a URL hash, exactly as activeRenderGate derives it: a sibling
-- health row's render_% reason when one exists, otherwise mapped from the verdict.
CREATE OR REPLACE FUNCTION migration_032_gate_reason(p_hash text) RETURNS text
LANGUAGE sql STABLE AS $$
    SELECT COALESCE(
        (SELECT h.failure_reason FROM token_media_health h
         WHERE h.media_url_hash = p_hash AND h.failure_reason LIKE 'render_%' LIMIT 1),
        (SELECT CASE p.verdict
                    WHEN 'known_bad_fingerprint' THEN 'render_known_bad'
                    WHEN 'stalled' THEN 'render_stalled'
                    ELSE 'render_blank'
                END
         FROM media_render_probes p WHERE p.media_url_hash = p_hash))
$$;

-- Applies any active render gate on the given URL hashes to their health rows, with the
-- same predicate and values as AcquireRenderGate, and makes those gated probes due so the
-- prober re-renders them and the application recomputes viewability for their tokens.
-- Callers hold pg_advisory_xact_lock(hashtext(hash)) for every hash (lockURLGate).
CREATE OR REPLACE FUNCTION migration_032_apply_gates(p_hashes text[]) RETURNS bigint
LANGUAGE plpgsql AS $$
DECLARE
    v_gated bigint;
BEGIN
    WITH gate AS (
        SELECT p.media_url_hash, p.last_error, migration_032_gate_reason(p.media_url_hash) AS reason
        FROM media_render_probes p
        WHERE p.media_url_hash = ANY (p_hashes) AND p.health_gated
        FOR UPDATE
    ), applied AS (
        UPDATE token_media_health h
        SET health_status   = 'broken',
            failure_reason  = g.reason,
            last_error      = g.last_error,
            last_checked_at = now()
        FROM gate g
        WHERE h.media_url_hash = g.media_url_hash
          AND (h.health_status IN ('healthy', 'unknown') OR h.failure_reason LIKE 'render_%')
        RETURNING h.media_url_hash
    )
    UPDATE media_render_probes p
    SET next_check_at = now()
    WHERE p.media_url_hash IN (SELECT DISTINCT media_url_hash FROM applied)
      AND p.next_check_at > now();
    SELECT count(*) INTO v_gated FROM media_render_probes p
    WHERE p.media_url_hash = ANY (p_hashes) AND p.health_gated;
    RETURN v_gated;
END $$;

-- Render probes (keyed by URL hash, independent of tokens). Called twice: before the
-- token batches (p_final = false) and after them (p_final = true).
--
-- Several old URLs can converge on one target (".../x/" and ".../x/index.html"), and the
-- target may already exist when post-deploy indexing created it. Keep exactly one row per
-- target. An existing new-host row always wins: it is evidence about the URL actually
-- served, while an old-host row describes a host that no longer resolves. Among old-host
-- rows a gated row wins, so a held gate is never dropped, then the latest capture.
--
-- The final pass deletes every old-host row still present. The first pass moved all of
-- them, so any that remain were written during the run by a render job that started
-- against an old URL. They measure a dead host, and no health row references them.
--
-- A gate that lands on a new-host URL must reach health rows that post-deploy indexing
-- already created there (inserted ungated). Locks are taken in hash order, as
-- lockChangedURLGates does.
CREATE OR REPLACE PROCEDURE migration_032_move_probes(p_final boolean)
LANGUAGE plpgsql AS $$
DECLARE
    v_hash  text;
    v_gated text[];
    v_rows  bigint;
BEGIN
    IF p_final THEN
        DELETE FROM media_render_probes WHERE media_url LIKE '%cdn.feralfileassets.com/%';
        GET DIAGNOSTICS v_rows = ROW_COUNT;
        RAISE NOTICE 'migration_032: final pass deleted % old-host render probe rows written during the run', v_rows;
    ELSE
        WITH cand AS (
            SELECT media_url_hash AS h, md5(migration_032_ff_cdn_move(media_url)) AS target,
                   health_gated, false AS is_target, captured_at
            FROM media_render_probes
            WHERE media_url LIKE '%cdn.feralfileassets.com/%'
            UNION ALL
            SELECT n.media_url_hash, n.media_url_hash, n.health_gated, true, n.captured_at
            FROM media_render_probes n
            WHERE n.media_url_hash IN (SELECT md5(migration_032_ff_cdn_move(media_url))
                                       FROM media_render_probes
                                       WHERE media_url LIKE '%cdn.feralfileassets.com/%')
        ), ranked AS (
            SELECT h, row_number() OVER (PARTITION BY target
                                         ORDER BY is_target DESC, health_gated DESC,
                                                  captured_at DESC NULLS LAST, h) AS rn
            FROM cand
        )
        DELETE FROM media_render_probes p
        USING ranked r
        WHERE p.media_url_hash = r.h AND r.rn > 1;

        UPDATE media_render_probes
        SET media_url            = migration_032_ff_cdn_move(media_url),
            media_url_hash       = md5(migration_032_ff_cdn_move(media_url)),
            next_check_at        = CASE WHEN verdict <> 'rendered_ok' THEN now() ELSE next_check_at END,
            consecutive_failures = CASE WHEN last_error ~ '(feralfileassets|resolution failed)' THEN 0
                                        ELSE consecutive_failures END
        WHERE media_url LIKE '%cdn.feralfileassets.com/%';
    END IF;
    COMMIT;

    SELECT array_agg(media_url_hash ORDER BY media_url_hash) INTO v_gated
    FROM media_render_probes
    WHERE health_gated AND media_url LIKE 'https://cdn.artworks.feralfile.io/%';
    IF v_gated IS NOT NULL THEN
        FOREACH v_hash IN ARRAY v_gated LOOP
            PERFORM pg_advisory_xact_lock(hashtext(v_hash));
        END LOOP;
        PERFORM migration_032_apply_gates(v_gated);
    END IF;
    COMMIT;
END $$;

-- 1. Render probes, first pass.
CALL migration_032_move_probes(false);

-- 2. Cloudflare media assets. One old row per (provider, target) moves (lowest id); a
--    row whose target already has an asset, or that lost that tie, keeps its old URL.
--    It is unreachable either way and an asset for the new URL exists.
UPDATE media_assets a
SET source_url      = migration_032_ff_cdn_move(a.source_url),
    source_url_hash = md5(migration_032_ff_cdn_move(a.source_url))
FROM (SELECT id, row_number() OVER (PARTITION BY provider, md5(migration_032_ff_cdn_move(source_url))
                                    ORDER BY id) AS rn
      FROM media_assets
      WHERE source_url LIKE '%cdn.feralfileassets.com/%') w
WHERE a.id = w.id
  AND w.rn = 1
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

CREATE OR REPLACE PROCEDURE migration_032_move_tokens(p_batch_size int DEFAULT 100)
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
    v_targets  text[];
    v_target   text;
    v_gates    bigint := 0;
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

        -- Media health. Serialize against render-gate transitions on every target URL
        -- (lockURLGate), in hash order to avoid deadlocks.
        SELECT array_agg(t ORDER BY t) INTO v_targets
        FROM (SELECT DISTINCT md5(migration_032_ff_cdn_move(media_url)) AS t
              FROM token_media_health
              WHERE token_id = ANY (v_ids) AND media_url LIKE '%cdn.feralfileassets.com/%') s;
        IF v_targets IS NOT NULL THEN
            FOREACH v_target IN ARRAY v_targets LOOP
                PERFORM pg_advisory_xact_lock(hashtext(v_target));
            END LOOP;
        END IF;

        -- Keep one row per (token, source, target) for the unique key: an existing
        -- new-host row wins, then the lowest id among old rows that converge.
        DELETE FROM token_media_health h
        USING (
            SELECT id, row_number() OVER (PARTITION BY token_id, media_source, target
                                          ORDER BY is_target DESC, id) AS rn
            FROM (SELECT id, token_id, media_source,
                         md5(migration_032_ff_cdn_move(media_url)) AS target, false AS is_target
                  FROM token_media_health
                  WHERE token_id = ANY (v_ids) AND media_url LIKE '%cdn.feralfileassets.com/%'
                  UNION ALL
                  SELECT id, token_id, media_source, media_url_hash, true
                  FROM token_media_health
                  WHERE token_id = ANY (v_ids) AND media_url NOT LIKE '%cdn.feralfileassets.com/%') c
        ) d
        WHERE h.id = d.id AND d.rn > 1;

        UPDATE token_media_health h
        SET media_url       = migration_032_ff_cdn_move(h.media_url),
            media_url_hash  = md5(migration_032_ff_cdn_move(h.media_url)),
            health_status   = CASE WHEN d.reset THEN 'unknown'::media_health_status ELSE h.health_status END,
            failure_reason  = CASE WHEN d.reset THEN NULL ELSE h.failure_reason END,
            last_error      = CASE WHEN d.reset THEN NULL ELSE h.last_error END,
            last_checked_at = CASE WHEN d.reset THEN 'epoch'::timestamptz ELSE h.last_checked_at END
        FROM (SELECT id,
                     -- Broken only because the old host stopped resolving, or held by a
                     -- render gate on the OLD url. A gate belongs to a URL: the old one's
                     -- does not travel (it may even have been acquired mid-run by a render
                     -- job against the dead host). The row is released like
                     -- ReleaseRenderGate does, and migration_032_apply_gates below re-gates
                     -- it iff the NEW url holds a gate.
                     ((health_status = 'broken' AND failure_reason = 'dns'
                       AND last_error LIKE '%cdn.feralfileassets.com%')
                      OR failure_reason LIKE 'render_%') AS reset
              FROM token_media_health
              WHERE token_id = ANY (v_ids) AND media_url LIKE '%cdn.feralfileassets.com/%') d
        WHERE h.id = d.id;
        GET DIAGNOSTICS v_health = ROW_COUNT;

        -- Moved rows inherit an active render gate on their new URL, as
        -- syncSingleMediaURL and UpdateMediaURLAndPropagate do.
        IF v_targets IS NOT NULL THEN
            v_gates := v_gates + migration_032_apply_gates(v_targets);
        END IF;
        v_targets := NULL;

        v_total_e := v_total_e + v_enriched;
        v_total_m := v_total_m + v_meta;
        v_total_h := v_total_h + v_health;
        RAISE NOTICE 'migration_032: through token_id % | enrichment % | metadata % | health % (totals % / % / %, gated targets %)',
            v_last, v_enriched, v_meta, v_health, v_total_e, v_total_m, v_total_h, v_gates;
        COMMIT;
    END LOOP;
END $$;

CALL migration_032_move_tokens();

-- 4. Render probes, final pass: drop old-host rows written by in-flight render jobs.
CALL migration_032_move_probes(true);
DROP PROCEDURE migration_032_move_probes(boolean);

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

DROP FUNCTION migration_032_apply_gates(text[]);
DROP FUNCTION migration_032_gate_reason(text);
DROP FUNCTION migration_032_ff_cdn_move(text);
