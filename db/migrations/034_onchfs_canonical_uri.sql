-- Migration 034: store onchfs works as onchfs:// URIs instead of fxhash's dead gateway.
--
-- Context (issue #173)
-- --------------------
-- fxhash shut down on 2026-08-17 and its onchfs gateway, https://onchfs.fxhash2.xyz, no
-- longer resolves. No other host serves onchfs. The works are intact on chain, and their
-- on-chain metadata names them as onchfs://<root>/?fxhash=… (artifactUri); the gateway URL
-- in our rows was produced by our resolver (Tezos, objkt) or supplied by vendors (fxhash's
-- animation_url, OpenSea's display_animation_url). The media health sweeper marked every
-- such row broken/dns, hiding ~1.7k tokens.
--
-- The application now stores onchfs:// as the canonical media URL and classifies a
-- well-formed one as healthy without any network call. This file rewrites rows already
-- stored. The rewrite is types.CanonicalOnChFSURI (internal/types/onchfs.go), mirrored
-- below by migration_034_onchfs(). Both must produce byte-identical strings because every
-- *_url_hash column is md5 of the exact URL; the self-check runs the Go test vectors.
--
-- What this file does (a reduced 032_feralfile_cdn_move.sql)
-- ----------------------------------------------------------
--   * media_render_probes: delete the gateway rows. onchfs is out of L1 scope (the render
--     query skips it), so there is nothing to move a probe to. None were gated on
--     2026-11-12.
--   * media_assets: move source_url + hash, one row per (provider, target), so Cloudflare
--     copies of single-file roots stay linked to the display URL.
--   * Then per batch of 100 tokens, in one transaction each:
--       - enrichment_sources / token_metadata: rewrite URL + hash and insert
--         'enrichment_updated' / 'metadata_updated' token_events, as 032 does, so clients
--         syncing changes refetch the token.
--       - token_media_health: move URL + hash in place under lockURLGate on every target
--         hash, keeping one row per (token, source, target). Every moved row becomes
--         'unknown' and is backdated, so the sweeper re-checks it first; the check is
--         local, the row turns healthy, and the sweeper recomputes is_viewable and emits
--         viewability_changed. A render gate on the old URL does not travel: no gate can
--         exist on an onchfs:// URL.
--   Raw payloads (vendor_json, origin_json, latest_json) are left verbatim.
--
-- Deploy ordering (REQUIRED)
-- --------------------------
-- Run this ONLY AFTER the application version containing types.CanonicalOnChFSURI is live
-- and no process on the previous version remains. Older code rewrites onchfs:// back to
-- the dead gateway on every re-index and would revert these rows. See "Migration 034" in
-- DEVELOPMENT.md.
--
-- Running
-- -------
-- This file does its own transaction control (COMMIT per batch). It must run OUTSIDE an
-- explicit BEGIN, in one psql session (the work list is a temp table):
--
--   psql ... -v ON_ERROR_STOP=1 -f db/migrations/034_onchfs_canonical_uri.sql
--
-- Workers can keep running. New code never writes a gateway URL, so the only races are the
-- ones 032 documents: CreateMediaAsset inserting the target key (the move retries), and a
-- render job still running against a gateway URL re-creating its probe row (the final
-- pass deletes it).
--
-- Sizing (2026-11-12): 1,683 token_metadata rows, 465 enrichment rows (456 opensea, 9
-- objkt), 2,148 health rows over 1,706 tokens, 12 render probes, 9 media assets. Expect
-- ~2.1k token_events.
--
-- Idempotency: every statement selects only rows still holding a gateway URL, so a re-run
-- touches nothing already moved and emits no duplicate events. Fresh installs: no-op.

\set ON_ERROR_STOP on

-- Gateway predicate and rewrite; must match types.CanonicalOnChFSURI exactly (same hosts,
-- 64-hex root, everything after the root kept verbatim).
CREATE OR REPLACE FUNCTION migration_034_is_gateway(u text) RETURNS boolean
LANGUAGE sql IMMUTABLE PARALLEL SAFE AS $$
    SELECT u ~ '^https?://[a-zA-Z0-9._-]*(?:onchfs|fxhash2?\.xyz)[a-zA-Z0-9._-]*(?::[0-9]+)?/[0-9a-fA-F]{64}(?:[/?].*)?$'
$$;

CREATE OR REPLACE FUNCTION migration_034_onchfs(u text) RETURNS text
LANGUAGE sql IMMUTABLE PARALLEL SAFE AS $$
    SELECT CASE
        WHEN migration_034_is_gateway(u)
            THEN regexp_replace(u, '^https?://[a-zA-Z0-9._-]*(?:onchfs|fxhash2?\.xyz)[a-zA-Z0-9._-]*(?::[0-9]+)?/([0-9a-fA-F]{64}(?:[/?].*)?)$', 'onchfs://\1')
        ELSE u
    END
$$;

-- Self-check against the Go test vectors (TestCanonicalOnChFSURI) before touching data.
DO $$
DECLARE
    v_case record;
    v_root text := 'b12f18fcd51a21cc3ace5090fa808c8d4e4552d9e2239a3886b89f8ea12ff685';
BEGIN
    FOR v_case IN SELECT * FROM (VALUES
        ('https://onchfs.fxhash2.xyz/' || v_root || '/?fxhash=0xb56f&fxiteration=44&fxminter=0x94E3&fxchain=ETHEREUM',
         'onchfs://' || v_root || '/?fxhash=0xb56f&fxiteration=44&fxminter=0x94E3&fxchain=ETHEREUM'),
        ('https://onchfs.fxhash2.xyz/' || v_root, 'onchfs://' || v_root),
        ('https://onchfs.fxhash2.xyz/' || v_root || '/', 'onchfs://' || v_root || '/'),
        ('https://onchfs.fxhash2.xyz/' || v_root || '?fxhash=oo1', 'onchfs://' || v_root || '?fxhash=oo1'),
        ('https://onchfs.fxhash2.xyz/' || v_root || '/index.html', 'onchfs://' || v_root || '/index.html'),
        ('https://onchfs.fxhash2.xyz/' || upper(v_root), 'onchfs://' || upper(v_root)),
        ('http://gateway.fxhash.xyz/' || v_root || '/', 'onchfs://' || v_root || '/'),
        ('https://onchfs.example.com:8443/' || v_root, 'onchfs://' || v_root),
        ('onchfs://' || v_root || '/?fxhash=oo1', 'onchfs://' || v_root || '/?fxhash=oo1'),
        ('https://onchfs.fxhash2.xyz/b12f18fc', 'https://onchfs.fxhash2.xyz/b12f18fc'),
        ('https://onchfs.fxhash2.xyz/' || v_root || '#x', 'https://onchfs.fxhash2.xyz/' || v_root || '#x'),
        ('https://example.com/' || v_root, 'https://example.com/' || v_root),
        ('https://ipfs.io/ipfs/QmP1u8KrSShWcF7iCMRhXFnJUNgMKqFrQ7qZMZfD9tjD3c', 'https://ipfs.io/ipfs/QmP1u8KrSShWcF7iCMRhXFnJUNgMKqFrQ7qZMZfD9tjD3c'),
        ('', '')
    ) AS t(input, expected) LOOP
        IF migration_034_onchfs(v_case.input) IS DISTINCT FROM v_case.expected THEN
            RAISE EXCEPTION 'migration_034: rewrite mismatch for %: got %, want %',
                v_case.input, migration_034_onchfs(v_case.input), v_case.expected;
        END IF;
    END LOOP;
END $$;

-- 1. Render probes: onchfs is out of L1 scope, so gateway probe rows are dropped.
DELETE FROM media_render_probes WHERE migration_034_is_gateway(media_url);

-- 2. Cloudflare media assets. One row per (provider, target) moves (lowest id); a row
--    whose target already has an asset, or that lost that tie, keeps its gateway URL.
--    CreateMediaAsset can insert a target while this runs; the update retries in a fresh
--    subtransaction, whose NOT EXISTS then sees that row.
DO $$
DECLARE
    v_retries int := 0;
BEGIN
    LOOP
        BEGIN
            UPDATE media_assets a
            SET source_url      = migration_034_onchfs(a.source_url),
                source_url_hash = md5(migration_034_onchfs(a.source_url))
            FROM (SELECT id, row_number() OVER (PARTITION BY provider, md5(migration_034_onchfs(source_url))
                                                ORDER BY id) AS rn
                  FROM media_assets
                  WHERE migration_034_is_gateway(source_url)) w
            WHERE a.id = w.id
              AND w.rn = 1
              AND NOT EXISTS (SELECT 1 FROM media_assets n
                              WHERE n.provider = a.provider
                                AND n.source_url_hash = md5(migration_034_onchfs(a.source_url)));
            EXIT;
        EXCEPTION WHEN unique_violation THEN
            v_retries := v_retries + 1;
            IF v_retries > 20 THEN
                RAISE;
            END IF;
            RAISE NOTICE 'migration_034: media asset key raced a concurrent writer, retry %', v_retries;
        END;
    END LOOP;
END $$;

-- 3. Work list of affected tokens (one sequential scan per table).
DROP TABLE IF EXISTS pg_temp.migration_034_tokens;
CREATE TEMP TABLE migration_034_tokens AS
    SELECT token_id FROM enrichment_sources
    WHERE migration_034_is_gateway(image_url) OR migration_034_is_gateway(animation_url)
    UNION
    SELECT token_id FROM token_metadata
    WHERE migration_034_is_gateway(image_url) OR migration_034_is_gateway(animation_url)
    UNION
    SELECT token_id FROM token_media_health
    WHERE migration_034_is_gateway(media_url);
CREATE UNIQUE INDEX ON migration_034_tokens (token_id);
ANALYZE migration_034_tokens;

CREATE OR REPLACE PROCEDURE migration_034_move_tokens(p_batch_size int DEFAULT 100)
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
BEGIN
    IF p_batch_size < 1 THEN
        RAISE EXCEPTION 'p_batch_size must be >= 1';
    END IF;

    LOOP
        SELECT array_agg(token_id ORDER BY token_id) INTO v_ids
        FROM (SELECT token_id FROM migration_034_tokens
              WHERE token_id > v_last ORDER BY token_id LIMIT p_batch_size) s;
        EXIT WHEN v_ids IS NULL;
        v_last := v_ids[array_length(v_ids, 1)];

        -- Enrichment: rewrite + one 'enrichment_updated' event per changed token.
        WITH src AS (
            SELECT token_id, vendor, image_url AS oi, animation_url AS oa,
                   migration_034_onchfs(image_url) AS ni,
                   migration_034_onchfs(animation_url) AS na
            FROM enrichment_sources
            WHERE token_id = ANY (v_ids)
              AND (migration_034_is_gateway(image_url) OR migration_034_is_gateway(animation_url))
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
                   migration_034_onchfs(image_url) AS ni,
                   migration_034_onchfs(animation_url) AS na
            FROM token_metadata
            WHERE token_id = ANY (v_ids)
              AND (migration_034_is_gateway(image_url) OR migration_034_is_gateway(animation_url))
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

        -- Media health. Serialize against writers on every target URL (lockURLGate), in
        -- hash order to avoid deadlocks.
        SELECT array_agg(t ORDER BY t) INTO v_targets
        FROM (SELECT DISTINCT md5(migration_034_onchfs(media_url)) AS t
              FROM token_media_health
              WHERE token_id = ANY (v_ids) AND migration_034_is_gateway(media_url)) s;
        IF v_targets IS NOT NULL THEN
            FOREACH v_target IN ARRAY v_targets LOOP
                PERFORM pg_advisory_xact_lock(hashtext(v_target));
            END LOOP;
        END IF;
        v_targets := NULL;

        -- Keep one row per (token, source, target) for the unique key: an existing
        -- onchfs:// row wins, then the lowest id among gateway rows that converge.
        DELETE FROM token_media_health h
        USING (
            SELECT id, row_number() OVER (PARTITION BY token_id, media_source, target
                                          ORDER BY is_target DESC, id) AS rn
            FROM (SELECT id, token_id, media_source,
                         md5(migration_034_onchfs(media_url)) AS target, false AS is_target
                  FROM token_media_health
                  WHERE token_id = ANY (v_ids) AND migration_034_is_gateway(media_url)
                  UNION ALL
                  SELECT id, token_id, media_source, media_url_hash, true
                  FROM token_media_health
                  WHERE token_id = ANY (v_ids) AND NOT migration_034_is_gateway(media_url)) c
        ) d
        WHERE h.id = d.id AND d.rn > 1;

        -- Every moved row is re-checked: its old verdict described a dead host.
        UPDATE token_media_health h
        SET media_url       = migration_034_onchfs(h.media_url),
            media_url_hash  = md5(migration_034_onchfs(h.media_url)),
            health_status   = 'unknown',
            failure_reason  = NULL,
            last_error      = NULL,
            last_checked_at = 'epoch'::timestamptz
        WHERE h.token_id = ANY (v_ids) AND migration_034_is_gateway(h.media_url);
        GET DIAGNOSTICS v_health = ROW_COUNT;

        v_total_e := v_total_e + v_enriched;
        v_total_m := v_total_m + v_meta;
        v_total_h := v_total_h + v_health;
        RAISE NOTICE 'migration_034: through token_id % | enrichment % | metadata % | health % (totals % / % / %)',
            v_last, v_enriched, v_meta, v_health, v_total_e, v_total_m, v_total_h;
        COMMIT;
    END LOOP;
END $$;

CALL migration_034_move_tokens();
DROP PROCEDURE migration_034_move_tokens(int);
DROP TABLE migration_034_tokens;

-- 4. Render probes, final pass: drop gateway rows written by in-flight render jobs.
DELETE FROM media_render_probes WHERE migration_034_is_gateway(media_url);

-- Verification: every count should be 0.
SELECT 'enrichment_sources' AS tbl, count(*) AS gateway_rows FROM enrichment_sources
    WHERE migration_034_is_gateway(image_url) OR migration_034_is_gateway(animation_url)
UNION ALL SELECT 'token_metadata', count(*) FROM token_metadata
    WHERE migration_034_is_gateway(image_url) OR migration_034_is_gateway(animation_url)
UNION ALL SELECT 'token_media_health', count(*) FROM token_media_health
    WHERE migration_034_is_gateway(media_url)
UNION ALL SELECT 'media_render_probes', count(*) FROM media_render_probes
    WHERE migration_034_is_gateway(media_url)
UNION ALL SELECT 'media_assets (conflicts left on purpose)', count(*) FROM media_assets
    WHERE migration_034_is_gateway(source_url);

DROP FUNCTION migration_034_onchfs(text);
DROP FUNCTION migration_034_is_gateway(text);
