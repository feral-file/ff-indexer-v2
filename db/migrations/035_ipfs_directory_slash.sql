-- Migration 035: give stored bare-CID-plus-query IPFS gateway URLs their directory slash.
--
-- Context (PR #178)
-- -----------------
-- Generative works address an iteration as ipfs://<cid>?<seed query> with no slash (fxhash
-- gentk v1/v2 ?fxhash=, Feral File ?edition_number=, and ?seed=, ?tokenHash=, ?payload=,
-- ?data=, ?hash=, ...). For a directory CID, filebase, raribleuserdata, versecontent and
-- Pinata answer that with a 301 to /ipfs/<cid>/ and DROP the query, so the work renders
-- without its seed: a blank frame or the wrong iteration, while every byte probe passes.
-- (ipfs.orbitor.dev and ipfs.feralfile.com keep the query on the redirect; their rows are
-- rewritten too, harmlessly.) PR #178 makes the application store <gw>/ipfs/<cid>/?<query>
-- instead. Rows stored before it keep the bare shape: a health check alone never heals
-- them, because the bare URL still answers 200 through the redirect. This file rewrites
-- them. Measured 2026-11-17 on one sampled row per (host, query key, column), 63 samples:
-- every slashed URL served the same content type as the bare one, files (?filename=, ?id=,
-- ?owner=) included.
--
-- The rewrite is types.NormalizeIPFSGatewayURL (internal/types/types.go), mirrored below
-- by migration_035_slash(). Both must produce byte-identical strings because every
-- *_url_hash column is md5 of the exact URL; the self-check runs the Go test vectors. The
-- SQL predicate is never looser than the Go one (a URL Go would leave alone is never
-- touched); it covers the path shape <gw>/ipfs/<cid>[?#]… and the CIDv1 subdomain shape
-- <cid>.ipfs.<host>[?#]…, and requires at least one character after the delimiter.
--
-- What this file does (034_onchfs_canonical_uri.sql with a different rewrite)
-- --------------------------------------------------------------------------
--   * media_render_probes: delete the bare rows. Their verdicts describe the redirected,
--     query-less render (typically the blank frame this fixes), so they must not carry
--     over; the slashed URL is probed fresh once it is L0-healthy again.
--   * media_assets: move source_url + hash to the slashed URL, one row per (provider,
--     target). A row whose target already has an asset keeps its bare URL; PR #178's
--     lookup still finds it through the bare alias (types.BareIPFSGatewayURL).
--   * Then per batch of 100 tokens, in one transaction each:
--       - enrichment_sources / token_metadata: rewrite URL + hash and insert
--         'enrichment_updated' / 'metadata_updated' token_events, so clients and devices
--         syncing changes refetch the token and pick up the playable URL.
--       - token_media_health: move URL + hash in place under lockURLGate on every target
--         hash, keeping one row per (token, source, target) (an existing slashed row from
--         a re-index wins). The L0 verdict and observations are KEPT: the slashed URL
--         serves the same bytes, and the API serves an animation URL only while its row
--         is 'healthy', so resetting to 'unknown' as 034 does would blank
--         display.animation_url on ~42k displaying works until the sweeper got round to
--         them (~20k rows/hour). A render gate on the BARE URL (failure_reason
--         'render_%') does not travel: it judged the query-less render, so that row
--         becomes 'unknown' and is backdated for a prompt re-check. A gate the SLASHED
--         URL already holds (L1 can gate it after #178 deploys, before this file reaches
--         a token still on the bare URL) does travel, as store.activeRenderGate requires:
--         the moved row becomes broken with that gate's reason. Those tokens, and any
--         whose bare row was dropped in favor of an existing slashed row (which may be
--         broken or gated), get is_viewable recomputed in SQL with their
--         'viewability_changed' token events and the token.viewability.changed webhook
--         job the sweeper would enqueue (a re-index would not: it emits only
--         token.indexing.*, which downstream search ignores).
--         None of either existed on 2026-11-17 (L1 runs in shadow mode).
--   Raw payloads (vendor_json, origin_json, latest_json) are left verbatim.
--
-- Deploy ordering (REQUIRED)
-- --------------------------
-- Run this ONLY AFTER the application version containing types.NormalizeIPFSGatewayURL
-- (PR #178) is live and no process on the previous version remains. Older code resolves
-- ipfs://<cid>?q back to the bare shape on every re-index and would revert these rows. See
-- "Migration 035" in DEVELOPMENT.md.
--
-- Running
-- -------
-- This file does its own transaction control (COMMIT per batch). It must run OUTSIDE an
-- explicit BEGIN, in one psql session (the work list is a temp table):
--
--   psql ... -v ON_ERROR_STOP=1 -v token_queue=<jobs.token_queue> \
--       -f db/migrations/035_ipfs_directory_slash.sql
--
-- token_queue is required and must be the deployment's jobs.token_queue (default
-- token_index): the viewability webhooks this file enqueues run on that queue.
--
-- Workers can keep running. New code never writes the bare shape. Each batch takes its
-- locks in UpdateMediaURLAndPropagate's order (URL gate locks, health rows, token_metadata,
-- enrichment_sources), so a concurrent gateway promotion cannot deadlock with it, and the
-- batch retries on any other lock conflict (deadlock, lock or serialization failure) up
-- to 10 times. The other races are the ones 034 documents: CreateMediaAsset inserting the
-- target key (the move retries), and a render job still running against a bare URL
-- re-creating its probe row (the final pass deletes it).
--
-- Sizing (2026-11-17): 44,563 token_metadata rows (42,287 animation, 2,340 image),
-- 1,868 enrichment rows (1,650 objkt, 218 opensea), 46,508 health rows over 44,887 tokens
-- (46,436 healthy), 17,854 render probes (none gated), 2,170 media assets; no subdomain
-- shapes. Expect ~46.4k token_events. 18,643 of the animation rows sit on a host that
-- drops the query (actually broken today); 23,644 on orbitor/feralfile (cosmetic).
-- Re-count before running (read-only, one sequential scan per table):
--
--   SELECT 'token_metadata', count(*) FROM token_metadata
--       WHERE image_url ~ '/ipfs/(Qm[1-9A-HJ-NP-Za-km-z]{44}|b[a-z2-7]{10,})[?#].'
--          OR animation_url ~ '/ipfs/(Qm[1-9A-HJ-NP-Za-km-z]{44}|b[a-z2-7]{10,})[?#].'
--   UNION ALL SELECT 'enrichment_sources', count(*) FROM enrichment_sources
--       WHERE image_url ~ '/ipfs/(Qm[1-9A-HJ-NP-Za-km-z]{44}|b[a-z2-7]{10,})[?#].'
--          OR animation_url ~ '/ipfs/(Qm[1-9A-HJ-NP-Za-km-z]{44}|b[a-z2-7]{10,})[?#].'
--   UNION ALL SELECT 'token_media_health', count(*) FROM token_media_health
--       WHERE media_url ~ '/ipfs/(Qm[1-9A-HJ-NP-Za-km-z]{44}|b[a-z2-7]{10,})[?#].';
--
-- Expect about one metadata_updated or enrichment_updated token_event per rewritten row.
--
-- Idempotency: every statement selects only rows still holding a bare URL, so a re-run
-- touches nothing already moved and emits no duplicate events. Fresh installs: no-op.

\set ON_ERROR_STOP on

-- Required: the queue the deployment's token worker polls (config jobs.token_queue; the
-- default is token_index). Viewability webhooks are enqueued on it; a wrong value leaves
-- them unclaimed.
\if :{?token_queue}
\else
DO $$ BEGIN RAISE EXCEPTION 'migration_035: pass -v token_queue=<jobs.token_queue>, e.g. -v token_queue=token_index'; END $$;
\endif

-- Bare predicate and rewrite; must match types.NormalizeIPFSGatewayURL exactly (same host,
-- the "/" inserted between the CID and its "?" or "#", everything else kept verbatim).
CREATE OR REPLACE FUNCTION migration_035_is_bare(u text) RETURNS boolean
LANGUAGE sql IMMUTABLE PARALLEL SAFE AS $$
    SELECT u ~ '^https?://[a-zA-Z0-9._-]+(?::[0-9]+)?/ipfs/(?:Qm[1-9A-HJ-NP-Za-km-z]{44}|b[a-z2-7]{10,})[?#].'
        OR u ~ '^https?://b[a-z2-7]{10,}\.ipfs\.[a-z0-9._-]+(?::[0-9]+)?[?#].'
$$;

CREATE OR REPLACE FUNCTION migration_035_slash(u text) RETURNS text
LANGUAGE sql IMMUTABLE PARALLEL SAFE AS $$
    SELECT CASE
        WHEN u ~ '^https?://[a-zA-Z0-9._-]+(?::[0-9]+)?/ipfs/(?:Qm[1-9A-HJ-NP-Za-km-z]{44}|b[a-z2-7]{10,})[?#].'
            THEN regexp_replace(u, '^(https?://[a-zA-Z0-9._-]+(?::[0-9]+)?/ipfs/(?:Qm[1-9A-HJ-NP-Za-km-z]{44}|b[a-z2-7]{10,}))([?#])', '\1/\2')
        WHEN u ~ '^https?://b[a-z2-7]{10,}\.ipfs\.[a-z0-9._-]+(?::[0-9]+)?[?#].'
            THEN regexp_replace(u, '^(https?://b[a-z2-7]{10,}\.ipfs\.[a-z0-9._-]+(?::[0-9]+)?)([?#])', '\1/\2')
        ELSE u
    END
$$;

-- ULID (Crockford base32: 48-bit millisecond time + 80 random bits), the webhook event_id
-- format the application uses (ulid.MustNewDefault).
CREATE OR REPLACE FUNCTION migration_035_ulid() RETURNS text
LANGUAGE plpgsql VOLATILE AS $$
DECLARE
    v_alphabet constant text := '0123456789ABCDEFGHJKMNPQRSTVWXYZ';
    v_ms       bigint := floor(extract(epoch FROM clock_timestamp()) * 1000);
    v_out      text := '';
BEGIN
    FOR i IN REVERSE 9..0 LOOP
        v_out := v_out || substr(v_alphabet, ((v_ms >> (i * 5)) & 31)::int + 1, 1);
    END LOOP;
    FOR i IN 1..16 LOOP
        v_out := v_out || substr(v_alphabet, floor(random() * 32)::int + 1, 1);
    END LOOP;
    RETURN v_out;
END $$;

-- Self-check against the Go test vectors (TestNormalizeIPFSGatewayURL, TestNormalizeIPFSRef)
-- before touching data.
DO $$
DECLARE
    v_case record;
    v_cid  text := 'QmUuKiWb5BBK5dERnfZBRebDrtxxkvwFrNYZGs7uwbwSon';
    v_sub  text := 'bafybeigdyrzt5sfp7udm7hu76uh7y26nf3efuylqabf3oclgtqy55fbzdi';
BEGIN
    FOR v_case IN SELECT * FROM (VALUES
        ('https://ipfs.raribleuserdata.com/ipfs/' || v_cid || '?fxhash=ooCaGyZkD8Yyw6zXM82kRriW6uUmWRYCSBxavnHtvyagq1EFeKu',
         'https://ipfs.raribleuserdata.com/ipfs/' || v_cid || '/?fxhash=ooCaGyZkD8Yyw6zXM82kRriW6uUmWRYCSBxavnHtvyagq1EFeKu'),
        ('https://ipfs.filebase.io/ipfs/' || v_cid || '?fxhash=oo123#view',
         'https://ipfs.filebase.io/ipfs/' || v_cid || '/?fxhash=oo123#view'),
        ('https://ipfs.filebase.io/ipfs/' || v_cid || '#seed=1',
         'https://ipfs.filebase.io/ipfs/' || v_cid || '/#seed=1'),
        ('http://localhost:8080/ipfs/' || v_sub || '?fxhash=oo1',
         'http://localhost:8080/ipfs/' || v_sub || '/?fxhash=oo1'),
        ('https://' || v_sub || '.ipfs.filebase.io?fxhash=oo123',
         'https://' || v_sub || '.ipfs.filebase.io/?fxhash=oo123'),
        ('https://' || v_sub || '.ipfs.dweb.link#view',
         'https://' || v_sub || '.ipfs.dweb.link/#view'),
        -- Unchanged: already slashed, a path before the query, no query, a bare delimiter
        -- with nothing after it, not a gateway URL, the ipfs scheme, a short CID.
        ('https://ipfs.filebase.io/ipfs/' || v_cid || '/?fxhash=oo123', 'https://ipfs.filebase.io/ipfs/' || v_cid || '/?fxhash=oo123'),
        ('https://ipfs.filebase.io/ipfs/' || v_cid || '/index.html?fxhash=oo123', 'https://ipfs.filebase.io/ipfs/' || v_cid || '/index.html?fxhash=oo123'),
        ('https://ipfs.filebase.io/ipfs/' || v_cid, 'https://ipfs.filebase.io/ipfs/' || v_cid),
        ('https://ipfs.filebase.io/ipfs/' || v_cid || '?', 'https://ipfs.filebase.io/ipfs/' || v_cid || '?'),
        ('https://' || v_sub || '.ipfs.filebase.io/?fxhash=oo123', 'https://' || v_sub || '.ipfs.filebase.io/?fxhash=oo123'),
        ('https://generator.artblocks.io/1/0xabc/88?x=1', 'https://generator.artblocks.io/1/0xabc/88?x=1'),
        ('ipfs://' || v_cid || '?fxhash=oo123', 'ipfs://' || v_cid || '?fxhash=oo123'),
        ('https://ipfs.io/ipfs/QmTEST1234?x=1', 'https://ipfs.io/ipfs/QmTEST1234?x=1'),
        ('https://example.com/a?ipfs/' || v_cid || '?x=1', 'https://example.com/a?ipfs/' || v_cid || '?x=1'),
        ('', '')
    ) AS t(input, expected) LOOP
        IF migration_035_slash(v_case.input) IS DISTINCT FROM v_case.expected THEN
            RAISE EXCEPTION 'migration_035: rewrite mismatch for %: got %, want %',
                v_case.input, migration_035_slash(v_case.input), v_case.expected;
        END IF;
        IF migration_035_is_bare(v_case.input) <> (v_case.input <> v_case.expected) THEN
            RAISE EXCEPTION 'migration_035: predicate mismatch for %', v_case.input;
        END IF;
    END LOOP;
END $$;

-- 1. Render probes on bare URLs judged the query-less render; drop them.
DELETE FROM media_render_probes WHERE migration_035_is_bare(media_url);

-- 2. Media assets. One row per (provider, target) moves (lowest id); a row whose target
--    already has an asset, or that lost that tie, keeps its bare URL (still found through
--    the bare alias). CreateMediaAsset can insert a target while this runs; the update
--    retries in a fresh subtransaction, whose NOT EXISTS then sees that row.
DO $$
DECLARE
    v_retries int := 0;
BEGIN
    LOOP
        BEGIN
            UPDATE media_assets a
            SET source_url      = migration_035_slash(a.source_url),
                source_url_hash = md5(migration_035_slash(a.source_url))
            FROM (SELECT id, row_number() OVER (PARTITION BY provider, md5(migration_035_slash(source_url))
                                                ORDER BY id) AS rn
                  FROM media_assets
                  WHERE migration_035_is_bare(source_url)) w
            WHERE a.id = w.id
              AND w.rn = 1
              AND NOT EXISTS (SELECT 1 FROM media_assets n
                              WHERE n.provider = a.provider
                                AND n.source_url_hash = md5(migration_035_slash(a.source_url)));
            EXIT;
        EXCEPTION WHEN unique_violation THEN
            v_retries := v_retries + 1;
            IF v_retries > 20 THEN
                RAISE;
            END IF;
            RAISE NOTICE 'migration_035: media asset key raced a concurrent writer, retry %', v_retries;
        END;
    END LOOP;
END $$;

-- 3. Work list of affected tokens (one sequential scan per table).
DROP TABLE IF EXISTS pg_temp.migration_035_tokens;
CREATE TEMP TABLE migration_035_tokens AS
    SELECT token_id FROM enrichment_sources
    WHERE migration_035_is_bare(image_url) OR migration_035_is_bare(animation_url)
    UNION
    SELECT token_id FROM token_metadata
    WHERE migration_035_is_bare(image_url) OR migration_035_is_bare(animation_url)
    UNION
    SELECT token_id FROM token_media_health
    WHERE migration_035_is_bare(media_url);
CREATE UNIQUE INDEX ON migration_035_tokens (token_id);
ANALYZE migration_035_tokens;

CREATE OR REPLACE PROCEDURE migration_035_move_tokens(p_queue text, p_batch_size int DEFAULT 100)
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
    v_moved    bigint[];
    v_gated    bigint[];
    v_deduped  bigint[];
    v_unviewed bigint;
    v_total_g  bigint := 0;
    v_total_v  bigint := 0;
    v_recomputed bigint;
    v_attempt  int;
BEGIN
    IF p_batch_size < 1 THEN
        RAISE EXCEPTION 'p_batch_size must be >= 1';
    END IF;
    IF coalesce(p_queue, '') = '' THEN
        RAISE EXCEPTION 'migration_035: the jobs.token_queue value is required (-v token_queue=...)';
    END IF;
    RAISE NOTICE 'migration_035: viewability webhooks go to queue % (must equal jobs.token_queue)', p_queue;

    LOOP
        SELECT array_agg(token_id ORDER BY token_id) INTO v_ids
        FROM (SELECT token_id FROM migration_035_tokens
              WHERE token_id > v_last ORDER BY token_id LIMIT p_batch_size) s;
        EXIT WHEN v_ids IS NULL;
        v_last := v_ids[array_length(v_ids, 1)];

        -- One batch per transaction, retried on lock conflicts with live writers (the
        -- subtransaction rolls the batch back; COMMIT follows only a clean run).
        v_attempt := 0;
        LOOP
            BEGIN
                v_unviewed := 0;
                v_recomputed := 0;
                -- Media health. Serialize against writers on every target URL (lockURLGate), in
                -- hash order to avoid deadlocks.
                SELECT array_agg(t ORDER BY t) INTO v_targets
                FROM (SELECT DISTINCT md5(migration_035_slash(media_url)) AS t
                      FROM token_media_health
                      WHERE token_id = ANY (v_ids) AND migration_035_is_bare(media_url)) s;
                IF v_targets IS NOT NULL THEN
                    FOREACH v_target IN ARRAY v_targets LOOP
                        PERFORM pg_advisory_xact_lock(hashtext(v_target));
                    END LOOP;
                END IF;
                v_targets := NULL;
                -- Then the bare health rows, in id order, before any metadata row: the same
                -- order as UpdateMediaURLAndPropagate (gate lock, health, token_metadata,
                -- enrichment_sources), so a concurrent gateway promotion cannot deadlock with
                -- this batch. Other writers are covered by the retry around the batch.
                PERFORM 1 FROM token_media_health
                WHERE token_id = ANY (v_ids) AND migration_035_is_bare(media_url)
                ORDER BY id FOR UPDATE;

                -- Keep one row per (token, source, target) for the unique key: an existing slashed
                -- row (written by a re-index on the new code) wins, then the lowest id among bare
                -- rows that converge. The deleted row may have been the token's only healthy one
                -- (the kept slashed row can be broken or gated), so its token is recomputed below.
                WITH deduped AS (
                DELETE FROM token_media_health h
                USING (
                    SELECT id, row_number() OVER (PARTITION BY token_id, media_source, target
                                                  ORDER BY is_target DESC, id) AS rn
                    FROM (SELECT id, token_id, media_source,
                                 md5(migration_035_slash(media_url)) AS target, false AS is_target
                          FROM token_media_health
                          WHERE token_id = ANY (v_ids) AND migration_035_is_bare(media_url)
                          UNION ALL
                          SELECT id, token_id, media_source, media_url_hash, true
                          FROM token_media_health
                          WHERE token_id = ANY (v_ids) AND NOT migration_035_is_bare(media_url)) c
                ) d
                WHERE h.id = d.id AND d.rn > 1
                RETURNING h.token_id
                )
                SELECT array_agg(DISTINCT token_id) INTO v_deduped FROM deduped;

                -- Move in place, keeping the L0 verdict (same bytes). A render gate on the bare
                -- URL judged the query-less render, so such a row is reset for a prompt re-check.
                WITH moved AS (
                    UPDATE token_media_health h
                    SET media_url       = migration_035_slash(h.media_url),
                        media_url_hash  = md5(migration_035_slash(h.media_url)),
                        health_status   = CASE WHEN h.failure_reason LIKE 'render\_%' THEN 'unknown'::media_health_status ELSE h.health_status END,
                        failure_reason  = CASE WHEN h.failure_reason LIKE 'render\_%' THEN NULL ELSE h.failure_reason END,
                        last_error      = CASE WHEN h.failure_reason LIKE 'render\_%' THEN NULL ELSE h.last_error END,
                        last_checked_at = CASE WHEN h.failure_reason LIKE 'render\_%' THEN 'epoch'::timestamptz ELSE h.last_checked_at END
                    WHERE h.token_id = ANY (v_ids) AND migration_035_is_bare(h.media_url)
                    RETURNING h.id
                )
                SELECT array_agg(id), count(*) INTO v_moved, v_health FROM moved;

                -- A gate the SLASHED URL already holds does travel (store.activeRenderGate: every
                -- writer that creates or moves health rows must consult it). After #178 deploys,
                -- L1 can gate a slashed URL before this backfill reaches a token still on the
                -- bare one. The probe row is the authority and is locked as activeRenderGate
                -- locks it; the reason comes from a sibling health row, else from the verdict.
                WITH gate AS (
                    SELECT p.media_url_hash, p.last_error,
                           coalesce((SELECT s.failure_reason FROM token_media_health s
                                     WHERE s.media_url_hash = p.media_url_hash
                                       AND s.failure_reason LIKE 'render\_%' LIMIT 1),
                                    CASE p.verdict WHEN 'known_bad_fingerprint' THEN 'render_known_bad'
                                                   WHEN 'stalled' THEN 'render_stalled'
                                                   ELSE 'render_blank' END) AS reason
                    FROM media_render_probes p
                    WHERE p.health_gated
                      AND p.media_url_hash IN (SELECT media_url_hash FROM token_media_health WHERE id = ANY (v_moved))
                    FOR UPDATE OF p
                ), inherited AS (
                    UPDATE token_media_health h
                    SET health_status  = 'broken',
                        failure_reason = g.reason,
                        last_error     = g.last_error
                    FROM gate g
                    WHERE h.id = ANY (v_moved) AND h.media_url_hash = g.media_url_hash
                    RETURNING h.token_id
                )
                SELECT array_agg(DISTINCT token_id) INTO v_gated FROM inherited;
                -- Recompute every token whose health rows changed in a way that can move
                -- is_viewable: an inherited gate, or a deduplicated row.
                SELECT array_agg(DISTINCT x) INTO v_gated
                FROM unnest(coalesce(v_gated, '{}') || coalesce(v_deduped, '{}')) x;

                -- Tokens that inherited a gate or lost a deduplicated row: recompute is_viewable exactly as
                -- BatchUpdateTokensViewability does (healthy, ungated animation; else no
                -- animation and a healthy, ungated image), with its token event and webhook job.
                IF v_gated IS NOT NULL THEN
                    WITH nv AS (
                        SELECT t.id, t.is_viewable AS old_viewable,
                               (EXISTS (SELECT 1 FROM token_media_health tmh
                                        WHERE tmh.token_id = t.id AND tmh.health_status = 'healthy'
                                          AND tmh.media_source IN ('metadata_animation', 'enrichment_animation')
                                          AND NOT EXISTS (SELECT 1 FROM media_render_probes p
                                                          WHERE p.media_url_hash = tmh.media_url_hash AND p.health_gated))
                                OR (NOT EXISTS (SELECT 1 FROM token_media_health tmh
                                                WHERE tmh.token_id = t.id
                                                  AND tmh.media_source IN ('metadata_animation', 'enrichment_animation'))
                                    AND EXISTS (SELECT 1 FROM token_media_health tmh
                                                WHERE tmh.token_id = t.id AND tmh.health_status = 'healthy'
                                                  AND tmh.media_source IN ('metadata_image', 'enrichment_image')
                                                  AND NOT EXISTS (SELECT 1 FROM media_render_probes p
                                                                  WHERE p.media_url_hash = tmh.media_url_hash AND p.health_gated)))
                               ) AS new_viewable
                        FROM tokens t
                        WHERE t.id = ANY (v_gated)
                    ), upd AS (
                        UPDATE tokens t SET is_viewable = nv.new_viewable
                        FROM nv
                        WHERE t.id = nv.id AND t.is_viewable <> nv.new_viewable
                        RETURNING t.id, t.token_cid, t.chain, t.standard, t.contract_address, t.token_number,
                                  nv.new_viewable, migration_035_ulid() AS event_id
                    ), ev AS (
                        INSERT INTO token_events (token_id, event_type, owner_address, occurred_at, metadata, created_at)
                        SELECT id, 'viewability_changed', NULL, clock_timestamp(),
                               jsonb_build_object('is_viewable', new_viewable), clock_timestamp()
                        FROM upd
                    )
                    -- The token.viewability.changed webhook, enqueued exactly as the sweeper's
                    -- triggerWebhook does (same job kind, queue, unique key and payload), so
                    -- subscribers such as search drop or restore the token.
                    INSERT INTO jobs (queue, kind, payload, unique_key)
                    SELECT p_queue, 'NotifyWebhookClients',
                           jsonb_build_array(jsonb_build_object(
                               'event_id', u.event_id,
                               'event_type', 'token.viewability.changed',
                               'timestamp', to_char(clock_timestamp() AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"'),
                               'data', jsonb_build_object(
                                   'token_cid', u.token_cid,
                                   'chain', u.chain::text,
                                   'standard', u.standard::text,
                                   'contract', u.contract_address,
                                   'token_number', u.token_number,
                                   'is_viewable', u.new_viewable))),
                           'webhook-notify-' || u.event_id
                    FROM upd u;
                    GET DIAGNOSTICS v_unviewed = ROW_COUNT;
                    v_recomputed := coalesce(array_length(v_gated, 1), 0);
                END IF;
                -- Token metadata: rewrite + one 'metadata_updated' event per changed token.
                WITH src AS (
                    SELECT token_id, image_url AS oi, animation_url AS oa,
                           migration_035_slash(image_url) AS ni,
                           migration_035_slash(animation_url) AS na
                    FROM token_metadata
                    WHERE token_id = ANY (v_ids)
                      AND (migration_035_is_bare(image_url) OR migration_035_is_bare(animation_url))
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

                -- Enrichment: rewrite + one 'enrichment_updated' event per changed token.
                WITH src AS (
                    SELECT token_id, vendor, image_url AS oi, animation_url AS oa,
                           migration_035_slash(image_url) AS ni,
                           migration_035_slash(animation_url) AS na
                    FROM enrichment_sources
                    WHERE token_id = ANY (v_ids)
                      AND (migration_035_is_bare(image_url) OR migration_035_is_bare(animation_url))
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


                EXIT;
            EXCEPTION WHEN deadlock_detected OR lock_not_available OR serialization_failure THEN
                v_attempt := v_attempt + 1;
                IF v_attempt >= 10 THEN
                    RAISE;
                END IF;
                RAISE NOTICE 'migration_035: batch through token_id % hit % (%), retry %',
                    v_last, SQLSTATE, SQLERRM, v_attempt;
                PERFORM pg_sleep(0.2 * v_attempt);
            END;
        END LOOP;
        v_total_g := v_total_g + v_recomputed;
        v_total_v := v_total_v + v_unviewed;
        v_moved := NULL;
        v_gated := NULL;
        v_deduped := NULL;

        v_total_e := v_total_e + v_enriched;
        v_total_m := v_total_m + v_meta;
        v_total_h := v_total_h + v_health;
        RAISE NOTICE 'migration_035: through token_id % | enrichment % | metadata % | health % (totals % / % / %; recomputed % tokens, viewability changed %)',
            v_last, v_enriched, v_meta, v_health, v_total_e, v_total_m, v_total_h, v_total_g, v_total_v;
        COMMIT;
    END LOOP;
END $$;

CALL migration_035_move_tokens(:'token_queue');
DROP PROCEDURE migration_035_move_tokens(text, int);
DROP TABLE migration_035_tokens;

-- 4. Render probes, final pass: drop bare rows written by in-flight render jobs.
DELETE FROM media_render_probes WHERE migration_035_is_bare(media_url);

-- Verification: every count should be 0, except media_assets rows intentionally left on a
-- bare URL because the slashed target already had an asset.
SELECT 'enrichment_sources' AS tbl, count(*) AS bare_rows FROM enrichment_sources
    WHERE migration_035_is_bare(image_url) OR migration_035_is_bare(animation_url)
UNION ALL SELECT 'token_metadata', count(*) FROM token_metadata
    WHERE migration_035_is_bare(image_url) OR migration_035_is_bare(animation_url)
UNION ALL SELECT 'token_media_health', count(*) FROM token_media_health
    WHERE migration_035_is_bare(media_url)
UNION ALL SELECT 'media_render_probes', count(*) FROM media_render_probes
    WHERE migration_035_is_bare(media_url)
UNION ALL SELECT 'media_assets (conflicts left on purpose)', count(*) FROM media_assets
    WHERE migration_035_is_bare(source_url);

DROP FUNCTION migration_035_ulid();
DROP FUNCTION migration_035_slash(text);
DROP FUNCTION migration_035_is_bare(text);
