-- Export would-gate URLs for local re-probing.
-- Samples up to 3 variants per artwork so a 500-variant work does not swamp the run.
--
--   psql "$DSN" -f tools/probeaudit/export_candidates.sql > candidates.csv
--
-- Raise the rn limit (or drop the WHERE) for a fuller sweep.
COPY (
  WITH cand AS (
    SELECT media_url,
           media_url_hash,
           verdict,
           consecutive_failures,
           last_error,
           split_part(media_url, '?', 1) AS base_url,
           CASE WHEN last_error LIKE '%context deadline exceeded%' THEN 'timeout'
                WHEN last_error LIKE 'blank frame%'                THEN 'blank'
                WHEN last_error LIKE '%net::ERR_ABORTED%'          THEN 'aborted'
                ELSE 'other' END                                   AS cause,
           row_number() OVER (PARTITION BY split_part(media_url, '?', 1)
                              ORDER BY consecutive_failures DESC, media_url) AS rn
    FROM media_render_probes
    WHERE consecutive_failures >= 2
      AND verdict IN ('blank', 'stalled')
      AND coalesce(last_error, '') NOT LIKE 'main document returned HTTP%'
      AND coalesce(last_error, '') NOT LIKE 'ssrf policy refused%'
  )
  SELECT c.media_url,
         c.base_url,
         c.cause,
         c.verdict          AS prod_verdict,
         c.consecutive_failures,
         coalesce((SELECT count(DISTINCT h.token_id)
                   FROM token_media_health h
                   WHERE h.media_url_hash = c.media_url_hash), 0) AS tokens,
         replace(left(coalesce(c.last_error, ''), 140), E'\n', ' ') AS prod_last_error
  FROM cand c
  WHERE c.rn <= 3
  ORDER BY c.cause, c.base_url, c.media_url
) TO STDOUT WITH CSV HEADER;
