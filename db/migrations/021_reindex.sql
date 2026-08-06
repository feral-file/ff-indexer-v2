BEGIN;

-- Migration 021_reindex: Enqueue IndexTokenMetadata jobs so pre-existing tokens get a
-- first spam verdict.
--
-- Context
-- -------
-- Migration 021 adds token_spam_verdicts, but leaves it empty. Neither writer discovers
-- tokens on its own:
--
--   * the enricher writes a verdict only while indexing a token, and
--   * the spam sweeper's queue query (GetTokenSpamVerdictsDueForCheck) reads
--     FROM token_spam_verdicts JOIN tokens — an inner join starting at the verdicts
--     table, with no NOT EXISTS / LEFT JOIN discovery pass.
--
-- So on a database that already holds tokens, the feature covers exactly nothing until
-- each token happens to be re-indexed for some unrelated reason. Tokens a vendor flagged
-- long ago keep rendering. This migration re-enriches them so the enricher writes that
-- first row, after which the sweeper keeps it fresh.
--
-- Why reindex rather than derive from stored vendor JSON
-- ------------------------------------------------------
-- enrichment_sources.vendor_json does often contain objkt's `flag` and OpenSea's
-- `is_disabled` today, so deriving the verdict in SQL looks tempting. It is not safe:
-- vendor_json is a snapshot of whatever fields the enricher chose to keep at the time it
-- ran, accumulated across every version of that code, not a contract that any given field
-- was captured on every historical row. Migration 018_reindex hit exactly this and
-- documents four separate fields that were missing from older vendor_json rows. Re-asking
-- the vendor is the only way to get a verdict that is both complete and current.
--
-- ⚠️ Deployment ordering: run this AFTER the new application code is live
-- -----------------------------------------------------------------------
-- This is the reverse of the usual rule in DEVELOPMENT.md ("run migrations before
-- deploying new application code"), which applies to 021.sql and is correct there —
-- steps 1-2 add the column, table, and widened CHECK constraint that the new code needs.
--
-- This file is different because it enqueues work for the running worker rather than
-- changing the schema. A worker running pre-021 code claims these jobs, runs the old
-- enricher, writes no verdict row, and marks the job succeeded. The jobs then leave the
-- active set and the backfill has silently done nothing.
--
-- Recovery if that happens: re-run THIS FILE only (jobs_unique_key_active guards only
-- active jobs, so finished ones do not block re-insertion). Do not re-run 021.sql — it
-- fails at ADD COLUMN.
--
-- Volume
-- ------
-- One job per already-enriched opensea/objkt token; on a large database that is a
-- substantial queue, and every backfilled verdict lands at now + initial_recheck_interval,
-- so the sweeper's first pass after it drains is a burst against the same vendor rate
-- limiter the enricher uses. Size it first:
--
--   SELECT vendor, count(*) FROM enrichment_sources
--    WHERE vendor IN ('opensea', 'objkt') GROUP BY vendor;
--
-- See docs/spam_filtering.md for mitigations (longer initial_recheck_interval, or pacing
-- this INSERT in batches) if the count is large.
--
-- Vendor predicate
-- ----------------
-- opensea and objkt are the only moderating vendors, matching schema.SpamSourceForVendor,
-- which returns ok=false for every other vendor. Rows from those vendors get no verdict at
-- indexing time either, so enqueueing them here would spend vendor quota to write nothing.
--
-- fxhash is deliberately NOT covered, and only partly by accident: enhanceFxhash falls back
-- to enhanceObjkt when the fxhash API has no gentk for the token, and those rows are stored
-- as vendor='objkt', so that subset is picked up here. Tokens fxhash does index are stored
-- as vendor='fxhash' and are skipped — consistent with the enricher, which writes no verdict
-- for them. That is the intended coverage gap for curated surfaces (see the accepted-gaps
-- list in docs/spam_filtering.md), not an oversight in this predicate.
--
-- Idempotency
-- -----------
-- The partial unique index jobs_unique_key_active enforces at most one active (pending or
-- running) job per (queue, kind, unique_key). The ON CONFLICT clause skips any token that
-- already has an active metadata job, so this migration is safe to re-run.
--
-- Fresh installs
-- --------------
-- Fresh installs have no enrichment_sources rows, so this INSERT produces no rows. No
-- changes to db/init_pg_db.sql are required.

INSERT INTO jobs (queue, kind, payload, status, unique_key)
SELECT
    'token_index'                                    AS queue,
    'IndexTokenMetadata'                             AS kind,
    jsonb_build_array(t.token_cid, null)             AS payload,
    'pending'                                        AS status,
    'index-metadata-' || t.token_cid                AS unique_key
FROM enrichment_sources es
JOIN tokens t ON t.id = es.token_id
WHERE es.vendor IN ('opensea', 'objkt')
ON CONFLICT (queue, kind, unique_key)
    WHERE status IN ('pending', 'running') AND unique_key IS NOT NULL
DO NOTHING;

COMMIT;
