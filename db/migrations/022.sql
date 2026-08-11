-- Migration 022: L0 content-validation observability on token_media_health.
--
-- The media health probe historically treated any 2xx response as "healthy" without ever
-- reading the response body. That let through every failure mode where a server answers
-- 200 with something other than the artwork: IPFS gateways returning an HTML directory
-- listing for a directory CID (feral-file#3482), gateway error pages served with 200,
-- ONCHFS assets that 500 in a browser but pass a bare HEAD (ff-indexer-v2#76), and
-- truncated or corrupt media. Those rows then drove tokens.is_viewable=true and FF1
-- rendered black screens (ff-indexer-v2#96).
--
-- The probe now performs a single ranged GET and validates the first bytes of the body
-- (declared vs sniffed content type, container header parse, directory-listing and
-- known-error-page markers). This migration adds the observability columns that record
-- what the probe saw, so a broken verdict is explainable and the fleet-wide delta
-- ("reported healthy" vs "actually valid") can be queried per failure class.
--
-- failure_reason is TEXT, not an enum: the values are app-owned, additive, and namespaced
-- (the render probe introduced later reserves the 'render_%' prefix); an enum would force
-- a migration for every new probe rule. The three columns are nullable with no default:
-- NULL simply means "not observed by the current probe yet", which is exactly the state of
-- every pre-existing row, so no backfill is needed and rows recreated by metadata upserts
-- (which reset health to 'unknown') start clean.
--
-- The media_health_status enum is deliberately unchanged (unknown|healthy|broken):
-- "reachable but wrong content" is still broken for viewability purposes; the reason
-- column, not a new status, carries the distinction.
--
-- LOCKING: metadata-only ADD COLUMN with no default on PG11+ — no table rewrite, brief
-- ACCESS EXCLUSIVE only. Safe under normal traffic.

BEGIN;

ALTER TABLE token_media_health
    ADD COLUMN failure_reason TEXT,
    ADD COLUMN observed_content_type TEXT,
    ADD COLUMN sniffed_content_type TEXT;

COMMENT ON COLUMN token_media_health.failure_reason IS
    'Machine-readable cause of the last broken verdict: http_status | dns | ssrf | '
    'type_mismatch | container_invalid | directory_listing | known_error_page | '
    'zero_length | truncated. The render_% prefix is reserved for the L1 render probe '
    '(render-gated rows are healed only by the render probe, never by the byte-level '
    'sweep). NULL when healthy, unknown, or not yet probed by the content-validating checker.';

COMMENT ON COLUMN token_media_health.observed_content_type IS
    'Content-Type header observed on the last probe. NULL when not yet probed by the '
    'content-validating checker.';

COMMENT ON COLUMN token_media_health.sniffed_content_type IS
    'Content type detected from the first bytes of the body (magic-byte sniffing) on the '
    'last probe. Drives render-probe class selection (HTML vs image vs video). NULL when '
    'not yet probed by the content-validating checker.';

COMMIT;
