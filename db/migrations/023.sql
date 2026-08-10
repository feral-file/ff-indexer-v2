-- Migration 023: L1 render probe observations (media_render_probes).
--
-- Level 0 (migration 022) validates bytes; it cannot see what a browser paints. An HTML
-- work whose <script src> points at a dead gateway passes every byte check and still
-- renders a black screen (ff-indexer-v2#96 class), and a placeholder image served for a
-- lapsed pin is a perfectly valid PNG. The L1 render probe loads L0-healthy URLs in
-- headless chromium, screenshots a frame, and classifies it: matching a known-bad render
-- fingerprint (directory listing, gateway error page, placeholder) gates viewability
-- immediately; blank or stalled renders gate only after consecutive failures (debounce),
-- because slow WebGL under software GL and intentionally dark works produce false blanks.
--
-- One row per URL (PK media_url_hash), not per token: what a URL renders to is a property
-- of the URL, mirroring the media_url_hash-wide matching of token_media_health updates.
-- Gating flows through the existing token_media_health row (failure_reason 'render_%'),
-- so viewability recomputation and events need no new machinery.
--
-- phash/baseline_phash are BIGINT holding the 64-bit DCT perceptual hash's bit pattern
-- (int64 two's complement; readers reinterpret as uint64). baseline_phash is the first
-- successful capture and is never overwritten: successive-capture drift comparison is
-- deliberately NOT implemented (capture-only, per the feral-file#3485 agreement) — the
-- baseline exists so drift can be switched on later against real history. engine_version
-- and viewport are recorded with every capture as a hard requirement: a chromium upgrade
-- changes rasterization, and a hash without its engine is not comparable to anything.
--
-- next_check_at is the sweeper's work-queue cursor (moderation-sweeper pattern): the due
-- query is served by the index below plus a LEFT JOIN discovering never-probed URLs.
--
-- LOCKING: brand-new objects only; no existing table is touched. Safe under traffic.

BEGIN;

CREATE TYPE render_probe_verdict AS ENUM ('rendered_ok', 'blank', 'stalled', 'known_bad_fingerprint');

CREATE TABLE media_render_probes (
    media_url_hash TEXT PRIMARY KEY,  -- MD5 of media_url, same keying as token_media_health
    media_url TEXT NOT NULL,
    phash BIGINT,                     -- 64-bit DCT pHash of the latest capture (NULL when capture failed)
    baseline_phash BIGINT,            -- first successful capture, never overwritten
    engine_version TEXT,              -- browser identity at capture time (User-Agent)
    viewport TEXT,                    -- capture viewport as "WxH"
    verdict render_probe_verdict NOT NULL,
    consecutive_failures INT NOT NULL DEFAULT 0,  -- blank/stalled debounce counter
    last_error TEXT,                  -- render failure detail (NULL on rendered_ok)
    captured_at TIMESTAMPTZ,          -- last successful screenshot time (NULL when never captured)
    next_check_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_media_render_probes_due ON media_render_probes (next_check_at);

CREATE TRIGGER update_media_render_probes_updated_at
    BEFORE UPDATE ON media_render_probes
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

COMMENT ON TABLE media_render_probes IS
    'L1 render-probe observations, one row per media URL. Records what headless chromium '
    'painted for the URL (phash + engine_version + viewport) and the render verdict. '
    'Gating flows through token_media_health.failure_reason (render_%); baseline_phash is '
    'capture-only — drift comparison is deferred (feral-file#3485).';

COMMENT ON COLUMN media_render_probes.phash IS
    '64-bit DCT perceptual hash bit pattern of the latest capture, stored as int64.';
COMMENT ON COLUMN media_render_probes.baseline_phash IS
    'pHash of the first successful capture; never overwritten so future drift detection '
    'has a stable reference.';
COMMENT ON COLUMN media_render_probes.consecutive_failures IS
    'Consecutive blank/stalled probes; viewability gates at the configured threshold '
    '(known_bad_fingerprint gates immediately and does not use this counter).';

COMMIT;
