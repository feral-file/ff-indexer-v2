-- Migration 012: Add source_url_hash to media_assets
-- Switch to hashed lookup and allow NULL source_url for data URIs

BEGIN;

ALTER TABLE media_assets
    ADD COLUMN IF NOT EXISTS source_url_hash TEXT;

UPDATE media_assets
SET source_url_hash = md5(source_url)
WHERE source_url_hash IS NULL
  AND source_url IS NOT NULL;

ALTER TABLE media_assets
    ALTER COLUMN source_url_hash SET NOT NULL;

-- Replace unique constraint to use source_url_hash
ALTER TABLE media_assets
    DROP CONSTRAINT IF EXISTS media_assets_source_url_provider_key;

ALTER TABLE media_assets
    ADD CONSTRAINT media_assets_source_url_hash_provider_key UNIQUE (source_url_hash, provider);

DROP INDEX IF EXISTS idx_media_assets_source_url;
CREATE INDEX IF NOT EXISTS idx_media_assets_source_url_hash ON media_assets (source_url_hash);

COMMIT;
