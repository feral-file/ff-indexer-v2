-- Migration 007: Add token_media_health table for tracking media URL health status
-- This table tracks health check status for media URLs associated with tokens.
-- Automatically synchronized when token_metadata or enrichment_sources are updated.

-- Create enum for health status
CREATE TYPE media_health_status AS ENUM ('unknown', 'healthy', 'broken', 'checking');

-- Create token_media_health table
CREATE TABLE token_media_health (
    id BIGSERIAL PRIMARY KEY,
    token_id BIGINT NOT NULL REFERENCES tokens(id) ON DELETE CASCADE,
    media_url TEXT NOT NULL,
    media_source TEXT NOT NULL,  -- 'metadata_image', 'metadata_animation', 'enrichment_image', 'enrichment_animation'
    health_status media_health_status NOT NULL DEFAULT 'unknown',
    last_checked_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_error TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    
    -- One health record per token per URL per source
    UNIQUE(token_id, media_url, media_source)
);

-- Essential indexes
CREATE INDEX idx_token_media_health_token_id ON token_media_health(token_id);
CREATE INDEX idx_token_media_health_url ON token_media_health(media_url);
CREATE INDEX idx_token_media_health_last_checked ON token_media_health(last_checked_at);
CREATE INDEX idx_token_media_health_token_status ON token_media_health(token_id, health_status);
CREATE INDEX idx_token_media_health_source ON token_media_health(media_source);

-- Add trigger for updated_at
CREATE TRIGGER update_token_media_health_updated_at
    BEFORE UPDATE ON token_media_health
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

COMMENT ON TABLE token_media_health IS 'Tracks health check status for media URLs associated with tokens. Includes source information to distinguish between metadata/enrichment and image/animation URLs.';
COMMENT ON COLUMN token_media_health.health_status IS 'Current health status: unknown (not checked), checking (in progress), healthy (accessible), broken (not accessible)';
COMMENT ON COLUMN token_media_health.media_source IS 'Source of the URL: metadata_image, metadata_animation, enrichment_image, enrichment_animation';
COMMENT ON COLUMN token_media_health.last_error IS 'Error message from last failed check. NULL if healthy.';

-- Add indexes for media URLs in token_metadata and enrichment_sources
-- These are needed for efficient URL propagation when alternatives are found
CREATE INDEX idx_token_metadata_image_url ON token_metadata(image_url) WHERE image_url IS NOT NULL;
CREATE INDEX idx_token_metadata_animation_url ON token_metadata(animation_url) WHERE animation_url IS NOT NULL;
CREATE INDEX idx_enrichment_sources_image_url ON enrichment_sources(image_url) WHERE image_url IS NOT NULL;
CREATE INDEX idx_enrichment_sources_animation_url ON enrichment_sources(animation_url) WHERE animation_url IS NOT NULL;

-- ============================================================================
-- Data Migration: Populate from existing metadata and enrichment sources
-- ============================================================================

-- Insert image URLs from metadata
INSERT INTO token_media_health (token_id, media_url, media_source, health_status, last_checked_at)
SELECT 
    token_id, 
    image_url as media_url, 
    'metadata_image' as media_source,
    'unknown' as health_status,
    '1970-01-01 00:00:00+00' as last_checked_at
FROM token_metadata 
WHERE image_url IS NOT NULL AND image_url != ''
ON CONFLICT (token_id, media_url, media_source) DO NOTHING;

-- Insert animation URLs from metadata
INSERT INTO token_media_health (token_id, media_url, media_source, health_status, last_checked_at)
SELECT 
    token_id, 
    animation_url as media_url, 
    'metadata_animation' as media_source,
    'unknown' as health_status,
    '1970-01-01 00:00:00+00' as last_checked_at
FROM token_metadata 
WHERE animation_url IS NOT NULL AND animation_url != ''
ON CONFLICT (token_id, media_url, media_source) DO NOTHING;

-- Insert image URLs from enrichment
INSERT INTO token_media_health (token_id, media_url, media_source, health_status, last_checked_at)
SELECT 
    token_id, 
    image_url as media_url, 
    'enrichment_image' as media_source,
    'unknown' as health_status,
    '1970-01-01 00:00:00+00' as last_checked_at
FROM enrichment_sources 
WHERE image_url IS NOT NULL AND image_url != ''
ON CONFLICT (token_id, media_url, media_source) DO NOTHING;

-- Insert animation URLs from enrichment
INSERT INTO token_media_health (token_id, media_url, media_source, health_status, last_checked_at)
SELECT 
    token_id, 
    animation_url as media_url, 
    'enrichment_animation' as media_source,
    'unknown' as health_status,
    '1970-01-01 00:00:00+00' as last_checked_at
FROM enrichment_sources 
WHERE animation_url IS NOT NULL AND animation_url != ''
ON CONFLICT (token_id, media_url, media_source) DO NOTHING;
