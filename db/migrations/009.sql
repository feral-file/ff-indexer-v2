-- Migration 009: Add viewability status to tokens
-- This adds a cached column to track if a token is viewable based purely on media health
-- A token is viewable if it has at least one healthy media URL

BEGIN;

-- Add the is_viewable column with default FALSE
ALTER TABLE tokens 
  ADD COLUMN is_viewable BOOLEAN NOT NULL DEFAULT false;

-- Migrate existing tokens that have metadata OR enrichment to TRUE for backward compatibility
-- This gives them the benefit of the doubt until the sweeper validates their media health
UPDATE tokens
SET is_viewable = true
WHERE EXISTS (
    SELECT 1 FROM token_metadata tm WHERE tm.token_id = tokens.id
) OR EXISTS (
    SELECT 1 FROM enrichment_sources es WHERE es.token_id = tokens.id
);

-- Add comment explaining the column
COMMENT ON COLUMN tokens.is_viewable IS 
  'Viewability status based purely on media health. TRUE = has at least one healthy media URL (animation preferred, fallback to image). FALSE = no URLs or all broken. Updated by: 1) IndexTokenMetadata workflow after checking URLs, 2) Media health sweeper after rechecking URLs.';

-- Create partial index for fast filtering of viewable tokens (expected to be majority case)
CREATE INDEX idx_tokens_viewable 
  ON tokens(is_viewable) 
  WHERE is_viewable = true;

-- Create compound index for querying by chain + viewability
CREATE INDEX idx_tokens_chain_is_viewable 
  ON tokens(chain, is_viewable);

-- Create compound index for querying by owner + viewability  
CREATE INDEX idx_tokens_current_owner_is_viewable 
  ON tokens(current_owner, is_viewable) 
  WHERE current_owner IS NOT NULL;

COMMIT;
