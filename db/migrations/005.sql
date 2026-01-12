-- Migration 005: Add Budgeted Indexing Mode quota tracking
-- This migration adds support for quota-based token indexing with rolling 24-hour windows

-- Add budgeted indexing mode columns to watched_addresses table
ALTER TABLE watched_addresses ADD COLUMN IF NOT EXISTS 
    daily_token_quota INTEGER NOT NULL DEFAULT 1000;
    
ALTER TABLE watched_addresses ADD COLUMN IF NOT EXISTS 
    quota_reset_at TIMESTAMPTZ;
    -- 24 hours from when address first started indexing in current period
    -- NULL means never indexed yet
    
ALTER TABLE watched_addresses ADD COLUMN IF NOT EXISTS 
    tokens_indexed_today INTEGER NOT NULL DEFAULT 0;
    -- Number of tokens indexed in current 24-hour quota period

-- Comments for documentation
COMMENT ON COLUMN watched_addresses.daily_token_quota IS 
    'Maximum tokens allowed per 24-hour period for this address (quota-based indexing)';
    
COMMENT ON COLUMN watched_addresses.quota_reset_at IS 
    'When the current 24-hour quota period ends (rolling window from first index)';
    
COMMENT ON COLUMN watched_addresses.tokens_indexed_today IS 
    'Number of tokens indexed in current 24-hour quota period';

