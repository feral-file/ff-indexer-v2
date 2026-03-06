-- Migration 014: Replace version-based sync with unified event log
-- This migration introduces token_events table for efficient collection sync
-- and removes the old version-based approach

BEGIN;

-- ============================================================================
-- Part 1: Remove old version-based sync mechanism
-- ============================================================================

-- Drop old indexes
DROP INDEX IF EXISTS idx_tokens_version;

-- Remove version column from tokens
ALTER TABLE tokens DROP COLUMN IF EXISTS version;

-- ============================================================================
-- Part 2: Create unified token events table
-- ============================================================================

-- Token events table - unified log for ownership and attribute changes
-- Replaces changes_journal for collection sync purposes
CREATE TABLE token_events (
    id BIGSERIAL PRIMARY KEY,
    token_id BIGINT NOT NULL REFERENCES tokens(id) ON DELETE CASCADE,
    
    -- Event type: ownership or attribute changes
    event_type TEXT NOT NULL CHECK (event_type IN (
        'acquired',              -- Token added to owner's collection
        'released',              -- Token removed from owner's collection
        'metadata_updated',      -- Token metadata changed
        'enrichment_updated',    -- Token enrichment changed
        'viewability_changed'    -- Token viewability changed
    )),
    
    -- Owner address (set for ownership events, NULL for attribute events that broadcast to all owners)
    owner_address TEXT,
    
    -- When the event actually occurred (NOT when it was recorded)
    occurred_at TIMESTAMPTZ NOT NULL,
    
    -- Lightweight metadata for reference (JSONB for flexibility)
    -- Examples:
    --   acquired: {"tx_hash": "0x...", "quantity": "1"}
    --   released: {"tx_hash": "0x...", "quantity": "1", "to_address": "0x..."}
    --   metadata_updated: {"changed_fields": ["name", "image_url"]}
    --   enrichment_updated: {"vendor": "artblocks", "changed_fields": ["animation_url"]}
    --   viewability_changed: {"is_viewable": true}
    metadata JSONB,
    
    -- When this event was recorded in the database
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- ============================================================================
-- Part 3: Create indexes for efficient queries
-- ============================================================================

-- Primary index for owner-scoped sync queries
-- Query: "Get all events for owner since timestamp" with created_at for consistent pagination
CREATE INDEX idx_token_events_owner_created 
ON token_events(owner_address, created_at ASC, id ASC) 
WHERE owner_address IS NOT NULL;

-- Index for general broadcast events queries (all broadcast events by created_at)
CREATE INDEX idx_token_events_created_id 
ON token_events(created_at ASC, id ASC);

-- Index for broadcast events by token_id (optimizes JOIN with balances table)
-- Critical for scalability: allows efficient lookup of broadcast events per owned token
CREATE INDEX idx_token_events_token_created 
ON token_events(token_id, created_at ASC, id ASC) 
WHERE owner_address IS NULL;

COMMIT;
