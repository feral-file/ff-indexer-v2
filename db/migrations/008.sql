-- Migration 008: Add token_viewability subject type
-- This enables tracking viewability changes in the changes_journal

-- Add token_viewability to subject_type enum
ALTER TYPE subject_type ADD VALUE IF NOT EXISTS 'token_viewability';
