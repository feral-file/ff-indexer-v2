-- Migration 013: Add version column for scoped state sync
-- This migration adds a version column to the tokens table to support
-- snapshot-based sync mechanism. Version increments on any user-visible change.

BEGIN;

-- Add version column with default value 1
ALTER TABLE tokens 
ADD COLUMN version BIGINT NOT NULL DEFAULT 1;

-- No new indexes needed! The query will use existing indexes:
-- 1. idx_balances_owner_address on balances(owner_address) - for finding owner's tokens
-- 2. tokens_pkey on tokens(id) - for JOIN on tokens.id
-- 3. Sequential scan of result set is minimal (only owned tokens, typically < 10k rows)

COMMIT;
