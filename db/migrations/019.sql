-- Migration 019: Enforce 1-based mint_number contract on release_members
-- The API and schema docs declare mint_number as authoritative and 1-based
-- (matching Feral File convention: first token is edition 1, not 0).
-- This constraint enforces that invariant at the database level so no
-- code path or direct DB write can persist an invalid zero/negative value.

BEGIN;

ALTER TABLE release_members
    ADD CONSTRAINT release_members_mint_number_positive CHECK (mint_number > 0);

COMMIT;
