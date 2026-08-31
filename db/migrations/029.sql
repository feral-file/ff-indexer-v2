-- Migration 029: keep the block timestamp on staged owner-scan logs.
--
-- Owner-scan windows served by the log warehouse (ff-indexer-v2 #144) carry
-- blockTimestamp on every log; the staging row dropped it, so the replay's
-- event parsing fell back to one vendor eth_getBlockByNumber per distinct
-- block — the exact calls the warehouse exists to remove — and a failing lookup
-- could fail the replay. Vendor-served windows still stage 0 (unknown), which
-- keeps the existing fallback.
--
-- Standard ordering: apply before deploying the image that writes the column.
-- Rows staged by an older image read back as 0 and behave as before.

BEGIN;

ALTER TABLE address_scan_logs
    ADD COLUMN IF NOT EXISTS block_timestamp BIGINT NOT NULL DEFAULT 0;

COMMENT ON COLUMN address_scan_logs.block_timestamp IS
    'Unix block time carried on the log (warehouse-served windows); 0 = unknown, resolved by the block provider at replay';

COMMIT;
