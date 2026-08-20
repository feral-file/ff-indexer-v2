-- Migration 027: checkpointed Ethereum owner-scan sessions.
--
-- Discovering an address's tokens walks owner-scoped eth_getLogs over the whole
-- chain (~7,500 calls at Infura's 10k block-span cap after query merging). The
-- pre-session implementation ran that walk as one atomic in-memory operation:
-- any mid-scan failure discarded every already-paid call and restarted from
-- block zero, and daily-quota pauses re-scanned the entire un-indexed remainder
-- because the discovered token list was never persisted (only a block-range
-- watermark). See docs/address_scan_sessions.md for the full design.
--
-- Three tables:
--  * address_scan_sessions — one active session per (chain, address); the
--    cursor_block checkpoint makes the window loop resumable at ~3-call
--    granularity. status: 'scanning' (window loop) -> 'replayed' (token list
--    persisted, logs deleted). Completed sessions are deleted.
--  * address_scan_logs — raw logs staged per window. The identity PK makes
--    window re-fetch after a crash idempotent (ON CONFLICT DO NOTHING).
--    Deleted in the same transaction that persists the replayed token list:
--    they are pure intermediate state, re-derivable from chain, and the
--    bulkiest rows.
--  * address_scan_tokens — the durable discovery result. indexed_at IS NULL
--    means pending; quota-paced indexing marks chunks as they land, so a
--    quota resume continues from here with zero further RPC.
--
-- No background janitor by design: a session leaks only if its address is
-- never indexed again, and its cost is bounded (logs die at replay). Re-run
-- safety: guarded by IF NOT EXISTS on every object.

BEGIN;

DO $$ BEGIN
    CREATE TYPE address_scan_session_status AS ENUM ('scanning', 'replayed');
EXCEPTION
    WHEN duplicate_object THEN NULL;
END $$;

CREATE TABLE IF NOT EXISTS address_scan_sessions (
    id BIGSERIAL PRIMARY KEY,
    chain blockchain_chain NOT NULL,
    address TEXT NOT NULL,
    -- Inclusive block range this session covers.
    from_block BIGINT NOT NULL,
    to_block BIGINT NOT NULL,
    -- Next un-fetched block; > to_block means the window loop is complete.
    cursor_block BIGINT NOT NULL,
    status address_scan_session_status NOT NULL DEFAULT 'scanning',
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    -- One session at a time per address: range selection is sequential
    -- (backward gap, then forward gap), and the unique constraint also
    -- protects against concurrent workers racing session creation.
    CONSTRAINT uq_address_scan_sessions_chain_address UNIQUE (chain, address)
);

CREATE TABLE IF NOT EXISTS address_scan_logs (
    session_id BIGINT NOT NULL REFERENCES address_scan_sessions (id) ON DELETE CASCADE,
    block_number BIGINT NOT NULL,
    tx_hash TEXT NOT NULL,
    log_index INTEGER NOT NULL,
    -- Emitting contract address.
    address TEXT NOT NULL,
    -- Topic hashes as 0x-prefixed hex strings, in topic order.
    topics TEXT[] NOT NULL,
    data BYTEA,
    tx_index INTEGER NOT NULL DEFAULT 0,
    block_hash TEXT,
    PRIMARY KEY (session_id, block_number, tx_hash, log_index)
);

CREATE TABLE IF NOT EXISTS address_scan_tokens (
    session_id BIGINT NOT NULL REFERENCES address_scan_sessions (id) ON DELETE CASCADE,
    token_cid TEXT NOT NULL,
    -- Last ownership-affecting block for this token; drives block-aligned chunking.
    block_number BIGINT NOT NULL,
    indexed_at TIMESTAMPTZ,
    PRIMARY KEY (session_id, token_cid)
);

-- Resume query: pending tokens for a session, newest blocks first.
CREATE INDEX IF NOT EXISTS idx_address_scan_tokens_pending
    ON address_scan_tokens (session_id, block_number DESC)
    WHERE indexed_at IS NULL;

DO $$ BEGIN
    CREATE TRIGGER update_address_scan_sessions_updated_at
        BEFORE UPDATE ON address_scan_sessions
        FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();
EXCEPTION
    WHEN duplicate_object THEN NULL;
END $$;

COMMENT ON TABLE address_scan_sessions IS 'Checkpointed Ethereum owner-scan progress: one active session per (chain, address); cursor_block resumes the window loop after any interruption (see docs/address_scan_sessions.md)';
COMMENT ON TABLE address_scan_logs IS 'Raw owner-scoped logs staged per scan window; identity PK makes window re-fetch idempotent; deleted when the session replays into address_scan_tokens';
COMMENT ON TABLE address_scan_tokens IS 'Durable owner-scan discovery result; indexed_at IS NULL = pending, so quota-paced indexing resumes with zero re-scan RPC';

COMMIT;
