-- Migration 001: Add token_ownership_periods table
-- This migration adds a new table to track token ownership periods for efficient querying
-- IMPORTANT: Run this migration AFTER the database is initialized with init_pg_db.sql

BEGIN;

-- ============================================================================
-- Step 1: Create token_ownership_periods table
-- ============================================================================

-- Check if table already exists
DO $$ 
BEGIN
    IF NOT EXISTS (SELECT FROM pg_tables WHERE schemaname = 'public' AND tablename = 'token_ownership_periods') THEN
        CREATE TABLE token_ownership_periods (
            id BIGSERIAL PRIMARY KEY,
            token_id BIGINT NOT NULL REFERENCES tokens (id) ON DELETE CASCADE,
            owner_address TEXT NOT NULL,
            acquired_at TIMESTAMPTZ NOT NULL,       -- When the address acquired the token (first transfer in)
            released_at TIMESTAMPTZ,                -- When the address released the token (balance became 0); NULL means still owns it
            created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
        );
        
        -- Create partial unique index to ensure only one active ownership period per token-owner combination
        CREATE UNIQUE INDEX unique_active_token_owner ON token_ownership_periods (token_id, owner_address) WHERE (released_at IS NULL);
        
        -- Apply updated_at trigger to token_ownership_periods
        CREATE TRIGGER update_token_ownership_periods_updated_at 
        BEFORE UPDATE ON token_ownership_periods 
        FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();
        
        RAISE NOTICE 'Created token_ownership_periods table';
    ELSE
        RAISE NOTICE 'Table token_ownership_periods already exists, skipping creation';
    END IF;
END $$;

-- ============================================================================
-- Step 2: Create indexes
-- ============================================================================

-- Composite index for token+owner period lookups
CREATE INDEX IF NOT EXISTS idx_token_ownership_token_owner_periods 
ON token_ownership_periods (token_id, owner_address, acquired_at, released_at);

-- Index for owner-based queries across all tokens
CREATE INDEX IF NOT EXISTS idx_token_ownership_owner_periods 
ON token_ownership_periods (owner_address, acquired_at, released_at);

-- Index for text-based token_id lookups (used in changes_journal queries)
CREATE INDEX IF NOT EXISTS idx_token_ownership_token_id_text_periods 
ON token_ownership_periods (CAST(token_id AS TEXT), owner_address, acquired_at, released_at);

-- Partial index for finding current owners efficiently
CREATE INDEX IF NOT EXISTS idx_token_ownership_current_owners 
ON token_ownership_periods (token_id, owner_address) 
WHERE released_at IS NULL;

-- ============================================================================
-- Step 4: Migrate existing data
-- ============================================================================

-- Only run migration if table is empty (to support re-running this migration safely)
DO $$ 
DECLARE
    row_count INTEGER;
BEGIN
    SELECT COUNT(*) INTO row_count FROM token_ownership_periods;
    
    IF row_count = 0 THEN
        RAISE NOTICE 'Migrating existing ownership data...';
        
        -- ========================================================================
        -- Step 3a: Migrate ERC721 tokens (single owner at a time)
        -- ========================================================================
        INSERT INTO token_ownership_periods (token_id, owner_address, acquired_at, released_at, created_at, updated_at)
        SELECT 
            pe_in.token_id,
            pe_in.to_address AS owner_address,
            pe_in.timestamp AS acquired_at,
            (
                -- Find the next transfer out from this address
                SELECT MIN(pe_out.timestamp)
                FROM provenance_events pe_out
                INNER JOIN tokens t_out ON pe_out.token_id = t_out.id
                WHERE pe_out.token_id = pe_in.token_id
                  AND pe_out.from_address = pe_in.to_address
                  AND pe_out.timestamp > pe_in.timestamp
                  AND t_out.standard = 'erc721'
            ) AS released_at,
            NOW() AS created_at,
            NOW() AS updated_at
        FROM provenance_events pe_in
        INNER JOIN tokens t ON pe_in.token_id = t.id
        WHERE pe_in.to_address IS NOT NULL
          AND pe_in.to_address != ''
          AND pe_in.to_address != '0x0000000000000000000000000000000000000000'
          AND t.standard = 'erc721'
        ORDER BY pe_in.token_id, pe_in.timestamp;
        
        RAISE NOTICE 'Migrated ERC721 ownership periods: % rows', (SELECT COUNT(*) FROM token_ownership_periods WHERE token_id IN (SELECT id FROM tokens WHERE standard = 'erc721'));
        
        -- ========================================================================
        -- Step 3b: Migrate ERC1155/FA2 tokens (multiple owners, quantity-based)
        -- ========================================================================
        
        -- For multi-edition tokens, we need to be more careful
        -- Use current balances and historical events to reconstruct ownership periods
        
        WITH token_owner_pairs AS (
            -- Get all unique (token_id, owner_address) pairs that ever had a balance
            SELECT DISTINCT
                pe.token_id,
                pe.to_address AS owner_address
            FROM provenance_events pe
            INNER JOIN tokens t ON pe.token_id = t.id
            WHERE pe.to_address IS NOT NULL
              AND pe.to_address != ''
              AND pe.to_address != '0x0000000000000000000000000000000000000000'
              AND t.standard IN ('erc1155', 'fa2')
        ),
        first_acquisition AS (
            -- For each pair, find when they first received tokens
            SELECT 
                top.token_id,
                top.owner_address,
                MIN(pe.timestamp) AS acquired_at
            FROM token_owner_pairs top
            INNER JOIN provenance_events pe ON pe.token_id = top.token_id 
                AND pe.to_address = top.owner_address
            GROUP BY top.token_id, top.owner_address
        ),
        balance_check AS (
            -- Check current balance for each pair
            SELECT 
                fa.token_id,
                fa.owner_address,
                fa.acquired_at,
                COALESCE(b.quantity, '0') AS current_quantity
            FROM first_acquisition fa
            LEFT JOIN balances b ON b.token_id = fa.token_id 
                AND b.owner_address = fa.owner_address
        ),
        last_full_transfer_out AS (
            -- For pairs with zero balance, find when balance last became zero
            SELECT 
                bc.token_id,
                bc.owner_address,
                bc.acquired_at,
                CASE 
                    WHEN CAST(bc.current_quantity AS NUMERIC) > 0 THEN NULL
                    ELSE (
                        -- Find the last transfer out that brought balance to 0
                        SELECT MAX(pe_out.timestamp)
                        FROM provenance_events pe_out
                        WHERE pe_out.token_id = bc.token_id
                          AND pe_out.from_address = bc.owner_address
                          AND pe_out.timestamp >= bc.acquired_at
                    )
                END AS released_at
            FROM balance_check bc
        )
        INSERT INTO token_ownership_periods (token_id, owner_address, acquired_at, released_at, created_at, updated_at)
        SELECT 
            lfto.token_id,
            lfto.owner_address,
            lfto.acquired_at,
            lfto.released_at,
            NOW() AS created_at,
            NOW() AS updated_at
        FROM last_full_transfer_out lfto
        ORDER BY lfto.token_id, lfto.acquired_at;
        
        RAISE NOTICE 'Migrated ERC1155/FA2 ownership periods: % rows', (SELECT COUNT(*) FROM token_ownership_periods WHERE token_id IN (SELECT id FROM tokens WHERE standard IN ('erc1155', 'fa2')));
        
    ELSE
        RAISE NOTICE 'token_ownership_periods table already contains data (% rows), skipping migration', row_count;
    END IF;
END $$;

-- ============================================================================
-- Step 4: Verification queries (informational only)
-- ============================================================================

-- Show summary of migrated data
DO $$
DECLARE
    total_periods INTEGER;
    active_periods INTEGER;
    closed_periods INTEGER;
    erc721_periods INTEGER;
    erc1155_fa2_periods INTEGER;
BEGIN
    SELECT COUNT(*) INTO total_periods FROM token_ownership_periods;
    SELECT COUNT(*) INTO active_periods FROM token_ownership_periods WHERE released_at IS NULL;
    SELECT COUNT(*) INTO closed_periods FROM token_ownership_periods WHERE released_at IS NOT NULL;
    SELECT COUNT(*) INTO erc721_periods FROM token_ownership_periods op INNER JOIN tokens t ON op.token_id = t.id WHERE t.standard = 'erc721';
    SELECT COUNT(*) INTO erc1155_fa2_periods FROM token_ownership_periods op INNER JOIN tokens t ON op.token_id = t.id WHERE t.standard IN ('erc1155', 'fa2');
    
    RAISE NOTICE '==========================================';
    RAISE NOTICE 'Migration Summary:';
    RAISE NOTICE '  Total ownership periods: %', total_periods;
    RAISE NOTICE '  Active (released_at IS NULL): %', active_periods;
    RAISE NOTICE '  Closed (released_at NOT NULL): %', closed_periods;
    RAISE NOTICE '  ERC721 periods: %', erc721_periods;
    RAISE NOTICE '  ERC1155/FA2 periods: %', erc1155_fa2_periods;
    RAISE NOTICE '==========================================';
END $$;

COMMIT;

