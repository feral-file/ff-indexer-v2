-- Migration 002: Add 'objkt' vendor type to vendor_type ENUM
-- This migration adds the 'objkt' vendor type to support objkt v3 API enrichment
-- IMPORTANT: This migration is idempotent and can be run multiple times safely

BEGIN;

-- ============================================================================
-- Step 1: Add 'objkt' to vendor_type ENUM if it doesn't already exist
-- ============================================================================

DO $$ 
BEGIN
    -- Check if 'objkt' already exists in the vendor_type enum
    IF NOT EXISTS (
        SELECT 1 
        FROM pg_enum 
        WHERE enumlabel = 'objkt' 
        AND enumtypid = (
            SELECT oid 
            FROM pg_type 
            WHERE typname = 'vendor_type'
        )
    ) THEN
        -- Add 'objkt' to the vendor_type enum
        -- Using exception handling for extra safety (in case of concurrent execution)
        BEGIN
            ALTER TYPE vendor_type ADD VALUE 'objkt';
            RAISE NOTICE 'Added ''objkt'' to vendor_type enum';
        EXCEPTION
            WHEN OTHERS THEN
                -- If the value already exists (or any other error), log and continue
                IF SQLSTATE = '42710' THEN  -- duplicate_object error code
                    RAISE NOTICE '''objkt'' already exists in vendor_type enum (caught exception), skipping';
                ELSE
                    -- Re-raise if it's a different error
                    RAISE;
                END IF;
        END;
    ELSE
        RAISE NOTICE '''objkt'' already exists in vendor_type enum, skipping';
    END IF;
END $$;

-- ============================================================================
-- Step 2: Verification (informational only)
-- ============================================================================

-- Show all vendor types in the enum
DO $$
DECLARE
    enum_values TEXT;
BEGIN
    SELECT string_agg(enumlabel, ', ' ORDER BY enumsortorder)
    INTO enum_values
    FROM pg_enum
    WHERE enumtypid = (SELECT oid FROM pg_type WHERE typname = 'vendor_type');
    
    RAISE NOTICE '==========================================';
    RAISE NOTICE 'Migration Summary:';
    RAISE NOTICE '  Current vendor_type enum values: %', enum_values;
    RAISE NOTICE '==========================================';
END $$;

COMMIT;

