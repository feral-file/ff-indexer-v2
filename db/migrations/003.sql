-- Migration 003: Add OpenSea to vendor_type enum
-- This migration adds 'opensea' as a new vendor type for enrichment sources

-- Add 'opensea' to the vendor_type enum
ALTER TYPE vendor_type ADD VALUE 'opensea';

-- Add comment
COMMENT ON TYPE vendor_type IS 'Vendor types for metadata enrichment: artblocks, fxhash, foundation, superrare, feralfile, objkt, opensea';

