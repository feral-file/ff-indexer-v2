-- Migration 019: Add vendor_release_slug to releases for URL-based client lookup
-- See plan docs/vendor_release_slug
--
-- vendor_release_slug is the human-readable URL segment from the vendor's website:
--   fxhash: "industrial-park" (from fxhash.xyz/generative/slug/industrial-park)
--   Art Blocks: "fidenza-by-tyler-hobbs" (from artblocks.io/collections/fidenza-by-tyler-hobbs)
--   Feral File: "data-pilgrims-01-769" (from feralfile.com/.../data-pilgrims-01-769)
--   objkt: KT1 contract address (same as vendor_release_id; no human slug in objkt API)
--
-- Nullable because existing rows are backfilled asynchronously via normal enrichment.
-- Partial unique index allows unlimited NULL slugs (not-yet-enriched rows) while
-- still enforcing uniqueness for slugs that are present.

BEGIN;

ALTER TABLE releases ADD COLUMN vendor_release_slug TEXT;

-- Partial unique index: (vendor, vendor_release_slug) only when slug IS NOT NULL.
-- Allows multiple rows with NULL slug; prevents two releases with identical slugs
-- for the same vendor.
CREATE UNIQUE INDEX releases_vendor_slug_idx
    ON releases (vendor, vendor_release_slug)
    WHERE vendor_release_slug IS NOT NULL;

COMMENT ON COLUMN releases.vendor_release_slug IS
    'URL slug for the release on the vendor website; nullable (backfilled via enrichment). '
    'For objkt, equals vendor_release_id (KT1 address). Unique per vendor when not null.';

COMMIT;
