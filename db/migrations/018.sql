-- Migration 018: Cross-vendor release abstraction with mint-ordered members
-- See https://github.com/feral-file/ff-indexer-v2/issues/93

BEGIN;

CREATE TABLE releases (
    id BIGSERIAL PRIMARY KEY,
    vendor vendor_type NOT NULL,
    vendor_release_id TEXT NOT NULL,
    name TEXT,
    total_mints BIGINT CHECK (total_mints IS NULL OR total_mints > 0),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (vendor, vendor_release_id)
);

CREATE TABLE release_members (
    id BIGSERIAL PRIMARY KEY,
    release_id BIGINT NOT NULL REFERENCES releases (id) ON DELETE CASCADE,
    token_id BIGINT NOT NULL REFERENCES tokens (id) ON DELETE CASCADE,
    mint_number BIGINT NOT NULL CHECK (mint_number > 0),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (release_id, token_id),
    UNIQUE (release_id, mint_number),
    UNIQUE (token_id)
);

CREATE INDEX release_members_release_id_mint_number_idx ON release_members (release_id, mint_number);

COMMENT ON TABLE releases IS 'Cross-vendor release (FF series, AB project) with stable internal id and external vendor_release_id';
COMMENT ON TABLE release_members IS 'Ordered member tokens for a release; mint_number is authoritative and 1-based';
COMMENT ON COLUMN releases.vendor_release_id IS 'External release key: FF seriesID UUID or AB {chainID}-{contract}-{projectID} (chain-qualified to prevent cross-chain collisions)';
COMMENT ON COLUMN releases.name IS 'Human-readable release title (e.g. "Fidenza"); populated from vendor enrichment';
COMMENT ON COLUMN releases.total_mints IS 'Declared max edition size from vendor (AB max_invocations, FF series.settings.maxArtwork); nullable when unknown';

CREATE TRIGGER update_releases_updated_at
    BEFORE UPDATE ON releases
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

COMMIT;
