-- Migration 033: remove release memberships in the platform-wide collection of a shared
-- minting contract (ff-indexer-v2 #176).
--
-- Context
-- -------
-- A contract that many artists mint on is never a series. OpenSea files nearly every
-- token on such a contract under one platform-wide collection ("superrare", "fnd",
-- "rarible", "zora"), and the enhancer used to turn that collection into the token's
-- release, so unrelated works by different artists became one series and ff-app grouped
-- them as one tile. The enhancer now drops a release that is one of the platform-wide
-- collections listed for the contract in the registry (tools/registry/publisher.json,
-- "shared_contracts" -> "platform_releases"). A dropped release writes nothing, and
-- release_members is last-writer-wins per token, so rows written before the change stay
-- until this file removes them.
--
-- The match is on the release, not the vendor: OpenSea also carves real series out of
-- these contracts ("xcopy-rarible" on Rarible's ERC-1155, a Gazell.io exhibition on
-- SuperRare), and those memberships stay. SuperRare's second slug below is an OpenSea
-- misfiling (a 34k-item bucket named after an unrelated address) that holds ~12% of its
-- tokens; it is no more a series than "superrare".
--
-- What this file does
-- -------------------
-- Deletes every release_members row whose token is on a contract listed below and whose
-- release is that contract's listed platform-wide collection (same vendor, same vendor
-- release id). The list is the registry's "shared_contracts" at this commit; matching is
-- case-insensitive, like the registry lookup.
--
-- The releases rows stay, even when no member is left (such as the platform-wide
-- "superrare" release the issue reports). A memberless release groups nothing. Deleting
-- it would race enrichment: an OpenSea collection can span shared and other contracts, and
-- a worker that has just upserted the release for a token on another contract would then
-- fail its member insert on the foreign key, which the metadata workflow does not retry.
-- Token enrichment, vendor JSON and moderation verdicts are untouched. Like
-- UpsertReleaseMember, it emits no token_events: clients see the change on their next
-- token read (the API takes release_id and mint_number from release_members).
--
-- Deploy ordering (REQUIRED): code first, then this file
-- ------------------------------------------------------
-- A worker running the previous code re-assigns the platform release whenever it
-- re-enriches a token, writing the rows back. So:
--   1. Sync ff-deploy's registry copy (ansible/app_defaults/indexer/registry/publisher.json)
--      with tools/registry/publisher.json; production reads that copy, and without the
--      "shared_contracts" section the new code drops nothing. The previous image ignores
--      the new section, so the sync and the image bump go out in one ff-deploy commit.
--   2. Deploy the application version that contains metadata.dropPlatformRelease and
--      confirm no process on the previous image is left.
--   3. Run this file:  psql ... -v ON_ERROR_STOP=1 -f db/migrations/033_shared_contract_releases.sql
-- Workers may keep running during step 3: the new code never writes one of these
-- memberships. Re-running is safe (the second run removes nothing).
--
-- A platform release added to the registry later needs the same cleanup for its own rows:
-- run this file's statements with that row in the list.
--
-- Not reversible in SQL. The removed rows are what the defect wrote; restoring them would
-- need the previous code and a re-enrichment.
--
-- Sizing (before) and verification (after: 0)
-- -------------------------------------------
--   SELECT count(*)
--   FROM release_members rm
--   JOIN releases r ON r.id = rm.release_id
--   JOIN tokens t ON t.id = rm.token_id
--   WHERE (t.chain, lower(t.contract_address), r.vendor, lower(r.vendor_release_id)) IN (
--       ('eip155:1', '0xb932a70a57673d89f4acffbe830e8ed7f75fb9e0', 'opensea', 'superrare'),
--       ('eip155:1', '0xb932a70a57673d89f4acffbe830e8ed7f75fb9e0', 'opensea', '0x57f1887a8bf19b14fc0df6fd9b2acc9af147ea85'),
--       ('eip155:1', '0x41a322b28d0ff354040e2cbc676f0320d8c8850d', 'opensea', 'superrare'),
--       ('eip155:1', '0x41a322b28d0ff354040e2cbc676f0320d8c8850d', 'opensea', '0x57f1887a8bf19b14fc0df6fd9b2acc9af147ea85'),
--       ('eip155:1', '0x3b3ee1931dc30c1957379fac9aba94d1c48a5405', 'opensea', 'fnd'),
--       ('eip155:1', '0x60f80121c31a0d46b5279700f9df786054aa5ee5', 'opensea', 'rarible'),
--       ('eip155:1', '0x6a5ff3ceecae9ceb96e6ac6c76b82af8b39f0eb3', 'opensea', 'rarible'),
--       ('eip155:1', '0xf6793da657495ffeff9ee6350824910abc21356c', 'opensea', 'rarible'),
--       ('eip155:1', '0xc9154424b823b10579895ccbe442d41b9abd96ed', 'opensea', 'rarible'),
--       ('eip155:1', '0xe03e4721c75188de6dd9ccf4fc0eb7c81399e305', 'opensea', 'rarible'),
--       ('eip155:1', '0xd07dc4262bcdbf85190c01c996b4c06a461d2430', 'opensea', 'rarible'),
--       ('eip155:1', '0xb66a603f4cfe17e3d27b87a8bfcad319856518b8', 'opensea', 'rarible'),
--       ('eip155:1', '0xabefbc9fd2f806065b4f3c237d4b59d9a97bcac7', 'opensea', 'zora'),
--       ('tezos:mainnet', 'kt1rj6pbjhpwc3m5rw5s2nbmefwbuwbdxton', 'objkt', 'kt1rj6pbjhpwc3m5rw5s2nbmefwbuwbdxton'));

BEGIN;

CREATE TEMP TABLE migration_033_platform_releases (
    chain blockchain_chain NOT NULL,
    address TEXT NOT NULL,
    vendor vendor_type NOT NULL,
    vendor_release_id TEXT NOT NULL
) ON COMMIT DROP;

INSERT INTO migration_033_platform_releases (chain, address, vendor, vendor_release_id) VALUES
    ('eip155:1', '0xb932a70a57673d89f4acffbe830e8ed7f75fb9e0', 'opensea', 'superrare'),                                   -- SuperRare (SUPR)
    ('eip155:1', '0xb932a70a57673d89f4acffbe830e8ed7f75fb9e0', 'opensea', '0x57f1887a8bf19b14fc0df6fd9b2acc9af147ea85'),  -- SuperRare (SUPR), OpenSea's misfiled bucket
    ('eip155:1', '0x41A322b28D0fF354040e2CbC676F0320d8c8850d', 'opensea', 'superrare'),                                   -- SupeRare v1 (SUPR)
    ('eip155:1', '0x41A322b28D0fF354040e2CbC676F0320d8c8850d', 'opensea', '0x57f1887a8bf19b14fc0df6fd9b2acc9af147ea85'),  -- SupeRare v1 (SUPR), OpenSea's misfiled bucket
    ('eip155:1', '0x3B3ee1931Dc30C1957379FAc9aba94D1C48a5405', 'opensea', 'fnd'),                                         -- Foundation (FND)
    ('eip155:1', '0x60F80121C31A0d46B5279700f9DF786054aa5eE5', 'opensea', 'rarible'),                                     -- Rarible ERC-721 (RARI)
    ('eip155:1', '0x6A5Ff3CEEcaE9CeB96E6ac6c76B82aF8B39f0Eb3', 'opensea', 'rarible'),                                     -- Rarible ERC-721, 2020 (RARI)
    ('eip155:1', '0xF6793dA657495ffeFF9Ee6350824910Abc21356C', 'opensea', 'rarible'),                                     -- Rarible ERC-721, 2020 (RARI)
    ('eip155:1', '0xc9154424B823b10579895cCBE442d41b9Abd96Ed', 'opensea', 'rarible'),                                     -- Rarible ERC-721 (RARI)
    ('eip155:1', '0xe03e4721c75188de6DD9ccf4Fc0eb7C81399E305', 'opensea', 'rarible'),                                     -- Rarible ERC-721 (RARI)
    ('eip155:1', '0xd07dc4262BCDbf85190C01c996b4C06a461d2430', 'opensea', 'rarible'),                                     -- Rarible ERC-1155 (RARI)
    ('eip155:1', '0xB66a603f4cFe17e3D27B87a8BfCaD319856518B8', 'opensea', 'rarible'),                                     -- Rarible ERC-1155 (RARI)
    ('eip155:1', '0xabEFBc9fD2F806065b4f3C237d4b59D9A97Bcac7', 'opensea', 'zora'),                                        -- Zora (ZORA)
    ('tezos:mainnet', 'KT1RJ6PbjHpwc3M5rw5s2Nbmefwbuwbdxton', 'objkt', 'KT1RJ6PbjHpwc3M5rw5s2Nbmefwbuwbdxton');          -- hic et nunc OBJKT

-- Memberships to remove. Starts from release_members (only tokens that have a release)
-- and reaches tokens and releases by primary key, so the case-insensitive match needs
-- no index.
CREATE TEMP TABLE migration_033_removed ON COMMIT DROP AS
SELECT rm.id, rm.release_id
FROM release_members rm
JOIN releases r ON r.id = rm.release_id
JOIN tokens t ON t.id = rm.token_id
JOIN migration_033_platform_releases p
  ON p.chain = t.chain
 AND lower(p.address) = lower(t.contract_address)
 AND p.vendor = r.vendor
 AND lower(p.vendor_release_id) = lower(r.vendor_release_id);

DELETE FROM release_members
WHERE id IN (SELECT id FROM migration_033_removed);

SELECT count(*) AS release_members_removed,
       count(DISTINCT release_id) AS releases_touched
FROM migration_033_removed;

COMMIT;
