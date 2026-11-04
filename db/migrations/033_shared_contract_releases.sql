-- Migration 033: remove release memberships that a collection-level vendor assigned to
-- tokens on a shared minting contract (ff-indexer-v2 #176).
--
-- Context
-- -------
-- A contract that many artists mint on is never a series. OpenSea files every token on
-- such a contract under one collection (all of SuperRare's shared contract is "SuperRare"),
-- and the enhancer used to turn that collection into the token's release, so unrelated
-- works by different artists became one series and ff-app grouped them as one tile. The
-- enhancer now drops a release from a collection-level vendor (OpenSea, objkt: any vendor
-- other than artblocks, fxhash, feralfile) on the registry's shared contracts
-- (tools/registry/publisher.json, "shared_contracts"). A dropped release writes nothing,
-- and release_members is last-writer-wins per token, so rows written before the change
-- stay until this file removes them.
--
-- What this file does
-- -------------------
--   1. Deletes every release_members row whose token is on a contract listed below and
--      whose release came from a vendor other than artblocks, fxhash or feralfile. The
--      list is the registry's "shared_contracts" at this commit; matching is
--      case-insensitive, like the registry lookup.
--   2. Deletes the releases those rows belonged to when no member is left (such as the
--      platform-wide "SuperRare" release the issue reports). A release that still has
--      members on other contracts is kept.
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
--      the new section, so the sync can go out first.
--   2. Deploy the application version that contains metadata.dropSharedContractRelease
--      and confirm no process on the previous image is left.
--   3. Run this file:  psql ... -v ON_ERROR_STOP=1 -f db/migrations/033_shared_contract_releases.sql
-- Workers may keep running during step 3: the new code never writes a membership on these
-- contracts. Re-running is safe (the second run removes nothing).
--
-- A shared contract added to the registry later needs the same cleanup for its own rows:
-- run this file's statements with that contract in the list.
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
--   WHERE r.vendor NOT IN ('artblocks', 'fxhash', 'feralfile')
--     AND (t.chain, lower(t.contract_address)) IN (
--       ('eip155:1', '0xb932a70a57673d89f4acffbe830e8ed7f75fb9e0'),
--       ('eip155:1', '0x41a322b28d0ff354040e2cbc676f0320d8c8850d'),
--       ('eip155:1', '0x3b3ee1931dc30c1957379fac9aba94d1c48a5405'),
--       ('eip155:1', '0x495f947276749ce646f68ac8c248420045cb7b5e'),
--       ('eip155:1', '0x60f80121c31a0d46b5279700f9df786054aa5ee5'),
--       ('eip155:1', '0xd07dc4262bcdbf85190c01c996b4c06a461d2430'),
--       ('eip155:1', '0xabefbc9fd2f806065b4f3c237d4b59d9a97bcac7'),
--       ('tezos:mainnet', 'kt1rj6pbjhpwc3m5rw5s2nbmefwbuwbdxton'));

BEGIN;

CREATE TEMP TABLE migration_033_shared_contracts (
    chain blockchain_chain NOT NULL,
    address TEXT NOT NULL
) ON COMMIT DROP;

INSERT INTO migration_033_shared_contracts (chain, address) VALUES
    ('eip155:1', '0xb932a70a57673d89f4acffbe830e8ed7f75fb9e0'),      -- SuperRare (SUPR)
    ('eip155:1', '0x41A322b28D0fF354040e2CbC676F0320d8c8850d'),      -- SupeRare v1 (SUPR)
    ('eip155:1', '0x3B3ee1931Dc30C1957379FAc9aba94D1C48a5405'),      -- Foundation (FND)
    ('eip155:1', '0x495f947276749Ce646f68AC8c248420045cb7b5e'),      -- OpenSea Shared Storefront
    ('eip155:1', '0x60F80121C31A0d46B5279700f9DF786054aa5eE5'),      -- Rarible ERC-721 (RARI)
    ('eip155:1', '0xd07dc4262BCDbf85190C01c996b4C06a461d2430'),      -- Rarible ERC-1155 (RARI)
    ('eip155:1', '0xabEFBc9fD2F806065b4f3C237d4b59D9A97Bcac7'),      -- Zora (ZORA)
    ('tezos:mainnet', 'KT1RJ6PbjHpwc3M5rw5s2Nbmefwbuwbdxton');      -- hic et nunc OBJKT

-- Memberships to remove. Starts from release_members (only tokens that have a release)
-- and reaches tokens by primary key, so the case-insensitive match needs no index.
CREATE TEMP TABLE migration_033_removed ON COMMIT DROP AS
SELECT rm.id, rm.release_id
FROM release_members rm
JOIN releases r ON r.id = rm.release_id
JOIN tokens t ON t.id = rm.token_id
JOIN migration_033_shared_contracts s
  ON s.chain = t.chain AND lower(s.address) = lower(t.contract_address)
WHERE r.vendor NOT IN ('artblocks', 'fxhash', 'feralfile');

DELETE FROM release_members
WHERE id IN (SELECT id FROM migration_033_removed);

DELETE FROM releases r
WHERE r.id IN (SELECT release_id FROM migration_033_removed)
  AND NOT EXISTS (SELECT 1 FROM release_members rm WHERE rm.release_id = r.id);

SELECT count(*) AS release_members_removed,
       count(DISTINCT release_id) AS releases_touched
FROM migration_033_removed;

COMMIT;
