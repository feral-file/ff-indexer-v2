-- Test Seed Data for FF Indexer v2
-- This file contains realistic test data for various scenarios
-- Used by pg_test.go for integration testing

-- =============================================================================
-- Sample Tokens
-- =============================================================================

-- ERC721 Token 1 (Ethereum Mainnet) - Single owner, with full metadata and healthy media
INSERT INTO tokens (id, token_cid, chain, standard, contract_address, token_number, current_owner, burned, is_viewable, last_provenance_timestamp, created_at, updated_at)
VALUES (
    1,
    'eip155:1:erc721:0xBC4CA0EdA7647A8aB7C2061c2E118A18a936f13D:1',
    'eip155:1',
    'erc721',
    '0xBC4CA0EdA7647A8aB7C2061c2E118A18a936f13D',
    '1',
    '0x1234567890123456789012345678901234567890',
    false,
    true, -- Has healthy image URLs
    now() - interval '30 days', -- last_provenance_timestamp (mint event)
    now() - interval '30 days',
    now() - interval '30 days'
);

-- ERC721 Token 2 (Ethereum Mainnet) - Different owner, with metadata and healthy media
INSERT INTO tokens (id, token_cid, chain, standard, contract_address, token_number, current_owner, burned, is_viewable, last_provenance_timestamp, created_at, updated_at)
VALUES (
    2,
    'eip155:1:erc721:0xBC4CA0EdA7647A8aB7C2061c2E118A18a936f13D:2',
    'eip155:1',
    'erc721',
    '0xBC4CA0EdA7647A8aB7C2061c2E118A18a936f13D',
    '2',
    '0xabcdefabcdefabcdefabcdefabcdefabcdefabcd',
    false,
    true, -- Has healthy image URLs
    now() - interval '25 days', -- last_provenance_timestamp (mint event)
    now() - interval '25 days',
    now() - interval '25 days'
);

-- ERC721 Token 3 (Ethereum Sepolia) - Test network, no media health
INSERT INTO tokens (id, token_cid, chain, standard, contract_address, token_number, current_owner, burned, is_viewable, last_provenance_timestamp, created_at, updated_at)
VALUES (
    3,
    'eip155:11155111:erc721:0x1111111111111111111111111111111111111111:1',
    'eip155:11155111',
    'erc721',
    '0x1111111111111111111111111111111111111111',
    '1',
    '0x2222222222222222222222222222222222222222',
    false,
    false, -- No media health records
    now() - interval '20 days', -- last_provenance_timestamp (mint event)
    now() - interval '20 days',
    now() - interval '20 days'
);

-- ERC1155 Token 1 (Ethereum Mainnet) - Multi-owner token with healthy animation
INSERT INTO tokens (id, token_cid, chain, standard, contract_address, token_number, current_owner, burned, is_viewable, last_provenance_timestamp, created_at, updated_at)
VALUES (
    4,
    'eip155:1:erc1155:0xd07dc4262BCDbf85190C01c996b4C06a461d2430:1',
    'eip155:1',
    'erc1155',
    '0xd07dc4262BCDbf85190C01c996b4C06a461d2430',
    '1',
    NULL,
    false,
    true, -- Has healthy animation URLs (takes priority)
    now() - interval '10 days', -- last_provenance_timestamp (latest transfer)
    now() - interval '15 days',
    now() - interval '15 days'
);

-- ERC721 Token 4 (Burned token) - no media health
INSERT INTO tokens (id, token_cid, chain, standard, contract_address, token_number, current_owner, burned, is_viewable, last_provenance_timestamp, created_at, updated_at)
VALUES (
    5,
    'eip155:1:erc721:0x3333333333333333333333333333333333333333:999',
    'eip155:1',
    'erc721',
    '0x3333333333333333333333333333333333333333',
    '999',
    NULL,
    true,
    false, -- Burned token, no media
    now() - interval '5 days', -- last_provenance_timestamp (burn event)
    now() - interval '10 days',
    now() - interval '5 days'
);

-- FA2 Token 1 (Tezos Mainnet) - no media health
INSERT INTO tokens (id, token_cid, chain, standard, contract_address, token_number, current_owner, burned, is_viewable, last_provenance_timestamp, created_at, updated_at)
VALUES (
    6,
    'tezos:mainnet:fa2:KT1RJ6PbjHpwc3M5rw5s2Nbmefwbuwbdxton:0',
    'tezos:mainnet',
    'fa2',
    'KT1RJ6PbjHpwc3M5rw5s2Nbmefwbuwbdxton',
    '0',
    NULL,
    false,
    false, -- No media health records
    now() - interval '8 days', -- last_provenance_timestamp (mint event)
    now() - interval '8 days',
    now() - interval '8 days'
);

-- Reset sequence
SELECT setval('tokens_id_seq', 100, true);

-- =============================================================================
-- Balances
-- =============================================================================

-- Balance for Token 1 (ERC721 - single owner)
INSERT INTO balances (id, token_id, owner_address, quantity, created_at, updated_at)
VALUES (
    1,
    1,
    '0x1234567890123456789012345678901234567890',
    '1',
    now() - interval '30 days',
    now() - interval '30 days'
);

-- Balance for Token 2 (ERC721 - single owner)
INSERT INTO balances (id, token_id, owner_address, quantity, created_at, updated_at)
VALUES (
    2,
    2,
    '0xabcdefabcdefabcdefabcdefabcdefabcdefabcd',
    '1',
    now() - interval '25 days',
    now() - interval '25 days'
);

-- Balance for Token 3 (ERC721 - single owner)
INSERT INTO balances (id, token_id, owner_address, quantity, created_at, updated_at)
VALUES (
    3,
    3,
    '0x2222222222222222222222222222222222222222',
    '1',
    now() - interval '20 days',
    now() - interval '20 days'
);

-- Balances for Token 4 (ERC1155 - multiple owners)
INSERT INTO balances (id, token_id, owner_address, quantity, created_at, updated_at)
VALUES (
    4,
    4,
    '0x4444444444444444444444444444444444444444',
    '50',
    now() - interval '15 days',
    now() - interval '15 days'
);

INSERT INTO balances (id, token_id, owner_address, quantity, created_at, updated_at)
VALUES (
    5,
    4,
    '0x5555555555555555555555555555555555555555',
    '30',
    now() - interval '12 days',
    now() - interval '12 days'
);

INSERT INTO balances (id, token_id, owner_address, quantity, created_at, updated_at)
VALUES (
    6,
    4,
    '0x6666666666666666666666666666666666666666',
    '20',
    now() - interval '10 days',
    now() - interval '10 days'
);

-- Balance for Token 6 (FA2 - Tezos)
INSERT INTO balances (id, token_id, owner_address, quantity, created_at, updated_at)
VALUES (
    7,
    6,
    'tz1VSUr8wwNhLAzempoch5d6hLRiTh8Cjcjb',
    '1',
    now() - interval '8 days',
    now() - interval '8 days'
);

-- Reset sequence
SELECT setval('balances_id_seq', 100, true);

-- =============================================================================
-- Token Metadata
-- =============================================================================

-- Metadata for Token 1
INSERT INTO token_metadata (
    token_id, origin_json, latest_json, latest_hash, enrichment_level,
    last_refreshed_at, image_url, image_url_hash, animation_url, animation_url_hash, name, description, artists, publisher, mime_type,
    created_at, updated_at
)
VALUES (
    1,
    '{"name":"Bored Ape #1","image":"ipfs://QmTest1","attributes":[{"trait_type":"Background","value":"Blue"}]}',
    '{"name":"Bored Ape #1","image":"ipfs://QmTest1","attributes":[{"trait_type":"Background","value":"Blue"}]}',
    'hash_token1',
    'vendor',
    now() - interval '30 days',
    'https://example.com/bayc/1.png',
    md5('https://example.com/bayc/1.png'),
    NULL,
    NULL,
    'Bored Ape #1',
    'A bored ape from the yacht club',
    '[{"did":"did:pkh:eip155:1:0x1234567890123456789012345678901234567890","name":"Yuga Labs"}]',
    '{"name":"OpenSea","url":"https://opensea.io"}',
    'image/png',
    now() - interval '30 days',
    now() - interval '29 days'
);

-- Metadata for Token 2
INSERT INTO token_metadata (
    token_id, origin_json, latest_json, latest_hash, enrichment_level,
    last_refreshed_at, image_url, image_url_hash, name, mime_type,
    created_at, updated_at
)
VALUES (
    2,
    '{"name":"Bored Ape #2","image":"ipfs://QmTest2"}',
    '{"name":"Bored Ape #2","image":"ipfs://QmTest2"}',
    'hash_token2',
    'none',
    now() - interval '25 days',
    'https://example.com/bayc/2.png',
    md5('https://example.com/bayc/2.png'),
    'Bored Ape #2',
    'image/png',
    now() - interval '25 days',
    now() - interval '25 days'
);

-- Metadata for Token 4 (ERC1155)
INSERT INTO token_metadata (
    token_id, origin_json, latest_json, latest_hash, enrichment_level,
    last_refreshed_at, image_url, image_url_hash, animation_url, animation_url_hash, name, mime_type,
    created_at, updated_at
)
VALUES (
    4,
    '{"name":"Art Block #1","image":"ipfs://QmTest4","animation_url":"ipfs://QmTest4Anim"}',
    '{"name":"Art Block #1","image":"ipfs://QmTest4","animation_url":"ipfs://QmTest4Anim"}',
    'hash_token4',
    'vendor',
    now() - interval '15 days',
    'https://example.com/artblock/1.png',
    md5('https://example.com/artblock/1.png'),
    'https://example.com/artblock/1.mp4',
    md5('https://example.com/artblock/1.mp4'),
    'Art Block #1',
    'video/mp4',
    now() - interval '15 days',
    now() - interval '14 days'
);

-- =============================================================================
-- Enrichment Sources
-- =============================================================================

-- Enrichment source for Token 1
INSERT INTO enrichment_sources (
    token_id, vendor, vendor_json, vendor_hash,
    image_url, image_url_hash, animation_url, animation_url_hash, name, description, artists, mime_type,
    created_at, updated_at
)
VALUES (
    1,
    'artblocks',
    '{"platform":"artblocks","project":"chromie-squiggle","token":"1"}',
    'vendor_hash_1',
    'https://artblocks.io/images/1.png',
    md5('https://artblocks.io/images/1.png'),
    NULL,
    NULL,
    'Chromie Squiggle #1',
    'A generative art piece from Art Blocks',
    '[{"did":"did:pkh:eip155:1:0xsnowfro123456789012345678901234567890","name":"Snowfro"}]',
    'image/png',
    now() - interval '29 days',
    now() - interval '29 days'
);

-- Enrichment source for Token 4
INSERT INTO enrichment_sources (
    token_id, vendor, vendor_json, vendor_hash,
    image_url, image_url_hash, animation_url, animation_url_hash, name, mime_type,
    created_at, updated_at
)
VALUES (
    4,
    'fxhash',
    '{"platform":"fxhash","project":"test","token":"1"}',
    'vendor_hash_4',
    'https://fxhash.xyz/images/1.png',
    md5('https://fxhash.xyz/images/1.png'),
    'https://fxhash.xyz/videos/1.mp4',
    md5('https://fxhash.xyz/videos/1.mp4'),
    'fxhash Genesis #1',
    'video/mp4',
    now() - interval '14 days',
    now() - interval '14 days'
);

-- Enrichment source for Token 2 (using OpenSea)
INSERT INTO enrichment_sources (
    token_id, vendor, vendor_json, vendor_hash,
    image_url, image_url_hash, name, description, artists, mime_type,
    created_at, updated_at
)
VALUES (
    2,
    'opensea',
    '{"identifier":"2","collection":"bored-ape-yacht-club","contract":"0xBC4CA0EdA7647A8aB7C2061c2E118A18a936f13D","name":"Bored Ape #2","description":"BAYC from OpenSea","image_url":"https://opensea.io/images/2.png"}',
    'vendor_hash_opensea_2',
    'https://opensea.io/images/2.png',
    md5('https://opensea.io/images/2.png'),
    'Bored Ape #2',
    'BAYC from OpenSea',
    '[{"did":"did:pkh:eip155:1:0xartist789012345678901234567890","name":"BAYC Artist"}]',
    'image/png',
    now() - interval '24 days',
    now() - interval '24 days'
);

-- =============================================================================
-- Provenance Events
-- =============================================================================

-- Mint event for Token 1
INSERT INTO provenance_events (
    id, token_id, chain, event_type, from_address, to_address, quantity,
    tx_hash, block_number, block_hash, timestamp, raw,
    created_at, updated_at
)
VALUES (
    1,
    1,
    'eip155:1',
    'mint',
    '0x0000000000000000000000000000000000000000',
    '0x1234567890123456789012345678901234567890',
    '1',
    '0xmint_token1_tx',
    15000000,
    '0xblock_mint1',
    now() - interval '30 days',
    '{"tx_hash":"0xmint_token1_tx","block_number":15000000,"tx_index":10}',
    now() - interval '30 days',
    now() - interval '30 days'
);

-- Mint event for Token 2
INSERT INTO provenance_events (
    id, token_id, chain, event_type, from_address, to_address, quantity,
    tx_hash, block_number, block_hash, timestamp, raw,
    created_at, updated_at
)
VALUES (
    2,
    2,
    'eip155:1',
    'mint',
    '0x0000000000000000000000000000000000000000',
    '0xabcdefabcdefabcdefabcdefabcdefabcdefabcd',
    '1',
    '0xmint_token2_tx',
    15000100,
    '0xblock_mint2',
    now() - interval '25 days',
    '{"tx_hash":"0xmint_token2_tx","block_number":15000100,"tx_index":5}',
    now() - interval '25 days',
    now() - interval '25 days'
);

-- Mint event for Token 3
INSERT INTO provenance_events (
    id, token_id, chain, event_type, from_address, to_address, quantity,
    tx_hash, block_number, block_hash, timestamp, raw,
    created_at, updated_at
)
VALUES (
    3,
    3,
    'eip155:11155111',
    'mint',
    '0x0000000000000000000000000000000000000000',
    '0x2222222222222222222222222222222222222222',
    '1',
    '0xmint_token3_tx',
    1000000,
    '0xblock_mint3',
    now() - interval '20 days',
    '{"tx_hash":"0xmint_token3_tx","block_number":1000000,"tx_index":1}',
    now() - interval '20 days',
    now() - interval '20 days'
);

-- Mint event for Token 4 (ERC1155 - 100 tokens)
INSERT INTO provenance_events (
    id, token_id, chain, event_type, from_address, to_address, quantity,
    tx_hash, block_number, block_hash, timestamp, raw,
    created_at, updated_at
)
VALUES (
    4,
    4,
    'eip155:1',
    'mint',
    '0x0000000000000000000000000000000000000000',
    '0x4444444444444444444444444444444444444444',
    '100',
    '0xmint_token4_tx',
    15000200,
    '0xblock_mint4',
    now() - interval '15 days',
    '{"tx_hash":"0xmint_token4_tx","block_number":15000200,"tx_index":20}',
    now() - interval '15 days',
    now() - interval '15 days'
);

-- Transfer events for Token 4 (ERC1155)
INSERT INTO provenance_events (
    id, token_id, chain, event_type, from_address, to_address, quantity,
    tx_hash, block_number, block_hash, timestamp, raw,
    created_at, updated_at
)
VALUES (
    5,
    4,
    'eip155:1',
    'transfer',
    '0x4444444444444444444444444444444444444444',
    '0x5555555555555555555555555555555555555555',
    '30',
    '0xtransfer_token4_tx1',
    15000300,
    '0xblock_transfer4_1',
    now() - interval '12 days',
    '{"tx_hash":"0xtransfer_token4_tx1","block_number":15000300,"tx_index":15}',
    now() - interval '12 days',
    now() - interval '12 days'
);

INSERT INTO provenance_events (
    id, token_id, chain, event_type, from_address, to_address, quantity,
    tx_hash, block_number, block_hash, timestamp, raw,
    created_at, updated_at
)
VALUES (
    6,
    4,
    'eip155:1',
    'transfer',
    '0x4444444444444444444444444444444444444444',
    '0x6666666666666666666666666666666666666666',
    '20',
    '0xtransfer_token4_tx2',
    15000400,
    '0xblock_transfer4_2',
    now() - interval '10 days',
    '{"tx_hash":"0xtransfer_token4_tx2","block_number":15000400,"tx_index":8}',
    now() - interval '10 days',
    now() - interval '10 days'
);

-- Mint and Burn events for Token 5
INSERT INTO provenance_events (
    id, token_id, chain, event_type, from_address, to_address, quantity,
    tx_hash, block_number, block_hash, timestamp, raw,
    created_at, updated_at
)
VALUES (
    7,
    5,
    'eip155:1',
    'mint',
    '0x0000000000000000000000000000000000000000',
    '0x3333333333333333333333333333333333333333',
    '1',
    '0xmint_token5_tx',
    15000500,
    '0xblock_mint5',
    now() - interval '10 days',
    '{"tx_hash":"0xmint_token5_tx","block_number":15000500,"tx_index":3}',
    now() - interval '10 days',
    now() - interval '10 days'
);

INSERT INTO provenance_events (
    id, token_id, chain, event_type, from_address, to_address, quantity,
    tx_hash, block_number, block_hash, timestamp, raw,
    created_at, updated_at
)
VALUES (
    8,
    5,
    'eip155:1',
    'burn',
    '0x3333333333333333333333333333333333333333',
    '0x0000000000000000000000000000000000000000',
    '1',
    '0xburn_token5_tx',
    15000600,
    '0xblock_burn5',
    now() - interval '5 days',
    '{"tx_hash":"0xburn_token5_tx","block_number":15000600,"tx_index":12}',
    now() - interval '5 days',
    now() - interval '5 days'
);

-- Mint event for Token 6 (FA2 - Tezos)
INSERT INTO provenance_events (
    id, token_id, chain, event_type, from_address, to_address, quantity,
    tx_hash, block_number, block_hash, timestamp, raw,
    created_at, updated_at
)
VALUES (
    9,
    6,
    'tezos:mainnet',
    'mint',
    'tz1burnburnburnburnburnburnburjAYjjX',
    'tz1VSUr8wwNhLAzempoch5d6hLRiTh8Cjcjb',
    '1',
    'onxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx',
    3000000,
    NULL,
    now() - interval '8 days',
    '{"operation_hash":"onxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx","level":3000000}',
    now() - interval '8 days',
    now() - interval '8 days'
);

-- Reset sequence
SELECT setval('provenance_events_id_seq', 100, true);

-- =============================================================================
-- Token Ownership Provenance (Latest per owner)
-- =============================================================================

-- Token 1: Current owner is 0x1234... (from mint)
INSERT INTO token_ownership_provenance (token_id, owner_address, last_timestamp, last_tx_index, last_event_type, created_at, updated_at)
VALUES (
    1,
    '0x1234567890123456789012345678901234567890',
    now() - interval '30 days',
    10,
    'mint',
    now() - interval '30 days',
    now() - interval '30 days'
);

-- Token 2: Current owner is 0xabcdef... (from mint)
INSERT INTO token_ownership_provenance (token_id, owner_address, last_timestamp, last_tx_index, last_event_type, created_at, updated_at)
VALUES (
    2,
    '0xabcdefabcdefabcdefabcdefabcdefabcdefabcd',
    now() - interval '25 days',
    5,
    'mint',
    now() - interval '25 days',
    now() - interval '25 days'
);

-- Token 3: Current owner is 0x2222... (from mint)
INSERT INTO token_ownership_provenance (token_id, owner_address, last_timestamp, last_tx_index, last_event_type, created_at, updated_at)
VALUES (
    3,
    '0x2222222222222222222222222222222222222222',
    now() - interval '20 days',
    1,
    'mint',
    now() - interval '20 days',
    now() - interval '20 days'
);

-- Token 4: Three current owners (ERC1155 - multi-owner)
-- Owner 1: 0x4444... (from mint at 15 days ago)
INSERT INTO token_ownership_provenance (token_id, owner_address, last_timestamp, last_tx_index, last_event_type, created_at, updated_at)
VALUES (
    4,
    '0x4444444444444444444444444444444444444444',
    now() - interval '15 days',
    20,
    'mint',
    now() - interval '15 days',
    now() - interval '15 days'
);

-- Owner 2: 0x5555... (from transfer at 12 days ago)
INSERT INTO token_ownership_provenance (token_id, owner_address, last_timestamp, last_tx_index, last_event_type, created_at, updated_at)
VALUES (
    4,
    '0x5555555555555555555555555555555555555555',
    now() - interval '12 days',
    15,
    'transfer',
    now() - interval '12 days',
    now() - interval '12 days'
);

-- Owner 3: 0x6666... (from transfer at 10 days ago - most recent for this token)
INSERT INTO token_ownership_provenance (token_id, owner_address, last_timestamp, last_tx_index, last_event_type, created_at, updated_at)
VALUES (
    4,
    '0x6666666666666666666666666666666666666666',
    now() - interval '10 days',
    8,
    'transfer',
    now() - interval '10 days',
    now() - interval '10 days'
);

-- Token 5: Burned, last owner was 0x3333... (burn event at 5 days ago)
-- Note: We don't track the burn event for the "to_address" (0x000...000)

-- Token 6: Current owner is tz1VSUr... (from mint)
INSERT INTO token_ownership_provenance (token_id, owner_address, last_timestamp, last_tx_index, last_event_type, created_at, updated_at)
VALUES (
    6,
    'tz1VSUr8wwNhLAzempoch5d6hLRiTh8Cjcjb',
    now() - interval '8 days',
    0, -- Tezos uses 'level' in raw JSON, but we'll use 0 for simplicity in test data
    'mint',
    now() - interval '8 days',
    now() - interval '8 days'
);

-- =============================================================================
-- Token Ownership Periods
-- =============================================================================

-- Ownership period for Token 1 (ERC721 - still owned by minter)
INSERT INTO token_ownership_periods (
    id, token_id, owner_address, acquired_at, released_at,
    created_at, updated_at
)
VALUES (
    1,
    1,
    '0x1234567890123456789012345678901234567890',
    now() - interval '30 days',
    NULL, -- Still owned
    now() - interval '30 days',
    now() - interval '30 days'
);

-- Ownership period for Token 2 (ERC721 - still owned by minter)
INSERT INTO token_ownership_periods (
    id, token_id, owner_address, acquired_at, released_at,
    created_at, updated_at
)
VALUES (
    2,
    2,
    '0xabcdefabcdefabcdefabcdefabcdefabcdefabcd',
    now() - interval '25 days',
    NULL, -- Still owned
    now() - interval '25 days',
    now() - interval '25 days'
);

-- Ownership period for Token 3 (ERC721 - still owned by minter)
INSERT INTO token_ownership_periods (
    id, token_id, owner_address, acquired_at, released_at,
    created_at, updated_at
)
VALUES (
    3,
    3,
    '0x2222222222222222222222222222222222222222',
    now() - interval '20 days',
    NULL, -- Still owned
    now() - interval '20 days',
    now() - interval '20 days'
);

-- Ownership periods for Token 4 (ERC1155 - multiple owners)
-- Owner 1: Original minter (still owns 50 tokens)
INSERT INTO token_ownership_periods (
    id, token_id, owner_address, acquired_at, released_at,
    created_at, updated_at
)
VALUES (
    4,
    4,
    '0x4444444444444444444444444444444444444444',
    now() - interval '15 days',
    NULL, -- Still owns 50 tokens
    now() - interval '15 days',
    now() - interval '15 days'
);

-- Owner 2: Received 30 tokens via first transfer
INSERT INTO token_ownership_periods (
    id, token_id, owner_address, acquired_at, released_at,
    created_at, updated_at
)
VALUES (
    5,
    4,
    '0x5555555555555555555555555555555555555555',
    now() - interval '12 days',
    NULL, -- Still owns 30 tokens
    now() - interval '12 days',
    now() - interval '12 days'
);

-- Owner 3: Received 20 tokens via second transfer
INSERT INTO token_ownership_periods (
    id, token_id, owner_address, acquired_at, released_at,
    created_at, updated_at
)
VALUES (
    6,
    4,
    '0x6666666666666666666666666666666666666666',
    now() - interval '10 days',
    NULL, -- Still owns 20 tokens
    now() - interval '10 days',
    now() - interval '10 days'
);

-- Ownership period for Token 5 (ERC721 - burned, period closed)
INSERT INTO token_ownership_periods (
    id, token_id, owner_address, acquired_at, released_at,
    created_at, updated_at
)
VALUES (
    7,
    5,
    '0x3333333333333333333333333333333333333333',
    now() - interval '10 days',
    now() - interval '5 days', -- Released when burned
    now() - interval '10 days',
    now() - interval '10 days'
);

-- Ownership period for Token 6 (FA2 - Tezos, still owned)
INSERT INTO token_ownership_periods (
    id, token_id, owner_address, acquired_at, released_at,
    created_at, updated_at
)
VALUES (
    8,
    6,
    'tz1VSUr8wwNhLAzempoch5d6hLRiTh8Cjcjb',
    now() - interval '8 days',
    NULL, -- Still owned
    now() - interval '8 days',
    now() - interval '8 days'
);

-- Reset sequence
SELECT setval('token_ownership_periods_id_seq', 100, true);

-- =============================================================================
-- Changes Journal
-- =============================================================================

-- Change for Token 1 mint
INSERT INTO changes_journal (
    id, subject_type, subject_id, changed_at, meta,
    created_at, updated_at
)
VALUES (
    1,
    'token',
    '1',
    now() - interval '30 days',
    '{"chain":"eip155:1","standard":"erc721","contract":"0xBC4CA0EdA7647A8aB7C2061c2E118A18a936f13D","token":"1","from":"0x0000000000000000000000000000000000000000","to":"0x1234567890123456789012345678901234567890","quantity":"1"}',
    now() - interval '30 days',
    now() - interval '30 days'
);

-- Change for Token 2 mint
INSERT INTO changes_journal (
    id, subject_type, subject_id, changed_at, meta,
    created_at, updated_at
)
VALUES (
    2,
    'token',
    '2',
    now() - interval '25 days',
    '{"chain":"eip155:1","standard":"erc721","contract":"0xBC4CA0EdA7647A8aB7C2061c2E118A18a936f13D","token":"2","from":"0x0000000000000000000000000000000000000000","to":"0xabcdefabcdefabcdefabcdefabcdefabcdefabcd","quantity":"1"}',
    now() - interval '25 days',
    now() - interval '25 days'
);

-- Change for Token 4 transfers
INSERT INTO changes_journal (
    id, subject_type, subject_id, changed_at, meta,
    created_at, updated_at
)
VALUES (
    3,
    'balance',
    '5',
    now() - interval '12 days',
    '{"chain":"eip155:1","standard":"erc1155","contract":"0xd07dc4262BCDbf85190C01c996b4C06a461d2430","token":"1","from":"0x4444444444444444444444444444444444444444","to":"0x5555555555555555555555555555555555555555","quantity":"30"}',
    now() - interval '12 days',
    now() - interval '12 days'
);

-- Change for Token 1 metadata update
INSERT INTO changes_journal (
    id, subject_type, subject_id, changed_at, meta,
    created_at, updated_at
)
VALUES (
    4,
    'metadata',
    '1',
    now() - interval '29 days',
    '{"old":{},"new":{"image_url":"https://example.com/bayc/1.png","animation_url":null,"artists":[{"did":"did:pkh:eip155:1:0x1234567890123456789012345678901234567890","name":"Yuga Labs"}],"publisher":{"name":"OpenSea","url":"https://opensea.io"},"mime_type":"image/png"}}',
    now() - interval '29 days',
    now() - interval '29 days'
);

-- Reset sequence
SELECT setval('changes_journal_id_seq', 100, true);

-- =============================================================================
-- Media Assets
-- =============================================================================

-- Media asset for Token 1
INSERT INTO media_assets (
    id, source_url, mime_type, file_size_bytes,
    provider, provider_asset_id, provider_metadata, variant_urls,
    created_at, updated_at
)
VALUES (
    1,
    'ipfs://QmTest1',
    'image/png',
    1024000,
    'cloudflare',
    'cf_asset_1',
    '{"account_id":"test_account","account_hash":"abc123"}',
    '{"thumbnail":"https://cdn.example.com/thumb/1.png","medium":"https://cdn.example.com/medium/1.png","original":"https://cdn.example.com/original/1.png"}',
    now() - interval '30 days',
    now() - interval '30 days'
);

-- Media asset for Token 4 (image)
INSERT INTO media_assets (
    id, source_url, mime_type, file_size_bytes,
    provider, provider_asset_id, provider_metadata, variant_urls,
    created_at, updated_at
)
VALUES (
    2,
    'ipfs://QmTest4',
    'image/png',
    2048000,
    'cloudflare',
    'cf_asset_4',
    '{"account_id":"test_account","account_hash":"abc123"}',
    '{"thumbnail":"https://cdn.example.com/thumb/4.png","medium":"https://cdn.example.com/medium/4.png","original":"https://cdn.example.com/original/4.png"}',
    now() - interval '15 days',
    now() - interval '15 days'
);

-- Media asset for Token 4 (animation)
INSERT INTO media_assets (
    id, source_url, mime_type, file_size_bytes,
    provider, provider_asset_id, provider_metadata, variant_urls,
    created_at, updated_at
)
VALUES (
    3,
    'ipfs://QmTest4Anim',
    'video/mp4',
    10240000,
    'cloudflare',
    'cf_asset_4_anim',
    '{"account_id":"test_account","account_hash":"abc123"}',
    '{"thumbnail":"https://cdn.example.com/thumb/4.mp4","medium":"https://cdn.example.com/medium/4.mp4","original":"https://cdn.example.com/original/4.mp4"}',
    now() - interval '15 days',
    now() - interval '15 days'
);

-- Reset sequence
SELECT setval('media_assets_id_seq', 100, true);

-- =============================================================================
-- Watched Addresses
-- =============================================================================

-- Watched address on Ethereum Mainnet
INSERT INTO watched_addresses (
    chain, address, watching, last_queried_at, last_successful_indexing_blk_range,
    created_at, updated_at
)
VALUES (
    'eip155:1',
    '0x1234567890123456789012345678901234567890',
    true,
    now() - interval '1 day',
    '{"eip155:1":{"min_block":15000000,"max_block":15500000}}',
    now() - interval '30 days',
    now() - interval '1 day'
);

-- Watched address on Ethereum Sepolia
INSERT INTO watched_addresses (
    chain, address, watching, last_queried_at, last_successful_indexing_blk_range,
    created_at, updated_at
)
VALUES (
    'eip155:11155111',
    '0x2222222222222222222222222222222222222222',
    true,
    now() - interval '2 hours',
    '{"eip155:11155111":{"min_block":1000000,"max_block":1100000}}',
    now() - interval '20 days',
    now() - interval '2 hours'
);

-- Watched address on Tezos Mainnet
INSERT INTO watched_addresses (
    chain, address, watching,
    created_at, updated_at
)
VALUES (
    'tezos:mainnet',
    'tz1VSUr8wwNhLAzempoch5d6hLRiTh8Cjcjb',
    true,
    now() - interval '8 days',
    now() - interval '8 days'
);

-- Not watched address
INSERT INTO watched_addresses (
    chain, address, watching,
    created_at, updated_at
)
VALUES (
    'eip155:1',
    '0x9999999999999999999999999999999999999999',
    false,
    now() - interval '60 days',
    now() - interval '60 days'
);

-- =============================================================================
-- Additional Test Configuration in Key-Value Store
-- =============================================================================

INSERT INTO key_value_store (key, value, created_at, updated_at)
VALUES ('test:config:enabled', 'true', now(), now())
ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value, updated_at = now();

INSERT INTO key_value_store (key, value, created_at, updated_at)
VALUES ('test:config:version', '1.0.0', now(), now())
ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value, updated_at = now();

-- =============================================================================
-- Webhook Clients
-- =============================================================================

-- Active webhook client listening to all events
INSERT INTO webhook_clients (
    id, client_id, webhook_url, webhook_secret, event_filters, is_active, retry_max_attempts,
    created_at, updated_at
)
VALUES (
    1,
    'client-all-events-123',
    'https://webhook.example.com/all',
    'secret_all_events_123456789',
    '["*"]',
    true,
    5,
    now() - interval '10 days',
    now() - interval '10 days'
);

-- Active webhook client listening to specific events
INSERT INTO webhook_clients (
    id, client_id, webhook_url, webhook_secret, event_filters, is_active, retry_max_attempts,
    created_at, updated_at
)
VALUES (
    2,
    'client-specific-events-456',
    'https://webhook.example.com/specific',
    'secret_specific_events_987654321',
    '["token.indexing.queryable", "token.indexing.viewable"]',
    true,
    3,
    now() - interval '5 days',
    now() - interval '5 days'
);

-- Inactive webhook client
INSERT INTO webhook_clients (
    id, client_id, webhook_url, webhook_secret, event_filters, is_active, retry_max_attempts,
    created_at, updated_at
)
VALUES (
    3,
    'client-inactive-789',
    'https://webhook.example.com/inactive',
    'secret_inactive_111222333',
    '["token.indexing.provenance_completed"]',
    false,
    5,
    now() - interval '30 days',
    now() - interval '15 days'
);

-- Reset sequence
SELECT setval('webhook_clients_id_seq', 100, true);

-- =============================================================================
-- Webhook Deliveries
-- =============================================================================

-- Successful delivery
INSERT INTO webhook_deliveries (
    id, client_id, event_id, event_type, payload, workflow_id, workflow_run_id,
    delivery_status, attempts, last_attempt_at, response_status, response_body, error_message,
    created_at, updated_at
)
VALUES (
    1,
    'client-all-events-123',
    '01JG8XAMPLE1111111111111111',
    'token.indexing.queryable',
    '{"event_id":"01JG8XAMPLE1111111111111111","event_type":"token.indexing.queryable","timestamp":"2024-01-15T10:00:00Z","data":{"token_cid":"eip155:1:erc721:0xBC4CA0EdA7647A8aB7C2061c2E118A18a936f13D:1","chain":"eip155:1","standard":"erc721","contract":"0xBC4CA0EdA7647A8aB7C2061c2E118A18a936f13D","token_number":"1","changed_at":"2024-01-15T10:00:00Z"}}',
    'webhook-delivery-client-all-events-123-01JG8XAMPLE1111111111111111',
    'run-id-123',
    'success',
    1,
    now() - interval '3 days',
    200,
    '{"status":"received"}',
    '',
    now() - interval '3 days',
    now() - interval '3 days'
);

-- Failed delivery after retries
INSERT INTO webhook_deliveries (
    id, client_id, event_id, event_type, payload, workflow_id, workflow_run_id,
    delivery_status, attempts, last_attempt_at, response_status, response_body, error_message,
    created_at, updated_at
)
VALUES (
    2,
    'client-specific-events-456',
    '01JG8XAMPLE2222222222222222',
    'token.indexing.viewable',
    '{"event_id":"01JG8XAMPLE2222222222222222","event_type":"token.indexing.viewable","timestamp":"2024-01-16T12:00:00Z","data":{"token_cid":"eip155:1:erc721:0xBC4CA0EdA7647A8aB7C2061c2E118A18a936f13D:2","chain":"eip155:1","standard":"erc721","contract":"0xBC4CA0EdA7647A8aB7C2061c2E118A18a936f13D","token_number":"2","changed_at":"2024-01-16T12:00:00Z"}}',
    'webhook-delivery-client-specific-events-456-01JG8XAMPLE2222222222222222',
    'run-id-456',
    'failed',
    3,
    now() - interval '2 days',
    500,
    '{"error":"internal server error"}',
    'HTTP 500',
    now() - interval '2 days',
    now() - interval '2 days'
);

-- Pending delivery
INSERT INTO webhook_deliveries (
    id, client_id, event_id, event_type, payload, workflow_id, workflow_run_id,
    delivery_status, attempts, last_attempt_at, response_status, response_body, error_message,
    created_at, updated_at
)
VALUES (
    3,
    'client-all-events-123',
    '01JG8XAMPLE3333333333333333',
    'token.indexing.provenance_completed',
    '{"event_id":"01JG8XAMPLE3333333333333333","event_type":"token.indexing.provenance_completed","timestamp":"2024-01-17T14:00:00Z","data":{"token_cid":"eip155:1:erc721:0xBC4CA0EdA7647A8aB7C2061c2E118A18a936f13D:1","chain":"eip155:1","standard":"erc721","contract":"0xBC4CA0EdA7647A8aB7C2061c2E118A18a936f13D","token_number":"1","changed_at":"2024-01-17T14:00:00Z"}}',
    'webhook-delivery-client-all-events-123-01JG8XAMPLE3333333333333333',
    'run-id-789',
    'pending',
    0,
    NULL,
    NULL,
    '',
    '',
    now() - interval '1 day',
    now() - interval '1 day'
);

-- Reset sequence
SELECT setval('webhook_deliveries_id_seq', 100, true);

-- =============================================================================
-- Token Media Health
-- =============================================================================

-- Media health for Token 1 (image from metadata - healthy)
INSERT INTO token_media_health (
    id, token_id, media_url, media_url_hash, media_source, health_status, last_checked_at, last_error,
    created_at, updated_at
)
VALUES (
    1,
    1,
    'https://example.com/bayc/1.png',
    md5('https://example.com/bayc/1.png'),
    'metadata_image',
    'healthy',
    now() - interval '1 day',
    NULL,
    now() - interval '30 days',
    now() - interval '1 day'
);

-- Media health for Token 1 (image from enrichment - healthy)
INSERT INTO token_media_health (
    id, token_id, media_url, media_url_hash, media_source, health_status, last_checked_at, last_error,
    created_at, updated_at
)
VALUES (
    2,
    1,
    'https://artblocks.io/images/1.png',
    md5('https://artblocks.io/images/1.png'),
    'enrichment_image',
    'healthy',
    now() - interval '1 day',
    NULL,
    now() - interval '29 days',
    now() - interval '1 day'
);

-- Media health for Token 2 (image from metadata - healthy)
INSERT INTO token_media_health (
    id, token_id, media_url, media_url_hash, media_source, health_status, last_checked_at, last_error,
    created_at, updated_at
)
VALUES (
    3,
    2,
    'https://example.com/bayc/2.png',
    md5('https://example.com/bayc/2.png'),
    'metadata_image',
    'healthy',
    now() - interval '2 days',
    NULL,
    now() - interval '25 days',
    now() - interval '2 days'
);

-- Media health for Token 2 (image from enrichment - healthy)
INSERT INTO token_media_health (
    id, token_id, media_url, media_url_hash, media_source, health_status, last_checked_at, last_error,
    created_at, updated_at
)
VALUES (
    4,
    2,
    'https://opensea.io/images/2.png',
    md5('https://opensea.io/images/2.png'),
    'enrichment_image',
    'healthy',
    now() - interval '2 days',
    NULL,
    now() - interval '24 days',
    now() - interval '2 days'
);

-- Media health for Token 4 (image from metadata - healthy)
INSERT INTO token_media_health (
    id, token_id, media_url, media_url_hash, media_source, health_status, last_checked_at, last_error,
    created_at, updated_at
)
VALUES (
    5,
    4,
    'https://example.com/artblock/1.png',
    md5('https://example.com/artblock/1.png'),
    'metadata_image',
    'healthy',
    now() - interval '1 day',
    NULL,
    now() - interval '15 days',
    now() - interval '1 day'
);

-- Media health for Token 4 (animation from metadata - healthy)
INSERT INTO token_media_health (
    id, token_id, media_url, media_url_hash, media_source, health_status, last_checked_at, last_error,
    created_at, updated_at
)
VALUES (
    6,
    4,
    'https://example.com/artblock/1.mp4',
    md5('https://example.com/artblock/1.mp4'),
    'metadata_animation',
    'healthy',
    now() - interval '1 day',
    NULL,
    now() - interval '15 days',
    now() - interval '1 day'
);

-- Media health for Token 4 (image from enrichment - healthy)
INSERT INTO token_media_health (
    id, token_id, media_url, media_url_hash, media_source, health_status, last_checked_at, last_error,
    created_at, updated_at
)
VALUES (
    7,
    4,
    'https://fxhash.xyz/images/1.png',
    md5('https://fxhash.xyz/images/1.png'),
    'enrichment_image',
    'healthy',
    now() - interval '1 day',
    NULL,
    now() - interval '14 days',
    now() - interval '1 day'
);

-- Media health for Token 4 (animation from enrichment - healthy)
INSERT INTO token_media_health (
    id, token_id, media_url, media_url_hash, media_source, health_status, last_checked_at, last_error,
    created_at, updated_at
)
VALUES (
    8,
    4,
    'https://fxhash.xyz/videos/1.mp4',
    md5('https://fxhash.xyz/videos/1.mp4'),
    'enrichment_animation',
    'healthy',
    now() - interval '1 day',
    NULL,
    now() - interval '14 days',
    now() - interval '1 day'
);

-- Token 3 (no media health), Token 5 (burned), and Token 6 (FA2, no media health)
-- have is_viewable = false due to lack of healthy media URLs

-- Reset sequence
SELECT setval('token_media_health_id_seq', 100, true);


