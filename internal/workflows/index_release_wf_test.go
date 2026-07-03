package workflows

// Unit tests for IndexRelease CID derivation helpers.
// Tests live in the same package (workflows) to access unexported helpers.
//
// We cannot import internal/mocks here because mocks/core_executor.go imports
// this package, creating a cycle. Instead, small local fakes implement the
// vendor client interfaces inline.

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/feral-file/ff-indexer-v2/internal/domain"
	"github.com/feral-file/ff-indexer-v2/internal/providers/vendors/feralfile"
	"github.com/feral-file/ff-indexer-v2/internal/providers/vendors/fxhash"
	"github.com/feral-file/ff-indexer-v2/internal/providers/vendors/opensea"
	"github.com/feral-file/ff-indexer-v2/internal/store/schema"
)

// ──────────────────────────────────────────────────────────────────────────────
// Local test fakes (avoid mocks import cycle)
// ──────────────────────────────────────────────────────────────────────────────

// fakeFxhashClient implements fxhash.Client for tests.
type fakeFxhashClient struct {
	gentks []fxhash.GentkRef
	err    error
}

func (f *fakeFxhashClient) GetGentk(_ context.Context, _, _ string) (*fxhash.Gentk, error) {
	return nil, nil
}

func (f *fakeFxhashClient) GetGentksByIteration(_ context.Context, _ string, _, _ int64) ([]fxhash.GentkRef, error) {
	return f.gentks, f.err
}

func (f *fakeFxhashClient) ResolveSlug(_ context.Context, _ string) (string, error) {
	return "", nil
}

// fakeFFClient implements feralfile.Client for tests.
type fakeFFClient struct {
	artworks []feralfile.ArtworkRef
	err      error
}

func (f *fakeFFClient) GetArtwork(_ context.Context, _ string) (*feralfile.Artwork, error) {
	return nil, nil
}

func (f *fakeFFClient) GetSeriesArtworks(_ context.Context, _ string, _, _ int64) ([]feralfile.ArtworkRef, error) {
	return f.artworks, f.err
}

func (f *fakeFFClient) ResolveSlug(_ context.Context, _ string) (string, error) {
	return "", nil
}

// fakeOpenSeaClient implements opensea.Client for tests.
type fakeOpenSeaClient struct {
	collection *opensea.CollectionMetadata
	slug       string
	err        error
}

func (f *fakeOpenSeaClient) GetNFT(_ context.Context, _, _ string) (*opensea.NFTMetadata, error) {
	return nil, nil
}

func (f *fakeOpenSeaClient) GetCollection(_ context.Context, _ string) (*opensea.CollectionMetadata, error) {
	return f.collection, f.err
}

func (f *fakeOpenSeaClient) ResolveSlug(_ context.Context, slug string) (string, error) {
	if f.err != nil {
		return "", f.err
	}
	if f.slug != "" {
		return f.slug, nil
	}
	return slug, nil
}

// ──────────────────────────────────────────────────────────────────────────────
// Art Blocks CID derivation
// ──────────────────────────────────────────────────────────────────────────────

func TestDeriveArtBlocksCIDs_Basic(t *testing.T) {
	t.Parallel()

	// vendor_release_id for AB project 78 on chain 1
	cids, skipped, err := deriveArtBlocksCIDs("1-0xa7d8d9ef8d8ce8992df33d8b8cf4aebabd5bd270-78", 1, 3)

	require.NoError(t, err)
	assert.Equal(t, 0, skipped)
	require.Len(t, cids, 3)

	// token_number = 78 * 1_000_000 + (mintNum - 1)
	assert.Contains(t, string(cids[0]), "78000000")
	assert.Contains(t, string(cids[1]), "78000001")
	assert.Contains(t, string(cids[2]), "78000002")

	for _, cid := range cids {
		chain, standard, _, _ := cid.Parse()
		assert.Equal(t, domain.ChainEthereumMainnet, chain)
		assert.Equal(t, domain.StandardERC721, standard)
	}
}

func TestDeriveArtBlocksCIDs_SingleMint(t *testing.T) {
	t.Parallel()

	cids, skipped, err := deriveArtBlocksCIDs("1-0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-5", 10, 10)

	require.NoError(t, err)
	assert.Equal(t, 0, skipped)
	require.Len(t, cids, 1)
	// token_number = 5 * 1_000_000 + 9 = 5_000_009
	assert.Contains(t, string(cids[0]), "5000009")
}

func TestDeriveArtBlocksCIDs_InvalidVendorReleaseID_NoSeparator(t *testing.T) {
	t.Parallel()

	_, _, err := deriveArtBlocksCIDs("noseparator", 1, 3)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "no separator")
}

func TestDeriveArtBlocksCIDs_InvalidVendorReleaseID_MissingProject(t *testing.T) {
	t.Parallel()

	_, _, err := deriveArtBlocksCIDs("1-0xabc", 1, 3)
	assert.Error(t, err)
}

// ──────────────────────────────────────────────────────────────────────────────
// objkt CID derivation
// ──────────────────────────────────────────────────────────────────────────────

func TestDeriveObjktCIDs_Basic(t *testing.T) {
	t.Parallel()

	cids, skipped, err := deriveObjktCIDs("KT1U6EHmNxJTkvaWJ4ThczG4FSDaHC21ssvi", 1, 3)

	require.NoError(t, err)
	assert.Equal(t, 0, skipped)
	require.Len(t, cids, 3)

	expectedTokenNums := []string{"1", "2", "3"}
	for i, cid := range cids {
		chain, standard, contract, tokenNumber := cid.Parse()
		assert.Equal(t, domain.ChainTezosMainnet, chain)
		assert.Equal(t, domain.StandardFA2, standard)
		assert.Equal(t, "KT1U6EHmNxJTkvaWJ4ThczG4FSDaHC21ssvi", contract)
		assert.Equal(t, expectedTokenNums[i], tokenNumber)
	}
}

func TestDeriveObjktCIDs_Range(t *testing.T) {
	t.Parallel()

	cids, skipped, err := deriveObjktCIDs("KT1abc", 5, 7)

	require.NoError(t, err)
	assert.Equal(t, 0, skipped)
	require.Len(t, cids, 3)
	assert.Contains(t, string(cids[0]), ":5")
	assert.Contains(t, string(cids[1]), ":6")
	assert.Contains(t, string(cids[2]), ":7")
}

// ──────────────────────────────────────────────────────────────────────────────
// mapFeralFileChain
// ──────────────────────────────────────────────────────────────────────────────

func TestMapFeralFileChain(t *testing.T) {
	t.Parallel()

	tests := []struct {
		chain        string
		wantChain    domain.Chain
		wantStandard domain.ChainStandard
		wantSkip     bool
	}{
		{"ethereum", domain.ChainEthereumMainnet, domain.StandardERC721, false},
		{"Ethereum", domain.ChainEthereumMainnet, domain.StandardERC721, false},
		{"tezos", domain.ChainTezosMainnet, domain.StandardFA2, false},
		{"Tezos", domain.ChainTezosMainnet, domain.StandardFA2, false},
		{"bitmark", "", "", true},
		{"Bitmark", "", "", true},
		{"unknown", "", "", true},
		{"", "", "", true},
	}

	for _, tt := range tests {
		t.Run(tt.chain, func(t *testing.T) {
			t.Parallel()
			chain, standard, skip := mapFeralFileChain(tt.chain)
			assert.Equal(t, tt.wantSkip, skip)
			if !tt.wantSkip {
				assert.Equal(t, tt.wantChain, chain)
				assert.Equal(t, tt.wantStandard, standard)
			}
		})
	}
}

// ──────────────────────────────────────────────────────────────────────────────
// fxhash CID derivation
// ──────────────────────────────────────────────────────────────────────────────

func TestDeriveFxhashCIDs_Success(t *testing.T) {
	t.Parallel()

	w := &coreWorkflows{
		config: CoreWorkflowsConfig{TezosChainID: domain.ChainTezosMainnet},
		fxhashClient: &fakeFxhashClient{
			gentks: []fxhash.GentkRef{
				{ContractAddress: "KT1abc", TokenID: "42", Iteration: 1},
				{ContractAddress: "KT1abc", TokenID: "43", Iteration: 2},
			},
		},
	}

	cids, skipped, err := w.deriveFxhashCIDs(context.Background(), "9997", 1, 2)

	require.NoError(t, err)
	assert.Equal(t, 0, skipped)
	require.Len(t, cids, 2)
	assert.Contains(t, string(cids[0]), "KT1abc")
	assert.Contains(t, string(cids[0]), ":42")
	assert.Contains(t, string(cids[1]), ":43")

	for _, cid := range cids {
		chain, standard, _, _ := cid.Parse()
		assert.Equal(t, domain.ChainTezosMainnet, chain)
		assert.Equal(t, domain.StandardFA2, standard)
	}
}

func TestDeriveFxhashCIDs_NilClient(t *testing.T) {
	t.Parallel()

	w := &coreWorkflows{fxhashClient: nil}
	_, _, err := w.deriveFxhashCIDs(context.Background(), "9997", 1, 10)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "fxhash client not configured")
}

func TestDeriveFxhashCIDs_APIError(t *testing.T) {
	t.Parallel()

	w := &coreWorkflows{
		config:       CoreWorkflowsConfig{TezosChainID: domain.ChainTezosMainnet},
		fxhashClient: &fakeFxhashClient{err: errors.New("network error")},
	}
	_, _, err := w.deriveFxhashCIDs(context.Background(), "9997", 1, 5)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "GetGentksByIteration failed")
}

func TestDeriveFxhashCIDs_PartialResult(t *testing.T) {
	t.Parallel()

	// API returns fewer results than requested range (gap in supply).
	w := &coreWorkflows{
		config: CoreWorkflowsConfig{TezosChainID: domain.ChainTezosMainnet},
		fxhashClient: &fakeFxhashClient{
			gentks: []fxhash.GentkRef{
				{ContractAddress: "KT1abc", TokenID: "1", Iteration: 1},
			},
		},
	}
	cids, skipped, err := w.deriveFxhashCIDs(context.Background(), "9997", 1, 5)

	require.NoError(t, err)
	assert.Equal(t, 4, skipped) // 5 requested, 1 returned
	assert.Len(t, cids, 1)
}

func TestDeriveFxhashCIDs_EmptyResult(t *testing.T) {
	t.Parallel()

	w := &coreWorkflows{
		config:       CoreWorkflowsConfig{TezosChainID: domain.ChainTezosMainnet},
		fxhashClient: &fakeFxhashClient{gentks: []fxhash.GentkRef{}},
	}
	cids, skipped, err := w.deriveFxhashCIDs(context.Background(), "9997", 1, 3)

	require.NoError(t, err)
	assert.Len(t, cids, 0)
	assert.Equal(t, 3, skipped) // 3 requested, 0 returned
}

// ──────────────────────────────────────────────────────────────────────────────
// Feral File CID derivation
// ──────────────────────────────────────────────────────────────────────────────

func TestDeriveFeralFileCIDs_EthereumArtworks(t *testing.T) {
	t.Parallel()

	w := &coreWorkflows{
		config: CoreWorkflowsConfig{EthereumChainID: domain.ChainEthereumMainnet},
		feralfileClient: &fakeFFClient{
			artworks: []feralfile.ArtworkRef{
				{Index: 0, Chain: "ethereum", ContractAddress: "0xabc", TokenID: "1"},
				{Index: 1, Chain: "ethereum", ContractAddress: "0xabc", TokenID: "2"},
			},
		},
	}
	cids, skipped, err := w.deriveFeralFileCIDs(context.Background(), "series-uuid", 1, 2)

	require.NoError(t, err)
	assert.Equal(t, 0, skipped)
	require.Len(t, cids, 2)
	chain, standard, _, _ := cids[0].Parse()
	assert.Equal(t, domain.ChainEthereumMainnet, chain)
	assert.Equal(t, domain.StandardERC721, standard)
}

func TestDeriveFeralFileCIDs_TezosArtworks(t *testing.T) {
	t.Parallel()

	w := &coreWorkflows{
		config: CoreWorkflowsConfig{TezosChainID: domain.ChainTezosMainnet},
		feralfileClient: &fakeFFClient{
			artworks: []feralfile.ArtworkRef{
				{Index: 0, Chain: "tezos", ContractAddress: "KT1def", TokenID: "5"},
			},
		},
	}
	cids, skipped, err := w.deriveFeralFileCIDs(context.Background(), "series-uuid", 1, 1)

	require.NoError(t, err)
	assert.Equal(t, 0, skipped)
	require.Len(t, cids, 1)
	chain, standard, contract, tokenNum := cids[0].Parse()
	assert.Equal(t, domain.ChainTezosMainnet, chain)
	assert.Equal(t, domain.StandardFA2, standard)
	assert.Equal(t, "KT1def", contract)
	assert.Equal(t, "5", tokenNum)
}

func TestDeriveFeralFileCIDs_SkipsBitmarkArtworks(t *testing.T) {
	t.Parallel()

	w := &coreWorkflows{
		config: CoreWorkflowsConfig{
			EthereumChainID: domain.ChainEthereumMainnet,
			TezosChainID:    domain.ChainTezosMainnet,
		},
		feralfileClient: &fakeFFClient{
			artworks: []feralfile.ArtworkRef{
				{Index: 0, Chain: "ethereum", ContractAddress: "0xabc", TokenID: "1"},
				{Index: 1, Chain: "bitmark", ContractAddress: "", TokenID: "some-bitmark-id"},
				{Index: 2, Chain: "tezos", ContractAddress: "KT1def", TokenID: "5"},
			},
		},
	}
	cids, skipped, err := w.deriveFeralFileCIDs(context.Background(), "series-uuid", 1, 3)

	require.NoError(t, err)
	assert.Equal(t, 1, skipped) // 1 Bitmark artwork skipped
	require.Len(t, cids, 2)
	chain0, _, _, _ := cids[0].Parse()
	chain1, _, _, _ := cids[1].Parse()
	assert.Equal(t, domain.ChainEthereumMainnet, chain0)
	assert.Equal(t, domain.ChainTezosMainnet, chain1)
}

func TestDeriveFeralFileCIDs_AllBitmarkSkipped(t *testing.T) {
	t.Parallel()

	w := &coreWorkflows{
		feralfileClient: &fakeFFClient{
			artworks: []feralfile.ArtworkRef{
				{Index: 0, Chain: "bitmark", ContractAddress: "", TokenID: "b1"},
				{Index: 1, Chain: "bitmark", ContractAddress: "", TokenID: "b2"},
			},
		},
	}
	cids, skipped, err := w.deriveFeralFileCIDs(context.Background(), "series-uuid", 1, 2)

	require.NoError(t, err)
	assert.Equal(t, 2, skipped)
	assert.Len(t, cids, 0)
}

func TestDeriveFeralFileCIDs_NilClient(t *testing.T) {
	t.Parallel()

	w := &coreWorkflows{feralfileClient: nil}
	_, _, err := w.deriveFeralFileCIDs(context.Background(), "series-uuid", 1, 10)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "feral file client not configured")
}

func TestDeriveFeralFileCIDs_APIError(t *testing.T) {
	t.Parallel()

	w := &coreWorkflows{
		feralfileClient: &fakeFFClient{err: errors.New("ff api error")},
	}
	_, _, err := w.deriveFeralFileCIDs(context.Background(), "series-uuid", 1, 5)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "GetSeriesArtworks failed")
}

func TestDeriveFeralFileCIDs_MissingContractSkipped(t *testing.T) {
	t.Parallel()

	w := &coreWorkflows{
		config: CoreWorkflowsConfig{EthereumChainID: domain.ChainEthereumMainnet},
		feralfileClient: &fakeFFClient{
			artworks: []feralfile.ArtworkRef{
				{Index: 0, Chain: "ethereum", ContractAddress: "", TokenID: "1"},
			},
		},
	}
	cids, skipped, err := w.deriveFeralFileCIDs(context.Background(), "series-uuid", 1, 1)

	require.NoError(t, err)
	assert.Equal(t, 1, skipped)
	assert.Len(t, cids, 0)
}

// ──────────────────────────────────────────────────────────────────────────────
// OpenSea CID derivation
// ──────────────────────────────────────────────────────────────────────────────

func TestDeriveOpenSeaCIDs_Success(t *testing.T) {
	t.Parallel()

	w := &coreWorkflows{
		config: CoreWorkflowsConfig{EthereumChainID: domain.ChainEthereumMainnet},
		openseaClient: &fakeOpenSeaClient{
			collection: &opensea.CollectionMetadata{
				Collection:  "boredapeyachtclub",
				Name:        "Bored Ape Yacht Club",
				TotalSupply: 10000,
				Contracts: []opensea.CollectionContract{
					{Address: "0xBC4CA0EdA7647A8aB7C2061c2E118A18a936f13D", Chain: "ethereum"},
				},
			},
		},
	}

	cids, skipped, err := w.deriveOpenSeaCIDs(context.Background(), "boredapeyachtclub", 1, 3)

	require.NoError(t, err)
	assert.Equal(t, 0, skipped)
	require.Len(t, cids, 3)

	// Contract address should be EIP-55 checksummed in CID (NormalizeAddress uses checksum, not lowercase).
	for _, cid := range cids {
		chain, standard, contract, _ := cid.Parse()
		assert.Equal(t, domain.ChainEthereumMainnet, chain)
		assert.Equal(t, domain.StandardERC721, standard)
		// EIP-55 checksum of the input address
		assert.Equal(t, "0xBC4CA0EdA7647A8aB7C2061c2E118A18a936f13D", contract)
	}
	// Verify token IDs are mint numbers.
	_, _, _, tokenID0 := cids[0].Parse()
	_, _, _, tokenID1 := cids[1].Parse()
	_, _, _, tokenID2 := cids[2].Parse()
	assert.Equal(t, "1", tokenID0)
	assert.Equal(t, "2", tokenID1)
	assert.Equal(t, "3", tokenID2)
}

func TestDeriveOpenSeaCIDs_NilClient(t *testing.T) {
	t.Parallel()

	w := &coreWorkflows{
		config:        CoreWorkflowsConfig{EthereumChainID: domain.ChainEthereumMainnet},
		openseaClient: nil,
	}
	_, _, err := w.deriveOpenSeaCIDs(context.Background(), "boredapeyachtclub", 1, 10)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "opensea client not configured")
}

func TestDeriveOpenSeaCIDs_APIError(t *testing.T) {
	t.Parallel()

	w := &coreWorkflows{
		config:        CoreWorkflowsConfig{EthereumChainID: domain.ChainEthereumMainnet},
		openseaClient: &fakeOpenSeaClient{err: errors.New("API down")},
	}
	_, _, err := w.deriveOpenSeaCIDs(context.Background(), "boredapeyachtclub", 1, 5)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "GetCollection failed")
}

func TestDeriveOpenSeaCIDs_NoContracts(t *testing.T) {
	t.Parallel()

	w := &coreWorkflows{
		config: CoreWorkflowsConfig{EthereumChainID: domain.ChainEthereumMainnet},
		openseaClient: &fakeOpenSeaClient{
			collection: &opensea.CollectionMetadata{
				Collection: "empty-collection",
				Contracts:  []opensea.CollectionContract{},
			},
		},
	}
	_, _, err := w.deriveOpenSeaCIDs(context.Background(), "empty-collection", 1, 5)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "no associated contracts")
}

func TestDeriveOpenSeaCIDs_UnsupportedChain(t *testing.T) {
	t.Parallel()

	w := &coreWorkflows{
		config: CoreWorkflowsConfig{EthereumChainID: domain.ChainEthereumMainnet},
		openseaClient: &fakeOpenSeaClient{
			collection: &opensea.CollectionMetadata{
				Collection: "solana-collection",
				Contracts: []opensea.CollectionContract{
					{Address: "SomeAddress", Chain: "solana"},
				},
			},
		},
	}
	_, _, err := w.deriveOpenSeaCIDs(context.Background(), "solana-collection", 1, 5)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "unsupported chain")
}

func TestDeriveOpenSeaCIDs_ChainMismatch(t *testing.T) {
	t.Parallel()

	// Indexer configured for Ethereum mainnet (eip155:1), collection is on Polygon (eip155:137).
	w := &coreWorkflows{
		config: CoreWorkflowsConfig{EthereumChainID: domain.ChainEthereumMainnet},
		openseaClient: &fakeOpenSeaClient{
			collection: &opensea.CollectionMetadata{
				Collection: "polygon-collection",
				Contracts: []opensea.CollectionContract{
					{Address: "0xabc", Chain: "polygon"},
				},
			},
		},
	}
	_, _, err := w.deriveOpenSeaCIDs(context.Background(), "polygon-collection", 1, 5)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "chain mismatch")
}

type recordingReleaseUpserter struct {
	calls []recordingReleaseUpsertCall
	err   error
}

type recordingReleaseUpsertCall struct {
	vendor          schema.Vendor
	vendorReleaseID string
	name            *string
	totalMints      *int64
	slug            *string
}

func (r *recordingReleaseUpserter) UpsertReleaseMetadata(_ context.Context, vendor schema.Vendor, vendorReleaseID string, name *string, totalMints *int64, slug *string) error {
	r.calls = append(r.calls, recordingReleaseUpsertCall{
		vendor:          vendor,
		vendorReleaseID: vendorReleaseID,
		name:            name,
		totalMints:      totalMints,
		slug:            slug,
	})
	return r.err
}

func TestUpsertOpenSeaReleaseFromCollection(t *testing.T) {
	t.Parallel()

	upserter := &recordingReleaseUpserter{}
	collection := &opensea.CollectionMetadata{
		Collection:  "boredapeyachtclub",
		Name:        "Bored Ape Yacht Club",
		TotalSupply: 10000,
	}

	err := upsertOpenSeaReleaseFromCollection(context.Background(), upserter, "boredapeyachtclub", collection)
	require.NoError(t, err)
	require.Len(t, upserter.calls, 1)

	call := upserter.calls[0]
	assert.Equal(t, schema.VendorOpenSea, call.vendor)
	assert.Equal(t, "boredapeyachtclub", call.vendorReleaseID)
	require.NotNil(t, call.name)
	assert.Equal(t, "Bored Ape Yacht Club", *call.name)
	require.NotNil(t, call.totalMints)
	assert.Equal(t, int64(10000), *call.totalMints)
	require.NotNil(t, call.slug)
	assert.Equal(t, "boredapeyachtclub", *call.slug)
}

// ──────────────────────────────────────────────────────────────────────────────
// validateChain
// ──────────────────────────────────────────────────────────────────────────────

func TestValidateChain_EVMMatch(t *testing.T) {
	t.Parallel()

	w := &coreWorkflows{
		config: CoreWorkflowsConfig{
			EthereumChainID: domain.Chain("eip155:1"),
			TezosChainID:    domain.ChainTezosMainnet,
		},
	}

	err := w.validateChain(schema.VendorArtBlocks, domain.Chain("eip155:1"))
	assert.NoError(t, err)
}

func TestValidateChain_EVMMismatch(t *testing.T) {
	t.Parallel()

	w := &coreWorkflows{
		config: CoreWorkflowsConfig{
			EthereumChainID: domain.Chain("eip155:11155111"), // Sepolia testnet
			TezosChainID:    domain.ChainTezosMainnet,
		},
	}

	err := w.validateChain(schema.VendorArtBlocks, domain.Chain("eip155:1")) // mainnet
	require.Error(t, err)
	assert.Contains(t, err.Error(), "chain mismatch")
}

func TestValidateChain_TezosMatch(t *testing.T) {
	t.Parallel()

	w := &coreWorkflows{
		config: CoreWorkflowsConfig{
			EthereumChainID: domain.ChainEthereumMainnet,
			TezosChainID:    domain.ChainTezosMainnet,
		},
	}

	err := w.validateChain(schema.VendorObjkt, domain.ChainTezosMainnet)
	assert.NoError(t, err)
}

func TestValidateChain_TezosMismatch(t *testing.T) {
	t.Parallel()

	w := &coreWorkflows{
		config: CoreWorkflowsConfig{
			EthereumChainID: domain.ChainEthereumMainnet,
			TezosChainID:    domain.Chain("tezos:ghostnet"),
		},
	}

	err := w.validateChain(schema.VendorObjkt, domain.ChainTezosMainnet) // mainnet vs ghostnet
	require.Error(t, err)
	assert.Contains(t, err.Error(), "chain mismatch")
}

func TestValidateChain_UnknownFamily(t *testing.T) {
	t.Parallel()

	w := &coreWorkflows{
		config: CoreWorkflowsConfig{
			EthereumChainID: domain.ChainEthereumMainnet,
			TezosChainID:    domain.ChainTezosMainnet,
		},
	}

	err := w.validateChain(schema.VendorArtBlocks, domain.Chain("solana:mainnet"))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unrecognized chain family")
}
