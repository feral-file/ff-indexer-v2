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
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/feral-file/ff-indexer-v2/internal/domain"
	"github.com/feral-file/ff-indexer-v2/internal/providers/vendors/feralfile"
	"github.com/feral-file/ff-indexer-v2/internal/providers/vendors/fxhash"
	"github.com/feral-file/ff-indexer-v2/internal/providers/vendors/objkt"
	"github.com/feral-file/ff-indexer-v2/internal/store/schema"
)

// int64Ptr returns a pointer to v; used in tests to construct *int64 literal values.
func int64Ptr(v int64) *int64 { return &v }

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

// fakeObjktClient implements objkt.Client for tests.
type fakeObjktClient struct {
	fa  *objkt.FA
	err error
}

func (f *fakeObjktClient) GetToken(_ context.Context, _, _ string) (*objkt.Token, error) {
	return nil, nil
}

func (f *fakeObjktClient) GetFA(_ context.Context, _ string) (*objkt.FA, error) {
	return f.fa, f.err
}

// ──────────────────────────────────────────────────────────────────────────────
// Art Blocks CID derivation
// ──────────────────────────────────────────────────────────────────────────────

func TestDeriveArtBlocksCIDs_Basic(t *testing.T) {
	t.Parallel()

	// vendor_release_id for AB project 78 on chain 1
	cids, skipped, err := deriveArtBlocksCIDs("1-0xa7d8d9ef8d8ce8992df33d8b8cf4aebabd5bd270-78", []int64{1, 2, 3})

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

	cids, skipped, err := deriveArtBlocksCIDs("1-0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-5", []int64{10})

	require.NoError(t, err)
	assert.Equal(t, 0, skipped)
	require.Len(t, cids, 1)
	// token_number = 5 * 1_000_000 + 9 = 5_000_009
	assert.Contains(t, string(cids[0]), "5000009")
}

func TestDeriveArtBlocksCIDs_SparseList(t *testing.T) {
	t.Parallel()

	cids, skipped, err := deriveArtBlocksCIDs("1-0xa7d8d9ef8d8ce8992df33d8b8cf4aebabd5bd270-78", []int64{1, 5, 10})

	require.NoError(t, err)
	assert.Equal(t, 0, skipped)
	require.Len(t, cids, 3)
	assert.Contains(t, string(cids[0]), "78000000")
	assert.Contains(t, string(cids[1]), "78000004")
	assert.Contains(t, string(cids[2]), "78000009")
}

func TestDeriveArtBlocksCIDs_InvalidVendorReleaseID_NoSeparator(t *testing.T) {
	t.Parallel()

	_, _, err := deriveArtBlocksCIDs("noseparator", []int64{1, 2, 3})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "no separator")
}

func TestDeriveArtBlocksCIDs_InvalidVendorReleaseID_MissingProject(t *testing.T) {
	t.Parallel()

	_, _, err := deriveArtBlocksCIDs("1-0xabc", []int64{1, 2, 3})
	assert.Error(t, err)
}

// ──────────────────────────────────────────────────────────────────────────────
// objkt CID derivation
// ──────────────────────────────────────────────────────────────────────────────

func TestDeriveObjktCIDs_Basic(t *testing.T) {
	t.Parallel()

	cids, skipped, err := deriveObjktCIDs("KT1U6EHmNxJTkvaWJ4ThczG4FSDaHC21ssvi", []int64{1, 2, 3})

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

func TestDeriveObjktCIDs_SparseList(t *testing.T) {
	t.Parallel()

	cids, skipped, err := deriveObjktCIDs("KT1abc", []int64{5, 7})

	require.NoError(t, err)
	assert.Equal(t, 0, skipped)
	require.Len(t, cids, 2)
	assert.Contains(t, string(cids[0]), ":5")
	assert.Contains(t, string(cids[1]), ":7")
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

	cids, skipped, err := w.deriveFxhashCIDs(context.Background(), "9997", []int64{1, 2})

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

// TestDeriveFxhashCIDs_FiltersByIteration verifies that gentks whose iteration is
// not in the requested mintNumbers set are excluded.
func TestDeriveFxhashCIDs_FiltersByIteration(t *testing.T) {
	t.Parallel()

	// API returns iterations 1, 2, 3 (bounding window 1..3).
	// We only requested mintNumbers=[1, 3], so iteration 2 should be filtered out.
	w := &coreWorkflows{
		config: CoreWorkflowsConfig{TezosChainID: domain.ChainTezosMainnet},
		fxhashClient: &fakeFxhashClient{
			gentks: []fxhash.GentkRef{
				{ContractAddress: "KT1abc", TokenID: "10", Iteration: 1},
				{ContractAddress: "KT1abc", TokenID: "11", Iteration: 2}, // not requested
				{ContractAddress: "KT1abc", TokenID: "12", Iteration: 3},
			},
		},
	}

	cids, skipped, err := w.deriveFxhashCIDs(context.Background(), "9997", []int64{1, 3})

	require.NoError(t, err)
	assert.Equal(t, 0, skipped) // both requested iterations were found
	require.Len(t, cids, 2)
	assert.Contains(t, string(cids[0]), ":10")
	assert.Contains(t, string(cids[1]), ":12")
}

func TestDeriveFxhashCIDs_NilClient(t *testing.T) {
	t.Parallel()

	w := &coreWorkflows{fxhashClient: nil}
	_, _, err := w.deriveFxhashCIDs(context.Background(), "9997", []int64{1, 2, 3})

	require.Error(t, err)
	assert.Contains(t, err.Error(), "fxhash client not configured")
}

func TestDeriveFxhashCIDs_APIError(t *testing.T) {
	t.Parallel()

	w := &coreWorkflows{
		config:       CoreWorkflowsConfig{TezosChainID: domain.ChainTezosMainnet},
		fxhashClient: &fakeFxhashClient{err: errors.New("network error")},
	}
	_, _, err := w.deriveFxhashCIDs(context.Background(), "9997", []int64{1, 2, 3, 4, 5})

	require.Error(t, err)
	assert.Contains(t, err.Error(), "GetGentksByIteration failed")
}

func TestDeriveFxhashCIDs_PartialResult(t *testing.T) {
	t.Parallel()

	// API returns only iteration 1 even though we requested [1,2,3,4,5].
	w := &coreWorkflows{
		config: CoreWorkflowsConfig{TezosChainID: domain.ChainTezosMainnet},
		fxhashClient: &fakeFxhashClient{
			gentks: []fxhash.GentkRef{
				{ContractAddress: "KT1abc", TokenID: "1", Iteration: 1},
			},
		},
	}
	cids, skipped, err := w.deriveFxhashCIDs(context.Background(), "9997", []int64{1, 2, 3, 4, 5})

	require.NoError(t, err)
	assert.Equal(t, 4, skipped) // 5 requested, 1 matched
	assert.Len(t, cids, 1)
}

func TestDeriveFxhashCIDs_EmptyResult(t *testing.T) {
	t.Parallel()

	w := &coreWorkflows{
		config:       CoreWorkflowsConfig{TezosChainID: domain.ChainTezosMainnet},
		fxhashClient: &fakeFxhashClient{gentks: []fxhash.GentkRef{}},
	}
	cids, skipped, err := w.deriveFxhashCIDs(context.Background(), "9997", []int64{1, 2, 3})

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
				{Index: int64Ptr(0), Chain: "ethereum", ContractAddress: "0xabc", TokenID: "1"},
				{Index: int64Ptr(1), Chain: "ethereum", ContractAddress: "0xabc", TokenID: "2"},
			},
		},
	}
	cids, skipped, err := w.deriveFeralFileCIDs(context.Background(), "series-uuid", []int64{1, 2})

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
				{Index: int64Ptr(0), Chain: "tezos", ContractAddress: "KT1def", TokenID: "5"},
			},
		},
	}
	cids, skipped, err := w.deriveFeralFileCIDs(context.Background(), "series-uuid", []int64{1})

	require.NoError(t, err)
	assert.Equal(t, 0, skipped)
	require.Len(t, cids, 1)
	chain, standard, contract, tokenNum := cids[0].Parse()
	assert.Equal(t, domain.ChainTezosMainnet, chain)
	assert.Equal(t, domain.StandardFA2, standard)
	assert.Equal(t, "KT1def", contract)
	assert.Equal(t, "5", tokenNum)
}

// TestDeriveFeralFileCIDs_FiltersByIndex verifies that artworks whose Index+1 is not
// in the requested mintNumbers set are excluded.
func TestDeriveFeralFileCIDs_FiltersByIndex(t *testing.T) {
	t.Parallel()

	// API returns artworks at Index 0,1,2 (bounding window 1..3).
	// We only requested mintNumbers=[1,3] (Index 0 and 2). Index 1 should be filtered.
	w := &coreWorkflows{
		config: CoreWorkflowsConfig{EthereumChainID: domain.ChainEthereumMainnet},
		feralfileClient: &fakeFFClient{
			artworks: []feralfile.ArtworkRef{
				{Index: int64Ptr(0), Chain: "ethereum", ContractAddress: "0xabc", TokenID: "10"},
				{Index: int64Ptr(1), Chain: "ethereum", ContractAddress: "0xabc", TokenID: "11"}, // not requested
				{Index: int64Ptr(2), Chain: "ethereum", ContractAddress: "0xabc", TokenID: "12"},
			},
		},
	}
	cids, skipped, err := w.deriveFeralFileCIDs(context.Background(), "series-uuid", []int64{1, 3})

	require.NoError(t, err)
	assert.Equal(t, 0, skipped) // both requested indices were found
	require.Len(t, cids, 2)
	assert.Contains(t, string(cids[0]), ":10")
	assert.Contains(t, string(cids[1]), ":12")
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
				{Index: int64Ptr(0), Chain: "ethereum", ContractAddress: "0xabc", TokenID: "1"},
				{Index: int64Ptr(1), Chain: "bitmark", ContractAddress: "", TokenID: "some-bitmark-id"},
				{Index: int64Ptr(2), Chain: "tezos", ContractAddress: "KT1def", TokenID: "5"},
			},
		},
	}
	cids, skipped, err := w.deriveFeralFileCIDs(context.Background(), "series-uuid", []int64{1, 2, 3})

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
				{Index: int64Ptr(0), Chain: "bitmark", ContractAddress: "", TokenID: "b1"},
				{Index: int64Ptr(1), Chain: "bitmark", ContractAddress: "", TokenID: "b2"},
			},
		},
	}
	cids, skipped, err := w.deriveFeralFileCIDs(context.Background(), "series-uuid", []int64{1, 2})

	require.NoError(t, err)
	assert.Equal(t, 2, skipped)
	assert.Len(t, cids, 0)
}

// TestDeriveFeralFileCIDs_NilIndexSkipped verifies that artworks with a nil Index are
// counted as skipped and do not match any requested mint number — including mint 1.
// A nil Index means the FF API omitted the edition number; coercing nil→0 would
// cause the artwork to match mint_number=1 (0+1), which is incorrect.
func TestDeriveFeralFileCIDs_NilIndexSkipped(t *testing.T) {
	t.Parallel()

	w := &coreWorkflows{
		config: CoreWorkflowsConfig{EthereumChainID: domain.ChainEthereumMainnet},
		feralfileClient: &fakeFFClient{
			artworks: []feralfile.ArtworkRef{
				// nil Index: release membership is unknown — must not match mint 1.
				{Index: nil, Chain: "ethereum", ContractAddress: "0xabc", TokenID: "10"},
				// Known index 1 (mint 2): should be returned.
				{Index: int64Ptr(1), Chain: "ethereum", ContractAddress: "0xabc", TokenID: "11"},
			},
		},
	}
	cids, skipped, err := w.deriveFeralFileCIDs(context.Background(), "series-uuid", []int64{1, 2})

	require.NoError(t, err)
	// skipped tracks API artworks we couldn't use (nil-index here), not unfound mint numbers.
	// The nil-index artwork is counted as skipped; index-1 (mint 2) is returned.
	assert.Equal(t, 1, skipped)
	require.Len(t, cids, 1)
	assert.Contains(t, string(cids[0]), ":11")
}

// TestDeriveFeralFileCIDs_AllNilIndexSkipped verifies that a response of all nil-index
// artworks returns no CIDs and skips all requested mints.
func TestDeriveFeralFileCIDs_AllNilIndexSkipped(t *testing.T) {
	t.Parallel()

	w := &coreWorkflows{
		config: CoreWorkflowsConfig{EthereumChainID: domain.ChainEthereumMainnet},
		feralfileClient: &fakeFFClient{
			artworks: []feralfile.ArtworkRef{
				{Index: nil, Chain: "ethereum", ContractAddress: "0xabc", TokenID: "1"},
				{Index: nil, Chain: "ethereum", ContractAddress: "0xabc", TokenID: "2"},
			},
		},
	}
	cids, skipped, err := w.deriveFeralFileCIDs(context.Background(), "series-uuid", []int64{1, 2})

	require.NoError(t, err)
	assert.Len(t, cids, 0)
	assert.Equal(t, 2, skipped) // both requested mints unresolved
}

func TestDeriveFeralFileCIDs_NilClient(t *testing.T) {
	t.Parallel()

	w := &coreWorkflows{feralfileClient: nil}
	_, _, err := w.deriveFeralFileCIDs(context.Background(), "series-uuid", []int64{1, 2, 3})

	require.Error(t, err)
	assert.Contains(t, err.Error(), "feral file client not configured")
}

func TestDeriveFeralFileCIDs_APIError(t *testing.T) {
	t.Parallel()

	w := &coreWorkflows{
		feralfileClient: &fakeFFClient{err: errors.New("ff api error")},
	}
	_, _, err := w.deriveFeralFileCIDs(context.Background(), "series-uuid", []int64{1, 2, 3, 4, 5})

	require.Error(t, err)
	assert.Contains(t, err.Error(), "GetSeriesArtworks failed")
}

func TestDeriveFeralFileCIDs_MissingContractSkipped(t *testing.T) {
	t.Parallel()

	w := &coreWorkflows{
		config: CoreWorkflowsConfig{EthereumChainID: domain.ChainEthereumMainnet},
		feralfileClient: &fakeFFClient{
			artworks: []feralfile.ArtworkRef{
				{Index: int64Ptr(0), Chain: "ethereum", ContractAddress: "", TokenID: "1"},
			},
		},
	}
	cids, skipped, err := w.deriveFeralFileCIDs(context.Background(), "series-uuid", []int64{1})

	require.NoError(t, err)
	assert.Equal(t, 1, skipped)
	assert.Len(t, cids, 0)
}

// ──────────────────────────────────────────────────────────────────────────────
// objkt CID derivation with custom-contract pre-check
// ──────────────────────────────────────────────────────────────────────────────

func TestDeriveObjktCIDs_CustomContractPasses(t *testing.T) {
	t.Parallel()

	w := &coreWorkflows{
		config:      CoreWorkflowsConfig{TezosChainID: domain.ChainTezosMainnet},
		objktClient: &fakeObjktClient{fa: &objkt.FA{Name: "Test Collection", CollectionType: "custom"}},
	}

	cids, skipped, err := w.deriveReleaseCIDs(context.Background(), schema.VendorObjkt, "KT1abc", []int64{1, 2, 3})

	require.NoError(t, err)
	assert.Equal(t, 0, skipped)
	require.Len(t, cids, 3)
	for i, cid := range cids {
		chain, standard, contract, tokenID := cid.Parse()
		assert.Equal(t, domain.ChainTezosMainnet, chain)
		assert.Equal(t, domain.StandardFA2, standard)
		assert.Equal(t, "KT1abc", contract)
		assert.Equal(t, fmt.Sprintf("%d", i+1), tokenID, "mintNum must equal tokenID for objkt")
	}
}

func TestDeriveObjktCIDs_NonCustomContractRejected(t *testing.T) {
	t.Parallel()

	nonCustomTypes := []string{"open", "open_generative", "curated"}
	for _, collType := range nonCustomTypes {
		collType := collType
		t.Run(collType, func(t *testing.T) {
			t.Parallel()

			w := &coreWorkflows{
				config: CoreWorkflowsConfig{TezosChainID: domain.ChainTezosMainnet},
				objktClient: &fakeObjktClient{
					err: fmt.Errorf("objkt contract KT1abc is a %q collection, not \"custom\": %w", collType, objkt.ErrCollectionNotCustom),
				},
			}

			_, _, err := w.deriveReleaseCIDs(context.Background(), schema.VendorObjkt, "KT1abc", []int64{1, 2, 3, 4, 5})

			require.Error(t, err)
			assert.Contains(t, err.Error(), "objkt pre-check failed")
		})
	}
}

func TestDeriveObjktCIDs_NilClientRejected(t *testing.T) {
	t.Parallel()

	w := &coreWorkflows{
		config:      CoreWorkflowsConfig{TezosChainID: domain.ChainTezosMainnet},
		objktClient: nil,
	}

	_, _, err := w.deriveReleaseCIDs(context.Background(), schema.VendorObjkt, "KT1abc", []int64{1})

	require.Error(t, err)
	assert.Contains(t, err.Error(), "objkt client not configured")
}

func TestDeriveObjktCIDs_GetFAAPIError(t *testing.T) {
	t.Parallel()

	w := &coreWorkflows{
		config:      CoreWorkflowsConfig{TezosChainID: domain.ChainTezosMainnet},
		objktClient: &fakeObjktClient{err: errors.New("API timeout")},
	}

	_, _, err := w.deriveReleaseCIDs(context.Background(), schema.VendorObjkt, "KT1abc", []int64{1})

	require.Error(t, err)
	assert.Contains(t, err.Error(), "objkt pre-check failed")
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
