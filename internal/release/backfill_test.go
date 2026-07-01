package release_test

import (
	"context"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/feral-file/ff-indexer-v2/internal/adapter"
	"github.com/feral-file/ff-indexer-v2/internal/domain"
	"github.com/feral-file/ff-indexer-v2/internal/logger"
	"github.com/feral-file/ff-indexer-v2/internal/mocks"
	"github.com/feral-file/ff-indexer-v2/internal/providers/vendors/feralfile"
	"github.com/feral-file/ff-indexer-v2/internal/release"
	"github.com/feral-file/ff-indexer-v2/internal/store/schema"
)

func TestMain(m *testing.M) {
	if err := logger.Initialize(logger.Config{Debug: false}); err != nil {
		panic(err)
	}
	os.Exit(m.Run())
}

func TestReleaseInfoFromArtBlocks(t *testing.T) {
	t.Parallel()

	// tokenID 1000005 → projectID 1, raw 0-based mint index 5, 1-based mint_number 6.
	// vendor_release_id includes chain: "{chainID}-{contract}-{projectID}".
	vendorReleaseID, mintNumber, err := release.ReleaseInfoFromArtBlocks(
		domain.ChainEthereumMainnet,
		"0xA7d8d9ef8D8Ce8992Df33D8b8CF4Aebabd5bD270",
		"1000005",
	)
	require.NoError(t, err)
	assert.Equal(t, "1-0xa7d8d9ef8d8ce8992df33d8b8cf4aebabd5bd270-1", vendorReleaseID)
	assert.Equal(t, int64(6), mintNumber)
}

// TestReleaseInfoFromArtBlocks_FirstTokenInProject verifies the boundary condition
// where the raw AB mint index is 0 (first token of a project). The returned
// mint_number must be 1, not 0, to satisfy the 1-based schema contract.
func TestReleaseInfoFromArtBlocks_FirstTokenInProject(t *testing.T) {
	t.Parallel()

	// tokenID 1000000 → projectID 1, raw 0-based mint index 0, 1-based mint_number 1.
	vendorReleaseID, mintNumber, err := release.ReleaseInfoFromArtBlocks(
		domain.ChainEthereumMainnet,
		"0xA7d8d9ef8D8Ce8992Df33D8b8CF4Aebabd5bD270",
		"1000000",
	)
	require.NoError(t, err)
	assert.Equal(t, "1-0xa7d8d9ef8d8ce8992df33d8b8cf4aebabd5bd270-1", vendorReleaseID)
	assert.Equal(t, int64(1), mintNumber)
}

// TestReleaseInfoFromArtBlocks_NonEVMChain verifies that a non-EIP-155 chain returns an error
// because Art Blocks is an EVM-only contract.
func TestReleaseInfoFromArtBlocks_NonEVMChain(t *testing.T) {
	t.Parallel()

	_, _, err := release.ReleaseInfoFromArtBlocks(
		domain.ChainTezosMainnet,
		"KT1ABC",
		"1000005",
	)
	require.Error(t, err)
}

func TestBackfillerReleaseInfoFromFeralFileUsesVendorJSON(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	jsonAdapter := adapter.NewJSON()
	ffClient := mocks.NewMockFeralFileClient(ctrl)

	backfiller := release.NewBackfiller(nil, ffClient, jsonAdapter, 100)

	source := schema.EnrichmentSource{
		VendorJSON: []byte(`{"seriesID":"series-uuid","index":4}`),
	}

	vendorReleaseID, mintNumber, err := backfiller.ReleaseInfoFromFeralFile(context.Background(), source, "123")
	require.NoError(t, err)
	assert.Equal(t, "series-uuid", vendorReleaseID)
	assert.Equal(t, int64(5), mintNumber)
}

func TestBackfillerReleaseInfoFromFeralFileFetchesAPIWhenMissingSeriesID(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	jsonAdapter := adapter.NewJSON()
	ffClient := mocks.NewMockFeralFileClient(ctrl)

	backfiller := release.NewBackfiller(nil, ffClient, jsonAdapter, 100)

	source := schema.EnrichmentSource{
		VendorJSON: []byte(`{"name":"Test"}`),
	}

	ffClient.EXPECT().GetArtwork(gomock.Any(), "123").Return(&feralfile.Artwork{
		SeriesID: "series-uuid",
		Index:    0,
	}, nil)

	vendorReleaseID, mintNumber, err := backfiller.ReleaseInfoFromFeralFile(context.Background(), source, "123")
	require.NoError(t, err)
	assert.Equal(t, "series-uuid", vendorReleaseID)
	assert.Equal(t, int64(1), mintNumber)
}

func TestBackfillerRunSuccess(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := mocks.NewMockStore(ctrl)
	ffClient := mocks.NewMockFeralFileClient(ctrl)
	backfiller := release.NewBackfiller(mockStore, ffClient, adapter.NewJSON(), 100)

	ctx := context.Background()
	const tokenID = uint64(1)
	sources := []schema.EnrichmentSource{{
		TokenID: tokenID,
		Vendor:  schema.VendorArtBlocks,
	}}

	mockStore.EXPECT().
		ListEnrichmentSourcesByVendors(ctx, gomock.Any(), 100, uint64(0)).
		Return(sources, nil)
	mockStore.EXPECT().
		GetTokenByID(ctx, tokenID).
		Return(&schema.Token{
			ID:              tokenID,
			Chain:           domain.ChainEthereumMainnet,
			ContractAddress: "0xA7d8d9ef8D8Ce8992Df33D8b8CF4Aebabd5bD270",
			TokenNumber:     "1000005",
		}, nil)
	// vendor_release_id is chain-qualified: "{chainID}-{contract}-{projectID}"
	mockStore.EXPECT().
		UpsertRelease(ctx, schema.VendorArtBlocks, "1-0xa7d8d9ef8d8ce8992df33d8b8cf4aebabd5bd270-1", gomock.Any(), gomock.Any()).
		Return(&schema.Release{ID: 42}, nil)
	// tokenID 1000005 → raw AB index 5 → 1-based mint_number 6
	mockStore.EXPECT().
		UpsertReleaseMember(ctx, uint64(42), tokenID, int64(6)).
		Return(nil)

	processed, err := backfiller.Run(ctx)
	require.NoError(t, err)
	assert.Equal(t, 1, processed)
}

func TestBackfillerRunPartialFailure(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := mocks.NewMockStore(ctrl)
	ffClient := mocks.NewMockFeralFileClient(ctrl)
	backfiller := release.NewBackfiller(mockStore, ffClient, adapter.NewJSON(), 100)

	ctx := context.Background()
	sources := []schema.EnrichmentSource{
		{TokenID: 1, Vendor: schema.VendorArtBlocks},
		{TokenID: 2, Vendor: schema.VendorArtBlocks},
	}

	mockStore.EXPECT().
		ListEnrichmentSourcesByVendors(ctx, gomock.Any(), 100, uint64(0)).
		Return(sources, nil)
	mockStore.EXPECT().
		GetTokenByID(ctx, uint64(1)).
		Return(&schema.Token{
			ID:              1,
			Chain:           domain.ChainEthereumMainnet,
			ContractAddress: "0xA7d8d9ef8D8Ce8992Df33D8b8CF4Aebabd5bD270",
			TokenNumber:     "1000005",
		}, nil)
	// vendor_release_id is chain-qualified: "{chainID}-{contract}-{projectID}"
	mockStore.EXPECT().
		UpsertRelease(ctx, schema.VendorArtBlocks, "1-0xa7d8d9ef8d8ce8992df33d8b8cf4aebabd5bd270-1", gomock.Any(), gomock.Any()).
		Return(&schema.Release{ID: 42}, nil)
	// tokenID 1000005 → raw AB index 5 → 1-based mint_number 6
	mockStore.EXPECT().
		UpsertReleaseMember(ctx, uint64(42), uint64(1), int64(6)).
		Return(nil)
	mockStore.EXPECT().
		GetTokenByID(ctx, uint64(2)).
		Return(nil, nil)

	processed, err := backfiller.Run(ctx)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "partial backfill: 1 succeeded, 1 failed")
	assert.Equal(t, 1, processed)
}
