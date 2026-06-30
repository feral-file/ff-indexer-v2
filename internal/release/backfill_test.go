package release_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/feral-file/ff-indexer-v2/internal/adapter"
	"github.com/feral-file/ff-indexer-v2/internal/mocks"
	"github.com/feral-file/ff-indexer-v2/internal/providers/vendors/feralfile"
	"github.com/feral-file/ff-indexer-v2/internal/release"
	"github.com/feral-file/ff-indexer-v2/internal/store/schema"
)

func TestReleaseInfoFromArtBlocks(t *testing.T) {
	t.Parallel()

	vendorReleaseID, mintNumber, err := release.ReleaseInfoFromArtBlocks(
		"0xA7d8d9ef8D8Ce8992Df33D8b8CF4Aebabd5bD270",
		"1000005",
	)
	require.NoError(t, err)
	assert.Equal(t, "0xa7d8d9ef8d8ce8992df33d8b8cf4aebabd5bd270-1", vendorReleaseID)
	assert.Equal(t, int64(5), mintNumber)
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
