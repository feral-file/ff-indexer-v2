package executor_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/feral-file/ff-indexer-v2/internal/adapter"
	"github.com/feral-file/ff-indexer-v2/internal/api/shared/executor"
	"github.com/feral-file/ff-indexer-v2/internal/api/shared/types"
	"github.com/feral-file/ff-indexer-v2/internal/domain"
	"github.com/feral-file/ff-indexer-v2/internal/mocks"
	"github.com/feral-file/ff-indexer-v2/internal/store/schema"
	internalTypes "github.com/feral-file/ff-indexer-v2/internal/types"
)

// newTestExecutor builds an executor wired with a mock store and real JSON/clock adapters.
// Only the store mock is returned because all other dependencies are not exercised by display tests.
func newTestExecutor(t *testing.T, ctrl *gomock.Controller) (executor.Executor, *mocks.MockStore) {
	t.Helper()
	mockStore := mocks.NewMockStore(ctrl)
	mockJobQueue := mocks.NewMockJobQueue(ctrl)
	mockBlacklist := mocks.NewMockBlacklistRegistry(ctrl)
	exec := executor.NewExecutor(
		mockStore,
		mockJobQueue,
		"token_index",
		mockBlacklist,
		adapter.NewJSON(),
		adapter.NewClock(),
		domain.Chain("tezos:mainnet"),
		domain.Chain("eip155:1"),
	)
	return exec, mockStore
}

// ptrStr is a convenience helper that returns a *string.
func ptrStr(s string) *string { return &s }

// displayMediaAssetFixture holds the shared state for display+media_asset expansion tests.
type displayMediaAssetFixture struct {
	tokenID       uint64
	rawCID        string
	normalizedCID string
	brokenURL     string
	healthyURL    string
	token         *schema.Token
	metadata      *schema.TokenMetadata
	enrichment    *schema.EnrichmentSource
	healthRows    map[uint64][]schema.TokenMediaHealth
	mediaAsset    schema.MediaAsset
}

// newDisplayMediaAssetFixture constructs the test state shared across display+media_asset tests.
// Scenario: enrichment has a broken animation URL; metadata has a healthy one.
// After health filtering, display and media_assets must both use the healthy metadata URL.
func newDisplayMediaAssetFixture() *displayMediaAssetFixture {
	const tokenID = uint64(42)
	const rawCID = "eip155:1:erc721:0xabc123:1" //nolint:gosec
	// The executor normalizes token CIDs before the store lookup; precompute for mock expectations.
	normalizedCID := domain.TokenCID(rawCID).Normalized().String()
	brokenURL := "https://ipfs.feralfile.com/ipfs/QmBROKEN"
	healthyURL := "https://ipfs.io/ipfs/QmHEALTHY"

	return &displayMediaAssetFixture{
		tokenID:       tokenID,
		rawCID:        rawCID,
		normalizedCID: normalizedCID,
		brokenURL:     brokenURL,
		healthyURL:    healthyURL,
		token: &schema.Token{
			ID:         tokenID,
			TokenCID:   normalizedCID,
			Chain:      "eip155:1",
			Standard:   "erc721",
			IsViewable: true,
			CreatedAt:  time.Now(),
			UpdatedAt:  time.Now(),
		},
		metadata: &schema.TokenMetadata{
			TokenID:      tokenID,
			AnimationURL: ptrStr(healthyURL),
		},
		enrichment: &schema.EnrichmentSource{
			TokenID:      tokenID,
			Vendor:       "feralfile",
			AnimationURL: ptrStr(brokenURL), // higher merge precedence but broken
		},
		healthRows: map[uint64][]schema.TokenMediaHealth{
			tokenID: {
				{
					TokenID:      tokenID,
					MediaURL:     brokenURL,
					MediaSource:  schema.MediaHealthSourceEnrichmentAnimation,
					HealthStatus: schema.MediaHealthStatusBroken,
				},
				{
					TokenID:      tokenID,
					MediaURL:     healthyURL,
					MediaSource:  schema.MediaHealthSourceMetadataAnimation,
					HealthStatus: schema.MediaHealthStatusHealthy,
				},
			},
		},
		mediaAsset: schema.MediaAsset{
			ID:            1,
			SourceURL:     healthyURL,
			SourceURLHash: internalTypes.MD5Hash(healthyURL),
			Provider:      "cloudflare",
		},
	}
}

func (f *displayMediaAssetFixture) setupMocks(mockStore *mocks.MockStore) {
	mockStore.EXPECT().
		GetTokenByTokenCID(gomock.Any(), f.normalizedCID).
		Return(f.token, nil)
	// Metadata and enrichment may be fetched more than once depending on expansion order
	// (display pre-population in the media_asset case + the media_asset source collection).
	mockStore.EXPECT().
		GetTokenMetadataByTokenID(gomock.Any(), f.tokenID).
		Return(f.metadata, nil).
		AnyTimes()
	mockStore.EXPECT().
		GetEnrichmentSourceByTokenID(gomock.Any(), f.tokenID).
		Return(f.enrichment, nil).
		AnyTimes()
	mockStore.EXPECT().
		GetTokenMediaHealthByTokenIDs(gomock.Any(), []uint64{f.tokenID}).
		Return(f.healthRows, nil)
	mockStore.EXPECT().
		GetMediaAssetsBySourceURLs(gomock.Any(), gomock.Any()).
		Return([]schema.MediaAsset{f.mediaAsset}, nil)
}

// TestGetToken_DisplayAndMediaAsset_HealthFiltered verifies the fix for the regression where
// display+media_asset expansion could return zero display-derived media assets because
// expandMediaAssets ran before tokenDTO.Display was populated.
//
// Scenario: enrichment animation URL is broken; metadata animation URL is healthy.
// Expected: display.animation_url = healthy metadata URL; media_assets derived from healthy URL only.
func TestGetToken_DisplayAndMediaAsset_HealthFiltered(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	f := newDisplayMediaAssetFixture()
	exec, mockStore := newTestExecutor(t, ctrl)
	f.setupMocks(mockStore)

	expansions := []types.Expansion{types.ExpansionDisplay, types.ExpansionMediaAsset}
	result, err := exec.GetToken(context.Background(), f.rawCID, expansions, nil, nil, nil, nil, nil)

	require.NoError(t, err)
	require.NotNil(t, result)

	// display.animation_url must be the healthy metadata URL, not the broken enrichment URL
	require.NotNil(t, result.Display, "display should be populated")
	assert.Equal(t, f.healthyURL, *result.Display.AnimationURL,
		"display.animation_url must be the confirmed-healthy URL, not the broken enrichment URL")

	// media_assets must be derived from the health-filtered display URL
	require.NotNil(t, result.MediaAssets, "media_assets should be populated")
	assert.Len(t, result.MediaAssets, 1, "only the healthy URL should produce a media asset")
	assert.Equal(t, f.healthyURL, result.MediaAssets[0].SourceURL,
		"media asset source URL must match the health-filtered display URL")
}

// TestGetToken_DisplayAndMediaAsset_ReverseExpansionOrder verifies that the ordering of
// expand=media_asset before expand=display produces the same health-filtered result.
// This was the key regression: media_asset expansion ran inside the loop while display
// was only populated in the post-loop block, so reversing the order left tokenDTO.Display
// nil when expandMediaAssets tried to read it.
func TestGetToken_DisplayAndMediaAsset_ReverseExpansionOrder(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	f := newDisplayMediaAssetFixture()
	exec, mockStore := newTestExecutor(t, ctrl)
	f.setupMocks(mockStore)

	// Reverse order: media_asset before display — this was the broken case before the fix.
	expansions := []types.Expansion{types.ExpansionMediaAsset, types.ExpansionDisplay}
	result, err := exec.GetToken(context.Background(), f.rawCID, expansions, nil, nil, nil, nil, nil)

	require.NoError(t, err)
	require.NotNil(t, result)
	require.NotNil(t, result.Display, "display should be populated regardless of expansion order")
	assert.Equal(t, f.healthyURL, *result.Display.AnimationURL,
		"display.animation_url must be the healthy URL regardless of expansion order")
	require.NotNil(t, result.MediaAssets, "media_assets should be populated regardless of expansion order")
	assert.Len(t, result.MediaAssets, 1)
	assert.Equal(t, f.healthyURL, result.MediaAssets[0].SourceURL,
		"media asset source URL must match the health-filtered display URL")
}
