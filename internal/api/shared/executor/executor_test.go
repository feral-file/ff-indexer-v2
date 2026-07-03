package executor_test

import (
	"context"
	"errors"
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
	"github.com/feral-file/ff-indexer-v2/internal/store"
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

func expectNoReleaseMembership(mockStore *mocks.MockStore, tokenIDs []uint64) {
	mockStore.EXPECT().
		GetReleaseMembersByTokenIDs(gomock.Any(), tokenIDs).
		Return(map[uint64]*schema.ReleaseMember{}, nil).
		AnyTimes()
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
	expectNoReleaseMembership(mockStore, []uint64{f.tokenID})
}

// containsURL returns a Matcher that verifies a []string argument contains target.
// Used to assert GetMediaAssetsBySourceURLs is called with a URL that may only exist in
// token_media_health.media_url (not in raw metadata/enrichment), which is the scenario our
// fix must handle.
func containsURL(target string) gomock.Matcher {
	return gomock.Cond(func(x any) bool {
		urls, ok := x.([]string)
		if !ok {
			return false
		}
		for _, u := range urls {
			if u == target {
				return true
			}
		}
		return false
	})
}

// healthOnlyFixture represents the scenario where ApplyHealthyMediaURLs selects a URL that
// exists only in token_media_health.media_url and NOT in raw metadata or enrichment sources.
//
// This simulates the PostgreSQL READ COMMITTED race window: the API reads metadata before a
// propagation transaction commits (metadata still has originalURL) but reads the health table
// after it commits (health row already has workingURL). ApplyHealthyMediaURLs then sets
// display.animation_url = workingURL, which would be absent from the media asset source URL
// set built from raw metadata — causing collectMediaAssetDTOsFromMap to return nothing for it
// without the fix that adds tokenDTO.Display.MediaURLs() to the DB lookup.
type healthOnlyFixture struct {
	tokenID       uint64
	normalizedCID string
	originalURL   string // stale/broken URL still in metadata
	workingURL    string // healthy URL only in health table
	token         *schema.Token
	metadata      *schema.TokenMetadata
	healthRows    map[uint64][]schema.TokenMediaHealth
	mediaAsset    schema.MediaAsset
}

func newHealthOnlyFixture() *healthOnlyFixture {
	const tokenID = uint64(43)
	const rawCID = "eip155:1:erc721:0xdef456:2" //nolint:gosec
	normalizedCID := domain.TokenCID(rawCID).Normalized().String()
	originalURL := "https://ipfs.feralfile.com/ipfs/QmSTALE"
	workingURL := "https://ipfs.io/ipfs/QmWORKING"

	return &healthOnlyFixture{
		tokenID:       tokenID,
		normalizedCID: normalizedCID,
		originalURL:   originalURL,
		workingURL:    workingURL,
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
			AnimationURL: ptrStr(originalURL), // stale; health row has workingURL
		},
		healthRows: map[uint64][]schema.TokenMediaHealth{
			tokenID: {
				{
					TokenID:      tokenID,
					MediaURL:     workingURL,
					MediaSource:  schema.MediaHealthSourceMetadataAnimation,
					HealthStatus: schema.MediaHealthStatusHealthy,
				},
			},
		},
		mediaAsset: schema.MediaAsset{
			ID:            2,
			SourceURL:     workingURL,
			SourceURLHash: internalTypes.MD5Hash(workingURL),
			Provider:      "cloudflare",
		},
	}
}

// TestGetToken_DisplayAndMediaAsset_HealthOnlyURL verifies the fix for the media asset
// lookup miss when the display URL comes only from token_media_health (not from raw
// metadata/enrichment). The fix appends tokenDTO.Display.MediaURLs() to the source URL
// set in expandMediaAssets before the DB fetch, ensuring the media asset is found.
func TestGetToken_DisplayAndMediaAsset_HealthOnlyURL(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	f := newHealthOnlyFixture()
	exec, mockStore := newTestExecutor(t, ctrl)

	mockStore.EXPECT().
		GetTokenByTokenCID(gomock.Any(), f.normalizedCID).
		Return(f.token, nil)
	mockStore.EXPECT().
		GetTokenMetadataByTokenID(gomock.Any(), f.tokenID).
		Return(f.metadata, nil).
		AnyTimes()
	mockStore.EXPECT().
		GetEnrichmentSourceByTokenID(gomock.Any(), f.tokenID).
		Return(nil, nil).
		AnyTimes()
	mockStore.EXPECT().
		GetTokenMediaHealthByTokenIDs(gomock.Any(), []uint64{f.tokenID}).
		Return(f.healthRows, nil)
	// workingURL exists only in the health table, not in raw metadata. The fix must add it
	// to the DB lookup. If it does not, this expectation will fail (missing call).
	mockStore.EXPECT().
		GetMediaAssetsBySourceURLs(gomock.Any(), containsURL(f.workingURL)).
		Return([]schema.MediaAsset{f.mediaAsset}, nil)
	expectNoReleaseMembership(mockStore, []uint64{f.tokenID})

	const rawCID = "eip155:1:erc721:0xdef456:2" //nolint:gosec
	result, err := exec.GetToken(context.Background(), rawCID,
		[]types.Expansion{types.ExpansionDisplay, types.ExpansionMediaAsset},
		nil, nil, nil, nil, nil)

	require.NoError(t, err)
	require.NotNil(t, result)
	require.NotNil(t, result.Display, "display must be populated")
	assert.Equal(t, f.workingURL, *result.Display.AnimationURL,
		"display.animation_url must be the URL from the health table, not the stale metadata URL")
	require.NotNil(t, result.MediaAssets, "media_assets must be populated")
	assert.Len(t, result.MediaAssets, 1, "exactly one media asset expected for the working URL")
	assert.Equal(t, f.workingURL, result.MediaAssets[0].SourceURL,
		"media asset source URL must match the health-filtered display URL")
}

// TestGetTokens_DisplayAndMediaAsset_HealthOnlyURL verifies the list-path analog: the bulk
// pre-compute loop in GetTokens adds health-filtered display URLs to allMediaSourceURLs before
// GetMediaAssetsBySourceURLs is called, so the media asset map contains the workingURL entry.
func TestGetTokens_DisplayAndMediaAsset_HealthOnlyURL(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	f := newHealthOnlyFixture()
	exec, mockStore := newTestExecutor(t, ctrl)

	mockStore.EXPECT().
		GetTokensByFilter(gomock.Any(), gomock.Any()).
		Return([]schema.Token{*f.token}, nil)
	mockStore.EXPECT().
		GetTokenMetadataByTokenIDs(gomock.Any(), []uint64{f.tokenID}).
		Return(map[uint64]*schema.TokenMetadata{f.tokenID: f.metadata}, nil)
	mockStore.EXPECT().
		GetEnrichmentSourcesByTokenIDs(gomock.Any(), []uint64{f.tokenID}).
		Return(map[uint64]*schema.EnrichmentSource{}, nil)
	mockStore.EXPECT().
		GetTokenMediaHealthByTokenIDs(gomock.Any(), []uint64{f.tokenID}).
		Return(f.healthRows, nil)
	// The pre-compute loop must have added workingURL to allMediaSourceURLs.
	mockStore.EXPECT().
		GetMediaAssetsBySourceURLs(gomock.Any(), containsURL(f.workingURL)).
		Return([]schema.MediaAsset{f.mediaAsset}, nil)
	expectNoReleaseMembership(mockStore, []uint64{f.tokenID})

	result, err := exec.GetTokens(context.Background(),
		nil, nil, nil, nil, []uint64{f.tokenID}, nil, nil,
		nil, nil, nil, nil, nil, nil, nil,
		[]types.Expansion{types.ExpansionDisplay, types.ExpansionMediaAsset})

	require.NoError(t, err)
	require.NotNil(t, result)
	require.Len(t, result.Tokens, 1)
	tok := result.Tokens[0]
	require.NotNil(t, tok.Display, "display must be populated")
	assert.Equal(t, f.workingURL, *tok.Display.AnimationURL,
		"display.animation_url must be the health-table URL (workingURL)")
	require.NotNil(t, tok.MediaAssets, "media_assets must be populated")
	assert.Len(t, tok.MediaAssets, 1)
	assert.Equal(t, f.workingURL, tok.MediaAssets[0].SourceURL)
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

// TestGetToken_DisplayExpansion_HealthQueryFailure verifies that a GetTokenMediaHealthByTokenIDs
// failure causes GetToken to return an error rather than silently passing through stale or
// broken display URLs. Health state is a correctness dependency of the display expansion, so
// its failure must be handled like metadata/enrichment DB failures.
func TestGetToken_DisplayExpansion_HealthQueryFailure(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	f := newDisplayMediaAssetFixture()
	exec, mockStore := newTestExecutor(t, ctrl)

	mockStore.EXPECT().
		GetTokenByTokenCID(gomock.Any(), f.normalizedCID).
		Return(f.token, nil)
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
		Return(nil, errors.New("connection timeout"))
	expectNoReleaseMembership(mockStore, []uint64{f.tokenID})

	result, err := exec.GetToken(context.Background(), f.rawCID,
		[]types.Expansion{types.ExpansionDisplay}, nil, nil, nil, nil, nil)

	require.Error(t, err, "GetToken must propagate health query failure as an error")
	assert.Nil(t, result, "result must be nil on health query failure")
}

// TestGetTokens_DisplayExpansion_HealthQueryFailure verifies that a GetTokenMediaHealthByTokenIDs
// failure causes GetTokens to return an error. The list path must behave consistently with the
// single-token path and with other expansion DB failures (metadata, enrichment).
func TestGetTokens_DisplayExpansion_HealthQueryFailure(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	f := newHealthOnlyFixture()
	exec, mockStore := newTestExecutor(t, ctrl)

	mockStore.EXPECT().
		GetTokensByFilter(gomock.Any(), gomock.Any()).
		Return([]schema.Token{*f.token}, nil)
	mockStore.EXPECT().
		GetTokenMetadataByTokenIDs(gomock.Any(), []uint64{f.tokenID}).
		Return(map[uint64]*schema.TokenMetadata{f.tokenID: f.metadata}, nil)
	mockStore.EXPECT().
		GetEnrichmentSourcesByTokenIDs(gomock.Any(), []uint64{f.tokenID}).
		Return(map[uint64]*schema.EnrichmentSource{}, nil)
	mockStore.EXPECT().
		GetTokenMediaHealthByTokenIDs(gomock.Any(), []uint64{f.tokenID}).
		Return(nil, errors.New("connection timeout"))

	result, err := exec.GetTokens(context.Background(),
		nil, nil, nil, nil, []uint64{f.tokenID}, nil, nil,
		nil, nil, nil, nil, nil, nil, nil,
		[]types.Expansion{types.ExpansionDisplay})

	require.Error(t, err, "GetTokens must propagate health query failure as an error")
	assert.Nil(t, result, "result must be nil on health query failure")
}

func TestGetRelease_Found(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	exec, mockStore := newTestExecutor(t, ctrl)

	name := "Fidenza"
	totalMints := int64(999)
	mockStore.EXPECT().
		GetReleaseByID(gomock.Any(), uint64(7)).
		Return(&schema.Release{
			ID:              7,
			Vendor:          schema.VendorArtBlocks,
			VendorReleaseID: "0xabc-1",
			Name:            &name,
			TotalMints:      &totalMints,
		}, nil)

	result, err := exec.GetRelease(context.Background(), 7)
	require.NoError(t, err)
	require.NotNil(t, result)
	assert.Equal(t, uint64(7), result.ID)
	assert.Equal(t, string(schema.VendorArtBlocks), result.Vendor)
	assert.Equal(t, "0xabc-1", result.VendorReleaseID)
	require.NotNil(t, result.Name)
	assert.Equal(t, "Fidenza", *result.Name)
	require.NotNil(t, result.TotalMints)
	assert.Equal(t, int64(999), *result.TotalMints)
}

func TestGetRelease_NotFound(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	exec, mockStore := newTestExecutor(t, ctrl)

	mockStore.EXPECT().
		GetReleaseByID(gomock.Any(), uint64(404)).
		Return(nil, nil)

	result, err := exec.GetRelease(context.Background(), 404)
	require.NoError(t, err)
	assert.Nil(t, result)
}

func TestListReleases_ByVendor(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	exec, mockStore := newTestExecutor(t, ctrl)

	vendor := schema.VendorArtBlocks
	limit := uint8(20)
	offset := uint64(0)
	mockStore.EXPECT().
		ListReleases(gomock.Any(), store.ReleaseQueryFilter{
			Vendor: &vendor,
			Limit:  21,
			Offset: 0,
		}).
		Return([]schema.Release{{
			ID:              7,
			Vendor:          schema.VendorArtBlocks,
			VendorReleaseID: "0xabc-1",
		}}, nil)

	result, err := exec.ListReleases(context.Background(), nil, &vendor, nil, &limit, &offset)
	require.NoError(t, err)
	require.Len(t, result.Items, 1)
	assert.Equal(t, uint64(7), result.Items[0].ID)
	assert.Nil(t, result.Offset)
}

func TestListReleases_Pagination(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	exec, mockStore := newTestExecutor(t, ctrl)

	vendorReleaseID := "series-uuid"
	limit := uint8(1)
	offset := uint64(0)
	mockStore.EXPECT().
		ListReleases(gomock.Any(), store.ReleaseQueryFilter{
			VendorReleaseID: &vendorReleaseID,
			Limit:           2,
			Offset:          0,
		}).
		Return([]schema.Release{
			{ID: 1, Vendor: schema.VendorFeralFile, VendorReleaseID: vendorReleaseID},
			{ID: 2, Vendor: schema.VendorFeralFile, VendorReleaseID: "other"},
		}, nil)

	result, err := exec.ListReleases(context.Background(), nil, nil, &vendorReleaseID, &limit, &offset)
	require.NoError(t, err)
	require.Len(t, result.Items, 1)
	assert.Equal(t, uint64(1), result.Items[0].ID)
	require.NotNil(t, result.Offset)
	assert.Equal(t, uint64(1), *result.Offset)
}

func TestListReleases_ByIDs(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	exec, mockStore := newTestExecutor(t, ctrl)

	ids := []uint64{3, 7}
	limit := uint8(20)
	offset := uint64(0)
	mockStore.EXPECT().
		ListReleases(gomock.Any(), store.ReleaseQueryFilter{
			IDs:    ids,
			Limit:  21,
			Offset: 0,
		}).
		Return([]schema.Release{
			{ID: 3, Vendor: schema.VendorFeralFile, VendorReleaseID: "uuid-3"},
			{ID: 7, Vendor: schema.VendorArtBlocks, VendorReleaseID: "0xabc-1"},
		}, nil)

	result, err := exec.ListReleases(context.Background(), ids, nil, nil, &limit, &offset)
	require.NoError(t, err)
	require.Len(t, result.Items, 2)
	assert.Equal(t, uint64(3), result.Items[0].ID)
	assert.Equal(t, uint64(7), result.Items[1].ID)
	assert.Nil(t, result.Offset)
}

func TestGetToken_AppliesReleaseMembership(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	const tokenID = uint64(42)
	const rawCID = "eip155:1:erc721:0xabc123:1" //nolint:gosec
	normalizedCID := domain.TokenCID(rawCID).Normalized().String()

	exec, mockStore := newTestExecutor(t, ctrl)
	mockStore.EXPECT().
		GetTokenByTokenCID(gomock.Any(), normalizedCID).
		Return(&schema.Token{
			ID:       tokenID,
			TokenCID: normalizedCID,
		}, nil)
	mockStore.EXPECT().
		GetReleaseMembersByTokenIDs(gomock.Any(), []uint64{tokenID}).
		Return(map[uint64]*schema.ReleaseMember{
			tokenID: {
				ReleaseID:  99,
				TokenID:    tokenID,
				MintNumber: 3,
			},
		}, nil)

	result, err := exec.GetToken(context.Background(), rawCID, nil, nil, nil, nil, nil, nil)
	require.NoError(t, err)
	require.NotNil(t, result)
	require.NotNil(t, result.ReleaseID)
	require.NotNil(t, result.MintNumber)
	assert.Equal(t, uint64(99), *result.ReleaseID)
	assert.Equal(t, int64(3), *result.MintNumber)
}
