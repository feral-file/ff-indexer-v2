package workflows_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/feral-file/ff-indexer-v2/internal/domain"
	"github.com/feral-file/ff-indexer-v2/internal/metadata"
	"github.com/feral-file/ff-indexer-v2/internal/mocks"
	"github.com/feral-file/ff-indexer-v2/internal/registry"
	"github.com/feral-file/ff-indexer-v2/internal/store/schema"
)

// ringersCID is a token from the 2026-10-02 incident: Art Blocks rate limiting failed
// its tokenURI fetch and the OpenSea fallback moved it into OpenSea's release.
var ringersCID = domain.NewTokenCID(domain.ChainEthereumMainnet, domain.StandardERC721,
	"0xa7d8d9ef8D8Ce8992Df33D8b8CF4Aebabd5bD270", "13000129")

// expectNoEnrichmentWrites fails the test if enrichment, release, or membership rows
// would be written.
func expectNoEnrichmentWrites(m *testExecutorMocks) {
	m.store.EXPECT().UpsertEnrichmentSource(gomock.Any(), gomock.Any()).Times(0)
	m.store.EXPECT().UpsertRelease(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Times(0)
	m.store.EXPECT().UpsertReleaseMember(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Times(0)
}

// Regression: with no fetched metadata, the publisher comes from the contract, so a
// known Art Blocks token is never enriched through OpenSea and keeps its release.
func TestEnhanceTokenMetadata_FetchFailedKnownPublisher_NoGenericFallback(t *testing.T) {
	m := setupTestExecutor(t)
	ctx := context.Background()

	publisherName := registry.PublisherNameArtBlocks
	m.metadataResolver.EXPECT().ResolvePublisher(ctx, ringersCID).
		Return(&metadata.Publisher{Name: &publisherName}, nil)

	// Real enhancer over client mocks with no expectations: an OpenSea or Art Blocks
	// call fails the test.
	actual := metadata.NewEnhancer(
		m.httpClient, mocks.NewMockURIResolver(m.ctrl),
		mocks.NewMockArtBlocksClient(m.ctrl), mocks.NewMockFeralFileClient(m.ctrl),
		mocks.NewMockFxhashClient(m.ctrl), mocks.NewMockObjktClient(m.ctrl), mocks.NewMockOpenSeaClient(m.ctrl),
		m.json, mocks.NewMockJCS(m.ctrl),
	)
	m.metadataEnhancer.EXPECT().Enhance(ctx, ringersCID, (*metadata.NormalizedMetadata)(nil), gomock.Any()).
		DoAndReturn(actual.Enhance)
	m.store.EXPECT().GetTokenByTokenCID(ctx, ringersCID.String()).
		Return(&schema.Token{ID: 1, TokenCID: ringersCID.String()}, nil)
	expectNoEnrichmentWrites(m)

	result, err := m.executor.EnhanceTokenMetadata(ctx, ringersCID, nil)

	require.ErrorIs(t, err, metadata.ErrArtBlocksTokenMetadataMissing)
	assert.Nil(t, result)
}

// A failed publisher lookup must skip enrichment instead of treating the token as
// publisher-less and routing it to the generic fallback.
func TestEnhanceTokenMetadata_PublisherLookupFails_SkipsEnrichment(t *testing.T) {
	m := setupTestExecutor(t)
	ctx := context.Background()

	m.store.EXPECT().GetTokenByTokenCID(ctx, ringersCID.String()).
		Return(&schema.Token{ID: 1, TokenCID: ringersCID.String()}, nil)
	m.metadataResolver.EXPECT().ResolvePublisher(ctx, ringersCID).Return(nil, assert.AnError)
	m.metadataEnhancer.EXPECT().Enhance(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Times(0)
	expectNoEnrichmentWrites(m)

	result, err := m.executor.EnhanceTokenMetadata(ctx, ringersCID, nil)

	require.ErrorIs(t, err, assert.AnError)
	assert.Nil(t, result)
}

// Fetched metadata whose publisher lookup failed during normalization carries the
// failure forward; enrichment is skipped without a second deployer lookup.
func TestEnhanceTokenMetadata_UnresolvedPublisherOnMetadata_SkipsEnrichment(t *testing.T) {
	m := setupTestExecutor(t)
	ctx := context.Background()

	m.store.EXPECT().GetTokenByTokenCID(ctx, ringersCID.String()).
		Return(&schema.Token{ID: 1, TokenCID: ringersCID.String()}, nil)
	m.metadataResolver.EXPECT().ResolvePublisher(gomock.Any(), gomock.Any()).Times(0)
	m.metadataEnhancer.EXPECT().Enhance(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Times(0)
	expectNoEnrichmentWrites(m)

	result, err := m.executor.EnhanceTokenMetadata(ctx, ringersCID, &metadata.NormalizedMetadata{PublisherUnresolved: true})

	require.Error(t, err)
	assert.Nil(t, result)
}

// Fetched metadata routes on the publisher resolved during normalization; the
// contract lookup is not repeated on the happy path.
func TestEnhanceTokenMetadata_FetchedMetadata_UsesItsPublisher(t *testing.T) {
	m := setupTestExecutor(t)
	ctx := context.Background()

	publisherName := registry.PublisherNameArtBlocks
	meta := &metadata.NormalizedMetadata{Publisher: &metadata.Publisher{Name: &publisherName}}
	m.store.EXPECT().GetTokenByTokenCID(ctx, ringersCID.String()).
		Return(&schema.Token{ID: 1, TokenCID: ringersCID.String()}, nil)
	m.metadataResolver.EXPECT().ResolvePublisher(gomock.Any(), gomock.Any()).Times(0)
	m.metadataEnhancer.EXPECT().Enhance(ctx, ringersCID, meta, meta.Publisher).Return(nil, nil)

	result, err := m.executor.EnhanceTokenMetadata(ctx, ringersCID, meta)

	require.NoError(t, err)
	assert.Nil(t, result)
}
