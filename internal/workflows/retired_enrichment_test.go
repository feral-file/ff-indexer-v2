package workflows_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/feral-file/ff-indexer-v2/internal/api/shared/dto"
	"github.com/feral-file/ff-indexer-v2/internal/domain"
	"github.com/feral-file/ff-indexer-v2/internal/metadata"
	"github.com/feral-file/ff-indexer-v2/internal/mocks"
	"github.com/feral-file/ff-indexer-v2/internal/providers/vendors/objkt"
	"github.com/feral-file/ff-indexer-v2/internal/store/schema"
	"github.com/feral-file/ff-indexer-v2/internal/types"
)

// TestEnhanceTokenMetadata_RetiredGatewayPreservesDisplay crosses the real vendor
// enhancer and executor boundary. A failed replacement must not reach persistence
// or displace an already-migrated enrichment URL that wins display precedence.
func TestEnhanceTokenMetadata_RetiredGatewayPreservesDisplay(t *testing.T) {
	m := setupTestExecutor(t)
	ctx := context.Background()
	cid := domain.NewTokenCID(domain.ChainTezosMainnet, domain.StandardFA2, "KT1LjmAdYQCLBjwv4S2oFkEzyHVkomAf5MrW", "15422")
	retired := "https://ipfs.io/ipfs/QmVJn8AG9x22BrbUaUj2CAQFtKMozSHyLvgV2X6X8dmtPw"
	playable := "https://ipfs.feralfile.com/ipfs/QmVJn8AG9x22BrbUaUj2CAQFtKMozSHyLvgV2X6X8dmtPw"
	base := &dto.TokenMetadataResponse{AnimationURL: &retired}
	existing := &dto.EnrichmentSourceResponse{AnimationURL: &playable}
	require.Equal(t, playable, *dto.MergeTokenDisplay(base, existing).AnimationURL)

	vendor := mocks.NewMockObjktClient(m.ctrl)
	resolver := mocks.NewMockURIResolver(m.ctrl)
	response := &objkt.Token{ArtifactURI: &retired, Mime: types.StringPtr("image/gif")}
	vendor.EXPECT().GetToken(ctx, gomock.Any(), "15422").Return(response, nil)
	m.json.EXPECT().Marshal(response).Return([]byte(`{}`), nil)
	resolver.EXPECT().Resolve(ctx, retired).Return("", assert.AnError)
	actual := metadata.NewEnhancer(
		m.httpClient, resolver,
		mocks.NewMockArtBlocksClient(m.ctrl), mocks.NewMockFeralFileClient(m.ctrl),
		mocks.NewMockFxhashClient(m.ctrl), vendor, mocks.NewMockOpenSeaClient(m.ctrl),
		m.json, mocks.NewMockJCS(m.ctrl),
	)
	m.metadataEnhancer.EXPECT().Enhance(ctx, cid, (*metadata.NormalizedMetadata)(nil)).
		DoAndReturn(actual.Enhance)
	m.store.EXPECT().GetTokenByTokenCID(ctx, cid.String()).Return(&schema.Token{ID: 1, TokenCID: cid.String()}, nil)
	// The existing enrichment is preserved because the executor must abort
	// before the persistence boundary, rather than writing nil or retired media.
	m.store.EXPECT().UpsertEnrichmentSource(gomock.Any(), gomock.Any()).Times(0)
	result, err := m.executor.EnhanceTokenMetadata(ctx, cid, nil)
	assert.ErrorIs(t, err, assert.AnError)
	assert.Nil(t, result)
	assert.Equal(t, playable, *dto.MergeTokenDisplay(base, existing).AnimationURL)
}
