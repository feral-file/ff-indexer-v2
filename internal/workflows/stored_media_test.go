package workflows_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/feral-file/ff-indexer-v2/internal/domain"
	"github.com/feral-file/ff-indexer-v2/internal/metadata"
	"github.com/feral-file/ff-indexer-v2/internal/store"
	"github.com/feral-file/ff-indexer-v2/internal/store/schema"
	"github.com/feral-file/ff-indexer-v2/internal/types"
)

// Fixtures for the stored gateway tests: the same CIDs behind the gateway a token
// already stores and behind the gateway a later race selected.
const (
	storedMediaImageCID     = "bafkreih52fwpk2d66qt6t5rsnwfjkauonv73hgddbffyeuvocgliiqhe2m"
	storedMediaAnimationCID = "bafybeihsfdyjhwuue3unor2n5yywnw74sp2lk5d7xlireo2ejxldx4jsf4"
	storedMediaChangedCID   = "bafybeiabfhzmgp5n7wgud5dcvk7hizj42bht2tgx5437o2jvc2fv3uizwq"
	storedMediaGateway      = "https://stored.example.com/ipfs/"
	racedMediaGateway       = "https://raced.example.com/ipfs/"
)

// TestResolveTokenMetadata_KeepsStoredGateway verifies the re-index contract for
// on-chain metadata: the gateway race may pick any host, but content the token
// already stores keeps its URL, while content that changed takes the new one.
func TestResolveTokenMetadata_KeepsStoredGateway(t *testing.T) {
	mocks := setupTestExecutor(t)
	defer tearDownTestExecutor(mocks)

	ctx := context.Background()
	tokenCID := domain.NewTokenCID(domain.ChainTezosMainnet, domain.StandardFA2, "KT1LT5jenuFUT2a76XKKojdtVq6TLFRG9xgN", "2")
	storedImage := storedMediaGateway + storedMediaImageCID
	storedAnimation := storedMediaGateway + storedMediaAnimationCID
	changedAnimation := racedMediaGateway + storedMediaChangedCID

	resolved := &metadata.NormalizedMetadata{
		Image:     racedMediaGateway + storedMediaImageCID,
		Animation: changedAnimation,
		Raw:       map[string]interface{}{"name": "fixture"},
	}

	mocks.metadataResolver.EXPECT().Resolve(ctx, tokenCID).Return(resolved, nil)
	mocks.store.EXPECT().
		GetTokenWithMetadataByTokenCID(ctx, tokenCID.String()).
		Return(&store.TokensWithMetadataResult{
			Token: &schema.Token{ID: 7, TokenCID: tokenCID.String()},
			Metadata: &schema.TokenMetadata{
				TokenID:      7,
				ImageURL:     &storedImage,
				AnimationURL: &storedAnimation,
			},
		}, nil)
	mocks.metadataResolver.EXPECT().RawHash(resolved).Return([]byte("hash"), []byte(`{"name":"fixture"}`), nil)
	mocks.clock.EXPECT().Now().Return(time.Now())
	mocks.store.EXPECT().
		UpsertTokenMetadata(ctx, gomock.Any()).
		DoAndReturn(func(_ context.Context, input store.CreateTokenMetadataInput) error {
			require.NotNil(t, input.ImageURL)
			assert.Equal(t, storedImage, *input.ImageURL, "unchanged content must keep its stored URL")
			require.NotNil(t, input.AnimationURL)
			assert.Equal(t, changedAnimation, *input.AnimationURL, "changed content must take the resolved URL")
			return nil
		})

	result, err := mocks.executor.ResolveTokenMetadata(ctx, tokenCID)

	require.NoError(t, err)
	// The workflow health-checks and media-indexes the returned URLs, so they must
	// be the stored ones too.
	assert.Equal(t, storedImage, result.Image)
	assert.Equal(t, changedAnimation, result.Animation)
}

// TestEnhanceTokenMetadata_KeepsStoredGateway verifies the same contract for vendor
// enrichment, whose URLs win display precedence over on-chain metadata.
func TestEnhanceTokenMetadata_KeepsStoredGateway(t *testing.T) {
	mocks := setupTestExecutor(t)
	defer tearDownTestExecutor(mocks)

	ctx := context.Background()
	tokenCID := domain.NewTokenCID(domain.ChainTezosMainnet, domain.StandardFA2, "KT1LT5jenuFUT2a76XKKojdtVq6TLFRG9xgN", "2")
	token := &schema.Token{ID: 7, TokenCID: tokenCID.String()}
	storedImage := storedMediaGateway + storedMediaImageCID
	storedAnimation := storedMediaGateway + storedMediaAnimationCID
	changedAnimation := racedMediaGateway + storedMediaChangedCID

	enhanced := &metadata.EnhancedMetadata{
		Vendor:       schema.VendorObjkt,
		VendorJSON:   []byte(`{"name":"fixture"}`),
		ImageURL:     types.StringPtr(racedMediaGateway + storedMediaImageCID),
		AnimationURL: types.StringPtr(changedAnimation),
	}

	mocks.store.EXPECT().GetTokenByTokenCID(ctx, tokenCID.String()).Return(token, nil)
	mocks.metadataEnhancer.EXPECT().Enhance(ctx, tokenCID, gomock.Any(), gomock.Any()).Return(enhanced, nil)
	mocks.store.EXPECT().
		GetEnrichmentSourceByTokenID(ctx, token.ID).
		Return(&schema.EnrichmentSource{
			TokenID:      token.ID,
			ImageURL:     &storedImage,
			AnimationURL: &storedAnimation,
		}, nil)
	mocks.metadataEnhancer.EXPECT().VendorJsonHash(enhanced).Return([]byte("hash"), nil)
	mocks.store.EXPECT().
		UpsertEnrichmentSource(ctx, gomock.Any()).
		DoAndReturn(func(_ context.Context, input store.CreateEnrichmentSourceInput) error {
			require.NotNil(t, input.ImageURL)
			assert.Equal(t, storedImage, *input.ImageURL, "unchanged content must keep its stored URL")
			require.NotNil(t, input.AnimationURL)
			assert.Equal(t, changedAnimation, *input.AnimationURL, "changed content must take the resolved URL")
			return nil
		})

	result, err := mocks.executor.EnhanceTokenMetadata(ctx, tokenCID, &metadata.NormalizedMetadata{})

	require.NoError(t, err)
	require.NotNil(t, result.ImageURL)
	assert.Equal(t, storedImage, *result.ImageURL)
	require.NotNil(t, result.AnimationURL)
	assert.Equal(t, changedAnimation, *result.AnimationURL)
}

// TestEnhanceTokenMetadata_FirstEnrichmentTakesResolvedGateway verifies that a token
// without a stored enrichment source stores the resolved URLs.
func TestEnhanceTokenMetadata_FirstEnrichmentTakesResolvedGateway(t *testing.T) {
	mocks := setupTestExecutor(t)
	defer tearDownTestExecutor(mocks)

	ctx := context.Background()
	tokenCID := domain.NewTokenCID(domain.ChainTezosMainnet, domain.StandardFA2, "KT1LT5jenuFUT2a76XKKojdtVq6TLFRG9xgN", "2")
	token := &schema.Token{ID: 7, TokenCID: tokenCID.String()}
	resolvedImage := racedMediaGateway + storedMediaImageCID

	enhanced := &metadata.EnhancedMetadata{
		Vendor:     schema.VendorObjkt,
		VendorJSON: []byte(`{"name":"fixture"}`),
		ImageURL:   types.StringPtr(resolvedImage),
	}

	mocks.store.EXPECT().GetTokenByTokenCID(ctx, tokenCID.String()).Return(token, nil)
	mocks.metadataEnhancer.EXPECT().Enhance(ctx, tokenCID, gomock.Any(), gomock.Any()).Return(enhanced, nil)
	mocks.store.EXPECT().GetEnrichmentSourceByTokenID(ctx, token.ID).Return(nil, nil)
	mocks.metadataEnhancer.EXPECT().VendorJsonHash(enhanced).Return([]byte("hash"), nil)
	mocks.store.EXPECT().
		UpsertEnrichmentSource(ctx, gomock.Any()).
		DoAndReturn(func(_ context.Context, input store.CreateEnrichmentSourceInput) error {
			require.NotNil(t, input.ImageURL)
			assert.Equal(t, resolvedImage, *input.ImageURL)
			assert.Nil(t, input.AnimationURL)
			return nil
		})

	_, err := mocks.executor.EnhanceTokenMetadata(ctx, tokenCID, &metadata.NormalizedMetadata{})

	require.NoError(t, err)
}

// TestEnhanceTokenMetadata_StoredSourceReadFailureStopsUpsert verifies that a failed
// read of the stored enrichment source is reported instead of storing the race
// winner, which would detach the token's media assets without any signal.
func TestEnhanceTokenMetadata_StoredSourceReadFailureStopsUpsert(t *testing.T) {
	mocks := setupTestExecutor(t)
	defer tearDownTestExecutor(mocks)

	ctx := context.Background()
	tokenCID := domain.NewTokenCID(domain.ChainTezosMainnet, domain.StandardFA2, "KT1LT5jenuFUT2a76XKKojdtVq6TLFRG9xgN", "2")
	token := &schema.Token{ID: 7, TokenCID: tokenCID.String()}
	readErr := errors.New("connection reset")

	mocks.store.EXPECT().GetTokenByTokenCID(ctx, tokenCID.String()).Return(token, nil)
	mocks.metadataEnhancer.EXPECT().Enhance(ctx, tokenCID, gomock.Any(), gomock.Any()).Return(&metadata.EnhancedMetadata{
		Vendor:   schema.VendorObjkt,
		ImageURL: types.StringPtr(racedMediaGateway + storedMediaImageCID),
	}, nil)
	mocks.store.EXPECT().GetEnrichmentSourceByTokenID(ctx, token.ID).Return(nil, readErr)

	result, err := mocks.executor.EnhanceTokenMetadata(ctx, tokenCID, &metadata.NormalizedMetadata{})

	require.ErrorIs(t, err, readErr)
	assert.Nil(t, result)
}
