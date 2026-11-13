package metadata_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/feral-file/ff-indexer-v2/internal/domain"
	"github.com/feral-file/ff-indexer-v2/internal/mocks"
	"github.com/feral-file/ff-indexer-v2/internal/providers/vendors/opensea"
	"github.com/feral-file/ff-indexer-v2/internal/registry"
	"github.com/feral-file/ff-indexer-v2/internal/types"
	"github.com/feral-file/ff-indexer-v2/internal/uri"
)

const (
	onchfsTestRoot      = "b12f18fcd51a21cc3ace5090fa808c8d4e4552d9e2239a3886b89f8ea12ff685"
	onchfsTestIteration = "/?fxhash=0xb56f&fxiteration=44&fxminter=0x94E3&fxchain=ETHEREUM"
	onchfsTestURI       = "onchfs://" + onchfsTestRoot + onchfsTestIteration
	onchfsTestGateway   = "https://onchfs.fxhash2.xyz/" + onchfsTestRoot + onchfsTestIteration
)

// TestResolver_OnChFSMediaIsCanonical verifies that both metadata standards store an
// onchfs work as its onchfs:// URI: TZIP-21's artifactUri as given, and an ERC-721
// animation_url that carries fxhash's dead gateway rewritten back to the URI. No HTTP
// request is made for MIME detection (the HTTP mock has no expectations).
func TestResolver_OnChFSMediaIsCanonical(t *testing.T) {
	tests := []struct {
		name  string
		field string
		value string
	}{
		{"TZIP-21 artifactUri", "artifactUri", onchfsTestURI},
		{"ERC-721 animation_url on the dead gateway", "animation_url", onchfsTestGateway},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := setupTestResolver(t)
			data := map[string]interface{}{tt.field: tt.value}
			chain, standard, contract := domain.ChainTezosMainnet, domain.StandardFA2, "KT1EfsNuqwLAWDd3o4pvfUx1CAh5GMdTrRvr"
			if tt.field == "animation_url" {
				chain, standard, contract = domain.ChainEthereumMainnet, domain.StandardERC721, "0x19DBc1c820dd3F13260829a4E06Dda6d9EF758DB"
				const metadataURL = "https://example.com/metadata.json"
				m.ethClient.EXPECT().IsVendorOnlyMetadata(contract).Return(false)
				m.ethClient.EXPECT().TokenURI(gomock.Any(), contract, "44", standard).Return(metadataURL, nil)
				m.uriResolver.EXPECT().Resolve(gomock.Any(), metadataURL).Return(metadataURL, nil)
				m.httpClient.EXPECT().GetAndUnmarshal(gomock.Any(), metadataURL, gomock.Any()).
					DoAndReturn(func(_ context.Context, _ string, output interface{}) error {
						*output.(*map[string]interface{}) = data
						return nil
					})
			} else {
				m.tzClient.EXPECT().GetTokenMetadata(gomock.Any(), contract, "44").Return(data, nil)
			}
			m.registry.EXPECT().LookupPublisherByCollection(chain, contract).
				Return(&registry.PublisherInfo{Name: registry.PublisherNameFXHash})
			actual := uri.NewResolver(m.httpClient, mocks.NewMockIO(m.ctrl), &uri.Config{})
			m.uriResolver.EXPECT().Resolve(gomock.Any(), tt.value).DoAndReturn(actual.Resolve).AnyTimes()
			m.uriResolver.EXPECT().Resolve(gomock.Any(), onchfsTestURI).DoAndReturn(actual.Resolve).AnyTimes()

			result, err := m.resolver.Resolve(context.Background(), domain.NewTokenCID(chain, standard, contract, "44"))

			require.NoError(t, err)
			require.NotNil(t, result)
			assert.Equal(t, onchfsTestURI, result.Animation)
			assert.Nil(t, result.MimeType, "onchfs has no gateway to detect a type from")
		})
	}
}

// TestEnhancer_OpenSeaOnChFSGatewayIsCanonical verifies that vendor media bypassing the
// URI resolver (OpenSea's display_animation_url) is still rewritten from fxhash's dead
// gateway to the onchfs:// URI before it reaches the enrichment upsert.
func TestEnhancer_OpenSeaOnChFSGatewayIsCanonical(t *testing.T) {
	m := setupTestEnhancer(t)
	ctx := context.Background()
	contract := "0x19DBc1c820dd3F13260829a4E06Dda6d9EF758DB"
	cid := domain.NewTokenCID(domain.ChainEthereumMainnet, domain.StandardERC721, contract, "44")
	nft := &opensea.NFTMetadata{Name: types.StringPtr("Untitled #44"), DisplayAnimationURL: types.StringPtr(onchfsTestGateway)}
	m.openseaClient.EXPECT().GetNFT(ctx, contract, "44").Return(nft, nil)
	m.json.EXPECT().Marshal(nft).Return([]byte(`{}`), nil)
	actual := uri.NewResolver(m.httpClient, mocks.NewMockIO(m.ctrl), &uri.Config{})
	m.uriResolver.EXPECT().Resolve(gomock.Any(), onchfsTestURI).DoAndReturn(actual.Resolve).AnyTimes()

	result, err := m.enhancer.Enhance(ctx, cid, nil, nil)

	require.NoError(t, err)
	require.NotNil(t, result)
	require.NotNil(t, result.AnimationURL)
	assert.Equal(t, onchfsTestURI, *result.AnimationURL)
	assert.Nil(t, result.MimeType)
}
