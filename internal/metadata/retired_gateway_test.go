package metadata_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"

	"go.uber.org/mock/gomock"

	"github.com/feral-file/ff-indexer-v2/internal/domain"
	"github.com/feral-file/ff-indexer-v2/internal/mocks"
	"github.com/feral-file/ff-indexer-v2/internal/providers/vendors/objkt"
	"github.com/feral-file/ff-indexer-v2/internal/providers/vendors/opensea"
	"github.com/feral-file/ff-indexer-v2/internal/registry"
	"github.com/feral-file/ff-indexer-v2/internal/types"
	"github.com/feral-file/ff-indexer-v2/internal/uri"
)

// TestResolver_RetiredGatewayFailure verifies that a rebuild cannot hand an
// unvalidated retired URL to the metadata upsert, in either metadata standard.
func TestResolver_RetiredGatewayFailure(t *testing.T) {
	for _, field := range []string{"displayUri", "artifactUri", "image", "animation_url"} {
		t.Run(field, func(t *testing.T) {
			m := setupTestResolver(t)
			const source = "https://ipfs.io/ipfs/QmVJn8AG9x22BrbUaUj2CAQFtKMozSHyLvgV2X6X8dmtPw"
			data := map[string]interface{}{field: source}
			chain := domain.ChainTezosMainnet
			standard := domain.StandardFA2
			contract := "KT1LjmAdYQCLBjwv4S2oFkEzyHVkomAf5MrW"
			if field == "image" || field == "animation_url" {
				chain = domain.ChainEthereumMainnet
				standard = domain.StandardERC721
				contract = "0x1234567890123456789012345678901234567890"
				const metadataURL = "https://example.com/metadata.json"
				m.ethClient.EXPECT().IsVendorOnlyMetadata(contract).Return(false)
				m.ethClient.EXPECT().TokenURI(gomock.Any(), contract, "15422", standard).Return(metadataURL, nil)
				m.uriResolver.EXPECT().Resolve(gomock.Any(), metadataURL).Return(metadataURL, nil)
				m.httpClient.EXPECT().GetAndUnmarshal(gomock.Any(), metadataURL, gomock.Any()).
					DoAndReturn(func(_ context.Context, _ string, output interface{}) error {
						*output.(*map[string]interface{}) = data
						return nil
					})
			} else {
				m.tzClient.EXPECT().GetTokenMetadata(gomock.Any(), contract, "15422").Return(data, nil)
			}
			m.registry.EXPECT().LookupPublisherByCollection(chain, contract).
				Return(&registry.PublisherInfo{Name: registry.PublisherNameArtBlocks})
			m.uriResolver.EXPECT().Resolve(gomock.Any(), source).Return("", assert.AnError).AnyTimes()

			result, err := m.resolver.Resolve(context.Background(), domain.NewTokenCID(chain, standard, contract, "15422"))

			assert.ErrorIs(t, err, assert.AnError)
			assert.ErrorIs(t, err, uri.ErrRetiredGatewayReplacement)
			assert.Nil(t, result, "failed normalization must leave existing persisted metadata untouched")
			assert.Equal(t, source, data[field], "the original vendor metadata remains intact")
		})
	}
}

// Unsupported browser-gateway paths must also fail the actual URI resolver
// inside vendor enrichment, before they can be handed to an upsert.
func TestEnhancer_UnsupportedRetiredGateway(t *testing.T) {
	m := setupTestEnhancer(t)
	ctx := context.Background()
	source := "https://ipfs.io/ipns/example.org/art.gif"
	cid := domain.NewTokenCID(domain.ChainTezosMainnet, domain.StandardFA2, "KT1LjmAdYQCLBjwv4S2oFkEzyHVkomAf5MrW", "15422")
	response := &objkt.Token{DisplayURI: &source, ArtifactURI: &source, Mime: types.StringPtr("image/gif")}
	m.objktClient.EXPECT().GetToken(ctx, gomock.Any(), "15422").Return(response, nil)
	m.json.EXPECT().Marshal(response).Return([]byte(`{}`), nil)
	actual := uri.NewResolver(m.httpClient, mocks.NewMockIO(m.ctrl), &uri.Config{})
	m.uriResolver.EXPECT().Resolve(ctx, source).DoAndReturn(actual.Resolve).AnyTimes()

	result, err := m.enhancer.Enhance(ctx, cid, nil)
	assert.ErrorIs(t, err, uri.ErrRetiredGatewayReplacement)
	assert.Nil(t, result)
}

// TestEnhancer_RetiredGatewayFailure verifies that enrichment after a failed
// normalization cannot emit an upsert that overwrites migrated enrichment.
func TestEnhancer_RetiredGatewayFailure(t *testing.T) {
	for _, vendor := range []string{"objkt", "opensea"} {
		t.Run(vendor, func(t *testing.T) {
			m := setupTestEnhancer(t)
			source := "https://ipfs.io/ipfs/QmVJn8AG9x22BrbUaUj2CAQFtKMozSHyLvgV2X6X8dmtPw"
			name := "SUNRISE"
			cid := domain.NewTokenCID(domain.ChainTezosMainnet, domain.StandardFA2, "KT1LjmAdYQCLBjwv4S2oFkEzyHVkomAf5MrW", "15422")
			if vendor == "objkt" {
				token := &objkt.Token{Name: &name, DisplayURI: &source, ArtifactURI: &source}
				m.objktClient.EXPECT().GetToken(gomock.Any(), gomock.Any(), "15422").Return(token, nil)
				m.json.EXPECT().Marshal(token).Return([]byte(`{"name":"SUNRISE"}`), nil)
			} else {
				cid = domain.NewTokenCID(domain.ChainEthereumMainnet, domain.StandardERC721, "0x1234567890123456789012345678901234567890", "15422")
				token := &opensea.NFTMetadata{Name: &name, ImageURL: &source, DisplayAnimationURL: &source}
				m.openseaClient.EXPECT().GetNFT(gomock.Any(), gomock.Any(), "15422").Return(token, nil)
				m.json.EXPECT().Marshal(token).Return([]byte(`{"name":"SUNRISE"}`), nil)
			}
			m.uriResolver.EXPECT().Resolve(gomock.Any(), gomock.Any()).Return("", assert.AnError).AnyTimes()
			result, err := m.enhancer.Enhance(context.Background(), cid, nil)
			assert.ErrorIs(t, err, assert.AnError)
			assert.ErrorIs(t, err, uri.ErrRetiredGatewayReplacement)
			assert.Nil(t, result, "keep the entire existing enrichment record on resolution failure")
		})
	}
}
