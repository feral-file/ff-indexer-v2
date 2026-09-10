package metadata_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"go.uber.org/mock/gomock"

	"github.com/feral-file/ff-indexer-v2/internal/domain"
	"github.com/feral-file/ff-indexer-v2/internal/registry"
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
			assert.Nil(t, result, "failed normalization must leave existing persisted metadata untouched")
			assert.Equal(t, source, data[field], "the original vendor metadata remains intact")
		})
	}
}
