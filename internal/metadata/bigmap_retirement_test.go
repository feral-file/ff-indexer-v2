package metadata_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/feral-file/ff-indexer-v2/internal/domain"
	"github.com/feral-file/ff-indexer-v2/internal/registry"
	"github.com/feral-file/ff-indexer-v2/internal/uri"
)

// A retired big-map URI failure must escape the optional refresh, while
// unrelated lookup/resolution/fetch failures retain the cached fallback.
func TestResolver_FA2BigMapRefreshFailurePolicy(t *testing.T) {
	const retired = "https://ipfs.io/ipfs/QmVJn8AG9x22BrbUaUj2CAQFtKMozSHyLvgV2X6X8dmtPw"
	const ordinary = "https://metadata.example.org/signed.json"
	for _, tc := range []struct {
		name        string
		metadataURI string
		lookupErr   error
		resolveErr  error
		fetchErr    error
		wantRetired bool
	}{
		{"retired replacement", retired, nil, fmt.Errorf("replacement: %w: %w", uri.ErrRetiredGatewayReplacement, assert.AnError), nil, true},
		{"ordinary resolution failure", ordinary, nil, assert.AnError, nil, false},
		{"ordinary fetch failure", ordinary, nil, nil, assert.AnError, false},
		{"big map unavailable", "", assert.AnError, nil, nil, false},
		{"no big map URI", "", nil, nil, nil, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			m := setupTestResolver(t)
			ctx := context.Background()
			cid := domain.NewTokenCID(domain.ChainTezosMainnet, domain.StandardFA2, "KT1ABC", "59")
			placeholder := map[string]interface{}{"name": "[WAITING TO BE SIGNED]"}
			m.tzClient.EXPECT().GetTokenMetadata(ctx, "KT1ABC", "59").Return(placeholder, nil)
			m.tzClient.EXPECT().GetTokenMetadataURI(ctx, "KT1ABC", "59").Return(tc.metadataURI, tc.lookupErr)
			if tc.lookupErr == nil && tc.metadataURI != "" {
				m.uriResolver.EXPECT().Resolve(ctx, tc.metadataURI).Return(tc.metadataURI, tc.resolveErr)
				if tc.resolveErr == nil {
					m.httpClient.EXPECT().GetAndUnmarshal(ctx, tc.metadataURI, gomock.Any()).Return(tc.fetchErr)
				}
			}
			// Allow the faulty cached normalization to finish before asserting
			// the returned result, rather than fail at a missing mock call.
			m.registry.EXPECT().LookupPublisherByCollection(domain.ChainTezosMainnet, "KT1ABC").
				Return(&registry.PublisherInfo{Name: registry.PublisherNameFXHash}).AnyTimes()

			result, err := m.resolver.Resolve(ctx, cid)
			if tc.wantRetired {
				require.ErrorIs(t, err, uri.ErrRetiredGatewayReplacement)
				require.ErrorIs(t, err, assert.AnError)
				require.Nil(t, result)
				return
			}
			require.NoError(t, err)
			require.NotNil(t, result)
			require.Equal(t, placeholder["name"], result.Name)
			require.Equal(t, placeholder, result.Raw)
		})
	}
}
