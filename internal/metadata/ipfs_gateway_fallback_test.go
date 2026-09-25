package metadata_test

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/feral-file/ff-indexer-v2/internal/domain"
	"github.com/feral-file/ff-indexer-v2/internal/registry"
	"github.com/feral-file/ff-indexer-v2/internal/security/ssrf"
)

// Already EIP-55 checksummed, the form NewTokenCID stores and the resolver passes on.
const fallbackContract = "0x0000000000000000000000000000000000000aBc"

// expectERC721MetadataURL wires the metadataURL lookup and the origin resolve (a plain HTTP
// gateway URL resolves to itself) for a single ERC-721 token.
func expectERC721MetadataURL(m *testResolverMocks, metadataURL string) domain.TokenCID {
	tokenCID := domain.NewTokenCID(domain.ChainEthereumMainnet, domain.StandardERC721, fallbackContract, "7")
	m.ethClient.EXPECT().IsVendorOnlyMetadata(fallbackContract).Return(false)
	m.ethClient.EXPECT().TokenURI(gomock.Any(), fallbackContract, "7", domain.StandardERC721).Return(metadataURL, nil)
	m.uriResolver.EXPECT().Resolve(gomock.Any(), metadataURL).Return(metadataURL, nil)
	return tokenCID
}

// expectMinimalNormalization covers normalization of a document with no media, so the
// tests only exercise the fetch path.
func expectMinimalNormalization(m *testResolverMocks) {
	m.registry.EXPECT().LookupPublisherByCollection(domain.ChainEthereumMainnet, fallbackContract).
		Return(&registry.PublisherInfo{Name: registry.PublisherNameArtBlocks, URL: "https://artblocks.io"})
}

func returnMetadata(md map[string]interface{}) func(context.Context, string, interface{}) error {
	return func(_ context.Context, _ string, result interface{}) error {
		*result.(*map[string]interface{}) = md
		return nil
	}
}

// A metadataURL pinned to a dead dedicated gateway is fetched by CID from the pool.
func TestResolver_MetadataFetch_FallsBackToIPFSGatewayPool(t *testing.T) {
	tests := []struct {
		name        string
		metadataURL string
		ref         string
	}{
		{
			name:        "path gateway (retired Infura)",
			metadataURL: "https://0prod.infura-ipfs.io/ipfs/QmQ81XEWR3LaRRhgMKjSNmV114qaA9nVAZJpwuWAvA4TjE/7",
			ref:         "QmQ81XEWR3LaRRhgMKjSNmV114qaA9nVAZJpwuWAvA4TjE/7",
		},
		{
			name:        "owner-only dedicated gateway (Pinata 403)",
			metadataURL: "https://peacevoid.mypinata.cloud/ipfs/QmUW3EVrNsXQ3jarBDU9CLTjjHvQYPS45VQgrTiFXWDnq9/1",
			ref:         "QmUW3EVrNsXQ3jarBDU9CLTjjHvQYPS45VQgrTiFXWDnq9/1",
		},
		{
			name:        "CID subdomain gateway",
			metadataURL: "https://bafybeig4rpntvpzws7xqgs5hax7iyztrwbrz3mzuj7urrvtelxgus4ptge.ipfs.nftstorage.link/7.json",
			ref:         "bafybeig4rpntvpzws7xqgs5hax7iyztrwbrz3mzuj7urrvtelxgus4ptge/7.json",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := setupTestResolver(t)
			defer tearDownTestResolver(m)

			tokenCID := expectERC721MetadataURL(m, tt.metadataURL)
			poolURL := "https://ipfs.filebase.io/ipfs/" + tt.ref
			gomock.InOrder(
				m.httpClient.EXPECT().GetAndUnmarshal(gomock.Any(), tt.metadataURL, gomock.Any()).
					Return(errors.New("unexpected status code 403")),
				m.uriResolver.EXPECT().Resolve(gomock.Any(), "ipfs://"+tt.ref).Return(poolURL, nil),
				m.httpClient.EXPECT().GetAndUnmarshal(gomock.Any(), poolURL, gomock.Any()).
					DoAndReturn(returnMetadata(map[string]interface{}{"name": "Recovered"})),
			)
			expectMinimalNormalization(m)

			result, err := m.resolver.Resolve(context.Background(), tokenCID)
			require.NoError(t, err)
			require.NotNil(t, result)
			assert.Equal(t, "Recovered", result.Name)
		})
	}
}

// A working origin is used as-is; the pool is never consulted.
func TestResolver_MetadataFetch_OriginSucceedsNoFallback(t *testing.T) {
	m := setupTestResolver(t)
	defer tearDownTestResolver(m)

	metadataURL := "https://gateway.example.com/ipfs/QmQ81XEWR3LaRRhgMKjSNmV114qaA9nVAZJpwuWAvA4TjE/7"
	tokenCID := expectERC721MetadataURL(m, metadataURL)
	m.httpClient.EXPECT().GetAndUnmarshal(gomock.Any(), metadataURL, gomock.Any()).
		DoAndReturn(returnMetadata(map[string]interface{}{"name": "Origin"}))
	expectMinimalNormalization(m)

	result, err := m.resolver.Resolve(context.Background(), tokenCID)
	require.NoError(t, err)
	assert.Equal(t, "Origin", result.Name)
}

// Failures that must keep today's behavior: no pool lookup, original error surfaced.
func TestResolver_MetadataFetch_NoFallback(t *testing.T) {
	cidURI := "https://0prod.infura-ipfs.io/ipfs/QmQ81XEWR3LaRRhgMKjSNmV114qaA9nVAZJpwuWAvA4TjE/7"
	tests := []struct {
		name        string
		metadataURL string
		fetchErr    error
		ctx         func() context.Context
	}{
		{
			name:        "not content-addressed",
			metadataURL: "https://www.crazycrows.club/api/5749",
			fetchErr:    errors.New("unexpected status code 404"),
			ctx:         context.Background,
		},
		{
			name:        "SSRF refusal stays a policy verdict",
			metadataURL: cidURI,
			fetchErr:    fmt.Errorf("Get %q: %w", cidURI, ssrf.ErrBlocked),
			ctx:         context.Background,
		},
		{
			name:        "caller context already ended",
			metadataURL: cidURI,
			fetchErr:    context.Canceled,
			ctx: func() context.Context {
				ctx, cancel := context.WithCancel(context.Background())
				cancel()
				return ctx
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := setupTestResolver(t)
			defer tearDownTestResolver(m)

			tokenCID := expectERC721MetadataURL(m, tt.metadataURL)
			m.httpClient.EXPECT().GetAndUnmarshal(gomock.Any(), tt.metadataURL, gomock.Any()).Return(tt.fetchErr)
			// Any uriResolver.Resolve("ipfs://…") call is unexpected and fails the test.

			_, err := m.resolver.Resolve(tt.ctx(), tokenCID)
			require.Error(t, err)
			assert.ErrorIs(t, err, tt.fetchErr)
		})
	}
}

// When the pool cannot serve the CID either, the origin error stays first in the chain.
func TestResolver_MetadataFetch_FallbackFailureKeepsOriginError(t *testing.T) {
	tests := []struct {
		name  string
		setup func(m *testResolverMocks, ref, poolURL string)
	}{
		{
			name: "no gateway serves the CID",
			setup: func(m *testResolverMocks, ref, _ string) {
				m.uriResolver.EXPECT().Resolve(gomock.Any(), "ipfs://"+ref).
					Return("", errors.New("no working IPFS gateway found"))
			},
		},
		{
			name: "pool gateway validated but fetch fails",
			setup: func(m *testResolverMocks, ref, poolURL string) {
				m.uriResolver.EXPECT().Resolve(gomock.Any(), "ipfs://"+ref).Return(poolURL, nil)
				m.httpClient.EXPECT().GetAndUnmarshal(gomock.Any(), poolURL, gomock.Any()).
					Return(errors.New("invalid character '<' looking for beginning of value"))
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := setupTestResolver(t)
			defer tearDownTestResolver(m)

			ref := "QmQ81XEWR3LaRRhgMKjSNmV114qaA9nVAZJpwuWAvA4TjE/7"
			metadataURL := "https://0prod.infura-ipfs.io/ipfs/" + ref
			originErr := errors.New("dial tcp: i/o timeout")
			tokenCID := expectERC721MetadataURL(m, metadataURL)
			m.httpClient.EXPECT().GetAndUnmarshal(gomock.Any(), metadataURL, gomock.Any()).Return(originErr)
			tt.setup(m, ref, "https://ipfs.filebase.io/ipfs/"+ref)

			_, err := m.resolver.Resolve(context.Background(), tokenCID)
			require.Error(t, err)
			assert.ErrorIs(t, err, originErr)
			assert.Contains(t, err.Error(), "IPFS gateway fallback")
		})
	}
}

// The pool may pick the very URL that just failed; it is not fetched twice.
func TestResolver_MetadataFetch_PoolReturnsSameURLNoRefetch(t *testing.T) {
	m := setupTestResolver(t)
	defer tearDownTestResolver(m)

	ref := "QmQ81XEWR3LaRRhgMKjSNmV114qaA9nVAZJpwuWAvA4TjE/7"
	metadataURL := "https://ipfs.filebase.io/ipfs/" + ref
	originErr := errors.New("unexpected status code 502")
	tokenCID := expectERC721MetadataURL(m, metadataURL)
	m.httpClient.EXPECT().GetAndUnmarshal(gomock.Any(), metadataURL, gomock.Any()).Return(originErr).Times(1)
	m.uriResolver.EXPECT().Resolve(gomock.Any(), "ipfs://"+ref).Return(metadataURL, nil)

	_, err := m.resolver.Resolve(context.Background(), tokenCID)
	require.Error(t, err)
	assert.ErrorIs(t, err, originErr)
}
