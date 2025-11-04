package metadata_test

import (
	"context"
	"os"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"

	"github.com/feral-file/ff-indexer-v2/internal/domain"
	"github.com/feral-file/ff-indexer-v2/internal/logger"
	"github.com/feral-file/ff-indexer-v2/internal/metadata"
	"github.com/feral-file/ff-indexer-v2/internal/mocks"
	"github.com/feral-file/ff-indexer-v2/internal/registry"
)

func TestMain(m *testing.M) {
	// Initialize logger for tests
	err := logger.Initialize(logger.Config{
		Debug: false,
	})
	if err != nil {
		panic(err)
	}

	code := m.Run()
	os.Exit(code)
}

// testResolverMocks contains all the mocks needed for testing the resolver
type testResolverMocks struct {
	ctrl        *gomock.Controller
	ethClient   *mocks.MockEthereumProviderClient
	tzClient    *mocks.MockTzKTClient
	httpClient  *mocks.MockHTTPClient
	uriResolver *mocks.MockURIResolver
	json        *mocks.MockJSON
	clock       *mocks.MockClock
	jcs         *mocks.MockJCS
	base64      *mocks.MockBase64
	store       *mocks.MockStore
	registry    *mocks.MockPublisherRegistry
	resolver    metadata.Resolver
}

// setupTestResolver creates all the mocks and resolver for testing
func setupTestResolver(t *testing.T) *testResolverMocks {
	ctrl := gomock.NewController(t)

	tm := &testResolverMocks{
		ctrl:        ctrl,
		ethClient:   mocks.NewMockEthereumProviderClient(ctrl),
		tzClient:    mocks.NewMockTzKTClient(ctrl),
		httpClient:  mocks.NewMockHTTPClient(ctrl),
		uriResolver: mocks.NewMockURIResolver(ctrl),
		json:        mocks.NewMockJSON(ctrl),
		clock:       mocks.NewMockClock(ctrl),
		jcs:         mocks.NewMockJCS(ctrl),
		base64:      mocks.NewMockBase64(ctrl),
		store:       mocks.NewMockStore(ctrl),
		registry:    mocks.NewMockPublisherRegistry(ctrl),
	}

	tm.resolver = metadata.NewResolver(
		tm.ethClient,
		tm.tzClient,
		tm.httpClient,
		tm.uriResolver,
		tm.json,
		tm.clock,
		tm.jcs,
		tm.base64,
		tm.store,
		tm.registry,
	)

	return tm
}

// tearDownTestResolver cleans up the test mocks
func tearDownTestResolver(mocks *testResolverMocks) {
	mocks.ctrl.Finish()
}

func TestResolver_Resolve_ERC721(t *testing.T) {
	mocks := setupTestResolver(t)
	defer tearDownTestResolver(mocks)

	tokenCID := domain.NewTokenCID(domain.ChainEthereumMainnet, domain.StandardERC721, "0x123", "1")

	// Mock ERC721 token URI call
	mocks.ethClient.
		EXPECT().
		ERC721TokenURI(gomock.Any(), "0x123", "1").
		Return("https://example.com/metadata.json", nil)

	// Mock URI resolver to return the URI as-is
	mocks.uriResolver.
		EXPECT().
		Resolve(gomock.Any(), "https://example.com/metadata.json").
		Return("https://example.com/metadata.json", nil)

	// Mock HTTP client to fetch metadata
	metadata := map[string]interface{}{
		"name":          "Test NFT",
		"description":   "A test NFT",
		"image":         "https://example.com/image.png",
		"animation_url": "https://example.com/animation.mp4",
		"artist":        "FF",
	}
	mocks.httpClient.
		EXPECT().
		Get(gomock.Any(), "https://example.com/metadata.json", gomock.Any()).
		DoAndReturn(func(ctx context.Context, url string, result interface{}) error {
			*result.(*map[string]interface{}) = metadata
			return nil
		})

	// Mock URI resolver and HTTP client for MIME type detection (animation URL only)
	// When both animation_url and image are present, detectMimeType prioritizes animation_url
	// and only uses it for MIME type detection, so image URL mocks are not needed
	mocks.uriResolver.
		EXPECT().
		Resolve(gomock.Any(), "https://example.com/animation.mp4").
		Return("https://example.com/animation.mp4", nil)
	mocks.httpClient.
		EXPECT().
		GetPartialContent(gomock.Any(), "https://example.com/animation.mp4", gomock.Any()).
		Return([]byte("fake animation data"), nil)

	// Mock registry lookup for publisher resolution (called once in resolvePublisher)
	// When LookupPublisherByCollection returns a publisher, resolvePublisher returns early
	// and doesn't call getContractDeployer, so no need to mock GetMinBlock, GetContractDeployer, or SetKeyValue
	mocks.registry.
		EXPECT().
		LookupPublisherByCollection(domain.ChainEthereumMainnet, "0x123").
		Return(&registry.PublisherInfo{
			Name: registry.PublisherNameArtBlocks,
			URL:  "https://artblocks.io",
		})

	result, err := mocks.resolver.Resolve(context.Background(), tokenCID)

	assert.NoError(t, err)
	assert.NotNil(t, result)
	assert.Equal(t, "Test NFT", result.Name)
	assert.Equal(t, "A test NFT", result.Description)
	assert.Equal(t, "https://example.com/image.png", result.Image)
	assert.Equal(t, "https://example.com/animation.mp4", result.Animation)
	assert.Equal(t, registry.PublisherNameArtBlocks, *result.Publisher.Name)
	assert.Equal(t, "text/plain; charset=utf-8", *result.MimeType)
	assert.Equal(t, "FF", result.Artists[0].Name)
	assert.Empty(t, result.Artists[0].DID)
}

func TestResolver_Resolve_ERC1155(t *testing.T) {
	mocks := setupTestResolver(t)
	defer tearDownTestResolver(mocks)

	tokenCID := domain.NewTokenCID(domain.ChainEthereumMainnet, domain.StandardERC1155, "0x123", "1")

	// Mock ERC1155 URI call (with placeholder)
	mocks.ethClient.
		EXPECT().
		ERC1155URI(gomock.Any(), "0x123", "1").
		Return("https://example.com/metadata/{id}.json", nil)

	// Mock URI resolver (placeholder is replaced before calling Resolve)
	mocks.uriResolver.
		EXPECT().
		Resolve(gomock.Any(), "https://example.com/metadata/1.json").
		Return("https://example.com/metadata/1.json", nil)

	// Mock HTTP client to fetch metadata
	metadata := map[string]interface{}{
		"name":        "Test ERC1155",
		"description": "A test ERC1155 token",
		"image":       "https://example.com/image.png",
	}
	mocks.httpClient.
		EXPECT().
		Get(gomock.Any(), "https://example.com/metadata/1.json", gomock.Any()).
		DoAndReturn(func(ctx context.Context, url string, result interface{}) error {
			*result.(*map[string]interface{}) = metadata
			return nil
		})

	// Mock URI resolver and HTTP client for MIME type detection (image URL)
	mocks.uriResolver.
		EXPECT().
		Resolve(gomock.Any(), "https://example.com/image.png").
		Return("https://example.com/image.png", nil)
	mocks.httpClient.
		EXPECT().
		GetPartialContent(gomock.Any(), "https://example.com/image.png", gomock.Any()).
		Return([]byte("fake image data"), nil)

	// Mock registry lookup for publisher resolution (called once in resolvePublisher)
	mocks.registry.
		EXPECT().
		LookupPublisherByCollection(domain.ChainEthereumMainnet, "0x123").
		Return(nil)
	mocks.registry.
		EXPECT().
		GetMinBlock(domain.ChainEthereumMainnet).
		Return(uint64(0), false)

	// Mock GetContractDeployer for publisher resolution (called once in resolvePublisher)
	mocks.ethClient.
		EXPECT().
		GetContractDeployer(gomock.Any(), "0x123", uint64(0)).
		Return("", nil)

	// Mock store for caching deployer (called once in getContractDeployer)
	mocks.store.
		EXPECT().
		SetKeyValue(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(nil)

	result, err := mocks.resolver.Resolve(context.Background(), tokenCID)

	assert.NoError(t, err)
	assert.NotNil(t, result)
	assert.Equal(t, "Test ERC1155", result.Name)
	assert.Equal(t, "A test ERC1155 token", result.Description)
	assert.Equal(t, "https://example.com/image.png", result.Image)
	assert.Equal(t, "text/plain; charset=utf-8", *result.MimeType)
}

func TestResolver_Resolve_FA2(t *testing.T) {
	mocks := setupTestResolver(t)
	defer tearDownTestResolver(mocks)

	tokenCID := domain.NewTokenCID(domain.ChainTezosMainnet, domain.StandardFA2, "KT1ABC", "1")

	// Mock TzKT GetTokenMetadata call
	metadata := map[string]interface{}{
		"name":        "Test FA2",
		"description": "A test FA2 token",
		"displayUri":  "ipfs://QmXXX",
		"artifactUri": "ipfs://QmYYY",
		"creators":    []string{"tz1ABC"},
	}
	mocks.tzClient.
		EXPECT().
		GetTokenMetadata(gomock.Any(), "KT1ABC", "1").
		Return(metadata, nil)

	// Mock JSON for creators unmarshaling
	mocks.json.
		EXPECT().
		Marshal(gomock.Any()).
		Return([]byte(`["tz1ABC"]`), nil)
	mocks.json.
		EXPECT().
		Unmarshal(gomock.Any(), gomock.Any()).
		Return(nil)

	// Mock URI resolver and HTTP client for MIME type detection (artifactUri)
	// Note: uriToGateway converts ipfs://QmYYY to https://ipfs.io/ipfs/QmYYY before detectMimeType is called
	mocks.uriResolver.
		EXPECT().
		Resolve(gomock.Any(), "https://ipfs.io/ipfs/QmYYY").
		Return("https://ipfs.io/ipfs/QmYYY", nil)
	mocks.httpClient.
		EXPECT().
		GetPartialContent(gomock.Any(), "https://ipfs.io/ipfs/QmYYY", gomock.Any()).
		Return([]byte("fake artifact data"), nil)

	// Mock registry lookup for publisher resolution (called once in resolvePublisher)
	mocks.registry.
		EXPECT().
		LookupPublisherByCollection(domain.ChainTezosMainnet, "KT1ABC").
		Return(nil)
	mocks.registry.
		EXPECT().
		GetMinBlock(domain.ChainTezosMainnet).
		Return(uint64(0), false)

	// Mock GetContractDeployer for Tezos (called once in resolvePublisher)
	mocks.tzClient.
		EXPECT().
		GetContractDeployer(gomock.Any(), "KT1ABC").
		Return("", nil)

	// Mock store for caching deployer (called once in getContractDeployer)
	mocks.store.EXPECT().
		SetKeyValue(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(nil)

	result, err := mocks.resolver.Resolve(context.Background(), tokenCID)

	assert.NoError(t, err)
	assert.NotNil(t, result)
	assert.Equal(t, "Test FA2", result.Name)
	assert.Equal(t, "A test FA2 token", result.Description)
	assert.Equal(t, "https://ipfs.io/ipfs/QmXXX", result.Image)
	assert.Equal(t, "https://ipfs.io/ipfs/QmYYY", result.Animation)
	assert.Equal(t, "text/plain; charset=utf-8", *result.MimeType)
}

func TestResolver_Resolve_UnsupportedStandard(t *testing.T) {
	mocks := setupTestResolver(t)
	defer tearDownTestResolver(mocks)

	// Create a token CID with unsupported standard
	tokenCID := domain.NewTokenCID(domain.ChainEthereumMainnet, domain.ChainStandard("unsupported"), "0x123", "1")

	result, err := mocks.resolver.Resolve(context.Background(), tokenCID)

	assert.Error(t, err)
	assert.Nil(t, result)
	assert.Contains(t, err.Error(), "unsupported standard")
}

func TestResolver_Resolve_DataURI(t *testing.T) {
	mocks := setupTestResolver(t)
	defer tearDownTestResolver(mocks)

	tokenCID := domain.NewTokenCID(domain.ChainEthereumMainnet, domain.StandardERC721, "0x123", "1")

	// Mock ERC721 token URI call returning data URI
	dataURI := "data:application/json;base64,eyJuYW1lIjoiVGVzdCBORlQifQ==" // {"name":"Test NFT"} in base64
	mocks.ethClient.
		EXPECT().
		ERC721TokenURI(gomock.Any(), "0x123", "1").
		Return(dataURI, nil)

	// Mock base64 decode for parsing data URI
	mocks.base64.
		EXPECT().
		Decode("eyJuYW1lIjoiVGVzdCBORlQifQ==").
		Return([]byte(`{"name":"Test NFT"}`), nil)

	// Mock JSON unmarshal for parsing data URI
	metadata := map[string]interface{}{
		"name": "Test NFT",
	}
	mocks.json.
		EXPECT().
		Unmarshal(gomock.Any(), gomock.Any()).
		DoAndReturn(func(data []byte, v interface{}) error {
			*v.(*map[string]interface{}) = metadata
			return nil
		})

	// Mock registry lookup for publisher resolution (called once in resolvePublisher)
	mocks.registry.
		EXPECT().
		LookupPublisherByCollection(domain.ChainEthereumMainnet, "0x123").
		Return(nil)
	mocks.registry.
		EXPECT().
		GetMinBlock(domain.ChainEthereumMainnet).
		Return(uint64(0), false)

	// Mock GetContractDeployer for publisher resolution (called once in resolvePublisher)
	mocks.ethClient.
		EXPECT().
		GetContractDeployer(gomock.Any(), "0x123", uint64(0)).
		Return("", nil)

	// Mock store for caching deployer (called once in getContractDeployer)
	mocks.store.
		EXPECT().
		SetKeyValue(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(nil)

	result, err := mocks.resolver.Resolve(context.Background(), tokenCID)

	assert.NoError(t, err)
	assert.NotNil(t, result)
	assert.Equal(t, "Test NFT", result.Name)
}

func TestResolver_RawHash(t *testing.T) {
	mocks := setupTestResolver(t)
	defer tearDownTestResolver(mocks)

	meta := &metadata.NormalizedMetadata{
		Raw: map[string]interface{}{
			"name": "Test NFT",
		},
	}

	// Mock JSON marshal
	mocks.json.
		EXPECT().
		Marshal(gomock.Any()).
		Return([]byte(`{"name":"Test NFT"}`), nil)

	// Mock JCS transform
	mocks.jcs.
		EXPECT().
		Transform(gomock.Any()).
		DoAndReturn(func(data []byte) ([]byte, error) {
			return data, nil // Simplified: in real usage, JCS canonicalizes the JSON
		})

	hash, raw, err := mocks.resolver.RawHash(meta)

	assert.NoError(t, err)
	assert.NotNil(t, hash)
	assert.NotNil(t, raw)
	assert.Equal(t, 32, len(hash)) // SHA256 hash length
	assert.Contains(t, string(raw), "Test NFT")
}
