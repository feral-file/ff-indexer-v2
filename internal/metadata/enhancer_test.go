package metadata_test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"

	"github.com/feral-file/ff-indexer-v2/internal/domain"
	"github.com/feral-file/ff-indexer-v2/internal/metadata"
	"github.com/feral-file/ff-indexer-v2/internal/mocks"
	"github.com/feral-file/ff-indexer-v2/internal/providers/vendors/artblocks"
	"github.com/feral-file/ff-indexer-v2/internal/providers/vendors/feralfile"
	"github.com/feral-file/ff-indexer-v2/internal/registry"
)

// testEnhancerMocks contains all the mocks needed for testing the enhancer
type testEnhancerMocks struct {
	ctrl            *gomock.Controller
	httpClient      *mocks.MockHTTPClient
	uriResolver     *mocks.MockURIResolver
	artblocksClient *mocks.MockArtBlocksClient
	feralfileClient *mocks.MockFeralFileClient
	fxhashClient    *mocks.MockFxhashClient
	json            *mocks.MockJSON
	jcs             *mocks.MockJCS
	enhancer        metadata.Enhancer
}

// setupTestEnhancer creates all the mocks and enhancer for testing
func setupTestEnhancer(t *testing.T) *testEnhancerMocks {
	ctrl := gomock.NewController(t)

	tm := &testEnhancerMocks{
		ctrl:            ctrl,
		httpClient:      mocks.NewMockHTTPClient(ctrl),
		uriResolver:     mocks.NewMockURIResolver(ctrl),
		artblocksClient: mocks.NewMockArtBlocksClient(ctrl),
		feralfileClient: mocks.NewMockFeralFileClient(ctrl),
		fxhashClient:    mocks.NewMockFxhashClient(ctrl),
		json:            mocks.NewMockJSON(ctrl),
		jcs:             mocks.NewMockJCS(ctrl),
	}

	tm.enhancer = metadata.NewEnhancer(
		tm.httpClient,
		tm.uriResolver,
		tm.artblocksClient,
		tm.feralfileClient,
		tm.fxhashClient,
		tm.json,
		tm.jcs,
	)

	return tm
}

// tearDownTestEnhancer cleans up the test mocks
func tearDownTestEnhancer(mocks *testEnhancerMocks) {
	mocks.ctrl.Finish()
}

func TestEnhancer_Enhance_ArtBlocks(t *testing.T) {
	mocks := setupTestEnhancer(t)
	defer tearDownTestEnhancer(mocks)

	tokenCID := domain.NewTokenCID(domain.ChainEthereumMainnet, domain.StandardERC721, "0xa7d8d9ef8D8Ce8992Df33D8b8CF4Aebabd5bD270", "1000005")
	publisherName := registry.PublisherNameArtBlocks

	// Create normalized metadata with ArtBlocks publisher
	normalizedMeta := &metadata.NormalizedMetadata{
		Raw: map[string]interface{}{
			"name":          "Test ArtBlocks",
			"image":         "https://example.com/image.png",
			"generator_url": "https://example.com/generator.html",
		},
		Publisher: &metadata.Publisher{
			Name: &publisherName,
			URL:  stringPtr("https://artblocks.io"),
		},
	}

	// Mock ArtBlocks client to return project metadata
	// Token ID 1000005 = project 1, mint 5
	projectMetadata := &artblocks.ProjectMetadata{
		ID:            "0xa7d8d9ef8D8Ce8992Df33D8b8CF4Aebabd5bD270-1",
		Name:          "Fidenza",
		Slug:          "fidenza",
		ArtistName:    "Tyler Hobbs",
		ArtistAddress: "0x1234567890123456789012345678901234567890",
		Description:   stringPtr("A generative art project"),
	}
	mocks.artblocksClient.
		EXPECT().
		GetProjectMetadata(gomock.Any(), "0xa7d8d9ef8d8ce8992df33d8b8cf4aebabd5bd270-1").
		Return(projectMetadata, nil)

	// Mock JSON marshal for project metadata
	vendorJSON := []byte(`{"id":"0xa7d8d9ef8D8Ce8992Df33D8b8CF4Aebabd5bD270-1","name":"Fidenza","artist_name":"Tyler Hobbs","description":"A generative art project","artist_address":"0x1234567890123456789012345678901234567890","slug":"fidenza"}`)
	mocks.json.
		EXPECT().
		Marshal(projectMetadata).
		Return(vendorJSON, nil)

	// Mock URI resolver and HTTP client for MIME type detection (generator_url)
	mocks.uriResolver.
		EXPECT().
		Resolve(gomock.Any(), "https://example.com/generator.html").
		Return("https://example.com/generator.html", nil)
	// Mock Head to fail so it falls back to GetPartialContent
	mocks.httpClient.
		EXPECT().
		Head(gomock.Any(), "https://example.com/generator.html").
		Return(nil, assert.AnError)
	mocks.httpClient.
		EXPECT().
		GetPartialContent(gomock.Any(), "https://example.com/generator.html", gomock.Any()).
		Return([]byte("fake generator data"), nil)

	result, err := mocks.enhancer.Enhance(context.Background(), tokenCID, normalizedMeta)

	assert.NoError(t, err)
	assert.NotNil(t, result)
	assert.Equal(t, registry.PublisherNameArtBlocks, result.Vendor)
	assert.Equal(t, vendorJSON, result.VendorJSON)
	assert.NotNil(t, result.Name)
	assert.Equal(t, "Fidenza #5", *result.Name)
	assert.NotNil(t, result.Description)
	assert.Equal(t, "A generative art project", *result.Description)
	assert.NotNil(t, result.ImageURL)
	assert.Equal(t, "https://example.com/image.png", *result.ImageURL)
	assert.NotNil(t, result.AnimationURL)
	assert.Equal(t, "https://example.com/generator.html", *result.AnimationURL)
	assert.Len(t, result.Artists, 1)
	assert.Equal(t, "Tyler Hobbs", result.Artists[0].Name)
	assert.NotEmpty(t, result.Artists[0].DID)
	assert.NotNil(t, result.MimeType)
}

func TestEnhancer_Enhance_ArtBlocks_NoDescription(t *testing.T) {
	mocks := setupTestEnhancer(t)
	defer tearDownTestEnhancer(mocks)

	tokenCID := domain.NewTokenCID(domain.ChainEthereumMainnet, domain.StandardERC721, "0xa7d8d9ef8D8Ce8992Df33D8b8CF4Aebabd5bD270", "1000005")
	publisherName := registry.PublisherNameArtBlocks

	// Create normalized metadata with ArtBlocks publisher
	normalizedMeta := &metadata.NormalizedMetadata{
		Raw: map[string]interface{}{
			"name":          "Test ArtBlocks",
			"image":         "https://example.com/image.png",
			"generator_url": "https://example.com/generator.html",
		},
		Publisher: &metadata.Publisher{
			Name: &publisherName,
			URL:  stringPtr("https://artblocks.io"),
		},
	}

	// Mock ArtBlocks client to return project metadata without description
	projectMetadata := &artblocks.ProjectMetadata{
		ID:            "0xa7d8d9ef8D8Ce8992Df33D8b8CF4Aebabd5bD270-1",
		Name:          "Fidenza",
		Slug:          "fidenza",
		ArtistName:    "Tyler Hobbs",
		ArtistAddress: "0x1234567890123456789012345678901234567890",
		Description:   nil,
	}
	mocks.artblocksClient.
		EXPECT().
		GetProjectMetadata(gomock.Any(), "0xa7d8d9ef8d8ce8992df33d8b8cf4aebabd5bd270-1").
		Return(projectMetadata, nil)

	// Mock JSON marshal for project metadata
	vendorJSON := []byte(`{"id":"0xa7d8d9ef8D8Ce8992Df33D8b8CF4Aebabd5bD270-1","name":"Fidenza"}`)
	mocks.json.
		EXPECT().
		Marshal(projectMetadata).
		Return(vendorJSON, nil)

	// Mock URI resolver and HTTP client for MIME type detection
	mocks.uriResolver.
		EXPECT().
		Resolve(gomock.Any(), "https://example.com/generator.html").
		Return("https://example.com/generator.html", nil)
	// Mock Head to fail so it falls back to GetPartialContent
	mocks.httpClient.
		EXPECT().
		Head(gomock.Any(), "https://example.com/generator.html").
		Return(nil, assert.AnError)
	mocks.httpClient.
		EXPECT().
		GetPartialContent(gomock.Any(), "https://example.com/generator.html", gomock.Any()).
		Return([]byte("fake generator data"), nil)

	result, err := mocks.enhancer.Enhance(context.Background(), tokenCID, normalizedMeta)

	assert.NoError(t, err)
	assert.NotNil(t, result)
	assert.Nil(t, result.Description) // Description should be nil when not provided
	assert.Equal(t, "Fidenza #5", *result.Name)
}

func TestEnhancer_Enhance_ArtBlocks_NoArtistAddress(t *testing.T) {
	mocks := setupTestEnhancer(t)
	defer tearDownTestEnhancer(mocks)

	tokenCID := domain.NewTokenCID(domain.ChainEthereumMainnet, domain.StandardERC721, "0xa7d8d9ef8D8Ce8992Df33D8b8CF4Aebabd5bD270", "1000005")
	publisherName := registry.PublisherNameArtBlocks

	// Create normalized metadata with ArtBlocks publisher
	normalizedMeta := &metadata.NormalizedMetadata{
		Raw: map[string]interface{}{
			"name":          "Test ArtBlocks",
			"image":         "https://example.com/image.png",
			"generator_url": "https://example.com/generator.html",
		},
		Publisher: &metadata.Publisher{
			Name: &publisherName,
			URL:  stringPtr("https://artblocks.io"),
		},
	}

	// Mock ArtBlocks client to return project metadata without artist address
	projectMetadata := &artblocks.ProjectMetadata{
		ID:            "0xa7d8d9ef8D8Ce8992Df33D8b8CF4Aebabd5bD270-1",
		Name:          "Fidenza",
		Slug:          "fidenza",
		ArtistName:    "Tyler Hobbs",
		ArtistAddress: "", // Empty artist address
		Description:   stringPtr("A generative art project"),
	}
	mocks.artblocksClient.
		EXPECT().
		GetProjectMetadata(gomock.Any(), "0xa7d8d9ef8d8ce8992df33d8b8cf4aebabd5bd270-1").
		Return(projectMetadata, nil)

	// Mock JSON marshal for project metadata
	vendorJSON := []byte(`{"id":"0xa7d8d9ef8D8Ce8992Df33D8b8CF4Aebabd5bD270-1","name":"Fidenza"}`)
	mocks.json.
		EXPECT().
		Marshal(projectMetadata).
		Return(vendorJSON, nil)

	// Mock URI resolver and HTTP client for MIME type detection
	mocks.uriResolver.
		EXPECT().
		Resolve(gomock.Any(), "https://example.com/generator.html").
		Return("https://example.com/generator.html", nil)
	// Mock Head to fail so it falls back to GetPartialContent
	mocks.httpClient.
		EXPECT().
		Head(gomock.Any(), "https://example.com/generator.html").
		Return(nil, assert.AnError)
	mocks.httpClient.
		EXPECT().
		GetPartialContent(gomock.Any(), "https://example.com/generator.html", gomock.Any()).
		Return([]byte("fake generator data"), nil)

	result, err := mocks.enhancer.Enhance(context.Background(), tokenCID, normalizedMeta)

	assert.NoError(t, err)
	assert.NotNil(t, result)
	assert.Empty(t, result.Artists) // No artists should be added when artist address is empty
}

func TestEnhancer_Enhance_ArtBlocks_NonEthereumChain(t *testing.T) {
	mocks := setupTestEnhancer(t)
	defer tearDownTestEnhancer(mocks)

	// ArtBlocks only supports Ethereum mainnet, so Tezos should return nil
	tokenCID := domain.NewTokenCID(domain.ChainTezosMainnet, domain.StandardFA2, "KT1ABC", "1")
	publisherName := registry.PublisherNameArtBlocks

	// Create normalized metadata with ArtBlocks publisher
	normalizedMeta := &metadata.NormalizedMetadata{
		Raw: map[string]interface{}{
			"name": "Test ArtBlocks",
		},
		Publisher: &metadata.Publisher{
			Name: &publisherName,
			URL:  stringPtr("https://artblocks.io"),
		},
	}

	// No mocks should be called since ArtBlocks enhancement is skipped for non-Ethereum chains

	result, err := mocks.enhancer.Enhance(context.Background(), tokenCID, normalizedMeta)

	assert.NoError(t, err)
	assert.Nil(t, result) // Should return nil for non-Ethereum chains
}

func TestEnhancer_Enhance_NoPublisher(t *testing.T) {
	mocks := setupTestEnhancer(t)
	defer tearDownTestEnhancer(mocks)

	tokenCID := domain.NewTokenCID(domain.ChainEthereumMainnet, domain.StandardERC721, "0x123", "1")

	// Create normalized metadata without publisher
	normalizedMeta := &metadata.NormalizedMetadata{
		Raw: map[string]interface{}{
			"name": "Test NFT",
		},
		Publisher: nil,
	}

	// No mocks should be called since enhancement is skipped when no publisher

	result, err := mocks.enhancer.Enhance(context.Background(), tokenCID, normalizedMeta)

	assert.NoError(t, err)
	assert.Nil(t, result) // Should return nil when no publisher
}

func TestEnhancer_Enhance_NoPublisherName(t *testing.T) {
	mocks := setupTestEnhancer(t)
	defer tearDownTestEnhancer(mocks)

	tokenCID := domain.NewTokenCID(domain.ChainEthereumMainnet, domain.StandardERC721, "0x123", "1")

	// Create normalized metadata with publisher but no name
	normalizedMeta := &metadata.NormalizedMetadata{
		Raw: map[string]interface{}{
			"name": "Test NFT",
		},
		Publisher: &metadata.Publisher{
			Name: nil,
			URL:  stringPtr("https://example.com"),
		},
	}

	// No mocks should be called since enhancement is skipped when no publisher name

	result, err := mocks.enhancer.Enhance(context.Background(), tokenCID, normalizedMeta)

	assert.NoError(t, err)
	assert.Nil(t, result) // Should return nil when no publisher name
}

func TestEnhancer_Enhance_UnsupportedPublisher(t *testing.T) {
	mocks := setupTestEnhancer(t)
	defer tearDownTestEnhancer(mocks)

	tokenCID := domain.NewTokenCID(domain.ChainEthereumMainnet, domain.StandardERC721, "0x123", "1")
	publisherName := registry.PublisherName("unsupported")

	// Create normalized metadata with unsupported publisher
	normalizedMeta := &metadata.NormalizedMetadata{
		Raw: map[string]interface{}{
			"name": "Test NFT",
		},
		Publisher: &metadata.Publisher{
			Name: &publisherName,
			URL:  stringPtr("https://example.com"),
		},
	}

	// No mocks should be called since enhancement is skipped for unsupported publishers

	result, err := mocks.enhancer.Enhance(context.Background(), tokenCID, normalizedMeta)

	assert.NoError(t, err)
	assert.Nil(t, result) // Should return nil for unsupported publishers
}

func TestEnhancer_Enhance_ArtBlocks_InvalidTokenID(t *testing.T) {
	mocks := setupTestEnhancer(t)
	defer tearDownTestEnhancer(mocks)

	tokenCID := domain.NewTokenCID(domain.ChainEthereumMainnet, domain.StandardERC721, "0xa7d8d9ef8D8Ce8992Df33D8b8CF4Aebabd5bD270", "invalid")
	publisherName := registry.PublisherNameArtBlocks

	// Create normalized metadata with ArtBlocks publisher
	normalizedMeta := &metadata.NormalizedMetadata{
		Raw: map[string]interface{}{
			"name": "Test ArtBlocks",
		},
		Publisher: &metadata.Publisher{
			Name: &publisherName,
			URL:  stringPtr("https://artblocks.io"),
		},
	}

	// No mocks should be called since token ID parsing fails

	result, err := mocks.enhancer.Enhance(context.Background(), tokenCID, normalizedMeta)

	assert.Error(t, err)
	assert.Nil(t, result)
	assert.Contains(t, err.Error(), "failed to parse ArtBlocks token ID")
}

func TestEnhancer_Enhance_ArtBlocks_APIError(t *testing.T) {
	mocks := setupTestEnhancer(t)
	defer tearDownTestEnhancer(mocks)

	tokenCID := domain.NewTokenCID(domain.ChainEthereumMainnet, domain.StandardERC721, "0xa7d8d9ef8D8Ce8992Df33D8b8CF4Aebabd5bD270", "1000005")
	publisherName := registry.PublisherNameArtBlocks

	// Create normalized metadata with ArtBlocks publisher
	normalizedMeta := &metadata.NormalizedMetadata{
		Raw: map[string]interface{}{
			"name": "Test ArtBlocks",
		},
		Publisher: &metadata.Publisher{
			Name: &publisherName,
			URL:  stringPtr("https://artblocks.io"),
		},
	}

	// Mock ArtBlocks client to return an error
	mocks.artblocksClient.
		EXPECT().
		GetProjectMetadata(gomock.Any(), "0xa7d8d9ef8d8ce8992df33d8b8cf4aebabd5bd270-1").
		Return(nil, assert.AnError)

	result, err := mocks.enhancer.Enhance(context.Background(), tokenCID, normalizedMeta)

	assert.Error(t, err)
	assert.Nil(t, result)
	assert.Contains(t, err.Error(), "failed to enhance ArtBlocks metadata")
}

func TestEnhancer_Enhance_ArtBlocks_MarshalError(t *testing.T) {
	mocks := setupTestEnhancer(t)
	defer tearDownTestEnhancer(mocks)

	tokenCID := domain.NewTokenCID(domain.ChainEthereumMainnet, domain.StandardERC721, "0xa7d8d9ef8D8Ce8992Df33D8b8CF4Aebabd5bD270", "1000005")
	publisherName := registry.PublisherNameArtBlocks

	// Create normalized metadata with ArtBlocks publisher
	normalizedMeta := &metadata.NormalizedMetadata{
		Raw: map[string]interface{}{
			"name": "Test ArtBlocks",
		},
		Publisher: &metadata.Publisher{
			Name: &publisherName,
			URL:  stringPtr("https://artblocks.io"),
		},
	}

	// Mock ArtBlocks client to return project metadata
	projectMetadata := &artblocks.ProjectMetadata{
		ID:            "0xa7d8d9ef8D8Ce8992Df33D8b8CF4Aebabd5bD270-1",
		Name:          "Fidenza",
		Slug:          "fidenza",
		ArtistName:    "Tyler Hobbs",
		ArtistAddress: "0x1234567890123456789012345678901234567890",
	}
	mocks.artblocksClient.
		EXPECT().
		GetProjectMetadata(gomock.Any(), "0xa7d8d9ef8d8ce8992df33d8b8cf4aebabd5bd270-1").
		Return(projectMetadata, nil)

	// Mock JSON marshal to return an error
	mocks.json.
		EXPECT().
		Marshal(projectMetadata).
		Return(nil, assert.AnError)

	result, err := mocks.enhancer.Enhance(context.Background(), tokenCID, normalizedMeta)

	assert.Error(t, err)
	assert.Nil(t, result)
	assert.Contains(t, err.Error(), "failed to enhance ArtBlocks metadata")
}

func TestEnhancer_VendorJsonHash(t *testing.T) {
	mocks := setupTestEnhancer(t)
	defer tearDownTestEnhancer(mocks)

	vendorJSON := []byte(`{"name":"Fidenza","id":"1"}`)
	canonicalizedJSON := []byte(`{"id":"1","name":"Fidenza"}`)

	enhancedMetadata := &metadata.EnhancedMetadata{
		Vendor:     registry.PublisherNameArtBlocks,
		VendorJSON: vendorJSON,
	}

	// Mock JCS transform
	mocks.jcs.
		EXPECT().
		Transform(vendorJSON).
		Return(canonicalizedJSON, nil)
	canonicalizedHash := sha256.Sum256(canonicalizedJSON)

	hash, err := mocks.enhancer.VendorJsonHash(enhancedMetadata)

	assert.NoError(t, err)
	assert.NotNil(t, hash)
	assert.Equal(t, 32, len(hash)) // SHA256 hash length
	assert.Equal(t, hex.EncodeToString(hash[:]), hex.EncodeToString(canonicalizedHash[:]))
}

func TestEnhancer_VendorJsonHash_JCSError(t *testing.T) {
	mocks := setupTestEnhancer(t)
	defer tearDownTestEnhancer(mocks)

	vendorJSON := []byte(`{"name":"Fidenza"}`)

	enhancedMetadata := &metadata.EnhancedMetadata{
		Vendor:     registry.PublisherNameArtBlocks,
		VendorJSON: vendorJSON,
	}

	// Mock JCS transform to return an error
	mocks.jcs.
		EXPECT().
		Transform(vendorJSON).
		Return(nil, assert.AnError)

	hash, err := mocks.enhancer.VendorJsonHash(enhancedMetadata)

	assert.Error(t, err)
	assert.Nil(t, hash)
	assert.Contains(t, err.Error(), "failed to canonicalize vendor JSON")
}

func TestEnhancer_Enhance_FeralFile(t *testing.T) {
	mocks := setupTestEnhancer(t)
	defer tearDownTestEnhancer(mocks)

	tokenCID := domain.NewTokenCID(domain.ChainEthereumMainnet, domain.StandardERC721, "0x1234567890abcdef", "68133196527112232794835997367314869505960984666033462681082934679485439444096")
	publisherName := registry.PublisherNameFeralFile

	// Create normalized metadata with Feral File publisher
	normalizedMeta := &metadata.NormalizedMetadata{
		Raw: map[string]interface{}{
			"name":  "Test Feral File",
			"image": "https://example.com/image.png",
		},
		Publisher: &metadata.Publisher{
			Name: &publisherName,
			URL:  stringPtr("https://feralfile.com"),
		},
	}

	// Mock Feral File client to return artwork data
	artwork := &feralfile.Artwork{
		ID:           "68133196527112232794835997367314869505960984666033462681082934679485439444096",
		Name:         "Money Vortex: Binaural Beats",
		ThumbnailURI: "previews/test/thumbnail.jpg",
		PreviewURI:   "previews/test/preview.html",
		Series: feralfile.Series{
			Medium:      "software",
			Description: "Test description",
			Artist: feralfile.Artist{
				AlumniAccount: feralfile.AlumniAccount{
					ID:    "adad3710-916d-44c5-bb74-b0a516ad1836",
					Alias: "Steve Pikelny",
					Addresses: map[string]string{
						"ethereum": "0x47144372eb383466d18fc91db9cd0396aa6c87a4",
					},
				}},
		},
	}

	mocks.feralfileClient.
		EXPECT().
		GetArtwork(gomock.Any(), "68133196527112232794835997367314869505960984666033462681082934679485439444096").
		Return(artwork, nil)

	// Mock JSON marshal for artwork
	vendorJSON := []byte(`{"id":"68133196527112232794835997367314869505960984666033462681082934679485439444096","name":"Money Vortex: Binaural Beats"}`)
	mocks.json.
		EXPECT().
		Marshal(artwork).
		Return(vendorJSON, nil)

	// Mock URI resolver and HTTP client for MIME type detection
	expectedImageURL := "https://cdn.feralfileassets.com/previews/test/thumbnail.jpg"
	expectedAnimationURL := "https://cdn.feralfileassets.com/previews/test/preview.html"

	mocks.uriResolver.
		EXPECT().
		Resolve(gomock.Any(), expectedAnimationURL).
		Return(expectedAnimationURL, nil)
	mocks.httpClient.
		EXPECT().
		Head(gomock.Any(), expectedAnimationURL).
		Return(nil, assert.AnError)
	mocks.httpClient.
		EXPECT().
		GetPartialContent(gomock.Any(), expectedAnimationURL, gomock.Any()).
		Return([]byte("fake html data"), nil)

	result, err := mocks.enhancer.Enhance(context.Background(), tokenCID, normalizedMeta)

	assert.NoError(t, err)
	assert.NotNil(t, result)
	assert.Equal(t, registry.PublisherNameFeralFile, result.Vendor)
	assert.Equal(t, "Money Vortex: Binaural Beats", *result.Name)
	assert.Equal(t, "Test description", *result.Description)
	assert.Equal(t, expectedImageURL, *result.ImageURL)
	assert.Equal(t, expectedAnimationURL, *result.AnimationURL)
	assert.Len(t, result.Artists, 1)
	assert.Equal(t, "Steve Pikelny", result.Artists[0].Name)
	expectedDID := domain.NewDID("0x47144372eb383466d18fc91db9cd0396aa6c87a4", domain.ChainEthereumMainnet)
	assert.Equal(t, expectedDID, result.Artists[0].DID)
}

func TestEnhancer_Enhance_FeralFile_ImageMedium(t *testing.T) {
	mocks := setupTestEnhancer(t)
	defer tearDownTestEnhancer(mocks)

	tokenCID := domain.NewTokenCID(domain.ChainTezosMainnet, domain.StandardFA2, "KT1TestContract", "12345")
	publisherName := registry.PublisherNameFeralFile

	// Create normalized metadata with Feral File publisher
	normalizedMeta := &metadata.NormalizedMetadata{
		Raw: map[string]interface{}{
			"name": "Test Image",
		},
		Publisher: &metadata.Publisher{
			Name: &publisherName,
			URL:  stringPtr("https://feralfile.com"),
		},
	}

	// Mock Feral File client to return artwork data with image medium
	artwork := &feralfile.Artwork{
		ID:           "12345",
		Name:         "Test Image Artwork",
		ThumbnailURI: "thumbnails/test.jpg",
		PreviewURI:   "previews/test-full.jpg",
		Series: feralfile.Series{
			Medium:      "image",
			Description: "Image artwork",
			Artist: feralfile.Artist{
				AlumniAccount: feralfile.AlumniAccount{
					Alias: "Test Artist",
					Addresses: map[string]string{
						"tezos": "tz1TestAddress",
					},
				},
			},
		},
	}

	mocks.feralfileClient.
		EXPECT().
		GetArtwork(gomock.Any(), "12345").
		Return(artwork, nil)

	// Mock JSON marshal for artwork
	vendorJSON := []byte(`{"id":"12345","name":"Test Image Artwork"}`)
	mocks.json.
		EXPECT().
		Marshal(artwork).
		Return(vendorJSON, nil)

	// Mock URI resolver and HTTP client for MIME type detection (only for image, no animation)
	expectedImageURL := "https://cdn.feralfileassets.com/previews/test-full.jpg"

	mocks.uriResolver.
		EXPECT().
		Resolve(gomock.Any(), expectedImageURL).
		Return(expectedImageURL, nil)
	mocks.httpClient.
		EXPECT().
		Head(gomock.Any(), expectedImageURL).
		Return(nil, assert.AnError)
	mocks.httpClient.
		EXPECT().
		GetPartialContent(gomock.Any(), expectedImageURL, gomock.Any()).
		Return([]byte("fake image data"), nil)

	result, err := mocks.enhancer.Enhance(context.Background(), tokenCID, normalizedMeta)

	assert.NoError(t, err)
	assert.NotNil(t, result)
	assert.Equal(t, registry.PublisherNameFeralFile, result.Vendor)
	assert.Equal(t, expectedImageURL, *result.ImageURL)
	assert.Nil(t, result.AnimationURL) // No animation URL for image medium
	assert.Len(t, result.Artists, 1)
	assert.Equal(t, "Test Artist", result.Artists[0].Name)
	expectedDID := domain.NewDID("tz1TestAddress", domain.ChainTezosMainnet)
	assert.Equal(t, expectedDID, result.Artists[0].DID)
}

// Helper function to create string pointers
func stringPtr(s string) *string {
	return &s
}
