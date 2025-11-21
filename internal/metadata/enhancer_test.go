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
	"github.com/feral-file/ff-indexer-v2/internal/providers/vendors/objkt"
	"github.com/feral-file/ff-indexer-v2/internal/registry"
	"github.com/feral-file/ff-indexer-v2/internal/store/schema"
)

// testEnhancerMocks contains all the mocks needed for testing the enhancer
type testEnhancerMocks struct {
	ctrl            *gomock.Controller
	httpClient      *mocks.MockHTTPClient
	uriResolver     *mocks.MockURIResolver
	artblocksClient *mocks.MockArtBlocksClient
	feralfileClient *mocks.MockFeralFileClient
	objktClient     *mocks.MockObjktClient
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
		objktClient:     mocks.NewMockObjktClient(ctrl),
		json:            mocks.NewMockJSON(ctrl),
		jcs:             mocks.NewMockJCS(ctrl),
	}

	tm.enhancer = metadata.NewEnhancer(
		tm.httpClient,
		tm.uriResolver,
		tm.artblocksClient,
		tm.feralfileClient,
		tm.objktClient,
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
	assert.Equal(t, schema.VendorArtBlocks, result.Vendor)
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
		Vendor:     schema.VendorArtBlocks,
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
		Vendor:     schema.VendorArtBlocks,
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
	assert.Equal(t, schema.VendorFeralFile, result.Vendor)
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
	assert.Equal(t, schema.VendorFeralFile, result.Vendor)
	assert.Equal(t, expectedImageURL, *result.ImageURL)
	assert.Nil(t, result.AnimationURL) // No animation URL for image medium
	assert.Len(t, result.Artists, 1)
	assert.Equal(t, "Test Artist", result.Artists[0].Name)
	expectedDID := domain.NewDID("tz1TestAddress", domain.ChainTezosMainnet)
	assert.Equal(t, expectedDID, result.Artists[0].DID)
}

func TestEnhancer_Enhance_FeralFile_MayaManStarQuest(t *testing.T) {
	mocks := setupTestEnhancer(t)
	defer tearDownTestEnhancer(mocks)

	// Use the Maya Man StarQuest contract address
	tokenCID := domain.NewTokenCID(domain.ChainEthereumMainnet, domain.StandardERC721, feralfile.MAYA_MAN_STARQUEST_CONTRACT, "12345")
	publisherName := registry.PublisherNameFeralFile

	// Create normalized metadata with Feral File publisher
	normalizedMeta := &metadata.NormalizedMetadata{
		Raw: map[string]interface{}{
			"name": "Test Maya Man StarQuest",
		},
		Publisher: &metadata.Publisher{
			Name: &publisherName,
			URL:  stringPtr("https://feralfile.com"),
		},
	}

	// Mock Feral File client to return artwork data with non-image medium (software)
	artwork := &feralfile.Artwork{
		ID:           "12345",
		Name:         "Maya Man StarQuest Episode",
		ThumbnailURI: "previews/maya/thumbnail.jpg",
		PreviewURI:   "previews/maya/preview.html",
		Series: feralfile.Series{
			Medium:      "software",
			Description: "Maya Man StarQuest episode description",
			Artist: feralfile.Artist{
				AlumniAccount: feralfile.AlumniAccount{
					ID:    "maya-artist-id",
					Alias: "Maya Artist",
					Addresses: map[string]string{
						"ethereum": "0x1234567890123456789012345678901234567890",
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
	vendorJSON := []byte(`{"id":"12345","name":"Maya Man StarQuest Episode"}`)
	mocks.json.
		EXPECT().
		Marshal(artwork).
		Return(vendorJSON, nil)

	// Mock URI resolver and HTTP client for MIME type detection
	// The animation URL should have &mode=episode appended for Maya Man StarQuest
	expectedAnimationURL := "https://cdn.feralfileassets.com/previews/maya/preview.html&mode=episode"
	expectedImageURL := "https://cdn.feralfileassets.com/previews/maya/thumbnail.jpg"

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
	assert.Equal(t, schema.VendorFeralFile, result.Vendor)
	assert.Equal(t, "Maya Man StarQuest Episode", *result.Name)
	assert.Equal(t, "Maya Man StarQuest episode description", *result.Description)
	assert.Equal(t, expectedImageURL, *result.ImageURL)
	// Verify that the animation URL has &mode=episode appended
	assert.Equal(t, expectedAnimationURL, *result.AnimationURL)
	assert.Len(t, result.Artists, 1)
	assert.Equal(t, "Maya Artist", result.Artists[0].Name)
	expectedDID := domain.NewDID("0x1234567890123456789012345678901234567890", domain.ChainEthereumMainnet)
	assert.Equal(t, expectedDID, result.Artists[0].DID)
}

func TestEnhancer_Enhance_Objkt(t *testing.T) {
	mocks := setupTestEnhancer(t)
	defer tearDownTestEnhancer(mocks)

	tokenCID := domain.NewTokenCID(domain.ChainTezosMainnet, domain.StandardFA2, "KT1EfsNuqwLAWDd3o4pvfUx1CAh5GMdTrRvr", "224128")
	publisherName := registry.PublisherNameFXHash

	// Create normalized metadata with fxhash publisher (non-FeralFile Tezos)
	normalizedMeta := &metadata.NormalizedMetadata{
		Raw: map[string]interface{}{
			"name": "Test fxhash token",
		},
		Publisher: &metadata.Publisher{
			Name: &publisherName,
			URL:  stringPtr("https://fxhash.xyz"),
		},
	}

	// Mock objkt client to return token data
	name := "Anticyclone #224128"
	description := "A generative artwork"
	displayURI := "ipfs://QmDisplay123"
	artifactURI := "ipfs://QmArtifact456"
	mime := "image/png"
	alias := "Artist Name"

	objktToken := &objkt.Token{
		Name:        &name,
		Description: &description,
		DisplayURI:  &displayURI,
		ArtifactURI: &artifactURI,
		Mime:        &mime,
		Creators: []objkt.Creator{
			{
				Holder: objkt.Holder{
					Address: "tz1VSUr8wwNhLAzempoch5d6hLRiTh8Cjcjb",
					Alias:   &alias,
				},
			},
		},
	}

	mocks.objktClient.
		EXPECT().
		GetToken(gomock.Any(), "KT1EfsNuqwLAWDd3o4pvfUx1CAh5GMdTrRvr", "224128").
		Return(objktToken, nil)

	// Mock JSON marshal for token
	vendorJSON := []byte(`{"name":"Anticyclone #224128","description":"A generative artwork"}`)
	mocks.json.
		EXPECT().
		Marshal(objktToken).
		Return(vendorJSON, nil)

	result, err := mocks.enhancer.Enhance(context.Background(), tokenCID, normalizedMeta)

	assert.NoError(t, err)
	assert.NotNil(t, result)
	assert.Equal(t, schema.VendorObjkt, result.Vendor)
	assert.Equal(t, vendorJSON, result.VendorJSON)
	assert.NotNil(t, result.Name)
	assert.Equal(t, "Anticyclone #224128", *result.Name)
	assert.NotNil(t, result.Description)
	assert.Equal(t, "A generative artwork", *result.Description)
	assert.NotNil(t, result.ImageURL)
	assert.Equal(t, domain.UriToGateway(displayURI), *result.ImageURL)
	assert.NotNil(t, result.AnimationURL)
	assert.Equal(t, domain.UriToGateway(artifactURI), *result.AnimationURL)
	assert.NotNil(t, result.MimeType)
	assert.Equal(t, mime, *result.MimeType)
	assert.Len(t, result.Artists, 1)
	assert.Equal(t, "Artist Name", result.Artists[0].Name)
	expectedDID := domain.NewDID("tz1VSUr8wwNhLAzempoch5d6hLRiTh8Cjcjb", domain.ChainTezosMainnet)
	assert.Equal(t, expectedDID, result.Artists[0].DID)
}

func TestEnhancer_Enhance_Objkt_MinimalFields(t *testing.T) {
	mocks := setupTestEnhancer(t)
	defer tearDownTestEnhancer(mocks)

	tokenCID := domain.NewTokenCID(domain.ChainTezosMainnet, domain.StandardFA2, "KT1TestContract", "12345")
	publisherName := registry.PublisherName("some_tezos_publisher")

	// Create normalized metadata with generic Tezos publisher
	normalizedMeta := &metadata.NormalizedMetadata{
		Raw: map[string]interface{}{
			"name": "Test token",
		},
		Publisher: &metadata.Publisher{
			Name: &publisherName,
			URL:  stringPtr("https://example.com"),
		},
	}

	// Mock objkt client to return token with minimal fields
	name := "Minimal Token"
	objktToken := &objkt.Token{
		Name:        &name,
		Description: nil,
		DisplayURI:  nil,
		ArtifactURI: nil,
		Mime:        nil,
		Creators:    []objkt.Creator{},
	}

	mocks.objktClient.
		EXPECT().
		GetToken(gomock.Any(), "KT1TestContract", "12345").
		Return(objktToken, nil)

	// Mock JSON marshal for token
	vendorJSON := []byte(`{"name":"Minimal Token"}`)
	mocks.json.
		EXPECT().
		Marshal(objktToken).
		Return(vendorJSON, nil)

	result, err := mocks.enhancer.Enhance(context.Background(), tokenCID, normalizedMeta)

	assert.NoError(t, err)
	assert.NotNil(t, result)
	assert.Equal(t, schema.VendorObjkt, result.Vendor)
	assert.Equal(t, "Minimal Token", *result.Name)
	assert.Nil(t, result.Description)
	assert.Nil(t, result.ImageURL)
	assert.Nil(t, result.AnimationURL)
	assert.Nil(t, result.MimeType)
	assert.Empty(t, result.Artists)
}

func TestEnhancer_Enhance_Objkt_MultipleCreators(t *testing.T) {
	mocks := setupTestEnhancer(t)
	defer tearDownTestEnhancer(mocks)

	tokenCID := domain.NewTokenCID(domain.ChainTezosMainnet, domain.StandardFA2, "KT1CollabContract", "999")
	publisherName := registry.PublisherNameFXHash

	normalizedMeta := &metadata.NormalizedMetadata{
		Raw: map[string]interface{}{
			"name": "Collab Token",
		},
		Publisher: &metadata.Publisher{
			Name: &publisherName,
			URL:  stringPtr("https://fxhash.xyz"),
		},
	}

	// Mock objkt client to return token with multiple creators
	name := "Collaborative Work"
	artist1 := "Artist One"
	artist2 := "Artist Two"

	objktToken := &objkt.Token{
		Name: &name,
		Creators: []objkt.Creator{
			{
				Holder: objkt.Holder{
					Address: "tz1Artist1Address111111111111111111",
					Alias:   &artist1,
				},
			},
			{
				Holder: objkt.Holder{
					Address: "tz1Artist2Address222222222222222222",
					Alias:   &artist2,
				},
			},
		},
	}

	mocks.objktClient.
		EXPECT().
		GetToken(gomock.Any(), "KT1CollabContract", "999").
		Return(objktToken, nil)

	// Mock JSON marshal
	vendorJSON := []byte(`{"name":"Collaborative Work"}`)
	mocks.json.
		EXPECT().
		Marshal(objktToken).
		Return(vendorJSON, nil)

	result, err := mocks.enhancer.Enhance(context.Background(), tokenCID, normalizedMeta)

	assert.NoError(t, err)
	assert.NotNil(t, result)
	assert.Len(t, result.Artists, 2)
	assert.Equal(t, "Artist One", result.Artists[0].Name)
	assert.Equal(t, "Artist Two", result.Artists[1].Name)
	expectedDID1 := domain.NewDID("tz1Artist1Address111111111111111111", domain.ChainTezosMainnet)
	expectedDID2 := domain.NewDID("tz1Artist2Address222222222222222222", domain.ChainTezosMainnet)
	assert.Equal(t, expectedDID1, result.Artists[0].DID)
	assert.Equal(t, expectedDID2, result.Artists[1].DID)
}

func TestEnhancer_Enhance_Objkt_CreatorWithoutAlias(t *testing.T) {
	mocks := setupTestEnhancer(t)
	defer tearDownTestEnhancer(mocks)

	tokenCID := domain.NewTokenCID(domain.ChainTezosMainnet, domain.StandardFA2, "KT1TestContract", "555")
	publisherName := registry.PublisherNameFXHash

	normalizedMeta := &metadata.NormalizedMetadata{
		Raw: map[string]interface{}{
			"name": "Token without alias",
		},
		Publisher: &metadata.Publisher{
			Name: &publisherName,
			URL:  stringPtr("https://fxhash.xyz"),
		},
	}

	// Mock objkt client to return token with creator but no alias
	name := "Anonymous Work"
	objktToken := &objkt.Token{
		Name: &name,
		Creators: []objkt.Creator{
			{
				Holder: objkt.Holder{
					Address: "tz1AnonAddress111111111111111111111",
					Alias:   nil, // No alias
				},
			},
		},
	}

	mocks.objktClient.
		EXPECT().
		GetToken(gomock.Any(), "KT1TestContract", "555").
		Return(objktToken, nil)

	// Mock JSON marshal
	vendorJSON := []byte(`{"name":"Anonymous Work"}`)
	mocks.json.
		EXPECT().
		Marshal(objktToken).
		Return(vendorJSON, nil)

	result, err := mocks.enhancer.Enhance(context.Background(), tokenCID, normalizedMeta)

	assert.NoError(t, err)
	assert.NotNil(t, result)
	assert.Len(t, result.Artists, 1)
	assert.Equal(t, "", result.Artists[0].Name) // Empty name when no alias
	expectedDID := domain.NewDID("tz1AnonAddress111111111111111111111", domain.ChainTezosMainnet)
	assert.Equal(t, expectedDID, result.Artists[0].DID)
}

func TestEnhancer_Enhance_Objkt_InvalidTezosAddress(t *testing.T) {
	mocks := setupTestEnhancer(t)
	defer tearDownTestEnhancer(mocks)

	tokenCID := domain.NewTokenCID(domain.ChainTezosMainnet, domain.StandardFA2, "KT1TestContract", "777")
	publisherName := registry.PublisherNameFXHash

	normalizedMeta := &metadata.NormalizedMetadata{
		Raw: map[string]interface{}{
			"name": "Token with invalid address",
		},
		Publisher: &metadata.Publisher{
			Name: &publisherName,
			URL:  stringPtr("https://fxhash.xyz"),
		},
	}

	// Mock objkt client to return token with invalid Tezos address
	name := "Test Token"
	alias := "Some Artist"
	objktToken := &objkt.Token{
		Name: &name,
		Creators: []objkt.Creator{
			{
				Holder: objkt.Holder{
					Address: "invalid_address", // Invalid Tezos address
					Alias:   &alias,
				},
			},
		},
	}

	mocks.objktClient.
		EXPECT().
		GetToken(gomock.Any(), "KT1TestContract", "777").
		Return(objktToken, nil)

	// Mock JSON marshal
	vendorJSON := []byte(`{"name":"Test Token"}`)
	mocks.json.
		EXPECT().
		Marshal(objktToken).
		Return(vendorJSON, nil)

	result, err := mocks.enhancer.Enhance(context.Background(), tokenCID, normalizedMeta)

	assert.NoError(t, err)
	assert.NotNil(t, result)
	assert.Empty(t, result.Artists) // Invalid address should be skipped
}

func TestEnhancer_Enhance_Objkt_APIError(t *testing.T) {
	mocks := setupTestEnhancer(t)
	defer tearDownTestEnhancer(mocks)

	tokenCID := domain.NewTokenCID(domain.ChainTezosMainnet, domain.StandardFA2, "KT1ErrorContract", "123")
	publisherName := registry.PublisherNameFXHash

	normalizedMeta := &metadata.NormalizedMetadata{
		Raw: map[string]interface{}{
			"name": "Test token",
		},
		Publisher: &metadata.Publisher{
			Name: &publisherName,
			URL:  stringPtr("https://fxhash.xyz"),
		},
	}

	// Mock objkt client to return an error
	mocks.objktClient.
		EXPECT().
		GetToken(gomock.Any(), "KT1ErrorContract", "123").
		Return(nil, assert.AnError)

	result, err := mocks.enhancer.Enhance(context.Background(), tokenCID, normalizedMeta)

	assert.Error(t, err)
	assert.Nil(t, result)
	assert.Contains(t, err.Error(), "failed to enhance objkt metadata")
}

func TestEnhancer_Enhance_Objkt_MarshalError(t *testing.T) {
	mocks := setupTestEnhancer(t)
	defer tearDownTestEnhancer(mocks)

	tokenCID := domain.NewTokenCID(domain.ChainTezosMainnet, domain.StandardFA2, "KT1TestContract", "456")
	publisherName := registry.PublisherNameFXHash

	normalizedMeta := &metadata.NormalizedMetadata{
		Raw: map[string]interface{}{
			"name": "Test token",
		},
		Publisher: &metadata.Publisher{
			Name: &publisherName,
			URL:  stringPtr("https://fxhash.xyz"),
		},
	}

	// Mock objkt client to return token
	name := "Test Token"
	objktToken := &objkt.Token{
		Name: &name,
	}

	mocks.objktClient.
		EXPECT().
		GetToken(gomock.Any(), "KT1TestContract", "456").
		Return(objktToken, nil)

	// Mock JSON marshal to return an error
	mocks.json.
		EXPECT().
		Marshal(objktToken).
		Return(nil, assert.AnError)

	result, err := mocks.enhancer.Enhance(context.Background(), tokenCID, normalizedMeta)

	assert.Error(t, err)
	assert.Nil(t, result)
	assert.Contains(t, err.Error(), "failed to enhance objkt metadata")
}

func TestEnhancer_Enhance_Objkt_FeralFileNotAffected(t *testing.T) {
	mocks := setupTestEnhancer(t)
	defer tearDownTestEnhancer(mocks)

	// Test that Feral File tokens on Tezos still use Feral File API, not objkt
	tokenCID := domain.NewTokenCID(domain.ChainTezosMainnet, domain.StandardFA2, "KT1FeralFileContract", "12345")
	publisherName := registry.PublisherNameFeralFile

	normalizedMeta := &metadata.NormalizedMetadata{
		Raw: map[string]interface{}{
			"name": "Feral File Token",
		},
		Publisher: &metadata.Publisher{
			Name: &publisherName,
			URL:  stringPtr("https://feralfile.com"),
		},
	}

	// Mock Feral File client (NOT objkt client)
	artwork := &feralfile.Artwork{
		ID:           "12345",
		Name:         "Feral File Artwork",
		ThumbnailURI: "thumbnails/test.jpg",
		PreviewURI:   "previews/test.jpg",
		Series: feralfile.Series{
			Medium:      "image",
			Description: "Test description",
			Artist: feralfile.Artist{
				AlumniAccount: feralfile.AlumniAccount{
					Alias: "Test Artist",
					Addresses: map[string]string{
						"tezos": "tz1FeralFileArtist111111111111111",
					},
				},
			},
		},
	}

	mocks.feralfileClient.
		EXPECT().
		GetArtwork(gomock.Any(), "12345").
		Return(artwork, nil)

	vendorJSON := []byte(`{"id":"12345","name":"Feral File Artwork"}`)
	mocks.json.
		EXPECT().
		Marshal(artwork).
		Return(vendorJSON, nil)

	expectedImageURL := "https://cdn.feralfileassets.com/previews/test.jpg"
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
	assert.Equal(t, schema.VendorFeralFile, result.Vendor) // Should be FeralFile, NOT objkt
}

func TestEnhancer_Enhance_Objkt_NonTezosChain(t *testing.T) {
	mocks := setupTestEnhancer(t)
	defer tearDownTestEnhancer(mocks)

	// objkt should only work for Tezos, so Ethereum should return nil
	tokenCID := domain.NewTokenCID(domain.ChainEthereumMainnet, domain.StandardERC721, "0x123", "1")
	publisherName := registry.PublisherName("unknown_publisher")

	normalizedMeta := &metadata.NormalizedMetadata{
		Raw: map[string]interface{}{
			"name": "Ethereum token",
		},
		Publisher: &metadata.Publisher{
			Name: &publisherName,
			URL:  stringPtr("https://example.com"),
		},
	}

	// No mocks should be called since objkt enhancement is skipped for non-Tezos chains

	result, err := mocks.enhancer.Enhance(context.Background(), tokenCID, normalizedMeta)

	assert.NoError(t, err)
	assert.Nil(t, result) // Should return nil for non-Tezos chains
}

// Helper function to create string pointers
func stringPtr(s string) *string {
	return &s
}
