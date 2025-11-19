package feralfile_test

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/feral-file/ff-indexer-v2/internal/adapter"
	"github.com/feral-file/ff-indexer-v2/internal/logger"
	"github.com/feral-file/ff-indexer-v2/internal/mocks"
	"github.com/feral-file/ff-indexer-v2/internal/providers/vendors/feralfile"
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

// TestClient_GetArtwork_Success tests successful artwork retrieval with mock
func TestClient_GetArtwork_Success(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockHTTPClient := mocks.NewMockHTTPClient(ctrl)
	client := feralfile.NewClient(mockHTTPClient, "https://feralfile.com/api")

	ctx := context.Background()
	tokenID := "68133196527112232794835997367314869505960984666033462681082934679485439444096"

	expectedResponse := feralfile.ArtworkResponse{
		Result: feralfile.Artwork{
			ID:           tokenID,
			Name:         "Test Artwork",
			ThumbnailURI: "/thumb.jpg",
			PreviewURI:   "/preview.jpg",
			Series: feralfile.Series{
				Title:       "Test Series",
				Description: "Test Description",
				Medium:      "Digital",
				Artist: feralfile.Artist{
					ID: "artist-123",
					AlumniAccount: feralfile.AlumniAccount{
						ID:    "alumni-123",
						Alias: "test-artist",
						Addresses: map[string]string{
							"ethereum": "0x1234567890123456789012345678901234567890",
							"tezos":    "tz1VSUr8wwNhLAzempoch5d6hLRiTh8Cjcjb",
						},
					},
				},
			},
		},
	}

	expectedURL := "https://feralfile.com/api/artworks/" + tokenID + "?includeArtist=true"

	mockHTTPClient.EXPECT().
		Get(ctx, expectedURL, gomock.Any()).
		DoAndReturn(func(ctx context.Context, url string, result interface{}) error {
			// Marshal and unmarshal to simulate real API behavior
			data, _ := json.Marshal(expectedResponse)
			return json.Unmarshal(data, result)
		}).
		Times(1)

	artwork, err := client.GetArtwork(ctx, tokenID)

	require.NoError(t, err)
	assert.NotNil(t, artwork)
	assert.Equal(t, tokenID, artwork.ID)
	assert.Equal(t, "Test Artwork", artwork.Name)
	assert.Equal(t, "/thumb.jpg", artwork.ThumbnailURI)
	assert.Equal(t, "/preview.jpg", artwork.PreviewURI)
	assert.Equal(t, "Test Series", artwork.Series.Title)
	assert.Equal(t, "artist-123", artwork.Series.Artist.ID)
	assert.Equal(t, "test-artist", artwork.Series.Artist.AlumniAccount.Alias)
	assert.Len(t, artwork.Series.Artist.AlumniAccount.Addresses, 2)
}

// TestClient_GetArtwork_HTTPError tests error handling when HTTP client returns an error
func TestClient_GetArtwork_HTTPError(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockHTTPClient := mocks.NewMockHTTPClient(ctrl)
	client := feralfile.NewClient(mockHTTPClient, "https://feralfile.com/api")

	ctx := context.Background()
	tokenID := "68133196527112232794835997367314869505960984666033462681082934679485439444096"
	expectedURL := "https://feralfile.com/api/artworks/" + tokenID + "?includeArtist=true"

	expectedError := errors.New("network error")

	mockHTTPClient.EXPECT().
		Get(ctx, expectedURL, gomock.Any()).
		Return(expectedError).
		Times(1)

	artwork, err := client.GetArtwork(ctx, tokenID)

	assert.Error(t, err)
	assert.Nil(t, artwork)
	assert.Contains(t, err.Error(), "failed to call Feral File API")
	assert.Contains(t, err.Error(), "network error")
}

// TestClient_GetArtwork_InvalidJSON tests error handling when API returns invalid JSON
func TestClient_GetArtwork_InvalidJSON(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockHTTPClient := mocks.NewMockHTTPClient(ctrl)
	client := feralfile.NewClient(mockHTTPClient, "https://feralfile.com/api")

	ctx := context.Background()
	tokenID := "68133196527112232794835997367314869505960984666033462681082934679485439444096"
	expectedURL := "https://feralfile.com/api/artworks/" + tokenID + "?includeArtist=true"

	mockHTTPClient.EXPECT().
		Get(ctx, expectedURL, gomock.Any()).
		DoAndReturn(func(ctx context.Context, url string, result interface{}) error {
			// Simulate invalid JSON by trying to unmarshal invalid data
			return json.Unmarshal([]byte("invalid json"), result)
		}).
		Times(1)

	artwork, err := client.GetArtwork(ctx, tokenID)

	assert.Error(t, err)
	assert.Nil(t, artwork)
	assert.Contains(t, err.Error(), "failed to call Feral File API")
}

// TestClient_GetArtwork_MissingResult tests error handling when API response is missing result field
func TestClient_GetArtwork_MissingResult(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockHTTPClient := mocks.NewMockHTTPClient(ctrl)
	client := feralfile.NewClient(mockHTTPClient, "https://feralfile.com/api")

	ctx := context.Background()
	tokenID := "68133196527112232794835997367314869505960984666033462681082934679485439444096"
	expectedURL := "https://feralfile.com/api/artworks/" + tokenID + "?includeArtist=true"

	// Response without result field
	invalidResponse := map[string]interface{}{
		"error": "not found",
	}

	mockHTTPClient.EXPECT().
		Get(ctx, expectedURL, gomock.Any()).
		DoAndReturn(func(ctx context.Context, url string, result interface{}) error {
			data, _ := json.Marshal(invalidResponse)
			return json.Unmarshal(data, result)
		}).
		Times(1)

	artwork, err := client.GetArtwork(ctx, tokenID)

	// This should succeed but artwork.Result will be empty
	// The actual validation depends on how the API behaves
	// We'll test that it doesn't panic
	assert.NoError(t, err)
	assert.NotNil(t, artwork)
	assert.Empty(t, artwork.ID)
	assert.Empty(t, artwork.Name)
}

// TestClient_GetArtwork_EmptyResponse tests handling of empty response
func TestClient_GetArtwork_EmptyResponse(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockHTTPClient := mocks.NewMockHTTPClient(ctrl)
	client := feralfile.NewClient(mockHTTPClient, "https://feralfile.com/api")

	ctx := context.Background()
	tokenID := "68133196527112232794835997367314869505960984666033462681082934679485439444096"
	expectedURL := "https://feralfile.com/api/artworks/" + tokenID + "?includeArtist=true"

	emptyResponse := feralfile.ArtworkResponse{
		Result: feralfile.Artwork{},
	}

	mockHTTPClient.EXPECT().
		Get(ctx, expectedURL, gomock.Any()).
		DoAndReturn(func(ctx context.Context, url string, result interface{}) error {
			data, _ := json.Marshal(emptyResponse)
			return json.Unmarshal(data, result)
		}).
		Times(1)

	artwork, err := client.GetArtwork(ctx, tokenID)

	assert.NoError(t, err)
	assert.NotNil(t, artwork)
	assert.Empty(t, artwork.ID)
	assert.Empty(t, artwork.Name)
}

// TestClient_GetArtwork_URLFormat tests that the URL is constructed correctly
func TestClient_GetArtwork_URLFormat(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockHTTPClient := mocks.NewMockHTTPClient(ctrl)
	apiBaseURL := "https://feralfile.com/api"
	client := feralfile.NewClient(mockHTTPClient, apiBaseURL)

	ctx := context.Background()
	tokenID := "12345"

	expectedURL := apiBaseURL + "/artworks/" + tokenID + "?includeArtist=true"

	mockHTTPClient.EXPECT().
		Get(ctx, expectedURL, gomock.Any()).
		DoAndReturn(func(ctx context.Context, url string, result interface{}) error {
			response := feralfile.ArtworkResponse{
				Result: feralfile.Artwork{
					ID: tokenID,
				},
			}
			data, _ := json.Marshal(response)
			return json.Unmarshal(data, result)
		}).
		Times(1)

	_, err := client.GetArtwork(ctx, tokenID)
	assert.NoError(t, err)
}

// TestArtwork_CanonicalName tests the CanonicalName method
func TestArtwork_CanonicalName(t *testing.T) {
	tests := []struct {
		name     string
		artwork  feralfile.Artwork
		expected string
	}{
		{
			name: "name starting with #",
			artwork: feralfile.Artwork{
				Name: "#123",
				Series: feralfile.Series{
					Title: "Test Series",
				},
			},
			expected: "Test Series #123",
		},
		{
			name: "name starting with AE",
			artwork: feralfile.Artwork{
				Name: "AE001",
				Series: feralfile.Series{
					Title: "Artist Edition",
				},
			},
			expected: "AE001",
		},
		{
			name: "name is AE",
			artwork: feralfile.Artwork{
				Name: "AE",
				Series: feralfile.Series{
					Title: "Artist Edition",
				},
			},
			expected: "Artist Edition AE",
		},
		{
			name: "name starting with AP",
			artwork: feralfile.Artwork{
				Name: "AP001",
				Series: feralfile.Series{
					Title: "Artist Proof",
				},
			},
			expected: "AP001",
		},
		{
			name: "name is AP",
			artwork: feralfile.Artwork{
				Name: "AP",
				Series: feralfile.Series{
					Title: "Artist Proof",
				},
			},
			expected: "Artist Proof AP",
		},
		{
			name: "name starting with PP",
			artwork: feralfile.Artwork{
				Name: "PP001",
				Series: feralfile.Series{
					Title: "Printer Proof",
				},
			},
			expected: "PP001",
		},
		{
			name: "name is PP",
			artwork: feralfile.Artwork{
				Name: "PP",
				Series: feralfile.Series{
					Title: "Printer Proof",
				},
			},
			expected: "Printer Proof PP",
		},
		{
			name: "regular name without prefix",
			artwork: feralfile.Artwork{
				Name: "Regular Artwork Name",
				Series: feralfile.Series{
					Title: "Test Series",
				},
			},
			expected: "Regular Artwork Name",
		},
		{
			name: "name with lowercase prefix should not match",
			artwork: feralfile.Artwork{
				Name: "ae001",
				Series: feralfile.Series{
					Title: "Test Series",
				},
			},
			expected: "ae001",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.artwork.CanonicalName()
			assert.Equal(t, tt.expected, result)
		})
	}
}

// TestURL tests the URL helper function
func TestURL(t *testing.T) {
	tests := []struct {
		name     string
		uri      string
		expected string
	}{
		{
			name:     "relative URI",
			uri:      "thumbnails/123.jpg",
			expected: "https://cdn.feralfileassets.com/thumbnails/123.jpg",
		},
		{
			name:     "absolute HTTP URL",
			uri:      "http://example.com/image.jpg",
			expected: "http://example.com/image.jpg",
		},
		{
			name:     "absolute HTTPS URL",
			uri:      "https://example.com/image.jpg",
			expected: "https://example.com/image.jpg",
		},
		{
			name:     "empty URI",
			uri:      "",
			expected: "",
		},
		{
			name:     "URI with leading slash",
			uri:      "/thumbnails/123.jpg",
			expected: "https://cdn.feralfileassets.com/thumbnails/123.jpg", // Note: double slash is current behavior
		},
		{
			name:     "IPFS URI",
			uri:      "ipfs://QmHash",
			expected: "ipfs://QmHash",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := feralfile.URL(tt.uri)
			assert.Equal(t, tt.expected, result)
		})
	}
}

// Integration tests - these test against the real Feral File API

func TestClient_GetArtwork_Integration(t *testing.T) {
	httpClient := adapter.NewHTTPClient(30 * time.Second)
	client := feralfile.NewClient(httpClient, feralfile.API_ENDPOINT)

	ctx := context.Background()

	// Test artwork IDs provided by the user
	artworkIDs := []string{
		"68133196527112232794835997367314869505960984666033462681082934679485439444096",
		"54927077953071573898060197382410853987230099039252111790486496240282061669504",
		"2d9351003e8f279b4c553ad60b4f3340dd295cf81206959f16c0efd231ec1811",
		"49496415076657029545097811331011115026006736793184670775359718991147383068624",
	}

	for _, tokenID := range artworkIDs {
		t.Run("tokenID_"+tokenID, func(t *testing.T) {
			artwork, err := client.GetArtwork(ctx, tokenID)

			require.NoError(t, err, "Failed to fetch artwork for tokenID: %s", tokenID)
			require.NotNil(t, artwork, "Artwork should not be nil for tokenID: %s", tokenID)

			// Validate response structure
			assert.NotEmpty(t, artwork.ID, "Artwork ID should not be empty")
			assert.NotEmpty(t, artwork.Name, "Artwork name should not be empty")
			assert.NotEmpty(t, artwork.Series.Title, "Series title should not be empty")
			assert.NotEmpty(t, artwork.Series.Medium, "Series medium should not be empty")
			assert.NotEmpty(t, artwork.Series.Description, "Series description should not be empty")
			assert.NotEmpty(t, artwork.Series.Artist.ID, "Artist ID should not be empty")
			assert.NotNil(t, artwork.Series.Artist.AlumniAccount, "AlumniAccount should not be nil")
			assert.NotEmpty(t, artwork.Series.Artist.AlumniAccount.ID, "AlumniAccount ID should not be empty")
			assert.NotEmpty(t, artwork.Series.Artist.AlumniAccount.Alias, "AlumniAccount alias should not be empty")
			assert.NotEmpty(t, artwork.Series.Artist.AlumniAccount.Addresses, "AlumniAccount addresses should not be empty")
			for chain, address := range artwork.Series.Artist.AlumniAccount.Addresses {
				if chain != "tezos" && chain != "ethereum" {
					t.Logf("Unsupported chain: %s", chain)
					t.FailNow()
				}
				assert.NotEmpty(t, address, "Address should not be empty")
			}

			// Test CanonicalName method with real data
			canonicalName := artwork.CanonicalName()
			assert.NotEmpty(t, canonicalName, "CanonicalName should not be empty")

			// Test URL helper with real URIs
			if artwork.ThumbnailURI != "" {
				thumbnailURL := feralfile.URL(artwork.ThumbnailURI)
				assert.NotEmpty(t, thumbnailURL, "Thumbnail URL should not be empty")
			}

			if artwork.PreviewURI != "" {
				previewURL := feralfile.URL(artwork.PreviewURI)
				assert.NotEmpty(t, previewURL, "Preview URL should not be empty")
			}

			// Log some details for debugging
			t.Logf("Artwork ID: %s", artwork.ID)
			t.Logf("Artwork Name: %s", artwork.Name)
			t.Logf("Canonical Name: %s", canonicalName)
			t.Logf("Series: %s", artwork.Series.Title)
			t.Logf("Artist ID: %s", artwork.Series.Artist.ID)
		})
	}
}

// TestClient_GetArtwork_Integration_InvalidID tests error handling with invalid artwork ID
func TestClient_GetArtwork_Integration_InvalidID(t *testing.T) {
	httpClient := adapter.NewHTTPClient(30 * time.Second)
	client := feralfile.NewClient(httpClient, feralfile.API_ENDPOINT)

	ctx := context.Background()
	invalidTokenID := "99999999999999999999999999999999999999999999999999999999999999999999999999999"

	_, err := client.GetArtwork(ctx, invalidTokenID)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to call Feral File API")
	assert.Contains(t, err.Error(), "artwork not found")
}

// TestClient_GetArtwork_Integration_ContextCancellation tests context cancellation
func TestClient_GetArtwork_Integration_ContextCancellation(t *testing.T) {
	httpClient := adapter.NewHTTPClient(30 * time.Second)
	client := feralfile.NewClient(httpClient, feralfile.API_ENDPOINT)

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately

	tokenID := "68133196527112232794835997367314869505960984666033462681082934679485439444096"

	artwork, err := client.GetArtwork(ctx, tokenID)

	// Should return an error due to context cancellation
	assert.Error(t, err)
	assert.Nil(t, artwork)
}
