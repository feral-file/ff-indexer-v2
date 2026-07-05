package feralfile_test

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

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
				Settings: feralfile.SeriesSettings{
					MaxArtwork: 75,
				},
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
		GetAndUnmarshal(ctx, expectedURL, gomock.Any()).
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
	assert.Equal(t, int64(75), artwork.Series.Settings.MaxArtwork)
}

// TestClient_GetArtwork_ParsesSeriesSettingsMaxArtwork verifies series.settings.maxArtwork
// is unmarshaled from the API response (regression guard for release total_mints sourcing).
func TestClient_GetArtwork_ParsesSeriesSettingsMaxArtwork(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockHTTPClient := mocks.NewMockHTTPClient(ctrl)
	client := feralfile.NewClient(mockHTTPClient, "https://feralfile.com/api")

	ctx := context.Background()
	tokenID := "68133196527112232794835997367314869505960984666033462681082934679485439444096"
	expectedURL := "https://feralfile.com/api/artworks/" + tokenID + "?includeArtist=true"

	mockHTTPClient.EXPECT().
		GetAndUnmarshal(ctx, expectedURL, gomock.Any()).
		DoAndReturn(func(ctx context.Context, url string, result interface{}) error {
			raw := []byte(`{
				"result": {
					"id": "` + tokenID + `",
					"name": "#1",
					"seriesID": "1f060e42-0000-0000-0000-000000000001",
					"index": 0,
					"series": {
						"title": "1DE94",
						"medium": "image",
						"settings": { "maxArtwork": 75 },
						"artist": {
							"alumniAccount": {
								"alias": "Raven Kwok",
								"addresses": { "ethereum": "0xabc" }
							}
						}
					}
				}
			}`)
			return json.Unmarshal(raw, result)
		}).
		Times(1)

	artwork, err := client.GetArtwork(ctx, tokenID)

	require.NoError(t, err)
	require.NotNil(t, artwork)
	assert.Equal(t, "1DE94", artwork.Series.Title)
	assert.Equal(t, int64(75), artwork.Series.Settings.MaxArtwork)
	assert.Equal(t, "Raven Kwok", artwork.Series.Artist.AlumniAccount.Alias)
}

// TestSeriesSettings_UnmarshalMissingMaxArtwork verifies absent maxArtwork defaults to zero
// without breaking artwork unmarshaling.
func TestSeriesSettings_UnmarshalMissingMaxArtwork(t *testing.T) {
	t.Parallel()

	var artwork feralfile.Artwork
	err := json.Unmarshal([]byte(`{
		"series": {
			"title": "Untitled Series",
			"settings": {}
		}
	}`), &artwork)
	require.NoError(t, err)
	assert.Equal(t, int64(0), artwork.Series.Settings.MaxArtwork)
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
		GetAndUnmarshal(ctx, expectedURL, gomock.Any()).
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
		GetAndUnmarshal(ctx, expectedURL, gomock.Any()).
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
		GetAndUnmarshal(ctx, expectedURL, gomock.Any()).
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
		GetAndUnmarshal(ctx, expectedURL, gomock.Any()).
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
		GetAndUnmarshal(ctx, expectedURL, gomock.Any()).
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
func TestArtwork_SeriesIDOrFallback(t *testing.T) {
	t.Parallel()

	t.Run("prefers top-level seriesID", func(t *testing.T) {
		artwork := feralfile.Artwork{
			SeriesID: "series-top",
			Series: feralfile.Series{
				ID: "series-nested",
			},
		}
		assert.Equal(t, "series-top", artwork.SeriesIDOrFallback())
	})

	t.Run("falls back to nested series ID", func(t *testing.T) {
		artwork := feralfile.Artwork{
			Series: feralfile.Series{
				ID: "series-nested",
			},
		}
		assert.Equal(t, "series-nested", artwork.SeriesIDOrFallback())
	})
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

// TestGetSeriesArtworks_Success verifies that artworks are parsed and returned correctly.
func TestGetSeriesArtworks_Success(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockHTTPClient := mocks.NewMockHTTPClient(ctrl)
	client := feralfile.NewClient(mockHTTPClient, "https://feralfile.com/api")

	mockHTTPClient.EXPECT().
		GetAndUnmarshal(gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, _ string, result interface{}) error {
			return json.Unmarshal([]byte(`{
				"result": [
					{"index":0,"chain":"ethereum","contractAddress":"0xabc","tokenID":"1"},
					{"index":1,"chain":"tezos","contractAddress":"KT1xyz","tokenID":"2"}
				]
			}`), result)
		}).
		Times(1)

	refs, err := client.GetSeriesArtworks(context.Background(), "series-uuid", 1, 2)

	require.NoError(t, err)
	require.Len(t, refs, 2)
	require.NotNil(t, refs[0].Index)
	assert.Equal(t, int64(0), *refs[0].Index)
	assert.Equal(t, "ethereum", refs[0].Chain)
	assert.Equal(t, "0xabc", refs[0].ContractAddress)
	assert.Equal(t, "1", refs[0].TokenID)
	require.NotNil(t, refs[1].Index)
	assert.Equal(t, int64(1), *refs[1].Index)
	assert.Equal(t, "tezos", refs[1].Chain)
}

// TestGetSeriesArtworks_NilIndex verifies that an artwork whose "index" field is absent
// in the JSON is represented as a nil pointer in ArtworkRef, not coerced to 0.
func TestGetSeriesArtworks_NilIndex(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockHTTPClient := mocks.NewMockHTTPClient(ctrl)
	client := feralfile.NewClient(mockHTTPClient, "https://feralfile.com/api")

	mockHTTPClient.EXPECT().
		GetAndUnmarshal(gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, _ string, result interface{}) error {
			// "index" is intentionally absent to simulate an unresolved release membership.
			return json.Unmarshal([]byte(`{
				"result": [
					{"chain":"ethereum","contractAddress":"0xabc","tokenID":"1"}
				]
			}`), result)
		}).
		Times(1)

	refs, err := client.GetSeriesArtworks(context.Background(), "series-uuid", 1, 1)

	require.NoError(t, err)
	require.Len(t, refs, 1)
	assert.Nil(t, refs[0].Index, "absent 'index' in JSON must produce a nil pointer, not 0")
	assert.Equal(t, "ethereum", refs[0].Chain)
}

// TestGetSeriesArtworks_EmptyResult verifies that an empty API result returns an empty slice.
func TestGetSeriesArtworks_EmptyResult(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockHTTPClient := mocks.NewMockHTTPClient(ctrl)
	client := feralfile.NewClient(mockHTTPClient, "https://feralfile.com/api")

	mockHTTPClient.EXPECT().
		GetAndUnmarshal(gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, _ string, result interface{}) error {
			return json.Unmarshal([]byte(`{"result":[]}`), result)
		}).
		Times(1)

	refs, err := client.GetSeriesArtworks(context.Background(), "series-uuid", 1, 10)

	require.NoError(t, err)
	assert.Empty(t, refs)
}

// TestGetSeriesArtworks_HTTPError verifies that HTTP errors are propagated.
func TestGetSeriesArtworks_HTTPError(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockHTTPClient := mocks.NewMockHTTPClient(ctrl)
	client := feralfile.NewClient(mockHTTPClient, "https://feralfile.com/api")

	mockHTTPClient.EXPECT().
		GetAndUnmarshal(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(errors.New("connection refused")).
		Times(1)

	refs, err := client.GetSeriesArtworks(context.Background(), "series-uuid", 1, 10)

	assert.Error(t, err)
	assert.Nil(t, refs)
	assert.Contains(t, err.Error(), "failed to call Feral File artworks API")
}

// TestGetSeriesArtworks_URLContainsSeriesAndOffset verifies that the correct API URL is
// constructed with the seriesID, sortBy, sortOrder, offset, and limit parameters.
func TestGetSeriesArtworks_URLContainsSeriesAndOffset(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockHTTPClient := mocks.NewMockHTTPClient(ctrl)
	client := feralfile.NewClient(mockHTTPClient, "https://feralfile.com/api")

	var capturedURL string
	mockHTTPClient.EXPECT().
		GetAndUnmarshal(gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, url string, result interface{}) error {
			capturedURL = url
			return json.Unmarshal([]byte(`{"result":[]}`), result)
		}).
		Times(1)

	_, _ = client.GetSeriesArtworks(context.Background(), "abc-123", 3, 5)

	// mintFrom=3 → offset=(3-1)=2; range=3 → limit=3
	assert.Contains(t, capturedURL, "seriesID=abc-123")
	assert.Contains(t, capturedURL, "sortBy=index")
	assert.Contains(t, capturedURL, "sortOrder=ASC")
	assert.Contains(t, capturedURL, "offset=2")
	assert.Contains(t, capturedURL, "limit=3")
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
		{
			name:     "Cloudflare Images URI",
			uri:      "https://imagedelivery.net/account/image",
			expected: "https://imagedelivery.net/account/image/xl",
		},
		{
			name:     "Cloudflare Images URI with variant",
			uri:      "https://imagedelivery.net/account/image/variant",
			expected: "https://imagedelivery.net/account/image/variant",
		},
		{
			name:     "Cloudflare Stream URI",
			uri:      "https://example.com/stream/1234567890",
			expected: "https://example.com/stream/1234567890",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := feralfile.URL(tt.uri)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestResolveSlug_Success(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockHTTPClient := mocks.NewMockHTTPClient(ctrl)
	client := feralfile.NewClient(mockHTTPClient, "https://feralfile.com/api")

	ctx := context.Background()
	expectedUUID := "series-uuid-123"

	mockHTTPClient.EXPECT().
		GetAndUnmarshal(ctx, "https://feralfile.com/api/series?slug=data-pilgrims-01-769&limit=1", gomock.Any()).
		DoAndReturn(func(_ context.Context, _ string, out interface{}) error {
			data := []byte(`{"result":[{"id":"series-uuid-123","slug":"data-pilgrims-01-769"}]}`)
			return json.Unmarshal(data, out)
		})

	id, err := client.ResolveSlug(ctx, "data-pilgrims-01-769")
	require.NoError(t, err)
	assert.Equal(t, expectedUUID, id)
}

func TestResolveSlug_NotFound(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockHTTPClient := mocks.NewMockHTTPClient(ctrl)
	client := feralfile.NewClient(mockHTTPClient, "https://feralfile.com/api")

	ctx := context.Background()

	mockHTTPClient.EXPECT().
		GetAndUnmarshal(ctx, "https://feralfile.com/api/series?slug=no-such-series&limit=1", gomock.Any()).
		DoAndReturn(func(_ context.Context, _ string, out interface{}) error {
			data := []byte(`{"result":[]}`)
			return json.Unmarshal(data, out)
		})

	_, err := client.ResolveSlug(ctx, "no-such-series")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "slug not found")
}

// TestGetSeriesArtworks_ReservedCharsInSeriesIDEncoded verifies that reserved URL characters
// in seriesID (e.g. &, =, %) are percent-encoded in the outbound request. Without encoding,
// a seriesID like "abc&foo=bar" would append extra query parameters to the Feral File URL.
func TestGetSeriesArtworks_ReservedCharsInSeriesIDEncoded(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockHTTPClient := mocks.NewMockHTTPClient(ctrl)
	client := feralfile.NewClient(mockHTTPClient, "https://feralfile.com/api")
	ctx := context.Background()

	// seriesID with reserved characters that must not alter the query string.
	seriesID := "abc&foo=bar"

	var capturedURL string
	mockHTTPClient.EXPECT().
		GetAndUnmarshal(gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, u string, out interface{}) error {
			capturedURL = u
			// Return an empty result to stop pagination.
			return json.Unmarshal([]byte(`{"result":[]}`), out)
		})

	_, err := client.GetSeriesArtworks(ctx, seriesID, 1, 1)
	require.NoError(t, err)

	// The raw seriesID must not appear in the URL; the encoded form must.
	assert.NotContains(t, capturedURL, "abc&foo=bar",
		"raw seriesID must not appear in URL; reserved chars must be encoded")
	assert.Contains(t, capturedURL, "abc%26foo%3Dbar",
		"seriesID must be percent-encoded in the URL")
}
