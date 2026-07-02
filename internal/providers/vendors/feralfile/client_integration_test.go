//go:build integration

package feralfile_test

// Integration tests for the feralfile client against the real Feral File API.
// Gated behind //go:build integration; excluded from plain go test ./... but
// included in CI and make test-integration via -tags=integration.
//
// Run with:
//
//	go test -tags=integration ./internal/providers/vendors/feralfile/...

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/feral-file/ff-indexer-v2/internal/adapter"
	"github.com/feral-file/ff-indexer-v2/internal/providers/vendors/feralfile"
)

// TestClient_GetArtwork_Integration fetches several known artworks and validates
// all release-relevant fields are present and well-formed.
func TestClient_GetArtwork_Integration(t *testing.T) {
	httpClient := adapter.NewHTTPClient(30 * time.Second)
	client := feralfile.NewClient(httpClient, feralfile.API_ENDPOINT)

	ctx := context.Background()

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

			canonicalName := artwork.CanonicalName()
			assert.NotEmpty(t, canonicalName, "CanonicalName should not be empty")

			if artwork.ThumbnailURI != "" {
				assert.NotEmpty(t, feralfile.URL(artwork.ThumbnailURI), "Thumbnail URL should not be empty")
			}
			if artwork.PreviewURI != "" {
				assert.NotEmpty(t, feralfile.URL(artwork.PreviewURI), "Preview URL should not be empty")
			}

			t.Logf("Artwork ID: %s", artwork.ID)
			t.Logf("Artwork Name: %s", artwork.Name)
			t.Logf("Canonical Name: %s", canonicalName)
			t.Logf("Series: %s", artwork.Series.Title)
			t.Logf("Artist ID: %s", artwork.Series.Artist.ID)
		})
	}
}

// TestClient_GetArtwork_Integration_InvalidID verifies that a non-existent artwork ID
// returns a recognizable error rather than a nil result.
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

// TestClient_GetArtwork_Integration_ContextCancellation verifies that a canceled context
// propagates as an error rather than hanging.
func TestClient_GetArtwork_Integration_ContextCancellation(t *testing.T) {
	httpClient := adapter.NewHTTPClient(30 * time.Second)
	client := feralfile.NewClient(httpClient, feralfile.API_ENDPOINT)

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancel immediately

	tokenID := "68133196527112232794835997367314869505960984666033462681082934679485439444096"

	artwork, err := client.GetArtwork(ctx, tokenID)

	assert.Error(t, err)
	assert.Nil(t, artwork)
}
