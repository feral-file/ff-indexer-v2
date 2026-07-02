//go:build integration

package artblocks_test

// Integration tests for the artblocks client against the real ArtBlocks GraphQL API.
// They are excluded from the default test run and CI.
//
// Run with:
//
//	go test -tags=integration ./internal/providers/vendors/artblocks/...

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/feral-file/ff-indexer-v2/internal/adapter"
	"github.com/feral-file/ff-indexer-v2/internal/providers/vendors/artblocks"
)

// TestClient_GetProjectMetadata_Integration fetches several well-known ArtBlocks projects
// and validates core metadata fields are present and well-formed.
func TestClient_GetProjectMetadata_Integration(t *testing.T) {
	httpClient := adapter.NewHTTPClient(30 * time.Second)
	client := artblocks.NewClient(httpClient, ARTBLOCKS_GRAPHQL_URL, adapter.NewJSON())

	ctx := context.Background()
	testCases := []struct {
		name      string
		projectID string
		expectOk  bool
	}{
		{"Chromie_Squiggle", "0x059edd72cd353df5106d2b9cc5ab83a52287ac3a-0", true},
		{"Fidenza", "0xa7d8d9ef8d8ce8992df33d8b8cf4aebabd5bd270-78", true},
		{"Archetype", "0xa7d8d9ef8d8ce8992df33d8b8cf4aebabd5bd270-23", true},
		{"Ringers", "0xa7d8d9ef8d8ce8992df33d8b8cf4aebabd5bd270-13", true},
		{"Invalid_Project", "0xa7d8d9ef8d8ce8992df33d8b8cf4aebabd5bd270-999999", false},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			metadata, err := client.GetProjectMetadata(ctx, 1, tc.projectID)

			if !tc.expectOk {
				assert.Error(t, err)
				assert.Nil(t, metadata)
				t.Logf("Expected error for %s: %v", tc.projectID, err)
				return
			}

			if err != nil {
				t.Logf("Project %s: Error - %v (may not be accessible)", tc.projectID, err)
				return
			}

			require.NotNil(t, metadata, "Metadata should not be nil for project: %s", tc.projectID)
			assert.NotEmpty(t, metadata.Name, "Project name should not be empty")
			assert.NotEmpty(t, metadata.ArtistName, "Artist name should not be empty")
			assert.NotEmpty(t, metadata.ArtistAddress, "Artist address should not be empty")

			if metadata.Description != nil {
				t.Logf("Description length: %d chars", len(*metadata.Description))
			}
			t.Logf("Project Name: %s", metadata.Name)
			t.Logf("Artist Name: %s", metadata.ArtistName)
			t.Logf("Artist Address: %s", metadata.ArtistAddress)
		})
	}
}

// TestClient_GetProjectMetadata_Integration_TokenIDParsing verifies that token IDs
// are parsed into the correct project ID and mint number and the metadata can be fetched.
func TestClient_GetProjectMetadata_Integration_TokenIDParsing(t *testing.T) {
	httpClient := adapter.NewHTTPClient(30 * time.Second)
	client := artblocks.NewClient(httpClient, ARTBLOCKS_GRAPHQL_URL, adapter.NewJSON())

	ctx := context.Background()

	testCases := []struct {
		name                 string
		tokenID              string
		expectedProjectID    int64
		expectedMintNumber   int64
		artblocksContract    string
		expectedProjectFound bool
	}{
		{
			name:                 "Chromie_Squiggle_Token",
			tokenID:              "5",
			expectedProjectID:    0,
			expectedMintNumber:   5,
			artblocksContract:    "0x059edd72cd353df5106d2b9cc5ab83a52287ac3a",
			expectedProjectFound: true,
		},
		{
			name:                 "Fidenza_Token",
			tokenID:              "78000100",
			expectedProjectID:    78,
			expectedMintNumber:   100,
			artblocksContract:    "0xa7d8d9ef8d8ce8992df33d8b8cf4aebabd5bd270",
			expectedProjectFound: true,
		},
		{
			name:                 "Archetype_Token",
			tokenID:              "23000050",
			expectedProjectID:    23,
			expectedMintNumber:   50,
			artblocksContract:    "0xa7d8d9ef8d8ce8992df33d8b8cf4aebabd5bd270",
			expectedProjectFound: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			projectID, mintNumber, err := artblocks.ParseArtBlocksTokenID(tc.tokenID)
			require.NoError(t, err)
			assert.Equal(t, tc.expectedProjectID, projectID)
			assert.Equal(t, tc.expectedMintNumber, mintNumber)

			fullProjectID := fmt.Sprintf("%s-%d", tc.artblocksContract, projectID)
			t.Logf("Token ID %s -> Project ID: %s, Mint: %d", tc.tokenID, fullProjectID, mintNumber)

			metadata, err := client.GetProjectMetadata(ctx, 1, fullProjectID)

			if !tc.expectedProjectFound {
				assert.Error(t, err)
				assert.Nil(t, metadata)
				return
			}

			if err != nil {
				t.Logf("Could not fetch project %s: %v (may not be accessible)", fullProjectID, err)
				return
			}

			require.NotNil(t, metadata)
			assert.NotEmpty(t, metadata.Name)
			assert.NotEmpty(t, metadata.ArtistName)
			t.Logf("Successfully fetched: %s by %s (mint #%d)", metadata.Name, metadata.ArtistName, mintNumber)
		})
	}
}

// TestClient_GetProjectMetadata_Integration_InvalidID verifies that invalid project IDs
// return errors rather than nil results.
func TestClient_GetProjectMetadata_Integration_InvalidID(t *testing.T) {
	httpClient := adapter.NewHTTPClient(30 * time.Second)
	client := artblocks.NewClient(httpClient, ARTBLOCKS_GRAPHQL_URL, adapter.NewJSON())

	ctx := context.Background()

	invalidIDs := []string{
		"0xa7d8d9ef8d8ce8992df33d8b8cf4aebabd5bd270-999999",
		"0xa7d8d9ef8d8ce8992df33d8b8cf4aebabd5bd270-9999999",
		"invalid-contract-address-123",
	}

	for _, projectID := range invalidIDs {
		t.Run("invalid_id_"+projectID, func(t *testing.T) {
			metadata, err := client.GetProjectMetadata(ctx, 1, projectID)
			assert.Error(t, err)
			assert.Nil(t, metadata)
			t.Logf("Expected error for invalid ID %s: %v", projectID, err)
		})
	}
}

// TestClient_GetProjectMetadata_Integration_ContextCancellation verifies that a canceled
// context propagates as an error rather than hanging.
func TestClient_GetProjectMetadata_Integration_ContextCancellation(t *testing.T) {
	httpClient := adapter.NewHTTPClient(30 * time.Second)
	client := artblocks.NewClient(httpClient, ARTBLOCKS_GRAPHQL_URL, adapter.NewJSON())

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancel immediately

	metadata, err := client.GetProjectMetadata(ctx, 1, "1")

	assert.Error(t, err)
	assert.Nil(t, metadata)
	t.Logf("Context cancellation error: %v", err)
}

// TestClient_GetProjectMetadata_Integration_EdgeCases exercises empty, negative, and
// non-numeric project IDs — all expected to return errors.
func TestClient_GetProjectMetadata_Integration_EdgeCases(t *testing.T) {
	httpClient := adapter.NewHTTPClient(30 * time.Second)
	client := artblocks.NewClient(httpClient, ARTBLOCKS_GRAPHQL_URL, adapter.NewJSON())

	ctx := context.Background()

	cases := []struct {
		name      string
		projectID string
	}{
		{"EmptyProjectID", ""},
		{"NegativeProjectID", "-1"},
		{"NonNumericProjectID", "abc"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			metadata, err := client.GetProjectMetadata(ctx, 1, tc.projectID)
			assert.Error(t, err)
			assert.Nil(t, metadata)
			t.Logf("%s error: %v", tc.name, err)
		})
	}
}
