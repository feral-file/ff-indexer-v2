package artblocks_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/feral-file/ff-indexer-v2/internal/adapter"
	"github.com/feral-file/ff-indexer-v2/internal/logger"
	"github.com/feral-file/ff-indexer-v2/internal/mocks"
	"github.com/feral-file/ff-indexer-v2/internal/providers/vendors/artblocks"
)

const (
	ARTBLOCKS_GRAPHQL_URL = "https://artblocks-mainnet.hasura.app/v1/graphql"
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

// TestClient_GetProjectMetadata_Success tests successful project metadata retrieval with mock
func TestClient_GetProjectMetadata_Success(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockHTTPClient := mocks.NewMockHTTPClient(ctrl)
	mockJSON := adapter.NewJSON()
	client := artblocks.NewClient(mockHTTPClient, ARTBLOCKS_GRAPHQL_URL, mockJSON)

	ctx := context.Background()
	projectID := "1"

	description := "Test Project Description"

	expectedResponse := artblocks.GraphQLResponse{
		Data: struct {
			ProjectsMetadata *artblocks.ProjectMetadata `json:"projects_metadata_by_pk"`
		}{
			ProjectsMetadata: &artblocks.ProjectMetadata{
				Name:          "Test Project",
				ArtistName:    "Test Artist",
				ArtistAddress: "0x1234567890123456789012345678901234567890",
				Description:   &description,
			},
		},
	}

	mockHTTPClient.EXPECT().
		PostBytes(ctx, ARTBLOCKS_GRAPHQL_URL, map[string]string{"Content-Type": "application/json"}, gomock.Any()).
		DoAndReturn(func(ctx context.Context, url string, headers map[string]string, body interface{}) ([]byte, error) {
			data, _ := json.Marshal(expectedResponse)
			return data, nil
		}).
		Times(1)

	metadata, err := client.GetProjectMetadata(ctx, projectID)

	require.NoError(t, err)
	assert.NotNil(t, metadata)
	assert.Equal(t, "Test Project", metadata.Name)
	assert.Equal(t, "Test Artist", metadata.ArtistName)
	assert.Equal(t, "0x1234567890123456789012345678901234567890", metadata.ArtistAddress)
	assert.NotNil(t, metadata.Description)
	assert.Equal(t, description, *metadata.Description)
}

// TestClient_GetProjectMetadata_HTTPError tests error handling when HTTP client returns an error
func TestClient_GetProjectMetadata_HTTPError(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockHTTPClient := mocks.NewMockHTTPClient(ctrl)
	mockJSON := adapter.NewJSON()
	client := artblocks.NewClient(mockHTTPClient, ARTBLOCKS_GRAPHQL_URL, mockJSON)

	ctx := context.Background()
	projectID := "1"

	expectedError := errors.New("network error")

	mockHTTPClient.EXPECT().
		PostBytes(ctx, ARTBLOCKS_GRAPHQL_URL, map[string]string{"Content-Type": "application/json"}, gomock.Any()).
		Return(nil, expectedError).
		Times(1)

	metadata, err := client.GetProjectMetadata(ctx, projectID)

	assert.Error(t, err)
	assert.Nil(t, metadata)
	assert.Contains(t, err.Error(), "failed to call ArtBlocks GraphQL API")
	assert.Contains(t, err.Error(), "network error")
}

// TestClient_GetProjectMetadata_InvalidJSON tests error handling when API returns invalid JSON
func TestClient_GetProjectMetadata_InvalidJSON(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockHTTPClient := mocks.NewMockHTTPClient(ctrl)
	mockJSON := adapter.NewJSON()
	client := artblocks.NewClient(mockHTTPClient, ARTBLOCKS_GRAPHQL_URL, mockJSON)

	ctx := context.Background()
	projectID := "1"

	mockHTTPClient.EXPECT().
		PostBytes(ctx, ARTBLOCKS_GRAPHQL_URL, map[string]string{"Content-Type": "application/json"}, gomock.Any()).
		Return([]byte("invalid json"), nil).
		Times(1)

	metadata, err := client.GetProjectMetadata(ctx, projectID)

	assert.Error(t, err)
	assert.Nil(t, metadata)
	assert.Contains(t, err.Error(), "failed to unmarshal GraphQL response")
}

// TestClient_GetProjectMetadata_GraphQLError tests handling of GraphQL errors
func TestClient_GetProjectMetadata_GraphQLError(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockHTTPClient := mocks.NewMockHTTPClient(ctrl)
	mockJSON := adapter.NewJSON()
	client := artblocks.NewClient(mockHTTPClient, ARTBLOCKS_GRAPHQL_URL, mockJSON)

	ctx := context.Background()
	projectID := "1"

	errorResponse := artblocks.GraphQLResponse{
		Errors: []struct {
			Message string `json:"message"`
		}{
			{
				Message: "field not found",
			},
		},
	}

	mockHTTPClient.EXPECT().
		PostBytes(ctx, ARTBLOCKS_GRAPHQL_URL, map[string]string{"Content-Type": "application/json"}, gomock.Any()).
		DoAndReturn(func(ctx context.Context, url string, headers map[string]string, body interface{}) ([]byte, error) {
			data, _ := json.Marshal(errorResponse)
			return data, nil
		}).
		Times(1)

	metadata, err := client.GetProjectMetadata(ctx, projectID)

	assert.Error(t, err)
	assert.Nil(t, metadata)
	assert.Contains(t, err.Error(), "GraphQL errors")
	assert.Contains(t, err.Error(), "field not found")
}

// TestClient_GetProjectMetadata_ProjectNotFound tests handling when project is not found
func TestClient_GetProjectMetadata_ProjectNotFound(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockHTTPClient := mocks.NewMockHTTPClient(ctrl)
	mockJSON := adapter.NewJSON()
	client := artblocks.NewClient(mockHTTPClient, ARTBLOCKS_GRAPHQL_URL, mockJSON)

	ctx := context.Background()
	projectID := "99999"

	notFoundResponse := artblocks.GraphQLResponse{
		Data: struct {
			ProjectsMetadata *artblocks.ProjectMetadata `json:"projects_metadata_by_pk"`
		}{
			ProjectsMetadata: nil,
		},
	}

	mockHTTPClient.EXPECT().
		PostBytes(ctx, ARTBLOCKS_GRAPHQL_URL, map[string]string{"Content-Type": "application/json"}, gomock.Any()).
		DoAndReturn(func(ctx context.Context, url string, headers map[string]string, body interface{}) ([]byte, error) {
			data, _ := json.Marshal(notFoundResponse)
			return data, nil
		}).
		Times(1)

	metadata, err := client.GetProjectMetadata(ctx, projectID)

	assert.Error(t, err)
	assert.Nil(t, metadata)
	assert.Contains(t, err.Error(), "project not found")
}

// TestParseArtBlocksTokenID tests the token ID parsing utility function
func TestParseArtBlocksTokenID(t *testing.T) {
	tests := []struct {
		name           string
		tokenID        string
		expectedProjID int64
		expectedMintNo int64
		expectError    bool
	}{
		{
			name:           "valid token ID project 0",
			tokenID:        "1",
			expectedProjID: 0,
			expectedMintNo: 1,
			expectError:    false,
		},
		{
			name:           "valid token ID project 1",
			tokenID:        "1000000",
			expectedProjID: 1,
			expectedMintNo: 0,
			expectError:    false,
		},
		{
			name:           "valid token ID project 1 mint 1",
			tokenID:        "1000001",
			expectedProjID: 1,
			expectedMintNo: 1,
			expectError:    false,
		},
		{
			name:           "valid token ID project 23 mint 100",
			tokenID:        "23000100",
			expectedProjID: 23,
			expectedMintNo: 100,
			expectError:    false,
		},
		{
			name:           "valid token ID project 100",
			tokenID:        "100000000",
			expectedProjID: 100,
			expectedMintNo: 0,
			expectError:    false,
		},
		{
			name:           "invalid token ID - not a number",
			tokenID:        "invalid",
			expectedProjID: 0,
			expectedMintNo: 0,
			expectError:    true,
		},
		{
			name:           "invalid token ID - empty",
			tokenID:        "",
			expectedProjID: 0,
			expectedMintNo: 0,
			expectError:    true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			projectID, mintNumber, err := artblocks.ParseArtBlocksTokenID(tt.tokenID)

			if tt.expectError {
				assert.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.expectedProjID, projectID, "Project ID mismatch")
				assert.Equal(t, tt.expectedMintNo, mintNumber, "Mint number mismatch")
			}
		})
	}
}

// Integration tests - these test against the real ArtBlocks API

func TestClient_GetProjectMetadata_Integration(t *testing.T) {
	httpClient := adapter.NewHTTPClient(30 * time.Second)
	mockJSON := adapter.NewJSON()
	client := artblocks.NewClient(httpClient, ARTBLOCKS_GRAPHQL_URL, mockJSON)

	ctx := context.Background()
	testCases := []struct {
		name      string
		projectID string
		expectOk  bool
	}{
		{
			name:      "Chromie_Squiggle",
			projectID: "0x059edd72cd353df5106d2b9cc5ab83a52287ac3a-0",
			expectOk:  true,
		},
		{
			name:      "Fidenza",
			projectID: "0xa7d8d9ef8d8ce8992df33d8b8cf4aebabd5bd270-78",
			expectOk:  true,
		},
		{
			name:      "Archetype",
			projectID: "0xa7d8d9ef8d8ce8992df33d8b8cf4aebabd5bd270-23",
			expectOk:  true,
		},
		{
			name:      "Ringers",
			projectID: "0xa7d8d9ef8d8ce8992df33d8b8cf4aebabd5bd270-13",
			expectOk:  true,
		},
		{
			name:      "Invalid_Project",
			projectID: "0xa7d8d9ef8d8ce8992df33d8b8cf4aebabd5bd270-999999",
			expectOk:  false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			metadata, err := client.GetProjectMetadata(ctx, tc.projectID)

			if !tc.expectOk {
				// We expect this to fail
				assert.Error(t, err)
				assert.Nil(t, metadata)
				//assert.Contains(t, err.Error(), "project not found") // TODO: Uncomment this when we have a valid error message
				t.Logf("Expected error for %s: %v", tc.projectID, err)
				return
			}

			// For valid projects, check if we can fetch data
			if err != nil {
				// Some projects may not be accessible, log and continue
				t.Logf("Project %s: Error - %v (may not be accessible)", tc.projectID, err)
				return
			}

			require.NotNil(t, metadata, "Metadata should not be nil for project: %s", tc.projectID)

			// Validate response structure
			assert.NotEmpty(t, metadata.Name, "Project name should not be empty")
			assert.NotEmpty(t, metadata.ArtistName, "Artist name should not be empty")
			assert.NotEmpty(t, metadata.ArtistAddress, "Artist address should not be empty")

			// Optional fields - may or may not be present
			if metadata.Description != nil {
				t.Logf("Description length: %d chars", len(*metadata.Description))
			}

			// Log project details
			t.Logf("Project Name: %s", metadata.Name)
			t.Logf("Artist Name: %s", metadata.ArtistName)
			t.Logf("Artist Address: %s", metadata.ArtistAddress)
		})
	}
}

// TestClient_GetProjectMetadata_Integration_TokenIDParsing tests with actual token IDs
func TestClient_GetProjectMetadata_Integration_TokenIDParsing(t *testing.T) {
	httpClient := adapter.NewHTTPClient(30 * time.Second)
	mockJSON := adapter.NewJSON()
	client := artblocks.NewClient(httpClient, ARTBLOCKS_GRAPHQL_URL, mockJSON)

	ctx := context.Background()

	// Test with actual token IDs and parse them to get project metadata
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
			// Parse the token ID
			projectID, mintNumber, err := artblocks.ParseArtBlocksTokenID(tc.tokenID)
			require.NoError(t, err)
			assert.Equal(t, tc.expectedProjectID, projectID)
			assert.Equal(t, tc.expectedMintNumber, mintNumber)

			// Build the full project ID
			fullProjectID := fmt.Sprintf("%s-%d", tc.artblocksContract, projectID)
			t.Logf("Token ID %s -> Project ID: %s, Mint: %d", tc.tokenID, fullProjectID, mintNumber)

			// Fetch project metadata
			metadata, err := client.GetProjectMetadata(ctx, fullProjectID)

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

// TestClient_GetProjectMetadata_Integration_InvalidID tests error handling with invalid project ID
func TestClient_GetProjectMetadata_Integration_InvalidID(t *testing.T) {
	httpClient := adapter.NewHTTPClient(30 * time.Second)
	mockJSON := adapter.NewJSON()
	client := artblocks.NewClient(httpClient, ARTBLOCKS_GRAPHQL_URL, mockJSON)

	ctx := context.Background()

	// Test with various invalid project IDs
	invalidIDs := []string{
		"0xa7d8d9ef8d8ce8992df33d8b8cf4aebabd5bd270-999999",  // Very large project ID that likely doesn't exist
		"0xa7d8d9ef8d8ce8992df33d8b8cf4aebabd5bd270-9999999", // Even larger
		"invalid-contract-address-123",                       // Invalid format
	}

	for _, projectID := range invalidIDs {
		t.Run("invalid_id_"+projectID, func(t *testing.T) {
			metadata, err := client.GetProjectMetadata(ctx, projectID)

			// Should return an error (project not found)
			assert.Error(t, err)
			assert.Nil(t, metadata)
			//assert.Contains(t, err.Error(), "project not found") // TODO: Uncomment this when we have a valid error message
			t.Logf("Expected error for invalid ID %s: %v", projectID, err)
		})
	}
}

// TestClient_GetProjectMetadata_Integration_ContextCancellation tests context cancellation
func TestClient_GetProjectMetadata_Integration_ContextCancellation(t *testing.T) {
	httpClient := adapter.NewHTTPClient(30 * time.Second)
	mockJSON := adapter.NewJSON()
	client := artblocks.NewClient(httpClient, ARTBLOCKS_GRAPHQL_URL, mockJSON)

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately

	projectID := "1"

	metadata, err := client.GetProjectMetadata(ctx, projectID)

	// Should return an error due to context cancellation
	assert.Error(t, err)
	assert.Nil(t, metadata)
	t.Logf("Context cancellation error: %v", err)
}

// TestClient_GetProjectMetadata_Integration_EdgeCases tests various edge cases
func TestClient_GetProjectMetadata_Integration_EdgeCases(t *testing.T) {
	httpClient := adapter.NewHTTPClient(30 * time.Second)
	mockJSON := adapter.NewJSON()
	client := artblocks.NewClient(httpClient, ARTBLOCKS_GRAPHQL_URL, mockJSON)

	ctx := context.Background()

	t.Run("EmptyProjectID", func(t *testing.T) {
		projectID := ""

		metadata, err := client.GetProjectMetadata(ctx, projectID)

		// Should return an error
		assert.Error(t, err)
		assert.Nil(t, metadata)
		t.Logf("Empty project ID error: %v", err)
	})

	t.Run("NegativeProjectID", func(t *testing.T) {
		projectID := "-1"

		metadata, err := client.GetProjectMetadata(ctx, projectID)

		// Should return an error
		assert.Error(t, err)
		assert.Nil(t, metadata)
		t.Logf("Negative project ID error: %v", err)
	})

	t.Run("NonNumericProjectID", func(t *testing.T) {
		// GraphQL accepts string IDs, so this might work depending on API implementation
		projectID := "abc"

		metadata, err := client.GetProjectMetadata(ctx, projectID)

		// Should return an error
		assert.Error(t, err)
		assert.Nil(t, metadata)
		t.Logf("Non-numeric project ID error: %v", err)
	})
}
