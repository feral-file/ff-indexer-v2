package artblocks_test

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

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
				Name:           "Test Project",
				ArtistName:     "Test Artist",
				ArtistAddress:  "0x1234567890123456789012345678901234567890",
				Description:    &description,
				MaxInvocations: 999,
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

	metadata, err := client.GetProjectMetadata(ctx, 1, projectID)

	require.NoError(t, err)
	assert.NotNil(t, metadata)
	assert.Equal(t, "Test Project", metadata.Name)
	assert.Equal(t, "Test Artist", metadata.ArtistName)
	assert.Equal(t, "0x1234567890123456789012345678901234567890", metadata.ArtistAddress)
	assert.NotNil(t, metadata.Description)
	assert.Equal(t, description, *metadata.Description)
	assert.Equal(t, 999, metadata.MaxInvocations)
}

// TestClient_GetProjectMetadata_ParsesMaxInvocations verifies max_invocations is unmarshaled
// from the GraphQL response (regression guard for release total_mints sourcing).
func TestClient_GetProjectMetadata_ParsesMaxInvocations(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockHTTPClient := mocks.NewMockHTTPClient(ctrl)
	mockJSON := adapter.NewJSON()
	client := artblocks.NewClient(mockHTTPClient, ARTBLOCKS_GRAPHQL_URL, mockJSON)

	ctx := context.Background()
	projectID := "0xa7d8d9ef8d8ce8992df33d8b8cf4aebabd5bd270-78"

	mockHTTPClient.EXPECT().
		PostBytes(ctx, ARTBLOCKS_GRAPHQL_URL, map[string]string{"Content-Type": "application/json"}, gomock.Any()).
		DoAndReturn(func(ctx context.Context, url string, headers map[string]string, body interface{}) ([]byte, error) {
			return []byte(`{
				"data": {
					"projects_metadata_by_pk": {
						"name": "Fidenza",
						"artist_name": "Tyler Hobbs",
						"artist_address": "0x1234567890123456789012345678901234567890",
						"description": "Generative art",
						"max_invocations": 999
					}
				}
			}`), nil
		}).
		Times(1)

	metadata, err := client.GetProjectMetadata(ctx, 1, projectID)

	require.NoError(t, err)
	require.NotNil(t, metadata)
	assert.Equal(t, "Fidenza", metadata.Name)
	assert.Equal(t, 999, metadata.MaxInvocations)
}

// TestClient_GetProjectMetadata_GraphQLQueryIncludesMaxInvocations verifies the outbound
// GraphQL query requests max_invocations so the field is not dropped at request time.
func TestClient_GetProjectMetadata_GraphQLQueryIncludesMaxInvocations(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockHTTPClient := mocks.NewMockHTTPClient(ctrl)
	mockJSON := adapter.NewJSON()
	client := artblocks.NewClient(mockHTTPClient, ARTBLOCKS_GRAPHQL_URL, mockJSON)

	ctx := context.Background()

	mockHTTPClient.EXPECT().
		PostBytes(ctx, ARTBLOCKS_GRAPHQL_URL, map[string]string{"Content-Type": "application/json"}, gomock.Any()).
		DoAndReturn(func(ctx context.Context, url string, headers map[string]string, body interface{}) ([]byte, error) {
			reader, ok := body.(io.Reader)
			require.True(t, ok, "PostBytes body must be an io.Reader")

			payload, err := io.ReadAll(reader)
			require.NoError(t, err)

			var request artblocks.GraphQLRequest
			require.NoError(t, json.Unmarshal(payload, &request))
			assert.Contains(t, request.Query, "max_invocations")

			return []byte(`{"data":{"projects_metadata_by_pk":{"name":"P","artist_name":"A","artist_address":"0x1","max_invocations":1}}}`), nil
		}).
		Times(1)

	_, err := client.GetProjectMetadata(ctx, 1, "1")
	require.NoError(t, err)
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

	metadata, err := client.GetProjectMetadata(ctx, 1, projectID)

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

	metadata, err := client.GetProjectMetadata(ctx, 1, projectID)

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

	metadata, err := client.GetProjectMetadata(ctx, 1, projectID)

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

	metadata, err := client.GetProjectMetadata(ctx, 1, projectID)

	assert.Error(t, err)
	assert.Nil(t, metadata)
	assert.Contains(t, err.Error(), "project not found")
	assert.Contains(t, err.Error(), "chain_id=1")
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
