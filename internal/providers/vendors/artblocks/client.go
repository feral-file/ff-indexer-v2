package artblocks

import (
	"bytes"
	"context"
	"fmt"
	"math/big"

	"github.com/feral-file/ff-indexer-v2/internal/adapter"
)

const (
	// ArtBlocks contracts use a specific token ID format: projectID * 1000000 + mintNumber
	ArtBlocksTokenIDMultiplier = 1000000
)

// ProjectMetadata represents the project metadata from ArtBlocks API
type ProjectMetadata struct {
	Name          string  `json:"name"`
	ArtistName    string  `json:"artist_name"`
	ArtistAddress string  `json:"artist_address"`
	Description   *string `json:"description"`
}

// GraphQLRequest represents a GraphQL request
type GraphQLRequest struct {
	Query     string                 `json:"query"`
	Variables map[string]interface{} `json:"variables"`
}

// GraphQLResponse represents a GraphQL response
type GraphQLResponse struct {
	Data struct {
		ProjectsMetadata *ProjectMetadata `json:"projects_metadata_by_pk"`
	} `json:"data"`
	Errors []struct {
		Message string `json:"message"`
	} `json:"errors"`
}

// Client defines the interface for ArtBlocks client operations to enable mocking
//
//go:generate mockgen -source=client.go -destination=../../../mocks/artblocks_client.go -package=mocks -mock_names=Client=MockArtBlocksClient
type Client interface {
	// GetProjectMetadata fetches project metadata from ArtBlocks GraphQL API
	GetProjectMetadata(ctx context.Context, projectID string) (*ProjectMetadata, error)
}

// ArtBlocksClient implements ArtBlocks client
type ArtBlocksClient struct {
	httpClient adapter.HTTPClient
	graphqlURL string
	json       adapter.JSON
}

// NewClient creates a new ArtBlocks client
func NewClient(httpClient adapter.HTTPClient, graphqlURL string, json adapter.JSON) Client {
	return &ArtBlocksClient{
		httpClient: httpClient,
		graphqlURL: graphqlURL,
		json:       json,
	}
}

// GetProjectMetadata fetches project metadata from ArtBlocks GraphQL API
func (c *ArtBlocksClient) GetProjectMetadata(ctx context.Context, projectID string) (*ProjectMetadata, error) {
	query := `query GetABProject($id: String!) {
		projects_metadata_by_pk(id: $id) {
			name
			artist_name
			artist_address
			description
		}
	}`

	request := GraphQLRequest{
		Query: query,
		Variables: map[string]interface{}{
			"id": projectID,
		},
	}

	requestBody, err := c.json.Marshal(request)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal GraphQL request: %w", err)
	}

	headers := map[string]string{
		"Content-Type": "application/json",
	}
	respBody, err := c.httpClient.PostBytes(ctx, c.graphqlURL, headers, bytes.NewReader(requestBody))
	if err != nil {
		return nil, fmt.Errorf("failed to call ArtBlocks GraphQL API: %w", err)
	}

	var response GraphQLResponse
	if err := c.json.Unmarshal(respBody, &response); err != nil {
		return nil, fmt.Errorf("failed to unmarshal GraphQL response: %w", err)
	}

	if len(response.Errors) > 0 {
		return nil, fmt.Errorf("GraphQL errors: %s", response.Errors[0].Message)
	}

	if response.Data.ProjectsMetadata == nil {
		return nil, fmt.Errorf("project not found: %s", projectID)
	}

	return response.Data.ProjectsMetadata, nil
}

// ParseArtBlocksTokenID parses an ArtBlocks token ID into project ID and mint number
// ArtBlocks token IDs are in the format: projectID * 1000000 + mintNumber
func ParseArtBlocksTokenID(tokenID string) (projectID int64, mintNumber int64, err error) {
	// Parse token ID as big int to handle large numbers
	tokenIDBigInt := new(big.Int)
	tokenIDBigInt, ok := tokenIDBigInt.SetString(tokenID, 10)
	if !ok {
		return 0, 0, fmt.Errorf("invalid token ID: %s", tokenID)
	}

	// Project ID = tokenID / 1000000
	multiplier := big.NewInt(ArtBlocksTokenIDMultiplier)
	projectIDBigInt := new(big.Int).Div(tokenIDBigInt, multiplier)
	projectID = projectIDBigInt.Int64()

	// Mint number = tokenID % 1000000
	mintNumberBigInt := new(big.Int).Mod(tokenIDBigInt, multiplier)
	mintNumber = mintNumberBigInt.Int64()

	return projectID, mintNumber, nil
}
