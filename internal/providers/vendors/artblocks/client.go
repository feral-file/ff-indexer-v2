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
	Name           string  `json:"name"`
	ArtistName     string  `json:"artist_name"`
	ArtistAddress  string  `json:"artist_address"`
	Description    *string `json:"description"`
	MaxInvocations int     `json:"max_invocations"`
	// Slug is the URL slug used on the Art Blocks website (e.g. "fidenza-by-tyler-hobbs").
	// Populated via the projects_metadata GraphQL query.
	Slug string `json:"slug"`
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

// slugProjectRef holds the minimal fields returned by the slug-based project lookup.
type slugProjectRef struct {
	ChainID int    `json:"chain_id"`
	ID      string `json:"id"` // "{contract}-{projectID}" composite
}

// gqlSlugResponse wraps the ArtBlocks GraphQL response for slug-based project lookup.
type gqlSlugResponse struct {
	Data struct {
		Projects []slugProjectRef `json:"projects_metadata"`
	} `json:"data"`
	Errors []struct {
		Message string `json:"message"`
	} `json:"errors"`
}

// Client defines the interface for ArtBlocks client operations to enable mocking
//
//go:generate mockgen -source=client.go -destination=../../../mocks/artblocks_client.go -package=mocks -mock_names=Client=MockArtBlocksClient
type Client interface {
	// GetProjectMetadata fetches project metadata from ArtBlocks GraphQL API.
	// chainID is the EVM numeric chain ID (e.g. 1 for Ethereum mainnet) required by projects_metadata_by_pk.
	GetProjectMetadata(ctx context.Context, chainID int, projectID string) (*ProjectMetadata, error)

	// ResolveSlug resolves an Art Blocks URL slug (e.g. "fidenza-by-tyler-hobbs") to the
	// chain-qualified vendor_release_id ("{chainID}-{contract}-{projectID}").
	// chainID filters results to one chain (1 = Ethereum mainnet). Returns an error when
	// no project matches the slug on that chain.
	ResolveSlug(ctx context.Context, chainID int, slug string) (string, error)
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
func (c *ArtBlocksClient) GetProjectMetadata(ctx context.Context, chainID int, projectID string) (*ProjectMetadata, error) {
	query := `query GetABProject($chain_id: Int!, $id: String!) {
		projects_metadata_by_pk(chain_id: $chain_id, id: $id) {
			name
			artist_name
			artist_address
			description
			max_invocations
			slug
		}
	}`

	request := GraphQLRequest{
		Query: query,
		Variables: map[string]interface{}{
			"chain_id": chainID,
			"id":       projectID,
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
		return nil, fmt.Errorf("project not found: chain_id=%d id=%s", chainID, projectID)
	}

	return response.Data.ProjectsMetadata, nil
}

// ResolveSlug resolves an Art Blocks URL slug to the chain-qualified vendor_release_id.
//
// Queries projects_metadata(where:{slug:{_eq:$slug},chain_id:{_eq:$chain_id}}) and formats
// the result as "{chainID}-{contract}-{projectID}" — the same format used by GetProjectMetadata
// and UpsertRelease. chainID 1 is Ethereum mainnet (the primary Art Blocks deployment).
//
// The slug-based lookup requires a chain constraint because the same slug could in principle
// appear on multiple chains during a migration. We require callers to specify chainID so the
// result is always unambiguous.
func (c *ArtBlocksClient) ResolveSlug(ctx context.Context, chainID int, slug string) (string, error) {
	query := `query ResolveABSlug($chain_id: Int!, $slug: String!) {
		projects_metadata(
			where: {chain_id: {_eq: $chain_id}, slug: {_eq: $slug}}
			limit: 1
		) {
			chain_id
			id
		}
	}`

	request := GraphQLRequest{
		Query: query,
		Variables: map[string]interface{}{
			"chain_id": chainID,
			"slug":     slug,
		},
	}

	requestBody, err := c.json.Marshal(request)
	if err != nil {
		return "", fmt.Errorf("failed to marshal GraphQL request: %w", err)
	}

	headers := map[string]string{
		"Content-Type": "application/json",
	}
	respBody, err := c.httpClient.PostBytes(ctx, c.graphqlURL, headers, bytes.NewReader(requestBody))
	if err != nil {
		return "", fmt.Errorf("failed to call ArtBlocks GraphQL API: %w", err)
	}

	var response gqlSlugResponse
	if err := c.json.Unmarshal(respBody, &response); err != nil {
		return "", fmt.Errorf("failed to unmarshal GraphQL response: %w", err)
	}

	if len(response.Errors) > 0 {
		return "", fmt.Errorf("GraphQL errors: %s", response.Errors[0].Message)
	}

	if len(response.Data.Projects) == 0 {
		return "", fmt.Errorf("art blocks slug not found: chain_id=%d slug=%q", chainID, slug)
	}

	proj := response.Data.Projects[0]
	// proj.ID from the API is "{contract}-{projectID}". We prefix with the numeric chainID
	// to form the vendor_release_id used in this indexer.
	vendorReleaseID := fmt.Sprintf("%d-%s", proj.ChainID, proj.ID)
	return vendorReleaseID, nil
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
