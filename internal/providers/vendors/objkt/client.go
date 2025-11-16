package objkt

import (
	"bytes"
	"context"
	"fmt"

	"github.com/feral-file/ff-indexer-v2/internal/adapter"
)

// TokenResponse represents the GraphQL response from objkt v3 API
type TokenResponse struct {
	Data struct {
		Token []Token `json:"token"`
	} `json:"data"`
}

// Token represents a token from objkt v3 API
type Token struct {
	Name         *string   `json:"name"`
	Description  *string   `json:"description"`
	DisplayURI   *string   `json:"display_uri"`
	ArtifactURI  *string   `json:"artifact_uri"`
	ThumbnailURI *string   `json:"thumbnail_uri"`
	Mime         *string   `json:"mime"`
	Metadata     any       `json:"metadata"`
	Creators     []Creator `json:"creators"`
}

// Creator represents a creator/artist in objkt API
type Creator struct {
	Holder Holder `json:"holder"`
}

// Holder represents the holder/owner information
type Holder struct {
	Address string  `json:"address"`
	Alias   *string `json:"alias"`
}

// GraphQLRequest represents a GraphQL request
type GraphQLRequest struct {
	Query         string      `json:"query"`
	Variables     interface{} `json:"variables"`
	OperationName string      `json:"operationName"`
}

// Client defines the interface for objkt client operations to enable mocking
//
//go:generate mockgen -source=client.go -destination=../../../mocks/objkt_client.go -package=mocks -mock_names=Client=MockObjktClient
type Client interface {
	// GetToken fetches token data from objkt v3 API
	GetToken(ctx context.Context, contractAddress, tokenID string) (*Token, error)
}

// ObjktClient implements objkt client
type ObjktClient struct {
	httpClient adapter.HTTPClient
	apiURL     string
	json       adapter.JSON
}

// NewClient creates a new objkt client
func NewClient(httpClient adapter.HTTPClient, apiURL string, json adapter.JSON) Client {
	return &ObjktClient{
		httpClient: httpClient,
		apiURL:     apiURL,
		json:       json,
	}
}

// GetToken fetches token data from objkt v3 API using GraphQL
func (c *ObjktClient) GetToken(ctx context.Context, contractAddress, tokenID string) (*Token, error) {
	// Construct the GraphQL query
	query := fmt.Sprintf(`query GetToken {
  token(
    where: {fa_contract: {_eq: "%s"}, token_id: {_eq: "%s"}}
  ) {
    artifact_uri
    description
    display_uri
    mime
    name
    thumbnail_uri
    metadata
    creators {
      holder {
        address
        alias
      }
    }
  }
}`, contractAddress, tokenID)

	// Create GraphQL request
	request := GraphQLRequest{
		Query:         query,
		Variables:     nil,
		OperationName: "GetToken",
	}

	// Marshal the request to JSON
	requestBody, err := c.json.Marshal(request)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal GraphQL request: %w", err)
	}

	// Make the POST request
	responseBody, err := c.httpClient.Post(ctx, c.apiURL, "application/json", bytes.NewReader(requestBody))
	if err != nil {
		return nil, fmt.Errorf("failed to call objkt v3 API: %w", err)
	}

	// Unmarshal the response
	var response TokenResponse
	if err := c.json.Unmarshal(responseBody, &response); err != nil {
		return nil, fmt.Errorf("failed to unmarshal objkt response: %w", err)
	}

	// Check if we got any results
	if len(response.Data.Token) == 0 {
		return nil, fmt.Errorf("token not found: contract=%s, tokenID=%s", contractAddress, tokenID)
	}

	return &response.Data.Token[0], nil
}
