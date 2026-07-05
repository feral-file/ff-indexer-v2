package objkt

import (
	"bytes"
	"context"
	"errors"
	"fmt"

	"github.com/feral-file/ff-indexer-v2/internal/adapter"
	"github.com/feral-file/ff-indexer-v2/internal/ratelimit"
)

const PROVIDER_NAME = "objkt"

// TokenResponse represents the GraphQL response from objkt v3 API
type TokenResponse struct {
	Data struct {
		Token []Token `json:"token"`
	} `json:"data"`
}

// FAResponse represents the GraphQL response from the objkt v3 API for a collection (FA2 contract).
type FAResponse struct {
	Data struct {
		FA []FA `json:"fa"`
	} `json:"data"`
}

// FA represents a Tezos FA2 collection (contract-level) from objkt v3 API.
// CollectionType distinguishes dedicated single-artist contracts ("custom") from
// open curated marketplaces ("open", "open_generative"). Only "custom" collections
// map cleanly to a single release; the KT1 address serves as the vendor_release_id.
type FA struct {
	Name           string `json:"name"`
	Editions       int64  `json:"editions"`
	CollectionType string `json:"collection_type"`
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
	FA           *FA       `json:"fa"`
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

// ErrCollectionNotCustom is returned by GetFA when the contract exists but is not a
// "custom" collection type. Only custom collections have per-contract sequential token
// IDs suitable for IndexRelease CID derivation.
var ErrCollectionNotCustom = errors.New("objkt collection is not a custom collection")

// Client defines the interface for objkt client operations to enable mocking
//
//go:generate mockgen -source=client.go -destination=../../../mocks/objkt_client.go -package=mocks -mock_names=Client=MockObjktClient
type Client interface {
	// GetToken fetches token data from objkt v3 API
	GetToken(ctx context.Context, contractAddress, tokenID string) (*Token, error)

	// GetFA fetches FA2 collection metadata for a given KT1 contract address.
	// Returns ErrCollectionNotCustom when the contract exists but is not a "custom"
	// collection type, so the caller can produce a clear error for unsupported contracts.
	// Returns an error when the contract is not found or the API call fails.
	GetFA(ctx context.Context, contractAddress string) (*FA, error)
}

// ObjktClient implements objkt client
type ObjktClient struct {
	httpClient  adapter.HTTPClient
	rateLimiter ratelimit.Limiter
	apiURL      string
	apiKey      string
	json        adapter.JSON
}

// NewClient creates a new objkt client
func NewClient(httpClient adapter.HTTPClient, rateLimiter ratelimit.Limiter, apiURL string, apiKey string, json adapter.JSON) Client {
	return &ObjktClient{
		httpClient:  httpClient,
		rateLimiter: rateLimiter,
		apiURL:      apiURL,
		apiKey:      apiKey,
		json:        json,
	}
}

// getTokenQuery is the static GraphQL query for GetToken.
// Caller-supplied contractAddress and tokenID are passed via variables, not interpolated.
//
//nolint:gosec // G101 false positive: "token_id" is a GraphQL field name, not a credential.
const getTokenQuery = `query GetToken($faContract: String!, $tokenId: String!) {
  token(
    where: {fa_contract: {_eq: $faContract}, token_id: {_eq: $tokenId}}
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
    fa {
      name
      editions
      collection_type
    }
  }
}`

type getTokenVars struct {
	FAContract string `json:"faContract"`
	TokenID    string `json:"tokenId"`
}

// GetToken fetches token data from objkt v3 API using GraphQL.
func (c *ObjktClient) GetToken(ctx context.Context, contractAddress, tokenID string) (*Token, error) {
	request := GraphQLRequest{
		Query:         getTokenQuery,
		Variables:     getTokenVars{FAContract: contractAddress, TokenID: tokenID},
		OperationName: "GetToken",
	}

	// Marshal the request to JSON
	requestBody, err := c.json.Marshal(request)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal GraphQL request: %w", err)
	}

	// Make the POST request
	responseBody, err := ratelimit.Do(ctx, c.rateLimiter, PROVIDER_NAME, func(ctx context.Context) ([]byte, error) {
		headers := map[string]string{
			"Content-Type": "application/json",
		}
		if c.apiKey != "" {
			headers["X-API-KEY"] = c.apiKey
		}

		return c.httpClient.PostBytes(ctx, c.apiURL, headers, bytes.NewReader(requestBody))
	})
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

// GetFA fetches FA2 collection metadata for a KT1 contract address from the objkt v3 API.
//
// Reason: before enqueuing IndexRelease CIDs for an objkt contract, the workflow must
// verify the contract is a "custom" collection. Only custom contracts have per-contract
// sequential 1-based token IDs — open/curated contracts are multi-artist and their
// token IDs are globally assigned, not sequential per collection.
//
// Returns ErrCollectionNotCustom when the contract exists but is not a "custom" type
// so callers can surface a clear, actionable error.
// getFAQuery is the static GraphQL query for GetFA.
// The contractAddress is caller-supplied through the public release-indexing endpoint
// and must be passed via a variable, not interpolated into the query string.
const getFAQuery = `query GetFA($contract: String!) {
  fa(
    where: {contract: {_eq: $contract}}
  ) {
    name
    editions
    collection_type
  }
}`

type getFAVars struct {
	Contract string `json:"contract"`
}

func (c *ObjktClient) GetFA(ctx context.Context, contractAddress string) (*FA, error) {
	request := GraphQLRequest{
		Query:         getFAQuery,
		Variables:     getFAVars{Contract: contractAddress},
		OperationName: "GetFA",
	}

	requestBody, err := c.json.Marshal(request)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal GetFA request: %w", err)
	}

	responseBody, err := ratelimit.Do(ctx, c.rateLimiter, PROVIDER_NAME, func(ctx context.Context) ([]byte, error) {
		headers := map[string]string{
			"Content-Type": "application/json",
		}
		if c.apiKey != "" {
			headers["X-API-KEY"] = c.apiKey
		}
		return c.httpClient.PostBytes(ctx, c.apiURL, headers, bytes.NewReader(requestBody))
	})
	if err != nil {
		return nil, fmt.Errorf("failed to call objkt v3 API (GetFA): %w", err)
	}

	var response FAResponse
	if err := c.json.Unmarshal(responseBody, &response); err != nil {
		return nil, fmt.Errorf("failed to unmarshal objkt GetFA response: %w", err)
	}

	if len(response.Data.FA) == 0 {
		return nil, fmt.Errorf("objkt collection not found: contract=%s", contractAddress)
	}

	fa := &response.Data.FA[0]
	if fa.CollectionType != "custom" {
		return nil, fmt.Errorf("objkt contract %s is a %q collection, not %q: %w",
			contractAddress, fa.CollectionType, "custom", ErrCollectionNotCustom)
	}

	return fa, nil
}
