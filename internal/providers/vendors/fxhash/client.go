// Package fxhash provides a client for the fxhash v2 GraphQL API.
// fxhash is a generative art platform on Tezos. Each minted edition (called a "gentk")
// belongs to a generative token (the project/release). This client fetches gentk data
// including its release membership and artist using the fxhash v2 API's onchain.objkt_by_pk
// query, keyed by "{contract}-{tokenID}".
package fxhash

import (
	"bytes"
	"context"
	"fmt"

	"github.com/feral-file/ff-indexer-v2/internal/adapter"
	"github.com/feral-file/ff-indexer-v2/internal/ratelimit"
)

const PROVIDER_NAME = "fxhash"

// WalletAccount holds the Tezos wallet address of a fxhash user.
type WalletAccount struct {
	Address string `json:"address"`
}

// Author represents a fxhash generative token author (artist).
type Author struct {
	Name          string         `json:"name"`
	WalletAccount *WalletAccount `json:"wallet_account"`
}

// GenerativeToken represents a fxhash generative token (the release/project level).
// ID is the stable numeric project identifier (e.g. "9997").
// OriginalSupply is the declared edition cap set by the artist; Supply is the number
// actually minted so far. Use OriginalSupply for total_mints when non-nil; fall back
// to Supply when the artist did not set an explicit cap (open editions).
type GenerativeToken struct {
	ID             string  `json:"id"`
	Name           string  `json:"name"`
	Slug           string  `json:"slug"`
	Supply         string  `json:"supply"`
	OriginalSupply *string `json:"original_supply"`
	Author         *Author `json:"author"`
}

// Gentk represents a single minted edition (iteration) of a fxhash generative token.
// Iteration is 1-based and is the mint number within the project.
type Gentk struct {
	Iteration       string           `json:"iteration"`
	DisplayURI      *string          `json:"display_uri"`
	ThumbnailURI    *string          `json:"thumbnail_uri"`
	Name            *string          `json:"name"`
	GenerativeToken *GenerativeToken `json:"generative_token"`
}

// GQLRequest represents a GraphQL request body.
type GQLRequest struct {
	Query         string      `json:"query"`
	Variables     interface{} `json:"variables"`
	OperationName string      `json:"operationName"`
}

// GQLResponse represents the top-level fxhash v2 GraphQL response envelope.
type GQLResponse struct {
	Data struct {
		Onchain struct {
			ObjktByPK *Gentk `json:"objkt_by_pk"`
		} `json:"onchain"`
	} `json:"data"`
	Errors []struct {
		Message string `json:"message"`
	} `json:"errors"`
}

// Client defines the interface for fxhash client operations.
//
//go:generate mockgen -source=client.go -destination=../../../mocks/fxhash_client.go -package=mocks -mock_names=Client=MockFxhashClient
type Client interface {
	// GetGentk fetches a fxhash gentk by contract address and token ID.
	// Returns (nil, nil) when the gentk is not found in the fxhash index (e.g. the token
	// is on a fxhash marketplace contract but was not minted through fxhash, or the
	// fxhash v2 API does not yet index it). The caller should treat nil as "not found"
	// and fall through to objkt enrichment.
	GetGentk(ctx context.Context, contractAddress, tokenID string) (*Gentk, error)
}

// fxhashClient implements the Client interface.
type fxhashClient struct {
	httpClient  adapter.HTTPClient
	rateLimiter ratelimit.Limiter
	apiURL      string
	json        adapter.JSON
}

// NewClient creates a new fxhash client.
// apiURL should be the fxhash v2 GraphQL endpoint (https://api.v2.fxhash.xyz/v1/graphql).
func NewClient(httpClient adapter.HTTPClient, rateLimiter ratelimit.Limiter, apiURL string, json adapter.JSON) Client {
	return &fxhashClient{
		httpClient:  httpClient,
		rateLimiter: rateLimiter,
		apiURL:      apiURL,
		json:        json,
	}
}

// GetGentk fetches a fxhash gentk by its on-chain identity.
// The fxhash v2 API identifies gentks by the composite key "{contract}-{tokenID}".
// This matches the on-chain (fa_contract, token_id) pair used by objkt.
func (c *fxhashClient) GetGentk(ctx context.Context, contractAddress, tokenID string) (*Gentk, error) {
	// The fxhash v2 objkt_by_pk id is "{contract}-{tokenID}".
	gentkID := fmt.Sprintf("%s-%s", contractAddress, tokenID)

	query := fmt.Sprintf(`query GetGentk {
  onchain {
    objkt_by_pk(id: "%s") {
      iteration
      display_uri
      thumbnail_uri
      name
      generative_token {
        id
        name
        slug
        supply
        original_supply
        author {
          name
          wallet_account {
            address
          }
        }
      }
    }
  }
}`, gentkID)

	reqBody := GQLRequest{
		Query:         query,
		Variables:     nil,
		OperationName: "GetGentk",
	}

	bodyBytes, err := c.json.Marshal(reqBody)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal fxhash request: %w", err)
	}

	responseBody, err := ratelimit.Do(ctx, c.rateLimiter, PROVIDER_NAME, func(ctx context.Context) ([]byte, error) {
		headers := map[string]string{
			"Content-Type": "application/json",
		}
		return c.httpClient.PostBytes(ctx, c.apiURL, headers, bytes.NewReader(bodyBytes))
	})
	if err != nil {
		return nil, fmt.Errorf("failed to call fxhash v2 API: %w", err)
	}

	var resp GQLResponse
	if err := c.json.Unmarshal(responseBody, &resp); err != nil {
		return nil, fmt.Errorf("failed to unmarshal fxhash response: %w", err)
	}

	if len(resp.Errors) > 0 {
		return nil, fmt.Errorf("fxhash API error: %s", resp.Errors[0].Message)
	}

	// nil means not found in fxhash index — not an error, caller falls back to objkt.
	return resp.Data.Onchain.ObjktByPK, nil
}
