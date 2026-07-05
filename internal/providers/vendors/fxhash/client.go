// Package fxhash provides a client for the fxhash v2 GraphQL API.
// fxhash is a generative art platform on Tezos. Each minted edition (called a "gentk")
// belongs to a generative token (the project/release). This client fetches gentk data
// including its release membership and artist using the fxhash v2 API's onchain.objkt_by_pk
// query, keyed by "{contract}-{tokenID}".
//
// GetGentksByIteration uses the bulk objkt list query to resolve on-chain token identities
// for a range of mint iterations, which is required for release-level CID derivation.
package fxhash

import (
	"bytes"
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/feral-file/ff-indexer-v2/internal/adapter"
	"github.com/feral-file/ff-indexer-v2/internal/ratelimit"
)

// fxhashIterationPageSize is the number of gentks fetched per request in GetGentksByIteration.
// Chosen conservatively to stay within Hasura's default row limit.
const fxhashIterationPageSize = 100

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

// GentkRef holds the minimal on-chain identity of a fxhash gentk, resolved from
// a bulk iteration-range query. Used for release-level CID derivation without
// full metadata enrichment.
type GentkRef struct {
	// ContractAddress is the Tezos KT1 contract address of the FA2 token.
	ContractAddress string
	// TokenID is the on-chain FA2 token ID (numeric string).
	TokenID string
	// Iteration is the 1-based mint number within the generative token.
	Iteration int64
}

// gqlListResponse wraps the fxhash v2 API response for bulk gentk list queries
// (onchain.objkt with where/limit/offset). Only the fields needed for CID
// derivation are decoded.
type gqlListResponse struct {
	Data struct {
		Onchain struct {
			Objkt []struct {
				// ID is the composite "{contract}-{tokenID}" key used by the fxhash API.
				ID        string `json:"id"`
				Iteration string `json:"iteration"` // numeric string, 1-based
			} `json:"objkt"`
		} `json:"onchain"`
	} `json:"data"`
	Errors []struct {
		Message string `json:"message"`
	} `json:"errors"`
}

// gqlResolveSlugResponse wraps the fxhash v2 API response for slug → token ID lookup.
type gqlResolveSlugResponse struct {
	Data struct {
		Onchain struct {
			GenerativeToken []struct {
				ID string `json:"id"`
			} `json:"generative_token"`
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

	// GetGentksByIteration fetches fxhash gentks for a generative token within a 1-based
	// iteration range [iterationFrom, iterationTo] (inclusive). Results are paginated
	// internally so the caller always receives the full requested range.
	//
	// Gentk token IDs are global integers that cannot be derived from mint numbers
	// by math alone — this API call is required for release-level CID derivation.
	// The returned GentkRefs include the on-chain (ContractAddress, TokenID) pair
	// needed to build a tezos:mainnet:fa2:{contract}:{tokenID} CID.
	//
	// Returns an empty slice when no gentks exist for the given iteration range
	// (e.g. a range beyond the current supply). Never returns a partial result on
	// error; any HTTP or GraphQL error causes the method to return an error.
	GetGentksByIteration(ctx context.Context, generativeTokenID string, iterationFrom, iterationTo int64) ([]GentkRef, error)

	// ResolveSlug resolves a fxhash URL slug (e.g. "industrial-park") to the numeric
	// generative token ID (e.g. "9997") used as vendor_release_id.
	// Returns an error when the slug does not match any known generative token.
	ResolveSlug(ctx context.Context, slug string) (string, error)
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

// getGentkQuery is the static GraphQL query for GetGentk.
// Using a variable rather than fmt.Sprintf interpolation prevents GraphQL injection
// from malformed contractAddress or tokenID values.
const getGentkQuery = `query GetGentk($id: String!) {
  onchain {
    objkt_by_pk(id: $id) {
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
}`

type getGentkVars struct {
	ID string `json:"id"`
}

// GetGentk fetches a fxhash gentk by its on-chain identity.
// The fxhash v2 API identifies gentks by the composite key "{contract}-{tokenID}".
// This matches the on-chain (fa_contract, token_id) pair used by objkt.
func (c *fxhashClient) GetGentk(ctx context.Context, contractAddress, tokenID string) (*Gentk, error) {
	// The fxhash v2 objkt_by_pk id is "{contract}-{tokenID}".
	gentkID := fmt.Sprintf("%s-%s", contractAddress, tokenID)

	reqBody := GQLRequest{
		Query:         getGentkQuery,
		Variables:     getGentkVars{ID: gentkID},
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

// getGentksByIterationQuery is the static GraphQL query for GetGentksByIteration.
// All caller-supplied values (generative token ID, iteration bounds, page controls)
// are passed via variables to prevent GraphQL injection.
//
// order_by: [{iteration: asc}] is required for deterministic multi-page pagination.
// Without a stable sort, Hasura can return different row orderings across pages,
// causing iterations to be duplicated or skipped when the span exceeds one page.
//
// iteration is a numeric column in the fxhash Postgres schema (confirmed by live API).
// Hasura serializes numeric values as JSON strings in responses and expects numeric variables.
// limit/offset use Int! (Hasura's standard pagination scalar).
const getGentksByIterationQuery = `query GetGentksByIteration($tokenId: String!, $iterationFrom: numeric!, $iterationTo: numeric!, $limit: Int!, $offset: Int!) {
  onchain {
    objkt(
      where: {generative_token: {id: {_eq: $tokenId}}, iteration: {_gte: $iterationFrom, _lte: $iterationTo}}
      order_by: [{iteration: asc}]
      limit: $limit
      offset: $offset
    ) {
      id
      iteration
    }
  }
}`

type getGentksByIterationVars struct {
	TokenID       string `json:"tokenId"`
	IterationFrom int64  `json:"iterationFrom"`
	IterationTo   int64  `json:"iterationTo"`
	Limit         int    `json:"limit"`
	Offset        int64  `json:"offset"`
}

// GetGentksByIteration fetches all fxhash gentks for a generative token within the
// given 1-based iteration range, paginating internally with fxhashIterationPageSize.
//
// Reason: fxhash gentk token IDs are global integers assigned at mint time and cannot
// be derived from iteration numbers by math. This query resolves the mapping from
// iteration (mint number) → on-chain (contract, tokenID) so that CIDs can be built
// for release-level indexing.
func (c *fxhashClient) GetGentksByIteration(ctx context.Context, generativeTokenID string, iterationFrom, iterationTo int64) ([]GentkRef, error) {
	var all []GentkRef
	offset := int64(0)

	for {
		reqBody := GQLRequest{
			Query: getGentksByIterationQuery,
			Variables: getGentksByIterationVars{
				TokenID:       generativeTokenID,
				IterationFrom: iterationFrom,
				IterationTo:   iterationTo,
				Limit:         fxhashIterationPageSize,
				Offset:        offset,
			},
			OperationName: "GetGentksByIteration",
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

		var resp gqlListResponse
		if err := c.json.Unmarshal(responseBody, &resp); err != nil {
			return nil, fmt.Errorf("failed to unmarshal fxhash response: %w", err)
		}

		if len(resp.Errors) > 0 {
			return nil, fmt.Errorf("fxhash API error: %s", resp.Errors[0].Message)
		}

		page := resp.Data.Onchain.Objkt
		for _, item := range page {
			ref, err := parseGentkRef(item.ID, item.Iteration)
			if err != nil {
				// Return an error for malformed entries. A parse failure here means the
				// fxhash API returned an unexpected token shape, which indicates an API
				// schema change. Surfacing it immediately is preferable to silently
				// producing a truncated result that drops requested mint numbers.
				return nil, fmt.Errorf("malformed fxhash gentk ref: %w", err)
			}
			all = append(all, ref)
		}

		if len(page) < fxhashIterationPageSize {
			// Last page: no more results to fetch.
			break
		}
		offset += int64(fxhashIterationPageSize)
	}

	return all, nil
}

// resolveSlugQuery is the static GraphQL query for ResolveSlug.
// The slug variable is caller-supplied and must not be interpolated into the query string.
const resolveSlugQuery = `query ResolveSlug($slug: String!) {
  onchain {
    generative_token(where: {slug: {_eq: $slug}}, limit: 1) {
      id
    }
  }
}`

type resolveSlugVars struct {
	Slug string `json:"slug"`
}

// ResolveSlug resolves a fxhash URL slug to the generative token ID used as vendor_release_id.
//
// Queries onchain.generative_token(where:{slug:{_eq:$slug}}) to map the human-readable slug
// from fxhash.xyz URLs to the stable numeric token ID. The numeric ID is used internally
// because it is stable; slugs can be renamed by artists.
func (c *fxhashClient) ResolveSlug(ctx context.Context, slug string) (string, error) {
	reqBody := GQLRequest{
		Query:         resolveSlugQuery,
		Variables:     resolveSlugVars{Slug: slug},
		OperationName: "ResolveSlug",
	}

	bodyBytes, err := c.json.Marshal(reqBody)
	if err != nil {
		return "", fmt.Errorf("failed to marshal fxhash request: %w", err)
	}

	responseBody, err := ratelimit.Do(ctx, c.rateLimiter, PROVIDER_NAME, func(ctx context.Context) ([]byte, error) {
		headers := map[string]string{
			"Content-Type": "application/json",
		}
		return c.httpClient.PostBytes(ctx, c.apiURL, headers, bytes.NewReader(bodyBytes))
	})
	if err != nil {
		return "", fmt.Errorf("failed to call fxhash v2 API: %w", err)
	}

	var resp gqlResolveSlugResponse
	if err := c.json.Unmarshal(responseBody, &resp); err != nil {
		return "", fmt.Errorf("failed to unmarshal fxhash response: %w", err)
	}

	if len(resp.Errors) > 0 {
		return "", fmt.Errorf("fxhash API error: %s", resp.Errors[0].Message)
	}

	tokens := resp.Data.Onchain.GenerativeToken
	if len(tokens) == 0 {
		return "", fmt.Errorf("fxhash slug not found: %q", slug)
	}

	return tokens[0].ID, nil
}

// parseGentkRef parses a fxhash composite id "{contract}-{tokenID}" and iteration
// string into a GentkRef. The separator is the last hyphen in the id because Tezos
// KT1 contract addresses use only Base58 characters (no hyphens).
func parseGentkRef(compositeID, iterationStr string) (GentkRef, error) {
	sep := strings.LastIndex(compositeID, "-")
	if sep < 0 {
		return GentkRef{}, fmt.Errorf("invalid fxhash composite id (no separator): %q", compositeID)
	}
	contract := compositeID[:sep]
	tokenID := compositeID[sep+1:]
	if contract == "" || tokenID == "" {
		return GentkRef{}, fmt.Errorf("invalid fxhash composite id (empty part): %q", compositeID)
	}

	iteration, err := strconv.ParseInt(iterationStr, 10, 64)
	if err != nil {
		return GentkRef{}, fmt.Errorf("invalid iteration %q: %w", iterationStr, err)
	}

	return GentkRef{
		ContractAddress: contract,
		TokenID:         tokenID,
		Iteration:       iteration,
	}, nil
}
