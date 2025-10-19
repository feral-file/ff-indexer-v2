package tezos

import (
	"context"
	"fmt"

	"github.com/feral-file/ff-indexer-v2/internal/adapter"
)

// TzKTTransaction represents a transaction from the TzKT API
type TzKTTransaction struct {
	Hash  string `json:"hash"`
	Level uint64 `json:"level"`
	Block string `json:"block"`
}

// TzKTTokenBalance represents a token balance from the TzKT API
type TzKTTokenBalance struct {
	Account   TzKTAccount `json:"account"`
	Balance   string      `json:"balance"`
	TokenID   string      `json:"tokenId"`
	LastLevel uint64      `json:"lastLevel"`
	LastTime  string      `json:"lastTime"`
}

// TzKTAccount represents an account address
type TzKTAccount struct {
	Address string `json:"address"`
}

// TzKTToken represents token information from the TzKT API
type TzKTToken struct {
	Contract TzKTContract           `json:"contract"`
	TokenID  string                 `json:"tokenId"`
	Standard string                 `json:"standard"`
	Metadata map[string]interface{} `json:"metadata"`
}

// TzKTContract represents a contract address
type TzKTContract struct {
	Address string `json:"address"`
}

// TzKTClient defines an interface for TzKT API client operations to enable mocking
//
//go:generate mockgen -source=tzkt_client.go -destination=../../mocks/tzkt_client.go -package=mocks -mock_names=TzKTClient=MockTzKTClient
type TzKTClient interface {
	// GetTransactionsByID retrieves transactions by its TzKT operation ID (internal database ID)
	GetTransactionsByID(ctx context.Context, txID uint64) ([]TzKTTransaction, error)

	// GetTokenBalances retrieves token balances for a specific token
	GetTokenBalances(ctx context.Context, contractAddress string, tokenID string) ([]TzKTTokenBalance, error)

	// GetTokenMetadata retrieves token metadata for a specific token
	GetTokenMetadata(ctx context.Context, contractAddress string, tokenID string) (map[string]interface{}, error)
}

// tzktClient is the concrete implementation of TzKTClient
type tzktClient struct {
	baseURL    string
	httpClient adapter.HTTPClient
}

// NewTzKTClient creates a new TzKT API client
func NewTzKTClient(baseURL string, httpClient adapter.HTTPClient) TzKTClient {
	return &tzktClient{
		baseURL:    baseURL,
		httpClient: httpClient,
	}
}

// GetTransactionsByID retrieves transactions by its TzKT operation ID (internal database ID)
func (c *tzktClient) GetTransactionsByID(ctx context.Context, txID uint64) ([]TzKTTransaction, error) {
	url := fmt.Sprintf("%s/v1/operations/transactions?id=%d", c.baseURL, txID)

	var txs []TzKTTransaction
	if err := c.httpClient.Get(ctx, url, &txs); err != nil {
		return nil, fmt.Errorf("failed to get transaction %d: %w", txID, err)
	}

	return txs, nil
}

// GetTokenBalances retrieves token balances for a specific token
// Returns list of holders with their balances (balance > 0)
func (c *tzktClient) GetTokenBalances(ctx context.Context, contractAddress string, tokenID string) ([]TzKTTokenBalance, error) {
	// TzKT API: GET /v1/tokens/balances?contract={address}&tokenId={id}&balance.gt=0
	url := fmt.Sprintf("%s/v1/tokens/balances?contract=%s&tokenId=%s&balance.gt=0", c.baseURL, contractAddress, tokenID)

	var balances []TzKTTokenBalance
	if err := c.httpClient.Get(ctx, url, &balances); err != nil {
		return nil, fmt.Errorf("failed to get token balances for %s/%s: %w", contractAddress, tokenID, err)
	}

	return balances, nil
}

// GetTokenMetadata retrieves token metadata for a specific token
func (c *tzktClient) GetTokenMetadata(ctx context.Context, contractAddress string, tokenID string) (map[string]interface{}, error) {
	// TzKT API: GET /v1/tokens?contract={address}&tokenId={id}
	url := fmt.Sprintf("%s/v1/tokens?contract=%s&tokenId=%s", c.baseURL, contractAddress, tokenID)

	var tokens []TzKTToken
	if err := c.httpClient.Get(ctx, url, &tokens); err != nil {
		return nil, fmt.Errorf("failed to get token metadata for %s/%s: %w", contractAddress, tokenID, err)
	}

	if len(tokens) == 0 {
		return nil, fmt.Errorf("token not found: %s/%s", contractAddress, tokenID)
	}

	return tokens[0].Metadata, nil
}
