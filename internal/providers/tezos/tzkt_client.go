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

// TzKTClient defines an interface for TzKT API client operations to enable mocking
//
//go:generate mockgen -source=tzkt_client.go -destination=../../mocks/tzkt_client.go -package=mocks -mock_names=TzKTClient=MockTzKTClient
type TzKTClient interface {
	GetTransactionByID(ctx context.Context, txID uint64) (*TzKTTransaction, error)
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

// GetTransactionByID retrieves a transaction by its TzKT transaction ID
func (c *tzktClient) GetTransactionByID(ctx context.Context, txID uint64) (*TzKTTransaction, error) {
	url := fmt.Sprintf("%s/v1/operations/transactions/%d", c.baseURL, txID)

	var tx TzKTTransaction
	if err := c.httpClient.Get(ctx, url, &tx); err != nil {
		return nil, fmt.Errorf("failed to get transaction %d: %w", txID, err)
	}

	return &tx, nil
}
