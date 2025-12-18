package tezos

import (
	"context"
	"fmt"

	"github.com/feral-file/ff-indexer-v2/internal/adapter"
	"github.com/feral-file/ff-indexer-v2/internal/block"
)

// TezosBlockFetcher implements block.BlockFetcher for Tezos
type TezosBlockFetcher struct {
	baseURL    string
	httpClient adapter.HTTPClient
}

// NewTezosBlockFetcher creates a new Tezos block fetcher
func NewTezosBlockFetcher(baseURL string, httpClient adapter.HTTPClient) block.BlockFetcher {
	return &TezosBlockFetcher{
		baseURL:    baseURL,
		httpClient: httpClient,
	}
}

// FetchLatestBlock fetches the latest block level from TzKT
func (f *TezosBlockFetcher) FetchLatestBlock(ctx context.Context) (uint64, error) {
	url := fmt.Sprintf("%s/v1/head", f.baseURL)

	var head struct {
		Level uint64 `json:"level"`
	}

	if err := f.httpClient.Get(ctx, url, &head); err != nil {
		return 0, fmt.Errorf("failed to get latest block from TzKT: %w", err)
	}

	return head.Level, nil
}
