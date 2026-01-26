package tezos

import (
	"context"
	"fmt"
	"time"

	"github.com/feral-file/ff-indexer-v2/internal/adapter"
	"github.com/feral-file/ff-indexer-v2/internal/block"
)

// TezosBlockFetcher implements block.BlockFetcher for Tezos
type TezosBlockFetcher struct {
	baseURL    string
	httpClient adapter.HTTPClient
	clock      adapter.Clock
}

// NewTezosBlockFetcher creates a new Tezos block fetcher
func NewTezosBlockFetcher(baseURL string, httpClient adapter.HTTPClient, clock adapter.Clock) block.BlockFetcher {
	return &TezosBlockFetcher{
		baseURL:    baseURL,
		httpClient: httpClient,
		clock:      clock,
	}
}

// FetchLatestBlock fetches the latest block level from TzKT
func (f *TezosBlockFetcher) FetchLatestBlock(ctx context.Context) (uint64, error) {
	url := fmt.Sprintf("%s/v1/head", f.baseURL)

	var head struct {
		Level uint64 `json:"level"`
	}

	if err := f.httpClient.GetAndUnmarshal(ctx, url, &head); err != nil {
		return 0, fmt.Errorf("failed to get latest block from TzKT: %w", err)
	}

	return head.Level, nil
}

// FetchBlockTimestamp fetches the timestamp for a given block level from TzKT
func (f *TezosBlockFetcher) FetchBlockTimestamp(ctx context.Context, blockNumber uint64) (time.Time, error) {
	url := fmt.Sprintf("%s/v1/blocks/%d", f.baseURL, blockNumber)

	var blockInfo struct {
		Level     uint64 `json:"level"`
		Timestamp string `json:"timestamp"`
	}

	if err := f.httpClient.GetAndUnmarshal(ctx, url, &blockInfo); err != nil {
		return time.Time{}, fmt.Errorf("failed to get block %d from TzKT: %w", blockNumber, err)
	}

	// Parse the timestamp (TzKT returns RFC3339 format)
	timestamp, err := f.clock.Parse(time.RFC3339, blockInfo.Timestamp)
	if err != nil {
		return time.Time{}, fmt.Errorf("failed to parse timestamp for block %d: %w", blockNumber, err)
	}

	return timestamp, nil
}
