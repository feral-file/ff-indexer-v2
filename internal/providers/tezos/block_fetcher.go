package tezos

import (
	"context"
	"fmt"
	"time"

	"github.com/feral-file/ff-indexer-v2/internal/adapter"
	"github.com/feral-file/ff-indexer-v2/internal/block"
	"github.com/feral-file/ff-indexer-v2/internal/ratelimit"
)

// TezosBlockFetcher implements block.BlockFetcher for Tezos
type TezosBlockFetcher struct {
	baseURL     string
	httpClient  adapter.HTTPClient
	rateLimiter ratelimit.Limiter
	clock       adapter.Clock
}

// NewTezosBlockFetcher creates a new Tezos block fetcher.
//
// Reason: the fetcher shares the "tzkt" rate-limiter bucket with the TzKT REST client —
// its requests hit the same api.tzkt.io per-IP quota, and unthrottled head fetches were
// part of the traffic mix behind the 2026-08-18 429 crash-loop. rateLimiter may be nil
// (tests), in which case calls pass through unthrottled.
func NewTezosBlockFetcher(baseURL string, httpClient adapter.HTTPClient, rateLimiter ratelimit.Limiter, clock adapter.Clock) block.BlockFetcher {
	return &TezosBlockFetcher{
		baseURL:     baseURL,
		httpClient:  httpClient,
		rateLimiter: rateLimiter,
		clock:       clock,
	}
}

// FetchLatestBlock fetches the latest block level from TzKT
func (f *TezosBlockFetcher) FetchLatestBlock(ctx context.Context) (uint64, error) {
	type headResponse struct {
		Level uint64 `json:"level"`
	}
	head, err := ratelimit.Do(ctx, f.rateLimiter, PROVIDER_NAME, func(ctx context.Context) (headResponse, error) {
		url := fmt.Sprintf("%s/v1/head", f.baseURL)

		var head headResponse
		if err := f.httpClient.GetAndUnmarshal(ctx, url, &head); err != nil {
			return headResponse{}, fmt.Errorf("failed to get latest block from TzKT: %w", err)
		}
		return head, nil
	})
	if err != nil {
		return 0, err
	}
	return head.Level, nil
}

// FetchBlockTimestamp fetches the timestamp for a given block level from TzKT
func (f *TezosBlockFetcher) FetchBlockTimestamp(ctx context.Context, blockNumber uint64) (time.Time, error) {
	type blockResponse struct {
		Level     uint64 `json:"level"`
		Timestamp string `json:"timestamp"`
	}
	blockInfo, err := ratelimit.Do(ctx, f.rateLimiter, PROVIDER_NAME, func(ctx context.Context) (blockResponse, error) {
		url := fmt.Sprintf("%s/v1/blocks/%d", f.baseURL, blockNumber)

		var blockInfo blockResponse
		if err := f.httpClient.GetAndUnmarshal(ctx, url, &blockInfo); err != nil {
			return blockResponse{}, fmt.Errorf("failed to get block %d from TzKT: %w", blockNumber, err)
		}
		return blockInfo, nil
	})
	if err != nil {
		return time.Time{}, err
	}

	// Parse the timestamp (TzKT returns RFC3339 format)
	timestamp, err := f.clock.Parse(time.RFC3339, blockInfo.Timestamp)
	if err != nil {
		return time.Time{}, fmt.Errorf("failed to parse timestamp for block %d: %w", blockNumber, err)
	}

	return timestamp, nil
}
