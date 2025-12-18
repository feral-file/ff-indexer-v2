package block

import (
	"context"
	"fmt"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/feral-file/ff-indexer-v2/internal/adapter"
	"github.com/feral-file/ff-indexer-v2/internal/logger"
)

// BlockInfo represents cached block information
type BlockInfo struct {
	Number    uint64
	Timestamp time.Time
}

// BlockHeadProvider provides cached access to the latest block information
// It reduces RPC calls to blockchain providers (Infura, TzKT) by caching the
// latest block number for a configurable TTL period.
//
//go:generate mockgen -source=head.go -destination=../mocks/block_head_provider.go -package=mocks -mock_names=BlockHeadProvider=MockBlockHeadProvider
type BlockHeadProvider interface {
	// GetLatestBlock returns the latest block number, potentially from cache
	GetLatestBlock(ctx context.Context) (uint64, error)
}

// BlockFetcher is the interface for fetching the latest block from the blockchain
//
//go:generate mockgen -source=head.go -destination=../mocks/block_head_provider.go -package=mocks -mock_names=BlockFetcher=MockBlockFetcher
type BlockFetcher interface {
	// FetchLatestBlock fetches the latest block from the blockchain
	FetchLatestBlock(ctx context.Context) (uint64, error)
}

// Config holds configuration for the BlockHeadProvider
type Config struct {
	// TTL is how long to cache the block number
	TTL time.Duration

	// StaleWindow is how long to use stale data if fetching fails
	// If the cached data is older than this and fetch fails, return error
	StaleWindow time.Duration
}

// blockHeadProvider implements BlockHeadProvider with TTL-based caching
type blockHeadProvider struct {
	fetcher BlockFetcher
	config  Config
	clock   adapter.Clock

	mu        sync.RWMutex
	blockInfo *BlockInfo
}

// NewBlockHeadProvider creates a new BlockHeadProvider with caching
func NewBlockHeadProvider(fetcher BlockFetcher, config Config, clock adapter.Clock) BlockHeadProvider {
	return &blockHeadProvider{
		fetcher: fetcher,
		config:  config,
		clock:   clock,
	}
}

// GetLatestBlock returns the latest block number, using cache if valid
func (p *blockHeadProvider) GetLatestBlock(ctx context.Context) (uint64, error) {
	// Check if cache is still valid
	p.mu.RLock()
	cached := p.blockInfo
	p.mu.RUnlock()

	now := p.clock.Now()

	// If cache is valid (within TTL), return cached value
	if cached != nil && now.Sub(cached.Timestamp) < p.config.TTL {
		logger.DebugCtx(ctx, "Using cached block number", zap.Uint64("block_number", cached.Number))
		return cached.Number, nil
	}

	// Cache expired or doesn't exist, fetch fresh data
	logger.DebugCtx(ctx, "Fetching latest block number from blockchain provider")
	blockNumber, err := p.fetcher.FetchLatestBlock(ctx)
	if err != nil {
		// If fetch failed, check if we can use stale cache
		if cached != nil && now.Sub(cached.Timestamp) < p.config.StaleWindow {
			// Use stale cache as fallback
			logger.DebugCtx(ctx, "Using stale block number", zap.Uint64("block_number", cached.Number))
			return cached.Number, nil
		}
		// No valid cache available and fetch failed
		return 0, fmt.Errorf("failed to fetch latest block and no valid cache available: %w", err)
	}

	// Update cache with fresh data
	p.mu.Lock()
	p.blockInfo = &BlockInfo{
		Number:    blockNumber,
		Timestamp: now,
	}
	p.mu.Unlock()

	return blockNumber, nil
}
