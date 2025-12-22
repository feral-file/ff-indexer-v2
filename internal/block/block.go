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

// BlockTimestampCache represents cached timestamp for a specific block number
type BlockTimestampCache struct {
	Timestamp time.Time
	CachedAt  time.Time
}

// BlockProvider provides cached access to the latest block information
// and block timestamps for any block number.
// It reduces RPC calls to blockchain providers (Infura, TzKT) by caching the
// latest block number and block timestamps for a configurable TTL period.
//
//go:generate mockgen -source=block.go -destination=../mocks/block_provider.go -package=mocks -mock_names=BlockProvider=MockBlockProvider
type BlockProvider interface {
	// GetLatestBlock returns the latest block number, potentially from cache
	GetLatestBlock(ctx context.Context) (uint64, error)

	// GetBlockTimestamp returns the timestamp for a given block number, potentially from cache
	GetBlockTimestamp(ctx context.Context, blockNumber uint64) (time.Time, error)
}

// BlockFetcher is the interface for fetching block information from the blockchain
//
//go:generate mockgen -source=block.go -destination=../mocks/block_provider.go -package=mocks -mock_names=BlockFetcher=MockBlockFetcher
type BlockFetcher interface {
	// FetchLatestBlock fetches the latest block from the blockchain
	FetchLatestBlock(ctx context.Context) (uint64, error)

	// FetchBlockTimestamp fetches the timestamp for a given block number
	FetchBlockTimestamp(ctx context.Context, blockNumber uint64) (time.Time, error)
}

// Config holds configuration for the BlockProvider
type Config struct {
	// TTL is how long to cache the block number
	TTL time.Duration

	// StaleWindow is how long to use stale data if fetching fails
	// If the cached data is older than this and fetch fails, return error
	StaleWindow time.Duration

	// BlockTimestampTTL is how long to cache block timestamps
	// Block timestamps are immutable once confirmed, so this can be quite long
	// Set to 0 to cache forever (recommended for confirmed blocks)
	BlockTimestampTTL time.Duration
}

// blockProvider implements BlockProvider with TTL-based caching
type blockProvider struct {
	fetcher BlockFetcher
	config  Config
	clock   adapter.Clock

	mu              sync.RWMutex
	blockInfo       *BlockInfo
	blockTimestamps map[uint64]*BlockTimestampCache
}

// NewBlockProvider creates a new BlockProvider with caching
func NewBlockProvider(fetcher BlockFetcher, config Config, clock adapter.Clock) BlockProvider {
	return &blockProvider{
		fetcher:         fetcher,
		config:          config,
		clock:           clock,
		blockTimestamps: make(map[uint64]*BlockTimestampCache),
	}
}

// GetLatestBlock returns the latest block number, using cache if valid
func (p *blockProvider) GetLatestBlock(ctx context.Context) (uint64, error) {
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

// GetBlockTimestamp returns the timestamp for a given block number, using cache if valid
func (p *blockProvider) GetBlockTimestamp(ctx context.Context, blockNumber uint64) (time.Time, error) {
	// Check if cache is still valid
	p.mu.RLock()
	cached := p.blockTimestamps[blockNumber]
	p.mu.RUnlock()

	now := p.clock.Now()

	// If cache is valid (within TTL or TTL is 0 for permanent cache), return cached value
	if cached != nil && (p.config.BlockTimestampTTL == 0 || now.Sub(cached.CachedAt) < p.config.BlockTimestampTTL) {
		logger.DebugCtx(ctx, "Using cached block timestamp",
			zap.Uint64("block_number", blockNumber),
			zap.Time("timestamp", cached.Timestamp))
		return cached.Timestamp, nil
	}

	// Cache expired or doesn't exist, fetch fresh data
	logger.DebugCtx(ctx, "Fetching block timestamp from blockchain provider",
		zap.Uint64("block_number", blockNumber))
	timestamp, err := p.fetcher.FetchBlockTimestamp(ctx, blockNumber)
	if err != nil {
		// If fetch failed, check if we can use stale cache
		if cached != nil && now.Sub(cached.CachedAt) < p.config.StaleWindow {
			// Use stale cache as fallback
			logger.DebugCtx(ctx, "Using stale block timestamp",
				zap.Uint64("block_number", blockNumber),
				zap.Time("timestamp", cached.Timestamp))
			return cached.Timestamp, nil
		}
		// No valid cache available and fetch failed
		return time.Time{}, fmt.Errorf("failed to fetch block timestamp for block %d and no valid cache available: %w", blockNumber, err)
	}

	// Update cache with fresh data
	p.mu.Lock()
	p.blockTimestamps[blockNumber] = &BlockTimestampCache{
		Timestamp: timestamp,
		CachedAt:  now,
	}
	p.mu.Unlock()

	return timestamp, nil
}
