package store

import (
	"context"

	"github.com/feral-file/ff-indexer-v2/internal/domain"
	"github.com/feral-file/ff-indexer-v2/internal/store/schema"
)

// Store defines the interface for database operations
type Store interface {
	// GetTokenByTokenCID retrieves a token by its canonical ID
	GetTokenByTokenCID(ctx context.Context, tokenCID string) (*schema.Token, error)
	// IsAnyAddressWatched checks if any of the given addresses are being watched on a specific chain
	IsAnyAddressWatched(ctx context.Context, chain domain.Chain, addresses []string) (bool, error)
	// GetBlockCursor retrieves the last processed block number for a chain
	GetBlockCursor(ctx context.Context, chain string) (uint64, error)
	// SetBlockCursor stores the last processed block number for a chain
	SetBlockCursor(ctx context.Context, chain string, blockNumber uint64) error
}
