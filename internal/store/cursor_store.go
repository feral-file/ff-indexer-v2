package store

import (
	"context"
	"errors"
	"fmt"
	"strconv"

	"gorm.io/gorm"

	"github.com/feral-file/ff-indexer-v2/internal/store/schema"
)

// CursorStore defines the interface for storing and retrieving block cursors
type CursorStore interface {
	// GetBlockCursor retrieves the last processed block number for a chain
	GetBlockCursor(ctx context.Context, chain string) (uint64, error)
	// SetBlockCursor stores the last processed block number for a chain
	SetBlockCursor(ctx context.Context, chain string, blockNumber uint64) error
}

type cursorStore struct {
	db *gorm.DB
}

// NewCursorStore creates a new cursor store
func NewCursorStore(db *gorm.DB) CursorStore {
	return &cursorStore{db: db}
}

// GetBlockCursor retrieves the last processed block number for a chain
func (s *cursorStore) GetBlockCursor(ctx context.Context, chain string) (uint64, error) {
	key := fmt.Sprintf("block_cursor:%s", chain)

	var kv schema.KeyValueStore
	err := s.db.WithContext(ctx).Where("key = ?", key).First(&kv).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return 0, nil // Return 0 if no cursor exists
		}
		return 0, fmt.Errorf("failed to get block cursor: %w", err)
	}

	blockNumber, err := strconv.ParseUint(kv.Value, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("failed to parse block cursor: %w", err)
	}

	return blockNumber, nil
}

// SetBlockCursor stores the last processed block number for a chain
func (s *cursorStore) SetBlockCursor(ctx context.Context, chain string, blockNumber uint64) error {
	key := fmt.Sprintf("block_cursor:%s", chain)
	value := strconv.FormatUint(blockNumber, 10)

	kv := schema.KeyValueStore{
		Key:   key,
		Value: value,
	}

	err := s.db.WithContext(ctx).Save(&kv).Error
	if err != nil {
		return fmt.Errorf("failed to set block cursor: %w", err)
	}

	return nil
}
