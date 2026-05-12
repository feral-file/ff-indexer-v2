package ingestion

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/feral-file/ff-indexer-v2/internal/adapter"
	"github.com/feral-file/ff-indexer-v2/internal/domain"
	"github.com/feral-file/ff-indexer-v2/internal/mocks"
	"github.com/feral-file/ff-indexer-v2/internal/store/schema"
	"github.com/feral-file/ff-indexer-v2/internal/types"
)

// TestFlushBlock_MonotonicCursorGuard_RejectsOldBlocks verifies that blocks
// older than the current cursor are rejected (prevents regression).
func TestFlushBlock_MonotonicCursorGuard_RejectsOldBlocks(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	source := mocks.NewMockBlockchainEventSource(ctrl)
	store := mocks.NewMockStore(ctrl)
	jq := mocks.NewMockJobQueue(ctrl)
	blacklist := mocks.NewMockBlacklistRegistry(ctrl)
	clock := adapter.NewClock()

	// Allow any GetBlockCursor calls
	store.EXPECT().GetBlockCursor(gomock.Any(), gomock.Any()).Return(uint64(100), nil).AnyTimes()

	// No SetBlockCursor should be called (block is rejected)

	source.EXPECT().Close()

	ctx := context.Background()
	runner := NewRunner(
		ctx, source, store, jq, blacklist,
		Config{
			ChainID:       domain.ChainEthereumMainnet,
			StartBlock:    1000,
			TokenQueue:    "token_index",
			QueueCapacity: 4,
		},
		clock,
	).(*runner)
	defer func() { _ = runner.Close() }()

	// Current cursor is 100, trying to flush block 50 (old)
	block := &blockBuffer{
		blockNumber: 50,
		events:      []*domain.BlockchainEvent{},
		firstSeen:   time.Now(),
	}

	newCursor, err := runner.flushBlock(ctx, block, 100)

	require.NoError(t, err, "flushBlock should not return error for old blocks")
	assert.Equal(t, uint64(100), newCursor, "Cursor should remain unchanged")
}

// TestFlushBlock_MonotonicCursorGuard_AllowsSameHeightBlocks verifies that
// blocks at the same height as cursor are processed (allows late arrivals
// after timeout flush).
func TestFlushBlock_MonotonicCursorGuard_AllowsSameHeightBlocks(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	source := mocks.NewMockBlockchainEventSource(ctrl)
	store := mocks.NewMockStore(ctrl)
	jq := mocks.NewMockJobQueue(ctrl)
	blacklist := mocks.NewMockBlacklistRegistry(ctrl)
	clock := adapter.NewClock()

	// Allow any GetBlockCursor calls
	store.EXPECT().GetBlockCursor(gomock.Any(), gomock.Any()).Return(uint64(100), nil).AnyTimes()

	// Event processing expectations
	blacklist.EXPECT().IsTokenCIDBlacklisted(gomock.Any()).Return(false)
	store.EXPECT().GetTokenByTokenCID(gomock.Any(), gomock.Any()).Return(nil, nil)
	store.EXPECT().IsAnyAddressWatched(gomock.Any(), gomock.Any(), gomock.Any()).Return(true, nil)
	jq.EXPECT().Enqueue(gomock.Any(), gomock.Any()).Return(&schema.Job{ID: 1}, true, nil)

	// SetBlockCursor should be called (same height is allowed)
	store.EXPECT().SetBlockCursor(gomock.Any(), string(domain.ChainEthereumMainnet), uint64(100)).Return(nil)

	source.EXPECT().Close()

	ctx := context.Background()
	runner := NewRunner(
		ctx, source, store, jq, blacklist,
		Config{
			ChainID:       domain.ChainEthereumMainnet,
			StartBlock:    1000,
			TokenQueue:    "token_index",
			QueueCapacity: 4,
		},
		clock,
	).(*runner)
	defer func() { _ = runner.Close() }()

	// Current cursor is 100, trying to flush block 100 (same height)
	event := &domain.BlockchainEvent{
		Chain:           domain.ChainEthereumMainnet,
		BlockNumber:     100,
		EventType:       domain.EventTypeTransfer,
		TxHash:          "0xtest",
		FromAddress:     types.StringPtr("0xfrom"),
		ToAddress:       types.StringPtr("0xto"),
		ContractAddress: "0xcontract",
		TokenNumber:     "1",
		Standard:        "erc721",
		Quantity:        "1",
	}

	block := &blockBuffer{
		blockNumber: 100,
		events:      []*domain.BlockchainEvent{event},
		firstSeen:   time.Now(),
	}

	newCursor, err := runner.flushBlock(ctx, block, 100)

	require.NoError(t, err, "flushBlock should succeed for same-height blocks")
	assert.Equal(t, uint64(100), newCursor, "Cursor should be set to block height")
}

// TestFlushBlock_MonotonicCursorGuard_AllowsNewerBlocks verifies that blocks
// newer than the current cursor are processed and advance the cursor.
func TestFlushBlock_MonotonicCursorGuard_AllowsNewerBlocks(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	source := mocks.NewMockBlockchainEventSource(ctrl)
	store := mocks.NewMockStore(ctrl)
	jq := mocks.NewMockJobQueue(ctrl)
	blacklist := mocks.NewMockBlacklistRegistry(ctrl)
	clock := adapter.NewClock()

	// Allow any GetBlockCursor calls
	store.EXPECT().GetBlockCursor(gomock.Any(), gomock.Any()).Return(uint64(100), nil).AnyTimes()

	// Event processing expectations
	blacklist.EXPECT().IsTokenCIDBlacklisted(gomock.Any()).Return(false)
	store.EXPECT().GetTokenByTokenCID(gomock.Any(), gomock.Any()).Return(nil, nil)
	store.EXPECT().IsAnyAddressWatched(gomock.Any(), gomock.Any(), gomock.Any()).Return(true, nil)
	jq.EXPECT().Enqueue(gomock.Any(), gomock.Any()).Return(&schema.Job{ID: 1}, true, nil)

	// SetBlockCursor should be called with new block number
	store.EXPECT().SetBlockCursor(gomock.Any(), string(domain.ChainEthereumMainnet), uint64(150)).Return(nil)

	source.EXPECT().Close()

	ctx := context.Background()
	runner := NewRunner(
		ctx, source, store, jq, blacklist,
		Config{
			ChainID:       domain.ChainEthereumMainnet,
			StartBlock:    1000,
			TokenQueue:    "token_index",
			QueueCapacity: 4,
		},
		clock,
	).(*runner)
	defer func() { _ = runner.Close() }()

	// Current cursor is 100, trying to flush block 150 (newer)
	event := &domain.BlockchainEvent{
		Chain:           domain.ChainEthereumMainnet,
		BlockNumber:     150,
		EventType:       domain.EventTypeTransfer,
		TxHash:          "0xtest",
		FromAddress:     types.StringPtr("0xfrom"),
		ToAddress:       types.StringPtr("0xto"),
		ContractAddress: "0xcontract",
		TokenNumber:     "1",
		Standard:        "erc721",
		Quantity:        "1",
	}

	block := &blockBuffer{
		blockNumber: 150,
		events:      []*domain.BlockchainEvent{event},
		firstSeen:   time.Now(),
	}

	newCursor, err := runner.flushBlock(ctx, block, 100)

	require.NoError(t, err, "flushBlock should succeed for newer blocks")
	assert.Equal(t, uint64(150), newCursor, "Cursor should advance to new block height")
}
