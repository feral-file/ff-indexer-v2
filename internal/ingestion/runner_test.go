package ingestion_test

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/feral-file/ff-indexer-v2/internal/adapter"
	"github.com/feral-file/ff-indexer-v2/internal/blockchain"
	"github.com/feral-file/ff-indexer-v2/internal/domain"
	"github.com/feral-file/ff-indexer-v2/internal/ingestion"
	"github.com/feral-file/ff-indexer-v2/internal/logger"
	"github.com/feral-file/ff-indexer-v2/internal/mocks"
	"github.com/feral-file/ff-indexer-v2/internal/providers/jobs"
	"github.com/feral-file/ff-indexer-v2/internal/store/schema"
	"github.com/feral-file/ff-indexer-v2/internal/types"
)

func TestMain(m *testing.M) {
	if err := logger.Initialize(logger.Config{Debug: false}); err != nil {
		panic(err)
	}
	os.Exit(m.Run())
}

func TestRunner_SingleBlockCursorForMultipleEventsInSameBlock(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	source := mocks.NewMockBlockchainEventSource(ctrl)
	store := mocks.NewMockStore(ctrl)
	jq := mocks.NewMockJobQueue(ctrl)
	blacklist := mocks.NewMockBlacklistRegistry(ctrl)
	clock := adapter.NewClock()

	e1 := transferEvent()
	e1.TxHash = "0xtx1"
	e1.BlockNumber = 100
	e2 := transferEvent()
	e2.TxHash = "0xtx2"
	e2.BlockNumber = 100
	e3 := transferEvent()
	e3.TxHash = "0xtx3"
	e3.BlockNumber = 101 // Next block triggers flush of block 100

	// Only e1, e2 are processed (block 100 completes when 101 arrives)
	for _, e := range []*domain.BlockchainEvent{e1, e2} {
		blacklist.EXPECT().IsTokenCIDBlacklisted(e.TokenCID()).Return(false)
		store.EXPECT().GetTokenByTokenCID(gomock.Any(), e.TokenCID().String()).Return(nil, nil)
		store.EXPECT().IsAnyAddressWatched(gomock.Any(), e.Chain, []string{"0xfrom", "0xto"}).Return(true, nil)
		jq.EXPECT().Enqueue(gomock.Any(), gomock.Any()).Return(&schema.Job{ID: 1}, true, nil)
	}

	// Block 100 flushes when block 101 arrives
	store.EXPECT().
		SetBlockCursor(gomock.Any(), string(e1.Chain), uint64(100)).
		Times(1).
		Return(nil)

	// e3 (block 101) is queued but NOT processed - shutdown before processing
	// Block 101 will replay on restart

	source.EXPECT().
		SubscribeEvents(gomock.Any(), uint64(1000), gomock.Any()).
		DoAndReturn(func(ctx context.Context, fromBlock uint64, handler blockchain.EventHandler) error {
			require.Equal(t, uint64(1000), fromBlock)
			require.NoError(t, handler(e1))
			require.NoError(t, handler(e2))
			require.NoError(t, handler(e3))
			// Give flush loop time to process before returning
			time.Sleep(50 * time.Millisecond)
			return context.Canceled
		})

	source.EXPECT().Close()

	ctx := context.Background()

	runner := ingestion.NewRunner(
		ctx,
		source,
		store,
		jq,
		blacklist,
		ingestion.Config{
			ChainID:       domain.ChainEthereumMainnet,
			StartBlock:    1000,
			TokenQueue:    "token_index",
			QueueCapacity: 4,
		},
		clock,
	)
	defer func() { _ = runner.Close() }()

	err := runner.Run(ctx)
	require.Error(t, err)
	assert.ErrorIs(t, err, context.Canceled)
	require.NoError(t, runner.Close())
}

func TestRunner_UsesConfiguredStartBlockAndFlushesEvent(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	source := mocks.NewMockBlockchainEventSource(ctrl)
	store := mocks.NewMockStore(ctrl)
	jq := mocks.NewMockJobQueue(ctrl)
	blacklist := mocks.NewMockBlacklistRegistry(ctrl)
	clock := adapter.NewClock()

	e1 := transferEvent()
	e1.BlockNumber = 100
	e2 := transferEvent()
	e2.TxHash = "0xtx2"
	e2.BlockNumber = 101

	// Process e1, then e2 arrival triggers flush of block 100
	blacklist.EXPECT().IsTokenCIDBlacklisted(e1.TokenCID()).Return(false)
	store.EXPECT().GetTokenByTokenCID(gomock.Any(), e1.TokenCID().String()).Return(nil, nil)
	store.EXPECT().IsAnyAddressWatched(gomock.Any(), e1.Chain, []string{"0xfrom", "0xto"}).Return(true, nil)
	jq.EXPECT().Enqueue(gomock.Any(), gomock.Any()).Return(&schema.Job{ID: 1}, true, nil)

	store.EXPECT().
		SetBlockCursor(gomock.Any(), string(e1.Chain), uint64(100)).
		Return(nil)

	// e2 (block 101) queued but not processed - shutdown before processing

	source.EXPECT().
		SubscribeEvents(gomock.Any(), uint64(1000), gomock.Any()).
		DoAndReturn(func(ctx context.Context, fromBlock uint64, handler blockchain.EventHandler) error {
			require.Equal(t, uint64(1000), fromBlock)
			require.NoError(t, handler(e1))
			require.NoError(t, handler(e2))
			// Give flush loop time to process before returning
			time.Sleep(50 * time.Millisecond)
			return context.Canceled
		})

	source.EXPECT().Close()

	ctx := context.Background()

	runner := ingestion.NewRunner(
		ctx,
		source,
		store,
		jq,
		blacklist,
		ingestion.Config{
			ChainID:       domain.ChainEthereumMainnet,
			StartBlock:    1000,
			TokenQueue:    "token_index",
			QueueCapacity: 4,
		},
		clock,
	)
	defer func() { _ = runner.Close() }()

	err := runner.Run(ctx)
	require.Error(t, err)
	assert.ErrorIs(t, err, context.Canceled)
	require.NoError(t, runner.Close())
}

func TestRunner_UsesLatestBlockWhenNoCursorExists(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	source := mocks.NewMockBlockchainEventSource(ctrl)
	store := mocks.NewMockStore(ctrl)
	jq := mocks.NewMockJobQueue(ctrl)
	blacklist := mocks.NewMockBlacklistRegistry(ctrl)
	clock := adapter.NewClock()

	store.EXPECT().GetBlockCursor(gomock.Any(), string(domain.ChainTezosMainnet)).Return(uint64(0), nil)

	source.EXPECT().GetLatestBlock(gomock.Any()).Return(uint64(42), nil)

	source.EXPECT().
		SubscribeEvents(gomock.Any(), uint64(42), gomock.Any()).
		DoAndReturn(func(ctx context.Context, fromBlock uint64, handler blockchain.EventHandler) error {
			assert.Equal(t, uint64(42), fromBlock)
			return context.Canceled
		})

	source.EXPECT().Close()

	ctx := context.Background()

	runner := ingestion.NewRunner(
		ctx,
		source,
		store,
		jq,
		blacklist,
		ingestion.Config{
			ChainID:       domain.ChainTezosMainnet,
			TokenQueue:    "token_index",
			QueueCapacity: 1,
		},
		clock,
	)
	defer func() { _ = runner.Close() }()

	err := runner.Run(ctx)
	require.Error(t, err)
	assert.ErrorIs(t, err, context.Canceled)
}

// TestRunner_ResumesFromStoredCursorPlusOne verifies the resume contract: a
// stored cursor of N means block N was already flushed, so subscribe must start
// from N+1. Regression coverage for the cursor-loss bug behind this rewrite.
func TestRunner_ResumesFromStoredCursorPlusOne(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	source := mocks.NewMockBlockchainEventSource(ctrl)
	store := mocks.NewMockStore(ctrl)
	jq := mocks.NewMockJobQueue(ctrl)
	blacklist := mocks.NewMockBlacklistRegistry(ctrl)
	clock := adapter.NewClock()

	store.EXPECT().GetBlockCursor(gomock.Any(), string(domain.ChainEthereumMainnet)).
		Return(uint64(100), nil)

	source.EXPECT().
		SubscribeEvents(gomock.Any(), uint64(101), gomock.Any()).
		DoAndReturn(func(ctx context.Context, fromBlock uint64, _ blockchain.EventHandler) error {
			require.Equal(t, uint64(101), fromBlock)
			return context.Canceled
		})

	source.EXPECT().Close()

	ctx := context.Background()
	runner := ingestion.NewRunner(
		ctx, source, store, jq, blacklist,
		ingestion.Config{
			ChainID:       domain.ChainEthereumMainnet,
			TokenQueue:    "token_index",
			QueueCapacity: 4,
		},
		clock,
	)
	defer func() { _ = runner.Close() }()

	err := runner.Run(ctx)
	require.Error(t, err)
	assert.ErrorIs(t, err, context.Canceled)
}

// TestRunner_FailsWhenGetBlockCursorFails covers the resolveStartBlock error
// path: a store error here must surface from Run before any subscription begins.
func TestRunner_FailsWhenGetBlockCursorFails(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	source := mocks.NewMockBlockchainEventSource(ctrl)
	store := mocks.NewMockStore(ctrl)
	jq := mocks.NewMockJobQueue(ctrl)
	blacklist := mocks.NewMockBlacklistRegistry(ctrl)
	clock := adapter.NewClock()

	store.EXPECT().GetBlockCursor(gomock.Any(), string(domain.ChainEthereumMainnet)).
		Return(uint64(0), errors.New("db connection failed"))

	source.EXPECT().Close()

	ctx := context.Background()
	runner := ingestion.NewRunner(
		ctx, source, store, jq, blacklist,
		ingestion.Config{
			ChainID:       domain.ChainEthereumMainnet,
			TokenQueue:    "token_index",
			QueueCapacity: 4,
		},
		clock,
	)
	defer func() { _ = runner.Close() }()

	err := runner.Run(ctx)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to get block cursor")
}

// TestRunner_FailsWhenGetLatestBlockFails covers the cold-start fallback in
// resolveStartBlock: with no stored cursor and StartBlock == 0, the runner
// asks the source for its head; that error must propagate.
func TestRunner_FailsWhenGetLatestBlockFails(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	source := mocks.NewMockBlockchainEventSource(ctrl)
	store := mocks.NewMockStore(ctrl)
	jq := mocks.NewMockJobQueue(ctrl)
	blacklist := mocks.NewMockBlacklistRegistry(ctrl)
	clock := adapter.NewClock()

	store.EXPECT().GetBlockCursor(gomock.Any(), string(domain.ChainEthereumMainnet)).
		Return(uint64(0), nil)
	source.EXPECT().GetLatestBlock(gomock.Any()).
		Return(uint64(0), errors.New("rpc unreachable"))

	source.EXPECT().Close()

	ctx := context.Background()
	runner := ingestion.NewRunner(
		ctx, source, store, jq, blacklist,
		ingestion.Config{
			ChainID:       domain.ChainEthereumMainnet,
			TokenQueue:    "token_index",
			QueueCapacity: 4,
		},
		clock,
	)
	defer func() { _ = runner.Close() }()

	err := runner.Run(ctx)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to get latest block number")
}

// TestRunner_PropagatesNonCancelSubscribeError verifies that a transport-level
// failure from the source surfaces unchanged from Run so the supervisor can
// restart, rather than being swallowed as a clean shutdown.
func TestRunner_PropagatesNonCancelSubscribeError(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	source := mocks.NewMockBlockchainEventSource(ctrl)
	store := mocks.NewMockStore(ctrl)
	jq := mocks.NewMockJobQueue(ctrl)
	blacklist := mocks.NewMockBlacklistRegistry(ctrl)
	clock := adapter.NewClock()

	wsErr := errors.New("websocket disconnected")
	source.EXPECT().
		SubscribeEvents(gomock.Any(), uint64(1000), gomock.Any()).
		Return(wsErr)
	source.EXPECT().Close()

	ctx := context.Background()
	runner := ingestion.NewRunner(
		ctx, source, store, jq, blacklist,
		ingestion.Config{
			ChainID:       domain.ChainEthereumMainnet,
			StartBlock:    1000,
			TokenQueue:    "token_index",
			QueueCapacity: 4,
		},
		clock,
	)
	defer func() { _ = runner.Close() }()

	err := runner.Run(ctx)
	require.Error(t, err)
	assert.ErrorIs(t, err, wsErr)
}

// TestRunner_FailsWhenSetBlockCursorFails exercises the flushFailed signal:
// a cursor write failure must abort ingestion (Run returns the wrapped error)
// instead of leaving the subscribe goroutine hung.
func TestRunner_FailsWhenSetBlockCursorFails(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	source := mocks.NewMockBlockchainEventSource(ctrl)
	store := mocks.NewMockStore(ctrl)
	jq := mocks.NewMockJobQueue(ctrl)
	blacklist := mocks.NewMockBlacklistRegistry(ctrl)
	clock := adapter.NewClock()

	e1 := transferEvent()
	e1.BlockNumber = 100
	e2 := transferEvent()
	e2.TxHash = "0xtx2"
	e2.BlockNumber = 101 // arrival of next block triggers flush of block 100

	blacklist.EXPECT().IsTokenCIDBlacklisted(e1.TokenCID()).Return(false)
	store.EXPECT().GetTokenByTokenCID(gomock.Any(), e1.TokenCID().String()).Return(nil, nil)
	store.EXPECT().IsAnyAddressWatched(gomock.Any(), e1.Chain, []string{"0xfrom", "0xto"}).Return(true, nil)
	jq.EXPECT().Enqueue(gomock.Any(), gomock.Any()).Return(&schema.Job{ID: 1}, true, nil)

	store.EXPECT().
		SetBlockCursor(gomock.Any(), string(e1.Chain), uint64(100)).
		Return(errors.New("cursor persistence failed"))

	source.EXPECT().
		SubscribeEvents(gomock.Any(), uint64(1000), gomock.Any()).
		DoAndReturn(func(ctx context.Context, _ uint64, handler blockchain.EventHandler) error {
			require.NoError(t, handler(e1))
			require.NoError(t, handler(e2))
			// Block on ctx so we observe the runner canceling subscribe in response
			// to the flush failure rather than us returning Canceled spontaneously.
			<-ctx.Done()
			return ctx.Err()
		})
	source.EXPECT().Close()

	ctx := context.Background()
	runner := ingestion.NewRunner(
		ctx, source, store, jq, blacklist,
		ingestion.Config{
			ChainID:       domain.ChainEthereumMainnet,
			StartBlock:    1000,
			TokenQueue:    "token_index",
			QueueCapacity: 4,
		},
		clock,
	)
	defer func() { _ = runner.Close() }()

	err := runner.Run(ctx)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to persist block cursor")
}

// TestRunner_FailsWhenEnqueueFails exercises the flushFailed signal for the
// other write path: a job enqueue failure must abort ingestion before the
// cursor advances, so the same block replays cleanly on restart.
func TestRunner_FailsWhenEnqueueFails(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	source := mocks.NewMockBlockchainEventSource(ctrl)
	store := mocks.NewMockStore(ctrl)
	jq := mocks.NewMockJobQueue(ctrl)
	blacklist := mocks.NewMockBlacklistRegistry(ctrl)
	clock := adapter.NewClock()

	e1 := transferEvent()
	e1.BlockNumber = 100
	e2 := transferEvent()
	e2.TxHash = "0xtx2"
	e2.BlockNumber = 101

	blacklist.EXPECT().IsTokenCIDBlacklisted(e1.TokenCID()).Return(false)
	store.EXPECT().GetTokenByTokenCID(gomock.Any(), e1.TokenCID().String()).Return(nil, nil)
	store.EXPECT().IsAnyAddressWatched(gomock.Any(), e1.Chain, []string{"0xfrom", "0xto"}).Return(true, nil)
	jq.EXPECT().Enqueue(gomock.Any(), gomock.Any()).
		Return(nil, false, errors.New("queue full"))

	// Cursor must NOT advance when enqueue fails.
	// (no SetBlockCursor expectation)

	source.EXPECT().
		SubscribeEvents(gomock.Any(), uint64(1000), gomock.Any()).
		DoAndReturn(func(ctx context.Context, _ uint64, handler blockchain.EventHandler) error {
			require.NoError(t, handler(e1))
			require.NoError(t, handler(e2))
			<-ctx.Done()
			return ctx.Err()
		})
	source.EXPECT().Close()

	ctx := context.Background()
	runner := ingestion.NewRunner(
		ctx, source, store, jq, blacklist,
		ingestion.Config{
			ChainID:       domain.ChainEthereumMainnet,
			StartBlock:    1000,
			TokenQueue:    "token_index",
			QueueCapacity: 4,
		},
		clock,
	)
	defer func() { _ = runner.Close() }()

	err := runner.Run(ctx)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to enqueue chain job")
}

// TestRunner_FailsWhenTokenLookupFails verifies that filter-read errors from
// GetTokenByTokenCID are fatal and halt ingestion immediately.
func TestRunner_FailsWhenTokenLookupFails(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	source := mocks.NewMockBlockchainEventSource(ctrl)
	store := mocks.NewMockStore(ctrl)
	jq := mocks.NewMockJobQueue(ctrl)
	blacklist := mocks.NewMockBlacklistRegistry(ctrl)
	clock := adapter.NewClock()

	e1 := transferEvent()
	e1.BlockNumber = 100
	e2 := transferEvent()
	e2.TxHash = "0xtx2"
	e2.BlockNumber = 101

	blacklist.EXPECT().IsTokenCIDBlacklisted(e1.TokenCID()).Return(false)
	store.EXPECT().GetTokenByTokenCID(gomock.Any(), e1.TokenCID().String()).
		Return(nil, errors.New("db query failed"))

	// Watch-list check and enqueue must NOT be reached.
	// Cursor must NOT advance.

	source.EXPECT().
		SubscribeEvents(gomock.Any(), uint64(1000), gomock.Any()).
		DoAndReturn(func(ctx context.Context, _ uint64, handler blockchain.EventHandler) error {
			require.NoError(t, handler(e1))
			require.NoError(t, handler(e2))
			<-ctx.Done()
			return ctx.Err()
		})
	source.EXPECT().Close()

	ctx := context.Background()
	runner := ingestion.NewRunner(
		ctx, source, store, jq, blacklist,
		ingestion.Config{
			ChainID:       domain.ChainEthereumMainnet,
			StartBlock:    1000,
			TokenQueue:    "token_index",
			QueueCapacity: 4,
		},
		clock,
	)
	defer func() { _ = runner.Close() }()

	err := runner.Run(ctx)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "token lookup failed")
}

// TestRunner_FailsWhenWatchListCheckFails verifies that filter-read errors from
// IsAnyAddressWatched are fatal and halt ingestion immediately.
func TestRunner_FailsWhenWatchListCheckFails(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	source := mocks.NewMockBlockchainEventSource(ctrl)
	store := mocks.NewMockStore(ctrl)
	jq := mocks.NewMockJobQueue(ctrl)
	blacklist := mocks.NewMockBlacklistRegistry(ctrl)
	clock := adapter.NewClock()

	e1 := transferEvent()
	e1.BlockNumber = 100
	e2 := transferEvent()
	e2.TxHash = "0xtx2"
	e2.BlockNumber = 101

	blacklist.EXPECT().IsTokenCIDBlacklisted(e1.TokenCID()).Return(false)
	store.EXPECT().GetTokenByTokenCID(gomock.Any(), e1.TokenCID().String()).Return(nil, nil)
	store.EXPECT().IsAnyAddressWatched(gomock.Any(), e1.Chain, []string{"0xfrom", "0xto"}).
		Return(false, errors.New("db query failed"))

	// Enqueue must NOT be reached.
	// Cursor must NOT advance.

	source.EXPECT().
		SubscribeEvents(gomock.Any(), uint64(1000), gomock.Any()).
		DoAndReturn(func(ctx context.Context, _ uint64, handler blockchain.EventHandler) error {
			require.NoError(t, handler(e1))
			require.NoError(t, handler(e2))
			<-ctx.Done()
			return ctx.Err()
		})
	source.EXPECT().Close()

	ctx := context.Background()
	runner := ingestion.NewRunner(
		ctx, source, store, jq, blacklist,
		ingestion.Config{
			ChainID:       domain.ChainEthereumMainnet,
			StartBlock:    1000,
			TokenQueue:    "token_index",
			QueueCapacity: 4,
		},
		clock,
	)
	defer func() { _ = runner.Close() }()

	err := runner.Run(ctx)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "watch-list check failed")
}

// TestRunner_DropsBlacklistedTokenEvents covers the blacklist short-circuit:
// matched events bypass token/address checks and the enqueue step entirely.
func TestRunner_DropsBlacklistedTokenEvents(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	source := mocks.NewMockBlockchainEventSource(ctrl)
	store := mocks.NewMockStore(ctrl)
	jq := mocks.NewMockJobQueue(ctrl)
	blacklist := mocks.NewMockBlacklistRegistry(ctrl)
	clock := adapter.NewClock()

	e1 := transferEvent()
	e1.BlockNumber = 100
	e2 := transferEvent()
	e2.TxHash = "0xtx2"
	e2.BlockNumber = 101

	blacklist.EXPECT().IsTokenCIDBlacklisted(e1.TokenCID()).Return(true)
	// No GetTokenByTokenCID, IsAnyAddressWatched, or Enqueue calls expected.

	store.EXPECT().
		SetBlockCursor(gomock.Any(), string(e1.Chain), uint64(100)).
		Return(nil)

	source.EXPECT().
		SubscribeEvents(gomock.Any(), uint64(1000), gomock.Any()).
		DoAndReturn(func(_ context.Context, _ uint64, handler blockchain.EventHandler) error {
			require.NoError(t, handler(e1))
			require.NoError(t, handler(e2))
			time.Sleep(50 * time.Millisecond)
			return context.Canceled
		})
	source.EXPECT().Close()

	ctx := context.Background()
	runner := ingestion.NewRunner(
		ctx, source, store, jq, blacklist,
		ingestion.Config{
			ChainID:       domain.ChainEthereumMainnet,
			StartBlock:    1000,
			TokenQueue:    "token_index",
			QueueCapacity: 4,
		},
		clock,
	)
	defer func() { _ = runner.Close() }()

	err := runner.Run(ctx)
	require.Error(t, err)
	assert.ErrorIs(t, err, context.Canceled)
}

// TestRunner_ProcessesKnownTokenWithoutAddressCheck covers the optimisation in
// shouldProcessEvent: when the token already exists locally we skip the
// watch-list call and enqueue directly.
func TestRunner_ProcessesKnownTokenWithoutAddressCheck(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	source := mocks.NewMockBlockchainEventSource(ctrl)
	store := mocks.NewMockStore(ctrl)
	jq := mocks.NewMockJobQueue(ctrl)
	blacklist := mocks.NewMockBlacklistRegistry(ctrl)
	clock := adapter.NewClock()

	e1 := transferEvent()
	e1.BlockNumber = 100
	e2 := transferEvent()
	e2.TxHash = "0xtx2"
	e2.BlockNumber = 101

	blacklist.EXPECT().IsTokenCIDBlacklisted(e1.TokenCID()).Return(false)
	store.EXPECT().GetTokenByTokenCID(gomock.Any(), e1.TokenCID().String()).
		Return(&schema.Token{ID: 123}, nil)
	// IsAnyAddressWatched MUST NOT be called when the token is already known.
	jq.EXPECT().Enqueue(gomock.Any(), gomock.Any()).Return(&schema.Job{ID: 1}, true, nil)

	store.EXPECT().
		SetBlockCursor(gomock.Any(), string(e1.Chain), uint64(100)).
		Return(nil)

	source.EXPECT().
		SubscribeEvents(gomock.Any(), uint64(1000), gomock.Any()).
		DoAndReturn(func(_ context.Context, _ uint64, handler blockchain.EventHandler) error {
			require.NoError(t, handler(e1))
			require.NoError(t, handler(e2))
			time.Sleep(50 * time.Millisecond)
			return context.Canceled
		})
	source.EXPECT().Close()

	ctx := context.Background()
	runner := ingestion.NewRunner(
		ctx, source, store, jq, blacklist,
		ingestion.Config{
			ChainID:       domain.ChainEthereumMainnet,
			StartBlock:    1000,
			TokenQueue:    "token_index",
			QueueCapacity: 4,
		},
		clock,
	)
	defer func() { _ = runner.Close() }()

	err := runner.Run(ctx)
	require.Error(t, err)
	assert.ErrorIs(t, err, context.Canceled)
}

// TestRunner_DropsEventsForUnwatchedAddresses covers the negative result from
// IsAnyAddressWatched: the event is dropped (no enqueue) but the block still
// flushes because the drop is intentional, not an error.
func TestRunner_DropsEventsForUnwatchedAddresses(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	source := mocks.NewMockBlockchainEventSource(ctrl)
	store := mocks.NewMockStore(ctrl)
	jq := mocks.NewMockJobQueue(ctrl)
	blacklist := mocks.NewMockBlacklistRegistry(ctrl)
	clock := adapter.NewClock()

	e1 := transferEvent()
	e1.BlockNumber = 100
	e2 := transferEvent()
	e2.TxHash = "0xtx2"
	e2.BlockNumber = 101

	blacklist.EXPECT().IsTokenCIDBlacklisted(e1.TokenCID()).Return(false)
	store.EXPECT().GetTokenByTokenCID(gomock.Any(), e1.TokenCID().String()).Return(nil, nil)
	store.EXPECT().IsAnyAddressWatched(gomock.Any(), e1.Chain, []string{"0xfrom", "0xto"}).
		Return(false, nil)
	// Enqueue MUST NOT be called.

	store.EXPECT().
		SetBlockCursor(gomock.Any(), string(e1.Chain), uint64(100)).
		Return(nil)

	source.EXPECT().
		SubscribeEvents(gomock.Any(), uint64(1000), gomock.Any()).
		DoAndReturn(func(_ context.Context, _ uint64, handler blockchain.EventHandler) error {
			require.NoError(t, handler(e1))
			require.NoError(t, handler(e2))
			time.Sleep(50 * time.Millisecond)
			return context.Canceled
		})
	source.EXPECT().Close()

	ctx := context.Background()
	runner := ingestion.NewRunner(
		ctx, source, store, jq, blacklist,
		ingestion.Config{
			ChainID:       domain.ChainEthereumMainnet,
			StartBlock:    1000,
			TokenQueue:    "token_index",
			QueueCapacity: 4,
		},
		clock,
	)
	defer func() { _ = runner.Close() }()

	err := runner.Run(ctx)
	require.Error(t, err)
	assert.ErrorIs(t, err, context.Canceled)
}

// TestRunner_DropsEventsWithNoAddresses covers the early-return when both
// from and to addresses are empty: we cannot ask the watch-list anything
// useful, so we drop the event without an extra store call.
func TestRunner_DropsEventsWithNoAddresses(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	source := mocks.NewMockBlockchainEventSource(ctrl)
	store := mocks.NewMockStore(ctrl)
	jq := mocks.NewMockJobQueue(ctrl)
	blacklist := mocks.NewMockBlacklistRegistry(ctrl)
	clock := adapter.NewClock()

	e1 := transferEvent()
	e1.BlockNumber = 100
	e1.FromAddress = nil
	e1.ToAddress = nil
	e2 := transferEvent()
	e2.TxHash = "0xtx2"
	e2.BlockNumber = 101

	blacklist.EXPECT().IsTokenCIDBlacklisted(e1.TokenCID()).Return(false)
	store.EXPECT().GetTokenByTokenCID(gomock.Any(), e1.TokenCID().String()).Return(nil, nil)
	// IsAnyAddressWatched MUST NOT be called when there is nothing to ask about.

	store.EXPECT().
		SetBlockCursor(gomock.Any(), string(e1.Chain), uint64(100)).
		Return(nil)

	source.EXPECT().
		SubscribeEvents(gomock.Any(), uint64(1000), gomock.Any()).
		DoAndReturn(func(_ context.Context, _ uint64, handler blockchain.EventHandler) error {
			require.NoError(t, handler(e1))
			require.NoError(t, handler(e2))
			time.Sleep(50 * time.Millisecond)
			return context.Canceled
		})
	source.EXPECT().Close()

	ctx := context.Background()
	runner := ingestion.NewRunner(
		ctx, source, store, jq, blacklist,
		ingestion.Config{
			ChainID:       domain.ChainEthereumMainnet,
			StartBlock:    1000,
			TokenQueue:    "token_index",
			QueueCapacity: 4,
		},
		clock,
	)
	defer func() { _ = runner.Close() }()

	err := runner.Run(ctx)
	require.Error(t, err)
	assert.ErrorIs(t, err, context.Canceled)
}

// TestRunner_IgnoresMetadataUpdateRangeEvents documents the deliberate
// no-op for range events: filters still run, but we do not start a workflow
// and the block still flushes so the cursor advances past them.
func TestRunner_IgnoresMetadataUpdateRangeEvents(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	source := mocks.NewMockBlockchainEventSource(ctrl)
	store := mocks.NewMockStore(ctrl)
	jq := mocks.NewMockJobQueue(ctrl)
	blacklist := mocks.NewMockBlacklistRegistry(ctrl)
	clock := adapter.NewClock()

	e1 := transferEvent()
	e1.EventType = domain.EventTypeMetadataUpdateRange
	e1.BlockNumber = 100
	e2 := transferEvent()
	e2.TxHash = "0xtx2"
	e2.BlockNumber = 101

	blacklist.EXPECT().IsTokenCIDBlacklisted(e1.TokenCID()).Return(false)
	store.EXPECT().GetTokenByTokenCID(gomock.Any(), e1.TokenCID().String()).Return(nil, nil)
	store.EXPECT().IsAnyAddressWatched(gomock.Any(), e1.Chain, []string{"0xfrom", "0xto"}).Return(true, nil)
	// Enqueue MUST NOT be called for MetadataUpdateRange.

	store.EXPECT().
		SetBlockCursor(gomock.Any(), string(e1.Chain), uint64(100)).
		Return(nil)

	source.EXPECT().
		SubscribeEvents(gomock.Any(), uint64(1000), gomock.Any()).
		DoAndReturn(func(_ context.Context, _ uint64, handler blockchain.EventHandler) error {
			require.NoError(t, handler(e1))
			require.NoError(t, handler(e2))
			time.Sleep(50 * time.Millisecond)
			return context.Canceled
		})
	source.EXPECT().Close()

	ctx := context.Background()
	runner := ingestion.NewRunner(
		ctx, source, store, jq, blacklist,
		ingestion.Config{
			ChainID:       domain.ChainEthereumMainnet,
			StartBlock:    1000,
			TokenQueue:    "token_index",
			QueueCapacity: 4,
		},
		clock,
	)
	defer func() { _ = runner.Close() }()

	err := runner.Run(ctx)
	require.Error(t, err)
	assert.ErrorIs(t, err, context.Canceled)
}

// TestRunner_EnqueuesJobsWithCorrectKind table-checks the EventType -> Job kind
// mapping by inspecting the EnqueueOptions handed to the job queue. This is
// the contract the workflow side reads against, so it deserves explicit cover.
func TestRunner_EnqueuesJobsWithCorrectKind(t *testing.T) {
	cases := []struct {
		name      string
		eventType domain.EventType
		wantKind  string
	}{
		{"mint", domain.EventTypeMint, "IndexTokenMint"},
		{"transfer", domain.EventTypeTransfer, "IndexTokenTransfer"},
		{"burn", domain.EventTypeBurn, "IndexTokenBurn"},
		{"metadata_update", domain.EventTypeMetadataUpdate, "IndexMetadataUpdate"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			source := mocks.NewMockBlockchainEventSource(ctrl)
			store := mocks.NewMockStore(ctrl)
			jq := mocks.NewMockJobQueue(ctrl)
			blacklist := mocks.NewMockBlacklistRegistry(ctrl)
			clock := adapter.NewClock()

			e1 := transferEvent()
			e1.EventType = tc.eventType
			e1.BlockNumber = 100
			e2 := transferEvent()
			e2.TxHash = "0xtx2"
			e2.BlockNumber = 101

			blacklist.EXPECT().IsTokenCIDBlacklisted(e1.TokenCID()).Return(false)
			store.EXPECT().GetTokenByTokenCID(gomock.Any(), e1.TokenCID().String()).Return(nil, nil)
			store.EXPECT().IsAnyAddressWatched(gomock.Any(), e1.Chain, []string{"0xfrom", "0xto"}).Return(true, nil)

			jq.EXPECT().
				Enqueue(gomock.Any(), gomock.Any()).
				DoAndReturn(func(_ context.Context, opts jobs.EnqueueOptions) (*schema.Job, bool, error) {
					assert.Equal(t, tc.wantKind, opts.Kind, "kind for %s", tc.name)
					assert.Equal(t, "token_index", opts.Queue)
					require.NotNil(t, opts.UniqueKey)
					assert.NotEmpty(t, *opts.UniqueKey)
					return &schema.Job{ID: 1}, true, nil
				})

			store.EXPECT().
				SetBlockCursor(gomock.Any(), string(e1.Chain), uint64(100)).
				Return(nil)

			source.EXPECT().
				SubscribeEvents(gomock.Any(), uint64(1000), gomock.Any()).
				DoAndReturn(func(_ context.Context, _ uint64, handler blockchain.EventHandler) error {
					require.NoError(t, handler(e1))
					require.NoError(t, handler(e2))
					time.Sleep(50 * time.Millisecond)
					return context.Canceled
				})
			source.EXPECT().Close()

			ctx := context.Background()
			runner := ingestion.NewRunner(
				ctx, source, store, jq, blacklist,
				ingestion.Config{
					ChainID:       domain.ChainEthereumMainnet,
					StartBlock:    1000,
					TokenQueue:    "token_index",
					QueueCapacity: 4,
				},
				clock,
			)
			defer func() { _ = runner.Close() }()

			err := runner.Run(ctx)
			require.Error(t, err)
			assert.ErrorIs(t, err, context.Canceled)
		})
	}
}

// TestRunner_ProcessesMultipleSequentialBlocks demonstrates the steady-state
// property: each block boundary flushes exactly once and the cursor advances
// in order. The final buffered block does not flush (subscribe ends before
// its successor arrives), which preserves the cursor-loss fix.
func TestRunner_ProcessesMultipleSequentialBlocks(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	source := mocks.NewMockBlockchainEventSource(ctrl)
	store := mocks.NewMockStore(ctrl)
	jq := mocks.NewMockJobQueue(ctrl)
	blacklist := mocks.NewMockBlacklistRegistry(ctrl)
	clock := adapter.NewClock()

	events := make([]*domain.BlockchainEvent, 0, 4)
	for i, blockNum := range []uint64{100, 101, 102, 103} {
		e := transferEvent()
		e.TxHash = "0xtx" + string(rune('1'+i))
		e.BlockNumber = blockNum
		events = append(events, e)
	}

	// Only the first three blocks complete (block 103 stays buffered when
	// subscribe returns), so we expect mocks for events[0..2] only.
	for _, e := range events[:3] {
		blacklist.EXPECT().IsTokenCIDBlacklisted(e.TokenCID()).Return(false)
		store.EXPECT().GetTokenByTokenCID(gomock.Any(), e.TokenCID().String()).Return(nil, nil)
		store.EXPECT().IsAnyAddressWatched(gomock.Any(), e.Chain, []string{"0xfrom", "0xto"}).Return(true, nil)
		jq.EXPECT().Enqueue(gomock.Any(), gomock.Any()).Return(&schema.Job{ID: 1}, true, nil)
	}

	store.EXPECT().SetBlockCursor(gomock.Any(), string(domain.ChainEthereumMainnet), uint64(100)).Return(nil)
	store.EXPECT().SetBlockCursor(gomock.Any(), string(domain.ChainEthereumMainnet), uint64(101)).Return(nil)
	store.EXPECT().SetBlockCursor(gomock.Any(), string(domain.ChainEthereumMainnet), uint64(102)).Return(nil)

	source.EXPECT().
		SubscribeEvents(gomock.Any(), uint64(1000), gomock.Any()).
		DoAndReturn(func(_ context.Context, _ uint64, handler blockchain.EventHandler) error {
			for _, e := range events {
				require.NoError(t, handler(e))
			}
			time.Sleep(50 * time.Millisecond)
			return context.Canceled
		})
	source.EXPECT().Close()

	ctx := context.Background()
	runner := ingestion.NewRunner(
		ctx, source, store, jq, blacklist,
		ingestion.Config{
			ChainID:       domain.ChainEthereumMainnet,
			StartBlock:    1000,
			TokenQueue:    "token_index",
			QueueCapacity: 8,
		},
		clock,
	)
	defer func() { _ = runner.Close() }()

	err := runner.Run(ctx)
	require.Error(t, err)
	assert.ErrorIs(t, err, context.Canceled)
}

// TestRunner_CloseIsIdempotent guards the closeOnce contract: callers may
// invoke Close from multiple paths (defer in test, supervisor shutdown) and
// each call must be safe and return nil without re-closing the source.
func TestRunner_CloseIsIdempotent(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	source := mocks.NewMockBlockchainEventSource(ctrl)
	store := mocks.NewMockStore(ctrl)
	jq := mocks.NewMockJobQueue(ctrl)
	blacklist := mocks.NewMockBlacklistRegistry(ctrl)
	clock := adapter.NewClock()

	// Source.Close must be invoked exactly once even if the runner is closed
	// repeatedly.
	source.EXPECT().Close().Times(1)

	ctx := context.Background()
	runner := ingestion.NewRunner(
		ctx, source, store, jq, blacklist,
		ingestion.Config{
			ChainID:       domain.ChainEthereumMainnet,
			StartBlock:    1000,
			TokenQueue:    "token_index",
			QueueCapacity: 4,
		},
		clock,
	)

	require.NoError(t, runner.Close())
	require.NoError(t, runner.Close())
	require.NoError(t, runner.Close())
}

// TestRunner_FlushesBlockAfterTimeout verifies that blocks are flushed after
// BlockFlushTimeout even when no next-block event arrives, preventing blocks
// from remaining buffered indefinitely when event streams are sparse.
func TestRunner_FlushesBlockAfterTimeout(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	source := mocks.NewMockBlockchainEventSource(ctrl)
	store := mocks.NewMockStore(ctrl)
	jq := mocks.NewMockJobQueue(ctrl)
	blacklist := mocks.NewMockBlacklistRegistry(ctrl)
	clock := adapter.NewClock()

	e1 := transferEvent()
	e1.BlockNumber = 100

	blacklist.EXPECT().IsTokenCIDBlacklisted(e1.TokenCID()).Return(false)
	store.EXPECT().GetTokenByTokenCID(gomock.Any(), e1.TokenCID().String()).Return(nil, nil)
	store.EXPECT().IsAnyAddressWatched(gomock.Any(), e1.Chain, []string{"0xfrom", "0xto"}).Return(true, nil)
	jq.EXPECT().Enqueue(gomock.Any(), gomock.Any()).Return(&schema.Job{ID: 1}, true, nil)

	// Block 100 should flush after timeout, even though no block 101 event arrives
	store.EXPECT().
		SetBlockCursor(gomock.Any(), string(e1.Chain), uint64(100)).
		Return(nil)

	source.EXPECT().
		SubscribeEvents(gomock.Any(), uint64(1000), gomock.Any()).
		DoAndReturn(func(ctx context.Context, _ uint64, handler blockchain.EventHandler) error {
			require.NoError(t, handler(e1))
			// Wait for timeout to trigger flush (100ms timeout + margin)
			time.Sleep(150 * time.Millisecond)
			return context.Canceled
		})
	source.EXPECT().Close()

	ctx := context.Background()
	runner := ingestion.NewRunner(
		ctx, source, store, jq, blacklist,
		ingestion.Config{
			ChainID:           domain.ChainEthereumMainnet,
			StartBlock:        1000,
			TokenQueue:        "token_index",
			QueueCapacity:     4,
			BlockFlushTimeout: 100 * time.Millisecond,
		},
		clock,
	)
	defer func() { _ = runner.Close() }()

	err := runner.Run(ctx)
	require.Error(t, err)
	assert.ErrorIs(t, err, context.Canceled)
}

// TestRunner_ResetsTimeoutOnNewEventInSameBlock verifies that the timeout is
// reset when new events arrive for the same block, preventing premature flush.
func TestRunner_ResetsTimeoutOnNewEventInSameBlock(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	source := mocks.NewMockBlockchainEventSource(ctrl)
	store := mocks.NewMockStore(ctrl)
	jq := mocks.NewMockJobQueue(ctrl)
	blacklist := mocks.NewMockBlacklistRegistry(ctrl)
	clock := adapter.NewClock()

	e1 := transferEvent()
	e1.TxHash = "0xtx1"
	e1.BlockNumber = 100
	e2 := transferEvent()
	e2.TxHash = "0xtx2"
	e2.BlockNumber = 100
	e3 := transferEvent()
	e3.TxHash = "0xtx3"
	e3.BlockNumber = 101

	// Both e1 and e2 should be processed before flush
	for _, e := range []*domain.BlockchainEvent{e1, e2} {
		blacklist.EXPECT().IsTokenCIDBlacklisted(e.TokenCID()).Return(false)
		store.EXPECT().GetTokenByTokenCID(gomock.Any(), e.TokenCID().String()).Return(nil, nil)
		store.EXPECT().IsAnyAddressWatched(gomock.Any(), e.Chain, []string{"0xfrom", "0xto"}).Return(true, nil)
		jq.EXPECT().Enqueue(gomock.Any(), gomock.Any()).Return(&schema.Job{ID: 1}, true, nil)
	}

	// Block 100 should flush when block 101 arrives (not due to timeout)
	store.EXPECT().
		SetBlockCursor(gomock.Any(), string(e1.Chain), uint64(100)).
		Return(nil)

	source.EXPECT().
		SubscribeEvents(gomock.Any(), uint64(1000), gomock.Any()).
		DoAndReturn(func(ctx context.Context, _ uint64, handler blockchain.EventHandler) error {
			require.NoError(t, handler(e1))
			// Wait less than timeout
			time.Sleep(50 * time.Millisecond)
			// Second event resets timer
			require.NoError(t, handler(e2))
			// Wait less than timeout again
			time.Sleep(50 * time.Millisecond)
			// Next block arrives before timeout (100ms total < 100ms timeout)
			require.NoError(t, handler(e3))
			time.Sleep(50 * time.Millisecond)
			return context.Canceled
		})
	source.EXPECT().Close()

	ctx := context.Background()
	runner := ingestion.NewRunner(
		ctx, source, store, jq, blacklist,
		ingestion.Config{
			ChainID:           domain.ChainEthereumMainnet,
			StartBlock:        1000,
			TokenQueue:        "token_index",
			QueueCapacity:     4,
			BlockFlushTimeout: 100 * time.Millisecond,
		},
		clock,
	)
	defer func() { _ = runner.Close() }()

	err := runner.Run(ctx)
	require.Error(t, err)
	assert.ErrorIs(t, err, context.Canceled)
}

func transferEvent() *domain.BlockchainEvent {
	return &domain.BlockchainEvent{
		Chain:           domain.ChainEthereumMainnet,
		Standard:        domain.StandardERC721,
		ContractAddress: "0x123",
		TokenNumber:     "1",
		EventType:       domain.EventTypeTransfer,
		FromAddress:     types.StringPtr("0xfrom"),
		ToAddress:       types.StringPtr("0xto"),
		Quantity:        "1",
		TxHash:          "0xtx",
		BlockNumber:     100,
	}
}
