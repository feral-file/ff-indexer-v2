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
	"github.com/feral-file/ff-indexer-v2/internal/store/schema"
)

type stubSource struct {
	subscribe func(ctx context.Context, fromBlock uint64, handler blockchain.EventHandler) error
	latest    uint64
}

func (s *stubSource) SubscribeEvents(ctx context.Context, fromBlock uint64, handler blockchain.EventHandler) error {
	return s.subscribe(ctx, fromBlock, handler)
}

func (s *stubSource) GetLatestBlock(context.Context) (uint64, error) {
	return s.latest, nil
}

func (s *stubSource) Close() {}

func TestMain(m *testing.M) {
	if err := logger.Initialize(logger.Config{Debug: false}); err != nil {
		panic(err)
	}
	os.Exit(m.Run())
}

func TestRunner_UsesConfiguredStartBlockAndFlushesEvent(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	store := mocks.NewMockStore(ctrl)
	jq := mocks.NewMockJobQueue(ctrl)
	blacklist := mocks.NewMockBlacklistRegistry(ctrl)
	clock := adapter.NewClock()
	flushed := make(chan struct{}, 1)

	event := transferEvent()
	blacklist.EXPECT().IsTokenCIDBlacklisted(event.TokenCID()).Return(false)
	store.EXPECT().GetTokenByTokenCID(gomock.Any(), event.TokenCID().String()).Return(nil, nil)
	store.EXPECT().IsAnyAddressWatched(gomock.Any(), event.Chain, []string{"0xfrom", "0xto"}).Return(true, nil)
	jq.EXPECT().Enqueue(gomock.Any(), gomock.Any()).Return(&schema.Job{ID: 1}, true, nil)
	store.EXPECT().
		SetBlockCursor(gomock.Any(), string(event.Chain), event.BlockNumber).
		DoAndReturn(func(context.Context, string, uint64) error {
			flushed <- struct{}{}
			return nil
		})

	ctx := context.Background()

	runner := ingestion.NewRunner(
		ctx,
		&stubSource{
			subscribe: func(ctx context.Context, fromBlock uint64, handler blockchain.EventHandler) error {
				require.Equal(t, uint64(1000), fromBlock)
				require.NoError(t, handler(event))
				return context.Canceled
			},
		},
		store,
		jq,
		blacklist,
		ingestion.Config{
			ChainID:       domain.ChainEthereumMainnet,
			StartBlock:    1000,
			TokenQueue:    "token_index",
			QueueCapacity: 1,
		},
		clock,
	)
	defer func() { _ = runner.Close() }()

	err := runner.Run(ctx)
	require.Error(t, err)
	assert.ErrorIs(t, err, context.Canceled)
	waitForSignal(t, flushed)
}

func TestRunner_UsesLatestBlockWhenNoCursorExists(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	store := mocks.NewMockStore(ctrl)
	jq := mocks.NewMockJobQueue(ctrl)
	blacklist := mocks.NewMockBlacklistRegistry(ctrl)
	clock := adapter.NewClock()

	store.EXPECT().GetBlockCursor(gomock.Any(), string(domain.ChainTezosMainnet)).Return(uint64(0), nil)

	ctx := context.Background()

	runner := ingestion.NewRunner(
		ctx,
		&stubSource{
			latest: 42,
			subscribe: func(ctx context.Context, fromBlock uint64, handler blockchain.EventHandler) error {
				assert.Equal(t, uint64(42), fromBlock)
				return context.Canceled
			},
		},
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

func TestRunner_RetriesCursorPersistenceWithoutRestartingWorkflow(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	store := mocks.NewMockStore(ctrl)
	jq := mocks.NewMockJobQueue(ctrl)
	blacklist := mocks.NewMockBlacklistRegistry(ctrl)
	clock := mocks.NewMockClock(ctrl)
	flushed := make(chan struct{}, 1)

	event := mintEvent()
	blacklist.EXPECT().IsTokenCIDBlacklisted(event.TokenCID()).Return(false)
	store.EXPECT().GetTokenByTokenCID(gomock.Any(), event.TokenCID().String()).Return(&schema.Token{}, nil)
	jq.EXPECT().Enqueue(gomock.Any(), gomock.Any()).Return(&schema.Job{ID: 1}, true, nil).Times(1)
	store.EXPECT().SetBlockCursor(gomock.Any(), string(event.Chain), event.BlockNumber).
		Return(errors.New("db unavailable"))
	retryCh := make(chan time.Time, 1)
	clock.EXPECT().After(time.Minute).Return(retryCh)
	store.EXPECT().
		SetBlockCursor(gomock.Any(), string(event.Chain), event.BlockNumber).
		DoAndReturn(func(context.Context, string, uint64) error {
			flushed <- struct{}{}
			return nil
		})

	ctx := context.Background()

	runner := ingestion.NewRunner(
		ctx,
		&stubSource{
			subscribe: func(ctx context.Context, fromBlock uint64, handler blockchain.EventHandler) error {
				require.NoError(t, handler(event))
				return context.Canceled
			},
		},
		store,
		jq,
		blacklist,
		ingestion.Config{
			ChainID:       domain.ChainEthereumMainnet,
			StartBlock:    100,
			TokenQueue:    "token_index",
			QueueCapacity: 1,
			RetryDelay:    time.Minute,
		},
		clock,
	)
	defer func() { _ = runner.Close() }()

	err := runner.Run(ctx)
	require.Error(t, err)
	assert.ErrorIs(t, err, context.Canceled)
	retryCh <- time.Now()
	waitForSignal(t, flushed)
}

func TestRunner_RetriesWorkflowStartUntilItSucceeds(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	store := mocks.NewMockStore(ctrl)
	jq := mocks.NewMockJobQueue(ctrl)
	blacklist := mocks.NewMockBlacklistRegistry(ctrl)
	clock := mocks.NewMockClock(ctrl)
	flushed := make(chan struct{}, 1)

	event := mintEvent()
	blacklist.EXPECT().IsTokenCIDBlacklisted(event.TokenCID()).Return(false).Times(2)
	store.EXPECT().GetTokenByTokenCID(gomock.Any(), event.TokenCID().String()).Return(&schema.Token{}, nil).Times(2)
	jq.EXPECT().Enqueue(gomock.Any(), gomock.Any()).Return(nil, false, errors.New("queue unavailable"))
	retryCh := make(chan time.Time, 1)
	clock.EXPECT().After(time.Minute).Return(retryCh)
	jq.EXPECT().Enqueue(gomock.Any(), gomock.Any()).Return(&schema.Job{ID: 1}, true, nil)
	store.EXPECT().
		SetBlockCursor(gomock.Any(), string(event.Chain), event.BlockNumber).
		DoAndReturn(func(context.Context, string, uint64) error {
			flushed <- struct{}{}
			return nil
		})

	ctx := context.Background()

	runner := ingestion.NewRunner(
		ctx,
		&stubSource{
			subscribe: func(ctx context.Context, fromBlock uint64, handler blockchain.EventHandler) error {
				require.NoError(t, handler(event))
				return context.Canceled
			},
		},
		store,
		jq,
		blacklist,
		ingestion.Config{
			ChainID:       domain.ChainEthereumMainnet,
			StartBlock:    100,
			TokenQueue:    "token_index",
			QueueCapacity: 1,
			RetryDelay:    time.Minute,
		},
		clock,
	)
	defer func() { _ = runner.Close() }()

	err := runner.Run(ctx)
	require.Error(t, err)
	assert.ErrorIs(t, err, context.Canceled)
	retryCh <- time.Now()
	waitForSignal(t, flushed)
}

func waitForSignal(t *testing.T, signal <-chan struct{}) {
	t.Helper()

	select {
	case <-signal:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for ingestion flush")
	}
}

func mintEvent() *domain.BlockchainEvent {
	return &domain.BlockchainEvent{
		Chain:           domain.ChainEthereumMainnet,
		Standard:        domain.StandardERC721,
		ContractAddress: "0x123",
		TokenNumber:     "1",
		EventType:       domain.EventTypeMint,
		ToAddress:       stringPtr("0xto"),
		Quantity:        "1",
		TxHash:          "0xtx",
		BlockNumber:     100,
	}
}

func transferEvent() *domain.BlockchainEvent {
	return &domain.BlockchainEvent{
		Chain:           domain.ChainEthereumMainnet,
		Standard:        domain.StandardERC721,
		ContractAddress: "0x123",
		TokenNumber:     "1",
		EventType:       domain.EventTypeTransfer,
		FromAddress:     stringPtr("0xfrom"),
		ToAddress:       stringPtr("0xto"),
		Quantity:        "1",
		TxHash:          "0xtx",
		BlockNumber:     100,
	}
}

func stringPtr(v string) *string {
	return &v
}
