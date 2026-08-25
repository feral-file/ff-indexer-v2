package ethereum_test

import (
	"context"
	"errors"
	"math/big"
	"testing"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/feral-file/ff-indexer-v2/internal/domain"
	"github.com/feral-file/ff-indexer-v2/internal/mocks"
	ethprovider "github.com/feral-file/ff-indexer-v2/internal/providers/ethereum"
	"github.com/feral-file/ff-indexer-v2/internal/providers/ethereum/adapters"
	"github.com/feral-file/ff-indexer-v2/internal/providers/ethereum/helpers"
)

// headFixture wires a mock client whose newHeads subscription has the given
// block numbers queued at subscribe time. It returns the client, a push
// function for delivering later heads (call it from a mock callback — heads
// pushed while the subscriber is mid-fetch are read afterwards, so tests can
// pin the fetch-per-head sequence without racing the head coalescing), and the
// subscription's error channel for injecting a transport failure.
func headFixture(t *testing.T, ctrl *gomock.Controller, initial ...uint64) (*mocks.MockEthereumProviderClient, func(uint64), chan error) {
	t.Helper()
	mockClient := mocks.NewMockEthereumProviderClient(ctrl)
	subErrCh := make(chan error, 1)
	var headCh chan<- *types.Header
	push := func(h uint64) { headCh <- &types.Header{Number: new(big.Int).SetUint64(h)} }
	mockClient.EXPECT().
		SubscribeNewHead(gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, ch chan<- *types.Header) (ethereum.Subscription, error) {
			headCh = ch
			for _, h := range initial {
				push(h)
			}
			return &mockSubscription{errCh: subErrCh}, nil
		})
	return mockClient, push, subErrCh
}

func transferLog(block uint64, index uint) types.Log {
	return types.Log{
		BlockNumber: block,
		Index:       index,
		Topics:      []common.Hash{helpers.TransferEventSignature},
	}
}

func eventFor(vLog types.Log) *domain.BlockchainEvent {
	return &domain.BlockchainEvent{
		Chain:       domain.ChainEthereumMainnet,
		EventType:   domain.EventTypeTransfer,
		TokenNumber: "1",
		TxHash:      "0xabc",
		BlockNumber: vLog.BlockNumber,
		LogIndex:    uint64(vLog.Index),
	}
}

// TestSubscribeEvents_FetchesEachHeadBlock pins the steady-state contract: with
// no gap, every head triggers exactly one fetch for that single block, and
// events reach the handler in fetch order.
func TestSubscribeEvents_FetchesEachHeadBlock(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	mockClient, push, _ := headFixture(t, ctrl, 100)
	subscriber, err := ethprovider.NewSubscriber(ethprovider.Config{ChainID: domain.ChainEthereumMainnet}, mockClient)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	log100, log101 := transferLog(100, 3), transferLog(101, 0)
	gomock.InOrder(
		mockClient.EXPECT().FetchIngestionLogs(gomock.Any(), uint64(100), uint64(100)).DoAndReturn(
			func(context.Context, uint64, uint64) ([]types.Log, error) {
				push(101)
				return []types.Log{log100}, nil
			}),
		mockClient.EXPECT().ParseEventLog(gomock.Any(), log100).Return(eventFor(log100), nil),
		mockClient.EXPECT().FetchIngestionLogs(gomock.Any(), uint64(101), uint64(101)).Return([]types.Log{log101}, nil),
		mockClient.EXPECT().ParseEventLog(gomock.Any(), log101).DoAndReturn(func(context.Context, types.Log) (*domain.BlockchainEvent, error) {
			cancel()
			return eventFor(log101), nil
		}),
	)

	var seen []uint64
	err = subscriber.SubscribeEvents(ctx, 100, func(e *domain.BlockchainEvent) error {
		seen = append(seen, e.BlockNumber)
		return nil
	})
	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, []uint64{100, 101}, seen)
}

// TestSubscribeEvents_FillsGapToHead pins the resume contract that the old
// eth_subscribe("logs") stream could not honor: when the first head is ahead of
// fromBlock (restart or socket drop), the whole gap is fetched in one range
// before live blocks continue from head+1.
func TestSubscribeEvents_FillsGapToHead(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	mockClient, push, _ := headFixture(t, ctrl, 105)
	subscriber, err := ethprovider.NewSubscriber(ethprovider.Config{ChainID: domain.ChainEthereumMainnet, MaxCatchupBlocks: 10}, mockClient)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	gomock.InOrder(
		mockClient.EXPECT().FetchIngestionLogs(gomock.Any(), uint64(100), uint64(105)).DoAndReturn(
			func(context.Context, uint64, uint64) ([]types.Log, error) {
				push(106)
				return nil, nil
			}),
		mockClient.EXPECT().FetchIngestionLogs(gomock.Any(), uint64(106), uint64(106)).DoAndReturn(
			func(context.Context, uint64, uint64) ([]types.Log, error) {
				cancel()
				return nil, nil
			}),
	)

	err = subscriber.SubscribeEvents(ctx, 100, func(*domain.BlockchainEvent) error {
		t.Fatal("no events expected")
		return nil
	})
	require.ErrorIs(t, err, context.Canceled)
}

// TestSubscribeEvents_CoalescesQueuedHeads pins that heads queued during a slow
// fetch collapse into one range fetch instead of one fetch per head.
func TestSubscribeEvents_CoalescesQueuedHeads(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	mockClient := mocks.NewMockEthereumProviderClient(ctrl)
	subErrCh := make(chan error, 1)
	// Deliver all heads synchronously inside the subscribe call so they are
	// already queued when the subscriber reads the first one.
	mockClient.EXPECT().
		SubscribeNewHead(gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, ch chan<- *types.Header) (ethereum.Subscription, error) {
			for _, h := range []uint64{100, 101, 102} {
				ch <- &types.Header{Number: new(big.Int).SetUint64(h)}
			}
			return &mockSubscription{errCh: subErrCh}, nil
		})
	subscriber, err := ethprovider.NewSubscriber(ethprovider.Config{ChainID: domain.ChainEthereumMainnet}, mockClient)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mockClient.EXPECT().FetchIngestionLogs(gomock.Any(), uint64(100), uint64(102)).DoAndReturn(
		func(context.Context, uint64, uint64) ([]types.Log, error) {
			cancel()
			return nil, nil
		})

	err = subscriber.SubscribeEvents(ctx, 100, func(*domain.BlockchainEvent) error { return nil })
	require.ErrorIs(t, err, context.Canceled)
}

// TestSubscribeEvents_RefetchesOnLowerHead pins reorg handling: a head below
// the next expected block re-fetches from that head and resumes after it.
func TestSubscribeEvents_RefetchesOnLowerHead(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	mockClient, push, _ := headFixture(t, ctrl, 100)
	subscriber, err := ethprovider.NewSubscriber(ethprovider.Config{ChainID: domain.ChainEthereumMainnet}, mockClient)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	gomock.InOrder(
		mockClient.EXPECT().FetchIngestionLogs(gomock.Any(), uint64(100), uint64(100)).DoAndReturn(
			func(context.Context, uint64, uint64) ([]types.Log, error) {
				push(99)
				return nil, nil
			}),
		mockClient.EXPECT().FetchIngestionLogs(gomock.Any(), uint64(99), uint64(99)).DoAndReturn(
			func(context.Context, uint64, uint64) ([]types.Log, error) {
				push(100)
				return nil, nil
			}),
		mockClient.EXPECT().FetchIngestionLogs(gomock.Any(), uint64(100), uint64(100)).DoAndReturn(
			func(context.Context, uint64, uint64) ([]types.Log, error) {
				cancel()
				return nil, nil
			}),
	)

	err = subscriber.SubscribeEvents(ctx, 100, func(*domain.BlockchainEvent) error { return nil })
	require.ErrorIs(t, err, context.Canceled)
}

// TestSubscribeEvents_CatchupTooLargeFails pins the cost guard: a gap wider
// than MaxCatchupBlocks is a fatal, named error before any logs are fetched.
func TestSubscribeEvents_CatchupTooLargeFails(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	mockClient, _, _ := headFixture(t, ctrl, 1_000_000)
	subscriber, err := ethprovider.NewSubscriber(ethprovider.Config{ChainID: domain.ChainEthereumMainnet, MaxCatchupBlocks: 50_000}, mockClient)
	require.NoError(t, err)

	err = subscriber.SubscribeEvents(context.Background(), 1, func(*domain.BlockchainEvent) error { return nil })
	require.ErrorIs(t, err, ethprovider.ErrCatchupTooLarge)
	require.Contains(t, err.Error(), "need blocks 1-1000000")
}

// TestSubscribeEvents_UnboundedCatchupWhenZero pins that MaxCatchupBlocks=0
// disables the guard, matching the repo's zero-value-disables-guard convention.
func TestSubscribeEvents_UnboundedCatchupWhenZero(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	mockClient, _, _ := headFixture(t, ctrl, 1_000_000)
	subscriber, err := ethprovider.NewSubscriber(ethprovider.Config{ChainID: domain.ChainEthereumMainnet}, mockClient)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	mockClient.EXPECT().FetchIngestionLogs(gomock.Any(), uint64(1), uint64(1_000_000)).DoAndReturn(
		func(context.Context, uint64, uint64) ([]types.Log, error) {
			cancel()
			return nil, nil
		})

	err = subscriber.SubscribeEvents(ctx, 1, func(*domain.BlockchainEvent) error { return nil })
	require.ErrorIs(t, err, context.Canceled)
}

func TestSubscribeEvents_FetchErrorFails(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	mockClient, _, _ := headFixture(t, ctrl, 100)
	subscriber, err := ethprovider.NewSubscriber(ethprovider.Config{ChainID: domain.ChainEthereumMainnet}, mockClient)
	require.NoError(t, err)

	fetchErr := errors.New("rpc down")
	mockClient.EXPECT().FetchIngestionLogs(gomock.Any(), uint64(100), uint64(100)).Return(nil, fetchErr)

	err = subscriber.SubscribeEvents(context.Background(), 100, func(*domain.BlockchainEvent) error { return nil })
	require.ErrorIs(t, err, fetchErr)
	require.Contains(t, err.Error(), "fetch ingestion logs for blocks 100-100")
}

func TestSubscribeEvents_SubscribeErrorFails(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	mockClient := mocks.NewMockEthereumProviderClient(ctrl)
	subErr := errors.New("dial failed")
	mockClient.EXPECT().SubscribeNewHead(gomock.Any(), gomock.Any()).Return(nil, subErr)
	subscriber, err := ethprovider.NewSubscriber(ethprovider.Config{ChainID: domain.ChainEthereumMainnet}, mockClient)
	require.NoError(t, err)

	err = subscriber.SubscribeEvents(context.Background(), 100, func(*domain.BlockchainEvent) error { return nil })
	require.ErrorIs(t, err, subErr)
}

func TestSubscribeEvents_SubscriptionErrorFails(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	mockClient, _, subErrCh := headFixture(t, ctrl)
	subscriber, err := ethprovider.NewSubscriber(ethprovider.Config{ChainID: domain.ChainEthereumMainnet}, mockClient)
	require.NoError(t, err)

	transportErr := errors.New("websocket closed 1006")
	subErrCh <- transportErr

	err = subscriber.SubscribeEvents(context.Background(), 100, func(*domain.BlockchainEvent) error { return nil })
	require.ErrorIs(t, err, transportErr)
	require.Contains(t, err.Error(), "new heads subscription error")
}

func TestSubscribeEvents_ParseErrorFails(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	mockClient, _, _ := headFixture(t, ctrl, 100)
	subscriber, err := ethprovider.NewSubscriber(ethprovider.Config{ChainID: domain.ChainEthereumMainnet}, mockClient)
	require.NoError(t, err)

	vLog := transferLog(100, 1)
	parseErr := errors.New("resolve block timestamp for block 100: get block timestamp: boom")
	mockClient.EXPECT().FetchIngestionLogs(gomock.Any(), uint64(100), uint64(100)).Return([]types.Log{vLog}, nil)
	mockClient.EXPECT().ParseEventLog(gomock.Any(), vLog).Return(nil, parseErr)

	err = subscriber.SubscribeEvents(context.Background(), 100, func(*domain.BlockchainEvent) error { return nil })
	require.ErrorIs(t, err, parseErr)
	require.Contains(t, err.Error(), "parse log at block 100 index 1")
}

func TestSubscribeEvents_HandlerErrorFails(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	mockClient, _, _ := headFixture(t, ctrl, 100)
	subscriber, err := ethprovider.NewSubscriber(ethprovider.Config{ChainID: domain.ChainEthereumMainnet}, mockClient)
	require.NoError(t, err)

	vLog := transferLog(100, 1)
	mockClient.EXPECT().FetchIngestionLogs(gomock.Any(), uint64(100), uint64(100)).Return([]types.Log{vLog}, nil)
	mockClient.EXPECT().ParseEventLog(gomock.Any(), vLog).Return(eventFor(vLog), nil)

	handlerErr := errors.New("queue closed")
	err = subscriber.SubscribeEvents(context.Background(), 100, func(*domain.BlockchainEvent) error { return handlerErr })
	require.ErrorIs(t, err, handlerErr)
}

// skipCase runs one fetched block with two logs where the second parse returns
// skipErr (or a nil event) and asserts only the first reaches the handler.
func skipCase(t *testing.T, parsed *domain.BlockchainEvent, skipErr error) {
	t.Helper()

	ctrl := gomock.NewController(t)
	mockClient, _, _ := headFixture(t, ctrl, 100)
	subscriber, err := ethprovider.NewSubscriber(ethprovider.Config{ChainID: domain.ChainEthereumMainnet}, mockClient)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	first := transferLog(100, 1)
	second := types.Log{BlockNumber: 100, Index: 2, Topics: []common.Hash{common.HexToHash("0xabc")}}
	mockClient.EXPECT().FetchIngestionLogs(gomock.Any(), uint64(100), uint64(100)).Return([]types.Log{first, second}, nil)
	gomock.InOrder(
		mockClient.EXPECT().ParseEventLog(gomock.Any(), first).Return(eventFor(first), nil),
		mockClient.EXPECT().ParseEventLog(gomock.Any(), second).DoAndReturn(func(context.Context, types.Log) (*domain.BlockchainEvent, error) {
			cancel()
			return parsed, skipErr
		}),
	)

	var handlerCalls int
	err = subscriber.SubscribeEvents(ctx, 100, func(*domain.BlockchainEvent) error {
		handlerCalls++
		return nil
	})
	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, 1, handlerCalls)
}

func TestSubscribeEvents_SkipsUnconfiguredContract(t *testing.T) {
	t.Parallel()
	skipCase(t, nil, adapters.ErrUnconfiguredContract)
}

func TestSubscribeEvents_SkipsUnexpectedEvent(t *testing.T) {
	t.Parallel()
	skipCase(t, nil, adapters.ErrUnexpectedEvent)
}

func TestSubscribeEvents_SkipsNilParsedEvent(t *testing.T) {
	t.Parallel()
	skipCase(t, nil, nil)
}

type mockSubscription struct {
	errCh chan error
}

func (m *mockSubscription) Unsubscribe() {
	close(m.errCh)
}

func (m *mockSubscription) Err() <-chan error {
	return m.errCh
}
