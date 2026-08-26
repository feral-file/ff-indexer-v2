package ethereum_test

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/feral-file/ff-indexer-v2/internal/adapter"
	"github.com/feral-file/ff-indexer-v2/internal/blockchain"
	"github.com/feral-file/ff-indexer-v2/internal/domain"
	"github.com/feral-file/ff-indexer-v2/internal/mocks"
	ethprovider "github.com/feral-file/ff-indexer-v2/internal/providers/ethereum"
	"github.com/feral-file/ff-indexer-v2/internal/providers/ethereum/adapters"
	"github.com/feral-file/ff-indexer-v2/internal/providers/ethereum/helpers"
)

// headChain builds parent-linked heads so the subscriber's continuity check
// sees a canonical chain unless a test deliberately breaks it. Hashes are
// synthetic: the subscriber only ever compares them, never recomputes them.
type headChain struct {
	last *adapter.BlockHead
	seq  uint64
}

func (c *headChain) hash(n uint64, tag string) common.Hash {
	c.seq++
	return common.BytesToHash([]byte(fmt.Sprintf("%s-%d-%d", tag, n, c.seq)))
}

// next returns a head at height n whose parent is the previously built head.
func (c *headChain) next(n uint64) *adapter.BlockHead {
	h := &adapter.BlockHead{Number: hexutil.Uint64(n), Hash: c.hash(n, "canonical")}
	if c.last != nil {
		h.ParentHash = c.last.Hash
	}
	c.last = h
	return h
}

// fork returns a head at height n that does NOT descend from the chain built
// so far (a replacement block after a reorg); it becomes the new tip.
func (c *headChain) fork(n uint64) *adapter.BlockHead {
	h := &adapter.BlockHead{Number: hexutil.Uint64(n), Hash: c.hash(n, "fork"), ParentHash: common.HexToHash("0xdead")}
	c.last = h
	return h
}

// headFixture wires a mock client whose newHeads subscription has the given
// headers queued at subscribe time. It returns the client, a push function for
// delivering later heads (call it from a mock callback — heads pushed while
// the subscriber is mid-fetch are read afterwards, so tests can pin the
// fetch-per-head sequence without racing the head coalescing), and the
// subscription's error channel for injecting a transport failure.
func headFixture(t *testing.T, ctrl *gomock.Controller, initial ...*adapter.BlockHead) (*mocks.MockEthereumProviderClient, func(*adapter.BlockHead), chan error) {
	t.Helper()
	mockClient := mocks.NewMockEthereumProviderClient(ctrl)
	subErrCh := make(chan error, 1)
	var headCh chan<- *adapter.BlockHead
	push := func(h *adapter.BlockHead) { headCh <- h }
	mockClient.EXPECT().
		SubscribeNewHead(gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, ch chan<- *adapter.BlockHead) (ethereum.Subscription, error) {
			headCh = ch
			for _, h := range initial {
				push(h)
			}
			return &mockSubscription{errCh: subErrCh}, nil
		})
	return mockClient, push, subErrCh
}

func newTestSubscriber(t *testing.T, client ethprovider.EthereumClient, maxCatchup uint64) blockchain.EventSource {
	t.Helper()
	sub, err := ethprovider.NewSubscriber(ethprovider.Config{ChainID: domain.ChainEthereumMainnet, MaxCatchupBlocks: maxCatchup}, client)
	require.NoError(t, err)
	return sub
}

// fetchThenPush returns a FetchIngestionLogs callback that queues heads for
// the next loop iteration and returns the given logs.
func fetchThenPush(push func(*adapter.BlockHead), heads []*adapter.BlockHead, logs ...types.Log) func(context.Context, uint64, uint64) ([]types.Log, error) {
	return func(context.Context, uint64, uint64) ([]types.Log, error) {
		for _, h := range heads {
			push(h)
		}
		return logs, nil
	}
}

// fetchThenCancel returns a FetchIngestionLogs callback that ends the test.
func fetchThenCancel(cancel context.CancelFunc, logs ...types.Log) func(context.Context, uint64, uint64) ([]types.Log, error) {
	return func(context.Context, uint64, uint64) ([]types.Log, error) {
		cancel()
		return logs, nil
	}
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
	chain := &headChain{}
	mockClient, push, _ := headFixture(t, ctrl, chain.next(100))
	subscriber := newTestSubscriber(t, mockClient, 0)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	log100, log101 := transferLog(100, 3), transferLog(101, 0)
	gomock.InOrder(
		mockClient.EXPECT().FetchIngestionLogs(gomock.Any(), uint64(100), uint64(100)).
			DoAndReturn(fetchThenPush(push, []*adapter.BlockHead{chain.next(101)}, log100)),
		mockClient.EXPECT().ParseEventLog(gomock.Any(), log100).Return(eventFor(log100), nil),
		mockClient.EXPECT().FetchIngestionLogs(gomock.Any(), uint64(101), uint64(101)).Return([]types.Log{log101}, nil),
		mockClient.EXPECT().ParseEventLog(gomock.Any(), log101).DoAndReturn(func(context.Context, types.Log) (*domain.BlockchainEvent, error) {
			cancel()
			return eventFor(log101), nil
		}),
	)

	var seen []uint64
	err := subscriber.SubscribeEvents(ctx, 100, func(e *domain.BlockchainEvent) error {
		seen = append(seen, e.BlockNumber)
		return nil
	})
	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, []uint64{100, 101}, seen)
}

// TestSubscribeEvents_FillsGapToHead pins the resume contract that the old
// eth_subscribe("logs") stream could not honor: when the first head is ahead of
// fromBlock (restart or socket drop), the whole gap is fetched before live
// blocks continue from head+1.
func TestSubscribeEvents_FillsGapToHead(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	chain := &headChain{}
	mockClient, push, _ := headFixture(t, ctrl, chain.next(105))
	subscriber := newTestSubscriber(t, mockClient, 10)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	gomock.InOrder(
		mockClient.EXPECT().FetchIngestionLogs(gomock.Any(), uint64(100), uint64(105)).
			DoAndReturn(fetchThenPush(push, []*adapter.BlockHead{chain.next(106)})),
		mockClient.EXPECT().FetchIngestionLogs(gomock.Any(), uint64(106), uint64(106)).DoAndReturn(fetchThenCancel(cancel)),
	)

	err := subscriber.SubscribeEvents(ctx, 100, func(*domain.BlockchainEvent) error {
		t.Fatal("no events expected")
		return nil
	})
	require.ErrorIs(t, err, context.Canceled)
}

// TestSubscribeEvents_CatchupIsBatched pins that a long gap is fetched in
// bounded batches, in order, instead of one range that materializes every raw
// log at once (~470 per mainnet block, mostly ERC-20 noise discarded later).
func TestSubscribeEvents_CatchupIsBatched(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	chain := &headChain{}
	mockClient, _, _ := headFixture(t, ctrl, chain.next(145))
	subscriber := newTestSubscriber(t, mockClient, 0)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	log110, log130 := transferLog(110, 0), transferLog(130, 0)
	gomock.InOrder(
		mockClient.EXPECT().FetchIngestionLogs(gomock.Any(), uint64(100), uint64(119)).Return([]types.Log{log110}, nil),
		mockClient.EXPECT().ParseEventLog(gomock.Any(), log110).Return(eventFor(log110), nil),
		mockClient.EXPECT().FetchIngestionLogs(gomock.Any(), uint64(120), uint64(139)).Return([]types.Log{log130}, nil),
		mockClient.EXPECT().ParseEventLog(gomock.Any(), log130).Return(eventFor(log130), nil),
		mockClient.EXPECT().FetchIngestionLogs(gomock.Any(), uint64(140), uint64(145)).DoAndReturn(fetchThenCancel(cancel)),
	)

	var seen []uint64
	err := subscriber.SubscribeEvents(ctx, 100, func(e *domain.BlockchainEvent) error {
		seen = append(seen, e.BlockNumber)
		return nil
	})
	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, []uint64{110, 130}, seen, "batches emit in order before the next fetch")
}

// TestSubscribeEvents_CoalescesQueuedHeads pins that heads queued during a slow
// fetch collapse into one range fetch instead of one fetch per head.
func TestSubscribeEvents_CoalescesQueuedHeads(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	chain := &headChain{}
	mockClient, _, _ := headFixture(t, ctrl, chain.next(100), chain.next(101), chain.next(102))
	subscriber := newTestSubscriber(t, mockClient, 0)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mockClient.EXPECT().FetchIngestionLogs(gomock.Any(), uint64(100), uint64(102)).DoAndReturn(fetchThenCancel(cancel))

	err := subscriber.SubscribeEvents(ctx, 100, func(*domain.BlockchainEvent) error { return nil })
	require.ErrorIs(t, err, context.Canceled)
}

// newLaggedSubscriber is newTestSubscriber with a confirmation lag.
func newLaggedSubscriber(t *testing.T, client ethprovider.EthereumClient, confirmations uint64) blockchain.EventSource {
	t.Helper()
	sub, err := ethprovider.NewSubscriber(ethprovider.Config{ChainID: domain.ChainEthereumMainnet, ConfirmationBlocks: confirmations}, client)
	require.NoError(t, err)
	return sub
}

// TestSubscribeEvents_EmitsOnlyConfirmedBlocks pins the confirmation lag: with
// K=2, heads 100..102 confirm only block 100; head 103 confirms 101. Nothing at
// or above head-K is ever fetched.
func TestSubscribeEvents_EmitsOnlyConfirmedBlocks(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	chain := &headChain{}
	mockClient, push, _ := headFixture(t, ctrl, chain.next(100), chain.next(101), chain.next(102))
	subscriber := newLaggedSubscriber(t, mockClient, 2)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	gomock.InOrder(
		mockClient.EXPECT().FetchIngestionLogs(gomock.Any(), uint64(100), uint64(100)).
			DoAndReturn(fetchThenPush(push, []*adapter.BlockHead{chain.next(103)})),
		mockClient.EXPECT().FetchIngestionLogs(gomock.Any(), uint64(101), uint64(101)).DoAndReturn(fetchThenCancel(cancel)),
	)

	err := subscriber.SubscribeEvents(ctx, 100, func(*domain.BlockchainEvent) error { return nil })
	require.ErrorIs(t, err, context.Canceled)
}

// TestSubscribeEvents_ShallowReorgWithinLagIsAbsorbed pins the reorg strategy:
// a replacement head above the emitted range (inside the lag) changes nothing
// already emitted and triggers no re-fetch; emission simply continues on the
// new canonical chain once it is confirmed.
func TestSubscribeEvents_ShallowReorgWithinLagIsAbsorbed(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	chain := &headChain{}
	mockClient, push, _ := headFixture(t, ctrl, chain.next(100), chain.next(101), chain.next(102))
	subscriber := newLaggedSubscriber(t, mockClient, 2)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	gomock.InOrder(
		// 100 emitted; then 102 is replaced (fork) and 103 builds on the fork.
		mockClient.EXPECT().FetchIngestionLogs(gomock.Any(), uint64(100), uint64(100)).
			DoAndReturn(fetchThenPush(push, []*adapter.BlockHead{chain.fork(102), chain.next(103)})),
		// tip 103 confirms 101 only — no re-fetch of 100, 102 not yet emitted.
		mockClient.EXPECT().FetchIngestionLogs(gomock.Any(), uint64(101), uint64(101)).DoAndReturn(fetchThenCancel(cancel)),
	)

	err := subscriber.SubscribeEvents(ctx, 100, func(*domain.BlockchainEvent) error { return nil })
	require.ErrorIs(t, err, context.Canceled)
}

// TestSubscribeEvents_DeepReorgIsReportedNotReplayed pins that a replacement
// of an already-emitted height is never re-fetched: the number-ordered runner
// cannot take it (it would flush the open block and reject the replacement),
// so the subscriber reports it and continues from the next unemitted height.
func TestSubscribeEvents_DeepReorgIsReportedNotReplayed(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	chain := &headChain{}
	mockClient, push, _ := headFixture(t, ctrl, chain.next(100))
	subscriber := newTestSubscriber(t, mockClient, 0) // no lag: 100 is emitted immediately

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	gomock.InOrder(
		// 100 emitted; then 100 itself is replaced and 101 builds on the fork.
		mockClient.EXPECT().FetchIngestionLogs(gomock.Any(), uint64(100), uint64(100)).
			DoAndReturn(fetchThenPush(push, []*adapter.BlockHead{chain.fork(100), chain.next(101)})),
		// Only 101 is fetched; the replaced 100 is reported, not replayed.
		mockClient.EXPECT().FetchIngestionLogs(gomock.Any(), uint64(101), uint64(101)).DoAndReturn(fetchThenCancel(cancel)),
	)

	err := subscriber.SubscribeEvents(ctx, 100, func(*domain.BlockchainEvent) error { return nil })
	require.ErrorIs(t, err, context.Canceled)
}

// TestSubscribeEvents_FutureStartBlockIsHardLowerBound pins that a start block
// ahead of the chain is honored literally: heads below it are ignored (not
// treated as replaced emitted blocks), and fetching starts exactly there.
func TestSubscribeEvents_FutureStartBlockIsHardLowerBound(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	chain := &headChain{}
	mockClient, push, _ := headFixture(t, ctrl, chain.next(400), chain.next(401), chain.next(500))
	subscriber := newTestSubscriber(t, mockClient, 0)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	gomock.InOrder(
		mockClient.EXPECT().FetchIngestionLogs(gomock.Any(), uint64(500), uint64(500)).
			DoAndReturn(fetchThenPush(push, []*adapter.BlockHead{chain.fork(499), chain.next(501)})),
		mockClient.EXPECT().FetchIngestionLogs(gomock.Any(), uint64(501), uint64(501)).DoAndReturn(fetchThenCancel(cancel)),
	)

	err := subscriber.SubscribeEvents(ctx, 500, func(*domain.BlockchainEvent) error { return nil })
	require.ErrorIs(t, err, context.Canceled)
}

// TestSubscribeEvents_ReportsScannedRangeAfterEvents pins the progress
// contract: after each emitted range the subscriber reports its upper bound,
// strictly after the range's events were handled, and a rejected report stops
// the subscription.
func TestSubscribeEvents_ReportsScannedRangeAfterEvents(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	chain := &headChain{}
	mockClient, push, _ := headFixture(t, ctrl, chain.next(105))
	subscriber := newTestSubscriber(t, mockClient, 0)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	log103 := transferLog(103, 0)
	gomock.InOrder(
		mockClient.EXPECT().FetchIngestionLogs(gomock.Any(), uint64(100), uint64(105)).
			DoAndReturn(fetchThenPush(push, []*adapter.BlockHead{chain.next(106)}, log103)),
		mockClient.EXPECT().ParseEventLog(gomock.Any(), log103).Return(eventFor(log103), nil),
		mockClient.EXPECT().FetchIngestionLogs(gomock.Any(), uint64(106), uint64(106)).Return(nil, nil),
	)

	var trace []string
	reportErr := errors.New("runner closed")
	subscriber.(blockchain.ProgressReporter).SetProgressHandler(func(through uint64) error {
		trace = append(trace, fmt.Sprintf("scanned:%d", through))
		if through == 106 {
			return reportErr
		}
		return nil
	})

	err := subscriber.SubscribeEvents(ctx, 100, func(e *domain.BlockchainEvent) error {
		trace = append(trace, fmt.Sprintf("event:%d", e.BlockNumber))
		return nil
	})
	require.ErrorIs(t, err, reportErr)
	require.Equal(t, []string{"event:103", "scanned:105", "scanned:106"}, trace)
}

// TestSubscribeEvents_CatchupTooLargeFails pins the cost guard: a gap wider
// than MaxCatchupBlocks is a fatal, named error before any logs are fetched.
func TestSubscribeEvents_CatchupTooLargeFails(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	chain := &headChain{}
	mockClient, _, _ := headFixture(t, ctrl, chain.next(1_000_000))
	subscriber := newTestSubscriber(t, mockClient, 50_000)

	err := subscriber.SubscribeEvents(context.Background(), 1, func(*domain.BlockchainEvent) error { return nil })
	require.ErrorIs(t, err, ethprovider.ErrCatchupTooLarge)
	require.Contains(t, err.Error(), "need blocks 1-1000000")
}

// TestSubscribeEvents_UnboundedCatchupWhenZero pins that MaxCatchupBlocks=0
// disables the guard, matching the repo's zero-value-disables-guard convention
// (the range is still walked in batches).
func TestSubscribeEvents_UnboundedCatchupWhenZero(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	chain := &headChain{}
	mockClient, _, _ := headFixture(t, ctrl, chain.next(1_000))
	subscriber := newTestSubscriber(t, mockClient, 0)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	mockClient.EXPECT().FetchIngestionLogs(gomock.Any(), uint64(1), uint64(20)).DoAndReturn(fetchThenCancel(cancel))

	err := subscriber.SubscribeEvents(ctx, 1, func(*domain.BlockchainEvent) error { return nil })
	require.ErrorIs(t, err, context.Canceled)
}

func TestSubscribeEvents_FetchErrorFails(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	chain := &headChain{}
	mockClient, _, _ := headFixture(t, ctrl, chain.next(100))
	subscriber := newTestSubscriber(t, mockClient, 0)

	fetchErr := errors.New("rpc down")
	mockClient.EXPECT().FetchIngestionLogs(gomock.Any(), uint64(100), uint64(100)).Return(nil, fetchErr)

	err := subscriber.SubscribeEvents(context.Background(), 100, func(*domain.BlockchainEvent) error { return nil })
	require.ErrorIs(t, err, fetchErr)
	require.Contains(t, err.Error(), "fetch ingestion logs for blocks 100-100")
}

func TestSubscribeEvents_SubscribeErrorFails(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	mockClient := mocks.NewMockEthereumProviderClient(ctrl)
	subErr := errors.New("dial failed")
	mockClient.EXPECT().SubscribeNewHead(gomock.Any(), gomock.Any()).Return(nil, subErr)
	subscriber := newTestSubscriber(t, mockClient, 0)

	err := subscriber.SubscribeEvents(context.Background(), 100, func(*domain.BlockchainEvent) error { return nil })
	require.ErrorIs(t, err, subErr)
}

func TestSubscribeEvents_SubscriptionErrorFails(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	mockClient, _, subErrCh := headFixture(t, ctrl)
	subscriber := newTestSubscriber(t, mockClient, 0)

	transportErr := errors.New("websocket closed 1006")
	subErrCh <- transportErr

	err := subscriber.SubscribeEvents(context.Background(), 100, func(*domain.BlockchainEvent) error { return nil })
	require.ErrorIs(t, err, transportErr)
	require.Contains(t, err.Error(), "new heads subscription error")
}

func TestSubscribeEvents_ParseErrorFails(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	chain := &headChain{}
	mockClient, _, _ := headFixture(t, ctrl, chain.next(100))
	subscriber := newTestSubscriber(t, mockClient, 0)

	vLog := transferLog(100, 1)
	parseErr := errors.New("resolve block timestamp for block 100: get block timestamp: boom")
	mockClient.EXPECT().FetchIngestionLogs(gomock.Any(), uint64(100), uint64(100)).Return([]types.Log{vLog}, nil)
	mockClient.EXPECT().ParseEventLog(gomock.Any(), vLog).Return(nil, parseErr)

	err := subscriber.SubscribeEvents(context.Background(), 100, func(*domain.BlockchainEvent) error { return nil })
	require.ErrorIs(t, err, parseErr)
	require.Contains(t, err.Error(), "parse log at block 100 index 1")
}

func TestSubscribeEvents_HandlerErrorFails(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	chain := &headChain{}
	mockClient, _, _ := headFixture(t, ctrl, chain.next(100))
	subscriber := newTestSubscriber(t, mockClient, 0)

	vLog := transferLog(100, 1)
	mockClient.EXPECT().FetchIngestionLogs(gomock.Any(), uint64(100), uint64(100)).Return([]types.Log{vLog}, nil)
	mockClient.EXPECT().ParseEventLog(gomock.Any(), vLog).Return(eventFor(vLog), nil)

	handlerErr := errors.New("queue closed")
	err := subscriber.SubscribeEvents(context.Background(), 100, func(*domain.BlockchainEvent) error { return handlerErr })
	require.ErrorIs(t, err, handlerErr)
}

// skipCase runs one fetched block with two logs where the second parse returns
// skipErr (or a nil event) and asserts only the first reaches the handler.
func skipCase(t *testing.T, parsed *domain.BlockchainEvent, skipErr error) {
	t.Helper()

	ctrl := gomock.NewController(t)
	chain := &headChain{}
	mockClient, _, _ := headFixture(t, ctrl, chain.next(100))
	subscriber := newTestSubscriber(t, mockClient, 0)

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
	err := subscriber.SubscribeEvents(ctx, 100, func(*domain.BlockchainEvent) error {
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
