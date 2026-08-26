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
	last     *adapter.BlockHead
	byHeight map[uint64]*adapter.BlockHead
	seq      uint64
}

func (c *headChain) hash(n uint64, tag string) common.Hash {
	c.seq++
	return common.BytesToHash([]byte(fmt.Sprintf("%s-%d-%d", tag, n, c.seq)))
}

func (c *headChain) remember(h *adapter.BlockHead) *adapter.BlockHead {
	if c.byHeight == nil {
		c.byHeight = map[uint64]*adapter.BlockHead{}
	}
	c.byHeight[uint64(h.Number)] = h
	c.last = h
	return h
}

// next returns a head at height n whose parent is the previously built head.
func (c *headChain) next(n uint64) *adapter.BlockHead {
	h := &adapter.BlockHead{Number: hexutil.Uint64(n), Hash: c.hash(n, "canonical")}
	if c.last != nil {
		h.ParentHash = c.last.Hash
	}
	return c.remember(h)
}

// fork returns a replacement head at height n: a different block that still
// descends from the head built at n-1 (a one-block reorg at n). It becomes the
// new tip.
func (c *headChain) fork(n uint64) *adapter.BlockHead {
	h := &adapter.BlockHead{Number: hexutil.Uint64(n), Hash: c.hash(n, "fork"), ParentHash: common.HexToHash("0xdead")}
	if parent, ok := c.byHeight[n-1]; ok {
		h.ParentHash = parent.Hash
	}
	return c.remember(h)
}

// orphanTip returns a head at height n whose parent is a block this chain has
// never seen — a reorg below n announced only by this later tip.
func (c *headChain) orphanTip(n uint64, parent common.Hash) *adapter.BlockHead {
	return c.remember(&adapter.BlockHead{Number: hexutil.Uint64(n), Hash: c.hash(n, "orphan-tip"), ParentHash: parent})
}

func head(n uint64, hash, parent common.Hash) *adapter.BlockHead {
	return &adapter.BlockHead{Number: hexutil.Uint64(n), Hash: hash, ParentHash: parent}
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
	mockClient, _, _ := headFixture(t, ctrl, chain.next(125))
	subscriber := newTestSubscriber(t, mockClient, 0)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	log105, log115 := transferLog(105, 0), transferLog(115, 0)
	gomock.InOrder(
		mockClient.EXPECT().FetchIngestionLogs(gomock.Any(), uint64(100), uint64(109)).Return([]types.Log{log105}, nil),
		mockClient.EXPECT().ParseEventLog(gomock.Any(), log105).Return(eventFor(log105), nil),
		mockClient.EXPECT().FetchIngestionLogs(gomock.Any(), uint64(110), uint64(119)).Return([]types.Log{log115}, nil),
		mockClient.EXPECT().ParseEventLog(gomock.Any(), log115).Return(eventFor(log115), nil),
		mockClient.EXPECT().FetchIngestionLogs(gomock.Any(), uint64(120), uint64(125)).DoAndReturn(fetchThenCancel(cancel)),
	)

	var seen []uint64
	err := subscriber.SubscribeEvents(ctx, 100, func(e *domain.BlockchainEvent) error {
		seen = append(seen, e.BlockNumber)
		return nil
	})
	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, []uint64{105, 115}, seen, "batches emit in order before the next fetch")
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

	fork100 := chain.fork(100)
	gomock.InOrder(
		// 100 emitted; then 100 itself is replaced and 101 builds on the fork.
		mockClient.EXPECT().FetchIngestionLogs(gomock.Any(), uint64(100), uint64(100)).
			DoAndReturn(fetchThenPush(push, []*adapter.BlockHead{fork100, chain.next(101)})),
		// 101's parent is the fork, not the emitted 100: reconcile confirms 100 was replaced.
		mockClient.EXPECT().HeadByNumber(gomock.Any(), uint64(100)).Return(fork100, nil),
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

	h501 := chain.next(501)
	below := chain.fork(499) // a replacement below the start block: ignored
	gomock.InOrder(
		mockClient.EXPECT().FetchIngestionLogs(gomock.Any(), uint64(500), uint64(500)).
			DoAndReturn(fetchThenPush(push, []*adapter.BlockHead{below, h501})),
		mockClient.EXPECT().FetchIngestionLogs(gomock.Any(), uint64(501), uint64(501)).DoAndReturn(fetchThenCancel(cancel)),
	)

	err := subscriber.SubscribeEvents(ctx, 500, func(*domain.BlockchainEvent) error { return nil })
	require.ErrorIs(t, err, context.Canceled)
}

// TestSubscribeEvents_TipOnlyReorgReconcilesToEmittedBoundary pins reconcile:
// a reorg announced only by a later tip (B103 whose parent is an unseen B102)
// walks canonical heads down by number until the retained chain matches — here
// at the emitted block 100 — replacing stale retained heads, with no re-fetch
// of anything emitted.
func TestSubscribeEvents_TipOnlyReorgReconcilesToEmittedBoundary(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	chain := &headChain{}
	a100, a101, a102 := chain.next(100), chain.next(101), chain.next(102)
	mockClient, push, _ := headFixture(t, ctrl, a100, a101, a102)
	subscriber := newLaggedSubscriber(t, mockClient, 2)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	b101 := head(101, common.HexToHash("0xb101"), a100.Hash) // rejoins the emitted chain at 100
	b102 := head(102, common.HexToHash("0xb102"), b101.Hash)
	b103 := chain.orphanTip(103, b102.Hash)
	gomock.InOrder(
		mockClient.EXPECT().FetchIngestionLogs(gomock.Any(), uint64(100), uint64(100)).
			DoAndReturn(fetchThenPush(push, []*adapter.BlockHead{b103})),
		mockClient.EXPECT().HeadByNumber(gomock.Any(), uint64(102)).Return(b102, nil),
		mockClient.EXPECT().HeadByNumber(gomock.Any(), uint64(101)).Return(b101, nil),
		// retained 100 == b101's parent: walk stops; 101 is confirmed by tip 103.
		mockClient.EXPECT().FetchIngestionLogs(gomock.Any(), uint64(101), uint64(101)).DoAndReturn(fetchThenCancel(cancel)),
	)

	err := subscriber.SubscribeEvents(ctx, 100, func(*domain.BlockchainEvent) error { return nil })
	require.ErrorIs(t, err, context.Canceled)
}

// TestSubscribeEvents_StaleTipDoesNotShortenLag pins that a delayed head whose
// parent the node no longer considers canonical is discarded: it must not be
// retained or raise the confirmation tip, so the lag keeps its full depth for
// the canonical chain (which then confirms 101 only when A103 arrives).
func TestSubscribeEvents_StaleTipDoesNotShortenLag(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	chain := &headChain{}
	a100, a101, a102 := chain.next(100), chain.next(101), chain.next(102)
	_, _ = a100, a101
	mockClient, push, _ := headFixture(t, ctrl, a100, a101, a102)
	subscriber := newLaggedSubscriber(t, mockClient, 2)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	staleB103 := head(103, common.HexToHash("0xb103"), common.HexToHash("0xb102"))
	a103 := chain.next(103) // canonical, on A102
	gomock.InOrder(
		mockClient.EXPECT().FetchIngestionLogs(gomock.Any(), uint64(100), uint64(100)).
			DoAndReturn(fetchThenPush(push, []*adapter.BlockHead{staleB103})),
		// B103's parent disagrees with retained A102; the node confirms A102 is canonical.
		mockClient.EXPECT().HeadByNumber(gomock.Any(), uint64(102)).DoAndReturn(
			func(context.Context, uint64) (*adapter.BlockHead, error) {
				push(a103) // arrives after the stale tip was processed
				return a102, nil
			}),
		// No fetch of 101 on the stale tip; A103 confirms it.
		mockClient.EXPECT().FetchIngestionLogs(gomock.Any(), uint64(101), uint64(101)).DoAndReturn(fetchThenCancel(cancel)),
	)

	err := subscriber.SubscribeEvents(ctx, 100, func(*domain.BlockchainEvent) error { return nil })
	require.ErrorIs(t, err, context.Canceled)
}

// TestSubscribeEvents_ReorgAfterSingleHeadCatchupReachesBoundary pins the
// initial-gap case: the first head (A105, lag 2) emits 100..103 with no
// received head at 103, so the boundary hash is fetched and retained; a reorg
// announced only by B106 then bridges the unreceived 104 and reaches 103,
// where the replaced emitted block is reported — and emission continues from
// 104 by number, never replaying 103.
func TestSubscribeEvents_ReorgAfterSingleHeadCatchupReachesBoundary(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	chain := &headChain{}
	a105 := chain.next(105)
	mockClient, push, _ := headFixture(t, ctrl, a105)
	subscriber := newLaggedSubscriber(t, mockClient, 2)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	a103 := head(103, common.HexToHash("0xa103"), common.HexToHash("0xa102"))
	b103 := head(103, common.HexToHash("0xb103"), common.HexToHash("0xa102"))
	b104 := head(104, common.HexToHash("0xb104"), b103.Hash)
	b105 := head(105, common.HexToHash("0xb105"), b104.Hash)
	b106 := chain.orphanTip(106, b105.Hash)
	gomock.InOrder(
		mockClient.EXPECT().HeadByNumber(gomock.Any(), uint64(103)).Return(a103, nil), // emitted boundary retained
		mockClient.EXPECT().FetchIngestionLogs(gomock.Any(), uint64(100), uint64(103)).
			DoAndReturn(fetchThenPush(push, []*adapter.BlockHead{b106})),
		mockClient.EXPECT().HeadByNumber(gomock.Any(), uint64(105)).Return(b105, nil), // retained A105 replaced
		mockClient.EXPECT().HeadByNumber(gomock.Any(), uint64(104)).Return(b104, nil), // bridged (never received)
		mockClient.EXPECT().HeadByNumber(gomock.Any(), uint64(103)).Return(b103, nil), // emitted boundary replaced: reported
		mockClient.EXPECT().FetchIngestionLogs(gomock.Any(), uint64(104), uint64(104)).DoAndReturn(fetchThenCancel(cancel)),
	)

	err := subscriber.SubscribeEvents(ctx, 100, func(*domain.BlockchainEvent) error { return nil })
	require.ErrorIs(t, err, context.Canceled)
}

// TestSubscribeEvents_ReorgAboveBoundaryAfterCatchupIsAbsorbed is the shallow
// counterpart: the bridged walk rejoins the retained chain at the boundary,
// so nothing is reported and emission simply continues.
func TestSubscribeEvents_ReorgAboveBoundaryAfterCatchupIsAbsorbed(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	chain := &headChain{}
	mockClient, push, _ := headFixture(t, ctrl, chain.next(105))
	subscriber := newLaggedSubscriber(t, mockClient, 2)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	a103 := head(103, common.HexToHash("0xa103"), common.HexToHash("0xa102"))
	b104 := head(104, common.HexToHash("0xb104"), a103.Hash) // rejoins at the boundary
	b105 := head(105, common.HexToHash("0xb105"), b104.Hash)
	b106 := chain.orphanTip(106, b105.Hash)
	gomock.InOrder(
		mockClient.EXPECT().HeadByNumber(gomock.Any(), uint64(103)).Return(a103, nil),
		mockClient.EXPECT().FetchIngestionLogs(gomock.Any(), uint64(100), uint64(103)).
			DoAndReturn(fetchThenPush(push, []*adapter.BlockHead{b106})),
		mockClient.EXPECT().HeadByNumber(gomock.Any(), uint64(105)).Return(b105, nil),
		mockClient.EXPECT().HeadByNumber(gomock.Any(), uint64(104)).Return(b104, nil),
		// 103 retained == b104's parent: rejoin, no fetch of 103.
		mockClient.EXPECT().FetchIngestionLogs(gomock.Any(), uint64(104), uint64(104)).DoAndReturn(fetchThenCancel(cancel)),
	)

	err := subscriber.SubscribeEvents(ctx, 100, func(*domain.BlockchainEvent) error { return nil })
	require.ErrorIs(t, err, context.Canceled)
}

// TestSubscribeEvents_QueuedDoubleReorgRejectsStaleHead pins the three-branch
// case: retained A102, incoming B103 (parent B102), while the node holds a
// third branch C102. B103's ancestry is not canonical, so it is discarded and
// neither the tip nor scanned progress advances; the retained chain is
// refreshed to C102, and the canonical C103 later confirms 101 without any
// further reconciliation fetch.
func TestSubscribeEvents_QueuedDoubleReorgRejectsStaleHead(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	chain := &headChain{}
	a100, a101, a102 := chain.next(100), chain.next(101), chain.next(102)
	_ = a100
	mockClient, push, _ := headFixture(t, ctrl, a100, a101, a102)
	subscriber := newLaggedSubscriber(t, mockClient, 2)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	b103 := head(103, common.HexToHash("0xb103"), common.HexToHash("0xb102"))
	c102 := head(102, common.HexToHash("0xc102"), a101.Hash)
	c103 := head(103, common.HexToHash("0xc103"), c102.Hash)
	var reported []uint64
	subscriber.(blockchain.ProgressReporter).SetProgressHandler(func(through uint64) error {
		reported = append(reported, through)
		return nil
	})
	gomock.InOrder(
		mockClient.EXPECT().FetchIngestionLogs(gomock.Any(), uint64(100), uint64(100)).
			DoAndReturn(fetchThenPush(push, []*adapter.BlockHead{b103})),
		// B103's parent matches neither retained A102 nor canonical C102: stale.
		mockClient.EXPECT().HeadByNumber(gomock.Any(), uint64(102)).DoAndReturn(
			func(context.Context, uint64) (*adapter.BlockHead, error) {
				push(c103)
				return c102, nil
			}),
		// Only the canonical C103 (parent == refreshed C102) confirms 101.
		mockClient.EXPECT().FetchIngestionLogs(gomock.Any(), uint64(101), uint64(101)).DoAndReturn(fetchThenCancel(cancel)),
	)

	err := subscriber.SubscribeEvents(ctx, 100, func(*domain.BlockchainEvent) error { return nil })
	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, []uint64{100, 101}, reported, "no progress between the stale head and the canonical confirmation")
}

// TestSubscribeEvents_ReplacementBelowTipResetsConfirmationDepth pins that
// accepting a replacement below the tip drops the stale descendants and
// measures the lag from the replacement branch: queued A103, A104, B101, B102,
// B103 (lag 2) must confirm only 101 — from B103 — never 101..102 from the
// stale A104.
func TestSubscribeEvents_ReplacementBelowTipResetsConfirmationDepth(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	chain := &headChain{}
	a100, a101, a102 := chain.next(100), chain.next(101), chain.next(102)
	a103, a104 := chain.next(103), chain.next(104)
	mockClient, push, _ := headFixture(t, ctrl, a100, a101, a102)
	subscriber := newLaggedSubscriber(t, mockClient, 2)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	b101 := head(101, common.HexToHash("0xb101"), a100.Hash) // replaces A101 on the emitted 100
	b102 := head(102, common.HexToHash("0xb102"), b101.Hash)
	b103 := head(103, common.HexToHash("0xb103"), b102.Hash)
	gomock.InOrder(
		mockClient.EXPECT().FetchIngestionLogs(gomock.Any(), uint64(100), uint64(100)).
			DoAndReturn(fetchThenPush(push, []*adapter.BlockHead{a103, a104, b101, b102, b103})),
		// With A104 still counted as tip this would be [101,102]; the
		// replacement branch's tip B103 confirms 101 only.
		mockClient.EXPECT().FetchIngestionLogs(gomock.Any(), uint64(101), uint64(101)).DoAndReturn(fetchThenCancel(cancel)),
	)

	err := subscriber.SubscribeEvents(ctx, 100, func(*domain.BlockchainEvent) error { return nil })
	require.ErrorIs(t, err, context.Canceled)
}

// TestSubscribeEvents_TipOnlyDeepReorgStopsAtLastEmitted pins the bound of the
// walk: it reaches the last emitted height (100), finds it replaced (the deep
// reorg signal), and does not walk further or replay anything.
func TestSubscribeEvents_TipOnlyDeepReorgStopsAtLastEmitted(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	chain := &headChain{}
	mockClient, push, _ := headFixture(t, ctrl, chain.next(100), chain.next(101), chain.next(102))
	subscriber := newLaggedSubscriber(t, mockClient, 2)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	b100 := head(100, common.HexToHash("0xb100"), common.HexToHash("0x99"))
	b101 := head(101, common.HexToHash("0xb101"), b100.Hash)
	b102 := head(102, common.HexToHash("0xb102"), b101.Hash)
	b103 := chain.orphanTip(103, b102.Hash)
	gomock.InOrder(
		mockClient.EXPECT().FetchIngestionLogs(gomock.Any(), uint64(100), uint64(100)).
			DoAndReturn(fetchThenPush(push, []*adapter.BlockHead{b103})),
		mockClient.EXPECT().HeadByNumber(gomock.Any(), uint64(102)).Return(b102, nil),
		mockClient.EXPECT().HeadByNumber(gomock.Any(), uint64(101)).Return(b101, nil),
		mockClient.EXPECT().HeadByNumber(gomock.Any(), uint64(100)).Return(b100, nil), // emitted 100 replaced: reported
		// no HeadByNumber(99); emission continues from 101 by number.
		mockClient.EXPECT().FetchIngestionLogs(gomock.Any(), uint64(101), uint64(101)).DoAndReturn(fetchThenCancel(cancel)),
	)

	err := subscriber.SubscribeEvents(ctx, 100, func(*domain.BlockchainEvent) error { return nil })
	require.ErrorIs(t, err, context.Canceled)
}

// TestSubscribeEvents_ReportsEveryBatchAndStopsOnLateFailure pins per-batch
// durability: each batch is reported (and, via the runner, persisted) before
// the next is fetched, so a failure in a later batch leaves the earlier ones
// reported and returns the fetch error.
func TestSubscribeEvents_ReportsEveryBatchAndStopsOnLateFailure(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	chain := &headChain{}
	mockClient, _, _ := headFixture(t, ctrl, chain.next(125))
	subscriber := newTestSubscriber(t, mockClient, 0)

	fetchErr := errors.New("provider 503")
	gomock.InOrder(
		mockClient.EXPECT().FetchIngestionLogs(gomock.Any(), uint64(100), uint64(109)).Return(nil, nil),
		mockClient.EXPECT().FetchIngestionLogs(gomock.Any(), uint64(110), uint64(119)).Return(nil, nil),
		mockClient.EXPECT().FetchIngestionLogs(gomock.Any(), uint64(120), uint64(125)).Return(nil, fetchErr),
	)

	var reported []uint64
	subscriber.(blockchain.ProgressReporter).SetProgressHandler(func(through uint64) error {
		reported = append(reported, through)
		return nil
	})

	err := subscriber.SubscribeEvents(context.Background(), 100, func(*domain.BlockchainEvent) error { return nil })
	require.ErrorIs(t, err, fetchErr)
	require.Equal(t, []uint64{109, 119}, reported, "batches before the failure were reported; the failed one was not")
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

// TestSubscribeEvents_CatchupBoundCoversPendingWindow pins that the bound is
// measured on the whole gap to the tip, not just the confirmed range: with
// lag 2 and max 10, a tip 12 blocks past the start is rejected even though
// only 10 would be emitted now, while a tip 10 past the start is accepted.
func TestSubscribeEvents_CatchupBoundCoversPendingWindow(t *testing.T) {
	t.Parallel()

	t.Run("gap of max+lag is rejected", func(t *testing.T) {
		t.Parallel()
		ctrl := gomock.NewController(t)
		chain := &headChain{}
		mockClient, _, _ := headFixture(t, ctrl, chain.next(111))
		sub, err := ethprovider.NewSubscriber(ethprovider.Config{ChainID: domain.ChainEthereumMainnet, MaxCatchupBlocks: 10, ConfirmationBlocks: 2}, mockClient)
		require.NoError(t, err)

		err = sub.SubscribeEvents(context.Background(), 100, func(*domain.BlockchainEvent) error { return nil })
		require.ErrorIs(t, err, ethprovider.ErrCatchupTooLarge)
		require.Contains(t, err.Error(), "need blocks 100-111 (12 blocks, max 10)")
	})

	t.Run("gap of exactly max is accepted", func(t *testing.T) {
		t.Parallel()
		ctrl := gomock.NewController(t)
		chain := &headChain{}
		mockClient, _, _ := headFixture(t, ctrl, chain.next(109))
		sub, err := ethprovider.NewSubscriber(ethprovider.Config{ChainID: domain.ChainEthereumMainnet, MaxCatchupBlocks: 10, ConfirmationBlocks: 2}, mockClient)
		require.NoError(t, err)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		boundary := head(107, common.HexToHash("0x107"), common.HexToHash("0x106"))
		gomock.InOrder(
			mockClient.EXPECT().HeadByNumber(gomock.Any(), uint64(107)).Return(boundary, nil),
			mockClient.EXPECT().FetchIngestionLogs(gomock.Any(), uint64(100), uint64(107)).DoAndReturn(fetchThenCancel(cancel)),
		)
		err = sub.SubscribeEvents(ctx, 100, func(*domain.BlockchainEvent) error { return nil })
		require.ErrorIs(t, err, context.Canceled)
	})
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
	mockClient.EXPECT().FetchIngestionLogs(gomock.Any(), uint64(1), uint64(10)).DoAndReturn(fetchThenCancel(cancel))

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
