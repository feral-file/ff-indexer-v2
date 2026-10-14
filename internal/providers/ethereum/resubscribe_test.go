package ethereum_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/rpc"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/feral-file/ff-indexer-v2/internal/adapter"
	"github.com/feral-file/ff-indexer-v2/internal/domain"
	"github.com/feral-file/ff-indexer-v2/internal/mocks"
	ethprovider "github.com/feral-file/ff-indexer-v2/internal/providers/ethereum"
)

// Tests for the newHeads subscription lifecycle in resubscribe.go: how a drop
// is recognized, how the subscription is re-established, and what survives it.

// fastClock returns a clock whose After fires at once and records every delay
// requested, so resubscribe tests pin the backoff schedule without sleeping.
// Delays are appended on the subscriber's goroutine, which is the test's own
// (SubscribeEvents is called synchronously), so the slice needs no locking.
func fastClock(ctrl *gomock.Controller) (*mocks.MockClock, *[]time.Duration) {
	clock := mocks.NewMockClock(ctrl)
	delays := &[]time.Duration{}
	clock.EXPECT().After(gomock.Any()).DoAndReturn(func(d time.Duration) <-chan time.Time {
		*delays = append(*delays, d)
		fired := make(chan time.Time, 1)
		fired <- time.Time{}
		return fired
	}).AnyTimes()
	return clock, delays
}

// subscribeAttempt is one scripted SubscribeNewHead outcome: a failure, or a
// fresh subscription with these heads queued at subscribe time.
type subscribeAttempt struct {
	err   error
	heads []*adapter.BlockHead
}

// subscribeSequence scripts consecutive SubscribeNewHead calls in order and
// returns the error channel of each subscription handed out (nil for failed
// attempts), so a test can drop a specific subscription with a transport error.
func subscribeSequence(mockClient *mocks.MockEthereumProviderClient, attempts ...subscribeAttempt) []chan error {
	errChs := make([]chan error, len(attempts))
	calls := make([]any, len(attempts))
	for i, attempt := range attempts {
		if attempt.err != nil {
			calls[i] = mockClient.EXPECT().SubscribeNewHead(gomock.Any(), gomock.Any()).Return(nil, attempt.err)
			continue
		}
		errCh := make(chan error, 1)
		errChs[i] = errCh
		heads := attempt.heads
		calls[i] = mockClient.EXPECT().
			SubscribeNewHead(gomock.Any(), gomock.Any()).
			DoAndReturn(func(_ context.Context, ch chan<- *adapter.BlockHead) (ethereum.Subscription, error) {
				for _, h := range heads {
					ch <- h
				}
				return &mockSubscription{errCh: errCh}, nil
			})
	}
	gomock.InOrder(calls...)
	return errChs
}

// fetchThenDrop returns a FetchIngestionLogs callback that ends the current
// subscription with a transport error after returning, so the drop is observed
// right after the fetched range was emitted.
func fetchThenDrop(errCh chan error, dropErr error) func(context.Context, uint64, uint64) ([]types.Log, error) {
	return func(context.Context, uint64, uint64) ([]types.Log, error) {
		errCh <- dropErr
		return nil, nil
	}
}

// TestSubscribeEvents_ResubscribesAfterDrop pins the fix for issue #82: a
// newHeads subscription that dies mid-stream (peer reset, provider close) is
// re-established in place instead of ending ingestion — which, in the unified
// process, exited the binary and took the API down for every restart. The
// position survives the drop: the first head on the new subscription fills
// the gap from the last emitted block in one eth_getLogs walk. The retained
// chain does not survive it — as after a restart, the reconciliation walk
// stops at the first unretained height instead of bridging the whole gap head
// by head (no HeadByNumber calls here; with a confirmation lag planRange
// would fetch the emitted boundary once).
func TestSubscribeEvents_ResubscribesAfterDrop(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	chain := &headChain{}
	mockClient := mocks.NewMockEthereumProviderClient(ctrl)
	h100 := chain.next(100)
	chain.next(101)
	chain.next(102)
	h103 := chain.next(103)
	errChs := subscribeSequence(mockClient,
		subscribeAttempt{heads: []*adapter.BlockHead{h100}},
		subscribeAttempt{heads: []*adapter.BlockHead{h103}},
	)
	clock, delays := fastClock(ctrl)
	subscriber, err := ethprovider.NewSubscriber(ethprovider.Config{ChainID: domain.ChainEthereumMainnet}, mockClient, clock)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	transportErr := errors.New("read tcp: connection reset by peer")
	gomock.InOrder(
		mockClient.EXPECT().FetchIngestionLogs(gomock.Any(), uint64(100), uint64(100)).DoAndReturn(fetchThenDrop(errChs[0], transportErr)),
		mockClient.EXPECT().FetchIngestionLogs(gomock.Any(), uint64(101), uint64(103)).DoAndReturn(fetchThenCancel(cancel)),
	)

	err = subscriber.SubscribeEvents(ctx, 100, func(*domain.BlockchainEvent) error { return nil })
	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, []time.Duration{time.Second}, *delays)
}

// TestSubscribeEvents_ResubscribeBacksOffAndResets pins the retry schedule:
// a failed resubscribe doubles the wait (1s, 2s, 4s …) and never gives up —
// the subscription worked before, so this is an outage, and exiting would only
// move the retry into Docker's restart loop — while a subscription that goes
// on to deliver heads resets the schedule for the next drop.
func TestSubscribeEvents_ResubscribeBacksOffAndResets(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	chain := &headChain{}
	mockClient := mocks.NewMockEthereumProviderClient(ctrl)
	dialErr := errors.New("dial tcp: connection refused")
	errChs := subscribeSequence(mockClient,
		subscribeAttempt{heads: []*adapter.BlockHead{chain.next(100)}},
		subscribeAttempt{err: dialErr},
		subscribeAttempt{err: dialErr},
		subscribeAttempt{heads: []*adapter.BlockHead{chain.next(101)}},
		subscribeAttempt{heads: []*adapter.BlockHead{chain.next(102)}},
	)
	clock, delays := fastClock(ctrl)
	subscriber, err := ethprovider.NewSubscriber(ethprovider.Config{ChainID: domain.ChainEthereumMainnet}, mockClient, clock)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	dropErr := errors.New("websocket closed 1006")
	gomock.InOrder(
		mockClient.EXPECT().FetchIngestionLogs(gomock.Any(), uint64(100), uint64(100)).DoAndReturn(fetchThenDrop(errChs[0], dropErr)),
		mockClient.EXPECT().FetchIngestionLogs(gomock.Any(), uint64(101), uint64(101)).DoAndReturn(fetchThenDrop(errChs[3], dropErr)),
		mockClient.EXPECT().FetchIngestionLogs(gomock.Any(), uint64(102), uint64(102)).DoAndReturn(fetchThenCancel(cancel)),
	)

	err = subscriber.SubscribeEvents(ctx, 100, func(*domain.BlockchainEvent) error { return nil })
	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, []time.Duration{time.Second, 2 * time.Second, 4 * time.Second, time.Second}, *delays)
}

// TestSubscribeEvents_ResubscribeKeepsBackingOffUntilHeadsFlow pins that a
// provider accepting the subscription and dropping it before any head arrives
// does not reset the schedule — only an emitted range proves the connection is
// healthy — and that the wait is capped at 30s.
func TestSubscribeEvents_ResubscribeKeepsBackingOffUntilHeadsFlow(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	chain := &headChain{}
	mockClient := mocks.NewMockEthereumProviderClient(ctrl)
	attempts := []subscribeAttempt{{heads: []*adapter.BlockHead{chain.next(100)}}}
	for range 6 {
		attempts = append(attempts, subscribeAttempt{}) // accepted, dropped before any head
	}
	attempts = append(attempts, subscribeAttempt{heads: []*adapter.BlockHead{chain.next(101)}})
	errChs := subscribeSequence(mockClient, attempts...)
	dropErr := errors.New("websocket closed 1006")
	for _, errCh := range errChs[1:7] {
		errCh <- dropErr
	}
	clock, delays := fastClock(ctrl)
	subscriber, err := ethprovider.NewSubscriber(ethprovider.Config{ChainID: domain.ChainEthereumMainnet}, mockClient, clock)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	gomock.InOrder(
		mockClient.EXPECT().FetchIngestionLogs(gomock.Any(), uint64(100), uint64(100)).DoAndReturn(fetchThenDrop(errChs[0], dropErr)),
		mockClient.EXPECT().FetchIngestionLogs(gomock.Any(), uint64(101), uint64(101)).DoAndReturn(fetchThenCancel(cancel)),
	)

	err = subscriber.SubscribeEvents(ctx, 100, func(*domain.BlockchainEvent) error { return nil })
	require.ErrorIs(t, err, context.Canceled)
	second := time.Second
	require.Equal(t, []time.Duration{second, 2 * second, 4 * second, 8 * second, 16 * second, 30 * second, 30 * second}, *delays)
}

// TestSubscribeEvents_ResubscribeStopsOnCancel pins that the backoff wait is
// cancellable: shutdown during an outage returns promptly with ctx.Err()
// instead of holding the process until the next attempt.
func TestSubscribeEvents_ResubscribeStopsOnCancel(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	chain := &headChain{}
	mockClient := mocks.NewMockEthereumProviderClient(ctrl)
	errChs := subscribeSequence(mockClient, subscribeAttempt{heads: []*adapter.BlockHead{chain.next(100)}})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	clock := mocks.NewMockClock(ctrl)
	clock.EXPECT().After(time.Second).DoAndReturn(func(time.Duration) <-chan time.Time {
		cancel()
		return make(chan time.Time) // never fires
	})
	subscriber, err := ethprovider.NewSubscriber(ethprovider.Config{ChainID: domain.ChainEthereumMainnet}, mockClient, clock)
	require.NoError(t, err)

	mockClient.EXPECT().FetchIngestionLogs(gomock.Any(), uint64(100), uint64(100)).
		DoAndReturn(fetchThenDrop(errChs[0], errors.New("websocket closed 1006")))

	err = subscriber.SubscribeEvents(ctx, 100, func(*domain.BlockchainEvent) error { return nil })
	require.ErrorIs(t, err, context.Canceled)
}

// TestSubscribeEvents_LaggedResubscribeFetchesBoundaryOnce is the regression
// guard for forgetHeads: with a confirmation lag, the first head after a drop
// makes planRange fetch the emitted boundary exactly once, and reconcile must
// not bridge the outage gap head by head (any HeadByNumber for 102 would fail
// the test as an unexpected call).
func TestSubscribeEvents_LaggedResubscribeFetchesBoundaryOnce(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	chain := &headChain{}
	mockClient := mocks.NewMockEthereumProviderClient(ctrl)
	initial := []*adapter.BlockHead{chain.next(100), chain.next(101), chain.next(102)}
	h103 := chain.next(103)
	errChs := subscribeSequence(mockClient,
		subscribeAttempt{heads: initial},
		subscribeAttempt{heads: []*adapter.BlockHead{h103}},
	)
	clock, _ := fastClock(ctrl)
	subscriber, err := ethprovider.NewSubscriber(ethprovider.Config{ChainID: domain.ChainEthereumMainnet, ConfirmationBlocks: 2}, mockClient, clock)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	gomock.InOrder(
		mockClient.EXPECT().FetchIngestionLogs(gomock.Any(), uint64(100), uint64(100)).DoAndReturn(fetchThenDrop(errChs[0], errors.New("websocket closed 1006"))),
		mockClient.EXPECT().HeadByNumber(gomock.Any(), uint64(101)).Return(chain.byHeight[101], nil), // emitted boundary, once
		mockClient.EXPECT().FetchIngestionLogs(gomock.Any(), uint64(101), uint64(101)).DoAndReturn(fetchThenCancel(cancel)),
	)

	err = subscriber.SubscribeEvents(ctx, 100, func(*domain.BlockchainEvent) error { return nil })
	require.ErrorIs(t, err, context.Canceled)
}

// TestSubscribeEvents_ResubscribeStopsOnCancelDuringAttempt pins that a
// shutdown landing inside a resubscribe attempt (the adapter returns the
// cancellation as its error) ends the loop at once — no further wait is
// scheduled, so nothing is logged as a failed attempt.
func TestSubscribeEvents_ResubscribeStopsOnCancelDuringAttempt(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	chain := &headChain{}
	mockClient := mocks.NewMockEthereumProviderClient(ctrl)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	first := make(chan error, 1)
	gomock.InOrder(
		mockClient.EXPECT().SubscribeNewHead(gomock.Any(), gomock.Any()).
			DoAndReturn(func(_ context.Context, ch chan<- *adapter.BlockHead) (ethereum.Subscription, error) {
				ch <- chain.next(100)
				return &mockSubscription{errCh: first}, nil
			}),
		mockClient.EXPECT().SubscribeNewHead(gomock.Any(), gomock.Any()).
			DoAndReturn(func(context.Context, chan<- *adapter.BlockHead) (ethereum.Subscription, error) {
				cancel()
				return nil, fmt.Errorf("ethereum operation SubscribeNewHead failed after retries: %w", context.Canceled)
			}),
	)
	clock, delays := fastClock(ctrl)
	subscriber, err := ethprovider.NewSubscriber(ethprovider.Config{ChainID: domain.ChainEthereumMainnet}, mockClient, clock)
	require.NoError(t, err)

	mockClient.EXPECT().FetchIngestionLogs(gomock.Any(), uint64(100), uint64(100)).
		DoAndReturn(fetchThenDrop(first, errors.New("websocket closed 1006")))

	err = subscriber.SubscribeEvents(ctx, 100, func(*domain.BlockchainEvent) error { return nil })
	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, []time.Duration{time.Second}, *delays)
}

// TestSubscribeEvents_DropSurfacedByInFlightFetchResubscribes pins
// ingestFailure: newHeads and eth_getLogs share one connection, so when the
// socket dies mid-fetch the fetch error arrives first and the subscription's
// own report a moment later. The drop is still recovered — whatever the error
// text — and the un-advanced position re-fetches the failed block with the
// gap. The grace timer never fires here, so the subscription's report must be
// what decides.
func TestSubscribeEvents_DropSurfacedByInFlightFetchResubscribes(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	chain := &headChain{}
	mockClient := mocks.NewMockEthereumProviderClient(ctrl)
	errChs := subscribeSequence(mockClient,
		subscribeAttempt{heads: []*adapter.BlockHead{chain.next(100)}},
		subscribeAttempt{heads: []*adapter.BlockHead{chain.next(101)}},
	)
	clock := mocks.NewMockClock(ctrl)
	clock.EXPECT().After(2 * time.Second).Return(make(chan time.Time)) // grace: never elapses
	clock.EXPECT().After(time.Second).DoAndReturn(func(time.Duration) <-chan time.Time {
		fired := make(chan time.Time, 1)
		fired <- time.Time{}
		return fired
	})
	subscriber, err := ethprovider.NewSubscriber(ethprovider.Config{ChainID: domain.ChainEthereumMainnet}, mockClient, clock)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	closeErr := errors.New("websocket: close 1001 (going away)")
	gomock.InOrder(
		mockClient.EXPECT().FetchIngestionLogs(gomock.Any(), uint64(100), uint64(100)).
			DoAndReturn(func(context.Context, uint64, uint64) ([]types.Log, error) {
				errChs[0] <- closeErr // the subscription learns of the same close
				return nil, fmt.Errorf("ethereum operation FilterLogs failed after retries: permanent error: %w", closeErr)
			}),
		mockClient.EXPECT().FetchIngestionLogs(gomock.Any(), uint64(100), uint64(101)).DoAndReturn(fetchThenCancel(cancel)),
	)

	err = subscriber.SubscribeEvents(ctx, 100, func(*domain.BlockchainEvent) error { return nil })
	require.ErrorIs(t, err, context.Canceled)
}

// TestSubscribeEvents_DropMidCatchupResumesAfterLastBatch pins that the
// position advances per batch, in step with the durable cursor: the socket
// dying under the second batch of a catch-up resumes at that batch, not at
// the start of the range (which would re-scan blocks the runner already
// accepted and measure the catch-up bound from a position the cursor has long
// passed). It also pins that a subscription which emitted batches before dying
// mid-fetch counts as healthy: the second drop is retried after 1 s, not 2 s.
func TestSubscribeEvents_DropMidCatchupResumesAfterLastBatch(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	chain := &headChain{}
	mockClient := mocks.NewMockEthereumProviderClient(ctrl)
	errChs := subscribeSequence(mockClient,
		subscribeAttempt{heads: []*adapter.BlockHead{chain.next(125)}},
		subscribeAttempt{heads: []*adapter.BlockHead{chain.next(126)}},
		subscribeAttempt{heads: []*adapter.BlockHead{chain.next(127)}},
	)
	clock := mocks.NewMockClock(ctrl)
	clock.EXPECT().After(2 * time.Second).Return(make(chan time.Time)).Times(2) // grace: never elapses
	clock.EXPECT().After(time.Second).DoAndReturn(func(time.Duration) <-chan time.Time {
		fired := make(chan time.Time, 1)
		fired <- time.Time{}
		return fired
	}).Times(2) // both drops followed emitted batches, so both waits are the minimum
	subscriber, err := ethprovider.NewSubscriber(ethprovider.Config{ChainID: domain.ChainEthereumMainnet}, mockClient, clock)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	closeErr := errors.New("websocket: close 1006 (abnormal closure)")
	dieDuringFetch := func(errCh chan error) func(context.Context, uint64, uint64) ([]types.Log, error) {
		return func(context.Context, uint64, uint64) ([]types.Log, error) {
			errCh <- closeErr
			return nil, closeErr
		}
	}
	gomock.InOrder(
		mockClient.EXPECT().FetchIngestionLogs(gomock.Any(), uint64(100), uint64(109)).Return(nil, nil),
		mockClient.EXPECT().FetchIngestionLogs(gomock.Any(), uint64(110), uint64(119)).DoAndReturn(dieDuringFetch(errChs[0])),
		// Resumed at the failed batch, with the new head folded into the range.
		mockClient.EXPECT().FetchIngestionLogs(gomock.Any(), uint64(110), uint64(119)).Return(nil, nil),
		mockClient.EXPECT().FetchIngestionLogs(gomock.Any(), uint64(120), uint64(126)).DoAndReturn(dieDuringFetch(errChs[1])),
		mockClient.EXPECT().FetchIngestionLogs(gomock.Any(), uint64(120), uint64(127)).DoAndReturn(fetchThenCancel(cancel)),
	)

	err = subscriber.SubscribeEvents(ctx, 100, func(*domain.BlockchainEvent) error { return nil })
	require.ErrorIs(t, err, context.Canceled)
}

// TestSubscribeEvents_ResubscribeAgainstClosedClientIsTerminal pins the
// counterpart of ClosedClientIsTerminal on the resubscribe path: when the rpc
// client was closed while the loop waited, every attempt would fail with
// rpc.ErrClientQuit, so the first such failure ends the stream instead of
// re-arming the schedule forever.
func TestSubscribeEvents_ResubscribeAgainstClosedClientIsTerminal(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	chain := &headChain{}
	mockClient := mocks.NewMockEthereumProviderClient(ctrl)
	errChs := subscribeSequence(mockClient,
		subscribeAttempt{heads: []*adapter.BlockHead{chain.next(100)}},
		subscribeAttempt{err: fmt.Errorf("ethereum operation SubscribeNewHead failed after retries: permanent error: %w", rpc.ErrClientQuit)},
	)
	clock, delays := fastClock(ctrl)
	subscriber, err := ethprovider.NewSubscriber(ethprovider.Config{ChainID: domain.ChainEthereumMainnet}, mockClient, clock)
	require.NoError(t, err)

	mockClient.EXPECT().FetchIngestionLogs(gomock.Any(), uint64(100), uint64(100)).
		DoAndReturn(fetchThenDrop(errChs[0], errors.New("websocket closed 1006")))

	err = subscriber.SubscribeEvents(context.Background(), 100, func(*domain.BlockchainEvent) error { return nil })
	require.ErrorIs(t, err, rpc.ErrClientQuit)
	require.NotErrorIs(t, err, context.Canceled)
	require.Equal(t, []time.Duration{time.Second}, *delays)
}

// TestSubscribeEvents_ClosedClientIsTerminal pins the one subscription ending
// that must NOT be retried: go-ethereum reports Client.Close as a nil error on
// Err(), and only Unsubscribe closes the channel. Both mean the client itself
// is gone, so resubscribing would spin against it forever.
func TestSubscribeEvents_ClosedClientIsTerminal(t *testing.T) {
	t.Parallel()

	end := map[string]func(*mockSubscription){
		"nil error (Client.Close)": func(s *mockSubscription) { s.errCh <- nil },
		"channel closed":           func(s *mockSubscription) { s.Unsubscribe() },
	}
	for name, endSubscription := range end {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			ctrl := gomock.NewController(t)
			mockClient := mocks.NewMockEthereumProviderClient(ctrl)
			sub := &mockSubscription{errCh: make(chan error, 1)}
			mockClient.EXPECT().SubscribeNewHead(gomock.Any(), gomock.Any()).Return(sub, nil) // exactly once
			endSubscription(sub)
			clock, delays := fastClock(ctrl)
			subscriber, err := ethprovider.NewSubscriber(ethprovider.Config{ChainID: domain.ChainEthereumMainnet}, mockClient, clock)
			require.NoError(t, err)

			err = subscriber.SubscribeEvents(context.Background(), 100, func(*domain.BlockchainEvent) error { return nil })
			require.Error(t, err)
			require.NotErrorIs(t, err, context.Canceled)
			require.Contains(t, err.Error(), "client closed")
			require.Empty(t, *delays)
		})
	}
}

// TestSubscribeEvents_ReplayedTipThenDropDoesNotResetBackoff pins two things
// about a provider that re-announces an already-emitted height on the new
// subscription (a lagging backend, or a tip replay on subscribe): the head is
// ignored rather than re-fetched or reported as a deep reorg — with nothing
// retained to compare against it is logged at info — and, because nothing was
// emitted, it does not count as proof of health: when that subscription then
// drops, the schedule keeps growing instead of snapping back to 1 s.
func TestSubscribeEvents_ReplayedTipThenDropDoesNotResetBackoff(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	chain := &headChain{}
	mockClient := mocks.NewMockEthereumProviderClient(ctrl)
	h100, h101 := chain.next(100), chain.next(101)
	errChs := subscribeSequence(mockClient,
		subscribeAttempt{heads: []*adapter.BlockHead{h100}},
		subscribeAttempt{heads: []*adapter.BlockHead{h100}}, // replays the emitted tip, then drops
		subscribeAttempt{heads: []*adapter.BlockHead{h101}},
	)
	dropErr := errors.New("websocket closed 1006")
	errChs[1] <- dropErr
	clock, delays := fastClock(ctrl)
	subscriber, err := ethprovider.NewSubscriber(ethprovider.Config{ChainID: domain.ChainEthereumMainnet}, mockClient, clock)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	gomock.InOrder(
		mockClient.EXPECT().FetchIngestionLogs(gomock.Any(), uint64(100), uint64(100)).DoAndReturn(fetchThenDrop(errChs[0], dropErr)),
		mockClient.EXPECT().FetchIngestionLogs(gomock.Any(), uint64(101), uint64(101)).DoAndReturn(fetchThenCancel(cancel)),
	)

	err = subscriber.SubscribeEvents(ctx, 100, func(*domain.BlockchainEvent) error { return nil })
	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, []time.Duration{time.Second, 2 * time.Second}, *delays)
}

// TestSubscribeEvents_BoundaryNotFoundAfterDropWaitsForNextHead pins that the
// emitted-boundary fetch planRange makes after a reconnect tolerates a lagging
// backend: a NotFound for the boundary defers emission to the next head
// instead of ending the stream, since this fetch now runs after every drop.
func TestSubscribeEvents_BoundaryNotFoundAfterDropWaitsForNextHead(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	chain := &headChain{}
	mockClient := mocks.NewMockEthereumProviderClient(ctrl)
	initial := []*adapter.BlockHead{chain.next(100), chain.next(101), chain.next(102)}
	h103, h104 := chain.next(103), chain.next(104)
	first := make(chan error, 1)
	var pushLater func(*adapter.BlockHead)
	gomock.InOrder(
		mockClient.EXPECT().SubscribeNewHead(gomock.Any(), gomock.Any()).
			DoAndReturn(func(_ context.Context, ch chan<- *adapter.BlockHead) (ethereum.Subscription, error) {
				for _, h := range initial {
					ch <- h
				}
				return &mockSubscription{errCh: first}, nil
			}),
		mockClient.EXPECT().SubscribeNewHead(gomock.Any(), gomock.Any()).
			DoAndReturn(func(_ context.Context, ch chan<- *adapter.BlockHead) (ethereum.Subscription, error) {
				pushLater = func(h *adapter.BlockHead) { ch <- h }
				ch <- h103
				return &mockSubscription{errCh: make(chan error, 1)}, nil
			}),
	)
	clock, _ := fastClock(ctrl)
	subscriber, err := ethprovider.NewSubscriber(ethprovider.Config{ChainID: domain.ChainEthereumMainnet, ConfirmationBlocks: 2}, mockClient, clock)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	gomock.InOrder(
		mockClient.EXPECT().FetchIngestionLogs(gomock.Any(), uint64(100), uint64(100)).DoAndReturn(fetchThenDrop(first, errors.New("websocket closed 1006"))),
		// Head 103 confirms through 101; the lagging backend has not got 101 yet.
		mockClient.EXPECT().HeadByNumber(gomock.Any(), uint64(101)).
			DoAndReturn(func(context.Context, uint64) (*adapter.BlockHead, error) {
				pushLater(h104)
				return nil, fmt.Errorf("block 101: %w", ethereum.NotFound)
			}),
		// Head 104 confirms through 102; the boundary is fetched there instead.
		mockClient.EXPECT().HeadByNumber(gomock.Any(), uint64(102)).Return(chain.byHeight[102], nil),
		mockClient.EXPECT().FetchIngestionLogs(gomock.Any(), uint64(101), uint64(102)).DoAndReturn(fetchThenCancel(cancel)),
	)

	err = subscriber.SubscribeEvents(ctx, 100, func(*domain.BlockchainEvent) error { return nil })
	require.ErrorIs(t, err, context.Canceled)
}

// TestNewSubscriber_RequiresClock pins that a missing clock is rejected at
// construction: it is first used on the first drop, so accepting nil would
// surface as a panic hours into a run.
func TestNewSubscriber_RequiresClock(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	_, err := ethprovider.NewSubscriber(ethprovider.Config{ChainID: domain.ChainEthereumMainnet}, mocks.NewMockEthereumProviderClient(ctrl), nil)
	require.ErrorContains(t, err, "clock is required")
}

// TestSubscribeEvents_ReconcileNotFoundDefersToNextHead pins the counterpart of
// the boundary tolerance for the reconciliation walk: when a head's parent is
// not retained and the bridging fetch gets NotFound from a lagging backend, the
// head is set aside (not retained, nothing emitted) and the next head retries
// the walk — instead of the walk failing and ending the stream.
func TestSubscribeEvents_ReconcileNotFoundDefersToNextHead(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	chain := &headChain{}
	mockClient := mocks.NewMockEthereumProviderClient(ctrl)
	h100, h101, h102, h103, h104 := chain.next(100), chain.next(101), chain.next(102), chain.next(103), chain.next(104)
	first := make(chan error, 1)
	var push func(*adapter.BlockHead)
	gomock.InOrder(
		mockClient.EXPECT().SubscribeNewHead(gomock.Any(), gomock.Any()).
			DoAndReturn(func(_ context.Context, ch chan<- *adapter.BlockHead) (ethereum.Subscription, error) {
				ch <- h100
				return &mockSubscription{errCh: first}, nil
			}),
		mockClient.EXPECT().SubscribeNewHead(gomock.Any(), gomock.Any()).
			DoAndReturn(func(_ context.Context, ch chan<- *adapter.BlockHead) (ethereum.Subscription, error) {
				push = func(h *adapter.BlockHead) { ch <- h }
				ch <- h101
				return &mockSubscription{errCh: make(chan error, 1)}, nil
			}),
	)
	clock, _ := fastClock(ctrl)
	subscriber, err := ethprovider.NewSubscriber(ethprovider.Config{ChainID: domain.ChainEthereumMainnet}, mockClient, clock)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	gomock.InOrder(
		mockClient.EXPECT().FetchIngestionLogs(gomock.Any(), uint64(100), uint64(100)).DoAndReturn(fetchThenDrop(first, errors.New("websocket closed 1006"))),
		// 101 is emitted and retained; the provider then skips 102 and announces 103.
		mockClient.EXPECT().FetchIngestionLogs(gomock.Any(), uint64(101), uint64(101)).
			DoAndReturn(func(context.Context, uint64, uint64) ([]types.Log, error) {
				push(h103) // read push at call time: it is set by the second subscribe
				return nil, nil
			}),
		// Bridging 102: the lagging backend has not got it yet -> 103 is set aside.
		mockClient.EXPECT().HeadByNumber(gomock.Any(), uint64(102)).
			DoAndReturn(func(context.Context, uint64) (*adapter.BlockHead, error) {
				push(h104)
				return nil, fmt.Errorf("block 102: %w", ethereum.NotFound)
			}),
		// 104 walks down through 103 and 102 (now available) to the retained 101.
		mockClient.EXPECT().HeadByNumber(gomock.Any(), uint64(103)).Return(h103, nil),
		mockClient.EXPECT().HeadByNumber(gomock.Any(), uint64(102)).Return(h102, nil),
		mockClient.EXPECT().FetchIngestionLogs(gomock.Any(), uint64(102), uint64(104)).DoAndReturn(fetchThenCancel(cancel)),
	)

	err = subscriber.SubscribeEvents(ctx, 100, func(*domain.BlockchainEvent) error { return nil })
	require.ErrorIs(t, err, context.Canceled)
}
