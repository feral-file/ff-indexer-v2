package adapter_test

import (
	"context"
	"errors"
	"fmt"
	"math/big"
	"sync"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/feral-file/ff-indexer-v2/internal/adapter"
	"github.com/feral-file/ff-indexer-v2/internal/mocks"
)

// threeTopicProbe is a probe that accepts a served set containing a 3-topic log.
func threeTopicProbe() adapter.LogWarehouseProbe {
	return adapter.LogWarehouseProbe{
		Name:  "three-topic shape",
		Query: ethereum.FilterQuery{FromBlock: big.NewInt(7), ToBlock: big.NewInt(7)},
		Accept: func(logs []types.Log) bool {
			for _, l := range logs {
				if len(l.Topics) == 3 {
					return true
				}
			}
			return false
		},
	}
}

func mainnetReqs(probes ...adapter.LogWarehouseProbe) adapter.LogWarehouseRequirements {
	return adapter.LogWarehouseRequirements{ChainID: 1, Probes: probes}
}

// newVerified builds the wrapper with a real clock and a retry interval short
// enough that a test's later calls fall outside the cooldown.
func newVerified(inner adapter.LogWarehouse, reqs adapter.LogWarehouseRequirements) *adapter.VerifiedLogWarehouse {
	return adapter.NewVerifiedLogWarehouse(inner, reqs, adapter.NewClock(), time.Nanosecond)
}

// TestVerifiedLogWarehouse_RoutesOnlyAfterVerification pins the gate: the
// first request verifies (chain id + probe, once), later requests pass
// straight through, and the probe is never re-run.
func TestVerifiedLogWarehouse_RoutesOnlyAfterVerification(t *testing.T) {
	t.Parallel()
	inner := mocks.NewMockLogWarehouse(gomock.NewController(t))
	inner.EXPECT().ChainID(gomock.Any()).Return(big.NewInt(1), nil).Times(1)
	inner.EXPECT().FilterLogs(gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, q ethereum.FilterQuery) ([]types.Log, error) {
			require.Equal(t, int64(7), q.FromBlock.Int64(), "the probe query is sent as-is")
			return []types.Log{{Topics: make([]common.Hash, 3)}}, nil
		}).Times(1)
	inner.EXPECT().Head(gomock.Any()).Return(uint64(100), nil).Times(2)

	w := newVerified(inner, mainnetReqs(threeTopicProbe()))
	head, err := w.Head(context.Background())
	require.NoError(t, err)
	require.Equal(t, uint64(100), head)
	_, err = w.Head(context.Background())
	require.NoError(t, err, "second call must not re-verify (strict Times(1) on ChainID/probe)")
}

// TestVerifiedLogWarehouse_PermanentRefusals pins that a chain mismatch, a
// probe answered without the shape, and a probe refused as out of scope each
// disable routing for good: every later request fails without touching the
// inner warehouse again.
func TestVerifiedLogWarehouse_PermanentRefusals(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name  string
		setup func(m *mocks.MockLogWarehouse)
		want  error
	}{
		{"chain mismatch", func(m *mocks.MockLogWarehouse) {
			m.EXPECT().ChainID(gomock.Any()).Return(big.NewInt(11155111), nil).Times(1)
		}, adapter.ErrLogWarehouseChainMismatch},
		{"probe without the shape", func(m *mocks.MockLogWarehouse) {
			m.EXPECT().ChainID(gomock.Any()).Return(big.NewInt(1), nil).Times(1)
			m.EXPECT().FilterLogs(gomock.Any(), gomock.Any()).Return([]types.Log{{Topics: make([]common.Hash, 4)}}, nil).Times(1)
		}, adapter.ErrLogWarehouseProbeFailed},
		{"probe out of scope", func(m *mocks.MockLogWarehouse) {
			m.EXPECT().ChainID(gomock.Any()).Return(big.NewInt(1), nil).Times(1)
			m.EXPECT().FilterLogs(gomock.Any(), gomock.Any()).
				Return(nil, fmt.Errorf("%w: blocks 7-7 extend below the warehouse coverage start 25000000", adapter.ErrOutOfScope)).Times(1)
		}, adapter.ErrLogWarehouseProbeFailed},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			inner := mocks.NewMockLogWarehouse(gomock.NewController(t))
			tc.setup(inner)
			w := newVerified(inner, mainnetReqs(threeTopicProbe()))

			require.ErrorIs(t, w.Verify(context.Background()), tc.want)
			_, err := w.Head(context.Background())
			require.ErrorIs(t, err, tc.want, "sticky: no request may reach the inner warehouse")
			_, err = w.FilterLogs(context.Background(), ethereum.FilterQuery{})
			require.ErrorIs(t, err, tc.want)
		})
	}
}

// TestVerifiedLogWarehouse_UnreachableRetriesNextCall pins the transient
// path: a warehouse that does not answer stays unverified (nothing is
// routed), and the next request verifies again and succeeds.
func TestVerifiedLogWarehouse_UnreachableRetriesNextCall(t *testing.T) {
	t.Parallel()
	inner := mocks.NewMockLogWarehouse(gomock.NewController(t))
	gomock.InOrder(
		inner.EXPECT().ChainID(gomock.Any()).Return(nil, errors.New("connection refused")),
		inner.EXPECT().ChainID(gomock.Any()).Return(big.NewInt(1), nil),
		inner.EXPECT().FilterLogs(gomock.Any(), gomock.Any()).Return(nil, errors.New("connection reset")),
		inner.EXPECT().ChainID(gomock.Any()).Return(big.NewInt(1), nil),
		inner.EXPECT().FilterLogs(gomock.Any(), gomock.Any()).Return([]types.Log{{Topics: make([]common.Hash, 3)}}, nil),
		inner.EXPECT().Head(gomock.Any()).Return(uint64(5), nil),
	)
	w := newVerified(inner, mainnetReqs(threeTopicProbe()))

	_, err := w.Head(context.Background())
	require.ErrorIs(t, err, adapter.ErrLogWarehouseUnverified, "chain id unanswered: not routed")
	_, err = w.Head(context.Background())
	require.ErrorIs(t, err, adapter.ErrLogWarehouseUnverified, "probe unanswered: still not routed")
	head, err := w.Head(context.Background())
	require.NoError(t, err)
	require.Equal(t, uint64(5), head)
}

// TestVerifiedLogWarehouse_NoProbesNeedsOnlyChainID pins that a chain without
// probes (a testnet) is trusted on the chain id alone.
func TestVerifiedLogWarehouse_NoProbesNeedsOnlyChainID(t *testing.T) {
	t.Parallel()
	inner := mocks.NewMockLogWarehouse(gomock.NewController(t))
	inner.EXPECT().ChainID(gomock.Any()).Return(big.NewInt(11155111), nil).Times(1)
	inner.EXPECT().FilterLogs(gomock.Any(), gomock.Any()).Return(nil, nil).Times(1)
	w := newVerified(inner, adapter.LogWarehouseRequirements{ChainID: 11155111})
	_, err := w.FilterLogs(context.Background(), ethereum.FilterQuery{})
	require.NoError(t, err)
}

// TestVerifiedLogWarehouse_TransientFailureIsCachedForRetryInterval pins the
// cooldown: after a transport failure, requests inside the retry interval
// fail at once without touching the inner warehouse; the first request after
// the interval verifies again.
func TestVerifiedLogWarehouse_TransientFailureIsCachedForRetryInterval(t *testing.T) {
	t.Parallel()
	ctrl := gomock.NewController(t)
	inner := mocks.NewMockLogWarehouse(ctrl)
	clock := mocks.NewMockClock(ctrl)
	base := time.Unix(1_700_000_000, 0)
	now := base
	clock.EXPECT().Now().DoAndReturn(func() time.Time { return now }).AnyTimes()
	gomock.InOrder(
		inner.EXPECT().ChainID(gomock.Any()).Return(nil, errors.New("connection refused")),
		inner.EXPECT().ChainID(gomock.Any()).Return(big.NewInt(1), nil),
		inner.EXPECT().Head(gomock.Any()).Return(uint64(9), nil),
	)
	w := adapter.NewVerifiedLogWarehouse(inner, adapter.LogWarehouseRequirements{ChainID: 1}, clock, 30*time.Second)

	_, err := w.Head(context.Background())
	require.ErrorIs(t, err, adapter.ErrLogWarehouseUnverified)
	for range 3 {
		now = now.Add(5 * time.Second)
		_, err = w.Head(context.Background())
		require.ErrorIs(t, err, adapter.ErrLogWarehouseUnverified, "inside the cooldown: cached, no RPC (strict mock)")
	}
	now = base.Add(31 * time.Second)
	head, err := w.Head(context.Background())
	require.NoError(t, err)
	require.Equal(t, uint64(9), head)
}

// TestVerifiedLogWarehouse_ConcurrentCallersDoNotWaitForVerification pins
// that a caller arriving while another goroutine's verification is blocked on
// the warehouse fails immediately (so its query falls through to the vendor)
// instead of queueing behind the timeout-bound RPC.
func TestVerifiedLogWarehouse_ConcurrentCallersDoNotWaitForVerification(t *testing.T) {
	t.Parallel()
	inner := mocks.NewMockLogWarehouse(gomock.NewController(t))
	started := make(chan struct{})
	release := make(chan struct{})
	inner.EXPECT().ChainID(gomock.Any()).DoAndReturn(func(context.Context) (*big.Int, error) {
		close(started)
		<-release
		return big.NewInt(1), nil
	}).Times(1)
	inner.EXPECT().Head(gomock.Any()).Return(uint64(3), nil).Times(1)
	w := newVerified(inner, adapter.LogWarehouseRequirements{ChainID: 1})

	var wg sync.WaitGroup
	wg.Add(1)
	var firstErr error
	go func() {
		defer wg.Done()
		_, firstErr = w.Head(context.Background())
	}()
	<-started

	done := make(chan error, 1)
	go func() {
		_, err := w.Head(context.Background())
		done <- err
	}()
	select {
	case err := <-done:
		require.ErrorIs(t, err, adapter.ErrLogWarehouseUnverified, "second caller must not block on the in-flight verification")
	case <-time.After(2 * time.Second):
		t.Fatal("second caller blocked behind the in-flight verification")
	}

	close(release)
	wg.Wait()
	require.NoError(t, firstErr)
}
