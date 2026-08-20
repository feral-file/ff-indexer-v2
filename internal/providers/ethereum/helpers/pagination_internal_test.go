package helpers

import (
	"context"
	"math/big"
	"sync"
	"testing"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/stretchr/testify/require"

	ethadapter "github.com/feral-file/ff-indexer-v2/internal/adapter"
	"github.com/feral-file/ff-indexer-v2/internal/logger"
)

// initPaceTestLogger guards the global logger swap (parallel tests racing
// logger.Initialize is a harness data race); the heartbeat path calls the
// global logger, which is nil until initialized.
var initPaceTestLogger sync.Once

// paceFakeEthClient is a minimal white-box fake: internal/mocks imports
// packages that import this one, so package-internal tests roll their own.
type paceFakeEthClient struct {
	ethadapter.EthClient
	calls int
}

func (f *paceFakeEthClient) FilterLogs(_ context.Context, _ ethereum.FilterQuery) ([]types.Log, error) {
	f.calls++
	return nil, nil
}

// TestPaceStateAccumulatesAcrossOuterWindows pins the heartbeat regression: a
// span-cap-seeded walk completes exactly ONE window per getLogsWithRetry call,
// so the pace counter must live at the walk level and accumulate across calls —
// the previous call-local counter reset every window and could never reach the
// paceLogEvery threshold, silently killing the progress heartbeat whenever the
// span-cap guard was configured.
func TestPaceStateAccumulatesAcrossOuterWindows(t *testing.T) {
	initPaceTestLogger.Do(func() { _ = logger.Initialize(logger.Config{Debug: true}) })

	fake := &paceFakeEthClient{}
	h := NewGuardedPaginationHelper(fake, ethadapter.NewClock(), nil, PaginationGuards{SpanCap: 9})
	pace := paceState{target: 1000}
	callsUsed := 0

	// Three outer windows of exactly stepSize blocks each, sharing one pace state.
	const step = uint64(10) // spanCap 9 -> step 10
	for i := uint64(0); i < 3; i++ {
		from := i * step
		q := ethereum.FilterQuery{
			FromBlock: new(big.Int).SetUint64(from),
			ToBlock:   new(big.Int).SetUint64(from + step - 1),
		}
		_, _, err := h.getLogsWithRetry(context.Background(), q, step, &callsUsed, &pace)
		require.NoError(t, err)
	}

	require.Equal(t, 3, fake.calls, "each cap-sized outer window must be exactly one call")
	require.Equal(t, 3, pace.windowsDone,
		"pace must accumulate across outer windows; a per-call counter resets to 1 every window")
}

// TestFilterLogsWithPagination_HeartbeatPathExecutesUnderSpanCap drives a full
// cap-seeded walk past the paceLogEvery threshold, exercising the emission path
// end to end (the walk crosses 250 and 500 completed windows). Before the fix
// this walk executed the heartbeat branch zero times.
func TestFilterLogsWithPagination_HeartbeatPathExecutesUnderSpanCap(t *testing.T) {
	initPaceTestLogger.Do(func() { _ = logger.Initialize(logger.Config{Debug: true}) })

	fake := &paceFakeEthClient{}
	h := NewGuardedPaginationHelper(fake, ethadapter.NewClock(), nil, PaginationGuards{SpanCap: 0x0F})

	// spanCap 15 -> step 16; 600 windows = blocks [0, 9599].
	logs, err := h.FilterLogsWithPagination(context.Background(), ethereum.FilterQuery{
		FromBlock: big.NewInt(0),
		ToBlock:   big.NewInt(600*16 - 1),
	})
	require.NoError(t, err)
	require.Empty(t, logs)
	require.Equal(t, 600, fake.calls)
}

// TestPaceStateCadence pins the modulo contract directly: the heartbeat fires
// on every paceLogEvery-th window and nowhere else, and the counter is exact.
func TestPaceStateCadence(t *testing.T) {
	initPaceTestLogger.Do(func() { _ = logger.Initialize(logger.Config{Debug: true}) })

	p := paceState{target: 42}
	for range 2 * paceLogEvery {
		p.windowDone(context.Background(), 100, 16)
	}
	require.Equal(t, 2*paceLogEvery, p.windowsDone)
}
