package helpers_test

import (
	"context"
	"fmt"
	"math/big"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	ethadapter "github.com/feral-file/ff-indexer-v2/internal/adapter"
	"github.com/feral-file/ff-indexer-v2/internal/mocks"
	"github.com/feral-file/ff-indexer-v2/internal/providers/ethereum/helpers"
)

type blockRange struct {
	from uint64
	to   uint64
}

func mergeBlockRanges(ranges []blockRange) []blockRange {
	if len(ranges) == 0 {
		return nil
	}

	sort.Slice(ranges, func(i, j int) bool {
		if ranges[i].from == ranges[j].from {
			return ranges[i].to < ranges[j].to
		}
		return ranges[i].from < ranges[j].from
	})

	merged := []blockRange{ranges[0]}
	for _, r := range ranges[1:] {
		last := &merged[len(merged)-1]
		if r.from <= last.to+1 {
			if r.to > last.to {
				last.to = r.to
			}
			continue
		}
		merged = append(merged, r)
	}

	return merged
}

func requireContiguousCoverage(t *testing.T, ranges []blockRange, fromBlock, toBlock uint64) {
	t.Helper()

	merged := mergeBlockRanges(ranges)
	require.NotEmpty(t, merged)
	require.Equal(t, fromBlock, merged[0].from, "coverage starts at wrong block")
	require.Equal(t, toBlock, merged[len(merged)-1].to, "coverage ends at wrong block")

	for i := 1; i < len(merged); i++ {
		require.Equal(t, merged[i-1].to+1, merged[i].from,
			"gap between ranges [%d-%d] and [%d-%d]",
			merged[i-1].from, merged[i-1].to, merged[i].from, merged[i].to)
	}
}

func TestFilterLogsWithPagination_SingleBlockRange(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	mockClient := mocks.NewMockEthClient(ctrl)

	const blockNum uint64 = 12_345_678
	expectedLog := types.Log{
		Address:     common.HexToAddress("0xb47e3cd837ddf8e4c57f05d70ab865de6e193bbb"),
		BlockNumber: blockNum,
		Index:       1,
	}

	mockClient.EXPECT().
		FilterLogs(gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, query ethereum.FilterQuery) ([]types.Log, error) {
			require.Equal(t, blockNum, query.FromBlock.Uint64())
			require.Equal(t, blockNum, query.ToBlock.Uint64())
			return []types.Log{expectedLog}, nil
		})

	pagination := helpers.NewPaginationHelper(mockClient, ethadapter.NewClock(), nil)
	logs, err := pagination.FilterLogsWithPagination(context.Background(), ethereum.FilterQuery{
		FromBlock: new(big.Int).SetUint64(blockNum),
		ToBlock:   new(big.Int).SetUint64(blockNum),
		Addresses: []common.Address{expectedLog.Address},
	})
	require.NoError(t, err)
	require.Len(t, logs, 1)
	require.Equal(t, expectedLog.BlockNumber, logs[0].BlockNumber)
}

func TestFilterLogsWithPagination_ReturnsLogOnOuterPageBoundaryBlock(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	mockClient := mocks.NewMockEthClient(ctrl)

	const (
		fromBlock       uint64 = 0
		toBlock         uint64 = 2_000_000
		boundaryBlock   uint64 = 1_000_000
		contractAddress        = "0xb47e3cd837ddf8e4c57f05d70ab865de6e193bbb"
	)

	boundaryLog := types.Log{
		Address:     common.HexToAddress(contractAddress),
		BlockNumber: boundaryBlock,
		Index:       7,
	}

	mockClient.EXPECT().
		FilterLogs(gomock.Any(), gomock.Any()).
		AnyTimes().
		DoAndReturn(func(_ context.Context, query ethereum.FilterQuery) ([]types.Log, error) {
			if query.FromBlock.Uint64() <= boundaryBlock && query.ToBlock.Uint64() >= boundaryBlock {
				return []types.Log{boundaryLog}, nil
			}
			return nil, nil
		})

	pagination := helpers.NewPaginationHelper(mockClient, ethadapter.NewClock(), nil)
	logs, err := pagination.FilterLogsWithPagination(context.Background(), ethereum.FilterQuery{
		FromBlock: new(big.Int).SetUint64(fromBlock),
		ToBlock:   new(big.Int).SetUint64(toBlock),
		Addresses: []common.Address{boundaryLog.Address},
	})
	require.NoError(t, err)
	require.Len(t, logs, 1)
	require.Equal(t, boundaryBlock, logs[0].BlockNumber)
}

func TestFilterLogsWithPagination_ContiguousFilterLogsCoverage(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	mockClient := mocks.NewMockEthClient(ctrl)

	const (
		fromBlock uint64 = 0
		toBlock   uint64 = 2_500_000
	)

	var queriedRanges []blockRange
	mockClient.EXPECT().
		FilterLogs(gomock.Any(), gomock.Any()).
		AnyTimes().
		DoAndReturn(func(_ context.Context, query ethereum.FilterQuery) ([]types.Log, error) {
			queriedRanges = append(queriedRanges, blockRange{
				from: query.FromBlock.Uint64(),
				to:   query.ToBlock.Uint64(),
			})
			return nil, nil
		})

	pagination := helpers.NewPaginationHelper(mockClient, ethadapter.NewClock(), nil)
	_, err := pagination.FilterLogsWithPagination(context.Background(), ethereum.FilterQuery{
		FromBlock: new(big.Int).SetUint64(fromBlock),
		ToBlock:   new(big.Int).SetUint64(toBlock),
	})
	require.NoError(t, err)
	requireContiguousCoverage(t, queriedRanges, fromBlock, toBlock)
}

func TestFilterLogsWithPagination_ReturnsLogsFromMultipleOuterPages(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	mockClient := mocks.NewMockEthClient(ctrl)

	const contractAddress = "0xb47e3cd837ddf8e4c57f05d70ab865de6e193bbb"
	addr := common.HexToAddress(contractAddress)

	logsByBlock := map[uint64]types.Log{
		100: {
			Address:     addr,
			BlockNumber: 100,
			Index:       1,
		},
		1_000_100: {
			Address:     addr,
			BlockNumber: 1_000_100,
			Index:       2,
		},
		2_000_200: {
			Address:     addr,
			BlockNumber: 2_000_200,
			Index:       3,
		},
	}

	mockClient.EXPECT().
		FilterLogs(gomock.Any(), gomock.Any()).
		AnyTimes().
		DoAndReturn(func(_ context.Context, query ethereum.FilterQuery) ([]types.Log, error) {
			var logs []types.Log
			for block, log := range logsByBlock {
				if query.FromBlock.Uint64() <= block && query.ToBlock.Uint64() >= block {
					logs = append(logs, log)
				}
			}
			return logs, nil
		})

	pagination := helpers.NewPaginationHelper(mockClient, ethadapter.NewClock(), nil)
	logs, err := pagination.FilterLogsWithPagination(context.Background(), ethereum.FilterQuery{
		FromBlock: big.NewInt(0),
		ToBlock:   big.NewInt(2_500_000),
		Addresses: []common.Address{addr},
	})
	require.NoError(t, err)
	require.Len(t, logs, len(logsByBlock))

	seen := make(map[uint64]struct{}, len(logs))
	for _, log := range logs {
		seen[log.BlockNumber] = struct{}{}
	}
	for block := range logsByBlock {
		_, ok := seen[block]
		require.True(t, ok, "expected log at block %d", block)
	}
}

func TestFilterLogsWithPagination_AdaptiveHalvingStillCoversRange(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	mockClient := mocks.NewMockEthClient(ctrl)
	mockClock := mocks.NewMockClock(ctrl)
	mockClock.EXPECT().Sleep(gomock.Any()).AnyTimes()

	const (
		fromBlock uint64 = 0
		toBlock   uint64 = 10_000
	)

	var queriedRanges []blockRange
	mockClient.EXPECT().
		FilterLogs(gomock.Any(), gomock.Any()).
		AnyTimes().
		DoAndReturn(func(_ context.Context, query ethereum.FilterQuery) ([]types.Log, error) {
			from := query.FromBlock.Uint64()
			to := query.ToBlock.Uint64()
			queriedRanges = append(queriedRanges, blockRange{from: from, to: to})

			if to-from+1 > 1_000 {
				return nil, fmt.Errorf("query returned more than 10000 results")
			}
			return nil, nil
		})

	pagination := helpers.NewPaginationHelper(mockClient, mockClock, nil)
	_, err := pagination.FilterLogsWithPagination(context.Background(), ethereum.FilterQuery{
		FromBlock: new(big.Int).SetUint64(fromBlock),
		ToBlock:   new(big.Int).SetUint64(toBlock),
	})
	require.NoError(t, err)
	requireContiguousCoverage(t, queriedRanges, fromBlock, toBlock)
}

// TestFilterLogsWithPagination_OneBlockTooManyResultsReturnsError tests that when a single-block
// query returns "too many results", pagination returns an explicit error instead of partial success.
func TestFilterLogsWithPagination_OneBlockTooManyResultsReturnsError(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	mockClient := mocks.NewMockEthClient(ctrl)
	mockClock := mocks.NewMockClock(ctrl)
	mockClock.EXPECT().Sleep(gomock.Any()).AnyTimes()

	const (
		fromBlock uint64 = 100
		toBlock   uint64 = 100 // Single block
	)

	// Every query for block 100 returns "too many results", even for a single block
	mockClient.EXPECT().
		FilterLogs(gomock.Any(), gomock.Any()).
		AnyTimes().
		DoAndReturn(func(_ context.Context, query ethereum.FilterQuery) ([]types.Log, error) {
			from := query.FromBlock.Uint64()
			to := query.ToBlock.Uint64()

			// Even a single-block query returns too many results
			if from == 100 && to == 100 {
				return nil, fmt.Errorf("query returned more than 10000 results")
			}

			return nil, nil
		})

	pagination := helpers.NewPaginationHelper(mockClient, mockClock, nil)
	logs, err := pagination.FilterLogsWithPagination(context.Background(), ethereum.FilterQuery{
		FromBlock: new(big.Int).SetUint64(fromBlock),
		ToBlock:   new(big.Int).SetUint64(toBlock),
	})

	// Should return an error, not empty logs
	require.Error(t, err)
	require.Nil(t, logs)
	require.Contains(t, err.Error(), "too many results in single block 100")
}

// TestFilterLogsWithPagination_RangeCappedProviderSplitsAndCompletes simulates a
// provider that caps the queried block span (e.g. "range 9999999 exceeds limit
// of 10000") instead of capping result counts. Pagination must recognize the
// error, split the window down to an accepted span, cover the whole range, and
// never probe above the discovered cap again — every above-cap probe is a
// guaranteed rejection plus a one-second sleep, and against the helper's
// one-minute default deadline a probe-per-window scan times out with partial
// progress on any real owner enumeration.
func TestFilterLogsWithPagination_RangeCappedProviderSplitsAndCompletes(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	mockClient := mocks.NewMockEthClient(ctrl)
	mockClock := mocks.NewMockClock(ctrl)

	var totalSlept time.Duration
	mockClock.EXPECT().
		Sleep(gomock.Any()).
		AnyTimes().
		Do(func(d time.Duration) {
			totalSlept += d
		})

	const (
		fromBlock uint64 = 0
		toBlock   uint64 = 1000
		spanLimit uint64 = 16
	)

	var (
		successRanges          []blockRange
		totalCalls             int
		sawSuccess             bool
		rejectionsAfterSuccess int
	)

	mockClient.EXPECT().
		FilterLogs(gomock.Any(), gomock.Any()).
		AnyTimes().
		DoAndReturn(func(_ context.Context, query ethereum.FilterQuery) ([]types.Log, error) {
			totalCalls++
			from := query.FromBlock.Uint64()
			to := query.ToBlock.Uint64()
			span := to - from + 1
			if span > spanLimit {
				if sawSuccess {
					rejectionsAfterSuccess++
				}
				return nil, fmt.Errorf("range %d exceeds limit of %d", span, spanLimit)
			}
			sawSuccess = true
			successRanges = append(successRanges, blockRange{from: from, to: to})
			return nil, nil
		})

	pagination := helpers.NewPaginationHelper(mockClient, mockClock, nil)
	logs, err := pagination.FilterLogsWithPagination(context.Background(), ethereum.FilterQuery{
		FromBlock: new(big.Int).SetUint64(fromBlock),
		ToBlock:   new(big.Int).SetUint64(toBlock),
	})

	require.NoError(t, err)
	require.Empty(t, logs)
	requireContiguousCoverage(t, successRanges, fromBlock, toBlock)

	// The cap is discovered once, during the initial halving cascade. Probing
	// above it after a success is a guaranteed rejection.
	require.Zero(t, rejectionsAfterSuccess, "pagination probed above the discovered range cap after a success")

	// Sleep happens only on rejections, so total slept time is the halving
	// cascade alone (~16s from the 1M default step down to the cap). Ramping
	// above the cap after every success slept once per window (~63s here),
	// which would blow the one-minute default deadline scaled to any real scan.
	require.LessOrEqual(t, totalSlept, 20*time.Second, "pagination slept beyond the initial halving cascade")

	// Covering 1001 blocks at a span cap of 16 is ~63 successful windows plus
	// the ~16-call cascade. The old reset-and-reprobe behavior took 2-17 calls
	// per window, well over 150.
	require.Less(t, totalCalls, 120, "pagination is re-paying rejected probes after successes")
}

func TestFilterLogsWithPagination_CapHoistedAcrossOuterWindows(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	mockClient := mocks.NewMockEthClient(ctrl)
	mockClock := mocks.NewMockClock(ctrl)

	var totalSlept time.Duration
	mockClock.EXPECT().
		Sleep(gomock.Any()).
		AnyTimes().
		Do(func(d time.Duration) {
			totalSlept += d
		})

	// Range spans four outer windows at the 1M default step; the provider caps
	// spans at 500k. The cap must be discovered in the first window and carried
	// to the remaining ones instead of being re-probed (and re-slept) per window.
	const (
		fromBlock uint64 = 0
		toBlock   uint64 = 3_999_999
		spanLimit uint64 = 500_000
	)

	var (
		successRanges          []blockRange
		totalCalls             int
		sawSuccess             bool
		rejectionsAfterSuccess int
	)

	mockClient.EXPECT().
		FilterLogs(gomock.Any(), gomock.Any()).
		AnyTimes().
		DoAndReturn(func(_ context.Context, query ethereum.FilterQuery) ([]types.Log, error) {
			totalCalls++
			from := query.FromBlock.Uint64()
			to := query.ToBlock.Uint64()
			span := to - from + 1
			if span > spanLimit {
				if sawSuccess {
					rejectionsAfterSuccess++
				}
				return nil, fmt.Errorf("range %d exceeds limit of %d", span, spanLimit)
			}
			sawSuccess = true
			successRanges = append(successRanges, blockRange{from: from, to: to})
			return nil, nil
		})

	pagination := helpers.NewPaginationHelper(mockClient, mockClock, nil)
	logs, err := pagination.FilterLogsWithPagination(context.Background(), ethereum.FilterQuery{
		FromBlock: new(big.Int).SetUint64(fromBlock),
		ToBlock:   new(big.Int).SetUint64(toBlock),
	})

	require.NoError(t, err)
	require.Empty(t, logs)
	requireContiguousCoverage(t, successRanges, fromBlock, toBlock)

	// The cap is a provider property, not a window property. Once any window
	// has discovered it, later outer windows must start at or below it.
	require.Zero(t, rejectionsAfterSuccess, "a later outer window probed above the cap discovered earlier in the walk")

	// Only the initial discovery cascade may sleep (1M -> 500k is one halving).
	require.LessOrEqual(t, totalSlept, 2*time.Second, "pagination re-paid the halving cascade in later outer windows")

	// 4M blocks at a 500k cap is ~8-12 successful calls plus the one-call
	// discovery cascade. Re-probing per outer window took 1 rejection + sleep
	// per window on top of that.
	require.Less(t, totalCalls, 16, "pagination made rejected probes beyond the initial discovery")
}

// TestFilterLogsWithPagination_SpanCapSeedsStepWithoutProbing pins the cost
// contract of a configured span cap: the walk never issues a window wider than
// the cap (so there is no halving cascade and no rejection sleeps — the strict
// clock mock fails the test on any Sleep), and outer windows align with the
// inner step so covering the range takes exactly ceil(blocks/(cap+1)) calls.
// The pre-guard outer loop cut windows one block wider than the inner step,
// which cost a second single-block call per window after the cap was hoisted.
// CallBudget is set to that exact call count to pin that a fitting walk is not
// aborted (no off-by-one in the budget check).
func TestFilterLogsWithPagination_SpanCapSeedsStepWithoutProbing(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	mockClient := mocks.NewMockEthClient(ctrl)
	mockClock := mocks.NewMockClock(ctrl) // no Sleep expectation: any sleep fails

	const (
		fromBlock     uint64 = 0
		toBlock       uint64 = 109 // 110 blocks at cap+1=17 per window -> exactly 7 calls
		spanCap       uint64 = 16
		expectedCalls int    = 7
	)

	var (
		ranges     []blockRange
		totalCalls int
	)
	mockClient.EXPECT().
		FilterLogs(gomock.Any(), gomock.Any()).
		AnyTimes().
		DoAndReturn(func(_ context.Context, query ethereum.FilterQuery) ([]types.Log, error) {
			totalCalls++
			from := query.FromBlock.Uint64()
			to := query.ToBlock.Uint64()
			require.LessOrEqual(t, to-from, spanCap, "window wider than the configured span cap")
			ranges = append(ranges, blockRange{from: from, to: to})
			return nil, nil
		})

	pagination := helpers.NewGuardedPaginationHelper(mockClient, mockClock, nil, helpers.PaginationGuards{
		SpanCap:    spanCap,
		CallBudget: expectedCalls,
	})
	logs, err := pagination.FilterLogsWithPagination(context.Background(), ethereum.FilterQuery{
		FromBlock: new(big.Int).SetUint64(fromBlock),
		ToBlock:   new(big.Int).SetUint64(toBlock),
	})

	require.NoError(t, err)
	require.Empty(t, logs)
	requireContiguousCoverage(t, ranges, fromBlock, toBlock)
	require.Equal(t, expectedCalls, totalCalls,
		"a span-cap-seeded walk must cover the range in exactly ceil(blocks/(cap+1)) calls")
}

// TestFilterLogsWithPagination_CallBudgetAborts pins the backstop: a walk that
// would exceed the configured FilterLogs call budget aborts with
// ErrCallBudgetExhausted after exactly budget calls instead of walking on and
// draining the provider's credit quota. The provider is range-capped and the
// cap is NOT configured, so the walk pays the halving cascade — the scenario
// the backstop exists for.
func TestFilterLogsWithPagination_CallBudgetAborts(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	mockClient := mocks.NewMockEthClient(ctrl)
	mockClock := mocks.NewMockClock(ctrl)
	mockClock.EXPECT().Sleep(gomock.Any()).AnyTimes()

	const (
		spanLimit  uint64 = 16
		callBudget int    = 10
	)

	totalCalls := 0
	mockClient.EXPECT().
		FilterLogs(gomock.Any(), gomock.Any()).
		AnyTimes().
		DoAndReturn(func(_ context.Context, query ethereum.FilterQuery) ([]types.Log, error) {
			totalCalls++
			span := query.ToBlock.Uint64() - query.FromBlock.Uint64()
			if span > spanLimit {
				return nil, fmt.Errorf("range %d exceeds limit of %d", span, spanLimit)
			}
			return nil, nil
		})

	pagination := helpers.NewGuardedPaginationHelper(mockClient, mockClock, nil, helpers.PaginationGuards{
		CallBudget: callBudget,
	})
	logs, err := pagination.FilterLogsWithPagination(context.Background(), ethereum.FilterQuery{
		FromBlock: new(big.Int).SetUint64(0),
		ToBlock:   new(big.Int).SetUint64(10_000),
	})

	require.ErrorIs(t, err, helpers.ErrCallBudgetExhausted)
	require.Nil(t, logs)
	require.Equal(t, callBudget, totalCalls, "the walk must stop issuing calls once the budget is spent")
}

// TestFilterLogsWithPagination_MaxConcurrentCallsBoundsAllWalks pins the
// process-wide eth_getLogs bound. One helper is shared by every walk in the
// token worker pool, so the bound must hold ACROSS walks, not per walk: twelve
// concurrent walks (think 4 workers × 3 merged owner queries) against a helper
// capped at 3 must never have more than 3 FilterLogs calls in flight. Each
// mocked call blocks briefly so the walks genuinely overlap; a bound that only
// held per walk would be caught by the in-flight counter exceeding the cap.
func TestFilterLogsWithPagination_MaxConcurrentCallsBoundsAllWalks(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	mockClient := mocks.NewMockEthClient(ctrl)
	mockClock := mocks.NewMockClock(ctrl)

	const (
		maxConcurrent = 3
		walks         = 12
		windowsPer    = 4
		spanCap       = uint64(100)
	)
	var (
		mu       sync.Mutex
		inFlight int
		maxSeen  int
		total    int
	)
	mockClient.EXPECT().
		FilterLogs(gomock.Any(), gomock.Any()).
		AnyTimes().
		DoAndReturn(func(_ context.Context, _ ethereum.FilterQuery) ([]types.Log, error) {
			mu.Lock()
			inFlight++
			if inFlight > maxSeen {
				maxSeen = inFlight
			}
			total++
			mu.Unlock()
			time.Sleep(5 * time.Millisecond) // force real overlap between walks
			mu.Lock()
			inFlight--
			mu.Unlock()
			return nil, nil
		})

	pagination := helpers.NewGuardedPaginationHelper(mockClient, mockClock, nil, helpers.PaginationGuards{
		SpanCap:            spanCap,
		MaxConcurrentCalls: maxConcurrent,
	})

	var wg sync.WaitGroup
	errs := make(chan error, walks)
	for i := range uint64(walks) {
		wg.Add(1)
		go func(i uint64) {
			defer wg.Done()
			from := i * 10_000
			_, err := pagination.FilterLogsWithPagination(context.Background(), ethereum.FilterQuery{
				FromBlock: new(big.Int).SetUint64(from),
				ToBlock:   new(big.Int).SetUint64(from + windowsPer*(spanCap+1) - 1),
			})
			errs <- err
		}(i)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}

	require.Equal(t, walks*windowsPer, total, "every window of every walk must still be fetched")
	require.LessOrEqual(t, maxSeen, maxConcurrent,
		"in-flight FilterLogs calls across ALL walks must never exceed MaxConcurrentCalls")
	require.Equal(t, maxConcurrent, maxSeen,
		"the bound must be reached, proving walks genuinely overlapped (otherwise the test proves nothing)")
}

// TestFilterLogsWithPagination_MaxConcurrentCallsCancellationDoesNotDeadlock
// pins that slot acquisition is context-aware: a walk whose context is already
// canceled while every slot is held by other walks must return promptly with
// the context error instead of blocking forever on the semaphore — otherwise a
// canceled scan could wedge a worker goroutine until the slot holders finish.
func TestFilterLogsWithPagination_MaxConcurrentCallsCancellationDoesNotDeadlock(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	mockClient := mocks.NewMockEthClient(ctrl)
	mockClock := mocks.NewMockClock(ctrl)

	holdersStarted := make(chan struct{}, 1)
	release := make(chan struct{})
	mockClient.EXPECT().
		FilterLogs(gomock.Any(), gomock.Any()).
		AnyTimes().
		DoAndReturn(func(_ context.Context, _ ethereum.FilterQuery) ([]types.Log, error) {
			select {
			case holdersStarted <- struct{}{}:
			default:
			}
			<-release // hold the only slot until the test lets go
			return nil, nil
		})

	pagination := helpers.NewGuardedPaginationHelper(mockClient, mockClock, nil, helpers.PaginationGuards{
		SpanCap:            100,
		MaxConcurrentCalls: 1,
	})
	oneWindow := ethereum.FilterQuery{FromBlock: big.NewInt(0), ToBlock: big.NewInt(100)}

	// Walk A takes the single slot and parks inside FilterLogs.
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		_, _ = pagination.FilterLogsWithPagination(context.Background(), oneWindow)
	}()
	<-holdersStarted

	// Walk B arrives with an already-canceled context: it must NOT hang.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	done := make(chan error, 1)
	go func() {
		_, err := pagination.FilterLogsWithPagination(ctx, oneWindow)
		done <- err
	}()
	select {
	case err := <-done:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(2 * time.Second):
		t.Fatal("canceled walk deadlocked waiting for a FilterLogs slot")
	}

	close(release)
	wg.Wait()
}
