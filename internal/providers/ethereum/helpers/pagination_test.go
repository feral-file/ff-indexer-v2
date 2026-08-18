package helpers_test

import (
	"context"
	"fmt"
	"math/big"
	"sort"
	"testing"

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
