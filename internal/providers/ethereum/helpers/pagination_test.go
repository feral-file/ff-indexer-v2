package helpers_test

import (
	"context"
	"math/big"
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
