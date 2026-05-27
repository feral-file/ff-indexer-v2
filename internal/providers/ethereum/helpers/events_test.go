package helpers_test

import (
	"context"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/feral-file/ff-indexer-v2/internal/domain"
	"github.com/feral-file/ff-indexer-v2/internal/mocks"
	"github.com/feral-file/ff-indexer-v2/internal/providers/ethereum/helpers"
)

func TestBaseEventFromLog_UsesLogBlockTimestamp(t *testing.T) {
	t.Parallel()

	vLog := types.Log{
		Address:        common.HexToAddress("0xb47e3cd837ddf8e4c57f05d70ab865de6e193bbb"),
		BlockNumber:    123,
		BlockHash:      common.HexToHash("0xabc"),
		TxHash:         common.HexToHash("0xdef"),
		BlockTimestamp: 1_700_000_000,
	}

	event, err := helpers.BaseEventFromLog(context.Background(), domain.ChainEthereumMainnet, vLog, nil)
	require.NoError(t, err)
	require.Equal(t, time.Unix(1_700_000_000, 0), event.Timestamp)
}

func TestBaseEventFromLog_FetchesTimestampWhenMissingOnLog(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	mockBlock := mocks.NewMockBlockProvider(ctrl)

	expected := time.Unix(1_600_000_000, 0)
	mockBlock.EXPECT().
		GetBlockTimestamp(gomock.Any(), uint64(456)).
		Return(expected, nil)

	vLog := types.Log{
		Address:     common.HexToAddress("0xb47e3cd837ddf8e4c57f05d70ab865de6e193bbb"),
		BlockNumber: 456,
		BlockHash:   common.HexToHash("0xabc"),
		TxHash:      common.HexToHash("0xdef"),
	}

	event, err := helpers.BaseEventFromLog(context.Background(), domain.ChainEthereumMainnet, vLog, mockBlock)
	require.NoError(t, err)
	require.Equal(t, expected, event.Timestamp)
}

func TestBaseEventFromLog_ReturnsErrorWhenTimestampLookupFails(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	mockBlock := mocks.NewMockBlockProvider(ctrl)

	mockBlock.EXPECT().
		GetBlockTimestamp(gomock.Any(), uint64(789)).
		Return(time.Time{}, context.DeadlineExceeded)

	vLog := types.Log{
		Address:     common.HexToAddress("0xb47e3cd837ddf8e4c57f05d70ab865de6e193bbb"),
		BlockNumber: 789,
		BlockHash:   common.HexToHash("0xabc"),
		TxHash:      common.HexToHash("0xdef"),
	}

	_, err := helpers.BaseEventFromLog(context.Background(), domain.ChainEthereumMainnet, vLog, mockBlock)
	require.Error(t, err)
	require.Contains(t, err.Error(), "resolve block timestamp")
}
