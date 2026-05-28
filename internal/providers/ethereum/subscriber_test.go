package ethereum_test

import (
	"context"
	"errors"
	"testing"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/feral-file/ff-indexer-v2/internal/domain"
	"github.com/feral-file/ff-indexer-v2/internal/mocks"
	ethprovider "github.com/feral-file/ff-indexer-v2/internal/providers/ethereum"
	"github.com/feral-file/ff-indexer-v2/internal/providers/ethereum/helpers"
)

func TestSubscribeEvents_ParseErrorFails(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	mockClient := mocks.NewMockEthereumProviderClient(ctrl)

	subscriber, err := ethprovider.NewSubscriber(ethprovider.Config{ChainID: domain.ChainEthereumMainnet}, mockClient, nil)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	subErrCh := make(chan error, 1)
	mockSub := &mockSubscription{errCh: subErrCh}

	mockClient.EXPECT().
		SubscribeFilterLogs(gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, _ ethereum.FilterQuery, ch chan<- types.Log) (ethereum.Subscription, error) {
			go func() {
				ch <- types.Log{
					BlockNumber: 100,
					Index:       1,
					Topics:      []common.Hash{helpers.TransferEventSignature},
				}
				cancel()
			}()
			return mockSub, nil
		})

	parseErr := errors.New("resolve block timestamp for block 100: get block timestamp: boom")
	mockClient.EXPECT().
		ParseEventLog(gomock.Any(), gomock.Any()).
		Return(nil, parseErr)

	err = subscriber.SubscribeEvents(ctx, 1, func(*domain.BlockchainEvent) error { return nil })
	require.Error(t, err)
	require.ErrorIs(t, err, parseErr)
	require.Contains(t, err.Error(), "parse log at block 100 index 1")
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
