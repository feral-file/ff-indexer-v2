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

func TestSubscribeEvents_ParseErrorSkipped(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	mockClient := mocks.NewMockEthereumProviderClient(ctrl)

	subscriber, err := ethprovider.NewSubscriber(ethprovider.Config{ChainID: domain.ChainEthereumMainnet}, mockClient, nil)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	subErrCh := make(chan error, 1)
	mockSub := &mockSubscription{errCh: subErrCh}

	var handlerCalls int
	mockClient.EXPECT().
		SubscribeFilterLogs(gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, _ ethereum.FilterQuery, ch chan<- types.Log) (ethereum.Subscription, error) {
			go func() {
				ch <- types.Log{
					BlockNumber: 100,
					Index:       1,
					Topics:      []common.Hash{helpers.TransferEventSignature},
				}
				ch <- types.Log{
					BlockNumber: 101,
					Index:       2,
					Topics:      []common.Hash{helpers.TransferEventSignature},
				}
			}()
			return mockSub, nil
		})

	parseErr := errors.New("resolve block timestamp for block 100: get block timestamp: boom")
	gomock.InOrder(
		mockClient.EXPECT().
			ParseEventLog(gomock.Any(), gomock.Any()).
			Return(nil, parseErr),
		mockClient.EXPECT().
			ParseEventLog(gomock.Any(), gomock.Any()).
			DoAndReturn(func(context.Context, types.Log) (*domain.BlockchainEvent, error) {
				cancel()
				return &domain.BlockchainEvent{
					Chain:       domain.ChainEthereumMainnet,
					EventType:   domain.EventTypeTransfer,
					TokenNumber: "1",
					TxHash:      "0xabc",
				}, nil
			}),
	)

	err = subscriber.SubscribeEvents(ctx, 1, func(*domain.BlockchainEvent) error {
		handlerCalls++
		return nil
	})
	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, 1, handlerCalls)
}

func TestSubscribeEvents_SkipsNilParsedEvent(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	mockClient := mocks.NewMockEthereumProviderClient(ctrl)

	subscriber, err := ethprovider.NewSubscriber(ethprovider.Config{ChainID: domain.ChainEthereumMainnet}, mockClient, nil)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	subErrCh := make(chan error, 1)
	mockSub := &mockSubscription{errCh: subErrCh}

	var handlerCalls int
	mockClient.EXPECT().
		SubscribeFilterLogs(gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, _ ethereum.FilterQuery, ch chan<- types.Log) (ethereum.Subscription, error) {
			go func() {
				ch <- types.Log{
					BlockNumber: 100,
					Index:       1,
					Topics:      []common.Hash{helpers.TransferEventSignature},
				}
				ch <- types.Log{
					BlockNumber: 101,
					Index:       2,
					Topics:      []common.Hash{common.HexToHash("0xdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef")},
				}
			}()
			return mockSub, nil
		})

	gomock.InOrder(
		mockClient.EXPECT().
			ParseEventLog(gomock.Any(), gomock.Any()).
			Return(&domain.BlockchainEvent{
				Chain:       domain.ChainEthereumMainnet,
				EventType:   domain.EventTypeTransfer,
				TokenNumber: "1",
				TxHash:      "0xabc",
			}, nil),
		mockClient.EXPECT().
			ParseEventLog(gomock.Any(), gomock.Any()).
			DoAndReturn(func(context.Context, types.Log) (*domain.BlockchainEvent, error) {
				cancel()
				return nil, nil
			}),
	)

	err = subscriber.SubscribeEvents(ctx, 1, func(*domain.BlockchainEvent) error {
		handlerCalls++
		return nil
	})
	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, 1, handlerCalls)
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
