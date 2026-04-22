package tezos_test

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"

	"github.com/feral-file/ff-indexer-v2/internal/domain"
	"github.com/feral-file/ff-indexer-v2/internal/logger"
	"github.com/feral-file/ff-indexer-v2/internal/mocks"
	"github.com/feral-file/ff-indexer-v2/internal/providers/tezos"
)

func TestMain(m *testing.M) {
	if err := logger.Initialize(logger.Config{Debug: false}); err != nil {
		panic(err)
	}
	os.Exit(m.Run())
}

func TestSubscribeEvents_StopsClientWhenSubscriptionFails(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	signalR := mocks.NewMockSignalR(ctrl)
	client := mocks.NewMockSignalRClient(ctrl)
	clock := mocks.NewMockClock(ctrl)

	sendErrCh := make(chan error, 1)
	sendErrCh <- errors.New("subscribe failed")
	timeoutCh := make(chan time.Time)

	signalR.EXPECT().NewClient(gomock.Any(), "wss://tzkt.example/ws", gomock.Any()).Return(client, nil)
	client.EXPECT().Start()
	clock.EXPECT().Sleep(time.Second)
	client.EXPECT().Send("SubscribeToTokenTransfers", gomock.Any()).Return(sendErrCh)
	clock.EXPECT().After(15 * time.Second).Return(timeoutCh)
	client.EXPECT().Stop()

	subscriber, err := tezos.NewSubscriber(tezos.Config{
		WebSocketURL: "wss://tzkt.example/ws",
		ChainID:      domain.ChainTezosMainnet,
	}, signalR, clock, mocks.NewMockTzKTClient(ctrl))
	require.NoError(t, err)

	err = subscriber.SubscribeEvents(context.Background(), 123, nil)
	require.Error(t, err)
	require.ErrorContains(t, err, "failed to subscribe to token transfers")
}
