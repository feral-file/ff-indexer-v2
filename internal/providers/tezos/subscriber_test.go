package tezos_test

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/feral-file/ff-indexer-v2/internal/adapter"
	"github.com/feral-file/ff-indexer-v2/internal/domain"
	"github.com/feral-file/ff-indexer-v2/internal/logger"
	"github.com/feral-file/ff-indexer-v2/internal/mocks"
	"github.com/feral-file/ff-indexer-v2/internal/providers/tezos"
)

// tzktSignalReceiver is the SignalR hub callback surface implemented by the Tezos
// subscriber (matches adapter.SignalR NewClient receiver). Used in black-box tests to
// inject hub messages without package-internal helpers.
type tzktSignalReceiver interface {
	Transfers(data interface{})
	Bigmaps(data interface{})
}

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

func TestSubscribeEvents_emitsIncompleteLevelAfterQuietFeedTimeout(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	var mu sync.Mutex
	base := time.Unix(1_700_000_000, 0)
	current := base

	clock := mocks.NewMockClock(ctrl)
	signalR := mocks.NewMockSignalR(ctrl)
	client := mocks.NewMockSignalRClient(ctrl)
	tzkt := mocks.NewMockTzKTClient(ctrl)

	sleepDone := make(chan struct{})
	clock.EXPECT().Sleep(time.Second).Do(func(time.Duration) { close(sleepDone) }).Times(1)

	clock.EXPECT().Now().DoAndReturn(func() time.Time {
		mu.Lock()
		defer mu.Unlock()
		return current
	}).AnyTimes()

	subLongPoll := make(chan time.Time)
	levelWake := make(chan time.Time, 1)
	clock.EXPECT().After(gomock.Any()).DoAndReturn(func(d time.Duration) <-chan time.Time {
		switch {
		case d == 15*time.Second:
			return subLongPoll
		case d <= 0:
			instant := make(chan time.Time, 1)
			instant <- time.Time{}
			return instant
		default:
			require.Equal(t, 60*time.Second, d)
			return levelWake
		}
	}).AnyTimes()

	var hub tzktSignalReceiver
	recvReady := make(chan struct{})
	signalR.EXPECT().NewClient(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, _ string, receiver any) (adapter.SignalRClient, error) {
			hub = receiver.(tzktSignalReceiver)
			close(recvReady)
			return client, nil
		},
	).Times(1)

	client.EXPECT().Start()
	okSend := func() <-chan error {
		ch := make(chan error, 1)
		ch <- nil
		return ch
	}
	client.EXPECT().Send("SubscribeToTokenTransfers", gomock.Any()).Return(okSend()).Times(1)
	client.EXPECT().Send("SubscribeToBigMaps", gomock.Any()).Return(okSend()).Times(1)
	client.EXPECT().Stop().Times(1)

	fromAddr := "tz1from"
	toAddr := "tz1to"
	wantEvt := &domain.BlockchainEvent{
		Chain:           domain.ChainTezosMainnet,
		Standard:        domain.StandardFA2,
		ContractAddress: "KT1TestContract",
		TokenNumber:     "0",
		EventType:       domain.EventTypeTransfer,
		FromAddress:     &fromAddr,
		ToAddress:       &toAddr,
		Quantity:        "1",
		TxHash:          "ophash",
		BlockNumber:     100,
		Timestamp:       base,
		TxIndex:         0,
		LogIndex:        0,
	}
	tzkt.EXPECT().ParseTransfer(gomock.Any(), gomock.Any()).Return(wantEvt, nil).Times(1)

	subscriber, err := tezos.NewSubscriber(tezos.Config{
		WebSocketURL: "wss://tzkt.example/ws",
		ChainID:      domain.ChainTezosMainnet,
	}, signalR, clock, tzkt)
	require.NoError(t, err)

	seen := make(chan *domain.BlockchainEvent, 1)
	handler := func(e *domain.BlockchainEvent) error {
		seen <- e
		return nil
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	errCh := make(chan error, 1)
	go func() {
		errCh <- subscriber.SubscribeEvents(ctx, 1, handler)
	}()

	<-recvReady
	<-sleepDone
	time.Sleep(100 * time.Millisecond)

	tr := tezos.TzKTTokenTransfer{
		Level: 100,
		Token: tezos.TzKTTokenInfo{
			Standard: domain.StandardFA2,
			Contract: tezos.TzKTContract{Address: "KT1TestContract"},
			TokenID:  "0",
		},
	}
	payload, err := json.Marshal([]tezos.TzKTTokenTransfer{tr})
	require.NoError(t, err)
	hub.Transfers(tezos.TzKTMessage{Type: tezos.MessageTypeData, Data: payload})

	// Let processStream merge the message while Now() is still base (firstSeen must not
	// be recorded after we jump the clock past the level timeout).
	time.Sleep(50 * time.Millisecond)

	mu.Lock()
	current = base.Add(70 * time.Second)
	mu.Unlock()

	levelWake <- time.Now()

	select {
	case got := <-seen:
		require.Equal(t, wantEvt.TxHash, got.TxHash)
		require.Equal(t, wantEvt.BlockNumber, got.BlockNumber)
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for handler after quiet-feed level timeout")
	}

	cancel()
	require.ErrorIs(t, <-errCh, context.Canceled)
}
