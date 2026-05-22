package tezos

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestEnqueueStream_overflowDuringBackfillReportsErrorAndCancels(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sub := &tzSubscriber{
		ctx:      ctx,
		cancel:   cancel,
		streamCh: make(chan streamMessage, 2),
		errCh:    make(chan error, 1),
	}

	sub.streamCh <- streamMessage{transfers: []TzKTTokenTransfer{{Level: 1}}}
	sub.streamCh <- streamMessage{transfers: []TzKTTokenTransfer{{Level: 2}}}

	sub.enqueueStream(streamMessage{transfers: []TzKTTokenTransfer{{Level: 3}}})

	require.ErrorIs(t, <-sub.errCh, ErrLiveStreamBufferOverflow)
	require.ErrorIs(t, ctx.Err(), context.Canceled)
}

func TestEnqueueStream_blocksWhenLiveStreamActive(t *testing.T) {
	sub := &tzSubscriber{
		ctx:      context.Background(),
		streamCh: make(chan streamMessage, 1),
	}
	sub.liveStreamActive.Store(true)

	sub.streamCh <- streamMessage{transfers: []TzKTTokenTransfer{{Level: 1}}}

	done := make(chan struct{})
	go func() {
		sub.enqueueStream(streamMessage{transfers: []TzKTTokenTransfer{{Level: 2}}})
		close(done)
	}()

	time.Sleep(50 * time.Millisecond)
	select {
	case <-done:
		t.Fatal("expected blocking enqueue while live stream is active and buffer is full")
	default:
	}

	<-sub.streamCh
	<-done
}

func TestEnqueueStream_nonBlockingDuringBackfillAcceptsUntilFull(t *testing.T) {
	sub := &tzSubscriber{
		ctx:      context.Background(),
		streamCh: make(chan streamMessage, 2),
		errCh:    make(chan error, 1),
	}

	sub.enqueueStream(streamMessage{transfers: []TzKTTokenTransfer{{Level: 1}}})
	sub.enqueueStream(streamMessage{transfers: []TzKTTokenTransfer{{Level: 2}}})

	require.Len(t, sub.streamCh, 2)

	select {
	case <-sub.errCh:
		t.Fatal("unexpected overflow error")
	default:
	}
}
