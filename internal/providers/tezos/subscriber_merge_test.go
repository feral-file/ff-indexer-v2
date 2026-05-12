package tezos

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/feral-file/ff-indexer-v2/internal/adapter"
	"github.com/feral-file/ff-indexer-v2/internal/domain"
)

// TestMergeStreamMessage_toleratesFeedLevelRegression verifies that when a feed delivers a
// lower level after a higher one, we still merge into buffers (warn-only guardrail in production).
func TestMergeStreamMessage_toleratesFeedLevelRegression(t *testing.T) {
	ctx := context.Background()
	c := &tzSubscriber{
		chainID: domain.Chain("tezos:mainnet"),
		clock:   adapter.NewClock(),
	}
	s := newStreamProcessorState()

	c.mergeStreamMessage(ctx, streamMessage{
		transfers: []TzKTTokenTransfer{{Level: 101}},
	}, s)
	require.Equal(t, uint64(101), s.highestTransferLevel)

	c.mergeStreamMessage(ctx, streamMessage{
		transfers: []TzKTTokenTransfer{{Level: 100}},
	}, s)
	require.Len(t, s.buffers[100].transfers, 1)
	require.Equal(t, uint64(101), s.highestTransferLevel)

	c.mergeStreamMessage(ctx, streamMessage{
		updates: []TzKTBigMapUpdate{{Level: 202}},
	}, s)
	require.Equal(t, uint64(202), s.highestBigmapLevel)

	c.mergeStreamMessage(ctx, streamMessage{
		updates: []TzKTBigMapUpdate{{Level: 200}},
	}, s)
	require.Len(t, s.buffers[200].bigmaps, 1)
	require.Equal(t, uint64(202), s.highestBigmapLevel)
}

func TestMergeAllPendingStream_drainsQueuedWithoutBlocking(t *testing.T) {
	ctx := context.Background()
	c := &tzSubscriber{
		chainID:  domain.Chain("tezos:mainnet"),
		clock:    adapter.NewClock(),
		streamCh: make(chan streamMessage, 2),
	}
	s := newStreamProcessorState()
	c.streamCh <- streamMessage{
		transfers: []TzKTTokenTransfer{{Level: 7}},
	}

	err := c.mergeAllPendingStream(ctx, s)
	require.NoError(t, err)
	require.Len(t, s.buffers[7].transfers, 1)

	select {
	case <-c.streamCh:
		t.Fatal("streamCh should be drained")
	default:
	}
}

// TestDrainBufferedLevels_repeatsAfterOverflowForceFlush verifies the outer loop runs again
// after forceFlushOldestLevel so we keep flushing until len(buffers) <= maxBufferedLevels.
func TestDrainBufferedLevels_repeatsAfterOverflowForceFlush(t *testing.T) {
	ctx := context.Background()
	c := &tzSubscriber{
		chainID: domain.Chain("tezos:mainnet"),
		clock:   adapter.NewClock(),
		handler: nil,
	}
	s := newStreamProcessorState()
	s.maxBufferedLevels = 1
	now := c.clock.Now()
	s.buffers[10] = &levelBuffer{level: 10, firstSeen: now}
	s.buffers[11] = &levelBuffer{level: 11, firstSeen: now}
	s.buffers[12] = &levelBuffer{level: 12, firstSeen: now}
	s.highestTransferLevel = 9
	s.highestBigmapLevel = 9

	err := c.drainBufferedLevels(ctx, s, false)
	require.NoError(t, err)
	require.LessOrEqual(t, len(s.buffers), s.maxBufferedLevels)
	require.True(t, s.emittedLevels[10])
	require.True(t, s.emittedLevels[11])
	require.Contains(t, s.buffers, uint64(12))
}

// TestMergeAllPendingStream_boundedUnderSustainedIngress verifies that mergeAllPendingStream
// drains at most maxPendingDrainBatch messages to prevent timeout starvation under sustained ingress.
func TestMergeAllPendingStream_boundedUnderSustainedIngress(t *testing.T) {
	ctx := context.Background()
	streamCh := make(chan streamMessage, 2000)
	c := &tzSubscriber{
		chainID:  domain.Chain("tezos:mainnet"),
		streamCh: streamCh,
		clock:    adapter.NewClock(),
	}
	s := newStreamProcessorState()
	s.maxBufferedLevels = 2000 // Set high to prevent force-flush during test

	// Queue 2000 messages to simulate sustained high ingress
	for i := uint64(0); i < 2000; i++ {
		streamCh <- streamMessage{
			transfers: []TzKTTokenTransfer{{Level: 100 + i}},
		}
	}

	// mergeAllPendingStream should drain at most maxPendingDrainBatch (1000) and then return
	err := c.mergeAllPendingStream(ctx, s)
	require.NoError(t, err)

	// Should have drained exactly 1000 messages (the cap), leaving 1000 in the channel
	remaining := len(streamCh)
	require.Equal(t, 1000, remaining, "Expected 1000 messages remaining in streamCh after bounded drain")

	// Should have buffered levels 100-1099 (1000 levels)
	require.Equal(t, 1000, len(s.buffers), "Expected 1000 buffered levels")
}
