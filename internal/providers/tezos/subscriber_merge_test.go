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
