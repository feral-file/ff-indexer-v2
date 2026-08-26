package ingestion_test

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/feral-file/ff-indexer-v2/internal/adapter"
	"github.com/feral-file/ff-indexer-v2/internal/blockchain"
	"github.com/feral-file/ff-indexer-v2/internal/domain"
	"github.com/feral-file/ff-indexer-v2/internal/ingestion"
	"github.com/feral-file/ff-indexer-v2/internal/mocks"
	"github.com/feral-file/ff-indexer-v2/internal/store/schema"
)

// progressSource is an EventSource that also implements
// blockchain.ProgressReporter, the way the Ethereum pull subscriber does.
type progressSource struct {
	*mocks.MockBlockchainEventSource
	progress blockchain.RangeProgressHandler
}

func (p *progressSource) SetProgressHandler(h blockchain.RangeProgressHandler) { p.progress = h }

func newProgressRunner(t *testing.T, ctrl *gomock.Controller, source *progressSource, store *mocks.MockStore, jq *mocks.MockJobQueue, blacklist *mocks.MockBlacklistRegistry) ingestion.Runner {
	t.Helper()
	return ingestion.NewRunner(context.Background(), source, store, jq, blacklist, ingestion.Config{
		ChainID:       domain.ChainEthereumMainnet,
		StartBlock:    1000,
		TokenQueue:    "token_index",
		QueueCapacity: 8,
	}, adapter.NewClock())
}

// TestRunner_ProgressPersistsCursorThroughEmptyRange pins the fix for a resume
// wedge: a scanned range with no events must still move the durable cursor,
// otherwise a restart re-scans it and, past max_catchup_blocks, fails forever.
func TestRunner_ProgressPersistsCursorThroughEmptyRange(t *testing.T) {
	ctrl := gomock.NewController(t)
	source := &progressSource{MockBlockchainEventSource: mocks.NewMockBlockchainEventSource(ctrl)}
	store := mocks.NewMockStore(ctrl)
	jq := mocks.NewMockJobQueue(ctrl)
	blacklist := mocks.NewMockBlacklistRegistry(ctrl)

	store.EXPECT().GetBlockCursor(gomock.Any(), gomock.Any()).Return(uint64(999), nil).AnyTimes()
	store.EXPECT().SetBlockCursor(gomock.Any(), string(domain.ChainEthereumMainnet), uint64(1050)).Times(1).Return(nil)

	source.EXPECT().
		SubscribeEvents(gomock.Any(), uint64(1000), gomock.Any()).
		DoAndReturn(func(ctx context.Context, _ uint64, _ blockchain.EventHandler) error {
			require.NotNil(t, source.progress, "runner must register the progress handler before subscribing")
			require.NoError(t, source.progress(1050))
			time.Sleep(50 * time.Millisecond)
			return context.Canceled
		})
	source.EXPECT().Close()

	runner := newProgressRunner(t, ctrl, source, store, jq, blacklist)
	defer func() { _ = runner.Close() }()
	require.ErrorIs(t, runner.Run(context.Background()), context.Canceled)
}

// TestRunner_ProgressFlushesOpenBlockThenAdvances pins ordering: a marker
// flushes the open block it covers (jobs enqueued, cursor at that block) before
// the cursor moves to the reported height, and a later marker for an empty
// range moves it again.
func TestRunner_ProgressFlushesOpenBlockThenAdvances(t *testing.T) {
	ctrl := gomock.NewController(t)
	source := &progressSource{MockBlockchainEventSource: mocks.NewMockBlockchainEventSource(ctrl)}
	store := mocks.NewMockStore(ctrl)
	jq := mocks.NewMockJobQueue(ctrl)
	blacklist := mocks.NewMockBlacklistRegistry(ctrl)

	e1 := transferEvent()
	e1.TxHash = "0xtx1"
	e1.BlockNumber = 1000

	store.EXPECT().GetBlockCursor(gomock.Any(), gomock.Any()).Return(uint64(0), nil).AnyTimes()
	blacklist.EXPECT().IsTokenCIDBlacklisted(e1.TokenCID()).Return(false)
	store.EXPECT().GetTokenByTokenCID(gomock.Any(), e1.TokenCID().String()).Return(nil, nil)
	store.EXPECT().IsAnyAddressWatched(gomock.Any(), e1.Chain, []string{"0xfrom", "0xto"}).Return(true, nil)
	jq.EXPECT().Enqueue(gomock.Any(), gomock.Any()).Return(&schema.Job{ID: 1}, true, nil)
	gomock.InOrder(
		store.EXPECT().SetBlockCursor(gomock.Any(), string(e1.Chain), uint64(1000)).Return(nil),
		store.EXPECT().SetBlockCursor(gomock.Any(), string(e1.Chain), uint64(1003)).Return(nil),
	)

	source.EXPECT().
		SubscribeEvents(gomock.Any(), uint64(1000), gomock.Any()).
		DoAndReturn(func(ctx context.Context, _ uint64, handler blockchain.EventHandler) error {
			require.NoError(t, handler(e1))
			require.NoError(t, source.progress(1000)) // block 1000 complete: flush now
			require.NoError(t, source.progress(1003)) // 1001..1003 scanned, empty
			time.Sleep(50 * time.Millisecond)
			return context.Canceled
		})
	source.EXPECT().Close()

	runner := newProgressRunner(t, ctrl, source, store, jq, blacklist)
	defer func() { _ = runner.Close() }()
	require.ErrorIs(t, runner.Run(context.Background()), context.Canceled)
}

// TestRunner_ProgressReturnsOnlyAfterPersist pins the ack: the report call
// returns after the cursor write for that height has completed, so a source
// may treat a nil return as durable.
func TestRunner_ProgressReturnsOnlyAfterPersist(t *testing.T) {
	ctrl := gomock.NewController(t)
	source := &progressSource{MockBlockchainEventSource: mocks.NewMockBlockchainEventSource(ctrl)}
	store := mocks.NewMockStore(ctrl)
	jq := mocks.NewMockJobQueue(ctrl)
	blacklist := mocks.NewMockBlacklistRegistry(ctrl)

	var persisted atomic.Uint64
	store.EXPECT().GetBlockCursor(gomock.Any(), gomock.Any()).Return(uint64(999), nil).AnyTimes()
	store.EXPECT().SetBlockCursor(gomock.Any(), string(domain.ChainEthereumMainnet), gomock.Any()).
		DoAndReturn(func(_ context.Context, _ string, through uint64) error {
			time.Sleep(20 * time.Millisecond) // make a premature return observable
			persisted.Store(through)
			return nil
		}).Times(2)

	source.EXPECT().
		SubscribeEvents(gomock.Any(), uint64(1000), gomock.Any()).
		DoAndReturn(func(ctx context.Context, _ uint64, _ blockchain.EventHandler) error {
			require.NoError(t, source.progress(1010))
			require.Equal(t, uint64(1010), persisted.Load(), "report must not return before the cursor write")
			require.NoError(t, source.progress(1020))
			require.Equal(t, uint64(1020), persisted.Load())
			return context.Canceled
		})
	source.EXPECT().Close()

	runner := newProgressRunner(t, ctrl, source, store, jq, blacklist)
	defer func() { _ = runner.Close() }()
	require.ErrorIs(t, runner.Run(context.Background()), context.Canceled)
}

// TestRunner_ProgressBelowCursorIsIgnored pins the monotonic guard for markers.
func TestRunner_ProgressBelowCursorIsIgnored(t *testing.T) {
	ctrl := gomock.NewController(t)
	source := &progressSource{MockBlockchainEventSource: mocks.NewMockBlockchainEventSource(ctrl)}
	store := mocks.NewMockStore(ctrl)
	jq := mocks.NewMockJobQueue(ctrl)
	blacklist := mocks.NewMockBlacklistRegistry(ctrl)

	store.EXPECT().GetBlockCursor(gomock.Any(), gomock.Any()).Return(uint64(2000), nil).AnyTimes()
	// No SetBlockCursor expectation: a marker below the cursor must not write.

	source.EXPECT().
		SubscribeEvents(gomock.Any(), uint64(1000), gomock.Any()).
		DoAndReturn(func(ctx context.Context, _ uint64, _ blockchain.EventHandler) error {
			require.NoError(t, source.progress(1500))
			time.Sleep(50 * time.Millisecond)
			return context.Canceled
		})
	source.EXPECT().Close()

	runner := newProgressRunner(t, ctrl, source, store, jq, blacklist)
	defer func() { _ = runner.Close() }()
	require.ErrorIs(t, runner.Run(context.Background()), context.Canceled)
}
