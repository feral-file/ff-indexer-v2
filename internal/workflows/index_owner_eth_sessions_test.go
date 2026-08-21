package workflows_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/feral-file/ff-indexer-v2/internal/domain"
	"github.com/feral-file/ff-indexer-v2/internal/store/schema"
	"github.com/feral-file/ff-indexer-v2/internal/workflows"
)

// TestIndexEthereumTokenOwner_ResumesMidScanSession pins the crash-resume
// contract of the window loop: an in-flight session resumes from its persisted
// cursor — the first window starts at CursorBlock, NOT at the session's
// FromBlock, so the already-scanned prefix is never re-fetched.
func TestIndexEthereumTokenOwner_ResumesMidScanSession(t *testing.T) {
	t.Parallel()
	cfg := ownerCfg()
	cfg.EthereumScanWindowBlocks = 1000
	d := newOwnerWf(t, cfg)
	defer d.Ctrl.Finish()
	ctx, exec, wf := d.Ctx, d.Exec, d.Wf
	addr := "0xResume000000000000000000000000000000000"
	chainID := domain.ChainEthereumMainnet
	const sessionID int64 = 31

	exec.EXPECT().EnsureWatchedAddressExists(gomock.Any(), addr, chainID, gomock.Any()).Return(nil)
	// An in-flight session exists: [1000, 4999] scanned up to cursor 3000.
	exec.EXPECT().GetEthereumScanSession(gomock.Any(), addr, chainID).Return(&workflows.ScanSessionInfo{
		ID: sessionID, FromBlock: 1000, ToBlock: 4999, CursorBlock: 3000,
	}, nil)
	// Exactly the two remaining windows — a window at 1000 or 2000 would be an
	// unexpected call and fail the strict mock.
	exec.EXPECT().FetchEthereumOwnerWindow(gomock.Any(), addr, uint64(3000), uint64(3999)).Return(nil, nil)
	exec.EXPECT().FetchEthereumOwnerWindow(gomock.Any(), addr, uint64(4000), uint64(4999)).Return(nil, nil)
	// Fetch order is not a contract (windows fetch concurrently); PERSIST order
	// is — the cursor is a contiguous-prefix marker.
	gomock.InOrder(
		exec.EXPECT().PersistEthereumScanWindow(gomock.Any(), sessionID, gomock.Nil(), uint64(3000), uint64(3999)).Return(nil),
		exec.EXPECT().PersistEthereumScanWindow(gomock.Any(), sessionID, gomock.Nil(), uint64(4000), uint64(4999)).Return(nil),
	)
	exec.EXPECT().ReplayEthereumScanSession(gomock.Any(), addr, sessionID).Return(0, nil)
	exec.EXPECT().GetPendingScanTokens(gomock.Any(), sessionID).Return(nil, nil)
	exec.EXPECT().GetIndexingBlockRangeForAddress(gomock.Any(), addr, chainID).
		Return(&workflows.BlockRangeResult{MinBlock: 0, MaxBlock: 0}, nil)
	exec.EXPECT().UpdateIndexingBlockRangeForAddress(gomock.Any(), addr, chainID, uint64(1000), uint64(4999)).Return(nil)
	exec.EXPECT().DeleteEthereumScanSession(gomock.Any(), sessionID).Return(nil)
	// Next loop pass: nothing left to scan.
	exec.EXPECT().GetEthereumScanSession(gomock.Any(), addr, chainID).Return(nil, nil)
	exec.EXPECT().GetIndexingBlockRangeForAddress(gomock.Any(), addr, chainID).
		Return(&workflows.BlockRangeResult{MinBlock: 1000, MaxBlock: 4999}, nil)
	exec.EXPECT().GetLatestEthereumBlock(gomock.Any()).Return(uint64(4999), nil)

	require.NoError(t, wf.IndexEthereumTokenOwner(ctx, addr, nil))
}

// TestIndexEthereumTokenOwner_ResumesReplayedSessionWithoutRPC pins the
// quota-resume promise of the design: a session already in the replayed state
// goes straight to indexing the persisted pending tokens — the strict mock has
// NO FetchEthereumOwnerWindow, PersistEthereumScanWindow or ReplayEthereumScanSession
// expectations, so any
// re-scan RPC fails the test.
func TestIndexEthereumTokenOwner_ResumesReplayedSessionWithoutRPC(t *testing.T) {
	t.Parallel()
	d := newOwnerWf(t, ownerCfg())
	defer d.Ctrl.Finish()
	ctx, exec, wf := d.Ctx, d.Exec, d.Wf
	addr := "0xReplayed00000000000000000000000000000000"
	chainID := domain.ChainEthereumMainnet
	const sessionID int64 = 32

	pending := []domain.TokenWithBlock{{
		TokenCID:    domain.NewTokenCID(chainID, domain.StandardERC721, "0xC00000000000000000000000000000000000001", "7"),
		BlockNumber: 2500,
	}}

	exec.EXPECT().EnsureWatchedAddressExists(gomock.Any(), addr, chainID, gomock.Any()).Return(nil)
	exec.EXPECT().GetEthereumScanSession(gomock.Any(), addr, chainID).Return(&workflows.ScanSessionInfo{
		ID: sessionID, FromBlock: 1000, ToBlock: 5000, CursorBlock: 5001, Replayed: true,
	}, nil)
	exec.EXPECT().GetPendingScanTokens(gomock.Any(), sessionID).Return(pending, nil)
	exec.EXPECT().GetIndexingBlockRangeForAddress(gomock.Any(), addr, chainID).
		Return(&workflows.BlockRangeResult{MinBlock: 0, MaxBlock: 0}, nil).Times(2)
	stubSuccessfulIndexToken(exec)
	exec.EXPECT().MarkScanTokensIndexed(gomock.Any(), sessionID, []domain.TokenCID{pending[0].TokenCID}).Return(nil)
	exec.EXPECT().UpdateIndexingBlockRangeForAddress(gomock.Any(), addr, chainID, uint64(1000), uint64(5000)).Return(nil)
	exec.EXPECT().DeleteEthereumScanSession(gomock.Any(), sessionID).Return(nil)
	// Next loop pass: done.
	exec.EXPECT().GetEthereumScanSession(gomock.Any(), addr, chainID).Return(nil, nil)
	exec.EXPECT().GetIndexingBlockRangeForAddress(gomock.Any(), addr, chainID).
		Return(&workflows.BlockRangeResult{MinBlock: 1000, MaxBlock: 5000}, nil)
	exec.EXPECT().GetLatestEthereumBlock(gomock.Any()).Return(uint64(5000), nil)

	require.NoError(t, wf.IndexEthereumTokenOwner(ctx, addr, nil))
}

// TestIndexEthereumTokenOwner_ScansBackwardGapThenForwardGap pins the session
// sequencing for a partially-covered watermark: the backward history gap is
// scanned and merged first, then the forward gap to the chain head — two
// sessions, each completing before the next is derived.
func TestIndexEthereumTokenOwner_ScansBackwardGapThenForwardGap(t *testing.T) {
	t.Parallel()
	d := newOwnerWf(t, ownerCfg())
	defer d.Ctrl.Finish()
	ctx, exec, wf := d.Ctx, d.Exec, d.Wf
	addr := "0xGaps00000000000000000000000000000000000"
	chainID := domain.ChainEthereumMainnet
	latest := uint64(5000)

	exec.EXPECT().EnsureWatchedAddressExists(gomock.Any(), addr, chainID, gomock.Any()).Return(nil)
	exec.EXPECT().GetEthereumScanSession(gomock.Any(), addr, chainID).Return(nil, nil).Times(3)

	gomock.InOrder(
		// Pass 1: watermark [2000, 4000] -> backward gap [1000, 1999].
		exec.EXPECT().GetIndexingBlockRangeForAddress(gomock.Any(), addr, chainID).
			Return(&workflows.BlockRangeResult{MinBlock: 2000, MaxBlock: 4000}, nil),
		exec.EXPECT().CreateEthereumScanSession(gomock.Any(), addr, chainID, uint64(1000), uint64(1999)).
			Return(&workflows.ScanSessionInfo{ID: 41, FromBlock: 1000, ToBlock: 1999, CursorBlock: 1000}, nil),
		exec.EXPECT().FetchEthereumOwnerWindow(gomock.Any(), addr, uint64(1000), uint64(1999)).Return(nil, nil),
		exec.EXPECT().PersistEthereumScanWindow(gomock.Any(), int64(41), gomock.Nil(), uint64(1000), uint64(1999)).Return(nil),
		exec.EXPECT().ReplayEthereumScanSession(gomock.Any(), addr, int64(41)).Return(0, nil),
		exec.EXPECT().GetPendingScanTokens(gomock.Any(), int64(41)).Return(nil, nil),
		// Complete pass 1: merge [1000, 1999] into [2000, 4000] -> [1000, 4000].
		exec.EXPECT().GetIndexingBlockRangeForAddress(gomock.Any(), addr, chainID).
			Return(&workflows.BlockRangeResult{MinBlock: 2000, MaxBlock: 4000}, nil),
		exec.EXPECT().UpdateIndexingBlockRangeForAddress(gomock.Any(), addr, chainID, uint64(1000), uint64(4000)).Return(nil),
		exec.EXPECT().DeleteEthereumScanSession(gomock.Any(), int64(41)).Return(nil),
		// Pass 2: watermark [1000, 4000] -> forward gap [4001, 5000].
		exec.EXPECT().GetIndexingBlockRangeForAddress(gomock.Any(), addr, chainID).
			Return(&workflows.BlockRangeResult{MinBlock: 1000, MaxBlock: 4000}, nil),
		exec.EXPECT().CreateEthereumScanSession(gomock.Any(), addr, chainID, uint64(4001), latest).
			Return(&workflows.ScanSessionInfo{ID: 42, FromBlock: 4001, ToBlock: latest, CursorBlock: 4001}, nil),
		exec.EXPECT().FetchEthereumOwnerWindow(gomock.Any(), addr, uint64(4001), latest).Return(nil, nil),
		exec.EXPECT().PersistEthereumScanWindow(gomock.Any(), int64(42), gomock.Nil(), uint64(4001), latest).Return(nil),
		exec.EXPECT().ReplayEthereumScanSession(gomock.Any(), addr, int64(42)).Return(0, nil),
		exec.EXPECT().GetPendingScanTokens(gomock.Any(), int64(42)).Return(nil, nil),
		// Complete pass 2: merge -> [1000, 5000].
		exec.EXPECT().GetIndexingBlockRangeForAddress(gomock.Any(), addr, chainID).
			Return(&workflows.BlockRangeResult{MinBlock: 1000, MaxBlock: 4000}, nil),
		exec.EXPECT().UpdateIndexingBlockRangeForAddress(gomock.Any(), addr, chainID, uint64(1000), latest).Return(nil),
		exec.EXPECT().DeleteEthereumScanSession(gomock.Any(), int64(42)).Return(nil),
		// Pass 3: fully covered -> done.
		exec.EXPECT().GetIndexingBlockRangeForAddress(gomock.Any(), addr, chainID).
			Return(&workflows.BlockRangeResult{MinBlock: 1000, MaxBlock: latest}, nil),
	)
	exec.EXPECT().GetLatestEthereumBlock(gomock.Any()).Return(latest, nil).Times(3)

	require.NoError(t, wf.IndexEthereumTokenOwner(ctx, addr, nil))
}

// TestIndexEthereumTokenOwner_ParallelFetchCommitsInCursorOrder is the
// correctness guard for the parallel window pipeline. Fetches run concurrently
// and are DELIBERATELY released in reverse order (last window completes first),
// so the test fails unless the committer reorders: the cursor is a
// contiguous-prefix marker, and persisting window N+1 before window N would let
// a crash in between leave a gap that resume silently skips.
//
// It also pins that the concurrency bound is honored — with concurrency 3 and 4
// windows, at most 3 fetches are ever in flight — and that, because the cursor
// is never rewound, persist is called exactly once per window.
func TestIndexEthereumTokenOwner_ParallelFetchCommitsInCursorOrder(t *testing.T) {
	t.Parallel()
	cfg := ownerCfg()
	cfg.EthereumScanWindowBlocks = 1000
	cfg.EthereumScanWindowConcurrency = 3
	d := newOwnerWf(t, cfg)
	defer d.Ctrl.Finish()
	ctx, exec, wf := d.Ctx, d.Exec, d.Wf
	addr := "0xParallel0000000000000000000000000000000"
	chainID := domain.ChainEthereumMainnet
	const sessionID int64 = 51
	const concurrency = 3

	// Four windows over [1000, 4999]; only three may be in flight at once.
	windows := [][2]uint64{{1000, 1999}, {2000, 2999}, {3000, 3999}, {4000, 4999}}

	var (
		mu       sync.Mutex
		inFlight int
		maxSeen  int
		persists []uint64 // fromBlock of each persist, in call order
	)
	// The first `concurrency` fetches all start before any finishes, then are
	// released last-first so completion order is the REVERSE of cursor order.
	started := make(chan uint64, len(windows))
	release := make(map[uint64]chan struct{})
	for _, w := range windows {
		release[w[0]] = make(chan struct{})
	}

	exec.EXPECT().EnsureWatchedAddressExists(gomock.Any(), addr, chainID, gomock.Any()).Return(nil)
	exec.EXPECT().GetEthereumScanSession(gomock.Any(), addr, chainID).Return(&workflows.ScanSessionInfo{
		ID: sessionID, FromBlock: 1000, ToBlock: 4999, CursorBlock: 1000,
	}, nil)
	for _, w := range windows {
		from, to := w[0], w[1]
		exec.EXPECT().FetchEthereumOwnerWindow(gomock.Any(), addr, from, to).
			DoAndReturn(func(fctx context.Context, _ string, f, _ uint64) ([]schema.AddressScanLog, error) {
				mu.Lock()
				inFlight++
				if inFlight > maxSeen {
					maxSeen = inFlight
				}
				mu.Unlock()
				started <- f
				select {
				case <-release[f]:
				case <-fctx.Done():
					return nil, fctx.Err()
				}
				mu.Lock()
				inFlight--
				mu.Unlock()
				return []schema.AddressScanLog{{BlockNumber: f}}, nil
			})
		exec.EXPECT().PersistEthereumScanWindow(gomock.Any(), sessionID, gomock.Any(), from, to).
			DoAndReturn(func(_ context.Context, _ int64, rows []schema.AddressScanLog, f, _ uint64) error {
				mu.Lock()
				defer mu.Unlock()
				require.Len(t, rows, 1)
				require.Equal(t, f, rows[0].BlockNumber, "persist must receive the rows fetched for ITS window")
				persists = append(persists, f)
				return nil
			}).Times(1)
	}
	exec.EXPECT().ReplayEthereumScanSession(gomock.Any(), addr, sessionID).Return(0, nil)
	exec.EXPECT().GetPendingScanTokens(gomock.Any(), sessionID).Return(nil, nil)
	exec.EXPECT().GetIndexingBlockRangeForAddress(gomock.Any(), addr, chainID).
		Return(&workflows.BlockRangeResult{MinBlock: 0, MaxBlock: 0}, nil)
	exec.EXPECT().UpdateIndexingBlockRangeForAddress(gomock.Any(), addr, chainID, uint64(1000), uint64(4999)).Return(nil)
	exec.EXPECT().DeleteEthereumScanSession(gomock.Any(), sessionID).Return(nil)
	exec.EXPECT().GetEthereumScanSession(gomock.Any(), addr, chainID).Return(nil, nil)
	exec.EXPECT().GetIndexingBlockRangeForAddress(gomock.Any(), addr, chainID).
		Return(&workflows.BlockRangeResult{MinBlock: 1000, MaxBlock: 4999}, nil)
	exec.EXPECT().GetLatestEthereumBlock(gomock.Any()).Return(uint64(4999), nil)

	// Release in reverse: wait until `concurrency` fetches are in flight, then
	// free the LAST-started one first, and so on. The fourth window can only
	// start once a slot frees, proving the bound.
	go func() {
		var inflight []uint64
		for range concurrency {
			inflight = append(inflight, <-started)
		}
		for i := len(inflight) - 1; i >= 0; i-- {
			close(release[inflight[i]])
		}
		last := <-started
		close(release[last])
	}()

	require.NoError(t, wf.IndexEthereumTokenOwner(ctx, addr, nil))

	mu.Lock()
	defer mu.Unlock()
	require.Equal(t, []uint64{1000, 2000, 3000, 4000}, persists,
		"windows must commit in cursor order even though fetches completed in reverse")
	require.LessOrEqual(t, maxSeen, concurrency, "concurrency bound must be honored")
	require.Equal(t, concurrency, maxSeen, "the pipeline must actually fetch concurrently, not serially")
}

// TestIndexEthereumTokenOwner_ParallelFetchFailureCancelsSiblings pins the
// failure contract: when one in-flight fetch fails, the sibling fetches are
// canceled via the group context and NOTHING past the last committed window is
// persisted — the strict mock has no PersistEthereumScanWindow expectation for
// the windows after the failure point.
func TestIndexEthereumTokenOwner_ParallelFetchFailureCancelsSiblings(t *testing.T) {
	t.Parallel()
	cfg := ownerCfg()
	cfg.EthereumScanWindowBlocks = 1000
	cfg.EthereumScanWindowConcurrency = 3
	d := newOwnerWf(t, cfg)
	defer d.Ctrl.Finish()
	ctx, exec, wf := d.Ctx, d.Exec, d.Wf
	addr := "0xParallelFail00000000000000000000000000"
	chainID := domain.ChainEthereumMainnet
	const sessionID int64 = 52
	rpcErr := errors.New("provider exploded")

	exec.EXPECT().EnsureWatchedAddressExists(gomock.Any(), addr, chainID, gomock.Any()).Return(nil)
	exec.EXPECT().GetEthereumScanSession(gomock.Any(), addr, chainID).Return(&workflows.ScanSessionInfo{
		ID: sessionID, FromBlock: 1000, ToBlock: 3999, CursorBlock: 1000,
	}, nil)

	// Window 1 fails immediately. Windows 2 and 3 block until canceled by the
	// group context, then return its error — proving cancellation propagated.
	var canceled atomic.Int32
	exec.EXPECT().FetchEthereumOwnerWindow(gomock.Any(), addr, uint64(1000), uint64(1999)).Return(nil, rpcErr)
	for _, from := range []uint64{2000, 3000} {
		exec.EXPECT().FetchEthereumOwnerWindow(gomock.Any(), addr, from, from+999).
			DoAndReturn(func(fctx context.Context, _ string, _, _ uint64) ([]schema.AddressScanLog, error) {
				<-fctx.Done()
				canceled.Add(1)
				return nil, fctx.Err()
			}).MaxTimes(1)
	}

	err := wf.IndexEthereumTokenOwner(ctx, addr, nil)
	require.ErrorIs(t, err, rpcErr, "the ORIGINAL failure must surface, not the cancellation it caused")
}

// TestScanWindows pins the window partition: consecutive, non-overlapping,
// covering exactly [cursor, toBlock], with a short final window and no
// uint64 overflow near the top of the range.
func TestScanWindows(t *testing.T) {
	t.Parallel()

	t.Run("exact multiple", func(t *testing.T) {
		t.Parallel()
		ws := workflows.ScanWindowsForTest(1000, 2999, 1000)
		require.Equal(t, [][2]uint64{{1000, 1999}, {2000, 2999}}, ws)
	})
	t.Run("short final window", func(t *testing.T) {
		t.Parallel()
		ws := workflows.ScanWindowsForTest(1000, 2500, 1000)
		require.Equal(t, [][2]uint64{{1000, 1999}, {2000, 2500}}, ws)
	})
	t.Run("single block range", func(t *testing.T) {
		t.Parallel()
		require.Equal(t, [][2]uint64{{7, 7}}, workflows.ScanWindowsForTest(7, 7, 1000))
	})
	t.Run("cursor past end yields nothing", func(t *testing.T) {
		t.Parallel()
		require.Empty(t, workflows.ScanWindowsForTest(5001, 5000, 1000))
	})
	t.Run("no overflow near uint64 max", func(t *testing.T) {
		t.Parallel()
		top := ^uint64(0)
		ws := workflows.ScanWindowsForTest(top-5, top, 1000)
		require.Equal(t, [][2]uint64{{top - 5, top}}, ws)
	})
}

// TestIndexEthereumTokenOwner_StalledFirstWindowBoundsFetchAhead is the
// regression guard for the fetch-ahead bound (review finding on the parallel
// pipeline). Fetch slots are released on COMMIT, not on fetch: while the
// earliest window is stalled, nothing can commit, so at most `concurrency`
// windows may ever be fetched — the fetchers must NOT race ahead into the rest
// of the range and pile it into the reorder buffer, because if the stalled
// window then fails, everything buffered is discarded and the advertised
// one-window recovery bound would be false.
//
// Scenario: concurrency 3, eight windows. Window 0 stalls; windows 1 and 2
// complete instantly and sit in the buffer behind it. Window 3 needs a slot
// that only a commit can free, and nothing can commit — so it must never be
// fetched. Verified to FAIL on the pre-fix pipeline (window 3 was fetched
// ahead). Then window 0 fails: nothing may be persisted, and the original
// error must surface.
func TestIndexEthereumTokenOwner_StalledFirstWindowBoundsFetchAhead(t *testing.T) {
	t.Parallel()
	cfg := ownerCfg()
	cfg.EthereumScanWindowBlocks = 1000
	cfg.EthereumScanWindowConcurrency = 3
	d := newOwnerWf(t, cfg)
	defer d.Ctrl.Finish()
	ctx, exec, wf := d.Ctx, d.Exec, d.Wf
	addr := "0xStalledFirst0000000000000000000000000"
	chainID := domain.ChainEthereumMainnet
	const sessionID int64 = 53
	rpcErr := errors.New("earliest window failed after stalling")

	exec.EXPECT().EnsureWatchedAddressExists(gomock.Any(), addr, chainID, gomock.Any()).Return(nil)
	exec.EXPECT().GetEthereumScanSession(gomock.Any(), addr, chainID).Return(&workflows.ScanSessionInfo{
		ID: sessionID, FromBlock: 1000, ToBlock: 8999, CursorBlock: 1000, // 8 windows
	}, nil)

	// Windows 1 and 2 signal completion; once both are buffered behind the
	// stalled window 0, the test gives the fetchers a moment to (wrongly) race
	// ahead, then fails window 0.
	var fastDone sync.WaitGroup
	fastDone.Add(2)
	releaseWindow0 := make(chan struct{})

	exec.EXPECT().FetchEthereumOwnerWindow(gomock.Any(), addr, uint64(1000), uint64(1999)).
		DoAndReturn(func(fctx context.Context, _ string, _, _ uint64) ([]schema.AddressScanLog, error) {
			select {
			case <-releaseWindow0:
				return nil, rpcErr
			case <-fctx.Done():
				return nil, fctx.Err()
			}
		})
	for _, from := range []uint64{2000, 3000} {
		exec.EXPECT().FetchEthereumOwnerWindow(gomock.Any(), addr, from, from+999).
			DoAndReturn(func(_ context.Context, _ string, f, _ uint64) ([]schema.AddressScanLog, error) {
				fastDone.Done()
				return []schema.AddressScanLog{{BlockNumber: f}}, nil
			})
	}
	// THE ASSERTION: no fetch beyond the concurrency bound may happen while
	// window 0 is stalled. The strict mock has NO expectation for windows 3..7,
	// so any such call fails the test — on the pre-fix pipeline, window 3 (and
	// beyond) was fetched here.
	//
	// Nothing may be persisted either: window 0 never commits, so no later
	// window can. No PersistEthereumScanWindow expectation is registered.

	go func() {
		fastDone.Wait()
		// Windows 1 and 2 are buffered behind window 0. If the bound were
		// broken, the fetchers would take window 3 right now — give them every
		// chance to, so a regression is caught rather than raced past.
		time.Sleep(150 * time.Millisecond)
		close(releaseWindow0)
	}()

	err := wf.IndexEthereumTokenOwner(ctx, addr, nil)
	require.ErrorIs(t, err, rpcErr, "the stalled window's failure must surface as the original error")
}

// TestIndexEthereumTokenOwner_ScanHeadLagsChainHead pins the reorg-safety
// margin: a session's to_block is latest − EthereumScanHeadLagBlocks, never
// latest itself. The checkpoint design makes this load-bearing — a block that
// reorged after being checkpointed would replay phantom ownership events AND
// mark its canonical replacement as already scanned. The strict mock only
// accepts a session ending at latest−lag, so a regression to latest (or any
// off-by-one) fails on CreateEthereumScanSession.
func TestIndexEthereumTokenOwner_ScanHeadLagsChainHead(t *testing.T) {
	t.Parallel()
	cfg := ownerCfg()
	cfg.EthereumScanHeadLagBlocks = 64
	d := newOwnerWf(t, cfg)
	defer d.Ctrl.Finish()
	ctx, exec, wf := d.Ctx, d.Exec, d.Wf
	addr := "0xHeadLag00000000000000000000000000000000"
	chainID := domain.ChainEthereumMainnet
	const latest, lag = uint64(5000), uint64(64)
	const scanHead = latest - lag // 4936
	const sessionID int64 = 61

	exec.EXPECT().EnsureWatchedAddressExists(gomock.Any(), addr, chainID, gomock.Any()).Return(nil)
	exec.EXPECT().GetEthereumScanSession(gomock.Any(), addr, chainID).Return(nil, nil)
	exec.EXPECT().GetIndexingBlockRangeForAddress(gomock.Any(), addr, chainID).
		Return(&workflows.BlockRangeResult{MinBlock: 0, MaxBlock: 0}, nil).Times(2)
	exec.EXPECT().GetLatestEthereumBlock(gomock.Any()).Return(latest, nil)
	// First run: [sweepStart, latest−lag] — NOT [sweepStart, latest].
	exec.EXPECT().CreateEthereumScanSession(gomock.Any(), addr, chainID, uint64(1000), scanHead).
		Return(&workflows.ScanSessionInfo{ID: sessionID, FromBlock: 1000, ToBlock: scanHead, CursorBlock: 1000}, nil)
	exec.EXPECT().FetchEthereumOwnerWindow(gomock.Any(), addr, uint64(1000), scanHead).Return(nil, nil)
	exec.EXPECT().PersistEthereumScanWindow(gomock.Any(), sessionID, gomock.Nil(), uint64(1000), scanHead).Return(nil)
	exec.EXPECT().ReplayEthereumScanSession(gomock.Any(), addr, sessionID).Return(0, nil)
	exec.EXPECT().GetPendingScanTokens(gomock.Any(), sessionID).Return(nil, nil)
	exec.EXPECT().UpdateIndexingBlockRangeForAddress(gomock.Any(), addr, chainID, uint64(1000), scanHead).Return(nil)
	exec.EXPECT().DeleteEthereumScanSession(gomock.Any(), sessionID).Return(nil)
	// Second pass: watermark reaches the scan head, so the 64 blocks inside the
	// lag window are deliberately NOT scanned now — no new session.
	exec.EXPECT().GetEthereumScanSession(gomock.Any(), addr, chainID).Return(nil, nil)
	exec.EXPECT().GetIndexingBlockRangeForAddress(gomock.Any(), addr, chainID).
		Return(&workflows.BlockRangeResult{MinBlock: 1000, MaxBlock: scanHead}, nil)
	exec.EXPECT().GetLatestEthereumBlock(gomock.Any()).Return(latest, nil)

	require.NoError(t, wf.IndexEthereumTokenOwner(ctx, addr, nil))
}

// TestIndexEthereumTokenOwner_ForwardSweepStopsAtScanHead pins the incremental
// case: with a watermark already up to a previous scan head, the next forward
// session covers [max+1, newLatest−lag] only. Blocks inside the lag window are
// left for a later sweep rather than scanned and checkpointed while reorgable.
func TestIndexEthereumTokenOwner_ForwardSweepStopsAtScanHead(t *testing.T) {
	t.Parallel()
	cfg := ownerCfg()
	cfg.EthereumScanHeadLagBlocks = 64
	d := newOwnerWf(t, cfg)
	defer d.Ctrl.Finish()
	ctx, exec, wf := d.Ctx, d.Exec, d.Wf
	addr := "0xHeadLagFwd000000000000000000000000000000"
	chainID := domain.ChainEthereumMainnet
	const latest = uint64(6000)
	const scanHead = latest - 64 // 5936
	const sessionID int64 = 62

	exec.EXPECT().EnsureWatchedAddressExists(gomock.Any(), addr, chainID, gomock.Any()).Return(nil)
	exec.EXPECT().GetEthereumScanSession(gomock.Any(), addr, chainID).Return(nil, nil).Times(2)
	gomock.InOrder(
		exec.EXPECT().GetIndexingBlockRangeForAddress(gomock.Any(), addr, chainID).
			Return(&workflows.BlockRangeResult{MinBlock: 1000, MaxBlock: 4936}, nil),
		exec.EXPECT().CreateEthereumScanSession(gomock.Any(), addr, chainID, uint64(4937), scanHead).
			Return(&workflows.ScanSessionInfo{ID: sessionID, FromBlock: 4937, ToBlock: scanHead, CursorBlock: 4937}, nil),
		exec.EXPECT().FetchEthereumOwnerWindow(gomock.Any(), addr, uint64(4937), scanHead).Return(nil, nil),
		exec.EXPECT().PersistEthereumScanWindow(gomock.Any(), sessionID, gomock.Nil(), uint64(4937), scanHead).Return(nil),
		exec.EXPECT().ReplayEthereumScanSession(gomock.Any(), addr, sessionID).Return(0, nil),
		exec.EXPECT().GetPendingScanTokens(gomock.Any(), sessionID).Return(nil, nil),
		exec.EXPECT().GetIndexingBlockRangeForAddress(gomock.Any(), addr, chainID).
			Return(&workflows.BlockRangeResult{MinBlock: 1000, MaxBlock: 4936}, nil),
		exec.EXPECT().UpdateIndexingBlockRangeForAddress(gomock.Any(), addr, chainID, uint64(1000), scanHead).Return(nil),
		exec.EXPECT().DeleteEthereumScanSession(gomock.Any(), sessionID).Return(nil),
		exec.EXPECT().GetIndexingBlockRangeForAddress(gomock.Any(), addr, chainID).
			Return(&workflows.BlockRangeResult{MinBlock: 1000, MaxBlock: scanHead}, nil),
	)
	exec.EXPECT().GetLatestEthereumBlock(gomock.Any()).Return(latest, nil).Times(2)

	require.NoError(t, wf.IndexEthereumTokenOwner(ctx, addr, nil))
}

// TestIndexEthereumTokenOwner_YoungChainBelowSweepStartScansNothing pins the
// saturating edge: when latest−lag is below the sweep start (a very young chain
// or a misconfigured sweep start), no session is created rather than one with an
// inverted or wrapped-around range. The strict mock has no
// CreateEthereumScanSession expectation.
func TestIndexEthereumTokenOwner_YoungChainBelowSweepStartScansNothing(t *testing.T) {
	t.Parallel()
	cfg := ownerCfg()
	cfg.EthereumScanHeadLagBlocks = 64
	d := newOwnerWf(t, cfg)
	defer d.Ctrl.Finish()
	ctx, exec, wf := d.Ctx, d.Exec, d.Wf
	addr := "0xYoungChain00000000000000000000000000000"
	chainID := domain.ChainEthereumMainnet

	exec.EXPECT().EnsureWatchedAddressExists(gomock.Any(), addr, chainID, gomock.Any()).Return(nil)
	exec.EXPECT().GetEthereumScanSession(gomock.Any(), addr, chainID).Return(nil, nil)
	exec.EXPECT().GetIndexingBlockRangeForAddress(gomock.Any(), addr, chainID).
		Return(&workflows.BlockRangeResult{MinBlock: 0, MaxBlock: 0}, nil)
	// latest 1040 − lag 64 = 976 < sweepStart 1000 → nothing confirmed to scan.
	exec.EXPECT().GetLatestEthereumBlock(gomock.Any()).Return(uint64(1040), nil)

	require.NoError(t, wf.IndexEthereumTokenOwner(ctx, addr, nil))
}

// TestScanHeadBlock pins the saturating subtraction used for the scan head.
func TestScanHeadBlock(t *testing.T) {
	t.Parallel()
	require.Equal(t, uint64(4936), workflows.ScanHeadBlockForTest(5000, 64))
	require.Equal(t, uint64(5000), workflows.ScanHeadBlockForTest(5000, 0), "zero lag disables the margin")
	require.Equal(t, uint64(0), workflows.ScanHeadBlockForTest(10, 64), "must saturate at 0, never wrap uint64")
	require.Equal(t, uint64(0), workflows.ScanHeadBlockForTest(64, 64))
}
