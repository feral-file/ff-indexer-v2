package workflows_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/feral-file/ff-indexer-v2/internal/domain"
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
	gomock.InOrder(
		exec.EXPECT().ScanEthereumOwnerWindow(gomock.Any(), addr, sessionID, uint64(3000), uint64(3999)).Return(nil),
		exec.EXPECT().ScanEthereumOwnerWindow(gomock.Any(), addr, sessionID, uint64(4000), uint64(4999)).Return(nil),
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
// NO ScanEthereumOwnerWindow or ReplayEthereumScanSession expectations, so any
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
		exec.EXPECT().ScanEthereumOwnerWindow(gomock.Any(), addr, int64(41), uint64(1000), uint64(1999)).Return(nil),
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
		exec.EXPECT().ScanEthereumOwnerWindow(gomock.Any(), addr, int64(42), uint64(4001), latest).Return(nil),
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
