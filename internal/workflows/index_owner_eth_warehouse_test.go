package workflows_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/feral-file/ff-indexer-v2/internal/domain"
	"github.com/feral-file/ff-indexer-v2/internal/workflows"
)

// TestSplitScanWindows pins the warehouse-aware partition: warehouse-sized
// windows up to min(head, toBlock), vendor-sized windows above, contiguous,
// covering exactly [cursor, toBlock].
func TestSplitScanWindows(t *testing.T) {
	t.Parallel()

	t.Run("head inside the range splits at the head", func(t *testing.T) {
		t.Parallel()
		ws := workflows.SplitScanWindowsForTest(0, 2_500_000, 2_050_000, 1_000_000, 10_000)
		require.Equal(t, [][2]uint64{
			{0, 999_999}, {1_000_000, 1_999_999}, {2_000_000, 2_050_000}, // warehouse
			{2_050_001, 2_060_000}, {2_060_001, 2_070_000}, // vendor, cap-sized …
		}, ws[:5])
		require.Equal(t, [2]uint64{2_490_001, 2_500_000}, ws[len(ws)-1])
		require.Len(t, ws, 3+45)
	})
	t.Run("head above the range is all warehouse windows", func(t *testing.T) {
		t.Parallel()
		ws := workflows.SplitScanWindowsForTest(1_000, 2_500_000, 9_000_000, 1_000_000, 10_000)
		require.Equal(t, [][2]uint64{{1_000, 1_000_999}, {1_001_000, 2_000_999}, {2_001_000, 2_500_000}}, ws)
	})
	t.Run("cursor above the head is all vendor windows", func(t *testing.T) {
		t.Parallel()
		ws := workflows.SplitScanWindowsForTest(5_000, 25_000, 4_999, 1_000_000, 10_000)
		require.Equal(t, [][2]uint64{{5_000, 14_999}, {15_000, 24_999}, {25_000, 25_000}}, ws)
	})
	t.Run("head exactly at toBlock leaves no vendor window", func(t *testing.T) {
		t.Parallel()
		ws := workflows.SplitScanWindowsForTest(0, 100, 100, 1_000_000, 10)
		require.Equal(t, [][2]uint64{{0, 100}}, ws)
	})
	t.Run("no overflow when the head is uint64 max", func(t *testing.T) {
		t.Parallel()
		top := ^uint64(0)
		ws := workflows.SplitScanWindowsForTest(top-5, top, top, 1_000, 10)
		require.Equal(t, [][2]uint64{{top - 5, top}}, ws)
	})
}

// TestIndexEthereumTokenOwner_WarehouseWindowsPlannedFromHead pins the window
// loop's use of the warehouse head: with a warehouse window size configured
// and a head inside the session range, the scan fetches warehouse-sized
// windows up to the head and cap-sized windows above it — the strict mock
// would fail on any other window shape — and still commits in cursor order.
func TestIndexEthereumTokenOwner_WarehouseWindowsPlannedFromHead(t *testing.T) {
	t.Parallel()
	cfg := ownerCfg()
	cfg.EthereumScanWindowBlocks = 1000
	cfg.EthereumWarehouseScanWindowBlocks = 5000
	d := newOwnerWf(t, cfg)
	defer d.Ctrl.Finish()
	ctx, exec, wf := d.Ctx, d.Exec, d.Wf
	addr := "0xWarehouse0000000000000000000000000000000"
	chainID := domain.ChainEthereumMainnet
	const sessionID int64 = 41

	exec.EXPECT().EnsureWatchedAddressExists(gomock.Any(), addr, chainID, gomock.Any()).Return(nil)
	exec.EXPECT().GetEthereumScanSession(gomock.Any(), addr, chainID).Return(&workflows.ScanSessionInfo{
		ID: sessionID, FromBlock: 1000, ToBlock: 12_999, CursorBlock: 1000,
	}, nil)
	// Warehouse head 10,999: [1000, 10999] in 5000-block windows, [11000, 12999] in 1000-block windows.
	exec.EXPECT().EthereumLogWarehouseHead(gomock.Any()).Return(uint64(10_999), true)
	exec.EXPECT().FetchEthereumOwnerWindow(gomock.Any(), addr, uint64(1000), uint64(5999)).Return(nil, nil)
	exec.EXPECT().FetchEthereumOwnerWindow(gomock.Any(), addr, uint64(6000), uint64(10_999)).Return(nil, nil)
	exec.EXPECT().FetchEthereumOwnerWindow(gomock.Any(), addr, uint64(11_000), uint64(11_999)).Return(nil, nil)
	exec.EXPECT().FetchEthereumOwnerWindow(gomock.Any(), addr, uint64(12_000), uint64(12_999)).Return(nil, nil)
	gomock.InOrder(
		exec.EXPECT().PersistEthereumScanWindow(gomock.Any(), sessionID, gomock.Nil(), uint64(1000), uint64(5999)).Return(nil),
		exec.EXPECT().PersistEthereumScanWindow(gomock.Any(), sessionID, gomock.Nil(), uint64(6000), uint64(10_999)).Return(nil),
		exec.EXPECT().PersistEthereumScanWindow(gomock.Any(), sessionID, gomock.Nil(), uint64(11_000), uint64(11_999)).Return(nil),
		exec.EXPECT().PersistEthereumScanWindow(gomock.Any(), sessionID, gomock.Nil(), uint64(12_000), uint64(12_999)).Return(nil),
	)
	exec.EXPECT().ReplayEthereumScanSession(gomock.Any(), addr, sessionID).Return(0, nil)
	exec.EXPECT().GetPendingScanTokens(gomock.Any(), sessionID).Return(nil, nil)
	exec.EXPECT().GetIndexingBlockRangeForAddress(gomock.Any(), addr, chainID).
		Return(&workflows.BlockRangeResult{MinBlock: 0, MaxBlock: 0}, nil)
	exec.EXPECT().UpdateIndexingBlockRangeForAddress(gomock.Any(), addr, chainID, uint64(1000), uint64(12_999)).Return(nil)
	exec.EXPECT().DeleteEthereumScanSession(gomock.Any(), sessionID).Return(nil)
	exec.EXPECT().GetEthereumScanSession(gomock.Any(), addr, chainID).Return(nil, nil)
	exec.EXPECT().GetIndexingBlockRangeForAddress(gomock.Any(), addr, chainID).
		Return(&workflows.BlockRangeResult{MinBlock: 1000, MaxBlock: 12_999}, nil)
	exec.EXPECT().GetLatestEthereumBlock(gomock.Any()).Return(uint64(12_999), nil)

	require.NoError(t, wf.IndexEthereumTokenOwner(ctx, addr, nil))
}

// TestIndexEthereumTokenOwner_WarehouseUnavailableKeepsVendorWindows pins the
// degraded path: the warehouse window size is configured but the head is
// unavailable at scan start, so the plan is the plain cap-sized partition.
func TestIndexEthereumTokenOwner_WarehouseUnavailableKeepsVendorWindows(t *testing.T) {
	t.Parallel()
	cfg := ownerCfg()
	cfg.EthereumScanWindowBlocks = 1000
	cfg.EthereumWarehouseScanWindowBlocks = 5000
	d := newOwnerWf(t, cfg)
	defer d.Ctrl.Finish()
	ctx, exec, wf := d.Ctx, d.Exec, d.Wf
	addr := "0xNoWarehouse00000000000000000000000000000"
	chainID := domain.ChainEthereumMainnet
	const sessionID int64 = 42

	exec.EXPECT().EnsureWatchedAddressExists(gomock.Any(), addr, chainID, gomock.Any()).Return(nil)
	exec.EXPECT().GetEthereumScanSession(gomock.Any(), addr, chainID).Return(&workflows.ScanSessionInfo{
		ID: sessionID, FromBlock: 1000, ToBlock: 2999, CursorBlock: 1000,
	}, nil)
	exec.EXPECT().EthereumLogWarehouseHead(gomock.Any()).Return(uint64(0), false)
	exec.EXPECT().FetchEthereumOwnerWindow(gomock.Any(), addr, uint64(1000), uint64(1999)).Return(nil, nil)
	exec.EXPECT().FetchEthereumOwnerWindow(gomock.Any(), addr, uint64(2000), uint64(2999)).Return(nil, nil)
	gomock.InOrder(
		exec.EXPECT().PersistEthereumScanWindow(gomock.Any(), sessionID, gomock.Nil(), uint64(1000), uint64(1999)).Return(nil),
		exec.EXPECT().PersistEthereumScanWindow(gomock.Any(), sessionID, gomock.Nil(), uint64(2000), uint64(2999)).Return(nil),
	)
	exec.EXPECT().ReplayEthereumScanSession(gomock.Any(), addr, sessionID).Return(0, nil)
	exec.EXPECT().GetPendingScanTokens(gomock.Any(), sessionID).Return(nil, nil)
	exec.EXPECT().GetIndexingBlockRangeForAddress(gomock.Any(), addr, chainID).
		Return(&workflows.BlockRangeResult{MinBlock: 0, MaxBlock: 0}, nil)
	exec.EXPECT().UpdateIndexingBlockRangeForAddress(gomock.Any(), addr, chainID, uint64(1000), uint64(2999)).Return(nil)
	exec.EXPECT().DeleteEthereumScanSession(gomock.Any(), sessionID).Return(nil)
	exec.EXPECT().GetEthereumScanSession(gomock.Any(), addr, chainID).Return(nil, nil)
	exec.EXPECT().GetIndexingBlockRangeForAddress(gomock.Any(), addr, chainID).
		Return(&workflows.BlockRangeResult{MinBlock: 1000, MaxBlock: 2999}, nil)
	exec.EXPECT().GetLatestEthereumBlock(gomock.Any()).Return(uint64(2999), nil)

	require.NoError(t, wf.IndexEthereumTokenOwner(ctx, addr, nil))
}
