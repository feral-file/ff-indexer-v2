package workflows

// ScanWindowsForTest exposes the window partition to external tests as plain
// (from, to) pairs.
func ScanWindowsForTest(cursor, toBlock, windowBlocks uint64) [][2]uint64 {
	ws := scanWindows(cursor, toBlock, windowBlocks)
	out := make([][2]uint64, len(ws))
	for i, w := range ws {
		out[i] = [2]uint64{w.fromBlock, w.toBlock}
	}
	return out
}

// SplitScanWindowsForTest exposes the warehouse-aware partition to external
// tests as plain (from, to) pairs.
func SplitScanWindowsForTest(cursor, toBlock, head, warehouseBlocks, windowBlocks uint64) [][2]uint64 {
	ws := splitScanWindows(cursor, toBlock, head, warehouseBlocks, windowBlocks)
	out := make([][2]uint64, len(ws))
	for i, w := range ws {
		out[i] = [2]uint64{w.fromBlock, w.toBlock}
	}
	return out
}
