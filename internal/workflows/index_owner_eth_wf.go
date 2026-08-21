package workflows

import (
	"context"
	"fmt"
	"sync"
	"time"

	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"

	"github.com/feral-file/ff-indexer-v2/internal/domain"
	"github.com/feral-file/ff-indexer-v2/internal/logger"
	"github.com/feral-file/ff-indexer-v2/internal/store/schema"
)

// scanProgressLogEvery is the window-loop heartbeat cadence: a full cap-seeded
// mainnet scan (~2,500 windows at a 10k span cap) logs ~10 times.
const scanProgressLogEvery = 250

// defaultScanWindowBlocks bounds a window when no span cap is configured
// (self-hosted node): checkpoints stay frequent while the pagination helper's
// adaptive halving absorbs dense windows internally.
const defaultScanWindowBlocks = 1_000_000

// interChunkDelay throttles consecutive token chunks to stay under third-party
// service rate limits (same pacing the Tezos owner flow uses).
const interChunkDelay = 2 * time.Second

// IndexEthereumTokenOwner indexes all tokens held by an Ethereum address via
// checkpointed scan sessions (docs/address_scan_sessions.md).
//
// Reason: the previous implementation ran the whole owner discovery as one
// atomic in-memory log walk — any mid-scan failure discarded ~7,500 already-paid
// eth_getLogs calls, and daily-quota pauses re-scanned the entire un-indexed
// remainder because the discovered token list was never persisted. Sessions
// invert the loop: progress commits to Postgres per window, the replayed token
// list is durable, and quota pauses resume with zero re-scan RPC.
//
// Constraints: sessions are processed one at a time per address — resume any
// in-flight session first (it captures a range decided in an earlier run), then
// derive the next un-scanned gap from the watermark until none remain. The
// watermark only advances when a session completes, so it strictly means
// "fully indexed range".
func (w *coreWorkflows) IndexEthereumTokenOwner(ctx context.Context, address string, jobID *int64) error {
	chainID := w.config.EthereumChainID
	logger.InfoCtx(ctx, "Starting Ethereum token owner indexing",
		zap.String("address", address),
		zap.Uint64("startBlock", w.config.EthereumTokenSweepStartBlock),
	)

	if err := w.executor.EnsureWatchedAddressExists(ctx, address, chainID, w.config.BudgetedIndexingDefaultDailyQuota); err != nil {
		logger.ErrorCtx(ctx, fmt.Errorf("failed to ensure watched address exists"),
			zap.Error(err), zap.String("address", address), zap.String("chainID", string(chainID)))
		return err
	}

	for {
		session, err := w.executor.GetEthereumScanSession(ctx, address, chainID)
		if err != nil {
			return err
		}
		if session == nil {
			session, err = w.nextEthereumScanSession(ctx, address, chainID)
			if err != nil {
				return err
			}
			if session == nil {
				break // whole [sweep start, latest] range scanned and indexed
			}
		}

		if err := w.runEthereumScanSession(ctx, address, chainID, session, jobID); err != nil {
			return err
		}
	}

	logger.InfoCtx(ctx, "Ethereum token owner indexing completed", zap.String("address", address))
	return nil
}

// nextEthereumScanSession derives the next un-scanned range from the stored
// watermark and creates a session for it: the backward gap (history below the
// watermark) first, then the forward gap (new blocks above it). Returns nil
// when the whole [sweep start, latest] range is covered. First run is the
// backward case with an empty watermark: [sweep start, latest].
func (w *coreWorkflows) nextEthereumScanSession(ctx context.Context, address string, chainID domain.Chain) (*ScanSessionInfo, error) {
	rangeResult, err := w.executor.GetIndexingBlockRangeForAddress(ctx, address, chainID)
	if err != nil {
		return nil, fmt.Errorf("failed to get indexing block range: %w", err)
	}
	latestBlock, err := w.executor.GetLatestEthereumBlock(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get latest block: %w", err)
	}
	sweepStart := w.config.EthereumTokenSweepStartBlock

	var fromBlock, toBlock uint64
	switch {
	case rangeResult.MinBlock == 0 && rangeResult.MaxBlock == 0:
		fromBlock, toBlock = sweepStart, latestBlock
	case rangeResult.MinBlock > sweepStart:
		fromBlock, toBlock = sweepStart, rangeResult.MinBlock-1
	case latestBlock > rangeResult.MaxBlock:
		fromBlock, toBlock = rangeResult.MaxBlock+1, latestBlock
	default:
		return nil, nil
	}

	logger.InfoCtx(ctx, "Creating owner scan session",
		zap.String("address", address),
		zap.Uint64("fromBlock", fromBlock),
		zap.Uint64("toBlock", toBlock),
	)
	return w.executor.CreateEthereumScanSession(ctx, address, chainID, fromBlock, toBlock)
}

// runEthereumScanSession drives one session through its remaining lifecycle:
// window loop → replay → quota-paced indexing → watermark merge + deletion.
// Each stage resumes idempotently from persisted state, so the function is safe
// to re-enter after any failure or reschedule.
func (w *coreWorkflows) runEthereumScanSession(
	ctx context.Context,
	address string,
	chainID domain.Chain,
	session *ScanSessionInfo,
	jobID *int64,
) error {
	if !session.Replayed {
		if err := w.scanEthereumSessionWindows(ctx, address, session); err != nil {
			return err
		}
		tokenCount, err := w.executor.ReplayEthereumScanSession(ctx, address, session.ID)
		if err != nil {
			return fmt.Errorf("failed to replay scan session: %w", err)
		}
		logger.InfoCtx(ctx, "Owner scan replayed",
			zap.String("address", address),
			zap.Int64("sessionID", session.ID),
			zap.Int("tokenCount", tokenCount),
		)
	}

	if err := w.indexEthereumScanTokens(ctx, address, chainID, session.ID, jobID); err != nil {
		return err
	}

	return w.completeEthereumScanSession(ctx, address, chainID, session)
}

// scanWindow is one fetch unit of the owner scan: a contiguous block range and
// its position in the session's window sequence.
type scanWindow struct {
	index     int
	fromBlock uint64
	toBlock   uint64
}

// fetchedWindow pairs a window with its fetched rows for the ordered committer.
type fetchedWindow struct {
	window scanWindow
	rows   []schema.AddressScanLog
}

// scanWindows splits [cursor, toBlock] into consecutive windows of windowBlocks.
func scanWindows(cursor, toBlock, windowBlocks uint64) []scanWindow {
	var windows []scanWindow
	for from := cursor; from <= toBlock; {
		end := from + windowBlocks - 1
		if end > toBlock || end < from { // < from: uint64 overflow near max
			end = toBlock
		}
		windows = append(windows, scanWindow{index: len(windows), fromBlock: from, toBlock: end})
		if end == toBlock {
			break
		}
		from = end + 1
	}
	return windows
}

// scanEthereumSessionWindows runs the window loop from the session's cursor to
// its target block, fetching up to EthereumScanWindowConcurrency windows at a
// time while committing them to the checkpoint strictly in order.
//
// Reason: the scan is purely RPC-latency-bound — one provider round-trip per
// window (~0.9s measured), windows independent of each other — so a sequential
// loop spends ~2,000 round-trips back to back (~32 min for a mainnet history).
// Fetching K windows concurrently divides that wall-clock by ~K at identical
// total credit cost; only the request rate rises, which is what the knob sizes.
//
// Constraints: the cursor is a contiguous-prefix marker, so persistence stays
// sequential — a reorder buffer holds windows that finish early until every
// earlier window has committed. Committing N+1 before N would let a crash in
// between leave a gap that resume silently skips. The first failure cancels
// every in-flight fetch via the group context; fetched-but-uncommitted windows
// are simply dropped (fetch has no side effects) and re-fetched on resume.
func (w *coreWorkflows) scanEthereumSessionWindows(ctx context.Context, address string, session *ScanSessionInfo) error {
	windowBlocks := w.config.EthereumScanWindowBlocks
	if windowBlocks == 0 {
		windowBlocks = defaultScanWindowBlocks
	}
	concurrency := w.config.EthereumScanWindowConcurrency
	if concurrency < 1 {
		concurrency = 1
	}

	windows := scanWindows(session.CursorBlock, session.ToBlock, windowBlocks)
	if len(windows) == 0 {
		return nil
	}

	g, gctx := errgroup.WithContext(ctx)
	pending := make(chan scanWindow)
	fetched := make(chan fetchedWindow, concurrency)

	// Producer: hands out windows in order; stops as soon as anything fails.
	g.Go(func() error {
		defer close(pending)
		for _, win := range windows {
			select {
			case pending <- win:
			case <-gctx.Done():
				return gctx.Err()
			}
		}
		return nil
	})

	// Fetchers: the parallel, side-effect-free half of the pipeline.
	var fetchers sync.WaitGroup
	for range concurrency {
		fetchers.Add(1)
		g.Go(func() error {
			defer fetchers.Done()
			for win := range pending {
				rows, err := w.executor.FetchEthereumOwnerWindow(gctx, address, win.fromBlock, win.toBlock)
				if err != nil {
					return err
				}
				select {
				case fetched <- fetchedWindow{window: win, rows: rows}:
				case <-gctx.Done():
					return gctx.Err()
				}
			}
			return nil
		})
	}
	// Close the results channel only once every fetcher has exited, so the
	// committer's range loop terminates on success and the channel is never
	// written after close on failure.
	go func() {
		fetchers.Wait()
		close(fetched)
	}()

	// Committer: the single, strictly ordered half of the pipeline.
	g.Go(func() error {
		return w.commitScanWindowsInOrder(gctx, address, session, fetched, len(windows))
	})

	return g.Wait()
}

// commitScanWindowsInOrder drains fetched windows and persists them in index
// order, buffering any that arrive ahead of their turn. Returns once all
// expected windows are committed or the context is canceled.
func (w *coreWorkflows) commitScanWindowsInOrder(
	ctx context.Context,
	address string,
	session *ScanSessionInfo,
	fetched <-chan fetchedWindow,
	expected int,
) error {
	buffer := make(map[int]fetchedWindow)
	next := 0
	scanStart := time.Now()

	for next < expected {
		fw, ok := <-fetched
		if !ok {
			// Fetchers exited before delivering everything: only reachable on
			// failure, whose error the group surfaces ahead of this one.
			return ctx.Err()
		}
		buffer[fw.window.index] = fw

		for {
			ready, exists := buffer[next]
			if !exists {
				break
			}
			delete(buffer, next)

			if err := w.executor.PersistEthereumScanWindow(ctx, session.ID, ready.rows, ready.window.fromBlock, ready.window.toBlock); err != nil {
				return err
			}
			next++

			if next%scanProgressLogEvery == 0 {
				logger.InfoCtx(ctx, "Owner scan window progress",
					zap.String("address", address),
					zap.Int64("sessionID", session.ID),
					zap.Uint64("atBlock", ready.window.toBlock+1),
					zap.Uint64("targetBlock", session.ToBlock),
					zap.Int("windowsDone", next),
					zap.Int("windowsTotal", expected),
					zap.Duration("elapsed", time.Since(scanStart)),
				)
			}
		}
	}
	return nil
}

// indexEthereumScanTokens drains the session's pending token list in
// block-aligned chunks under the daily quota, stamping each chunk's tokens
// indexed as it lands. On quota exhaustion the job reschedules for the quota
// reset; the next run resumes from the remaining pending rows with no RPC.
func (w *coreWorkflows) indexEthereumScanTokens(
	ctx context.Context,
	address string,
	chainID domain.Chain,
	sessionID int64,
	jobID *int64,
) error {
	pending, err := w.executor.GetPendingScanTokens(ctx, sessionID)
	if err != nil {
		return err
	}
	if len(pending) == 0 {
		return nil
	}

	// The very first scan of an address (no watermark yet) gets the larger
	// first-batch target so a fresh wallet surfaces tokens quickly; resumed and
	// incremental sessions pace with the subsequent target throughout.
	firstTarget := w.config.EthereumOwnerSubsequentBatchTarget
	rangeResult, err := w.executor.GetIndexingBlockRangeForAddress(ctx, address, chainID)
	if err != nil {
		return err
	}
	if rangeResult.MinBlock == 0 && rangeResult.MaxBlock == 0 {
		firstTarget = w.config.EthereumOwnerFirstBatchTarget
	}

	sortTokensByBlock(pending, true)
	chunks := chunkTokensByTargetBlockAligned(pending, firstTarget, w.config.EthereumOwnerSubsequentBatchTarget)

	for i, chunk := range chunks {
		logger.InfoCtx(ctx, "Processing scan token chunk",
			zap.Int("chunkIndex", i+1),
			zap.Int("totalChunks", len(chunks)),
			zap.Int("tokenCount", len(chunk)),
		)

		chunkResult, err := w.processChunkWithQuota(ctx, address, chainID, chunk,
			fmt.Sprintf("scan chunk %d/%d", i+1, len(chunks)), jobID)
		if err != nil {
			return err
		}

		// Stamp exactly the tokens that were indexed — a quota-truncated chunk
		// indexes a block-aligned prefix, and stamping the requested chunk instead
		// would silently drop the remainder on resume.
		if cids := tokenCIDsOf(chunkResult.IndexedTokens); len(cids) > 0 {
			if err := w.executor.MarkScanTokensIndexed(ctx, sessionID, cids); err != nil {
				return fmt.Errorf("failed to mark scan tokens indexed: %w", err)
			}
		}

		if !chunkResult.Continue {
			logger.InfoCtx(ctx, "Quota exhausted during scan token indexing, will reschedule",
				zap.Int("chunksCompleted", i+1),
				zap.Int("totalChunks", len(chunks)),
			)
			return w.returnQuotaReschedule(ctx, jobID, chunkResult.QuotaResetAt)
		}

		// Pace between chunks only — the last chunk has nothing to throttle for.
		if i < len(chunks)-1 {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(interChunkDelay):
			}
		}
	}
	return nil
}

// completeEthereumScanSession merges the fully-indexed range into the address
// watermark and deletes the session.
//
// Constraints: watermark first, delete second — if the delete fails, the next
// run resumes an empty session and completes it again idempotently, whereas the
// reverse order could lose the range on a crash between the writes and trigger
// a full re-scan.
func (w *coreWorkflows) completeEthereumScanSession(ctx context.Context, address string, chainID domain.Chain, session *ScanSessionInfo) error {
	rangeResult, err := w.executor.GetIndexingBlockRangeForAddress(ctx, address, chainID)
	if err != nil {
		return err
	}

	newMin, newMax := session.FromBlock, session.ToBlock
	if rangeResult.MinBlock != 0 || rangeResult.MaxBlock != 0 {
		if rangeResult.MinBlock < newMin {
			newMin = rangeResult.MinBlock
		}
		if rangeResult.MaxBlock > newMax {
			newMax = rangeResult.MaxBlock
		}
	}
	if err := w.executor.UpdateIndexingBlockRangeForAddress(ctx, address, chainID, newMin, newMax); err != nil {
		return fmt.Errorf("failed to update block range: %w", err)
	}

	if err := w.executor.DeleteEthereumScanSession(ctx, session.ID); err != nil {
		return fmt.Errorf("failed to delete completed scan session: %w", err)
	}

	logger.InfoCtx(ctx, "Owner scan session completed",
		zap.String("address", address),
		zap.Int64("sessionID", session.ID),
		zap.Uint64("scannedFromBlock", session.FromBlock),
		zap.Uint64("scannedToBlock", session.ToBlock),
	)
	return nil
}

// tokenCIDsOf extracts the CIDs from a token slice.
func tokenCIDsOf(tokens []domain.TokenWithBlock) []domain.TokenCID {
	cids := make([]domain.TokenCID, len(tokens))
	for i, token := range tokens {
		cids[i] = token.TokenCID
	}
	return cids
}
