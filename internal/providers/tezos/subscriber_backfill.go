package tezos

import (
	"context"
	"fmt"

	"go.uber.org/zap"

	"github.com/feral-file/ff-indexer-v2/internal/blockchain"
	"github.com/feral-file/ff-indexer-v2/internal/logger"
)

const (
	backfillPageSize      = 1000
	backfillEmitAllLevels = ^uint64(0)
)

type backfillStats struct {
	transferCount int
	bigmapCount   int
	levelCount    int
}

// backfillHistoricLevels pages TzKT REST from fromLevel through toLevel (inclusive), merges
// transfers and token_metadata bigmap updates by level, and emits them through handler in
// ascending level order. Pages are merged incrementally so memory stays bounded on large gaps.
//
// Call after SignalR subscriptions are active; live batches for levels above toLevel should
// remain in streamCh until processStream starts.
//
// Reason: TzKT hub subscriptions are live-only; REST backfill closes the resume gap after
// downtime. Boundary overlap with streamCh is tolerated via runner monotonic guards and job keys.
//
// Constraints: REST filters must stay aligned with SubscribeToTokenTransfers /
// SubscribeToBigMaps filters when those are narrowed in the future.
func (c *tzSubscriber) backfillHistoricLevels(
	ctx context.Context,
	fromLevel, toLevel uint64,
	handler blockchain.EventHandler,
) (uint64, error) {
	if handler == nil {
		return 0, fmt.Errorf("backfill handler is required")
	}

	targetLevel := toLevel
	if targetLevel == 0 {
		head, err := c.tzktClient.GetLatestBlock(ctx)
		if err != nil {
			return 0, fmt.Errorf("failed to get latest block for backfill: %w", err)
		}
		targetLevel = head
	}

	if fromLevel > targetLevel {
		logger.InfoCtx(ctx, "Skipping TzKT REST backfill; fromLevel is at or past target head",
			zap.String("chain", string(c.chainID)),
			zap.Uint64("from_level", fromLevel),
			zap.Uint64("to_level", targetLevel))
		return targetLevel, nil
	}

	logger.InfoCtx(ctx, "Starting TzKT REST backfill after SignalR subscribe",
		zap.String("chain", string(c.chainID)),
		zap.Uint64("from_level", fromLevel),
		zap.Uint64("to_level", targetLevel))

	prevHandler := c.handler
	c.handler = handler
	defer func() {
		c.handler = prevHandler
	}()

	stats := backfillStats{}
	highestEmitted, err := c.streamBackfillRange(ctx, fromLevel, targetLevel, &stats)
	if err != nil {
		return highestEmitted, err
	}

	logger.InfoCtx(ctx, "TzKT REST backfill completed",
		zap.String("chain", string(c.chainID)),
		zap.Uint64("from_level", fromLevel),
		zap.Uint64("to_level", targetLevel),
		zap.Uint64("highest_emitted", highestEmitted),
		zap.Int("levels", stats.levelCount),
		zap.Int("transfers", stats.transferCount),
		zap.Int("bigmaps", stats.bigmapCount))

	if highestEmitted == 0 {
		return targetLevel, nil
	}

	return highestEmitted, nil
}

// streamBackfillRange incrementally pages both REST feeds, merges by level, and emits complete
// levels in ascending order without materializing the full gap in memory.
func (c *tzSubscriber) streamBackfillRange(
	ctx context.Context,
	fromLevel, toLevel uint64,
	stats *backfillStats,
) (uint64, error) {
	transfers := newTransferBackfillIter(c, ctx, fromLevel, toLevel)
	bigmaps := newBigmapBackfillIter(c, ctx, fromLevel, toLevel)
	pending := make(map[uint64]*levelBuffer)

	var highestEmitted uint64
	for {
		if err := ctx.Err(); err != nil {
			return highestEmitted, err
		}

		tLevel, tOK, err := transfers.peekLevel()
		if err != nil {
			return highestEmitted, err
		}
		bLevel, bOK, err := bigmaps.peekLevel()
		if err != nil {
			return highestEmitted, err
		}

		if !tOK && !bOK {
			highest, err := c.flushBackfillPending(ctx, pending, backfillEmitAllLevels, stats)
			if err != nil {
				return highestEmitted, err
			}
			if highest > highestEmitted {
				highestEmitted = highest
			}
			break
		}

		switch {
		case tOK && (!bOK || tLevel <= bLevel):
			transfer, err := transfers.pop()
			if err != nil {
				return highestEmitted, err
			}
			appendTransferToBackfillPending(pending, transfer)
		case bOK:
			update, err := bigmaps.pop()
			if err != nil {
				return highestEmitted, err
			}
			appendBigmapToBackfillPending(pending, update)
		}

		tLevel, tOK, err = transfers.peekLevel()
		if err != nil {
			return highestEmitted, err
		}
		bLevel, bOK, err = bigmaps.peekLevel()
		if err != nil {
			return highestEmitted, err
		}

		cutoff := backfillEmitAllLevels
		if tOK || bOK {
			cutoff = backfillEmitCutoff(tLevel, tOK, bLevel, bOK)
		}
		highest, err := c.flushBackfillPending(ctx, pending, cutoff, stats)
		if err != nil {
			return highestEmitted, fmt.Errorf("backfill emit: %w", err)
		}
		if highest > highestEmitted {
			highestEmitted = highest
		}
	}

	return highestEmitted, nil
}

// backfillEmitCutoff returns the lowest level still present in either feed peek stream.
// Levels strictly below this cutoff are safe to emit.
func backfillEmitCutoff(tLevel uint64, tOK bool, bLevel uint64, bOK bool) uint64 {
	cutoff := backfillEmitAllLevels
	if tOK && tLevel < cutoff {
		cutoff = tLevel
	}
	if bOK && bLevel < cutoff {
		cutoff = bLevel
	}
	return cutoff
}

func appendTransferToBackfillPending(pending map[uint64]*levelBuffer, transfer TzKTTokenTransfer) {
	level := transfer.Level
	if pending[level] == nil {
		pending[level] = &levelBuffer{level: level}
	}
	pending[level].transfers = append(pending[level].transfers, transfer)
}

func appendBigmapToBackfillPending(pending map[uint64]*levelBuffer, update TzKTBigMapUpdate) {
	if update.Path != "token_metadata" {
		return
	}
	level := update.Level
	if pending[level] == nil {
		pending[level] = &levelBuffer{level: level}
	}
	pending[level].bigmaps = append(pending[level].bigmaps, update)
}

func (c *tzSubscriber) flushBackfillPending(
	ctx context.Context,
	pending map[uint64]*levelBuffer,
	cutoff uint64,
	stats *backfillStats,
) (uint64, error) {
	var highestEmitted uint64
	for _, level := range sortedLevels(pending) {
		if level >= cutoff {
			break
		}
		buf := pending[level]
		if err := c.emitLevel(ctx, buf); err != nil {
			return highestEmitted, fmt.Errorf("backfill emit level %d: %w", level, err)
		}
		stats.transferCount += len(buf.transfers)
		stats.bigmapCount += len(buf.bigmaps)
		stats.levelCount++
		if level > highestEmitted {
			highestEmitted = level
		}
		delete(pending, level)
	}
	return highestEmitted, nil
}

func groupBackfillByLevel(transfers []TzKTTokenTransfer, updates []TzKTBigMapUpdate) map[uint64]*levelBuffer {
	buffers := make(map[uint64]*levelBuffer)

	for i := range transfers {
		appendTransferToBackfillPending(buffers, transfers[i])
	}

	for i := range updates {
		appendBigmapToBackfillPending(buffers, updates[i])
	}

	return buffers
}

type transferBackfillIter struct {
	subscriber *tzSubscriber
	ctx        context.Context
	fromLevel  uint64
	toLevel    uint64
	offset     int
	page       []TzKTTokenTransfer
	idx        int
	exhausted  bool
}

func newTransferBackfillIter(subscriber *tzSubscriber, ctx context.Context, fromLevel, toLevel uint64) *transferBackfillIter {
	return &transferBackfillIter{
		subscriber: subscriber,
		ctx:        ctx,
		fromLevel:  fromLevel,
		toLevel:    toLevel,
	}
}

func (it *transferBackfillIter) peekLevel() (uint64, bool, error) {
	if err := it.ensureItem(); err != nil {
		return 0, false, err
	}
	if it.idx >= len(it.page) {
		return 0, false, nil
	}
	return it.page[it.idx].Level, true, nil
}

func (it *transferBackfillIter) pop() (TzKTTokenTransfer, error) {
	if err := it.ensureItem(); err != nil {
		return TzKTTokenTransfer{}, err
	}
	if it.idx >= len(it.page) {
		return TzKTTokenTransfer{}, fmt.Errorf("transfer backfill iterator exhausted")
	}
	transfer := it.page[it.idx]
	it.idx++
	return transfer, nil
}

func (it *transferBackfillIter) ensureItem() error {
	for it.idx >= len(it.page) {
		if it.exhausted {
			return nil
		}
		if err := it.ctx.Err(); err != nil {
			return err
		}

		page, err := it.subscriber.tzktClient.GetTokenTransfersByLevelRange(
			it.ctx, it.fromLevel, it.toLevel, backfillPageSize, it.offset,
		)
		if err != nil {
			return fmt.Errorf("page token transfers [%d, %d] at offset %d: %w", it.fromLevel, it.toLevel, it.offset, err)
		}

		it.offset += len(page)
		it.page = page
		it.idx = 0

		if len(page) == 0 {
			it.exhausted = true
			return nil
		}
		if len(page) < backfillPageSize {
			it.exhausted = true
		}

		logger.DebugCtx(it.ctx, "TzKT REST backfill transfers page",
			zap.String("chain", string(it.subscriber.chainID)),
			zap.Uint64("from_level", it.fromLevel),
			zap.Uint64("to_level", it.toLevel),
			zap.Int("offset", it.offset),
			zap.Int("page_size", len(page)))
	}
	return nil
}

type bigmapBackfillIter struct {
	subscriber *tzSubscriber
	ctx        context.Context
	fromLevel  uint64
	toLevel    uint64
	offset     int
	page       []TzKTBigMapUpdate
	idx        int
	exhausted  bool
}

func newBigmapBackfillIter(subscriber *tzSubscriber, ctx context.Context, fromLevel, toLevel uint64) *bigmapBackfillIter {
	return &bigmapBackfillIter{
		subscriber: subscriber,
		ctx:        ctx,
		fromLevel:  fromLevel,
		toLevel:    toLevel,
	}
}

func (it *bigmapBackfillIter) peekLevel() (uint64, bool, error) {
	if err := it.ensureItem(); err != nil {
		return 0, false, err
	}
	if it.idx >= len(it.page) {
		return 0, false, nil
	}
	return it.page[it.idx].Level, true, nil
}

func (it *bigmapBackfillIter) pop() (TzKTBigMapUpdate, error) {
	if err := it.ensureItem(); err != nil {
		return TzKTBigMapUpdate{}, err
	}
	if it.idx >= len(it.page) {
		return TzKTBigMapUpdate{}, fmt.Errorf("bigmap backfill iterator exhausted")
	}
	update := it.page[it.idx]
	it.idx++
	return update, nil
}

func (it *bigmapBackfillIter) ensureItem() error {
	for it.idx >= len(it.page) {
		if it.exhausted {
			return nil
		}
		if err := it.ctx.Err(); err != nil {
			return err
		}

		page, err := it.subscriber.tzktClient.GetBigMapUpdatesByLevelRange(
			it.ctx, it.fromLevel, it.toLevel, backfillPageSize, it.offset,
		)
		if err != nil {
			return fmt.Errorf("page bigmap updates [%d, %d] at offset %d: %w", it.fromLevel, it.toLevel, it.offset, err)
		}

		it.offset += len(page)
		it.page = page
		it.idx = 0

		if len(page) == 0 {
			it.exhausted = true
			return nil
		}
		if len(page) < backfillPageSize {
			it.exhausted = true
		}

		logger.DebugCtx(it.ctx, "TzKT REST backfill bigmap page",
			zap.String("chain", string(it.subscriber.chainID)),
			zap.Uint64("from_level", it.fromLevel),
			zap.Uint64("to_level", it.toLevel),
			zap.Int("offset", it.offset),
			zap.Int("page_size", len(page)))
	}
	return nil
}
