package tezos

import (
	"context"
	"fmt"

	"go.uber.org/zap"

	"github.com/feral-file/ff-indexer-v2/internal/blockchain"
	"github.com/feral-file/ff-indexer-v2/internal/logger"
)

const backfillPageSize = 1000

type backfillStats struct {
	transferCount int
	bigmapCount   int
	levelCount    int
}

// backfillHistoricLevels pages TzKT REST from fromLevel through toLevel (inclusive), merges
// transfers and token_metadata bigmap updates by level, and emits them through handler in
// ascending level order. Call after SignalR subscriptions are active; live batches for
// levels above toLevel should remain in streamCh until processStream starts.
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

	transfers, err := c.pageAllTransfersInRange(ctx, fromLevel, targetLevel)
	if err != nil {
		return 0, err
	}

	updates, err := c.pageAllBigMapUpdatesInRange(ctx, fromLevel, targetLevel)
	if err != nil {
		return 0, err
	}

	buffers := groupBackfillByLevel(transfers, updates)
	levels := sortedLevels(buffers)

	prevHandler := c.handler
	c.handler = handler
	defer func() {
		c.handler = prevHandler
	}()

	stats := backfillStats{}
	var highestEmitted uint64
	for _, level := range levels {
		buf := buffers[level]
		if err := c.emitLevel(ctx, buf); err != nil {
			return highestEmitted, fmt.Errorf("backfill emit level %d: %w", level, err)
		}
		stats.transferCount += len(buf.transfers)
		stats.bigmapCount += len(buf.bigmaps)
		stats.levelCount++
		if level > highestEmitted {
			highestEmitted = level
		}
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

func groupBackfillByLevel(transfers []TzKTTokenTransfer, updates []TzKTBigMapUpdate) map[uint64]*levelBuffer {
	buffers := make(map[uint64]*levelBuffer)

	for i := range transfers {
		t := transfers[i]
		level := t.Level
		if buffers[level] == nil {
			buffers[level] = &levelBuffer{level: level}
		}
		buffers[level].transfers = append(buffers[level].transfers, t)
	}

	for i := range updates {
		u := updates[i]
		if u.Path != "token_metadata" {
			continue
		}
		level := u.Level
		if buffers[level] == nil {
			buffers[level] = &levelBuffer{level: level}
		}
		buffers[level].bigmaps = append(buffers[level].bigmaps, u)
	}

	return buffers
}

func (c *tzSubscriber) pageAllTransfersInRange(ctx context.Context, fromLevel, toLevel uint64) ([]TzKTTokenTransfer, error) {
	var all []TzKTTokenTransfer
	offset := 0

	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}

		page, err := c.tzktClient.GetTokenTransfersByLevelRange(ctx, fromLevel, toLevel, backfillPageSize, offset)
		if err != nil {
			return nil, fmt.Errorf("page token transfers [%d, %d] at offset %d: %w", fromLevel, toLevel, offset, err)
		}

		all = append(all, page...)
		if len(page) < backfillPageSize {
			break
		}

		offset += len(page)
		logger.DebugCtx(ctx, "TzKT REST backfill transfers page",
			zap.String("chain", string(c.chainID)),
			zap.Uint64("from_level", fromLevel),
			zap.Uint64("to_level", toLevel),
			zap.Int("offset", offset),
			zap.Int("page_size", len(page)))
	}

	return all, nil
}

func (c *tzSubscriber) pageAllBigMapUpdatesInRange(ctx context.Context, fromLevel, toLevel uint64) ([]TzKTBigMapUpdate, error) {
	var all []TzKTBigMapUpdate
	offset := 0

	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}

		page, err := c.tzktClient.GetBigMapUpdatesByLevelRange(ctx, fromLevel, toLevel, backfillPageSize, offset)
		if err != nil {
			return nil, fmt.Errorf("page bigmap updates [%d, %d] at offset %d: %w", fromLevel, toLevel, offset, err)
		}

		all = append(all, page...)
		if len(page) < backfillPageSize {
			break
		}

		offset += len(page)
		logger.DebugCtx(ctx, "TzKT REST backfill bigmap page",
			zap.String("chain", string(c.chainID)),
			zap.Uint64("from_level", fromLevel),
			zap.Uint64("to_level", toLevel),
			zap.Int("offset", offset),
			zap.Int("page_size", len(page)))
	}

	return all, nil
}
