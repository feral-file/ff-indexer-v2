package helpers

import (
	"context"
	"math/big"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/core/types"
	"go.uber.org/zap"

	ethadapter "github.com/feral-file/ff-indexer-v2/internal/adapter"
	"github.com/feral-file/ff-indexer-v2/internal/logger"
)

// warehouseLeg serves the historical part of a log walk from the log
// warehouse: blocks [from, min(to, head)] in one query, where head is the
// warehouse head at the time of the call. It returns the logs and the first
// block the vendor walk must still cover (to+1 when the warehouse covered the
// whole range, so the vendor walk becomes a no-op).
//
// Reason: this is the routing split of ff-indexer-v2#130 Phase 2. Every
// history query the indexer issues — owner scans, provenance, replays, the
// ingestion catch-up — goes through FilterLogsWithPagination, so routing here
// moves all of it off the metered vendor without touching a call site. The
// warehouse has no span cap, so one call replaces the ~2,600-window vendor
// walk of a genesis-to-head query.
//
// Trade-offs: the warehouse head is read on every call (one cheap local
// eth_blockNumber) rather than cached, so the split is always exact and a
// per-block ingestion fetch above the head costs one local call, not a refused
// eth_getLogs. Any warehouse failure — a scope refusal (the range is below
// coverage, the filter names a foreign signature, the warehouse is under
// maintenance), a transport error, or the per-request timeout — falls through
// to the vendor for the whole range at once, logged at WARN: the agreed policy
// is that a warehouse outage degrades to pre-warehouse cost (still bounded by
// the credit guards), never to a stalled scan. Only a canceled or expired
// caller context is returned as an error.
//
// Constraints: warehouse calls do not count against PaginationGuards.CallBudget
// (a vendor cost backstop). The returned logs are in (block, log index) order,
// as the warehouse guarantees and as the vendor leg appended after them
// preserves, so no re-sort is needed.
func (h *PaginationHelper) warehouseLeg(ctx context.Context, query ethereum.FilterQuery, from, to uint64) ([]types.Log, uint64, error) {
	head, err := h.guards.LogWarehouse.Head(ctx)
	if err != nil {
		return h.fallThrough(ctx, from, to, "head lookup", err)
	}
	if from > head {
		// Nothing to serve: the range sits entirely above the warehouse head
		// (the steady-state per-block ingestion case). Not logged — it is the
		// normal path for every tip block.
		return nil, from, nil
	}
	served := min(to, head)
	logs, err := h.warehouseFetch(ctx, query, from, served)
	if err != nil {
		return h.fallThrough(ctx, from, to, "eth_getLogs", err)
	}
	logger.DebugCtx(ctx, "Log range served by the warehouse",
		zap.Uint64("fromBlock", from), zap.Uint64("toBlock", served),
		zap.Uint64("warehouseHead", head), zap.Int("logs", len(logs)))
	return logs, served + 1, nil
}

// fallThrough is the single exit for a failed warehouse leg: the whole range
// goes to the vendor (next = from, no logs) unless the caller's context is
// done, in which case that error is returned instead of a vendor walk that
// would fail the same way.
func (h *PaginationHelper) fallThrough(ctx context.Context, from, to uint64, stage string, err error) ([]types.Log, uint64, error) {
	if ctx.Err() != nil {
		return nil, from, ctx.Err()
	}
	logger.WarnCtx(ctx, "Log warehouse unavailable for range, falling through to the vendor",
		zap.String("stage", stage),
		zap.Bool("outOfScope", ethadapter.IsOutOfScope(err)),
		zap.Uint64("fromBlock", from), zap.Uint64("toBlock", to),
		zap.Error(err))
	return nil, from, nil
}

// warehouseFetch runs one warehouse eth_getLogs for [from, to], bisecting the
// range when the warehouse reports its result cap ("query returned more than
// N results", recognized by IsTooManyResultsError) until each half fits. A
// single block over the cap is returned as an error (falls through to the
// vendor, whose walk has the receipts path for dense blocks). There is no
// sleep between halves: unlike a vendor's rate limit, the warehouse cap is a
// response-size bound, and the halves are independent local queries.
func (h *PaginationHelper) warehouseFetch(ctx context.Context, query ethereum.FilterQuery, from, to uint64) ([]types.Log, error) {
	rangeQuery := query
	rangeQuery.FromBlock = new(big.Int).SetUint64(from)
	rangeQuery.ToBlock = new(big.Int).SetUint64(to)
	logs, err := h.guards.LogWarehouse.FilterLogs(ctx, rangeQuery)
	if err == nil {
		return logs, nil
	}
	if from == to || !IsTooManyResultsError(err) {
		return nil, err
	}
	mid := from + (to-from)/2
	left, err := h.warehouseFetch(ctx, query, from, mid)
	if err != nil {
		return nil, err
	}
	right, err := h.warehouseFetch(ctx, query, mid+1, to)
	if err != nil {
		return nil, err
	}
	return append(left, right...), nil
}
