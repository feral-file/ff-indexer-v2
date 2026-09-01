package helpers

import (
	"context"
	"errors"
	"fmt"
	"math/big"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"
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
// maintenance), a transport error, the per-request timeout, or an
// unverified/refused warehouse — is handled by fallThrough per the configured
// policy (PaginationGuards.LogWarehouseVendorFallthrough): the query either
// fails with an explicit ERROR (strict, the default — a warehouse outage never
// re-issues the walk against the metered vendor) or falls through to the vendor
// for the whole range at once, logged at WARN, still bounded by the credit
// guards. Only a canceled or expired caller context is always returned as an
// error, never mistaken for either policy.
//
// Constraints: warehouse calls do not count against PaginationGuards.CallBudget
// (a vendor cost backstop). The returned logs are in (block, log index) order,
// as the warehouse guarantees and as the vendor leg appended after them
// preserves, so no re-sort is needed.
func (h *PaginationHelper) warehouseLeg(ctx context.Context, query ethereum.FilterQuery, from, to uint64, erc1155ID *common.Hash) ([]types.Log, uint64, error) {
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
	logs, err := h.warehouseFetch(ctx, query, from, served, erc1155ID)
	if err != nil {
		// A single block whose logs exceed the warehouse result cap is a
		// receipts signal, not an outage. In strict mode surface it verbatim so
		// FetchIngestionLogs's receipt path (client.go errors.As on it) pages
		// the block without a vendor walk; owner scans and other callers fail,
		// as they do for any block they cannot page — strict mode does not fall
		// through. When vendor fall-through is enabled, route it like any other
		// failure: the vendor's result cap is independent of the warehouse's, so
		// the vendor may serve the block, or reproduce the overflow for the
		// receipt path — preserving the pre-strict fall-through behavior for
		// availability-first deployments.
		var overflow *SingleBlockOverflowError
		if errors.As(err, &overflow) && !h.guards.LogWarehouseVendorFallthrough {
			return nil, from, err
		}
		return h.fallThrough(ctx, from, to, "eth_getLogs", err)
	}
	logger.DebugCtx(ctx, "Log range served by the warehouse",
		zap.Uint64("fromBlock", from), zap.Uint64("toBlock", served),
		zap.Uint64("warehouseHead", head), zap.Int("logs", len(logs)))
	return logs, served + 1, nil
}

// fallThrough is the single exit for a failed warehouse leg. A canceled or
// expired caller context is always returned as such — a vendor walk would fail
// the same way. Otherwise the outcome depends on the configured policy:
//
//   - strict (LogWarehouseVendorFallthrough false, the default): the query
//     fails with the warehouse error wrapped and logged at ERROR. The vendor is
//     not touched, so a warehouse outage cannot silently re-issue a
//     genesis-to-head walk against the metered vendor and burn credits. The
//     failure is loud and actionable, which is the point — the operator runs
//     the warehouse as the primary log source and wants an outage surfaced, not
//     absorbed. A scope refusal is failed here too (the flag gates every
//     failure by the operator's chosen policy): in normal operation the warehouse
//     covers every signature and range the indexer asks for below its head, so
//     a scope refusal signals a real coverage gap or maintenance that the
//     operator must see, not a routine hand-off.
//   - fall-through (LogWarehouseVendorFallthrough true): the whole range goes
//     to the vendor (next = from, no logs), logged at WARN. The credit guards
//     still bound that walk. This is the original "never stall" policy, for
//     deployments that prefer availability over cost.
func (h *PaginationHelper) fallThrough(ctx context.Context, from, to uint64, stage string, err error) ([]types.Log, uint64, error) {
	if ctx.Err() != nil {
		return nil, from, ctx.Err()
	}
	outOfScope := ethadapter.IsOutOfScope(err)
	if !h.guards.LogWarehouseVendorFallthrough {
		failErr := fmt.Errorf("log warehouse unavailable at %s for range [%d, %d] and vendor fall-through disabled: %w", stage, from, to, err)
		logger.ErrorCtx(ctx, failErr,
			zap.String("stage", stage),
			zap.Bool("outOfScope", outOfScope),
			zap.Uint64("fromBlock", from), zap.Uint64("toBlock", to))
		return nil, from, failErr
	}
	logger.WarnCtx(ctx, "Log warehouse unavailable for range, falling through to the vendor",
		zap.String("stage", stage),
		zap.Bool("outOfScope", outOfScope),
		zap.Uint64("fromBlock", from), zap.Uint64("toBlock", to),
		zap.Error(err))
	return nil, from, nil
}

// warehouseFetch runs one warehouse eth_getLogs for [from, to], bisecting the
// range when the warehouse reports its result cap ("query returned more than
// N results", recognized by IsTooManyResultsError) until each half fits. A
// single block over the cap is returned as a SingleBlockOverflowError — the
// same signal the vendor walk raises — so FetchIngestionLogs's receipt path
// (client.go) can recover a dense warehouse block; see warehouseLeg for how the
// strict and fall-through policies each route that error. There is no sleep
// between halves: unlike a vendor's rate limit, the warehouse cap is a
// response-size bound, and the halves are independent local queries.
func (h *PaginationHelper) warehouseFetch(ctx context.Context, query ethereum.FilterQuery, from, to uint64, erc1155ID *common.Hash) ([]types.Log, error) {
	rangeQuery := query
	rangeQuery.FromBlock = new(big.Int).SetUint64(from)
	rangeQuery.ToBlock = new(big.Int).SetUint64(to)
	logs, err := h.guards.LogWarehouse.FilterLogs(ctx, rangeQuery, erc1155ID)
	if err == nil {
		return logs, nil
	}
	if !IsTooManyResultsError(err) {
		return nil, err
	}
	if from == to {
		// One block over the warehouse result cap cannot be split further and
		// cannot be paged by any eth_getLogs; only block receipts can serve it.
		// Raise the same error the vendor walk raises so the receipt recovery
		// fires on the warehouse leg too.
		return nil, &SingleBlockOverflowError{Block: from, Err: err}
	}
	mid := from + (to-from)/2
	left, err := h.warehouseFetch(ctx, query, from, mid, erc1155ID)
	if err != nil {
		return nil, err
	}
	right, err := h.warehouseFetch(ctx, query, mid+1, to, erc1155ID)
	if err != nil {
		return nil, err
	}
	return append(left, right...), nil
}
