package helpers

import (
	"context"
	"errors"
	"fmt"
	"math/big"
	"time"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/core/types"
	"go.uber.org/zap"

	ethadapter "github.com/feral-file/ff-indexer-v2/internal/adapter"
	"github.com/feral-file/ff-indexer-v2/internal/block"
	"github.com/feral-file/ff-indexer-v2/internal/logger"
)

// SingleBlockOverflowError reports a block whose matching logs exceed the
// provider's per-query result cap: the walk halved down to one block and the
// provider still refused it. Callers that can obtain the block's logs another
// way (block receipts) errors.As on it; everyone else treats it as fatal.
type SingleBlockOverflowError struct {
	Block uint64
	Err   error
}

func (e *SingleBlockOverflowError) Error() string {
	return fmt.Sprintf("too many results in single block %d: %v", e.Block, e.Err)
}

func (e *SingleBlockOverflowError) Unwrap() error { return e.Err }

// ErrCallBudgetExhausted marks a pagination walk aborted by PaginationGuards.CallBudget.
// Callers can errors.Is on it to distinguish a cost guard from a provider failure.
var ErrCallBudgetExhausted = errors.New("pagination call budget exhausted")

// PaginationGuards bounds the RPC cost of a pagination walk against a
// credit-metered provider. The zero value disables both guards.
//
// Reason: on a provider that caps eth_getLogs by block span (Infura: toBlock-fromBlock
// <= 10000), a genesis-to-head walk is thousands of sequential calls at ~255 credits
// each. An unbounded walk can consume an entire daily credit quota; discovering the
// cap dynamically additionally pays a halving cascade of rejected calls per walk.
type PaginationGuards struct {
	// SpanCap is the provider's known eth_getLogs block-range cap, expressed as the
	// maximum accepted toBlock-fromBlock difference (10000 for Infura; Chainstack's
	// "10,000 blocks" is unverified — configure 9999 until a soak confirms). When set, step
	// sizing starts at the cap instead of probing down to it with rejected calls and
	// one-second sleeps. Dynamic discovery still applies on top, so a misconfigured
	// (too large) value degrades to the old cascade behavior instead of failing.
	SpanCap uint64
	// CallBudget is the maximum number of FilterLogs RPC calls one
	// FilterLogsWithPagination walk may issue. Exceeding it aborts the walk with
	// ErrCallBudgetExhausted instead of silently draining the provider quota. It is a
	// backstop, not a pace-setter: size it above ceil(chain head / SpanCap) so every
	// legitimate full-history walk fits (mainnet at a 10k cap needs ~2,600 calls).
	CallBudget int
}

// paceState is the walk-scoped progress heartbeat: long walks against a
// range-capped, rate-limited provider are legitimately slow (thousands of
// windows); without a heartbeat the only observable states are "running" and
// "dead at the deadline". It lives at the FilterLogsWithPagination level and is
// threaded through getLogsWithRetry because window completions must accumulate
// across outer windows — see the getLogsWithRetry doc.
type paceState struct {
	walkStart   time.Time
	target      uint64
	windowsDone int
}

// paceLogEvery is the heartbeat cadence in successful windows: a full
// cap-seeded mainnet walk (~2,600 windows at a 10k span cap) logs ~10 times.
const paceLogEvery = 250

// windowDone records one successfully fetched window and emits the heartbeat
// every paceLogEvery windows. atBlock is the next block the walk will fetch.
func (p *paceState) windowDone(ctx context.Context, atBlock, stepSize uint64) {
	p.windowsDone++
	if p.windowsDone%paceLogEvery != 0 {
		return
	}
	logger.InfoCtx(ctx, "Log pagination walk progress",
		zap.Uint64("atBlock", atBlock),
		zap.Uint64("targetBlock", p.target),
		zap.Uint64("stepSize", stepSize),
		zap.Int("windowsDone", p.windowsDone),
		zap.Duration("elapsed", time.Since(p.walkStart)),
	)
}

// PaginationHelper paginates eth_getLogs queries with adaptive step sizing and retry.
type PaginationHelper struct {
	ethClient     ethadapter.EthClient
	clock         ethadapter.Clock
	blockProvider block.BlockProvider
	guards        PaginationGuards
}

// NewPaginationHelper creates a helper for paginated log queries with no cost guards.
func NewPaginationHelper(
	ethClient ethadapter.EthClient,
	clock ethadapter.Clock,
	blockProvider block.BlockProvider,
) *PaginationHelper {
	return NewGuardedPaginationHelper(ethClient, clock, blockProvider, PaginationGuards{})
}

// NewGuardedPaginationHelper creates a helper whose walks are bounded by guards.
func NewGuardedPaginationHelper(
	ethClient ethadapter.EthClient,
	clock ethadapter.Clock,
	blockProvider block.BlockProvider,
	guards PaginationGuards,
) *PaginationHelper {
	return &PaginationHelper{
		ethClient:     ethClient,
		clock:         clock,
		blockProvider: blockProvider,
		guards:        guards,
	}
}

// noDeadlineTimeout bounds a pagination walk whose caller set no deadline.
// Sized for the worst legitimate case: a genesis-to-head walk against a
// provider that caps eth_getLogs at 10k blocks (~2,300 sequential calls at
// mainnet head ~23M blocks), while the owner-scan legs run concurrently and
// share the provider's rate limit — throttling backoffs (5s initial, per
// executeWithRetry) dominate wall-clock, and a measured prod scan was still
// walking at 30 minutes. This is a backstop against a wedged walk, not a
// pace-setter: each RPC call is already bounded by its own retry budget
// (~5 minutes), so a stuck walk still dies long before this fires. The
// original one-minute bound predates range-cap handling — it assumed
// 10M-block steps and a handful of calls, and killed every full-history
// owner scan on a capped provider before it could finish.
const noDeadlineTimeout = 4 * time.Hour

// FilterLogsWithPagination fetches logs across a block range using adaptive pagination.
func (h *PaginationHelper) FilterLogsWithPagination(ctx context.Context, query ethereum.FilterQuery) ([]types.Log, error) {
	timeoutCtx := ctx
	var cancel context.CancelFunc
	if _, hasDeadline := ctx.Deadline(); !hasDeadline {
		timeoutCtx, cancel = context.WithTimeout(ctx, noDeadlineTimeout)
		defer cancel()
	}

	if query.BlockHash != nil {
		return h.ethClient.FilterLogs(timeoutCtx, query)
	}

	var fromBlock, toBlock *big.Int
	if query.FromBlock != nil {
		fromBlock = query.FromBlock
	} else {
		fromBlock = big.NewInt(0)
	}

	if query.ToBlock != nil {
		toBlock = query.ToBlock
	} else {
		latestBlockNum, err := h.blockProvider.GetLatestBlock(timeoutCtx)
		if err != nil {
			return nil, fmt.Errorf("failed to get latest block: %w", err)
		}
		toBlock = new(big.Int).SetUint64(latestBlockNum)
	}

	var allLogs []types.Log
	currentFrom := new(big.Int).Set(fromBlock)
	stepSize := h.calculateStepSize(timeoutCtx, query)
	callsUsed := 0
	// Walk-scoped pace state: with a configured span cap the outer and inner
	// window sizes coincide, so every getLogsWithRetry call completes after ONE
	// window — a counter local to that function can never reach the heartbeat
	// threshold, silently killing the progress log the moment the guard is on.
	pace := paceState{walkStart: time.Now(), target: toBlock.Uint64()}

	for currentFrom.Cmp(toBlock) <= 0 {
		select {
		case <-timeoutCtx.Done():
			logger.WarnCtx(ctx, "Context deadline exceeded during log pagination, returning partial logs",
				zap.Int("partialLogsCount", len(allLogs)),
				zap.Uint64("processedUpToBlock", currentFrom.Uint64()-1),
				zap.Uint64("targetToBlock", toBlock.Uint64()),
			)
			return allLogs, timeoutCtx.Err()
		default:
		}

		// An outer window spans exactly stepSize blocks (from..from+stepSize-1),
		// matching the inner walk's window arithmetic. The former from+stepSize left
		// one extra block per window, which cost a second single-block RPC call per
		// window once the range cap was hoisted here — doubling the walk's call count.
		currentTo := new(big.Int).Add(currentFrom, new(big.Int).SetUint64(stepSize-1))
		if currentTo.Cmp(toBlock) > 0 {
			currentTo.Set(toBlock)
		}

		rangeQuery := query
		rangeQuery.FromBlock = new(big.Int).Set(currentFrom)
		rangeQuery.ToBlock = currentTo

		logs, cappedStep, err := h.getLogsWithRetry(timeoutCtx, rangeQuery, stepSize, &callsUsed, &pace)
		if err != nil {
			if timeoutCtx.Err() != nil {
				return allLogs, timeoutCtx.Err()
			}
			return nil, fmt.Errorf("failed to get logs for range %d-%d: %w", currentFrom.Uint64(), currentTo.Uint64(), err)
		}

		// A provider's block-range cap holds for the whole walk, not just the
		// window that discovered it. Without hoisting it here, every later
		// window re-probes at the original step and re-pays the halving
		// cascade (one rejected call plus a one-second sleep per halving).
		if cappedStep < stepSize {
			stepSize = cappedStep
		}

		allLogs = append(allLogs, logs...)
		currentFrom.SetUint64(currentTo.Uint64() + 1)
	}

	return allLogs, nil
}

// calculateStepSize picks the initial pagination step for a query.
//
// The per-query heuristics predate span-capped providers: they assume the only
// limit is result count, so denser event families get smaller steps. When the
// provider's span cap is configured (guards.SpanCap), it clamps every heuristic:
// a step above the cap is a guaranteed rejection plus a one-second sleep per
// halving, paid on every walk.
func (h *PaginationHelper) calculateStepSize(ctx context.Context, query ethereum.FilterQuery) uint64 {
	step := h.heuristicStepSize(ctx, query)
	if h.guards.SpanCap > 0 && step > h.guards.SpanCap+1 {
		// A step of N covers blocks from..from+N-1, i.e. a span of toBlock-fromBlock
		// = N-1, so the largest accepted step is SpanCap+1.
		step = h.guards.SpanCap + 1
	}
	return step
}

func (h *PaginationHelper) heuristicStepSize(ctx context.Context, query ethereum.FilterQuery) uint64 {
	const (
		defaultStepSize         = uint64(1_000_000)
		erc721TokenStepSize     = uint64(30_000_000)
		erc721OwnerStepSize     = uint64(10_000_000)
		erc1155OwnerStepSize    = uint64(10_000_000)
		erc1155ContractStepSize = uint64(5_000_000)
	)

	if len(query.Topics) == 0 {
		return defaultStepSize
	}

	eventSignatures := query.Topics[0]
	if len(eventSignatures) == 0 {
		return defaultStepSize
	}

	hasERC721Transfer := false
	hasERC1155Transfer := false
	for _, sig := range eventSignatures {
		if sig == TransferEventSignature || sig == EIP4906MetadataUpdateEventSignature {
			hasERC721Transfer = true
		}
		if sig == ERC1155TransferSingleEventSignature || sig == ERC1155TransferBatchEventSignature || sig == ERC1155URIEventSignature {
			hasERC1155Transfer = true
		}
	}

	hasTokenIDIndexed := false
	hasOwnerIndexed := false

	if hasERC721Transfer {
		if len(query.Topics) > 1 && len(query.Topics[1]) > 0 {
			hasOwnerIndexed = true
		}
		if len(query.Topics) > 2 && len(query.Topics[2]) > 0 {
			hasOwnerIndexed = true
		}
		if len(query.Topics) > 3 && len(query.Topics[3]) > 0 {
			hasTokenIDIndexed = true
		}
	} else if hasERC1155Transfer {
		if len(query.Topics) > 2 && len(query.Topics[2]) > 0 {
			hasOwnerIndexed = true
		}
		if len(query.Topics) > 3 && len(query.Topics[3]) > 0 {
			hasOwnerIndexed = true
		}
	}

	switch {
	case hasERC721Transfer && hasTokenIDIndexed:
		return erc721TokenStepSize
	case hasERC721Transfer && hasOwnerIndexed:
		return erc721OwnerStepSize
	case hasERC1155Transfer && hasOwnerIndexed:
		return erc1155OwnerStepSize
	case hasERC1155Transfer:
		logger.DebugCtx(ctx, "Using medium step size for ERC1155 contract query",
			zap.Uint64("stepSize", erc1155ContractStepSize))
		return erc1155ContractStepSize
	default:
		return defaultStepSize
	}
}

// getLogsWithRetry walks one outer window, halving the step on
// too-many-results/range-cap rejections. The second return value is the step
// ceiling after the walk: it equals the given stepSize unless the provider
// reported a block-range cap, in which case it is the discovered cap so the
// caller can stop probing above it in later windows.
//
// callsUsed counts FilterLogs calls across the whole FilterLogsWithPagination
// walk (all outer windows share one budget); when guards.CallBudget is set and
// exhausted, the walk aborts with ErrCallBudgetExhausted before the next call.
// pace is likewise walk-scoped: successful windows accumulate across every
// outer window so the progress heartbeat fires regardless of how the walk is
// partitioned (a span-cap-seeded walk completes exactly one window per call
// here, which would starve any per-call counter).
func (h *PaginationHelper) getLogsWithRetry(ctx context.Context, query ethereum.FilterQuery, stepSize uint64, callsUsed *int, pace *paceState) ([]types.Log, uint64, error) {
	currentStepSize := stepSize
	// maxStepSize is the ceiling the step may ramp back up to after successes.
	// It starts at the caller's step size and shrinks permanently when the
	// provider reports a block-range cap: span caps are a fixed provider
	// property, so probing above an accepted span again is a guaranteed
	// rejection plus a one-second sleep on every window.
	maxStepSize := stepSize

	var allLogs []types.Log
	currentFrom := new(big.Int).Set(query.FromBlock)

	for currentFrom.Cmp(query.ToBlock) <= 0 {
		select {
		case <-ctx.Done():
			return allLogs, maxStepSize, ctx.Err()
		default:
		}

		currentTo := new(big.Int).Add(currentFrom, new(big.Int).SetUint64(currentStepSize-1))
		if currentTo.Cmp(query.ToBlock) > 0 {
			currentTo.Set(query.ToBlock)
		}

		queryCopy := query
		queryCopy.FromBlock = new(big.Int).Set(currentFrom)
		queryCopy.ToBlock = new(big.Int).Set(currentTo)

		if h.guards.CallBudget > 0 && *callsUsed >= h.guards.CallBudget {
			return allLogs, maxStepSize, fmt.Errorf(
				"%w: %d FilterLogs calls used, aborting at block range [%d, %d]",
				ErrCallBudgetExhausted, *callsUsed, currentFrom.Uint64(), currentTo.Uint64())
		}
		*callsUsed++
		logs, err := h.ethClient.FilterLogs(ctx, queryCopy)
		if err == nil {
			allLogs = append(allLogs, logs...)
			currentFrom.SetUint64(currentTo.Uint64() + 1)
			pace.windowDone(ctx, currentFrom.Uint64(), currentStepSize)
			// Ramp the step back up gradually instead of resetting to the
			// original. Against providers with a hard block-range cap the
			// original step fails on every window, so a full reset re-pays
			// the whole halving cascade (with 1s sleeps) after each success.
			if currentStepSize < maxStepSize {
				currentStepSize *= 2
				if currentStepSize > maxStepSize {
					currentStepSize = maxStepSize
				}
			}
			continue
		}

		if ctx.Err() != nil {
			return allLogs, maxStepSize, ctx.Err()
		}

		if !IsTooManyResultsError(err) {
			return nil, maxStepSize, err
		}

		// Cannot split further - return explicit error
		if currentFrom.Cmp(currentTo) == 0 {
			return nil, maxStepSize, &SingleBlockOverflowError{Block: currentFrom.Uint64(), Err: err}
		}

		currentStepSize = currentStepSize / 2
		if currentStepSize == 0 {
			return nil, maxStepSize, fmt.Errorf("step size exhausted at block range [%d, %d]: %w", currentFrom.Uint64(), currentTo.Uint64(), err)
		}

		if IsBlockRangeCapError(err) {
			maxStepSize = currentStepSize
		}

		h.clock.Sleep(time.Second)
	}

	return allLogs, maxStepSize, nil
}
