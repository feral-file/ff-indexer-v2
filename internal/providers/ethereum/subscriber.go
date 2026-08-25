package ethereum

import (
	"context"
	"errors"
	"fmt"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"go.uber.org/zap"

	"github.com/feral-file/ff-indexer-v2/internal/adapter"
	"github.com/feral-file/ff-indexer-v2/internal/blockchain"
	"github.com/feral-file/ff-indexer-v2/internal/domain"
	"github.com/feral-file/ff-indexer-v2/internal/logger"
	"github.com/feral-file/ff-indexer-v2/internal/providers/ethereum/adapters"
)

const (
	// headBufferSize bounds queued newHeads notifications while a log fetch is
	// in flight. Heads are coalesced on read, so the buffer absorbs a burst, not
	// an outage: go-ethereum queues up to 20k more behind it and then drops the
	// subscription with an error, which restarts ingestion from the cursor.
	headBufferSize = 64

	// catchupBatchBlocks bounds one eth_getLogs fetch while filling a gap.
	// Raw topic matches run ~470 per mainnet block (ERC-20 Transfers share the
	// ERC-721 signature and are only discarded at parse time), so 20 blocks is
	// ~9.4k logs — under Infura's 10k-result cap, so the pagination helper does
	// not pay a halving cascade, and a few MB in memory instead of the ~23M
	// logs a whole max_catchup_blocks range would materialize at once.
	catchupBatchBlocks = 20

	// reorgOverlap is how many already-emitted blocks are re-fetched when a
	// head's parent hash does not match the last head seen. Two is the most the
	// runner can use: it flushes block N when N+1's first event arrives and
	// accepts a same-height re-flush, so the open block and the one just
	// flushed are recoverable; anything older is behind the monotonic cursor
	// and dropped regardless of what the subscriber re-fetches.
	reorgOverlap = 2

	// catchupLogEvery is the progress cadence (in batches) during a gap fill.
	catchupLogEvery = 50
)

// ErrCatchupTooLarge is returned when the gap between the requested start block
// and the chain head exceeds Config.MaxCatchupBlocks. It is deliberately fatal:
// a gap that large is a stale database or an unreviewed rewind, and walking it
// silently would cost millions of eth_getLogs credits (see docs/constraints.md
// on cursor resets).
var ErrCatchupTooLarge = errors.New("ethereum ingestion catch-up exceeds max_catchup_blocks")

// Config holds the configuration for Ethereum subscription
type Config struct {
	WebSocketURL string       // WebSocket URL used for the newHeads subscription
	ChainID      domain.Chain // e.g., "eip155:1" for Ethereum mainnet
	// MaxCatchupBlocks bounds the block range fetched to reach the chain head
	// (from the durable cursor after a restart or socket drop). 0 = unbounded.
	MaxCatchupBlocks uint64
}

type ethSubscriber struct {
	client  EthereumClient
	chainID domain.Chain
	cfg     Config
}

// NewSubscriber creates a new Ethereum event subscriber.
func NewSubscriber(cfg Config, ethereumClient EthereumClient) (blockchain.EventSource, error) {
	return &ethSubscriber{
		client:  ethereumClient,
		chainID: cfg.ChainID,
		cfg:     cfg,
	}, nil
}

// headRange is the block range one iteration of the head loop must emit.
type headRange struct {
	from, to uint64
	// hash is the node-reported hash of the head at `to`, kept for the next
	// continuity check (the wire hash — see adapter.BlockHead).
	hash common.Hash
}

// streamState is the subscriber's position: the next block to fetch, the lower
// bound below which nothing is ever fetched (the caller's fromBlock — a future
// start_block must stay a hard boundary even while heads arrive below it),
// and the hash of the last head emitted, for parent-hash continuity.
type streamState struct {
	next       uint64
	lowerBound uint64
	lastHash   common.Hash
	haveHash   bool
}

// SubscribeEvents streams indexable events from fromBlock onward, driven by the
// newHeads subscription: each new head triggers eth_getLogs fetches covering
// every block not yet emitted, up to and including that head.
//
// Reason: the former eth_subscribe("logs") stream pushed every chain-wide log
// matching the NFT topics — ~99% ERC-20 Transfers sharing the ERC-721 signature
// — which a per-notification-priced provider bills at ~100M requests/month.
// Pulling each block's logs over HTTP keeps the same filter at ~2 requests per
// block. It also makes fromBlock real: eth_subscribe("logs") never replays
// history, so blocks mined during a socket drop used to be lost; now the gap
// [fromBlock, head] is fetched on the first head and the runner flushes it in
// order before live blocks.
//
// Trade-offs: events land one fetch round-trip after the head (well under the
// 12 s block interval). Reorgs are handled by number with a bounded overlap
// (see nextRange); the runner's monotonic cursor, not the subscriber, is what
// limits how deep a reorg can be repaired — exactly as with the push stream.
//
// Constraints: a single stream orders everything. Two independent log
// subscriptions were rejected because go-ethereum forwards each on its own
// buffered goroutine, so under runner backpressure their drains interleave and
// the runner — which flushes on the first event of a *different* block — would
// flush partial blocks and drop the stragglers behind its monotonic cursor.
//
// Parse failures for indexable logs stop the subscription so ingestion can
// retry from the durable cursor. Intentionally ignored logs are returned as
// (nil, nil) from ParseEventLog and skipped.
func (s *ethSubscriber) SubscribeEvents(ctx context.Context, fromBlock uint64, handler blockchain.EventHandler) error {
	heads := make(chan *adapter.BlockHead, headBufferSize)
	sub, err := s.client.SubscribeNewHead(ctx, heads)
	if err != nil {
		return fmt.Errorf("failed to subscribe to new heads: %w", err)
	}
	defer func() {
		logger.InfoCtx(ctx, "Unsubscribing from ethereum new heads")
		sub.Unsubscribe()
		logger.InfoCtx(ctx, "Unsubscribed from ethereum new heads")
	}()

	state := streamState{next: fromBlock, lowerBound: fromBlock}
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case err := <-sub.Err():
			return fmt.Errorf("new heads subscription error: %w", err)
		case head := <-heads:
			rng, ok := s.nextRange(ctx, &state, head, drainHeads(heads))
			if !ok {
				continue
			}
			if err := s.ingestRange(ctx, rng, handler); err != nil {
				return err
			}
			state.next = rng.to + 1
			state.lastHash, state.haveHash = rng.hash, true
		}
	}
}

// drainHeads returns every head already queued behind the one just read.
func drainHeads(heads <-chan *adapter.BlockHead) []*adapter.BlockHead {
	var queued []*adapter.BlockHead
	for {
		select {
		case h := <-heads:
			queued = append(queued, h)
		default:
			return queued
		}
	}
}

// nextRange turns the head just read plus any queued behind it into the block
// range to emit, or ok=false when there is nothing to do yet.
//
// Rules, in order:
//   - Heads below the lower bound are ignored: a future start_block is a hard
//     boundary, and re-fetches never dip under it.
//   - The range ends at the highest queued head; it starts at `next`, lowered to
//     the lowest queued head if any is at or below `next` (replacement headers
//     after a reorg arrive at heights already emitted — geth emits every new
//     canonical header, and coalescing must not skip the earlier ones).
//   - If the first head continues from `next` but its parent is not the last
//     head we emitted, the chain reorganized underneath us without a
//     replacement header we could act on; re-fetch reorgOverlap blocks.
//
// Re-emitted heights land on the runner's same-height tolerance and the job
// unique keys, so an overlap is never worse than a duplicate-suppressed replay.
func (s *ethSubscriber) nextRange(ctx context.Context, st *streamState, first *adapter.BlockHead, queued []*adapter.BlockHead) (headRange, bool) {
	all := append([]*adapter.BlockHead{first}, queued...)
	from, to := st.next, uint64(0)
	var toHash common.Hash
	found := false
	for _, h := range all {
		n := uint64(h.Number)
		if n < st.lowerBound {
			continue
		}
		if !found || n >= to {
			to, toHash, found = n, h.Hash, true
		}
		if n < from {
			logger.WarnCtx(ctx, "Ethereum head below next expected block, re-fetching (reorg or duplicate head)",
				zap.Uint64("head", n), zap.Uint64("next", st.next))
			from = n
		}
	}
	if !found {
		logger.DebugCtx(ctx, "Ignoring ethereum heads below start block",
			zap.Uint64("head", uint64(first.Number)), zap.Uint64("startBlock", st.lowerBound))
		return headRange{}, false
	}

	if st.haveHash && uint64(first.Number) == st.next && first.ParentHash != st.lastHash {
		overlapFrom := st.next - min(reorgOverlap, st.next)
		logger.WarnCtx(ctx, "Ethereum head does not continue from last emitted head, re-fetching overlap (reorg)",
			zap.Uint64("head", uint64(first.Number)), zap.String("parent", first.ParentHash.Hex()),
			zap.String("lastHead", st.lastHash.Hex()), zap.Uint64("refetchFrom", overlapFrom))
		if overlapFrom < from {
			from = overlapFrom
		}
	}
	if from < st.lowerBound {
		from = st.lowerBound
	}
	return headRange{from: from, to: to, hash: toHash}, true
}

// ingestRange fetches and emits every indexable log in [rng.from, rng.to] in
// bounded batches so a long catch-up streams through memory instead of
// materializing the whole range (see catchupBatchBlocks).
func (s *ethSubscriber) ingestRange(ctx context.Context, rng headRange, handler blockchain.EventHandler) error {
	span := rng.to - rng.from + 1
	if s.cfg.MaxCatchupBlocks > 0 && span > s.cfg.MaxCatchupBlocks {
		return fmt.Errorf("%w: need blocks %d-%d (%d blocks, max %d); reset the block cursor deliberately or raise ethereum.max_catchup_blocks",
			ErrCatchupTooLarge, rng.from, rng.to, span, s.cfg.MaxCatchupBlocks)
	}
	if span > 1 {
		logger.InfoCtx(ctx, "Ethereum ingestion catching up to head",
			zap.Uint64("fromBlock", rng.from), zap.Uint64("toBlock", rng.to), zap.Uint64("blocks", span))
	}

	batches := 0
	for from := rng.from; from <= rng.to; from += catchupBatchBlocks {
		// A shutdown or runner failure mid-catch-up must not keep paying for
		// batches whose events nobody will consume.
		if err := ctx.Err(); err != nil {
			return err
		}
		to := min(from+catchupBatchBlocks-1, rng.to)
		logs, err := s.client.FetchIngestionLogs(ctx, from, to)
		if err != nil {
			return fmt.Errorf("fetch ingestion logs for blocks %d-%d: %w", from, to, err)
		}
		for _, vLog := range logs {
			if err := s.emitLog(ctx, vLog, handler); err != nil {
				return err
			}
		}
		batches++
		if batches%catchupLogEvery == 0 {
			logger.InfoCtx(ctx, "Ethereum ingestion catch-up progress",
				zap.Uint64("throughBlock", to), zap.Uint64("targetBlock", rng.to), zap.Int("batches", batches))
		}
	}
	return nil
}

// emitLog parses one log and hands the resulting event to the handler.
// Skips are explicit: known custom signatures from unconfigured contracts are
// expected; anything the filter delivered that no adapter recognizes is a
// misconfiguration worth an error log but not worth stalling ingestion.
func (s *ethSubscriber) emitLog(ctx context.Context, vLog types.Log, handler blockchain.EventHandler) error {
	event, err := s.client.ParseEventLog(ctx, vLog)
	if err != nil {
		if errors.Is(err, context.Canceled) {
			return ctx.Err()
		}
		if errors.Is(err, adapters.ErrUnconfiguredContract) {
			logger.DebugCtx(ctx, "Skipping known custom signature from unconfigured contract",
				zap.String("signature", vLog.Topics[0].Hex()),
				zap.String("address", vLog.Address.Hex()),
				zap.Uint64("block", vLog.BlockNumber))
			return nil
		}
		if errors.Is(err, adapters.ErrUnexpectedEvent) {
			logger.ErrorCtx(ctx, errors.New("filter sent unexpected event signature - possible misconfiguration"),
				zap.String("signature", vLog.Topics[0].Hex()),
				zap.String("address", vLog.Address.Hex()),
				zap.Uint64("block", vLog.BlockNumber),
				zap.Error(err))
			return nil
		}
		// Fatal errors (timestamp lookup, malformed data, etc.)
		return fmt.Errorf("parse log at block %d index %d: %w", vLog.BlockNumber, vLog.Index, err)
	}
	if event == nil {
		return nil
	}
	if err := handler(event); err != nil {
		return fmt.Errorf("failed to handle ethereum event %s at block %d: %w", event.TxHash, event.BlockNumber, err)
	}
	return nil
}

// GetLatestBlock returns the latest block number using cached provider
func (s *ethSubscriber) GetLatestBlock(ctx context.Context) (uint64, error) {
	return s.client.GetLatestBlock(ctx)
}

// Close closes the connection
func (s *ethSubscriber) Close() {
	if s.client == nil {
		return
	}

	s.client.Close()
	logger.Info("Ethereum WebSocket connection closed")
}
