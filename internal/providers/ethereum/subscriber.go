package ethereum

import (
	"context"
	"errors"
	"fmt"

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
	// ConfirmationBlocks is how many blocks behind the newest head ingestion
	// emits: a block is fetched only once head - ConfirmationBlocks reaches it.
	// 0 emits the tip immediately.
	ConfirmationBlocks uint64
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

// streamState is the subscriber's position: the next block to emit, the lower
// bound below which nothing is ever emitted (the caller's fromBlock — a future
// start_block must stay a hard boundary even while heads arrive below it), the
// highest head seen, and the heads received at heights not yet emitted (plus
// the last emitted one), keyed by height, for reorg accounting.
type streamState struct {
	next       uint64
	lowerBound uint64
	tip        uint64
	heads      map[uint64]*adapter.BlockHead
}

// SubscribeEvents streams indexable events from fromBlock onward, driven by the
// newHeads subscription: each new head triggers eth_getLogs fetches covering
// every block not yet emitted, up to head - ConfirmationBlocks.
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
// Trade-offs: events land ConfirmationBlocks blocks (≈12 s each) plus one fetch
// round-trip after the tip. That lag is the reorg strategy — see planRange:
// the runner downstream orders by block number only and never rewinds its
// cursor, so a replaced block cannot be repaired once emitted; instead blocks
// are emitted only after the chain has built ConfirmationBlocks on top, and a
// reorg deeper than that is reported as an error, never replayed. The push
// stream emitted the tip immediately and silently indexed orphaned blocks.
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

	state := streamState{next: fromBlock, lowerBound: fromBlock, heads: map[uint64]*adapter.BlockHead{}}
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case err := <-sub.Err():
			return fmt.Errorf("new heads subscription error: %w", err)
		case head := <-heads:
			from, to, ok := s.planRange(ctx, &state, append([]*adapter.BlockHead{head}, drainHeads(heads)...))
			if !ok {
				continue
			}
			if err := s.ingestRange(ctx, from, to, handler); err != nil {
				return err
			}
			state.advance(to)
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

// planRange records the received heads and returns the block range to emit,
// or ok=false when the chain has not yet confirmed anything new.
//
// Reorg accounting, by height relative to `next` (the first unemitted block):
//   - below the lower bound: ignored — a future start_block is a hard boundary;
//   - at or above next: recorded; a replacement (different hash at the same
//     height) simply overwrites — that is a shallow reorg absorbed by the
//     confirmation lag, logged at info;
//   - below next: an emitted height was replaced, i.e. the reorg is deeper than
//     ConfirmationBlocks. It is logged as an error naming the affected heights
//     and NOT re-emitted: the runner orders by number and never rewinds, so a
//     replay would flush the open (orphaned) block, advance the cursor past the
//     replacement, and drop it — worse than an explicit, operator-visible gap.
//
// A parent-hash mismatch inside the range about to be emitted is reported the
// same way (it is the same deep reorg seen through a provider that announces
// only the new tip).
func (s *ethSubscriber) planRange(ctx context.Context, st *streamState, batch []*adapter.BlockHead) (from, to uint64, ok bool) {
	for _, h := range batch {
		st.record(ctx, h)
	}
	if st.tip < s.cfg.ConfirmationBlocks {
		return 0, 0, false
	}
	to = st.tip - s.cfg.ConfirmationBlocks
	if to < st.next {
		return 0, 0, false
	}
	st.checkContinuity(ctx, st.next, to)
	return st.next, to, true
}

// record stores one head according to the rules in planRange.
func (st *streamState) record(ctx context.Context, h *adapter.BlockHead) {
	n := uint64(h.Number)
	if n < st.lowerBound {
		logger.DebugCtx(ctx, "Ignoring ethereum head below start block",
			zap.Uint64("head", n), zap.Uint64("startBlock", st.lowerBound))
		return
	}
	if n > st.tip {
		st.tip = n
	}
	prev, seen := st.heads[n]
	if n < st.next {
		if seen && prev.Hash == h.Hash {
			return // duplicate notification of an already-emitted head
		}
		logger.ErrorCtx(ctx, errors.New("ethereum reorg deeper than confirmation lag: an emitted block was replaced"),
			zap.Uint64("height", n), zap.Uint64("lastEmitted", st.next-1),
			zap.String("newHash", h.Hash.Hex()), zap.String("hint", "events for the affected heights may be orphaned; reindex the range"))
		return
	}
	if seen && prev.Hash != h.Hash {
		logger.InfoCtx(ctx, "Ethereum shallow reorg absorbed within confirmation lag",
			zap.Uint64("height", n), zap.String("old", prev.Hash.Hex()), zap.String("new", h.Hash.Hex()))
	}
	st.heads[n] = h
}

// checkContinuity reports a parent-hash break inside [from, to] against the
// heads recorded so far. It never changes what is emitted (see planRange).
func (st *streamState) checkContinuity(ctx context.Context, from, to uint64) {
	for n := from; n <= to; n++ {
		h, ok := st.heads[n]
		if !ok || n == 0 {
			continue
		}
		parent, ok := st.heads[n-1]
		if !ok {
			continue
		}
		if h.ParentHash != parent.Hash {
			logger.ErrorCtx(ctx, errors.New("ethereum reorg deeper than confirmation lag: parent hash does not match the emitted chain"),
				zap.Uint64("height", n), zap.String("parent", h.ParentHash.Hex()), zap.String("emitted", parent.Hash.Hex()),
				zap.String("hint", "events for the affected heights may be orphaned; reindex the range"))
		}
	}
}

// advance moves past `to` and forgets heads below the last emitted height.
func (st *streamState) advance(to uint64) {
	st.next = to + 1
	for n := range st.heads {
		if n+1 < st.next {
			delete(st.heads, n)
		}
	}
}

// ingestRange fetches and emits every indexable log in [from, to] in bounded
// batches so a long catch-up streams through memory instead of materializing
// the whole range (see catchupBatchBlocks).
func (s *ethSubscriber) ingestRange(ctx context.Context, from, to uint64, handler blockchain.EventHandler) error {
	span := to - from + 1
	if s.cfg.MaxCatchupBlocks > 0 && span > s.cfg.MaxCatchupBlocks {
		return fmt.Errorf("%w: need blocks %d-%d (%d blocks, max %d); reset the block cursor deliberately or raise ethereum.max_catchup_blocks",
			ErrCatchupTooLarge, from, to, span, s.cfg.MaxCatchupBlocks)
	}
	if span > 1 {
		logger.InfoCtx(ctx, "Ethereum ingestion catching up to head",
			zap.Uint64("fromBlock", from), zap.Uint64("toBlock", to), zap.Uint64("blocks", span))
	}

	batches := 0
	for batchFrom := from; batchFrom <= to; batchFrom += catchupBatchBlocks {
		// A shutdown or runner failure mid-catch-up must not keep paying for
		// batches whose events nobody will consume.
		if err := ctx.Err(); err != nil {
			return err
		}
		batchTo := min(batchFrom+catchupBatchBlocks-1, to)
		logs, err := s.client.FetchIngestionLogs(ctx, batchFrom, batchTo)
		if err != nil {
			return fmt.Errorf("fetch ingestion logs for blocks %d-%d: %w", batchFrom, batchTo, err)
		}
		for _, vLog := range logs {
			if err := s.emitLog(ctx, vLog, handler); err != nil {
				return err
			}
		}
		batches++
		if batches%catchupLogEvery == 0 {
			logger.InfoCtx(ctx, "Ethereum ingestion catch-up progress",
				zap.Uint64("throughBlock", batchTo), zap.Uint64("targetBlock", to), zap.Int("batches", batches))
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
