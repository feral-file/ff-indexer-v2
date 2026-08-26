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
	// Raw topic matches average ~470 per mainnet block (ERC-20 Transfers share
	// the ERC-721 signature and are only discarded at parse time) but busy
	// stretches run well above that: measured live, a 20-block batch tripped
	// Infura's 10k-result cap (its hint suggested 18) and the halving cascade
	// plus 503 retries stalled the first batch for over a minute. Ten blocks is
	// ~4.7k logs on average — margin under the cap, so batches complete in one
	// call, and a few MB in memory instead of the ~23M logs a whole
	// max_catchup_blocks range would materialize at once.
	catchupBatchBlocks = 10

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
	client   EthereumClient
	chainID  domain.Chain
	cfg      Config
	progress blockchain.RangeProgressHandler
}

// SetProgressHandler implements blockchain.ProgressReporter: after every
// emitted range the subscriber reports its upper bound so the runner can flush
// the open block and persist the cursor even through event-less ranges.
func (s *ethSubscriber) SetProgressHandler(handler blockchain.RangeProgressHandler) {
	s.progress = handler
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
			from, to, ok, err := s.planRange(ctx, &state, append([]*adapter.BlockHead{head}, drainHeads(heads)...))
			if err != nil {
				return err
			}
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
// A head whose parent disagrees with the retained chain is reconciled against
// canonical heads by number (see reconcile), so a deep reorg announced only by
// a later tip is still found and reported.
func (s *ethSubscriber) planRange(ctx context.Context, st *streamState, batch []*adapter.BlockHead) (from, to uint64, ok bool, err error) {
	for _, h := range batch {
		if err := s.record(ctx, st, h); err != nil {
			return 0, 0, false, err
		}
	}
	// The catch-up bound covers the whole gap to the tip, pending window
	// included: measuring only the confirmed range would let a gap of
	// max+lag through, and a large lag would defer an oversized gap past the
	// bound one block at a time.
	if s.cfg.MaxCatchupBlocks > 0 && st.tip >= st.next && st.tip-st.next+1 > s.cfg.MaxCatchupBlocks {
		return 0, 0, false, fmt.Errorf("%w: need blocks %d-%d (%d blocks, max %d); reset the block cursor deliberately or raise ethereum.max_catchup_blocks",
			ErrCatchupTooLarge, st.next, st.tip, st.tip-st.next+1, s.cfg.MaxCatchupBlocks)
	}
	if st.tip < s.cfg.ConfirmationBlocks {
		return 0, 0, false, nil
	}
	to = st.tip - s.cfg.ConfirmationBlocks
	if to < st.next {
		return 0, 0, false, nil
	}
	// The emitted boundary must carry a canonical hash for later reconciliation
	// to compare against. Received heads cover it in steady state; after a
	// (re)subscribe the first head can sit above it, so fetch it once.
	if _, ok := st.heads[to]; !ok {
		boundary, err := s.client.HeadByNumber(ctx, to)
		if err != nil {
			return 0, 0, false, fmt.Errorf("fetch emitted boundary head %d: %w", to, err)
		}
		st.heads[to] = boundary
	}
	return st.next, to, true, nil
}

// record stores one head according to the rules in planRange, reconciling
// the retained chain when the head's parent disagrees with it.
func (s *ethSubscriber) record(ctx context.Context, st *streamState, h *adapter.BlockHead) error {
	n := uint64(h.Number)
	if n < st.lowerBound {
		logger.DebugCtx(ctx, "Ignoring ethereum head below start block",
			zap.Uint64("head", n), zap.Uint64("startBlock", st.lowerBound))
		return nil
	}
	prev, seen := st.heads[n]
	if n < st.next {
		if seen && prev.Hash == h.Hash {
			return nil // duplicate notification of an already-emitted head
		}
		st.reportDeepReorg(ctx, n, h.Hash)
		return nil
	}
	// Reconcile before retaining: a head is only allowed to extend the
	// retained chain (and raise the confirmation tip) once its ancestry agrees
	// with it. A stale tip — one whose parent the node itself no longer
	// considers canonical — must not shorten the lag for everyone else.
	stale, err := s.reconcile(ctx, st, h)
	if err != nil {
		return err
	}
	if stale {
		logger.DebugCtx(ctx, "Ignoring stale ethereum head (parent is not canonical)",
			zap.Uint64("height", n), zap.String("hash", h.Hash.Hex()), zap.String("parent", h.ParentHash.Hex()))
		return nil
	}
	if seen && prev.Hash != h.Hash {
		logger.InfoCtx(ctx, "Ethereum shallow reorg absorbed within confirmation lag",
			zap.Uint64("height", n), zap.String("old", prev.Hash.Hex()), zap.String("new", h.Hash.Hex()))
	}
	st.heads[n] = h
	if n > st.tip {
		st.tip = n
	}
	return nil
}

// reconcile handles a head whose parent disagrees with the retained head at the
// previous height: the chain reorganized somewhere below it, and a provider
// may announce that only through this later tip. It walks canonical heads by
// number (wire hashes) down from the parent until the retained chain matches
// again, replacing stale retained heads and bridging heights no head was
// received for (they are fetched and retained so the walk can continue to the
// emitted boundary, which planRange guarantees is retained). Reaching an
// emitted height with a different hash is a reorg deeper than the lag:
// reported, never replayed (see planRange). Fetches happen only on a known
// mismatch and are bounded by the retained window (the confirmation lag plus
// any coalesced heads).
//
// It returns stale=true when the node says the retained or canonical chain
// disagrees with the new head's ancestry — the new head is the stale one and
// must not be retained. Not covered: a deep reorg that spans a process
// restart — the retained heads live in memory and the cursor stores no hash.
func (s *ethSubscriber) reconcile(ctx context.Context, st *streamState, h *adapter.BlockHead) (stale bool, err error) {
	n := uint64(h.Number)
	if n == 0 {
		return false, nil
	}
	// Bridging unreceived heights is only meaningful once an emitted boundary
	// is retained to walk down to; before the first emission there is nothing
	// a reorg could have orphaned, so unretained heights end the walk.
	_, bridge := st.heads[st.next-1]
	expected := h.ParentHash
	for k := n - 1; ; k-- {
		retained, ok := st.heads[k]
		if ok && retained.Hash == expected {
			return false, nil // the chains rejoin here
		}
		if !ok && (!bridge || k < st.next) {
			return false, nil // nothing retained to reconcile against
		}
		canonical, err := s.client.HeadByNumber(ctx, k)
		if err != nil {
			return false, fmt.Errorf("reconcile reorg at height %d: %w", k, err)
		}
		if ok && retained.Hash != canonical.Hash {
			// The retained chain is stale here whatever the incoming head is;
			// refresh it so later checks compare against the canonical chain.
			if k < st.next {
				st.reportDeepReorg(ctx, k, canonical.Hash)
			} else {
				logger.InfoCtx(ctx, "Ethereum shallow reorg absorbed within confirmation lag",
					zap.Uint64("height", k), zap.String("old", retained.Hash.Hex()), zap.String("new", canonical.Hash.Hex()))
			}
		}
		st.heads[k] = canonical
		if canonical.Hash != expected {
			// The ancestry the incoming head claims is not what the node holds
			// canonical at this height: the incoming head is stale, whether the
			// retained head was canonical (round-5 case) or a third branch.
			return true, nil
		}
		expected = canonical.ParentHash
		if k == 0 {
			return false, nil
		}
	}
}

// reportDeepReorg logs the operator-visible signal for an emitted block that
// the chain has since replaced.
func (st *streamState) reportDeepReorg(ctx context.Context, height uint64, newHash common.Hash) {
	logger.ErrorCtx(ctx, errors.New("ethereum reorg deeper than confirmation lag: an emitted block was replaced"),
		zap.Uint64("height", height), zap.Uint64("lastEmitted", st.next-1),
		zap.String("newHash", newHash.Hex()), zap.String("hint", "events for the affected heights may be orphaned; reindex the range"))
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
// the whole range (see catchupBatchBlocks). The catch-up bound was already
// enforced on the whole gap by planRange.
func (s *ethSubscriber) ingestRange(ctx context.Context, from, to uint64, handler blockchain.EventHandler) error {
	span := to - from + 1
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
		// Report every batch, not the range: the runner persists the cursor
		// before returning, so a later batch failing (and the restart it
		// causes) never re-scans this one or trips the catch-up bound on it.
		if s.progress != nil {
			if err := s.progress(batchTo); err != nil {
				return fmt.Errorf("report scanned range through %d: %w", batchTo, err)
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
