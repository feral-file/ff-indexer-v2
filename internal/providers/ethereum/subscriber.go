package ethereum

import (
	"context"
	"errors"
	"fmt"

	"github.com/ethereum/go-ethereum/core/types"
	"go.uber.org/zap"

	"github.com/feral-file/ff-indexer-v2/internal/blockchain"
	"github.com/feral-file/ff-indexer-v2/internal/domain"
	"github.com/feral-file/ff-indexer-v2/internal/logger"
	"github.com/feral-file/ff-indexer-v2/internal/providers/ethereum/adapters"
)

// headBufferSize bounds queued newHeads notifications while a log fetch is in
// flight. Heads are coalesced on read (only the newest matters), so the buffer
// absorbs a burst, not an outage: if it overflows, go-ethereum drops the
// subscription with an error and the runner restarts from the durable cursor.
const headBufferSize = 64

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

// SubscribeEvents streams indexable events from fromBlock onward, driven by the
// newHeads subscription: each new head triggers one eth_getLogs fetch covering
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
// 12 s block interval); tip reorgs are handled by number, so a replaced block's
// events are re-emitted at the same height when its successor head arrives and
// rely on the runner's same-height tolerance plus job dedup, exactly as the
// push stream did.
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
	heads := make(chan *types.Header, headBufferSize)
	sub, err := s.client.SubscribeNewHead(ctx, heads)
	if err != nil {
		return fmt.Errorf("failed to subscribe to new heads: %w", err)
	}
	defer func() {
		logger.InfoCtx(ctx, "Unsubscribing from ethereum new heads")
		sub.Unsubscribe()
		logger.InfoCtx(ctx, "Unsubscribed from ethereum new heads")
	}()

	next := fromBlock
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case err := <-sub.Err():
			return fmt.Errorf("new heads subscription error: %w", err)
		case head := <-heads:
			// Only the newest head matters: everything below it is covered by
			// the same range fetch. Draining here keeps a slow catch-up from
			// paying one fetch per queued head afterwards.
			head = drainHeads(heads, head)
			next, err = s.ingestThrough(ctx, next, head.Number.Uint64(), handler)
			if err != nil {
				return err
			}
		}
	}
}

// drainHeads returns the last head already queued behind the one just read.
func drainHeads(heads <-chan *types.Header, latest *types.Header) *types.Header {
	for {
		select {
		case h := <-heads:
			latest = h
		default:
			return latest
		}
	}
}

// ingestThrough fetches and emits every indexable log in [min(next, head), head]
// and returns the next block to fetch (head + 1).
//
// A head below next means the tip was reorganized (or a duplicate notification);
// re-fetching from that height re-emits the replaced blocks' events at the same
// heights, which the runner accepts (same-height flush) and job unique keys
// deduplicate.
func (s *ethSubscriber) ingestThrough(ctx context.Context, next, head uint64, handler blockchain.EventHandler) (uint64, error) {
	from := next
	if head < from {
		logger.WarnCtx(ctx, "Ethereum head below next expected block, re-fetching (reorg or duplicate head)",
			zap.Uint64("head", head), zap.Uint64("next", next))
		from = head
	}

	span := head - from + 1
	if s.cfg.MaxCatchupBlocks > 0 && span > s.cfg.MaxCatchupBlocks {
		return next, fmt.Errorf("%w: need blocks %d-%d (%d blocks, max %d); reset the block cursor deliberately or raise ethereum.max_catchup_blocks",
			ErrCatchupTooLarge, from, head, span, s.cfg.MaxCatchupBlocks)
	}
	if span > 1 {
		logger.InfoCtx(ctx, "Ethereum ingestion catching up to head",
			zap.Uint64("fromBlock", from), zap.Uint64("toBlock", head), zap.Uint64("blocks", span))
	}

	logs, err := s.client.FetchIngestionLogs(ctx, from, head)
	if err != nil {
		return next, fmt.Errorf("fetch ingestion logs for blocks %d-%d: %w", from, head, err)
	}
	for _, vLog := range logs {
		if err := s.emitLog(ctx, vLog, handler); err != nil {
			return next, err
		}
	}
	return head + 1, nil
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
