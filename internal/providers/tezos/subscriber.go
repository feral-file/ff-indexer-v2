// Package tezos subscribes to Tezos events via TzKT SignalR.
//
// It buffers transfers and bigmaps by block level so the runner normally receives strictly
// ascending levels (see processStream for trade-offs: overflow flush, age timeout, pruning).
//
// emittedLevels is pruned to a sliding window of about 2× maxBufferedLevels (default 40 levels
// when maxBufferedLevels is 20) so memory stays bounded. Rows far outside that window can be
// emitted again if they arrive very late; the runner must keep a monotonic cursor and tolerate
// duplicate or replayed work via job keys and idempotent handlers.
//
// On subscribe, REST backfill from fromLevel through chain head runs before SignalR connect
// because hub methods are live-only. See docs/architecture.md (TzKT resume).
package tezos

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"go.uber.org/zap"

	"github.com/feral-file/ff-indexer-v2/internal/adapter"
	"github.com/feral-file/ff-indexer-v2/internal/blockchain"
	"github.com/feral-file/ff-indexer-v2/internal/domain"
	"github.com/feral-file/ff-indexer-v2/internal/logger"
)

const (
	SUBSCRIBE_TIMEOUT      = 15 * time.Second
	defaultEventBufferSize = 2048
	// Level buffer configuration
	defaultMaxBufferedLevels = 20               // Max buffered levels before force-flush (~5 min tolerance)
	defaultLevelTimeout      = 60 * time.Second // Timeout for latest level completion
)

// streamProcessorState is the in-memory state for processStream (per-level buffers and high-water marks).
type streamProcessorState struct {
	buffers              map[uint64]*levelBuffer
	emittedLevels        map[uint64]bool
	highestTransferLevel uint64
	highestBigmapLevel   uint64
	maxBufferedLevels    int
	levelTimeout         time.Duration
}

func newStreamProcessorState() *streamProcessorState {
	return &streamProcessorState{
		buffers:           make(map[uint64]*levelBuffer),
		emittedLevels:     make(map[uint64]bool),
		maxBufferedLevels: defaultMaxBufferedLevels,
		levelTimeout:      defaultLevelTimeout,
	}
}

// Config is Tezos/TzKT subscription settings.
type Config struct {
	WebSocketURL string       // WebSocket URL (e.g., https://api.tzkt.io/v1/ws)
	ChainID      domain.Chain // e.g., "tezos:mainnet"
}

// MessageType is the TzKT SignalR envelope kind.
type MessageType int

const (
	MessageTypeState MessageType = iota
	MessageTypeData
	MessageTypeReorg
	MessageTypeSubscribed
)

// TzKTMessage is a TzKT SignalR JSON envelope.
type TzKTMessage struct {
	Type  MessageType     `json:"type"`
	State uint64          `json:"state"`
	Data  json.RawMessage `json:"data,omitempty"`
}

type tzSubscriber struct {
	ctx        context.Context
	cancel     context.CancelFunc
	wsURL      string
	chainID    domain.Chain
	client     adapter.SignalRClient
	connected  bool
	handler    blockchain.EventHandler
	signalR    adapter.SignalR
	clock      adapter.Clock
	tzktClient TzKTClient
	config     Config
	streamCh   chan streamMessage
	errCh      chan error
}

type streamMessage struct {
	transfers []TzKTTokenTransfer
	updates   []TzKTBigMapUpdate
}

// levelBuffer batches transfers and bigmap rows for one block level before emission.
type levelBuffer struct {
	level     uint64
	transfers []TzKTTokenTransfer
	bigmaps   []TzKTBigMapUpdate
	firstSeen time.Time
}

// NewSubscriber builds a Tezos EventSource backed by TzKT SignalR.
func NewSubscriber(cfg Config, signalR adapter.SignalR, clock adapter.Clock, tzktClient TzKTClient) (blockchain.EventSource, error) {
	return &tzSubscriber{
		wsURL:      cfg.WebSocketURL,
		chainID:    cfg.ChainID,
		signalR:    signalR,
		clock:      clock,
		tzktClient: tzktClient,
		config:     cfg,
	}, nil
}

// SubscribeEvents backfills historic levels via TzKT REST, then connects for FA2 transfers
// and token_metadata bigmap updates over SignalR.
func (c *tzSubscriber) SubscribeEvents(ctx context.Context, fromLevel uint64, handler blockchain.EventHandler) error {
	if c.connected {
		logger.WarnCtx(ctx, "Already connected to TzKT SignalR")
		return nil
	}

	logger.InfoCtx(ctx, "TzKT subscribe starting with REST backfill before SignalR",
		zap.String("chain", string(c.chainID)),
		zap.Uint64("from_level", fromLevel))

	c.handler = handler

	backfilledTo, err := c.backfillHistoricLevels(ctx, fromLevel, 0, handler)
	if err != nil {
		return fmt.Errorf("REST backfill failed: %w", err)
	}

	logger.InfoCtx(ctx, "TzKT REST backfill complete, connecting SignalR",
		zap.String("chain", string(c.chainID)),
		zap.Uint64("from_level", fromLevel),
		zap.Uint64("backfilled_to", backfilledTo))

	subscriptionCtx, cancel := context.WithCancel(ctx)
	c.ctx = subscriptionCtx
	c.cancel = cancel

	c.streamCh = make(chan streamMessage, defaultEventBufferSize)
	c.errCh = make(chan error, 1)

	// Create SignalR client with connection
	client, err := c.signalR.NewClient(subscriptionCtx, c.wsURL, c)
	if err != nil {
		cancel()
		c.cancel = nil
		return fmt.Errorf("failed to create SignalR client: %w", err)
	}

	c.client = client

	defer func() {
		if !c.connected && c.client != nil {
			c.Close()
		}
	}()

	// Connect to SignalR hub
	client.Start()

	// Wait for connection
	c.clock.Sleep(time.Second)

	// Subscribe to token transfers (live stream after REST backfill through chain head).
	sendErrChan := client.Send("SubscribeToTokenTransfers", map[string]interface{}{})
	select {
	case err := <-sendErrChan:
		if err != nil {
			return fmt.Errorf("failed to subscribe to token transfers: %w", err)
		}
	case <-c.clock.After(SUBSCRIBE_TIMEOUT):
		// Timeout waiting for send
		return fmt.Errorf("timeout waiting for token transfers subscription: %w", domain.ErrSubscriptionFailed)
	}

	// Subscribe to big map updates for metadata changes
	// Filter for token_metadata path to get only metadata-related updates
	// Cross-feed ordering is handled by level-based buffering in processStream()
	sendErrChan = client.Send("SubscribeToBigMaps", map[string]interface{}{
		"path": "token_metadata",
	})
	select {
	case err := <-sendErrChan:
		if err != nil {
			return fmt.Errorf("failed to subscribe to big maps: %w", err)
		}
	case <-c.clock.After(SUBSCRIBE_TIMEOUT):
		// Timeout waiting for send
		return fmt.Errorf("timeout waiting for big maps subscription: %w", domain.ErrSubscriptionFailed)
	}

	go c.processStream(subscriptionCtx)
	c.connected = true

	select {
	case err := <-c.errCh:
		c.Close()
		return err
	case <-ctx.Done():
		c.Close()
		logger.InfoCtx(ctx, "TzKT WebSocket connection closed due to context done")
		return ctx.Err()
	}
}

// Transfers is the SignalR callback named "transfers".
func (c *tzSubscriber) Transfers(data interface{}) {
	// Marshal data to JSON
	jsonData, err := json.Marshal(data)
	if err != nil {
		logger.ErrorCtx(c.ctx, errors.New("error marshaling transfers data"), zap.Error(err))
		return
	}

	// Unmarshal data to TzKTMessage
	var msg TzKTMessage
	if err := json.Unmarshal(jsonData, &msg); err != nil {
		logger.ErrorCtx(c.ctx, errors.New("error unmarshaling transfers message"), zap.Error(err))
		return
	}

	// Type 0 is state/confirmation message, ignore it
	if msg.Type == MessageTypeState {
		logger.DebugCtx(c.ctx, "Received transfers state message", zap.Any("state", msg.State))
		return
	}

	// Only handle data messages
	if msg.Type != MessageTypeData {
		// FIXME: Handle other message types
		logger.WarnCtx(c.ctx, "Transfers message type is not data", zap.Any("type", msg.Type))
		return
	}

	if msg.Data == nil {
		logger.WarnCtx(c.ctx, "Transfers message without data field")
		return
	}

	// Unmarshal data array into transfer events
	var transfers []TzKTTokenTransfer
	if err := json.Unmarshal(msg.Data, &transfers); err != nil {
		logger.ErrorCtx(c.ctx, errors.New("error unmarshaling transfers data"), zap.Error(err))
		return
	}

	c.enqueueStream(streamMessage{transfers: transfers})
}

// Bigmaps is the SignalR callback named "bigmaps".
func (c *tzSubscriber) Bigmaps(data interface{}) {
	// Marshal data to JSON
	jsonData, err := json.Marshal(data)
	if err != nil {
		logger.ErrorCtx(c.ctx, errors.New("error marshaling bigmaps data"), zap.Error(err))
		return
	}

	// Unmarshal data to TzKTMessage
	var msg TzKTMessage
	if err := json.Unmarshal(jsonData, &msg); err != nil {
		logger.ErrorCtx(c.ctx, errors.New("error unmarshaling bigmaps message"), zap.Error(err))
		return
	}

	// Type 0 is state/confirmation message, ignore it
	if msg.Type == MessageTypeState {
		logger.DebugCtx(c.ctx, "Received bigmaps state message", zap.Any("state", msg.State))
		return
	}

	// Only handle data messages
	if msg.Type != MessageTypeData {
		// FIXME: Handle other message types
		logger.WarnCtx(c.ctx, "Bigmaps message type is not data", zap.Any("type", msg.Type))
		return
	}

	if msg.Data == nil {
		logger.WarnCtx(c.ctx, "Bigmaps message without data field")
		return
	}

	// Unmarshal data array into bigmap updates
	var updates []TzKTBigMapUpdate
	if err := json.Unmarshal(msg.Data, &updates); err != nil {
		logger.ErrorCtx(c.ctx, errors.New("error unmarshaling bigmaps data"), zap.Error(err))
		return
	}

	c.enqueueStream(streamMessage{updates: updates})
}

// GetLatestBlock returns the chain tip level from TzKT REST.
func (c *tzSubscriber) GetLatestBlock(ctx context.Context) (uint64, error) {
	return c.tzktClient.GetLatestBlock(ctx)
}

// Close stops the SignalR client and clears subscription state.
func (c *tzSubscriber) Close() {
	if c.cancel != nil {
		c.cancel()
		c.cancel = nil
	}

	if c.client != nil {
		c.client.Stop()
		c.client = nil
	}

	c.connected = false
	logger.InfoCtx(c.ctx, "TzKT WebSocket connection closed")
}

// enqueueStream sends a merged batch to processStream, or drops if the context is done.
func (c *tzSubscriber) enqueueStream(msg streamMessage) {
	if c.streamCh == nil {
		return
	}

	select {
	case <-c.ctx.Done():
	case c.streamCh <- msg:
	}
}

// processStream merges transfers and bigmaps feeds by level, then emits in ascending level order.
//
// Two independent SignalR feeds can interleave; without buffering, one feed could advance the
// cursor past data from the other. Defaults: maxBufferedLevels 20, levelTimeout 60s, prune window
// ~2× that (~40 levels). Effects: ~one level extra latency; overflow may force a partial level
// (logged); age timeout can seal an incomplete level; late rows inside the window are dropped
// after emit; very late rows after pruning may emit again — runner job keys and idempotent
// handlers must absorb duplicates. Runner must keep a monotonic cursor.
//
// Timeout uses firstSeen+levelTimeout per blocking level (not runner-style “idle reset”).
// Per-feed level order is assumed non-decreasing; warnIfFeedLevelRegression logs violations.
func (c *tzSubscriber) processStream(ctx context.Context) {
	s := newStreamProcessorState()
	var levelTimeoutC <-chan time.Time

	for {
		select {
		case <-ctx.Done():
			return
		case <-levelTimeoutC:
			if err := c.handleStreamTimeout(ctx, s); err != nil {
				c.reportError(err)
				return
			}
			levelTimeoutC = c.nextIncompleteLevelTimeoutChan(s)
		case msg := <-c.streamCh:
			if err := c.handleStreamMessage(ctx, msg, s); err != nil {
				c.reportError(err)
				return
			}
			levelTimeoutC = c.nextIncompleteLevelTimeoutChan(s)
		}
	}
}

// handleStreamTimeout drains a bounded batch from streamCh first (avoids sealing under select
// fairness while a same-level batch waits on streamCh), then runs a timeout-driven drain.
func (c *tzSubscriber) handleStreamTimeout(ctx context.Context, s *streamProcessorState) error {
	if err := c.mergeAllPendingStream(ctx, s); err != nil {
		return err
	}
	return c.drainBufferedLevels(ctx, s, true)
}

// handleStreamMessage merges one batch and drains complete levels (no timeout path).
func (c *tzSubscriber) handleStreamMessage(ctx context.Context, msg streamMessage, s *streamProcessorState) error {
	c.mergeStreamMessage(ctx, msg, s)
	return c.drainBufferedLevels(ctx, s, false)
}

// mergeAllPendingStream non-blockingly drains streamCh (cap per call) before a timeout drain.
// Unbounded drain would starve timeouts under constant ingress; capped batch avoids that.
// Returns ctx.Err() if the context is canceled during the drain.
func (c *tzSubscriber) mergeAllPendingStream(ctx context.Context, s *streamProcessorState) error {
	const maxPendingDrainBatch = 1000
	for i := 0; i < maxPendingDrainBatch; i++ {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case msg := <-c.streamCh:
			c.mergeStreamMessage(ctx, msg, s)
			if err := c.drainBufferedLevels(ctx, s, false); err != nil {
				return err
			}
		default:
			return nil
		}
	}
	return nil
}

// nextIncompleteLevelTimeoutChan returns clock.After(delay) for the next incomplete level deadline
// (firstSeen + levelTimeout), or nil if none applies. Unlike runner idle flush, later levels do
// not reset the age clock for an older incomplete level.
func (c *tzSubscriber) nextIncompleteLevelTimeoutChan(s *streamProcessorState) <-chan time.Time {
	sorted := sortedLevels(s.buffers)
	delay, ok := nextIncompleteTimeout(sorted, s.buffers, s.highestTransferLevel, s.highestBigmapLevel, s.levelTimeout, c.clock.Now())
	if !ok {
		return nil
	}
	return c.clock.After(delay)
}

// warnIfFeedLevelRegression logs when a feed delivers level < its high-water mark.
// Completion logic assumes each feed is non-decreasing by level; we warn and keep merging.
func (c *tzSubscriber) warnIfFeedLevelRegression(ctx context.Context, feed string, level, highWater uint64) {
	if highWater == 0 || level >= highWater {
		return
	}
	logger.WarnCtx(ctx, "TzKT SignalR feed level regressed below prior high water; monotonic-by-level assumption may be violated (continuing)",
		zap.String("chain", string(c.chainID)),
		zap.String("feed", feed),
		zap.Uint64("level", level),
		zap.Uint64("high_water_mark", highWater))
}

// mergeStreamMessage appends one SignalR batch into per-level buffers.
func (c *tzSubscriber) mergeStreamMessage(ctx context.Context, msg streamMessage, s *streamProcessorState) {
	for i := range msg.transfers {
		t := &msg.transfers[i]
		level := t.Level

		c.warnIfFeedLevelRegression(ctx, "transfers", level, s.highestTransferLevel)

		if s.emittedLevels[level] {
			logger.WarnCtx(ctx, "Dropping late transfer for already-emitted level",
				zap.Uint64("level", level))
			continue
		}

		if s.buffers[level] == nil {
			s.buffers[level] = &levelBuffer{
				level:     level,
				firstSeen: c.clock.Now(),
			}
		}
		s.buffers[level].transfers = append(s.buffers[level].transfers, *t)

		if level > s.highestTransferLevel {
			s.highestTransferLevel = level
		}
	}

	for i := range msg.updates {
		u := &msg.updates[i]
		level := u.Level

		c.warnIfFeedLevelRegression(ctx, "bigmaps", level, s.highestBigmapLevel)

		if s.emittedLevels[level] {
			logger.WarnCtx(ctx, "Dropping late bigmap for already-emitted level",
				zap.Uint64("level", level))
			continue
		}

		if s.buffers[level] == nil {
			s.buffers[level] = &levelBuffer{
				level:     level,
				firstSeen: c.clock.Now(),
			}
		}
		s.buffers[level].bigmaps = append(s.buffers[level].bigmaps, *u)

		if level > s.highestBigmapLevel {
			s.highestBigmapLevel = level
		}
	}
}

// drainBufferedLevels emits complete levels in order; if fromTimeout, may emit one timed-out incomplete
// level first. Repeats after force-flush until buffer size is OK so newly complete levels are not stranded.
func (c *tzSubscriber) drainBufferedLevels(ctx context.Context, s *streamProcessorState, fromTimeout bool) error {
	runTimeout := fromTimeout

	for {
		sorted := sortedLevels(s.buffers)

		if runTimeout && len(sorted) > 0 {
			for _, level := range sorted {
				buf := s.buffers[level]
				if buf == nil {
					continue
				}
				if !isLevelComplete(level, s.highestTransferLevel, s.highestBigmapLevel) {
					now := c.clock.Now()
					if incompleteLevelTimedOut(buf.firstSeen, s.levelTimeout, now) {
						logger.WarnCtx(ctx, "Emitting incomplete level due to timeout",
							zap.Uint64("level", level),
							zap.Int("transfers", len(buf.transfers)),
							zap.Int("bigmaps", len(buf.bigmaps)),
							zap.Duration("age", now.Sub(buf.firstSeen)))
						if err := c.emitLevel(ctx, buf); err != nil {
							return err
						}
						delete(s.buffers, level)
						s.emittedLevels[level] = true
					}
					break
				}
			}
			sorted = sortedLevels(s.buffers)
			runTimeout = false
		}

		for _, level := range sorted {
			buf := s.buffers[level]
			if buf == nil {
				continue
			}
			if !isLevelComplete(level, s.highestTransferLevel, s.highestBigmapLevel) {
				break
			}
			if err := c.emitLevel(ctx, buf); err != nil {
				return err
			}
			delete(s.buffers, level)
			s.emittedLevels[level] = true
		}

		if len(s.buffers) > s.maxBufferedLevels {
			if err := c.forceFlushOldestLevel(ctx, s.buffers, s.emittedLevels); err != nil {
				return err
			}
			continue
		}
		break
	}

	if len(s.emittedLevels) > s.maxBufferedLevels*2 {
		c.pruneEmittedLevels(s.emittedLevels, s.highestTransferLevel, s.highestBigmapLevel, s.maxBufferedLevels)
	}

	return nil
}

// handleTransfers parses FA2 transfers and delivers them to the handler.
func (c *tzSubscriber) handleTransfers(ctx context.Context, transfers []TzKTTokenTransfer) error {
	for i := range transfers {
		transfer := transfers[i]
		if transfer.Token.Standard != domain.StandardFA2 {
			logger.DebugCtx(ctx, "Skipping token is not fa2",
				zap.String("standard", string(transfer.Token.Standard)),
				zap.String("contract", transfer.Token.Contract.Address),
				zap.String("tokenId", transfer.Token.TokenID))
			continue
		}

		event, err := c.tzktClient.ParseTransfer(ctx, &transfer)
		if err != nil {
			logger.ErrorCtx(ctx, errors.New("error parsing transfer"), zap.Error(err))
			continue
		}
		if event == nil || c.handler == nil {
			continue
		}

		if err := c.handler(event); err != nil {
			return fmt.Errorf("failed to handle tezos transfer event %s at block %d: %w", event.TxHash, event.BlockNumber, err)
		}
	}

	return nil
}

// handleBigMapUpdates parses token_metadata updates and delivers them to the handler.
func (c *tzSubscriber) handleBigMapUpdates(ctx context.Context, updates []TzKTBigMapUpdate) error {
	for i := range updates {
		update := updates[i]
		if update.Path != "token_metadata" {
			continue
		}

		event, err := c.tzktClient.ParseBigMapUpdate(ctx, &update)
		if err != nil {
			logger.ErrorCtx(ctx, errors.New("error parsing big map update"), zap.Error(err))
			continue
		}
		if event == nil || c.handler == nil {
			continue
		}

		if err := c.handler(event); err != nil {
			return fmt.Errorf("failed to handle tezos metadata event %s at block %d: %w", event.TxHash, event.BlockNumber, err)
		}
	}

	return nil
}

// reportError sends the first fatal error to errCh (non-blocking if full).
func (c *tzSubscriber) reportError(err error) {
	select {
	case c.errCh <- err:
	default:
	}
}

// emitLevel delivers buffered transfers then bigmaps for one level (deterministic order).
// Duplicate emission is possible for very old late rows after pruning; the runner dedupes active
// jobs, and handlers should treat replays after a terminal job as idempotent.
func (c *tzSubscriber) emitLevel(ctx context.Context, buf *levelBuffer) error {
	// Emit transfers first
	if err := c.handleTransfers(ctx, buf.transfers); err != nil {
		return err
	}

	// Then emit bigmaps
	if err := c.handleBigMapUpdates(ctx, buf.bigmaps); err != nil {
		return err
	}

	logger.DebugCtx(ctx, "Level emitted",
		zap.Uint64("level", buf.level),
		zap.Int("transfers", len(buf.transfers)),
		zap.Int("bigmaps", len(buf.bigmaps)))

	return nil
}

// forceFlushOldestLevel emits the lowest buffered level when len(buffers) > maxBufferedLevels.
// May be partial if a feed is stalled; prefers OOM prevention over holding the line forever.
// Uses lowest level (not oldest wall time) to preserve ascending emission order.
func (c *tzSubscriber) forceFlushOldestLevel(ctx context.Context, buffers map[uint64]*levelBuffer, emittedLevels map[uint64]bool) error {
	// Find lowest buffered level to maintain ascending emission order
	var lowestLevel uint64
	first := true
	for level := range buffers {
		if first || level < lowestLevel {
			lowestLevel = level
			first = false
		}
	}

	buf := buffers[lowestLevel]

	// Log error: this indicates one feed is stuck/lagging significantly
	logger.ErrorCtx(ctx, errors.New("force-flushing incomplete level due to buffer overflow (feed lag detected)"),
		zap.Uint64("level", lowestLevel),
		zap.Int("transfers", len(buf.transfers)),
		zap.Int("bigmaps", len(buf.bigmaps)),
		zap.Duration("age", c.clock.Now().Sub(buf.firstSeen)),
		zap.Int("buffer_size", len(buffers)))

	// Emit whatever we have (partial data better than blocking indefinitely)
	if err := c.emitLevel(ctx, buf); err != nil {
		return err
	}
	delete(buffers, lowestLevel)
	emittedLevels[lowestLevel] = true // Mark as emitted to prevent re-emission

	return nil
}

// pruneEmittedLevels trims emittedLevels to a sliding window (~2× maxBufferedLevels behind the
// max of both feed high-water marks) so the map does not grow without bound. Levels dropped from
// the map are no longer treated as already-emitted here, so very old late events might replay;
// runner job keys and idempotent handlers cover that. Baseline uses max(both feeds) so one stalled
// feed does not stop pruning.
func (c *tzSubscriber) pruneEmittedLevels(emittedLevels map[uint64]bool, highestTransferLevel, highestBigmapLevel uint64, maxBufferedLevels int) {
	// Use maximum level from both feeds as pruning baseline
	// This ensures pruning progresses even if one feed is stalled
	maxTrackedLevel := highestTransferLevel
	if highestBigmapLevel > maxTrackedLevel {
		maxTrackedLevel = highestBigmapLevel
	}

	// Prune threshold: keep levels within 2x buffer size of max tracked level.
	// Multiply in uint64 to avoid int overflow before conversion (gosec G115).
	ml := maxBufferedLevels
	if ml < 0 {
		ml = 0
	}
	window := uint64(ml) * 2 //nolint:gosec // ml clamped ≥0; maxBufferedLevels comes from bounded config (see processStream default).
	var pruneThreshold uint64
	if maxTrackedLevel <= window {
		pruneThreshold = 0 // Keep all levels, nothing to prune yet
	} else {
		pruneThreshold = maxTrackedLevel - window
	}

	// Remove levels below threshold
	for level := range emittedLevels {
		if level < pruneThreshold {
			delete(emittedLevels, level)
		}
	}
}
