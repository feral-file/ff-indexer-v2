// Package tezos implements Tezos blockchain event subscription via TzKT SignalR.
//
// CRITICAL DEPENDENCY: This subscriber provides level-based buffering and ascending
// emission order within its operational window (40 levels), but relies on the runner
// to enforce monotonic cursor updates. Without monotonic cursor guard in the runner,
// very old late arrivals (>40 levels after pruning) can cause cursor regression.
//
// RESUME / fromLevel (known limitation): SubscribeEvents receives fromLevel from the
// ingestion runner (next level after the persisted cursor). TzKT SignalR methods
// SubscribeToTokenTransfers and SubscribeToBigMaps accept only filter maps (e.g.
// account, contract, tokenId, path)—not a starting level—so WebSocket pushes only
// events after subscription connects. Levels between fromLevel and chain head at
// connect time are not replayed over the socket unless a separate REST backfill is
// added (TODO(tezos-resume) in SubscribeEvents). After extended downtime, use API or
// batch indexing where gaps matter; see docs/architecture.md (TzKT resume gap).
//
// See processStream() doc comment for complete safety contract.
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

// Config holds the configuration for Tezos/TzKT subscription
type Config struct {
	WebSocketURL string       // WebSocket URL (e.g., https://api.tzkt.io/v1/ws)
	ChainID      domain.Chain // e.g., "tezos:mainnet"
}

// MessageType - TzKT message type
type MessageType int

const (
	MessageTypeState MessageType = iota
	MessageTypeData
	MessageTypeReorg
	MessageTypeSubscribed
)

// TzKTMessage wraps TzKT SignalR messages
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

// levelBuffer groups events by Tezos level (block height) for cross-feed coordination.
// Ensures transfers and bigmap updates from the same level are processed together.
type levelBuffer struct {
	level     uint64
	transfers []TzKTTokenTransfer
	bigmaps   []TzKTBigMapUpdate
	firstSeen time.Time
}

// NewSubscriber creates a new Tezos/TzKT event subscriber
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

// SubscribeEvents subscribes to FA2 transfer events and metadata updates via TzKT SignalR.
//
// fromLevel is the level the ingestion runner intends to subscribe from (persisted cursor + 1
// when start_level is unset). It is not passed to TzKT: SubscribeToTokenTransfers and
// SubscribeToBigMaps do not accept a historic level parameter in published TzKT WebSocket docs
// (filters are account / contract / tokenId / path / tags / ptr only). This implementation
// therefore opens a live-only stream after connect; any gap between fromLevel and the chain
// at connect time requires a REST backfill or out-of-band reindex — see TODO(tezos-resume).
func (c *tzSubscriber) SubscribeEvents(ctx context.Context, fromLevel uint64, handler blockchain.EventHandler) error {
	if c.connected {
		logger.WarnCtx(ctx, "Already connected to TzKT SignalR")
		return nil
	}

	logger.InfoCtx(ctx, "TzKT SignalR subscribing (live stream only; fromLevel recorded for ops — see docs/architecture.md TzKT resume gap)",
		zap.String("chain", string(c.chainID)),
		zap.Uint64("from_level", fromLevel))

	subscriptionCtx, cancel := context.WithCancel(ctx)
	c.ctx = subscriptionCtx
	c.cancel = cancel

	// Store handler for callback
	c.handler = handler
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

	// Subscribe to token transfers (live stream only — no historic fromLevel in TzKT API).
	// TODO(tezos-resume): Before this, optionally page TzKT REST (/v1/tokens/transfers,
	// bigmap updates APIs) from fromLevel to head, normalize through the same parsers, then
	// connect here for a gap-free restart after crashes or long outages.
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

// Transfers handles incoming transfer events from TzKT SignalR
// Method name must match the SignalR target name "transfers"
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

// Bigmaps handles incoming big map update events from TzKT SignalR
// Method name must match the SignalR target name "bigmaps"
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

// GetLatestBlock returns the latest block level from TzKT API
func (c *tzSubscriber) GetLatestBlock(ctx context.Context) (uint64, error) {
	return c.tzktClient.GetLatestBlock(ctx)
}

// Close closes the SignalR connection
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

func (c *tzSubscriber) enqueueStream(msg streamMessage) {
	if c.streamCh == nil {
		return
	}

	select {
	case <-c.ctx.Done():
	case c.streamCh <- msg:
	}
}

// processStream implements level-based buffering and cross-feed coordination for Tezos events.
//
// Reason: Tezos has two independent SignalR feeds (transfers and bigmaps) that can arrive out of order.
// Without coordination, a later bigmap event could advance the cursor past earlier transfer events,
// causing data loss on restart.
//
// Trade-offs:
// - Adds latency (~1 level delay) but ensures event ordering within each level
// - Force-flush on buffer overflow may emit partial level data (rare, logged as error)
// - Timeout-based completion for latest level only, handles feed lag gracefully
// - Late arrivals within sliding window (40 levels) are dropped (logged as warning)
// - Very old late arrivals (>40 levels old) may be re-emitted, but runner job dedup prevents duplicate work
//
// Constraints:
// - Buffer size limited to maxBufferedLevels (default: 20 levels, ~5 min tolerance)
// - Level timeout ensures progress even if one feed is stuck (default: 60s)
// - Emits levels in strictly ascending order (force-flush uses lowest level, not oldest time)
// - Emitted levels sealed within sliding window; old seals pruned for memory safety
// - Very old late arrivals (>40 levels / ~10 min old) may create duplicate jobs if original terminal
// - Event processing must be idempotent to handle rare duplicate-after-terminal-job case
//
// Safety guarantees (subscriber-level):
// - Single-emission within sliding window (40 levels)
// - Strictly ascending emission for active levels (within buffer + window)
// - Memory bounded via pruning (prevents OOM)
//
// Safety requirements (runner-level - CRITICAL):
// - **MUST enforce monotonic cursor updates** (reject backward movement)
// - **MUST provide duplicate active-job prevention** (via job unique keys)
// - **MUST implement idempotent event processing**
//
// Without monotonic cursor guard in runner:
// - Very old late arrivals (>40 levels after pruning) can be re-emitted
// - Runner may write older block cursor, causing cursor regression
// - This defeats the entire purpose of level-based coordination
//
// This subscriber provides ascending order within its operational window;
// runner must enforce monotonic cursor persistence to prevent regression from
// very old late arrivals after pruning.
//
// Assumptions:
// - TzKT SignalR feeds are monotonic by level (each feed emits events in non-decreasing level order)
// - This allows us to use "highest level seen" as a reliable completion signal
func (c *tzSubscriber) processStream(ctx context.Context) {
	buffers := make(map[uint64]*levelBuffer)
	emittedLevels := make(map[uint64]bool) // Track emitted levels to prevent re-emission
	maxBufferedLevels := defaultMaxBufferedLevels
	levelTimeout := defaultLevelTimeout
	
	// Track highest level seen from each feed
	var highestTransferLevel uint64
	var highestBigmapLevel uint64

	for {
		select {
		case <-ctx.Done():
			return
		case msg := <-c.streamCh:
			// Buffer transfers by level (skip already-emitted levels)
			for i := range msg.transfers {
				t := &msg.transfers[i]
				level := t.Level
				
				// Skip events for already-emitted levels (late arrivals after timeout)
				if emittedLevels[level] {
					logger.WarnCtx(ctx, "Dropping late transfer for already-emitted level",
						zap.Uint64("level", level))
					continue
				}
				
				if buffers[level] == nil {
					buffers[level] = &levelBuffer{
						level:     level,
						firstSeen: c.clock.Now(),
					}
				}
				buffers[level].transfers = append(buffers[level].transfers, *t)
				
				// Track highest level seen from transfers feed
				if level > highestTransferLevel {
					highestTransferLevel = level
				}
			}
			
			// Buffer bigmaps by level (skip already-emitted levels)
			for i := range msg.updates {
				u := &msg.updates[i]
				level := u.Level
				
				// Skip events for already-emitted levels (late arrivals after timeout)
				if emittedLevels[level] {
					logger.WarnCtx(ctx, "Dropping late bigmap for already-emitted level",
						zap.Uint64("level", level))
					continue
				}
				
				if buffers[level] == nil {
					buffers[level] = &levelBuffer{
						level:     level,
						firstSeen: c.clock.Now(),
					}
				}
				buffers[level].bigmaps = append(buffers[level].bigmaps, *u)
				
				// Track highest level seen from bigmaps feed
				if level > highestBigmapLevel {
					highestBigmapLevel = level
				}
			}

			// Get sorted levels for processing
			sortedLevels := getSortedLevels(buffers)
			
			// Process completed levels in ascending order
			// Stop at first incomplete level to maintain strict ascending emission
			for _, level := range sortedLevels {
				buf := buffers[level]
				
				// Check if level is complete
				isComplete := isLevelComplete(buf.level, highestTransferLevel, highestBigmapLevel)
				
				if isComplete {
					// Emit completed level
					if err := c.emitLevel(ctx, buf); err != nil {
						c.reportError(err)
						return
					}
					delete(buffers, level)
					emittedLevels[level] = true // Mark as emitted to prevent re-emission
				} else {
					// Found incomplete level - check if it's timed out
					isTimedOut := hasLevelTimedOut(buf.firstSeen, levelTimeout, c.clock.Now())
					
					if isTimedOut {
						// Timeout on incomplete level - emit it and continue
						logger.WarnCtx(ctx, "Emitting incomplete level due to timeout",
							zap.Uint64("level", level),
							zap.Int("transfers", len(buf.transfers)),
							zap.Int("bigmaps", len(buf.bigmaps)),
							zap.Duration("age", c.clock.Now().Sub(buf.firstSeen)))
						
						if err := c.emitLevel(ctx, buf); err != nil {
							c.reportError(err)
							return
						}
						delete(buffers, level)
						emittedLevels[level] = true
					} else {
						// Incomplete and not timed out - stop here to maintain ascending order
						// Higher levels remain buffered until this level completes or times out
						break
					}
				}
			}

			// Safety: Force-flush oldest level if buffer size exceeded
			if len(buffers) > maxBufferedLevels {
				if err := c.forceFlushOldestLevel(ctx, buffers, emittedLevels); err != nil {
					c.reportError(err)
					return
				}
			}
			
			// Prune emittedLevels map to prevent unbounded growth
			// Keep only recent levels within a sliding window
			if len(emittedLevels) > maxBufferedLevels*2 {
				c.pruneEmittedLevels(emittedLevels, highestTransferLevel, highestBigmapLevel, maxBufferedLevels)
			}
		}
	}
}

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

func (c *tzSubscriber) reportError(err error) {
	select {
	case c.errCh <- err:
	default:
	}
}

// emitLevel emits all events from a level buffer to the handler.
// Events are emitted in deterministic order: transfers first, then bigmaps.
//
// Note: Events are passed to runner which enqueues jobs with unique keys.
// If this level is emitted multiple times (e.g., due to very old late arrival after pruning):
// - Runner prevents duplicate active jobs (pending/running)
// - If original job already terminal (succeeded/failed), new job may be created
// - Event processing must be idempotent to handle this rare edge case
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

// forceFlushOldestLevel force-flushes the lowest buffered level when buffer size is exceeded.
// This prevents memory exhaustion if one feed is stuck or lagging significantly.
//
// Reason: Unbounded buffering could cause OOM if one feed stops sending events.
// Trade-offs: May emit partial level data, but prevents indefinite blocking.
// Constraints: Only triggers when buffer exceeds maxBufferedLevels threshold.
//
// Safety: Flushes lowest level (not oldest by time) to maintain ascending emission order.
// This ensures cursor progression is monotonic and prevents cursor regression.
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

// pruneEmittedLevels removes old entries from emittedLevels to prevent unbounded memory growth.
// Keeps a sliding window of recent levels around the current processing position.
//
// Reason: emittedLevels tracks every emitted level, growing without bound as chain height increases.
// For long-running subscribers, this causes memory leak that could lead to OOM.
//
// Trade-offs:
// - Sacrifices strict single-emission guarantee at subscriber level for very old late arrivals
// - Very old late arrivals (>40 levels / ~10 min old) won't be detected as duplicates here
// - Acceptable because:
//   * Late arrivals >10 min old indicate severe feed issues (rare in practice)
//   * Runner-level job queue prevents duplicate ACTIVE jobs (pending/running)
//   * Event processing is expected to be idempotent (duplicate after terminal job is safe)
//   * Without pruning, memory grows indefinitely (1 entry per level forever)
//
// Constraints: Keeps 2x maxBufferedLevels as safety margin (default: 40 levels, ~10 min).
//
// Safety: Uses maximum of both feeds for pruning baseline to ensure forward progress
// even if one feed is stalled. This prevents unbounded growth under feed asymmetry.
//
// Guarantee: Subscriber provides "best-effort single-emission within sliding window";
// runner prevents duplicate active jobs; processing must be idempotent for terminal-job case.
func (c *tzSubscriber) pruneEmittedLevels(emittedLevels map[uint64]bool, highestTransferLevel, highestBigmapLevel uint64, maxBufferedLevels int) {
	// Use maximum level from both feeds as pruning baseline
	// This ensures pruning progresses even if one feed is stalled
	maxTrackedLevel := highestTransferLevel
	if highestBigmapLevel > maxTrackedLevel {
		maxTrackedLevel = highestBigmapLevel
	}
	
	// Prune threshold: keep levels within 2x buffer size of max tracked level
	// Guard against uint64 underflow (important during startup or low levels)
	window := uint64(maxBufferedLevels * 2)
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
