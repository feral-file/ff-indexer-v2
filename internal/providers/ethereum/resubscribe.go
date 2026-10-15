package ethereum

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/rpc"
	"go.uber.org/zap"

	"github.com/feral-file/ff-indexer-v2/internal/adapter"
	"github.com/feral-file/ff-indexer-v2/internal/logger"
)

// This file holds the newHeads subscription lifecycle: how a subscription is
// opened, how its death is told apart from an ingestion failure, and how it is
// re-established. SubscribeEvents (subscriber.go) drives it.

const (
	// resubscribeMinDelay is the wait before the first resubscribe attempt after
	// a drop, and the value the schedule returns to once a subscription has
	// delivered a head: a one-off socket reset costs about a second.
	resubscribeMinDelay = time.Second

	// resubscribeMaxDelay caps the doubling wait between attempts, so a provider
	// that is down — or that accepts subscriptions only to drop them before any
	// head — is retried at most every 30 s.
	resubscribeMaxDelay = 30 * time.Second

	// dropGracePeriod is how long an ingestion failure waits for the
	// subscription to report its own death before being treated as the call's
	// own fault (see ingestFailure). go-ethereum fails the in-flight call first
	// and notifies the subscription afterwards, on the subscription's own
	// goroutine, so every drop seen this way pays a scheduling hop here; the
	// full period is only ever waited out on a genuinely fatal error, so it is
	// generous rather than tight.
	dropGracePeriod = 2 * time.Second
)

// errSubscriptionDropped marks a newHeads subscription that ended on its own —
// a transport reset, a provider-side close, or go-ethereum's notification queue
// overflowing — as distinct from an ingestion failure. It is the one error
// SubscribeEvents recovers from in place; everything else still ends the stream.
var errSubscriptionDropped = errors.New("new heads subscription dropped")

// errClientClosed marks the one subscription ending that is terminal rather
// than recoverable: the rpc client itself was closed, so no resubscribe can
// succeed. Seen as a nil value on Subscription.Err() or as rpc.ErrClientQuit
// from a subscribe attempt.
var errClientClosed = errors.New("new heads subscription ended: client closed")

// headStream is one live newHeads subscription with the channel it delivers on.
// Each (re)subscribe gets a fresh channel: whatever is still queued on a dead
// subscription's channel is not carried over — the first head on the new one
// fills the gap from the position, which covers those blocks anyway.
type headStream struct {
	heads chan *adapter.BlockHead
	sub   ethereum.Subscription
}

// subscribeHeads opens a newHeads subscription on a fresh channel.
func (s *ethSubscriber) subscribeHeads(ctx context.Context) (headStream, error) {
	heads := make(chan *adapter.BlockHead, headBufferSize)
	sub, err := s.client.SubscribeNewHead(ctx, heads)
	if err != nil {
		return headStream{}, err
	}
	return headStream{heads: heads, sub: sub}, nil
}

// close ends the subscription, bracketed in logs so a hang in Unsubscribe is
// diagnosable: go-ethereum's unsubscribe request runs with a background
// context, so on a half-open socket it can block for good. The bracket is at
// info while the subscription is still live — shutdown and fatal exits, where
// a wedged socket is exactly where the hang would hide — and at debug once it
// has died on its own, where Unsubscribe is a no-op and the caller logs the
// drop itself.
func (hs headStream) close(ctx context.Context, live bool) {
	log := logger.DebugCtx
	if live {
		log = logger.InfoCtx
	}
	log(ctx, "Unsubscribing from ethereum new heads")
	hs.sub.Unsubscribe()
	log(ctx, "Unsubscribed from ethereum new heads")
}

// subscriptionEnded classifies a value read from Subscription.Err(). go-ethereum
// reports Client.Close as a nil error, and only Unsubscribe closes the channel,
// so either means the client itself is gone: terminal, since resubscribing
// would spin against a closed client. Anything else is a drop to recover from.
func subscriptionEnded(subErr error, ok bool) error {
	if !ok || subErr == nil {
		return errClientClosed
	}
	return fmt.Errorf("%w: %w", errSubscriptionDropped, subErr)
}

// ingestFailure decides whether an error from planRange or ingestRange is
// really the subscription's transport dying under an in-flight call.
//
// Reason: newHeads and the eth_getLogs / eth_getBlockByNumber calls share one
// rpc connection, so when the socket closes mid-fetch the failed call reports
// it first and the subscription only a moment later, on its own goroutine.
// Waiting a grace period for that report lets the drop be recovered however
// the adapter classified the call's error text (its retry list is syscall
// codes plus known strings, which a websocket close frame need not match).
// Trade-offs: the decision is by coincidence in time, not by error identity.
// A call that failed for its own reasons sees no report and stays fatal,
// paying the grace once before the process exits; a fatal error that happens
// to coincide with a drop is retried once, recurs on the next head without
// one, and exits then. A typed transport error from the adapter would make
// this deterministic and is the natural follow-up. Constraints: on shutdown
// the cause is returned as is — it already carries the cancellation — and the
// wait itself observes ctx.
func (s *ethSubscriber) ingestFailure(ctx context.Context, hs headStream, cause error) error {
	if ctx.Err() != nil {
		return cause
	}
	select {
	case subErr, ok := <-hs.sub.Err():
		return fmt.Errorf("%w (surfaced by in-flight call: %w)", subscriptionEnded(subErr, ok), cause)
	case <-s.clock.After(dropGracePeriod):
		return cause
	case <-ctx.Done():
		return cause // shutdown during the grace: the cause already ends the stream
	}
}

// resubscribeBackoff is the wait schedule between resubscribe attempts: it
// doubles on every attempt up to resubscribeMaxDelay and is reset only once a
// subscription has delivered a head (see stream).
type resubscribeBackoff struct {
	delay time.Duration
}

// next returns the wait before the coming attempt and advances the schedule.
func (b *resubscribeBackoff) next() time.Duration {
	d := b.delay
	b.delay = min(d*2, resubscribeMaxDelay)
	return d
}

// reset restarts the schedule after a healthy subscription.
func (b *resubscribeBackoff) reset() {
	b.delay = resubscribeMinDelay
}

// logDrop records a subscription drop before the resubscribe. Reason: the
// ERROR in resubscribe fires only when subscribing itself fails, so a provider
// that accepts the subscription and keeps dropping it — or closes the socket
// on every catch-up fetch — would otherwise be a WARN-only loop with ingestion
// silently stalled. Once the schedule has reached its cap the drops are
// recurring without progress (an emitted range would have reset it), and the
// line escalates to ERROR so the stall is visible.
func logDrop(ctx context.Context, err error, next uint64, retry *resubscribeBackoff) {
	fields := []zap.Field{zap.Error(err), zap.Uint64("nextBlock", next), zap.Duration("retryIn", retry.delay)}
	if retry.delay >= resubscribeMaxDelay {
		logger.ErrorCtx(ctx, errors.New("ethereum new heads subscription keeps dropping without progress; ingestion stalled"), fields...)
		return
	}
	logger.WarnCtx(ctx, "Ethereum new heads subscription dropped; resubscribing", fields...)
}

// resubscribe re-establishes the newHeads subscription after a drop, waiting
// out the schedule before each attempt until one succeeds or ctx ends.
//
// Reason: it never gives up on its own. The subscription worked before, so a
// failure here is a provider or network outage, not misconfiguration, and
// exiting would only move the retry into Docker's restart loop — with the API
// restarting alongside. Every failed attempt is logged at ERROR, and a drop
// that keeps recurring without progress escalates to ERROR too (see logDrop),
// so a long outage is visible rather than silent — including a
// misconfiguration that only surfaces mid-run (a rotated API key): failing
// fast there would just be the restart loop again, so the ERROR line is the
// signal. ethereum.max_catchup_blocks
// still applies to the gap the eventual reconnect finds: an outage longer than
// that ends in ErrCatchupTooLarge on the first head, fatal by design exactly
// as after a restart (and a restart loop until the operator resets the cursor
// or raises the bound — docs/constraints.md), so a lowered bound also shortens
// the outage this loop can ride out. The one failure that is terminal is the
// rpc client itself having been closed (rpc.ErrClientQuit): no attempt can
// succeed, matching subscriptionEnded.
//
// Trade-offs: this schedule spaces attempts, not dials. The adapter's
// SubscribeNewHead already retries connection-level errors (refused, reset,
// timeout) internally with its own jittered 5–30 s backoff for up to five
// minutes, so during a hard outage one attempt here can last that long; the
// doubling matters for errors the adapter calls permanent (an RPC-level
// rejection, say), which would otherwise be retried every second.
// Constraints: both the wait and a failed attempt must observe ctx so that a
// shutdown mid-outage is prompt and is not logged as a resubscribe failure.
func (s *ethSubscriber) resubscribe(ctx context.Context, retry *resubscribeBackoff) (headStream, error) {
	for {
		select {
		case <-ctx.Done():
			return headStream{}, ctx.Err()
		case <-s.clock.After(retry.next()):
		}
		hs, err := s.subscribeHeads(ctx)
		if err == nil {
			logger.InfoCtx(ctx, "Ethereum new heads subscription re-established")
			return hs, nil
		}
		if ctx.Err() != nil {
			return headStream{}, ctx.Err()
		}
		if errors.Is(err, rpc.ErrClientQuit) {
			return headStream{}, fmt.Errorf("%w: %w", errClientClosed, err)
		}
		logger.ErrorCtx(ctx, errors.New("ethereum new heads resubscribe failed; retrying"),
			zap.Error(err), zap.Duration("retryIn", retry.delay))
	}
}
