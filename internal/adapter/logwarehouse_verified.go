package adapter

import (
	"context"
	"errors"
	"fmt"
	"math/big"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/rpc"
	"go.uber.org/zap"

	"github.com/feral-file/ff-indexer-v2/internal/logger"
)

// ErrLogWarehouseUnverified marks a warehouse whose identity could not be
// checked yet (it did not answer). It is transient: the next call verifies
// again. Nothing is routed through an unverified warehouse.
var ErrLogWarehouseUnverified = errors.New("log warehouse not verified")

// ErrLogWarehouseChainMismatch marks a warehouse that stores a different chain
// than the indexer is configured for. Permanent: routing stays disabled for
// the life of the process (serving Sepolia history as mainnet, or the reverse,
// would silently corrupt every scan).
var ErrLogWarehouseChainMismatch = errors.New("log warehouse chain mismatch")

// ErrLogWarehouseProbeFailed marks a warehouse that answered a capability
// probe without the logs the indexer depends on. Permanent for the same
// reason: a warehouse that lacks a required shape would return complete-looking
// answers with those logs missing, and a routing client cannot tell.
var ErrLogWarehouseProbeFailed = errors.New("log warehouse capability probe failed")

// LogWarehouseProbe is one capability check: a filter whose answer must satisfy
// Accept before the warehouse is trusted. Probes exist because the warehouse
// contract is "the vendor's answer minus a documented set of shapes", and the
// indexer's correctness depends on which shapes are in that set: a warehouse
// build or backfill that omits a shape a call site relies on must be refused,
// not silently routed to.
type LogWarehouseProbe struct {
	// Name identifies the probe in logs and errors.
	Name string
	// Query is sent as-is; it should be narrow (a single block) so the probe
	// is cheap and cannot trip the warehouse result cap.
	Query ethereum.FilterQuery
	// Accept reports whether the served logs prove the capability.
	Accept func(logs []types.Log) bool
}

// LogWarehouseRequirements is what a warehouse must satisfy before any query
// is routed through it.
type LogWarehouseRequirements struct {
	// ChainID the warehouse must report from eth_chainId.
	ChainID uint64
	// Probes are checked in order after the chain id; all must pass.
	Probes []LogWarehouseProbe
}

// VerifiedLogWarehouse gates every request on a one-time verification of the
// warehouse's identity and capabilities.
//
// Reason: the warehouse endpoint is a bare URL. A startup check alone is not
// enough — a warehouse that is down at startup is tolerated (routing falls
// through until it answers), so without this gate a wrong-chain or
// wrong-build warehouse that comes up later would be routed to unverified.
// Verification therefore happens lazily on the first request that reaches an
// unverified warehouse, and its outcome is sticky: success routes for the life
// of the process, a mismatch or failed probe disables routing permanently
// (every request returns the permanent error and the caller falls through to
// the vendor), a transport error leaves the warehouse unverified so the next
// request tries again.
//
// Constraints — "fall through, never stall" applies to verification too. The
// RPCs run outside the mutex; a caller that arrives while another goroutine's
// verification is in flight does not wait for it but fails at once with
// ErrLogWarehouseUnverified (and falls through to the vendor), and a transient
// failure is cached for retryInterval so that an outage costs one bounded
// attempt per interval, not one timeout per concurrent caller. Owner scans
// issue their merged queries concurrently and ingestion fetches every block,
// so without both rules an unreachable warehouse would serialize minutes of
// timeouts in front of a healthy vendor.
type VerifiedLogWarehouse struct {
	inner         LogWarehouse
	reqs          LogWarehouseRequirements
	clock         Clock
	retryInterval time.Duration

	mu          sync.Mutex
	verified    bool
	generation  uint64 // incremented on every successful verification
	disabled    error
	inFlight    bool
	lastFailure error
	nextAttempt time.Time
}

// DefaultVerifyRetryInterval is how long a transient verification failure is
// remembered before the next request tries again.
const DefaultVerifyRetryInterval = 30 * time.Second

// NewVerifiedLogWarehouse wraps inner so that no request is served until reqs
// are met. Verify may be called eagerly at startup to fail fast on a permanent
// problem. A non-positive retryInterval uses DefaultVerifyRetryInterval.
func NewVerifiedLogWarehouse(inner LogWarehouse, reqs LogWarehouseRequirements, clock Clock, retryInterval time.Duration) *VerifiedLogWarehouse {
	if retryInterval <= 0 {
		retryInterval = DefaultVerifyRetryInterval
	}
	return &VerifiedLogWarehouse{inner: inner, reqs: reqs, clock: clock, retryInterval: retryInterval}
}

// Verify checks the requirements unless already decided. It returns nil once
// the warehouse is trusted, ErrLogWarehouseUnverified (wrapped) when the
// warehouse did not answer — or is being verified by another caller, or
// failed within the last retryInterval — and ErrLogWarehouseChainMismatch or
// ErrLogWarehouseProbeFailed (wrapped) when it answered wrongly; the latter
// two are remembered and returned by every later call.
func (w *VerifiedLogWarehouse) Verify(ctx context.Context) error {
	_, err := w.verify(ctx)
	return err
}

// verify is Verify plus the generation the caller is routed under: observe
// demotes only for a failure from that generation, so a request that was
// already in flight when the warehouse was demoted and re-verified cannot
// knock the recovered warehouse back down.
func (w *VerifiedLogWarehouse) verify(ctx context.Context) (uint64, error) {
	if gen, err, decided := w.begin(); decided {
		return gen, err
	}
	err := w.check(ctx)
	return w.finish(ctx, err), err
}

// begin decides under the lock whether this caller runs a verification:
// decided=true means the answer is already known (verified, disabled, in
// flight elsewhere, or inside the retry cooldown) and err is that answer; gen
// is the current verification generation.
func (w *VerifiedLogWarehouse) begin() (uint64, error, bool) { //nolint:revive // error-before-bool reads best at the call site
	w.mu.Lock()
	defer w.mu.Unlock()
	switch {
	case w.verified:
		return w.generation, nil, true
	case w.disabled != nil:
		return w.generation, w.disabled, true
	case w.inFlight:
		return w.generation, fmt.Errorf("%w: verification in progress", ErrLogWarehouseUnverified), true
	case w.lastFailure != nil && w.clock.Now().Before(w.nextAttempt):
		return w.generation, w.lastFailure, true
	}
	w.inFlight = true
	return w.generation, nil, false
}

// finish records the outcome of a verification attempt under the lock. An
// attempt that ended because the caller's own context was done says nothing
// about the warehouse, so it is not cached: the next caller verifies at once.
func (w *VerifiedLogWarehouse) finish(ctx context.Context, err error) uint64 {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.inFlight = false
	switch {
	case err == nil:
		w.verified, w.lastFailure = true, nil
		w.generation++
		logger.InfoCtx(ctx, "Log warehouse verified; historical eth_getLogs is routed through it",
			zap.Uint64("chainId", w.reqs.ChainID), zap.Int("probes", len(w.reqs.Probes)))
	case errors.Is(err, ErrLogWarehouseChainMismatch), errors.Is(err, ErrLogWarehouseProbeFailed):
		w.disabled = err
		logger.ErrorCtx(ctx, fmt.Errorf("log warehouse refused; every eth_getLogs falls through to the vendor until restart: %w", err))
	case ctx.Err() != nil:
		// caller-specific; leave lastFailure/nextAttempt untouched
	default:
		w.lastFailure = err
		w.nextAttempt = w.clock.Now().Add(w.retryInterval)
	}
	return w.generation
}

// observe applies the outage cooldown to a request that failed AFTER
// verification: a warehouse that stops answering is demoted to unverified with
// the same retry interval, so the callers that follow fall through at once
// instead of each paying a timeout. Only outages count — an error the
// warehouse itself answered with (any rpc.Error: the result-cap split signal,
// a scope refusal) proves it is alive, and a failure caused by the caller's
// own context says nothing about it. gen is the generation the request was
// routed under: a failure from an older generation is stale — the warehouse
// was demoted and re-verified while the request was in flight — and must not
// demote the recovered one.
func (w *VerifiedLogWarehouse) observe(ctx context.Context, gen uint64, err error) {
	if err == nil || ctx.Err() != nil || IsOutOfScope(err) {
		return
	}
	var rpcErr rpc.Error
	if errors.As(err, &rpcErr) {
		return
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if !w.verified || w.generation != gen {
		return
	}
	w.verified = false
	w.lastFailure = fmt.Errorf("%w: %w", ErrLogWarehouseUnverified, err)
	w.nextAttempt = w.clock.Now().Add(w.retryInterval)
	logger.WarnCtx(ctx, "Log warehouse stopped answering; eth_getLogs falls through to the vendor until it re-verifies",
		zap.Duration("retryAfter", w.retryInterval), zap.Error(err))
}

// check runs the chain-id comparison and the probes once.
func (w *VerifiedLogWarehouse) check(ctx context.Context) error {
	got, err := w.inner.ChainID(ctx)
	if err != nil {
		return fmt.Errorf("%w: eth_chainId: %w", ErrLogWarehouseUnverified, err)
	}
	if want := new(big.Int).SetUint64(w.reqs.ChainID); got.Cmp(want) != 0 {
		return fmt.Errorf("%w: warehouse serves chain id %s, indexer is configured for %s", ErrLogWarehouseChainMismatch, got, want)
	}
	for _, probe := range w.reqs.Probes {
		if err := w.runProbe(ctx, probe); err != nil {
			return err
		}
	}
	return nil
}

// runProbe sends one probe. A scope refusal is permanent — the warehouse
// does not cover the probe's block, so it cannot hold the shape either — while
// any other error keeps the warehouse unverified.
func (w *VerifiedLogWarehouse) runProbe(ctx context.Context, probe LogWarehouseProbe) error {
	logs, err := w.inner.FilterLogs(ctx, probe.Query, nil)
	switch {
	case IsOutOfScope(err):
		return fmt.Errorf("%w: probe %q refused: %w", ErrLogWarehouseProbeFailed, probe.Name, err)
	case err != nil:
		return fmt.Errorf("%w: probe %q: %w", ErrLogWarehouseUnverified, probe.Name, err)
	case !probe.Accept(logs):
		return fmt.Errorf("%w: probe %q returned %d logs without the required shape", ErrLogWarehouseProbeFailed, probe.Name, len(logs))
	}
	return nil
}

// Head serves the warehouse head once verified.
func (w *VerifiedLogWarehouse) Head(ctx context.Context) (uint64, error) {
	gen, err := w.verify(ctx)
	if err != nil {
		return 0, err
	}
	head, err := w.inner.Head(ctx)
	w.observe(ctx, gen, err)
	return head, err
}

// FilterLogs serves a filter once verified. erc1155ID is forwarded to the
// warehouse's erc1155Id filter unchanged (see LogWarehouse.FilterLogs).
func (w *VerifiedLogWarehouse) FilterLogs(ctx context.Context, query ethereum.FilterQuery, erc1155ID *common.Hash) ([]types.Log, error) {
	gen, err := w.verify(ctx)
	if err != nil {
		return nil, err
	}
	logs, err := w.inner.FilterLogs(ctx, query, erc1155ID)
	w.observe(ctx, gen, err)
	return logs, err
}

// ChainID passes through: it is the verification input, never gated by it.
func (w *VerifiedLogWarehouse) ChainID(ctx context.Context) (*big.Int, error) {
	return w.inner.ChainID(ctx)
}

// Close closes the wrapped warehouse.
func (w *VerifiedLogWarehouse) Close() {
	w.inner.Close()
}
