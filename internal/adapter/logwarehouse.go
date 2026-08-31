package adapter

import (
	"context"
	"errors"
	"fmt"
	"math/big"
	"strings"
	"time"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/ethereum/go-ethereum/rpc"
)

// LogWarehouse is the self-hosted eth_getLogs warehouse (feral-file/ff-eth-logs):
// a PostgreSQL store of every NFT-relevant mainnet log, served through
// go-ethereum's own filter semantics with no block-span cap and no per-call
// billing. It answers only what it stores; anything else is a scope error the
// caller must route to the vendor (see IsOutOfScope).
//
// Reason: it is deliberately NOT an EthClient. It serves three methods, its
// failure policy is "fall through to the vendor at once" rather than the
// vendor client's five-minute retry budget (a warehouse outage must not stall
// ingestion or a scan for minutes per call), and keeping the surface minimal
// makes the routing split explicit: history through here, live state through
// the vendor.
//
//go:generate mockgen -source=logwarehouse.go -destination=../mocks/logwarehouse.go -package=mocks -mock_names=LogWarehouse=MockLogWarehouse,LogWarehouseDialer=MockLogWarehouseDialer
type LogWarehouse interface {
	// Head returns the warehouse head: the last block whose logs are fully
	// stored — NOT the chain tip. It is the split point between the warehouse
	// leg and the vendor leg of a log query.
	Head(ctx context.Context) (uint64, error)

	// FilterLogs runs one eth_getLogs against the warehouse for the whole
	// range at once (no pagination needed: the warehouse has no span cap).
	// Returns an error satisfying IsOutOfScope when the warehouse refuses the
	// filter, and the vendor-style "query returned more than N results" error
	// above the warehouse result cap, which helpers.IsTooManyResultsError
	// recognizes so the caller can split the range.
	FilterLogs(ctx context.Context, query ethereum.FilterQuery) ([]types.Log, error)

	// ChainID returns the chain the warehouse stores (eth_chainId), for the
	// startup check that it matches the indexer's configured chain.
	ChainID(ctx context.Context) (*big.Int, error)

	// Close releases the connection.
	Close()
}

// LogWarehouseDialer creates LogWarehouse clients.
type LogWarehouseDialer interface {
	// Dial connects to the warehouse JSON-RPC endpoint. timeout bounds every
	// individual request; see NewRealLogWarehouse.
	Dial(ctx context.Context, rawurl string, timeout time.Duration) (LogWarehouse, error)
}

// RealLogWarehouseDialer implements LogWarehouseDialer over go-ethereum's rpc client.
type RealLogWarehouseDialer struct{}

// NewLogWarehouseDialer creates the real dialer.
func NewLogWarehouseDialer() LogWarehouseDialer {
	return &RealLogWarehouseDialer{}
}

// Dial connects to the warehouse. HTTP endpoints do not open a connection
// here, so a warehouse that is down at startup is only discovered by the
// first request — by design: the routing client treats it as "fall through".
func (d *RealLogWarehouseDialer) Dial(ctx context.Context, rawurl string, timeout time.Duration) (LogWarehouse, error) {
	client, err := rpc.DialContext(ctx, rawurl)
	if err != nil {
		return nil, err
	}
	return NewRealLogWarehouse(client, timeout), nil
}

// scopeErrorPrefix is the message prefix ff-eth-logs puts on every refusal it
// wants a routing client to treat as "ask the vendor" (rpcapi.ScopeError). It
// is part of the warehouse's API contract (docs/api_design.md there): the
// wording avoids "range", "limit" and "too many" so the pagination helper's
// result-cap classifier never mistakes a scope error for a window to halve.
const scopeErrorPrefix = "out of warehouse scope"

// scopeErrorCode is the JSON-RPC code the warehouse uses for scope errors
// (geth's default handler-error code).
const scopeErrorCode = -32000

// ErrOutOfScope is the sentinel returned (wrapped) by RealLogWarehouse when the
// warehouse refuses a request as outside its stored set or covered interval.
var ErrOutOfScope = errors.New("log warehouse: out of scope")

// IsOutOfScope reports whether err is a warehouse scope refusal — the one
// warehouse error class that is expected in normal operation (the range is
// above the head, below coverage, or under maintenance) and means "route this
// query to the vendor", never "retry" and never "split the window".
func IsOutOfScope(err error) bool {
	if errors.Is(err, ErrOutOfScope) {
		return true
	}
	var rpcErr rpc.Error
	return errors.As(err, &rpcErr) &&
		rpcErr.ErrorCode() == scopeErrorCode &&
		strings.HasPrefix(rpcErr.Error(), scopeErrorPrefix)
}

// RealLogWarehouse is the production LogWarehouse over a JSON-RPC connection.
//
// Reason: every request runs exactly once under its own deadline — no retry,
// no backoff. The warehouse sits on the private network, so a failure is
// either a real outage (which the caller handles by falling through to the
// vendor immediately) or a query that legitimately exceeds the deadline (which
// the vendor walk then serves at its usual cost). Retrying here would only
// delay that decision.
type RealLogWarehouse struct {
	rpc     *rpc.Client
	eth     *ethclient.Client
	timeout time.Duration
}

// DefaultLogWarehouseTimeout bounds one warehouse request when the caller
// passes no timeout. It matches the warehouse's own server write timeout.
const DefaultLogWarehouseTimeout = 120 * time.Second

// NewRealLogWarehouse wraps an rpc client. A non-positive timeout falls back
// to DefaultLogWarehouseTimeout: an unbounded warehouse call would hold a
// scan window or an ingestion batch for as long as a wedged connection lasts.
func NewRealLogWarehouse(client *rpc.Client, timeout time.Duration) *RealLogWarehouse {
	if timeout <= 0 {
		timeout = DefaultLogWarehouseTimeout
	}
	return &RealLogWarehouse{rpc: client, eth: ethclient.NewClient(client), timeout: timeout}
}

// bounded applies the per-request deadline.
func (w *RealLogWarehouse) bounded(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(ctx, w.timeout)
}

// Head returns the warehouse head via eth_blockNumber.
func (w *RealLogWarehouse) Head(ctx context.Context) (uint64, error) {
	ctx, cancel := w.bounded(ctx)
	defer cancel()
	var head hexutil.Uint64
	if err := w.rpc.CallContext(ctx, &head, "eth_blockNumber"); err != nil {
		return 0, w.classify(err)
	}
	return uint64(head), nil
}

// FilterLogs runs eth_getLogs. The decoded logs carry blockTimestamp (the
// warehouse always sets it), which lets helpers.BaseEventFromLog skip the
// per-block eth_getBlockByNumber lookup it otherwise needs for vendor logs.
func (w *RealLogWarehouse) FilterLogs(ctx context.Context, query ethereum.FilterQuery) ([]types.Log, error) {
	ctx, cancel := w.bounded(ctx)
	defer cancel()
	logs, err := w.eth.FilterLogs(ctx, query)
	if err != nil {
		return nil, w.classify(err)
	}
	return logs, nil
}

// ChainID returns eth_chainId.
func (w *RealLogWarehouse) ChainID(ctx context.Context) (*big.Int, error) {
	ctx, cancel := w.bounded(ctx)
	defer cancel()
	id, err := w.eth.ChainID(ctx)
	if err != nil {
		return nil, w.classify(err)
	}
	return id, nil
}

// Close closes the connection.
func (w *RealLogWarehouse) Close() {
	w.rpc.Close()
}

// classify wraps scope refusals in ErrOutOfScope so callers can errors.Is on
// the sentinel; every other error passes through unchanged (the rpc.Error code
// and message survive for the too-many-results classifier).
func (w *RealLogWarehouse) classify(err error) error {
	if IsOutOfScope(err) {
		return fmt.Errorf("%w: %w", ErrOutOfScope, err)
	}
	return err
}
