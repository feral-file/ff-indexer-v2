package adapter

import (
	"context"
	"errors"
	"fmt"
	"math/big"
	"net"
	neturl "net/url"
	"strings"
	"syscall"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/ethereum/go-ethereum/rpc"
	"go.uber.org/zap"

	"github.com/feral-file/ff-indexer-v2/internal/logger"
)

// EthClient defines an interface for Ethereum client operations to enable mocking
//
//go:generate mockgen -source=ethclient.go -destination=../mocks/ethclient.go -package=mocks -mock_names=EthClient=MockEthClient
type EthClient interface {
	// SubscribeFilterLogs subscribes to filter logs
	SubscribeFilterLogs(ctx context.Context, query ethereum.FilterQuery, ch chan<- types.Log) (ethereum.Subscription, error)

	// SubscribeNewHead subscribes to new block heads (eth_subscribe newHeads),
	// delivering the node-reported hash — see BlockHead for why not types.Header.
	SubscribeNewHead(ctx context.Context, ch chan<- *BlockHead) (ethereum.Subscription, error)

	// FilterLogs retrieves logs that match the filter query
	FilterLogs(ctx context.Context, query ethereum.FilterQuery) ([]types.Log, error)

	// BlockByNumber returns a block by number
	BlockByNumber(ctx context.Context, number *big.Int) (*types.Block, error)

	// HeaderByNumber returns a header by number
	HeaderByNumber(ctx context.Context, number *big.Int) (*types.Header, error)

	// HeadByNumber returns the canonical head at a height with the node-reported
	// hash (eth_getBlockByNumber without transactions); see BlockHead
	HeadByNumber(ctx context.Context, number uint64) (*BlockHead, error)

	// BlockReceipts returns every receipt of the block at the given number
	// (eth_getBlockReceipts) — the complete log source for a block whose
	// matching logs exceed the provider's eth_getLogs result cap
	BlockReceipts(ctx context.Context, number *big.Int) ([]*types.Receipt, error)

	// CallContract calls a contract function
	CallContract(ctx context.Context, msg ethereum.CallMsg, blockNumber *big.Int) ([]byte, error)

	// CodeAt returns the code of the given account at the given block number
	CodeAt(ctx context.Context, account common.Address, blockNumber *big.Int) ([]byte, error)

	// TransactionReceipt returns the receipt of a transaction by transaction hash
	TransactionReceipt(ctx context.Context, txHash common.Hash) (*types.Receipt, error)

	// TransactionSender returns the sender address of a transaction
	TransactionSender(ctx context.Context, tx *types.Transaction, block common.Hash, index uint) (common.Address, error)

	// Close closes the connection
	Close()
}

// BlockHead is the subset of a newHeads notification that chain ingestion needs.
//
// Reason: ethclient.SubscribeNewHead decodes notifications into types.Header,
// which drops the node-reported "hash". Recomputing it with Header.Hash() does
// not reproduce it for current mainnet headers — the decoded struct lacks
// fields newer than this go-ethereum version's RLP encoding (verified live:
// every local hash differed from eth_getBlockByNumber's while each head's
// parentHash matched the node hash of its predecessor). Parent-hash continuity
// therefore has to be checked against the wire hash, so ingestion subscribes
// through the raw RPC client into this type instead.
type BlockHead struct {
	Number     hexutil.Uint64 `json:"number"`
	Hash       common.Hash    `json:"hash"`
	ParentHash common.Hash    `json:"parentHash"`
}

// EthClientDialer defines an interface for dialing Ethereum clients
//
//go:generate mockgen -source=ethclient.go -destination=../mocks/ethclient.go -package=mocks -mock_names=EthClientDialer=MockEthClientDialer
type EthClientDialer interface {
	Dial(ctx context.Context, rawurl string) (EthClient, error)
}

// RealEthClientDialer implements EthClientDialer using the standard ethclient package
type RealEthClientDialer struct{}

// NewEthClientDialer creates a new real Ethereum client dialer
func NewEthClientDialer() EthClientDialer {
	return &RealEthClientDialer{}
}

func (a *RealEthClientDialer) Dial(ctx context.Context, rawurl string) (EthClient, error) {
	client, err := ethclient.DialContext(ctx, rawurl)
	if err != nil {
		return nil, err
	}
	return NewRealEthClient(client, rawurl), nil
}

// RealEthClient wraps the ethclient.Client with retry logic
type RealEthClient struct {
	client *ethclient.Client
	url    string
}

// NewRealEthClient creates a new RealEthClient. The stored URL is reduced to
// scheme and host: it exists only for log context, and provider URLs carry the
// API key in the path (Infura, Chainstack), which must never reach the logs.
func NewRealEthClient(client *ethclient.Client, url string) *RealEthClient {
	return &RealEthClient{
		client: client,
		url:    endpointForLogs(url),
	}
}

// endpointForLogs strips everything but scheme and host from an RPC URL.
func endpointForLogs(rawurl string) string {
	parsed, err := neturl.Parse(rawurl)
	if err != nil || parsed.Host == "" {
		return "<redacted>"
	}
	return parsed.Scheme + "://" + parsed.Host
}

// isRetryableEthError determines if an Ethereum error should trigger a retry
func isRetryableEthError(err error) bool {
	// Context cancellation and deadline exceeded are not retryable
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return false
	}

	// Check for url.Error (wraps most HTTP client errors)
	var urlErr *neturl.Error
	if errors.As(err, &urlErr) {
		// Check the underlying error
		err = urlErr.Err
	}

	// Check for net.Error interface (network-related errors)
	var netErr net.Error
	if errors.As(err, &netErr) {
		// Timeout errors are retryable
		if netErr.Timeout() {
			return true
		}
	}

	// Check for specific network errors that are retryable
	var opErr *net.OpError
	if errors.As(err, &opErr) {
		// Connection refused is retryable (server might be temporarily down)
		if errors.Is(opErr.Err, syscall.ECONNREFUSED) {
			return true
		}
		// Connection reset by peer is retryable
		if errors.Is(opErr.Err, syscall.ECONNRESET) {
			return true
		}
		// Network unreachable is retryable
		if errors.Is(opErr.Err, syscall.ENETUNREACH) {
			return true
		}
		// Host unreachable is retryable
		if errors.Is(opErr.Err, syscall.EHOSTUNREACH) {
			return true
		}
	}

	// DNS errors might be retryable
	var dnsErr *net.DNSError
	if errors.As(err, &dnsErr) {
		// Temporary DNS errors are retryable
		if dnsErr.Temporary() {
			return true
		}
		// DNS timeouts are retryable
		if dnsErr.Timeout() {
			return true
		}
		// Permanent DNS errors (e.g., no such host) are not retryable
		return false
	}

	// Check for RPC errors that are retryable
	// Common retryable error messages from Ethereum nodes
	errMsg := err.Error()
	retryableMessages := []string{
		"connection refused",
		"connection reset",
		"connection timed out",
		"query cancelled", //nolint:misspell
		"i/o timeout",
		"broken pipe",
		"rate limit",
		"too many requests",
		"service unavailable",
		"bad gateway",
		"gateway timeout",
		"temporarily unavailable",
		// Matched against the LOWERCASED error message below, so these must be
		// lowercase too: as uppercase literals they could never match, which
		// silently made "unexpected EOF" — a textbook transient failure — a
		// permanent error that aborted an entire owner scan.
		"econnreset",
		"etimedout",
		"eof",
		// Provider-side 500s on a well-formed query (Infura returns a bare
		// "Internal error"). Retrying is bounded by the 5-minute budget above,
		// while not retrying discards every already-paid call in the walk.
		"internal error",
	}

	errMsgLower := strings.ToLower(errMsg)
	for _, retryableMsg := range retryableMessages {
		if strings.Contains(errMsgLower, retryableMsg) {
			return true
		}
	}

	// Check for HTTP status codes in error messages
	if strings.Contains(errMsgLower, "502") || // Bad gateway
		strings.Contains(errMsgLower, "503") || // Service unavailable
		strings.Contains(errMsgLower, "504") { // Gateway timeout
		return true
	}

	// By default, unknown errors are not retryable to avoid infinite retries
	return false
}

// executeWithRetry executes an operation with exponential backoff retry
func (c *RealEthClient) executeWithRetry(ctx context.Context, operation func() error, operationName string) error {
	// Configure exponential backoff
	b := backoff.NewExponentialBackOff()
	b.InitialInterval = 5 * time.Second
	b.MaxInterval = 30 * time.Second
	b.MaxElapsedTime = 5 * time.Minute // Total retry duration
	b.Multiplier = 2.0
	b.RandomizationFactor = 0.5 // Add jitter to prevent thundering herd

	retryOperation := func() error {
		err := operation()
		if err != nil {
			// Check if the error is retryable
			if isRetryableEthError(err) {
				logger.WarnCtx(ctx, "retryable ethereum error encountered",
					zap.Error(err),
					zap.String("operation", operationName),
					zap.String("url", c.url))
				return fmt.Errorf("retryable error: %w", err)
			}
			// Permanent errors should not be retried
			logger.DebugCtx(ctx, "permanent ethereum error encountered",
				zap.Error(err),
				zap.String("operation", operationName),
				zap.String("url", c.url))
			return backoff.Permanent(fmt.Errorf("permanent error: %w", err))
		}
		return nil
	}

	// Execute with retry and context support
	if err := backoff.Retry(retryOperation, backoff.WithContext(b, ctx)); err != nil {
		return fmt.Errorf("ethereum operation %s failed after retries: %w", operationName, err)
	}

	return nil
}

// SubscribeFilterLogs subscribes to filter logs with retry logic
func (c *RealEthClient) SubscribeFilterLogs(ctx context.Context, query ethereum.FilterQuery, ch chan<- types.Log) (ethereum.Subscription, error) {
	var sub ethereum.Subscription
	err := c.executeWithRetry(ctx, func() error {
		var err error
		sub, err = c.client.SubscribeFilterLogs(ctx, query, ch)
		return err
	}, "SubscribeFilterLogs")
	return sub, err
}

// SubscribeNewHead subscribes to new block heads with retry logic. Only the
// subscribe call is retried; a subscription that later fails surfaces through
// its Err channel and is the caller's to re-establish. It uses the raw RPC
// client so the notification's own hash reaches the caller (see BlockHead).
func (c *RealEthClient) SubscribeNewHead(ctx context.Context, ch chan<- *BlockHead) (ethereum.Subscription, error) {
	var sub ethereum.Subscription
	err := c.executeWithRetry(ctx, func() error {
		var err error
		sub, err = c.client.Client().EthSubscribe(ctx, ch, "newHeads")
		return err
	}, "SubscribeNewHead")
	return sub, err
}

// FilterLogs retrieves logs that match the filter query with retry logic
func (c *RealEthClient) FilterLogs(ctx context.Context, query ethereum.FilterQuery) ([]types.Log, error) {
	var logs []types.Log
	err := c.executeWithRetry(ctx, func() error {
		var err error
		logs, err = c.client.FilterLogs(ctx, query)
		return err
	}, "FilterLogs")
	return logs, err
}

// BlockByNumber returns a block by number with retry logic
func (c *RealEthClient) BlockByNumber(ctx context.Context, number *big.Int) (*types.Block, error) {
	var block *types.Block
	err := c.executeWithRetry(ctx, func() error {
		var err error
		block, err = c.client.BlockByNumber(ctx, number)
		return err
	}, "BlockByNumber")
	return block, err
}

// HeaderByNumber returns a header by number with retry logic
func (c *RealEthClient) HeaderByNumber(ctx context.Context, number *big.Int) (*types.Header, error) {
	var header *types.Header
	err := c.executeWithRetry(ctx, func() error {
		var err error
		header, err = c.client.HeaderByNumber(ctx, number)
		return err
	}, "HeaderByNumber")
	return header, err
}

// HeadByNumber fetches the canonical head at a height with retry logic. It goes
// through the raw RPC client for the same reason as SubscribeNewHead: only the
// wire hash is comparable with subscription hashes.
func (c *RealEthClient) HeadByNumber(ctx context.Context, number uint64) (*BlockHead, error) {
	var head *BlockHead
	err := c.executeWithRetry(ctx, func() error {
		var result *BlockHead
		if err := c.client.Client().CallContext(ctx, &result, "eth_getBlockByNumber", hexutil.EncodeUint64(number), false); err != nil {
			return err
		}
		if result == nil {
			return backoff.Permanent(fmt.Errorf("block %d: %w", number, ethereum.NotFound))
		}
		head = result
		return nil
	}, "HeadByNumber")
	return head, err
}

// BlockReceipts returns the receipts of a block with retry logic
func (c *RealEthClient) BlockReceipts(ctx context.Context, number *big.Int) ([]*types.Receipt, error) {
	var receipts []*types.Receipt
	err := c.executeWithRetry(ctx, func() error {
		var err error
		receipts, err = c.client.BlockReceipts(ctx, rpc.BlockNumberOrHashWithNumber(rpc.BlockNumber(number.Int64())))
		return err
	}, "BlockReceipts")
	return receipts, err
}

// CallContract calls a contract function with retry logic
func (c *RealEthClient) CallContract(ctx context.Context, msg ethereum.CallMsg, blockNumber *big.Int) ([]byte, error) {
	var result []byte
	err := c.executeWithRetry(ctx, func() error {
		var err error
		result, err = c.client.CallContract(ctx, msg, blockNumber)
		return err
	}, "CallContract")
	return result, err
}

// CodeAt returns the code of the given account at the given block number with retry logic
func (c *RealEthClient) CodeAt(ctx context.Context, account common.Address, blockNumber *big.Int) ([]byte, error) {
	var code []byte
	err := c.executeWithRetry(ctx, func() error {
		var err error
		code, err = c.client.CodeAt(ctx, account, blockNumber)
		return err
	}, "CodeAt")
	return code, err
}

// TransactionReceipt returns the receipt of a transaction by transaction hash with retry logic
func (c *RealEthClient) TransactionReceipt(ctx context.Context, txHash common.Hash) (*types.Receipt, error) {
	var receipt *types.Receipt
	err := c.executeWithRetry(ctx, func() error {
		var err error
		receipt, err = c.client.TransactionReceipt(ctx, txHash)
		// ethereum.NotFound is a permanent error, not retryable
		if errors.Is(err, ethereum.NotFound) {
			return backoff.Permanent(err)
		}
		return err
	}, "TransactionReceipt")
	return receipt, err
}

// TransactionSender returns the sender address of a transaction with retry logic
func (c *RealEthClient) TransactionSender(ctx context.Context, tx *types.Transaction, block common.Hash, index uint) (common.Address, error) {
	var sender common.Address
	err := c.executeWithRetry(ctx, func() error {
		var err error
		sender, err = c.client.TransactionSender(ctx, tx, block, index)
		return err
	}, "TransactionSender")
	return sender, err
}

// Close closes the connection
func (c *RealEthClient) Close() {
	c.client.Close()
}
