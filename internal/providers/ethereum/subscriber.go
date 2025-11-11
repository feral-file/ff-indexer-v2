package ethereum

import (
	"context"
	"errors"
	"fmt"
	"math/big"
	"time"

	"github.com/alitto/pond/v2"
	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"go.uber.org/zap"

	"github.com/feral-file/ff-indexer-v2/internal/adapter"
	"github.com/feral-file/ff-indexer-v2/internal/domain"
	"github.com/feral-file/ff-indexer-v2/internal/logger"
	"github.com/feral-file/ff-indexer-v2/internal/messaging"
)

const (
	DEFAULT_WORKER_POOL_SIZE  = 20
	DEFAULT_WORKER_QUEUE_SIZE = 2048
)

// Config holds the configuration for Ethereum subscription
type Config struct {
	WebSocketURL    string       // WebSocket URL (e.g., wss://mainnet.infura.io/ws/v3/YOUR_PROJECT_ID)
	ChainID         domain.Chain // e.g., "eip155:1" for Ethereum mainnet
	WorkerPoolSize  int          // Number of concurrent workers
	WorkerQueueSize int          // Size of the task queue
}

type ethSubscriber struct {
	client  EthereumClient
	chainID domain.Chain
	clock   adapter.Clock
	cfg     Config
}

// Event signatures
var (
	// Transfer event signature - shared by ERC20 and ERC721
	// ERC20: Transfer(address indexed from, address indexed to, uint256 value) - 3 topics
	// ERC721: Transfer(address indexed from, address indexed to, uint256 indexed tokenId) - 4 topics
	transferEventSignature = crypto.Keccak256Hash([]byte("Transfer(address,address,uint256)"))

	// ERC1155 TransferSingle(address indexed operator, address indexed from, address indexed to, uint256 id, uint256 value)
	transferSingleEventSignature = crypto.Keccak256Hash([]byte("TransferSingle(address,address,address,uint256,uint256)"))

	// ERC1155 TransferBatch(address indexed operator, address indexed from, address indexed to, uint256[] ids, uint256[] values)
	transferBatchEventSignature = crypto.Keccak256Hash([]byte("TransferBatch(address,address,address,uint256[],uint256[])"))

	// EIP-4906 MetadataUpdate(uint256 _tokenId)
	metadataUpdateEventSignature = crypto.Keccak256Hash([]byte("MetadataUpdate(uint256)"))

	// EIP-4906 BatchMetadataUpdate(uint256 _fromTokenId, uint256 _toTokenId)
	batchMetadataUpdateEventSignature = crypto.Keccak256Hash([]byte("BatchMetadataUpdate(uint256,uint256)"))

	// ERC1155 URI(string _value, uint256 indexed _id)
	uriEventSignature = crypto.Keccak256Hash([]byte("URI(string,uint256)"))
)

// NewSubscriber creates a new Ethereum event subscriber
func NewSubscriber(ctx context.Context, cfg Config, ethereumClient EthereumClient, clock adapter.Clock) (messaging.Subscriber, error) {
	return &ethSubscriber{
		client:  ethereumClient,
		chainID: cfg.ChainID,
		clock:   clock,
		cfg:     cfg,
	}, nil
}

// SubscribeEvents subscribes to ERC721/ERC1155 transfer and metadata update events
func (s *ethSubscriber) SubscribeEvents(ctx context.Context, fromBlock uint64, handler messaging.EventHandler) error {
	// Get worker pool configuration from config or use defaults
	workerPoolSize := s.cfg.WorkerPoolSize
	if workerPoolSize == 0 {
		workerPoolSize = DEFAULT_WORKER_POOL_SIZE
	}
	workerQueueSize := s.cfg.WorkerQueueSize
	if workerQueueSize == 0 {
		workerQueueSize = DEFAULT_WORKER_QUEUE_SIZE
	}

	// Create worker pool for concurrent event processing
	pool := pond.NewPool(
		workerPoolSize,
		pond.WithQueueSize(workerQueueSize),
		pond.WithContext(ctx),
	)

	logger.InfoCtx(ctx, "Ethereum worker pool created",
		zap.Int("workers", workerPoolSize),
		zap.Int("queue_size", workerQueueSize),
		zap.String("chain", string(s.chainID)))

	// Ensure graceful shutdown of worker pool
	defer func() {
		logger.InfoCtx(ctx, "Shutting down ethereum worker pool",
			zap.Uint64("submitted", pool.SubmittedTasks()),
			zap.Uint64("waiting", pool.WaitingTasks()),
			zap.Uint64("successful", pool.SuccessfulTasks()),
			zap.Uint64("failed", pool.FailedTasks()))

		pool.StopAndWait()

		logger.InfoCtx(ctx, "Ethereum worker pool shutdown complete",
			zap.Uint64("total_submitted", pool.SubmittedTasks()),
			zap.Uint64("total_completed", pool.CompletedTasks()),
			zap.Uint64("total_failed", pool.FailedTasks()))
	}()

	// Start periodic metrics logging
	metricsTicker := s.clock.NewTicker(30 * time.Second)
	defer metricsTicker.Stop()

	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-metricsTicker.C:
				logger.InfoCtx(ctx, "Ethereum worker pool metrics",
					zap.Int64("running_workers", pool.RunningWorkers()),
					zap.Uint64("waiting_tasks", pool.WaitingTasks()),
					zap.Uint64("completed_tasks", pool.CompletedTasks()),
					zap.Uint64("failed_tasks", pool.FailedTasks()))
			}
		}
	}()

	query := ethereum.FilterQuery{
		FromBlock: new(big.Int).SetUint64(fromBlock),
		Topics: [][]common.Hash{
			{
				transferEventSignature,            // ERC20/ERC721 Transfer (will filter ERC20 in parseLog)
				transferSingleEventSignature,      // ERC1155 TransferSingle
				transferBatchEventSignature,       // ERC1155 TransferBatch
				metadataUpdateEventSignature,      // EIP-4906 MetadataUpdate
				batchMetadataUpdateEventSignature, // EIP-4906 BatchMetadataUpdate
				uriEventSignature,                 // ERC1155 URI
			},
		},
	}

	logs := make(chan types.Log, workerQueueSize)
	sub, err := s.client.SubscribeFilterLogs(ctx, query, logs)
	if err != nil {
		return fmt.Errorf("failed to subscribe to filter logs: %w", err)
	}
	defer func() {
		logger.InfoCtx(ctx, "Unsubscribing from ethereum events logs")
		sub.Unsubscribe()
		logger.InfoCtx(ctx, "Unsubscribed from ethereum events logs")
	}()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case err := <-sub.Err():
			return fmt.Errorf("subscription error: %w", err)
		case vLog := <-logs:
			// Make an explicit copy to avoid closure capture issues with loop variable
			logEntry := vLog

			// Submit event processing to worker pool instead of spawning unbounded goroutines
			pool.SubmitErr(func() error {
				event, err := s.client.ParseEventLog(ctx, logEntry)
				if err != nil && !errors.Is(err, context.Canceled) {
					logger.ErrorCtx(ctx, errors.New("Error parsing log"), zap.Error(err))
					return nil
				}

				if event == nil {
					return nil
				}

				if err := handler(event); err != nil {
					logger.ErrorCtx(ctx, err,
						zap.Error(errors.New("Error handling event")),
						zap.String("tx_hash", event.TxHash),
						zap.Uint64("block", event.BlockNumber))
					return err
				}
				return nil
			})
		}
	}
}

// GetLatestBlock returns the latest block number
func (s *ethSubscriber) GetLatestBlock(ctx context.Context) (uint64, error) {
	header, err := s.client.HeaderByNumber(ctx, nil)
	if err != nil {
		return 0, fmt.Errorf("failed to get latest block: %w", err)
	}
	return header.Number.Uint64(), nil
}

// Close closes the connection
func (s *ethSubscriber) Close() {
	if s.client == nil {
		return
	}

	s.client.Close()
	logger.Info("Ethereum WebSocket connection closed")
}
