package ethereum

import (
	"context"
	"errors"
	"fmt"
	"math/big"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"

	"github.com/feral-file/ff-indexer-v2/internal/blockchain"
	"github.com/feral-file/ff-indexer-v2/internal/domain"
	"github.com/feral-file/ff-indexer-v2/internal/logger"
	"github.com/feral-file/ff-indexer-v2/internal/providers/ethereum/helpers"
	contractregistry "github.com/feral-file/ff-indexer-v2/internal/providers/ethereum/registry"
)

const (
	defaultLogBufferSize = 2048
)

// Config holds the configuration for Ethereum subscription
type Config struct {
	WebSocketURL string       // WebSocket URL (e.g., wss://mainnet.infura.io/ws/v3/YOUR_PROJECT_ID)
	ChainID      domain.Chain // e.g., "eip155:1" for Ethereum mainnet
}

type ethSubscriber struct {
	client          EthereumClient
	chainID         domain.Chain
	cfg             Config
	adapterRegistry *contractregistry.AdapterRegistry
}

// NewSubscriber creates a new Ethereum event subscriber.
func NewSubscriber(cfg Config, ethereumClient EthereumClient, adapterRegistry *contractregistry.AdapterRegistry) (blockchain.EventSource, error) {
	return &ethSubscriber{
		client:          ethereumClient,
		chainID:         cfg.ChainID,
		cfg:             cfg,
		adapterRegistry: adapterRegistry,
	}, nil
}

// SubscribeEvents subscribes to ERC721/ERC1155 transfer and metadata update events
func (s *ethSubscriber) SubscribeEvents(ctx context.Context, fromBlock uint64, handler blockchain.EventHandler) error {
	allEventSignatures := helpers.StandardEventSignatures()
	if s.adapterRegistry != nil {
		allEventSignatures = append(allEventSignatures, s.adapterRegistry.GetCustomEventSignaturesForChain(s.chainID)...)
	}

	query := ethereum.FilterQuery{
		FromBlock: new(big.Int).SetUint64(fromBlock),
		Topics: [][]common.Hash{
			allEventSignatures,
		},
	}

	logs := make(chan types.Log, defaultLogBufferSize)
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
			event, err := s.client.ParseEventLog(ctx, vLog)
			if err != nil {
				if errors.Is(err, context.Canceled) {
					return ctx.Err()
				}
				return fmt.Errorf("parse log at block %d index %d: %w", vLog.BlockNumber, vLog.Index, err)
			}

			if event == nil {
				continue
			}

			if err := handler(event); err != nil {
				return fmt.Errorf("failed to handle ethereum event %s at block %d: %w", event.TxHash, event.BlockNumber, err)
			}
		}
	}
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
