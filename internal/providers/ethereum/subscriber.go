package ethereum

import (
	"context"
	"fmt"
	"math/big"
	"strings"

	logger "github.com/bitmark-inc/autonomy-logger"
	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"go.uber.org/zap"

	"github.com/feral-file/ff-indexer-v2/internal/adapter"
	"github.com/feral-file/ff-indexer-v2/internal/domain"
	"github.com/feral-file/ff-indexer-v2/internal/subscriber"
)

// Config holds the configuration for Ethereum subscription
type Config struct {
	WebSocketURL string       // WebSocket URL (e.g., wss://mainnet.infura.io/ws/v3/YOUR_PROJECT_ID)
	ChainID      domain.Chain // e.g., "eip155:1" for Ethereum mainnet
}

type ethSubscriber struct {
	client  adapter.EthClient
	chainID domain.Chain
	clock   adapter.Clock
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
func NewSubscriber(cfg Config, ethClientDialer adapter.EthClientDialer, clock adapter.Clock) (subscriber.Subscriber, error) {
	client, err := ethClientDialer.Dial(cfg.WebSocketURL)
	if err != nil {
		return nil, fmt.Errorf("failed to dial Ethereum WebSocket: %w", err)
	}

	return &ethSubscriber{
		client:  client,
		chainID: cfg.ChainID,
		clock:   clock,
	}, nil
}

// SubscribeEvents subscribes to ERC721/ERC1155 transfer and metadata update events
func (s *ethSubscriber) SubscribeEvents(ctx context.Context, fromBlock uint64, handler subscriber.EventHandler) error {
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

	logs := make(chan types.Log)
	sub, err := s.client.SubscribeFilterLogs(ctx, query, logs)
	if err != nil {
		return fmt.Errorf("failed to subscribe to filter logs: %w", err)
	}
	defer sub.Unsubscribe()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case err := <-sub.Err():
			return fmt.Errorf("subscription error: %w", err)
		case vLog := <-logs:
			event, err := s.parseLog(ctx, vLog)
			if err != nil {
				logger.Error(err, zap.String("message", "Error parsing log"))
				continue
			}

			if event == nil {
				continue
			}

			if err := handler(event); err != nil {
				logger.Error(err, zap.String("message", "Error handling event"))
			}
		}
	}
}

// parseLog parses an Ethereum log into a standardized blockchain event
func (s *ethSubscriber) parseLog(ctx context.Context, vLog types.Log) (*domain.BlockchainEvent, error) {
	// Get block to extract timestamp
	block, err := s.client.BlockByNumber(ctx, new(big.Int).SetUint64(vLog.BlockNumber))
	if err != nil {
		return nil, fmt.Errorf("failed to get block: %w", err)
	}

	event := &domain.BlockchainEvent{
		Chain:           s.chainID,
		ContractAddress: vLog.Address.Hex(),
		TxHash:          vLog.TxHash.Hex(),
		BlockNumber:     vLog.BlockNumber,
		BlockHash:       vLog.BlockHash.Hex(),
		Timestamp:       s.clock.Unix(int64(block.Time()), 0), //nolint:gosec,G115 // block.Time() returns a uint64 from geth which is safe to cast
		LogIndex:        uint64(vLog.Index),
	}

	// Parse based on event signature
	switch vLog.Topics[0] {
	case transferEventSignature:
		// This signature is shared by ERC20 and ERC721
		// ERC20 has 3 topics (signature, from, to) with value in data
		// ERC721 has 4 topics (signature, from, to, tokenId) with no data

		if len(vLog.Topics) == 3 {
			// ERC20 Transfer - skip as we only index NFTs
			logger.Debug("Skipping ERC20 transfer event",
				zap.String("contract", vLog.Address.Hex()),
				zap.String("txHash", vLog.TxHash.Hex()))
			return nil, nil // skip ERC20 transfer events
		}

		if len(vLog.Topics) != 4 {
			return nil, fmt.Errorf("invalid Transfer event: expected 3 or 4 topics, got %d", len(vLog.Topics))
		}

		// ERC721 Transfer(address indexed from, address indexed to, uint256 indexed tokenId)
		event.Standard = domain.StandardERC721
		event.FromAddress = common.BytesToAddress(vLog.Topics[1].Bytes()).Hex()
		event.ToAddress = common.BytesToAddress(vLog.Topics[2].Bytes()).Hex()
		event.TokenNumber = new(big.Int).SetBytes(vLog.Topics[3].Bytes()).String()
		event.Quantity = "1"
		event.EventType = s.determineTransferEventType(event.FromAddress, event.ToAddress)

	case transferSingleEventSignature:
		// ERC1155 TransferSingle(address indexed operator, address indexed from, address indexed to, uint256 id, uint256 value)
		if len(vLog.Topics) != 4 {
			return nil, fmt.Errorf("invalid ERC1155 TransferSingle event: expected 4 topics, got %d", len(vLog.Topics))
		}
		if len(vLog.Data) < 64 {
			return nil, fmt.Errorf("invalid ERC1155 TransferSingle event: insufficient data")
		}

		event.Standard = domain.StandardERC1155
		event.FromAddress = common.BytesToAddress(vLog.Topics[2].Bytes()).Hex()
		event.ToAddress = common.BytesToAddress(vLog.Topics[3].Bytes()).Hex()

		// Parse data: first 32 bytes = token ID, next 32 bytes = value
		event.TokenNumber = new(big.Int).SetBytes(vLog.Data[0:32]).String()
		event.Quantity = new(big.Int).SetBytes(vLog.Data[32:64]).String()
		event.EventType = s.determineTransferEventType(event.FromAddress, event.ToAddress)

	case transferBatchEventSignature:
		// ERC1155 TransferBatch(address indexed operator, address indexed from, address indexed to, uint256[] ids, uint256[] values)
		// Note: This is a batch transfer event, which transfers multiple token types in a single transaction
		// For now, we'll return an error as batch transfers need special handling
		logger.Debug("Skipping ERC1155 TransferBatch event",
			zap.String("contract", vLog.Address.Hex()),
			zap.String("txHash", vLog.TxHash.Hex()))
		return nil, nil // skip ERC1155 TransferBatch events

	case metadataUpdateEventSignature:
		// EIP-4906 MetadataUpdate(uint256 _tokenId)
		if len(vLog.Topics) != 2 {
			return nil, fmt.Errorf("invalid MetadataUpdate event: expected 2 topics, got %d", len(vLog.Topics))
		}

		event.Standard = domain.StandardERC721 // EIP-4906 is for ERC721
		event.TokenNumber = new(big.Int).SetBytes(vLog.Topics[1].Bytes()).String()
		event.EventType = domain.EventTypeMetadataUpdate
		event.Quantity = "1"

	case batchMetadataUpdateEventSignature:
		// EIP-4906 BatchMetadataUpdate(uint256 _fromTokenId, uint256 _toTokenId)
		// Emit a single range event to avoid flooding NATS queue
		// The event-bridge will handle expanding this into individual token updates
		if len(vLog.Topics) != 3 {
			return nil, fmt.Errorf("invalid BatchMetadataUpdate event: expected 3 topics, got %d", len(vLog.Topics))
		}

		fromTokenId := new(big.Int).SetBytes(vLog.Topics[1].Bytes())
		toTokenId := new(big.Int).SetBytes(vLog.Topics[2].Bytes())

		event.Standard = domain.StandardERC721
		event.TokenNumber = fromTokenId.String()
		event.ToTokenNumber = toTokenId.String()
		event.EventType = domain.EventTypeMetadataUpdateRange
		event.Quantity = "1" // Range size can be calculated as (toTokenId - fromTokenId + 1)

	case uriEventSignature:
		// ERC1155 URI(string _value, uint256 indexed _id)
		if len(vLog.Topics) != 2 {
			return nil, fmt.Errorf("invalid URI event: expected 2 topics, got %d", len(vLog.Topics))
		}

		event.Standard = domain.StandardERC1155
		event.TokenNumber = new(big.Int).SetBytes(vLog.Topics[1].Bytes()).String()
		event.EventType = domain.EventTypeMetadataUpdate
		event.Quantity = "1"

	default:
		return nil, fmt.Errorf("unknown event signature: %s", vLog.Topics[0].Hex())
	}

	return event, nil
}

// determineTransferEventType determines the event type based on from/to addresses
func (s *ethSubscriber) determineTransferEventType(from, to string) domain.EventType {
	from = strings.ToLower(from)
	to = strings.ToLower(to)

	if from == domain.ETHEREUM_ZERO_ADDRESS {
		return domain.EventTypeMint
	}
	if to == domain.ETHEREUM_ZERO_ADDRESS {
		return domain.EventTypeBurn
	}
	return domain.EventTypeTransfer
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
}
