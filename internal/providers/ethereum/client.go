package ethereum

import (
	"context"
	"fmt"
	"math/big"
	"strings"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"go.uber.org/zap"

	logger "github.com/bitmark-inc/autonomy-logger"

	"github.com/feral-file/ff-indexer-v2/internal/adapter"
	"github.com/feral-file/ff-indexer-v2/internal/domain"
)

type EthereumClient interface {
	// ParseEventLog parses an Ethereum log into a standardized blockchain event
	ParseEventLog(ctx context.Context, vLog types.Log) (*domain.BlockchainEvent, error)

	// SubscribeFilterLogs subscribes to filter logs
	SubscribeFilterLogs(ctx context.Context, query ethereum.FilterQuery, ch chan<- types.Log) (ethereum.Subscription, error)

	// BlockByNumber returns a block by number
	BlockByNumber(ctx context.Context, number *big.Int) (*types.Block, error)

	// HeaderByNumber returns a header by number
	HeaderByNumber(ctx context.Context, number *big.Int) (*types.Header, error)

	// ERC721TokenURI fetches the tokenURI from an ERC721 contract
	ERC721TokenURI(ctx context.Context, contractAddress string, tokenNumber string) (string, error)

	// ERC1155URI fetches the uri from an ERC1155 contract
	ERC1155URI(ctx context.Context, contractAddress, tokenNumber string) (string, error)

	// ERC1155BalanceOf fetches the balance of a specific token ID for an owner from an ERC1155 contract
	ERC1155BalanceOf(ctx context.Context, contractAddress, ownerAddress, tokenNumber string) (string, error)

	// GetTokenEvents fetches all historical events for a specific token
	GetTokenEvents(ctx context.Context, contractAddress, tokenNumber string, standard domain.ChainStandard) ([]domain.BlockchainEvent, error)

	// Close closes the connection
	Close()
}

type ethereumClient struct {
	chainID domain.Chain
	client  adapter.EthClient
	clock   adapter.Clock
}

func NewClient(chainID domain.Chain, client adapter.EthClient, clock adapter.Clock) EthereumClient {
	return &ethereumClient{chainID: chainID, client: client, clock: clock}
}

// SubscribeFilterLogs subscribes to filter logs
func (c *ethereumClient) SubscribeFilterLogs(ctx context.Context, query ethereum.FilterQuery, ch chan<- types.Log) (ethereum.Subscription, error) {
	return c.client.SubscribeFilterLogs(ctx, query, ch)
}

// BlockByNumber returns a block by number
func (c *ethereumClient) BlockByNumber(ctx context.Context, number *big.Int) (*types.Block, error) {
	return c.client.BlockByNumber(ctx, number)
}

// HeaderByNumber returns a header by number
func (c *ethereumClient) HeaderByNumber(ctx context.Context, number *big.Int) (*types.Header, error) {
	return c.client.HeaderByNumber(ctx, number)
}

// ERC721TokenURI fetches the tokenURI from an ERC721 contract
func (c *ethereumClient) ERC721TokenURI(ctx context.Context, contractAddress string, tokenNumber string) (string, error) {
	// ERC721 tokenURI function signature: tokenURI(uint256) returns (string)
	tokenURIABI, err := abi.JSON(strings.NewReader(`[{"constant":true,"inputs":[{"name":"tokenId","type":"uint256"}],"name":"tokenURI","outputs":[{"name":"","type":"string"}],"payable":false,"stateMutability":"view","type":"function"}]`))
	if err != nil {
		return "", fmt.Errorf("failed to parse ABI: %w", err)
	}

	tokenID, ok := new(big.Int).SetString(tokenNumber, 10)
	if !ok {
		return "", fmt.Errorf("invalid token number: %s", tokenNumber)
	}

	data, err := tokenURIABI.Pack("tokenURI", tokenID)
	if err != nil {
		return "", fmt.Errorf("failed to pack data: %w", err)
	}

	contractAddr := common.HexToAddress(contractAddress)
	result, err := c.client.CallContract(ctx, ethereum.CallMsg{
		To:   &contractAddr,
		Data: data,
	}, nil)
	if err != nil {
		return "", fmt.Errorf("failed to call contract: %w", err)
	}

	var uri string
	if err := tokenURIABI.UnpackIntoInterface(&uri, "tokenURI", result); err != nil {
		return "", fmt.Errorf("failed to unpack result: %w", err)
	}

	return uri, nil
}

// ERC1155URI fetches the uri from an ERC1155 contract
func (c *ethereumClient) ERC1155URI(ctx context.Context, contractAddress, tokenNumber string) (string, error) {
	// ERC1155 uri function signature: uri(uint256) returns (string)
	uriABI, err := abi.JSON(strings.NewReader(`[{"constant":true,"inputs":[{"name":"id","type":"uint256"}],"name":"uri","outputs":[{"name":"","type":"string"}],"payable":false,"stateMutability":"view","type":"function"}]`))
	if err != nil {
		return "", fmt.Errorf("failed to parse ABI: %w", err)
	}

	tokenID, ok := new(big.Int).SetString(tokenNumber, 10)
	if !ok {
		return "", fmt.Errorf("invalid token number: %s", tokenNumber)
	}

	data, err := uriABI.Pack("uri", tokenID)
	if err != nil {
		return "", fmt.Errorf("failed to pack data: %w", err)
	}

	contractAddr := common.HexToAddress(contractAddress)
	result, err := c.client.CallContract(ctx, ethereum.CallMsg{
		To:   &contractAddr,
		Data: data,
	}, nil)
	if err != nil {
		return "", fmt.Errorf("failed to call contract: %w", err)
	}

	var uri string
	if err := uriABI.UnpackIntoInterface(&uri, "uri", result); err != nil {
		return "", fmt.Errorf("failed to unpack result: %w", err)
	}

	return uri, nil
}

// ERC1155BalanceOf fetches the balance of a specific token ID for an owner from an ERC1155 contract
func (c *ethereumClient) ERC1155BalanceOf(ctx context.Context, contractAddress, ownerAddress, tokenNumber string) (string, error) {
	// ERC1155 balanceOf function signature: balanceOf(address,uint256) returns (uint256)
	balanceOfABI, err := abi.JSON(strings.NewReader(`[{"constant":true,"inputs":[{"name":"account","type":"address"},{"name":"id","type":"uint256"}],"name":"balanceOf","outputs":[{"name":"","type":"uint256"}],"payable":false,"stateMutability":"view","type":"function"}]`))
	if err != nil {
		return "", fmt.Errorf("failed to parse ABI: %w", err)
	}

	tokenID, ok := new(big.Int).SetString(tokenNumber, 10)
	if !ok {
		return "", fmt.Errorf("invalid token number: %s", tokenNumber)
	}

	owner := common.HexToAddress(ownerAddress)
	data, err := balanceOfABI.Pack("balanceOf", owner, tokenID)
	if err != nil {
		return "", fmt.Errorf("failed to pack data: %w", err)
	}

	contractAddr := common.HexToAddress(contractAddress)
	result, err := c.client.CallContract(ctx, ethereum.CallMsg{
		To:   &contractAddr,
		Data: data,
	}, nil)
	if err != nil {
		return "", fmt.Errorf("failed to call contract: %w", err)
	}

	var balance *big.Int
	if err := balanceOfABI.UnpackIntoInterface(&balance, "balanceOf", result); err != nil {
		return "", fmt.Errorf("failed to unpack result: %w", err)
	}

	return balance.String(), nil
}

// GetTokenEvents fetches all historical events for a specific token
func (c *ethereumClient) GetTokenEvents(ctx context.Context, contractAddress, tokenNumber string, standard domain.ChainStandard) ([]domain.BlockchainEvent, error) {
	// Parse token number to big.Int
	tokenID, ok := new(big.Int).SetString(tokenNumber, 10)
	if !ok {
		return nil, fmt.Errorf("invalid token number: %s", tokenNumber)
	}

	contractAddr := common.HexToAddress(contractAddress)
	var query ethereum.FilterQuery

	// Build filter query based on standard
	switch standard {
	case domain.StandardERC721:
		// For ERC721, tokenId is indexed (topic[3]), so we can filter directly
		tokenIDHash := common.BigToHash(tokenID)
		query = ethereum.FilterQuery{
			FromBlock: big.NewInt(0), // From genesis
			ToBlock:   nil,           // To latest
			Addresses: []common.Address{contractAddr},
			Topics: [][]common.Hash{
				{
					transferEventSignature,       // Transfer events
					metadataUpdateEventSignature, // MetadataUpdate events
					//batchMetadataUpdateEventSignature, // FIXME: Handle batch metadata updates properly
				},
				nil,           // Any from address
				nil,           // Any to address
				{tokenIDHash}, // Specific token ID
			},
		}

	case domain.StandardERC1155:
		// For ERC1155, token ID is in data, not topics, so we fetch all events for this contract
		// and filter by token ID later
		query = ethereum.FilterQuery{
			FromBlock: big.NewInt(0),
			ToBlock:   nil,
			Addresses: []common.Address{contractAddr},
			Topics: [][]common.Hash{
				{
					transferSingleEventSignature, // TransferSingle events
					//transferBatchEventSignature,  // FIXME: Handle batch transfers properly
					uriEventSignature, // URI events (metadata updates)
				},
			},
		}

	default:
		return nil, fmt.Errorf("unsupported token standard: %s", standard)
	}

	// Fetch logs
	// FIXME handle pagination
	logs, err := c.client.FilterLogs(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to filter logs: %w", err)
	}

	// Parse logs and convert to EthereumTokenEvent
	events := make([]domain.BlockchainEvent, 0)
	for _, vLog := range logs {
		event, err := c.ParseEventLog(ctx, vLog)
		if err != nil {
			// Log error but continue processing
			logger.Warn("Failed to parse event log", zap.Error(err))
			continue
		}
		if event != nil {
			events = append(events, *event)
		}
	}

	return events, nil
}

// parseLog parses an Ethereum log into a standardized blockchain event
func (c *ethereumClient) ParseEventLog(ctx context.Context, vLog types.Log) (*domain.BlockchainEvent, error) {
	// Get block to extract timestamp
	block, err := c.client.BlockByNumber(ctx, new(big.Int).SetUint64(vLog.BlockNumber))
	if err != nil {
		return nil, fmt.Errorf("failed to get block: %w", err)
	}

	blockHash := vLog.BlockHash.Hex()
	event := &domain.BlockchainEvent{
		Chain:           c.chainID,
		ContractAddress: vLog.Address.Hex(),
		TxHash:          vLog.TxHash.Hex(),
		BlockNumber:     vLog.BlockNumber,
		BlockHash:       &blockHash,
		Timestamp:       c.clock.Unix(int64(block.Time()), 0), //nolint:gosec,G115 // block.Time() returns a uint64 from geth which is safe to cast
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
		fromAddress := common.BytesToAddress(vLog.Topics[1].Bytes()).Hex()
		event.FromAddress = &fromAddress
		toAddress := common.BytesToAddress(vLog.Topics[2].Bytes()).Hex()
		event.ToAddress = &toAddress
		event.TokenNumber = new(big.Int).SetBytes(vLog.Topics[3].Bytes()).String()
		event.Quantity = "1"
		event.EventType = domain.TransferEventType(event.FromAddress, event.ToAddress)

	case transferSingleEventSignature:
		// ERC1155 TransferSingle(address indexed operator, address indexed from, address indexed to, uint256 id, uint256 value)
		if len(vLog.Topics) != 4 {
			return nil, fmt.Errorf("invalid ERC1155 TransferSingle event: expected 4 topics, got %d", len(vLog.Topics))
		}
		if len(vLog.Data) < 64 {
			return nil, fmt.Errorf("invalid ERC1155 TransferSingle event: insufficient data")
		}

		event.Standard = domain.StandardERC1155
		fromAddress := common.BytesToAddress(vLog.Topics[2].Bytes()).Hex()
		event.FromAddress = &fromAddress
		toAddress := common.BytesToAddress(vLog.Topics[3].Bytes()).Hex()
		event.ToAddress = &toAddress

		// Parse data: first 32 bytes = token ID, next 32 bytes = value
		event.TokenNumber = new(big.Int).SetBytes(vLog.Data[0:32]).String()
		event.Quantity = new(big.Int).SetBytes(vLog.Data[32:64]).String()
		event.EventType = domain.TransferEventType(event.FromAddress, event.ToAddress)

	case transferBatchEventSignature:
		// ERC1155 TransferBatch(address indexed operator, address indexed from, address indexed to, uint256[] ids, uint256[] values)
		// Note: This is a batch transfer event, which transfers multiple token types in a single transaction
		// For now, we'll skip the event as batch transfers need special handling
		// FIXME handle batch transfers properly
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

// Close closes the connection
func (c *ethereumClient) Close() {
	c.client.Close()
}
