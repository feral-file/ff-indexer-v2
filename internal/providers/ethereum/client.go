package ethereum

import (
	"context"
	"fmt"
	"math/big"
	"sort"
	"strings"
	"time"

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

	// ERC721OwnerOf fetches the current owner of an ERC721 token
	ERC721OwnerOf(ctx context.Context, contractAddress, tokenNumber string) (string, error)

	// ERC1155BalanceOf fetches the balance of a specific token ID for an owner from an ERC1155 contract
	ERC1155BalanceOf(ctx context.Context, contractAddress, ownerAddress, tokenNumber string) (string, error)

	// GetTokenEvents fetches all historical events for a specific token
	GetTokenEvents(ctx context.Context, contractAddress, tokenNumber string, standard domain.ChainStandard) ([]domain.BlockchainEvent, error)

	// ERC1155Balances calculates all current ERC1155 token balances by replaying transfer events
	ERC1155Balances(ctx context.Context, contractAddress, tokenNumber string) (map[string]string, error)

	// GetTokenCIDsByOwnerAndBlockRange retrieves all token CIDs for an owner within a block range
	// It queries both ERC721 and ERC1155 transfer events where the address is either sender or receiver
	GetTokenCIDsByOwnerAndBlockRange(ctx context.Context, ownerAddress string, fromBlock, toBlock uint64) ([]domain.TokenCID, error)

	// GetContractDeployer retrieves the deployer address for a contract
	// minBlock specifies the earliest block to search (0 = search from genesis)
	GetContractDeployer(ctx context.Context, contractAddress string, minBlock uint64) (string, error)

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

// filterLogsWithPagination is an internal method that handles pagination for FilterLogs
// to work around Infura's 10k log limitation
func (c *ethereumClient) filterLogsWithPagination(ctx context.Context, query ethereum.FilterQuery) ([]types.Log, error) {
	// Create a context with timeout (1 minute)
	timeoutCtx, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()

	// If blockhash is specified, use it directly (no pagination needed)
	if query.BlockHash != nil {
		return c.client.FilterLogs(timeoutCtx, query)
	}

	// 1. Detect initial start/end blocks (genesis and latest)
	var fromBlock, toBlock *big.Int
	if query.FromBlock != nil {
		fromBlock = query.FromBlock
	} else {
		fromBlock = big.NewInt(0) // Genesis
	}

	if query.ToBlock != nil {
		toBlock = query.ToBlock
	} else {
		// Get latest block
		latestBlock, err := c.client.HeaderByNumber(timeoutCtx, nil)
		if err != nil {
			return nil, fmt.Errorf("failed to get latest block: %w", err)
		}
		toBlock = latestBlock.Number
	}

	// 2. Start from genesis, step 1M blocks, process query
	var allLogs []types.Log
	currentFrom := new(big.Int).Set(fromBlock)
	stepSize := uint64(1000000) // 1M blocks

	for currentFrom.Cmp(toBlock) < 0 {
		// Calculate current range
		currentTo := new(big.Int).Add(currentFrom, big.NewInt(int64(stepSize)))
		if currentTo.Cmp(toBlock) > 0 {
			currentTo.Set(toBlock)
		}

		// Create query for current range
		rangeQuery := query
		rangeQuery.FromBlock = new(big.Int).Set(currentFrom)
		rangeQuery.ToBlock = currentTo

		// Try to get logs for current range with retry logic
		logs, err := c.getLogsWithRetry(timeoutCtx, rangeQuery, stepSize)
		if err != nil {
			return nil, fmt.Errorf("failed to get logs for range %d-%d: %w", currentFrom.Uint64(), currentTo.Uint64(), err)
		}

		allLogs = append(allLogs, logs...)

		// Move to next range - use the actual end of the processed range
		currentFrom.SetUint64(currentTo.Uint64() + 1)
	}

	return allLogs, nil
}

// getLogsWithRetry attempts to get logs with retry logic and step size reduction
// It processes the entire range from query.FromBlock to query.ToBlock in chunks
func (c *ethereumClient) getLogsWithRetry(ctx context.Context, query ethereum.FilterQuery, stepSize uint64) ([]types.Log, error) {
	currentStepSize := stepSize

	var allLogs []types.Log
	currentFrom := new(big.Int).Set(query.FromBlock)

	// Process the entire range in chunks
	for currentFrom.Cmp(query.ToBlock) <= 0 {
		// Calculate current range based on current step size
		currentTo := new(big.Int).Add(currentFrom, new(big.Int).SetUint64(currentStepSize-1))
		if currentTo.Cmp(query.ToBlock) > 0 {
			currentTo.Set(query.ToBlock)
		}

		// Create query for current chunk
		queryCopy := query
		queryCopy.FromBlock = new(big.Int).Set(currentFrom)
		queryCopy.ToBlock = new(big.Int).Set(currentTo)

		logs, err := c.client.FilterLogs(ctx, queryCopy)
		if err == nil {
			// Success - accumulate logs and move to next chunk
			allLogs = append(allLogs, logs...)

			// Move to next chunk using the full step size
			currentFrom.SetUint64(currentTo.Uint64() + 1)
			continue
		}

		// 3. If other errors than rate limited, return error
		if !isTooManyResultsError(err) {
			return nil, err
		}

		// 4. If rate limited, divide the step by 2 and try again
		currentStepSize = currentStepSize / 2

		logger.Warn("Too many results, reducing step size",
			zap.Uint64("oldStepSize", currentStepSize*2),
			zap.Uint64("newStepSize", currentStepSize),
			zap.Uint64("fromBlock", currentFrom.Uint64()),
			zap.Uint64("toBlock", currentTo.Uint64()))
	}

	return allLogs, nil
}

// isTooManyResultsError checks if the error is related to too many results
func isTooManyResultsError(err error) bool {
	if err == nil {
		return false
	}

	errStr := err.Error()
	// Check for common "too many results" error messages
	return strings.Contains(errStr, "query returned more than 10000 results") ||
		strings.Contains(errStr, "query timeout exceeded") ||
		strings.Contains(errStr, "too many results") ||
		strings.Contains(errStr, "exceeded maximum")
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

// ERC721OwnerOf fetches the current owner of an ERC721 token
func (c *ethereumClient) ERC721OwnerOf(ctx context.Context, contractAddress, tokenNumber string) (string, error) {
	// ERC721 ownerOf function signature: ownerOf(uint256) returns (address)
	ownerOfABI, err := abi.JSON(strings.NewReader(`[{"constant":true,"inputs":[{"name":"tokenId","type":"uint256"}],"name":"ownerOf","outputs":[{"name":"","type":"address"}],"payable":false,"stateMutability":"view","type":"function"}]`))
	if err != nil {
		return "", fmt.Errorf("failed to parse ABI: %w", err)
	}

	tokenID, ok := new(big.Int).SetString(tokenNumber, 10)
	if !ok {
		return "", fmt.Errorf("invalid token number: %s", tokenNumber)
	}

	data, err := ownerOfABI.Pack("ownerOf", tokenID)
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

	var owner common.Address
	if err := ownerOfABI.UnpackIntoInterface(&owner, "ownerOf", result); err != nil {
		return "", fmt.Errorf("failed to unpack result: %w", err)
	}

	return owner.Hex(), nil
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

	// Fetch logs with pagination to handle Infura's 10k limitation
	logs, err := c.filterLogsWithPagination(ctx, query)
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

// ERC1155Balances calculates all current ERC1155 token balances by replaying transfer events from oldest to newest
func (c *ethereumClient) ERC1155Balances(ctx context.Context, contractAddress, tokenNumber string) (map[string]string, error) {
	// Parse token number to big.Int
	tokenID, ok := new(big.Int).SetString(tokenNumber, 10)
	if !ok {
		return nil, fmt.Errorf("invalid token number: %s", tokenNumber)
	}

	contractAddr := common.HexToAddress(contractAddress)

	// For ERC1155, token ID is in data, not topics, so we fetch all TransferSingle events for this contract
	// and filter by token ID later
	query := ethereum.FilterQuery{
		FromBlock: big.NewInt(0),
		ToBlock:   nil,
		Addresses: []common.Address{contractAddr},
		Topics: [][]common.Hash{
			{
				transferSingleEventSignature,
				//transferBatchEventSignature, // FIXME: Handle batch transfers properly
			},
		},
	}

	// Fetch logs with pagination to handle Infura's 10k limitation
	logs, err := c.filterLogsWithPagination(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to filter logs: %w", err)
	}

	// Build balances map by replaying transfer logs
	balances := make(map[string]*big.Int)

	for _, vLog := range logs {
		// ERC1155 TransferSingle(address indexed operator, address indexed from, address indexed to, uint256 id, uint256 value)
		if len(vLog.Topics) != 4 {
			logger.Warn("Invalid ERC1155 TransferSingle event: unexpected topic count",
				zap.Int("topics", len(vLog.Topics)),
				zap.String("txHash", vLog.TxHash.Hex()))
			continue
		}
		if len(vLog.Data) < 64 {
			logger.Warn("Invalid ERC1155 TransferSingle event: insufficient data",
				zap.String("txHash", vLog.TxHash.Hex()))
			continue
		}

		// Parse data: first 32 bytes = token ID, next 32 bytes = value
		logTokenID := new(big.Int).SetBytes(vLog.Data[0:32])
		// Filter by token ID
		if logTokenID.Cmp(tokenID) != 0 {
			continue
		}

		fromAddr := common.BytesToAddress(vLog.Topics[2].Bytes()).Hex()
		toAddr := common.BytesToAddress(vLog.Topics[3].Bytes()).Hex()
		quantity := new(big.Int).SetBytes(vLog.Data[32:64])

		// Process transfer: subtract from 'from' address, add to 'to' address
		if fromAddr != "" && fromAddr != domain.ETHEREUM_ZERO_ADDRESS {
			if balances[fromAddr] == nil {
				balances[fromAddr] = big.NewInt(0)
			}
			balances[fromAddr] = new(big.Int).Sub(balances[fromAddr], quantity)
		}

		if toAddr != "" && toAddr != domain.ETHEREUM_ZERO_ADDRESS {
			if balances[toAddr] == nil {
				balances[toAddr] = big.NewInt(0)
			}
			balances[toAddr] = new(big.Int).Add(balances[toAddr], quantity)
		}
	}

	// Convert to string map and filter out zero/negative balances
	result := make(map[string]string)
	for addr, balance := range balances {
		if balance.Cmp(big.NewInt(0)) > 0 {
			result[addr] = balance.String()
		}
	}

	return result, nil
}

// GetTokenCIDsByOwnerAndBlockRange retrieves all token CIDs for an owner within a block range
// It queries both ERC721 and ERC1155 transfer events where the address is either sender or receiver
func (c *ethereumClient) GetTokenCIDsByOwnerAndBlockRange(ctx context.Context, ownerAddress string, fromBlock, toBlock uint64) ([]domain.TokenCID, error) {
	owner := common.HexToAddress(ownerAddress)
	ownerHash := common.BytesToHash(owner.Bytes())

	// Define all query configurations
	// We need to query both ERC721 and ERC1155 transfers where the address is either sender or receiver
	queries := []ethereum.FilterQuery{
		// ERC721 Transfer where address is `from` (topic[1])
		{
			FromBlock: new(big.Int).SetUint64(fromBlock),
			ToBlock:   new(big.Int).SetUint64(toBlock),
			Topics: [][]common.Hash{
				{transferEventSignature}, // Transfer event
				{ownerHash},              // from address
			},
		},
		// ERC721 Transfer where address is `to` (topic[2])
		{
			FromBlock: new(big.Int).SetUint64(fromBlock),
			ToBlock:   new(big.Int).SetUint64(toBlock),
			Topics: [][]common.Hash{
				{transferEventSignature}, // Transfer event
				nil,                      // any from address
				{ownerHash},              // to address
			},
		},
		// ERC1155 TransferSingle where address is `from` (topic[2])
		{
			FromBlock: new(big.Int).SetUint64(fromBlock),
			ToBlock:   new(big.Int).SetUint64(toBlock),
			Topics: [][]common.Hash{
				{transferSingleEventSignature}, // TransferSingle event
				nil,                            // any operator
				{ownerHash},                    // from address
			},
		},
		// ERC1155 TransferSingle where address is `to` (topic[3])
		{
			FromBlock: new(big.Int).SetUint64(fromBlock),
			ToBlock:   new(big.Int).SetUint64(toBlock),
			Topics: [][]common.Hash{
				{transferSingleEventSignature}, // TransferSingle event
				nil,                            // any operator
				nil,                            // any from address
				{ownerHash},                    // to address
			},
		},
	}

	type queryResult struct {
		logs []types.Log
		err  error
	}

	// Execute all queries in parallel
	resultsCh := make(chan queryResult, len(queries))
	for _, q := range queries {
		go func(query ethereum.FilterQuery) {
			logs, err := c.filterLogsWithPagination(ctx, query)
			resultsCh <- queryResult{logs: logs, err: err}
		}(q)
	}

	// Collect all results and merge logs
	var allLogs []types.Log
	for range queries {
		result := <-resultsCh
		if result.err != nil {
			return nil, fmt.Errorf("failed to query logs: %w", result.err)
		}
		allLogs = append(allLogs, result.logs...)
	}

	// Use a map to collect unique token CIDs
	tokenCIDMap := make(map[domain.TokenCID]bool)

	// Process all logs - distinguish between ERC721 and ERC1155 by event signature
	for _, vLog := range allLogs {
		if len(vLog.Topics) < 1 {
			continue
		}

		switch vLog.Topics[0] {
		case transferEventSignature:
			// ERC721 Transfer (4 topics: signature, from, to, tokenId)
			// Skip ERC20 (3 topics)
			if len(vLog.Topics) != 4 {
				continue
			}

			tokenID := new(big.Int).SetBytes(vLog.Topics[3].Bytes())
			tokenCID := domain.TokenCID(fmt.Sprintf("%s:%s:%s:%s",
				c.chainID,
				domain.StandardERC721,
				vLog.Address.Hex(),
				tokenID.String()))
			tokenCIDMap[tokenCID] = true

		case transferSingleEventSignature:
			// ERC1155 TransferSingle (4 topics: signature, operator, from, to; data contains tokenId and value)
			if len(vLog.Topics) != 4 || len(vLog.Data) < 64 {
				continue
			}

			// Parse token ID from data (first 32 bytes)
			tokenID := new(big.Int).SetBytes(vLog.Data[0:32])
			tokenCID := domain.TokenCID(fmt.Sprintf("%s:%s:%s:%s",
				c.chainID,
				domain.StandardERC1155,
				vLog.Address.Hex(),
				tokenID.String()))
			tokenCIDMap[tokenCID] = true
		}
	}

	// Convert map to slice
	tokenCIDs := make([]domain.TokenCID, 0, len(tokenCIDMap))
	for tokenCID := range tokenCIDMap {
		tokenCIDs = append(tokenCIDs, tokenCID)
	}

	return tokenCIDs, nil
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
		TxIndex:         uint64(vLog.TxIndex),
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

// GetContractDeployer retrieves the deployer address for a contract
// This method finds the contract creation transaction by binary searching for the block
// where the contract was deployed
// minBlock specifies the earliest block to search (0 = search from genesis)
func (c *ethereumClient) GetContractDeployer(ctx context.Context, contractAddress string, minBlock uint64) (string, error) {
	addr := common.HexToAddress(contractAddress)

	// Get current block number
	latestHeader, err := c.client.HeaderByNumber(ctx, nil)
	if err != nil {
		return "", fmt.Errorf("failed to get latest header: %w", err)
	}
	latestBlock := latestHeader.Number.Uint64()

	// Validate minBlock
	if minBlock > latestBlock {
		return "", fmt.Errorf("minBlock (%d) is greater than latest block (%d)", minBlock, latestBlock)
	}

	// Binary search to find the block where contract was created
	// We look for the first block where the contract has code
	// sort.Search finds the smallest index i in [0, n) where f(i) is true
	// We adjust the search to start from minBlock
	searchRange := int(latestBlock - minBlock + 1)
	var searchErr error
	relativeBlock := uint64(sort.Search(searchRange, func(i int) bool {
		blockNum := minBlock + uint64(i)
		code, err := c.client.CodeAt(ctx, addr, new(big.Int).SetUint64(blockNum))
		if err != nil {
			// Store error for later handling, but continue search
			searchErr = err
			return false
		}
		return len(code) > 0
	}))

	creationBlock := minBlock + relativeBlock

	// Check if contract was found (sort.Search returns n if not found)
	if relativeBlock >= uint64(searchRange) {
		if searchErr != nil {
			return "", fmt.Errorf("failed to find contract (encountered errors during search): %w", searchErr)
		}
		return "", fmt.Errorf("contract not found: %s (searched blocks %d-%d)", contractAddress, minBlock, latestBlock)
	}

	// Get the block where contract was created
	block, err := c.client.BlockByNumber(ctx, new(big.Int).SetUint64(creationBlock))
	if err != nil {
		return "", fmt.Errorf("failed to get block %d: %w", creationBlock, err)
	}

	// Find the transaction that created the contract
	// The contract creation transaction has the contract address as the result
	for _, tx := range block.Transactions() {
		// Contract creation transactions have nil To address
		if tx.To() != nil {
			continue
		}

		// Get transaction receipt to check contract address
		receipt, err := c.client.TransactionReceipt(ctx, tx.Hash())
		if err != nil {
			continue
		}

		if receipt.ContractAddress == addr {
			// Found the creation transaction
			sender, err := c.client.TransactionSender(ctx, tx, block.Hash(), receipt.TransactionIndex)
			if err != nil {
				return "", fmt.Errorf("failed to get transaction sender: %w", err)
			}
			return sender.Hex(), nil
		}
	}

	return "", fmt.Errorf("contract creation transaction not found for %s at block %d", contractAddress, creationBlock)
}

// Close closes the connection
func (c *ethereumClient) Close() {
	c.client.Close()
}
