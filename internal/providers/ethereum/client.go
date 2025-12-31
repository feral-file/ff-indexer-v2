package ethereum

import (
	"context"
	"errors"
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

	"github.com/feral-file/ff-indexer-v2/internal/adapter"
	"github.com/feral-file/ff-indexer-v2/internal/block"
	"github.com/feral-file/ff-indexer-v2/internal/domain"
	"github.com/feral-file/ff-indexer-v2/internal/logger"
)

// ErrExecutionReverted is returned when a function call reverts
var ErrExecutionReverted = errors.New("execution reverted")

// ErrContractNotFound is returned when a contract is not found or self-destructed
var ErrContractNotFound = errors.New("contract not found")

// ErrOutOfGas is returned when a function call runs out of gas
var ErrOutOfGas = errors.New("out of gas")

// ErrOriginationNotFound is returned when an origination is not found for a contract
var ErrOriginationNotFound = errors.New("origination not found")

// EthereumClient defines the interface for Ethereum client operations
//
//go:generate mockgen -source=client.go -destination=../../mocks/ethereum_provider_client.go -package=mocks -mock_names=EthereumClient=MockEthereumProviderClient
type EthereumClient interface {
	// ParseEventLog parses an Ethereum log into a standardized blockchain event
	ParseEventLog(ctx context.Context, vLog types.Log) (*domain.BlockchainEvent, error)

	// SubscribeFilterLogs subscribes to filter logs
	SubscribeFilterLogs(ctx context.Context, query ethereum.FilterQuery, ch chan<- types.Log) (ethereum.Subscription, error)

	// GetLatestBlock returns the latest block number
	GetLatestBlock(ctx context.Context) (uint64, error)

	// ERC721TokenURI fetches the tokenURI from an ERC721 contract
	ERC721TokenURI(ctx context.Context, contractAddress string, tokenNumber string) (string, error)

	// ERC1155URI fetches the uri from an ERC1155 contract
	ERC1155URI(ctx context.Context, contractAddress, tokenNumber string) (string, error)

	// ERC721OwnerOf fetches the current owner of an ERC721 token
	ERC721OwnerOf(ctx context.Context, contractAddress, tokenNumber string) (string, error)

	// ERC1155BalanceOf fetches the balance of a specific token ID for an owner from an ERC1155 contract
	ERC1155BalanceOf(ctx context.Context, contractAddress, ownerAddress, tokenNumber string) (string, error)

	// ERC1155BalanceOfBatch fetches balances for multiple addresses for a specific token ID from an ERC1155 contract
	// Handles chunking internally to respect RPC limits (default 200 addresses per chunk)
	// Returns a map of address -> balance string
	ERC1155BalanceOfBatch(ctx context.Context, contractAddress, tokenNumber string, addresses []string) (map[string]string, error)

	// GetTokenEvents fetches all historical events for a specific token
	// Returns events in ascending order of timestamp
	GetTokenEvents(ctx context.Context, contractAddress, tokenNumber string, standard domain.ChainStandard) ([]domain.BlockchainEvent, error)

	// ERC1155Balances calculates all current ERC1155 token balances by replaying transfer events
	ERC1155Balances(ctx context.Context, contractAddress, tokenNumber string) (map[string]string, error)

	// GetERC1155BalanceAndEventsForOwner fetches the current balance and transfer events for a specific ERC1155 token and owner
	// This is optimized for owner-specific indexing by filtering events where owner is sender or receiver
	// Supports both TransferSingle and TransferBatch events
	GetERC1155BalanceAndEventsForOwner(ctx context.Context, contractAddress, tokenNumber, ownerAddress string) (balance string, events []domain.BlockchainEvent, err error)

	// GetTokenCIDsByOwnerAndBlockRange retrieves all token CIDs with block numbers for an owner within a block range
	// It queries both ERC721 and ERC1155 transfer events where the address is either sender or receiver
	GetTokenCIDsByOwnerAndBlockRange(ctx context.Context, ownerAddress string, fromBlock, toBlock uint64) ([]domain.TokenWithBlock, error)

	// GetContractDeployer retrieves the deployer address for a contract
	// minBlock specifies the earliest block to search (0 = search from genesis)
	GetContractDeployer(ctx context.Context, contractAddress string, minBlock uint64) (string, error)

	// TokenExists checks if a token exists on the blockchain
	// For ERC721: uses ownerOf and catches execution revert errors
	// For ERC1155: checks mint and burn events in logs
	TokenExists(ctx context.Context, contractAddress, tokenNumber string, standard domain.ChainStandard) (bool, error)

	// Close closes the connection
	Close()
}

type ethereumClient struct {
	chainID       domain.Chain
	client        adapter.EthClient
	clock         adapter.Clock
	blockProvider block.BlockProvider
}

// NewClient creates a new Ethereum client with the provided dependencies
func NewClient(chainID domain.Chain, client adapter.EthClient, clock adapter.Clock, blockProvider block.BlockProvider) EthereumClient {
	return &ethereumClient{
		chainID:       chainID,
		client:        client,
		clock:         clock,
		blockProvider: blockProvider,
	}
}

// SubscribeFilterLogs subscribes to filter logs
func (f *ethereumClient) SubscribeFilterLogs(ctx context.Context, query ethereum.FilterQuery, ch chan<- types.Log) (ethereum.Subscription, error) {
	return f.client.SubscribeFilterLogs(ctx, query, ch)
}

// calculateStepSize determines the optimal step size for pagination based on query specificity
// More specific queries (with indexed parameters) can use larger steps to reduce RPC calls
// while staying within Infura's 10k log limitation
//
// Step size guidelines for Ethereum mainnet (~4.5-5 months per 1M blocks):
// - 30M blocks (~12 years): ERC721 with specific token ID (very few events per token)
// - 10M blocks (~4 years): ERC721 with specific owner address
// - 10M blocks (~4 years): ERC1155 with specific owner address
// - 5M blocks (~2 years): ERC1155 queries for entire contract (moderate activity)
// - 1M blocks (~5 months): ERC1155 queries for high-activity contracts
func (f *ethereumClient) calculateStepSize(ctx context.Context, query ethereum.FilterQuery) uint64 {
	// Default conservative step size for unrecognized patterns
	const (
		defaultStepSize         = uint64(1_000_000)  // 1M blocks
		erc721TokenStepSize     = uint64(30_000_000) // 30M blocks - ERC721 with token ID
		erc721OwnerStepSize     = uint64(10_000_000) // 10M blocks - ERC721 with owner
		erc1155OwnerStepSize    = uint64(10_000_000) // 10M blocks - ERC1155 with owner
		erc1155ContractStepSize = uint64(5_000_000)  // 5M blocks - ERC1155 contract only
	)

	// If no topics specified, return default
	if len(query.Topics) == 0 {
		return defaultStepSize
	}

	// Analyze event signatures in topics[0]
	eventSignatures := query.Topics[0]
	if len(eventSignatures) == 0 {
		return defaultStepSize
	}

	// Determine if this is an ERC721 or ERC1155 query based on event signatures
	hasERC721Transfer := false
	hasERC1155Transfer := false

	for _, sig := range eventSignatures {
		if sig == transferEventSignature || sig == metadataUpdateEventSignature {
			hasERC721Transfer = true
		}
		if sig == transferSingleEventSignature || sig == uriEventSignature {
			hasERC1155Transfer = true
		}
	}

	// Analyze indexed parameters based on token standard
	// ERC721 Transfer: topics[1]=from, topics[2]=to, topics[3]=tokenId (ALL indexed)
	// ERC1155 TransferSingle: topics[1]=operator, topics[2]=from, topics[3]=to (tokenId NOT indexed - in data)
	hasTokenIDIndexed := false // ERC721 token ID in topics[3] (only ERC721 has indexed token ID)
	hasOwnerIndexed := false   // Owner address filtered: topics[1]/[2] for ERC721, topics[2]/[3] for ERC1155

	// Check for specific filters that make the query more targeted
	if hasERC721Transfer {
		// For ERC721: topics[1] or topics[2] = owner addresses, topics[3] = token ID
		if len(query.Topics) > 1 && len(query.Topics[1]) > 0 {
			hasOwnerIndexed = true
		}
		if len(query.Topics) > 2 && len(query.Topics[2]) > 0 {
			hasOwnerIndexed = true
		}
		if len(query.Topics) > 3 && len(query.Topics[3]) > 0 {
			hasTokenIDIndexed = true
		}
	} else if hasERC1155Transfer {
		// For ERC1155: topics[1] is operator (not useful), topics[2]=from, topics[3]=to
		// Token ID is NOT indexed (it's in data field), so hasTokenIDIndexed stays false
		if len(query.Topics) > 2 && len(query.Topics[2]) > 0 {
			hasOwnerIndexed = true
		}
		if len(query.Topics) > 3 && len(query.Topics[3]) > 0 {
			hasOwnerIndexed = true
		}
	}

	// Determine step size based on query pattern
	switch {
	case hasERC721Transfer && hasTokenIDIndexed:
		// ERC721 with specific token ID indexed in topics[3]
		// Very specific: typically only a few transfers per token over its lifetime
		// Example: GetTokenEvents for ERC721
		logger.DebugCtx(ctx, "Using large step size for ERC721 token query",
			zap.Uint64("stepSize", erc721TokenStepSize))
		return erc721TokenStepSize

	case hasERC721Transfer && hasOwnerIndexed:
		// ERC721 with owner address indexed (topics[1] or topics[2])
		// Moderately specific: owner likely has limited number of tokens
		// Example: GetTokenCIDsByOwnerAndBlockRange for ERC721
		logger.DebugCtx(ctx, "Using medium-large step size for ERC721 owner query",
			zap.Uint64("stepSize", erc721OwnerStepSize))
		return erc721OwnerStepSize

	case hasERC1155Transfer && hasOwnerIndexed:
		// ERC1155 with owner address indexed (topics[2]=from or topics[3]=to)
		// Moderately specific: owner may have more ERC1155 tokens than ERC721
		// Example: GetTokenCIDsByOwnerAndBlockRange for ERC1155
		logger.DebugCtx(ctx, "Using medium step size for ERC1155 owner query",
			zap.Uint64("stepSize", erc1155OwnerStepSize))
		return erc1155OwnerStepSize

	case hasERC1155Transfer && len(query.Addresses) > 0:
		// ERC1155 with only contract address specified (no owner/token ID indexed)
		// Less specific: must fetch all contract events and filter client-side
		// Note: Token ID is never indexed in ERC1155 (it's in data field)
		// Example: GetTokenEvents for ERC1155, ERC1155Balances, TokenExists
		logger.DebugCtx(ctx, "Using medium-small step size for ERC1155 contract query",
			zap.Uint64("stepSize", erc1155ContractStepSize))
		return erc1155ContractStepSize

	default:
		// Unrecognized pattern or low specificity - use conservative default
		logger.DebugCtx(ctx, "Using default step size for query",
			zap.Uint64("stepSize", defaultStepSize),
			zap.Bool("hasERC721", hasERC721Transfer),
			zap.Bool("hasERC1155", hasERC1155Transfer),
			zap.Bool("hasOwnerIndexed", hasOwnerIndexed),
			zap.Bool("hasTokenIDIndexed", hasTokenIDIndexed))
		return defaultStepSize
	}
}

// filterLogsWithPagination is an internal method that handles pagination for FilterLogs
// to work around Infura's 10k log limitation
func (f *ethereumClient) filterLogsWithPagination(ctx context.Context, query ethereum.FilterQuery) ([]types.Log, error) {
	// Use the parent context directly if it already has a deadline (e.g., from ERC1155Balances)
	// Otherwise, create a context with timeout (1 minute)
	timeoutCtx := ctx
	var cancel context.CancelFunc
	if _, hasDeadline := ctx.Deadline(); !hasDeadline {
		timeoutCtx, cancel = context.WithTimeout(ctx, time.Minute)
		defer cancel()
	}

	// If blockhash is specified, use it directly (no pagination needed)
	if query.BlockHash != nil {
		return f.client.FilterLogs(timeoutCtx, query)
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
		// Get latest block using cached provider
		latestBlockNum, err := f.GetLatestBlock(timeoutCtx)
		if err != nil {
			return nil, fmt.Errorf("failed to get latest block: %w", err)
		}
		toBlock = new(big.Int).SetUint64(latestBlockNum)
	}

	// 2. Calculate optimal step size based on query specificity
	// More specific queries (with indexed parameters) can use larger steps
	var allLogs []types.Log
	currentFrom := new(big.Int).Set(fromBlock)
	stepSize := f.calculateStepSize(timeoutCtx, query)

	logger.DebugCtx(ctx, "Starting log pagination",
		zap.Uint64("fromBlock", fromBlock.Uint64()),
		zap.Uint64("toBlock", toBlock.Uint64()),
		zap.Uint64("totalBlockRange", toBlock.Uint64()-fromBlock.Uint64()),
		zap.Uint64("initialStepSize", stepSize),
		zap.Uint64("estimatedIterations", (toBlock.Uint64()-fromBlock.Uint64())/stepSize+1))

	for currentFrom.Cmp(toBlock) < 0 {
		// Check if context is canceled or deadline exceeded before processing next range
		select {
		case <-timeoutCtx.Done():
			// Context deadline exceeded or canceled - return partial logs collected so far
			logger.WarnCtx(ctx, "Context deadline exceeded during log pagination, returning partial logs",
				zap.Int("partialLogsCount", len(allLogs)),
				zap.Uint64("processedUpToBlock", currentFrom.Uint64()-1),
				zap.Uint64("targetToBlock", toBlock.Uint64()),
			)
			return allLogs, timeoutCtx.Err()
		default:
			// Continue processing
		}

		// Calculate current range
		currentTo := new(big.Int).Add(currentFrom, new(big.Int).SetUint64(stepSize))
		if currentTo.Cmp(toBlock) > 0 {
			currentTo.Set(toBlock)
		}

		// Create query for current range
		rangeQuery := query
		rangeQuery.FromBlock = new(big.Int).Set(currentFrom)
		rangeQuery.ToBlock = currentTo

		// Try to get logs for current range with retry logic
		logs, err := f.getLogsWithRetry(timeoutCtx, rangeQuery, stepSize)
		if err != nil {
			// If timeout/canceled, return partial logs instead of error
			if timeoutCtx.Err() != nil {
				logger.WarnCtx(ctx, "Timeout during getLogsWithRetry, returning partial logs",
					zap.Int("partialLogsCount", len(allLogs)),
					zap.Uint64("processedUpToBlock", currentFrom.Uint64()-1),
					zap.Uint64("targetToBlock", toBlock.Uint64()),
				)
				return allLogs, timeoutCtx.Err()
			}
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
//
// This method provides a safety net for the dynamic step sizing in filterLogsWithPagination:
// Even if the initial step size is too aggressive (e.g., a high-activity contract with more
// than 10k events), this method will automatically halve the step size and retry until it
// succeeds or the step size reaches 0. This makes the system adaptive to actual on-chain activity.
//
// After successfully processing a chunk, the step size is reset to the original value for the
// next chunk. This handles burst activity (e.g., popular NFT mints) in specific ranges without
// unnecessarily reducing the step size for all remaining chunks.
func (f *ethereumClient) getLogsWithRetry(ctx context.Context, query ethereum.FilterQuery, stepSize uint64) ([]types.Log, error) {
	originalStepSize := stepSize
	currentStepSize := stepSize

	var allLogs []types.Log
	currentFrom := new(big.Int).Set(query.FromBlock)

	// Process the entire range in chunks
	for currentFrom.Cmp(query.ToBlock) <= 0 {
		// Check if context is canceled or deadline exceeded
		select {
		case <-ctx.Done():
			// Return partial logs on timeout
			return allLogs, ctx.Err()
		default:
			// Continue processing
		}

		// Calculate current range based on current step size
		currentTo := new(big.Int).Add(currentFrom, new(big.Int).SetUint64(currentStepSize-1))
		if currentTo.Cmp(query.ToBlock) > 0 {
			currentTo.Set(query.ToBlock)
		}

		// Create query for current chunk
		queryCopy := query
		queryCopy.FromBlock = new(big.Int).Set(currentFrom)
		queryCopy.ToBlock = new(big.Int).Set(currentTo)

		logs, err := f.client.FilterLogs(ctx, queryCopy)
		if err == nil {
			// Success - accumulate logs and move to next chunk
			allLogs = append(allLogs, logs...)

			// Move to next chunk and reset step size to original
			// This allows handling burst activity in specific ranges without
			// permanently reducing the step size for all remaining chunks
			currentFrom.SetUint64(currentTo.Uint64() + 1)
			currentStepSize = originalStepSize
			continue
		}

		// Check if error is due to context timeout/cancellation
		if ctx.Err() != nil {
			return allLogs, ctx.Err()
		}

		// 3. If other errors than rate limited, return error
		if !isTooManyResultsError(err) {
			return nil, err
		}

		// 4. If rate limited, divide the step by 2 and try again (for the same range)
		currentStepSize = currentStepSize / 2
		if currentStepSize == 0 {
			// If step size is 0, return the logs we have so far
			logger.WarnCtx(ctx, "Step size is 0, returning partial logs",
				zap.Int("partialLogsCount", len(allLogs)),
				zap.Uint64("processedUpToBlock", currentFrom.Uint64()-1),
				zap.Uint64("targetToBlock", query.ToBlock.Uint64()),
			)
			return allLogs, nil
		}

		// Sleep for 1 second to avoid overwhelming the API
		f.clock.Sleep(time.Second * 1)

		logger.DebugCtx(ctx, "Too many results, reducing step size and retrying same range",
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
		strings.Contains(errStr, "too many results")
}

// GetLatestBlock returns the latest block number using the cached provider
func (f *ethereumClient) GetLatestBlock(ctx context.Context) (uint64, error) {
	return f.blockProvider.GetLatestBlock(ctx)
}

// ERC721TokenURI fetches the tokenURI from an ERC721 contract
func (f *ethereumClient) ERC721TokenURI(ctx context.Context, contractAddress string, tokenNumber string) (string, error) {
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
	result, err := f.client.CallContract(ctx, ethereum.CallMsg{
		To:   &contractAddr,
		Data: data,
	}, nil)
	if err != nil {
		return "", fmt.Errorf("failed to call contract: %w", err)
	}
	if len(result) == 0 {
		// This is likely the contract has been self-destructed
		return "", ErrContractNotFound
	}

	var uri string
	if err := tokenURIABI.UnpackIntoInterface(&uri, "tokenURI", result); err != nil {
		return "", fmt.Errorf("failed to unpack result: %w", err)
	}

	return uri, nil
}

// ERC721OwnerOf fetches the current owner of an ERC721 token
func (f *ethereumClient) ERC721OwnerOf(ctx context.Context, contractAddress, tokenNumber string) (string, error) {
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
	result, err := f.client.CallContract(ctx, ethereum.CallMsg{
		To:   &contractAddr,
		Data: data,
	}, nil)
	if err != nil {
		return "", fmt.Errorf("failed to call contract: %w", err)
	}
	if len(result) == 0 {
		// This is likely the contract has been self-destructed
		return "", ErrContractNotFound
	}

	var owner common.Address
	if err := ownerOfABI.UnpackIntoInterface(&owner, "ownerOf", result); err != nil {
		return "", fmt.Errorf("failed to unpack result: %w", err)
	}

	return owner.Hex(), nil
}

// ERC1155URI fetches the uri from an ERC1155 contract
func (f *ethereumClient) ERC1155URI(ctx context.Context, contractAddress, tokenNumber string) (string, error) {
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
	result, err := f.client.CallContract(ctx, ethereum.CallMsg{
		To:   &contractAddr,
		Data: data,
	}, nil)
	if err != nil {
		return "", fmt.Errorf("failed to call contract: %w", err)
	}
	if len(result) == 0 {
		// This is likely the contract has been self-destructed
		return "", ErrContractNotFound
	}

	var uri string
	if err := uriABI.UnpackIntoInterface(&uri, "uri", result); err != nil {
		return "", fmt.Errorf("failed to unpack result: %w", err)
	}

	return uri, nil
}

// ERC1155BalanceOf fetches the balance of a specific token ID for an owner from an ERC1155 contract
func (f *ethereumClient) ERC1155BalanceOf(ctx context.Context, contractAddress, ownerAddress, tokenNumber string) (string, error) {
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
	result, err := f.client.CallContract(ctx, ethereum.CallMsg{
		To:   &contractAddr,
		Data: data,
	}, nil)
	if err != nil {
		return "", fmt.Errorf("failed to call contract: %w", err)
	}
	if len(result) == 0 {
		// This is likely the contract has been self-destructed
		return "", ErrContractNotFound
	}

	var balance *big.Int
	if err := balanceOfABI.UnpackIntoInterface(&balance, "balanceOf", result); err != nil {
		return "", fmt.Errorf("failed to unpack result: %w", err)
	}

	return balance.String(), nil
}

// ERC1155BalanceOfBatch fetches balances for multiple addresses for a specific token ID from an ERC1155 contract
// It uses the ERC1155 balanceOfBatch function which allows querying multiple address-token pairs in a single RPC call
// Handles chunking internally to respect RPC limits (200 address-token pairs per chunk by default)
// Returns a map of address -> balance string (only includes addresses with non-zero balances)
func (f *ethereumClient) ERC1155BalanceOfBatch(ctx context.Context, contractAddress, tokenNumber string, addresses []string) (map[string]string, error) {
	// Parse token number to big.Int
	tokenID, ok := new(big.Int).SetString(tokenNumber, 10)
	if !ok {
		return nil, fmt.Errorf("invalid token number: %s", tokenNumber)
	}

	// ERC1155 balanceOfBatch function signature: balanceOfBatch(address[],uint256[]) returns (uint256[])
	balanceOfBatchABI, err := abi.JSON(strings.NewReader(`[{"constant":true,"inputs":[{"name":"accounts","type":"address[]"},{"name":"ids","type":"uint256[]"}],"name":"balanceOfBatch","outputs":[{"name":"","type":"uint256[]"}],"payable":false,"stateMutability":"view","type":"function"}]`))
	if err != nil {
		return nil, fmt.Errorf("failed to parse ABI: %w", err)
	}

	contractAddr := common.HexToAddress(contractAddress)
	result := make(map[string]string)

	// Process in chunks to avoid hitting RPC limits
	// Start with 200 pairs per chunk as recommended
	const chunkSize = 200

	for i := 0; i < len(addresses); i += chunkSize {
		// Calculate chunk boundaries
		end := min(i+chunkSize, len(addresses))
		chunk := addresses[i:end]

		// Build arrays for the batch call
		// For querying the same token ID for multiple addresses,
		// we need parallel arrays of addresses and token IDs
		accountAddresses := make([]common.Address, len(chunk))
		tokenIDs := make([]*big.Int, len(chunk))

		for j, addr := range chunk {
			accountAddresses[j] = common.HexToAddress(addr)
			tokenIDs[j] = tokenID // Same token ID for all addresses
		}

		// Pack the call data
		data, err := balanceOfBatchABI.Pack("balanceOfBatch", accountAddresses, tokenIDs)
		if err != nil {
			return nil, fmt.Errorf("failed to pack balanceOfBatch data: %w", err)
		}

		// Make the contract call
		callResult, err := f.client.CallContract(ctx, ethereum.CallMsg{
			To:   &contractAddr,
			Data: data,
		}, nil)
		if err != nil {
			return nil, fmt.Errorf("failed to call balanceOfBatch for chunk %d-%d: %w", i, end, err)
		}
		if len(callResult) == 0 {
			// This is likely the contract has been self-destructed
			return nil, ErrContractNotFound
		}

		// Unpack the result
		var balances []*big.Int
		if err := balanceOfBatchABI.UnpackIntoInterface(&balances, "balanceOfBatch", callResult); err != nil {
			return nil, fmt.Errorf("failed to unpack balanceOfBatch result: %w", err)
		}

		// Verify result length matches input length
		if len(balances) != len(chunk) {
			return nil, fmt.Errorf("balanceOfBatch returned unexpected number of results: expected %d, got %d", len(chunk), len(balances))
		}

		// Map addresses to their balances (only store non-zero balances)
		for j, balance := range balances {
			if balance.Cmp(big.NewInt(0)) > 0 {
				result[chunk[j]] = balance.String()
			}
		}
	}

	return result, nil
}

// GetTokenEvents fetches all historical events for a specific token
func (f *ethereumClient) GetTokenEvents(ctx context.Context, contractAddress, tokenNumber string, standard domain.ChainStandard) ([]domain.BlockchainEvent, error) {
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
	logs, err := f.filterLogsWithPagination(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to filter logs: %w", err)
	}

	// Parse logs and convert to EthereumTokenEvent
	events := make([]domain.BlockchainEvent, 0)
	for _, vLog := range logs {
		event, err := f.ParseEventLog(ctx, vLog)
		if err != nil {
			// Log error but continue processing
			logger.WarnCtx(ctx, "Failed to parse event log", zap.Error(err))
			continue
		}
		if event != nil {
			events = append(events, *event)
		}
	}

	// Sort events by timestamp, then block number, then transaction index for deterministic ordering
	sort.SliceStable(events, func(i, j int) bool {
		// First, compare timestamps
		if !events[i].Timestamp.Equal(events[j].Timestamp) {
			return events[i].Timestamp.Before(events[j].Timestamp)
		}

		// If timestamps are equal, compare block numbers
		if events[i].BlockNumber != events[j].BlockNumber {
			return events[i].BlockNumber < events[j].BlockNumber
		}

		// If block numbers are also equal, compare transaction indices
		return events[i].TxIndex < events[j].TxIndex
	})

	return events, nil
}

// ERC1155Balances calculates all current ERC1155 token balances by replaying transfer events from oldest to newest
// It scans backward from the latest block with a maximum threshold of 10M blocks
// If the operation times out (30 seconds), it returns partial balances gracefully
//
// FIXME: This approach has limitations for high-activity ERC1155 contracts:
// 1. Scanning only recent 10M blocks may miss historical transfers, resulting in incomplete balances
// 2. Fetching all contract events and filtering client-side is inefficient (can't filter by token ID at RPC level)
// 3. For contracts with millions of events, this can still hit Infura's 10k log limitation repeatedly
func (f *ethereumClient) ERC1155Balances(ctx context.Context, contractAddress, tokenNumber string) (map[string]string, error) {
	// Create a timeout context (30 seconds) to prevent indefinite blocking
	timeoutCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	// Parse token number to big.Int
	tokenID, ok := new(big.Int).SetString(tokenNumber, 10)
	if !ok {
		return nil, fmt.Errorf("invalid token number: %s", tokenNumber)
	}

	contractAddr := common.HexToAddress(contractAddress)

	// Get the latest block number using cached provider
	latestBlock, err := f.GetLatestBlock(timeoutCtx)
	if err != nil {
		return nil, fmt.Errorf("failed to get latest block: %w", err)
	}

	// Calculate fromBlock: scan backward from latest block with max 10M blocks threshold
	const maxBlockThreshold = uint64(10_000_000)
	var fromBlock uint64
	if latestBlock > maxBlockThreshold {
		fromBlock = latestBlock - maxBlockThreshold
	} else {
		fromBlock = 0
	}

	logger.InfoCtx(ctx, "Fetching ERC1155 balances",
		zap.String("contract", contractAddress),
		zap.String("tokenNumber", tokenNumber),
		zap.Uint64("fromBlock", fromBlock),
		zap.Uint64("toBlock", latestBlock),
		zap.Uint64("blockRange", latestBlock-fromBlock),
	)

	// For ERC1155, token ID is in data, not topics, so we fetch all TransferSingle events for this contract
	// and filter by token ID later
	query := ethereum.FilterQuery{
		FromBlock: new(big.Int).SetUint64(fromBlock),
		ToBlock:   new(big.Int).SetUint64(latestBlock),
		Addresses: []common.Address{contractAddr},
		Topics: [][]common.Hash{
			{
				transferSingleEventSignature,
				//transferBatchEventSignature, // FIXME: Handle batch transfers properly
			},
		},
	}

	// Fetch logs with pagination to handle Infura's 10k limitation
	// If timeout occurs, we'll get partial logs
	logs, err := f.filterLogsWithPagination(timeoutCtx, query)
	if err != nil {
		// If context deadline exceeded, return partial balances with warning
		if timeoutCtx.Err() == context.DeadlineExceeded {
			logger.WarnCtx(timeoutCtx, "ERC1155 balance fetch timed out, returning partial balances",
				zap.String("contract", contractAddress),
				zap.String("tokenNumber", tokenNumber),
				zap.Int("partialLogsCount", len(logs)),
				zap.Uint64("fromBlock", fromBlock),
				zap.Uint64("toBlock", latestBlock),
			)
			// Continue with partial logs - better than nothing
		} else {
			return nil, fmt.Errorf("failed to filter logs: %w", err)
		}
	}

	// Build balances map by replaying transfer logs
	balances := make(map[string]*big.Int)

	for _, vLog := range logs {
		// ERC1155 TransferSingle(address indexed operator, address indexed from, address indexed to, uint256 id, uint256 value)
		if len(vLog.Topics) != 4 {
			logger.WarnCtx(timeoutCtx, "Invalid ERC1155 TransferSingle event: unexpected topic count",
				zap.Int("topics", len(vLog.Topics)),
				zap.String("txHash", vLog.TxHash.Hex()))
			continue
		}
		if len(vLog.Data) < 64 {
			logger.WarnCtx(timeoutCtx, "Invalid ERC1155 TransferSingle event: insufficient data",
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

// GetERC1155BalanceAndEventsForOwner fetches the current balance and transfer events for a specific ERC1155 token and owner
// This is optimized for owner-specific indexing by filtering events where owner is sender or receiver
// Supports both TransferSingle and TransferBatch events
func (f *ethereumClient) GetERC1155BalanceAndEventsForOwner(ctx context.Context, contractAddress, tokenNumber, ownerAddress string) (string, []domain.BlockchainEvent, error) {
	// Parse token number to big.Int
	tokenID, ok := new(big.Int).SetString(tokenNumber, 10)
	if !ok {
		return "", nil, fmt.Errorf("invalid token number: %s", tokenNumber)
	}

	contractAddr := common.HexToAddress(contractAddress)
	owner := common.HexToAddress(ownerAddress)
	ownerHash := common.BytesToHash(owner.Bytes())

	// Step 1: Get current balance via contract call (most reliable for current state)
	balance, err := f.ERC1155BalanceOf(ctx, contractAddress, ownerAddress, tokenNumber)
	if err != nil {
		return "", nil, fmt.Errorf("failed to get ERC1155 balance: %w", err)
	}

	// Step 2: Get the latest block number
	latestBlock, err := f.GetLatestBlock(ctx)
	if err != nil {
		return "", nil, fmt.Errorf("failed to get latest block: %w", err)
	}

	// Calculate fromBlock: scan from genesis (we want complete history for this owner)
	// Note: We could optimize by scanning from contract deployment block if needed
	fromBlock := uint64(0)

	logger.InfoCtx(ctx, "Fetching ERC1155 events for owner",
		zap.String("contract", contractAddress),
		zap.String("tokenNumber", tokenNumber),
		zap.String("owner", ownerAddress),
		zap.Uint64("fromBlock", fromBlock),
		zap.Uint64("toBlock", latestBlock),
	)

	// Step 3: Define queries to fetch TransferSingle and TransferBatch events involving this owner
	queries := []ethereum.FilterQuery{
		// TransferSingle where owner is `from` (topic[2])
		{
			FromBlock: new(big.Int).SetUint64(fromBlock),
			ToBlock:   new(big.Int).SetUint64(latestBlock),
			Addresses: []common.Address{contractAddr},
			Topics: [][]common.Hash{
				{transferSingleEventSignature},
				nil,         // any operator
				{ownerHash}, // from address
			},
		},
		// TransferSingle where owner is `to` (topic[3])
		{
			FromBlock: new(big.Int).SetUint64(fromBlock),
			ToBlock:   new(big.Int).SetUint64(latestBlock),
			Addresses: []common.Address{contractAddr},
			Topics: [][]common.Hash{
				{transferSingleEventSignature},
				nil,         // any operator
				nil,         // any from address
				{ownerHash}, // to address
			},
		},
		// TransferBatch where owner is `from` (topic[2])
		{
			FromBlock: new(big.Int).SetUint64(fromBlock),
			ToBlock:   new(big.Int).SetUint64(latestBlock),
			Addresses: []common.Address{contractAddr},
			Topics: [][]common.Hash{
				{transferBatchEventSignature},
				nil,         // any operator
				{ownerHash}, // from address
			},
		},
		// TransferBatch where owner is `to` (topic[3])
		{
			FromBlock: new(big.Int).SetUint64(fromBlock),
			ToBlock:   new(big.Int).SetUint64(latestBlock),
			Addresses: []common.Address{contractAddr},
			Topics: [][]common.Hash{
				{transferBatchEventSignature},
				nil,         // any operator
				nil,         // any from address
				{ownerHash}, // to address
			},
		},
	}

	// Step 4: Execute all queries in parallel
	type queryResult struct {
		logs []types.Log
		err  error
	}

	resultsCh := make(chan queryResult, len(queries))
	for _, q := range queries {
		query := q // capture loop variable
		go func() {
			logs, err := f.filterLogsWithPagination(ctx, query)
			resultsCh <- queryResult{logs: logs, err: err}
		}()
	}

	// Collect results
	var allLogs []types.Log
	for range queries {
		result := <-resultsCh
		if result.err != nil {
			return "", nil, fmt.Errorf("failed to fetch logs: %w", result.err)
		}
		allLogs = append(allLogs, result.logs...)
	}

	// Step 5: Sort logs by block number and log index for chronological processing
	sort.Slice(allLogs, func(i, j int) bool {
		if allLogs[i].BlockNumber != allLogs[j].BlockNumber {
			return allLogs[i].BlockNumber < allLogs[j].BlockNumber
		}
		return allLogs[i].Index < allLogs[j].Index
	})

	// Step 6: Parse logs and extract events for the specific token
	events := make([]domain.BlockchainEvent, 0)

	for _, vLog := range allLogs {
		if len(vLog.Topics) < 1 {
			continue
		}

		switch vLog.Topics[0] {
		case transferSingleEventSignature:
			// TransferSingle(address indexed operator, address indexed from, address indexed to, uint256 id, uint256 value)
			if len(vLog.Topics) != 4 || len(vLog.Data) < 64 {
				logger.WarnCtx(ctx, "Invalid TransferSingle event",
					zap.String("txHash", vLog.TxHash.Hex()),
					zap.Int("topics", len(vLog.Topics)))
				continue
			}

			// Parse token ID from data (first 32 bytes)
			logTokenID := new(big.Int).SetBytes(vLog.Data[0:32])

			// Filter by token ID - only include events for the token we care about
			if logTokenID.Cmp(tokenID) != 0 {
				continue
			}

			// Parse the event using existing ParseEventLog
			event, err := f.ParseEventLog(ctx, vLog)
			if err != nil {
				logger.WarnCtx(ctx, "Failed to parse TransferSingle event",
					zap.Error(err),
					zap.String("txHash", vLog.TxHash.Hex()))
				continue
			}

			if event != nil {
				events = append(events, *event)
			}

		case transferBatchEventSignature:
			// TransferBatch(address indexed operator, address indexed from, address indexed to, uint256[] ids, uint256[] values)
			if len(vLog.Topics) != 4 || len(vLog.Data) < 64 {
				logger.WarnCtx(ctx, "Invalid TransferBatch event",
					zap.String("txHash", vLog.TxHash.Hex()),
					zap.Int("topics", len(vLog.Topics)))
				continue
			}

			// Parse the batch event and extract only transfers for our token
			batchEvents, err := f.parseTransferBatchForToken(ctx, vLog, tokenID)
			if err != nil {
				logger.WarnCtx(ctx, "Failed to parse TransferBatch event",
					zap.Error(err),
					zap.String("txHash", vLog.TxHash.Hex()))
				continue
			}

			events = append(events, batchEvents...)
		}
	}

	logger.InfoCtx(ctx, "Fetched ERC1155 events for owner",
		zap.String("contract", contractAddress),
		zap.String("tokenNumber", tokenNumber),
		zap.String("owner", ownerAddress),
		zap.Int("eventCount", len(events)),
		zap.String("currentBalance", balance),
	)

	return balance, events, nil
}

// parseTransferBatchForToken parses a TransferBatch event and extracts transfers for a specific token ID
func (f *ethereumClient) parseTransferBatchForToken(ctx context.Context, vLog types.Log, tokenID *big.Int) ([]domain.BlockchainEvent, error) {
	// TransferBatch(address indexed operator, address indexed from, address indexed to, uint256[] ids, uint256[] values)
	// Data contains ABI-encoded arrays: ids[] and values[]

	if len(vLog.Topics) != 4 {
		return nil, fmt.Errorf("invalid TransferBatch event: expected 4 topics, got %d", len(vLog.Topics))
	}

	// Parse addresses from topics
	fromAddress := common.BytesToAddress(vLog.Topics[2].Bytes()).Hex()
	toAddress := common.BytesToAddress(vLog.Topics[3].Bytes()).Hex()

	// Decode the dynamic arrays from Data field
	// The data contains two dynamic arrays: uint256[] ids, uint256[] values
	// ABI encoding for dynamic arrays:
	// [0:32]   offset to ids array
	// [32:64]  offset to values array
	// [64:96]  length of ids array
	// [96:...] ids array elements (32 bytes each)
	// [...:..] length of values array
	// [...:..] values array elements (32 bytes each)

	if len(vLog.Data) < 128 {
		return nil, fmt.Errorf("insufficient data for TransferBatch event: got %d bytes", len(vLog.Data))
	}

	// Parse ids array
	idsOffset := new(big.Int).SetBytes(vLog.Data[0:32]).Uint64()
	if idsOffset+32 > uint64(len(vLog.Data)) {
		return nil, fmt.Errorf("invalid ids offset: %d", idsOffset)
	}

	idsLength := new(big.Int).SetBytes(vLog.Data[idsOffset : idsOffset+32]).Uint64()
	if idsOffset+32+idsLength*32 > uint64(len(vLog.Data)) {
		return nil, fmt.Errorf("invalid ids array length: %d", idsLength)
	}

	// Parse values array
	valuesOffset := new(big.Int).SetBytes(vLog.Data[32:64]).Uint64()
	if valuesOffset+32 > uint64(len(vLog.Data)) {
		return nil, fmt.Errorf("invalid values offset: %d", valuesOffset)
	}

	valuesLength := new(big.Int).SetBytes(vLog.Data[valuesOffset : valuesOffset+32]).Uint64()
	if valuesOffset+32+valuesLength*32 > uint64(len(vLog.Data)) {
		return nil, fmt.Errorf("invalid values array length: %d", valuesLength)
	}

	if idsLength != valuesLength {
		return nil, fmt.Errorf("ids and values array length mismatch: %d != %d", idsLength, valuesLength)
	}

	// Extract events for matching token IDs
	events := make([]domain.BlockchainEvent, 0)

	for i := range idsLength {
		idStart := idsOffset + 32 + i*32
		idEnd := idStart + 32
		id := new(big.Int).SetBytes(vLog.Data[idStart:idEnd])

		// Only process if this is the token we're looking for
		if id.Cmp(tokenID) != 0 {
			continue
		}

		valueStart := valuesOffset + 32 + i*32
		valueEnd := valueStart + 32
		value := new(big.Int).SetBytes(vLog.Data[valueStart:valueEnd])

		// Get block timestamp
		blockTimestamp, err := f.blockProvider.GetBlockTimestamp(ctx, vLog.BlockNumber)
		if err != nil {
			logger.WarnCtx(ctx, "Failed to get block timestamp, using current time",
				zap.Error(err),
				zap.Uint64("blockNumber", vLog.BlockNumber))
			blockTimestamp = f.clock.Now()
		}

		blockHash := vLog.BlockHash.Hex()

		// Create event
		event := domain.BlockchainEvent{
			Chain:           f.chainID,
			ContractAddress: vLog.Address.Hex(),
			Standard:        domain.StandardERC1155,
			TokenNumber:     id.String(),
			FromAddress:     &fromAddress,
			ToAddress:       &toAddress,
			Quantity:        value.String(),
			TxHash:          vLog.TxHash.Hex(),
			BlockNumber:     vLog.BlockNumber,
			BlockHash:       &blockHash,
			TxIndex:         uint64(vLog.TxIndex), //nolint:gosec,G115
			Timestamp:       blockTimestamp,
		}
		event.EventType = domain.TransferEventType(event.FromAddress, event.ToAddress)

		events = append(events, event)
	}

	return events, nil
}

// GetTokenCIDsByOwnerAndBlockRange retrieves all token CIDs with block numbers for an owner within a block range
// It queries both ERC721 and ERC1155 transfer events where the address is either sender or receiver
// Returns tokens that the owner possesses at the end of the block range with their last interaction block
func (f *ethereumClient) GetTokenCIDsByOwnerAndBlockRange(ctx context.Context, ownerAddress string, fromBlock, toBlock uint64) ([]domain.TokenWithBlock, error) {
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
		// ERC1155 TransferBatch where address is `from` (topic[2])
		{
			FromBlock: new(big.Int).SetUint64(fromBlock),
			ToBlock:   new(big.Int).SetUint64(toBlock),
			Topics: [][]common.Hash{
				{transferBatchEventSignature}, // TransferBatch event
				nil,                           // any operator
				{ownerHash},                   // from address
			},
		},
		// ERC1155 TransferBatch where address is `to` (topic[3])
		{
			FromBlock: new(big.Int).SetUint64(fromBlock),
			ToBlock:   new(big.Int).SetUint64(toBlock),
			Topics: [][]common.Hash{
				{transferBatchEventSignature}, // TransferBatch event
				nil,                           // any operator
				nil,                           // any from address
				{ownerHash},                   // to address
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
			logs, err := f.filterLogsWithPagination(ctx, query)
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

	// Deduplicate logs for safer ownership tracking
	// Use a map with unique key: blockNumber + txHash + logIndex
	logMap := make(map[string]types.Log)
	for _, vLog := range allLogs {
		key := fmt.Sprintf("%d-%s-%d", vLog.BlockNumber, vLog.TxHash.Hex(), vLog.Index)
		logMap[key] = vLog
	}

	// Convert back to slice and sort chronologically
	allLogs = make([]types.Log, 0, len(logMap))
	for _, vLog := range logMap {
		allLogs = append(allLogs, vLog)
	}

	// Sort logs chronologically (by block number, then by log index)
	// This ensures deterministic processing order for correct ownership tracking
	sort.Slice(allLogs, func(i, j int) bool {
		if allLogs[i].BlockNumber != allLogs[j].BlockNumber {
			return allLogs[i].BlockNumber < allLogs[j].BlockNumber
		}
		return allLogs[i].Index < allLogs[j].Index
	})

	// Track balance changes per token to determine ownership at the end of the block range
	// For ERC721: map stores the last transfer log (to determine final owner)
	// For ERC1155: map stores net balance change (incoming - outgoing)
	type tokenBalance struct {
		standard    domain.ChainStandard
		lastLog     *types.Log // For ERC721: last transfer log
		netBalance  *big.Int   // For ERC1155: net balance change
		blockNumber uint64     // Block number of last update
		logIndex    uint       // Log index of last update
	}
	balanceMap := make(map[domain.TokenCID]*tokenBalance)

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

			fromAddr := common.BytesToAddress(vLog.Topics[1].Bytes())
			toAddr := common.BytesToAddress(vLog.Topics[2].Bytes())

			// Skip if neither from nor to is the owner
			if fromAddr != owner && toAddr != owner {
				continue
			}

			tokenID := new(big.Int).SetBytes(vLog.Topics[3].Bytes())
			tokenCID := domain.NewTokenCID(f.chainID, domain.StandardERC721, vLog.Address.Hex(), tokenID.String())

			// For ERC721, track the last transfer log (chronologically)
			existing := balanceMap[tokenCID]
			if existing == nil ||
				vLog.BlockNumber > existing.blockNumber ||
				(vLog.BlockNumber == existing.blockNumber && vLog.Index > existing.logIndex) {
				logCopy := vLog
				balanceMap[tokenCID] = &tokenBalance{
					standard:    domain.StandardERC721,
					lastLog:     &logCopy,
					blockNumber: vLog.BlockNumber,
					logIndex:    vLog.Index,
				}
			}

		case transferSingleEventSignature:
			// ERC1155 TransferSingle (4 topics: signature, operator, from, to; data contains tokenId and value)
			if len(vLog.Topics) != 4 || len(vLog.Data) < 64 {
				continue
			}

			fromAddr := common.BytesToAddress(vLog.Topics[2].Bytes())
			toAddr := common.BytesToAddress(vLog.Topics[3].Bytes())

			// Skip if neither from nor to is the owner
			if fromAddr != owner && toAddr != owner {
				continue
			}

			// Parse token ID and amount from data
			tokenID := new(big.Int).SetBytes(vLog.Data[0:32])
			amount := new(big.Int).SetBytes(vLog.Data[32:64])

			tokenCID := domain.NewTokenCID(f.chainID, domain.StandardERC1155, vLog.Address.Hex(), tokenID.String())

			// For ERC1155, track net balance change
			existing := balanceMap[tokenCID]
			if existing == nil {
				existing = &tokenBalance{
					standard:    domain.StandardERC1155,
					netBalance:  big.NewInt(0),
					blockNumber: vLog.BlockNumber,
					logIndex:    vLog.Index,
				}
				balanceMap[tokenCID] = existing
			}

			// Calculate balance change: +amount if receiving, -amount if sending
			if toAddr == owner {
				existing.netBalance.Add(existing.netBalance, amount)
			}
			if fromAddr == owner {
				existing.netBalance.Sub(existing.netBalance, amount)
			}

			// Update block number to track latest interaction
			if vLog.BlockNumber > existing.blockNumber ||
				(vLog.BlockNumber == existing.blockNumber && vLog.Index > existing.logIndex) {
				existing.blockNumber = vLog.BlockNumber
				existing.logIndex = vLog.Index
			}

		case transferBatchEventSignature:
			// ERC1155 TransferBatch (4 topics: signature, operator, from, to; data contains tokenIds and values arrays)
			if len(vLog.Topics) != 4 || len(vLog.Data) < 64 {
				continue
			}

			fromAddr := common.BytesToAddress(vLog.Topics[2].Bytes())
			toAddr := common.BytesToAddress(vLog.Topics[3].Bytes())

			// Skip if neither from nor to is the owner
			if fromAddr != owner && toAddr != owner {
				continue
			}

			// Parse arrays from data - ABI encoding of two arrays (ids and values)
			// Data format: [offset_ids, offset_values, length_ids, ...ids..., length_values, ...values...]
			if len(vLog.Data) < 128 {
				continue
			}

			// Read array lengths and data offsets
			idsOffset := new(big.Int).SetBytes(vLog.Data[0:32]).Uint64()
			valuesOffset := new(big.Int).SetBytes(vLog.Data[32:64]).Uint64()

			if idsOffset+32 > uint64(len(vLog.Data)) || valuesOffset+32 > uint64(len(vLog.Data)) {
				continue
			}

			idsLength := new(big.Int).SetBytes(vLog.Data[idsOffset : idsOffset+32]).Uint64()
			valuesLength := new(big.Int).SetBytes(vLog.Data[valuesOffset : valuesOffset+32]).Uint64()

			if idsLength != valuesLength {
				continue
			}

			idsStart := idsOffset + 32
			valuesStart := valuesOffset + 32

			if idsStart+idsLength*32 > uint64(len(vLog.Data)) || valuesStart+valuesLength*32 > uint64(len(vLog.Data)) {
				continue
			}

			// Process each token in the batch
			for i := range idsLength {
				idStart := idsStart + i*32
				valueStart := valuesStart + i*32

				tokenID := new(big.Int).SetBytes(vLog.Data[idStart : idStart+32])
				amount := new(big.Int).SetBytes(vLog.Data[valueStart : valueStart+32])

				tokenCID := domain.NewTokenCID(f.chainID, domain.StandardERC1155, vLog.Address.Hex(), tokenID.String())

				// For ERC1155, track net balance change
				existing := balanceMap[tokenCID]
				if existing == nil {
					existing = &tokenBalance{
						standard:    domain.StandardERC1155,
						netBalance:  big.NewInt(0),
						blockNumber: vLog.BlockNumber,
						logIndex:    vLog.Index,
					}
					balanceMap[tokenCID] = existing
				}

				// Calculate balance change: +amount if receiving, -amount if sending
				if toAddr == owner {
					existing.netBalance.Add(existing.netBalance, amount)
				}
				if fromAddr == owner {
					existing.netBalance.Sub(existing.netBalance, amount)
				}

				// Update block number to track latest interaction
				if vLog.BlockNumber > existing.blockNumber ||
					(vLog.BlockNumber == existing.blockNumber && vLog.Index > existing.logIndex) {
					existing.blockNumber = vLog.BlockNumber
					existing.logIndex = vLog.Index
				}
			}
		}
	}

	// Filter tokens based on final ownership status and include block numbers
	tokensWithBlocks := make([]domain.TokenWithBlock, 0, len(balanceMap))
	for tokenCID, balance := range balanceMap {
		switch balance.standard {
		case domain.StandardERC721:
			// For ERC721: owner must be the 'to' address in the last transfer
			// ERC721 Transfer must have exactly 4 topics: [signature, from, to, tokenId]
			if balance.lastLog != nil && len(balance.lastLog.Topics) == 4 {
				toAddr := common.BytesToAddress(balance.lastLog.Topics[2].Bytes())
				if toAddr == owner {
					tokensWithBlocks = append(tokensWithBlocks, domain.TokenWithBlock{
						TokenCID:    tokenCID,
						BlockNumber: balance.blockNumber,
					})
				}
			}
		case domain.StandardERC1155:
			// For ERC1155: net balance must be positive
			if balance.netBalance != nil && balance.netBalance.Cmp(big.NewInt(0)) > 0 {
				tokensWithBlocks = append(tokensWithBlocks, domain.TokenWithBlock{
					TokenCID:    tokenCID,
					BlockNumber: balance.blockNumber,
				})
			}
		}
	}

	return tokensWithBlocks, nil
}

// parseLog parses an Ethereum log into a standardized blockchain event
func (f *ethereumClient) ParseEventLog(ctx context.Context, vLog types.Log) (*domain.BlockchainEvent, error) {
	blockHash := vLog.BlockHash.Hex()
	event := &domain.BlockchainEvent{
		Chain:           f.chainID,
		ContractAddress: vLog.Address.Hex(),
		TxHash:          vLog.TxHash.Hex(),
		BlockNumber:     vLog.BlockNumber,
		BlockHash:       &blockHash,
		Timestamp:       time.Unix(int64(vLog.BlockTimestamp), 0), //nolint:gosec,G115
	}

	// Parse based on event signature
	switch vLog.Topics[0] {
	case transferEventSignature:
		// This signature is shared by ERC20 and ERC721
		// ERC20 has 3 topics (signature, from, to) with value in data
		// ERC721 has 4 topics (signature, from, to, tokenId) with no data

		if len(vLog.Topics) == 3 {
			// ERC20 Transfer - skip as we only index NFTs
			logger.DebugCtx(ctx, "Skipping ERC20 transfer event",
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
		logger.DebugCtx(ctx, "Skipping ERC1155 TransferBatch event",
			zap.String("contract", vLog.Address.Hex()),
			zap.String("txHash", vLog.TxHash.Hex()))
		return nil, nil // skip ERC1155 TransferBatch events

	case metadataUpdateEventSignature:
		// EIP-4906 MetadataUpdate(uint256 _tokenId)
		// _tokenId is not indexed, so it's in the data field
		if len(vLog.Topics) != 1 {
			return nil, fmt.Errorf("invalid MetadataUpdate event: expected 1 topic, got %d", len(vLog.Topics))
		}
		if len(vLog.Data) < 32 {
			return nil, fmt.Errorf("invalid MetadataUpdate event: insufficient data")
		}

		event.Standard = domain.StandardERC721 // EIP-4906 is for ERC721
		event.TokenNumber = new(big.Int).SetBytes(vLog.Data[0:32]).String()
		event.EventType = domain.EventTypeMetadataUpdate
		event.Quantity = "1"

	case batchMetadataUpdateEventSignature:
		// EIP-4906 BatchMetadataUpdate(uint256 _fromTokenId, uint256 _toTokenId)
		// _fromTokenId and _toTokenId are not indexed, so they're in the data field
		// Emit a single range event to avoid flooding NATS queue
		// The event-bridge will handle expanding this into individual token updates
		if len(vLog.Topics) != 1 {
			return nil, fmt.Errorf("invalid BatchMetadataUpdate event: expected 1 topic, got %d", len(vLog.Topics))
		}
		if len(vLog.Data) < 64 {
			return nil, fmt.Errorf("invalid BatchMetadataUpdate event: insufficient data")
		}

		fromTokenId := new(big.Int).SetBytes(vLog.Data[0:32])
		toTokenId := new(big.Int).SetBytes(vLog.Data[32:64])

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
func (f *ethereumClient) GetContractDeployer(ctx context.Context, contractAddress string, minBlock uint64) (string, error) {
	addr := common.HexToAddress(contractAddress)

	// Get current block number using cached provider
	latestBlock, err := f.GetLatestBlock(ctx)
	if err != nil {
		return "", fmt.Errorf("failed to get latest block: %w", err)
	}

	// Validate minBlock
	if minBlock > latestBlock {
		return "", fmt.Errorf("minBlock (%d) is greater than latest block (%d)", minBlock, latestBlock)
	}

	// Binary search to find the block where contract was created
	// We look for the first block where the contract has code
	// sort.Search finds the smallest index i in [0, n) where f(i) is true
	// We adjust the search to start from minBlock
	searchRange := int(latestBlock - minBlock + 1) //nolint:gosec,G115 // Suppose the block range is not too large for int overflow
	var searchErr error
	relativeBlock := uint64(sort.Search(searchRange, func(i int) bool { //nolint:gosec,G115 // Casting int to uint64 is safe for block range, there is no negative block number
		blockNum := minBlock + uint64(i) //nolint:gosec,G115 // Casting int to uint64 is safe for block range, there is no negative block number
		code, err := f.client.CodeAt(ctx, addr, new(big.Int).SetUint64(blockNum))
		if err != nil {
			// Store error for later handling, but continue search
			searchErr = err
			return false
		}
		return len(code) > 0
	}))

	creationBlock := minBlock + relativeBlock

	// Check if contract was found (sort.Search returns n if not found)
	if relativeBlock >= uint64(searchRange) { //nolint:gosec,G115 // Casting int to uint64 is safe for block range, there is no negative block number
		if searchErr != nil {
			return "", fmt.Errorf("failed to find contract (encountered errors during search): %w", searchErr)
		}

		logger.WarnCtx(ctx, "Deployer not found for contract",
			zap.String("contract", contractAddress),
			zap.Uint64("minBlock", minBlock),
			zap.Uint64("latestBlock", latestBlock),
		)
		return "", ErrOriginationNotFound
	}

	// Get the block where contract was created
	block, err := f.client.BlockByNumber(ctx, new(big.Int).SetUint64(creationBlock))
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
		receipt, err := f.client.TransactionReceipt(ctx, tx.Hash())
		if err != nil {
			continue
		}

		if receipt.ContractAddress == addr {
			// Found the creation transaction
			sender, err := f.client.TransactionSender(ctx, tx, block.Hash(), receipt.TransactionIndex)
			if err != nil {
				return "", fmt.Errorf("failed to get transaction sender: %w", err)
			}
			return sender.Hex(), nil
		}
	}

	logger.WarnCtx(ctx, "Contract creation transaction not found for deployer contract",
		zap.String("contract", contractAddress),
		zap.Uint64("creationBlock", creationBlock),
		zap.String("blockNumber", block.Number().String()),
	)

	return "", ErrOriginationNotFound
}

// TokenExists checks if a token exists on the blockchain
// For ERC721: uses ownerOf and catches execution revert errors
// For ERC1155: checks recent transfers and balanceOf for multiple recipients
func (f *ethereumClient) TokenExists(ctx context.Context, contractAddress, tokenNumber string, standard domain.ChainStandard) (bool, error) {
	switch standard {
	case domain.StandardERC721:
		// For ERC721, try to call ownerOf. If it reverts, the token doesn't exist.
		_, err := f.ERC721OwnerOf(ctx, contractAddress, tokenNumber)
		if err != nil {
			// Check if it's an execution revert error (token doesn't exist)
			if strings.Contains(err.Error(), "execution reverted") ||
				strings.Contains(err.Error(), "nonexistent token") {
				return false, nil
			}
			// Other errors should be propagated
			return false, fmt.Errorf("failed to check ERC721 token existence: %w", err)
		}
		return true, nil

	case domain.StandardERC1155:
		// For ERC1155, find recent transfers in the last 10M blocks and check balanceOf for multiple recipients
		// This avoids scanning entire history which can hit Infura's 10k log limitation
		//
		// Strategy: Scan backward up to 10M blocks to find recent non-burn transfers,
		// then call balanceOf on multiple recent recipients to handle partial burns and transfers.
		// If no recent transfers found, assume token doesn't exist.
		//
		// FIXME: This approach has limitations:
		// 1. May return false negatives for very old/inactive tokens (>10M blocks)
		// 2. Only checks TransferSingle events (not TransferBatch) for performance
		// 3. Limited to checking 5 most recent recipients (trade-off between accuracy and RPC calls)
		// 4. Race condition: token could be transferred between our scan and balanceOf call (unlikely but possible)
		//
		// TODO: Potential improvements:
		// - Implement on-demand re-indexing for historical tokens if they appear in provenance events
		// - Support TransferBatch events for more complete coverage
		// - Use indexed subgraph or archive node for better ERC1155 querying
		// - Cache existence checks per token to avoid repeated scans

		timeoutCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
		defer cancel()

		tokenID, ok := new(big.Int).SetString(tokenNumber, 10)
		if !ok {
			return false, fmt.Errorf("invalid token number: %s", tokenNumber)
		}

		contractAddr := common.HexToAddress(contractAddress)
		zeroAddress := common.HexToAddress(domain.ETHEREUM_ZERO_ADDRESS)

		// Get the latest block number using cached provider
		latestBlock, err := f.GetLatestBlock(timeoutCtx)
		if err != nil {
			return false, fmt.Errorf("failed to get latest block: %w", err)
		}

		// Scan backward up to 10M blocks
		const maxBlockThreshold = uint64(10_000_000)
		var fromBlock uint64
		if latestBlock > maxBlockThreshold {
			fromBlock = latestBlock - maxBlockThreshold
		}

		logger.InfoCtx(timeoutCtx, "Checking ERC1155 token existence via recent transfers",
			zap.String("contract", contractAddress),
			zap.String("tokenNumber", tokenNumber),
			zap.Uint64("fromBlock", fromBlock),
			zap.Uint64("toBlock", latestBlock),
		)

		// Query all TransferSingle events for this contract in the block range
		query := ethereum.FilterQuery{
			FromBlock: new(big.Int).SetUint64(fromBlock),
			ToBlock:   new(big.Int).SetUint64(latestBlock),
			Addresses: []common.Address{contractAddr},
			Topics: [][]common.Hash{
				{transferSingleEventSignature}, // TransferSingle events only (most common)
			},
		}

		logs, err := f.filterLogsWithPagination(timeoutCtx, query)
		if err != nil && timeoutCtx.Err() != context.DeadlineExceeded {
			return false, fmt.Errorf("failed to fetch transfer logs: %w", err)
		}

		// Collect recent unique recipients for this token ID (in reverse chronological order)
		// We check multiple recipients to handle cases where:
		// - Last recipient transferred tokens away (ERC1155 allows multiple holders)
		// - Last recipient partially burned their balance (but others still hold tokens)
		const maxRecipientsToCheck = 5
		type recipientInfo struct {
			address     common.Address
			blockNumber uint64
			logIndex    uint
		}
		var recentRecipients []recipientInfo
		seenRecipients := make(map[common.Address]bool)

		for _, vLog := range logs {
			// TransferSingle: topics[0]=signature, topics[1]=operator, topics[2]=from, topics[3]=to
			// Data contains: id (32 bytes) and value (32 bytes)
			if len(vLog.Topics) != 4 || len(vLog.Data) < 64 {
				continue
			}

			// Extract token ID from data (first 32 bytes)
			logTokenID := new(big.Int).SetBytes(vLog.Data[0:32])
			if logTokenID.Cmp(tokenID) != 0 {
				continue // Not the token we're looking for
			}

			// Extract recipient (to address)
			toAddr := common.BytesToAddress(vLog.Topics[3].Bytes())

			// Skip burn events (to = 0x0)
			if toAddr == zeroAddress {
				continue
			}

			// Add unique recipients (avoid checking same address multiple times)
			if !seenRecipients[toAddr] {
				recentRecipients = append(recentRecipients, recipientInfo{
					address:     toAddr,
					blockNumber: vLog.BlockNumber,
					logIndex:    vLog.Index,
				})
				seenRecipients[toAddr] = true
			}
		}

		// If no recent transfers found in the scan window, assume token doesn't exist
		// (or is so old/inactive that it's not worth indexing)
		if len(recentRecipients) == 0 {
			logger.InfoCtx(timeoutCtx, "No recent ERC1155 transfers found in scan window, assuming token doesn't exist",
				zap.String("contract", contractAddress),
				zap.String("tokenNumber", tokenNumber),
				zap.Uint64("scannedBlocks", latestBlock-fromBlock),
			)
			return false, nil
		}

		// Sort recipients by block number + log index (most recent first)
		// This ensures we check the most likely current holders first
		for i := 0; i < len(recentRecipients)-1; i++ {
			for j := i + 1; j < len(recentRecipients); j++ {
				if recentRecipients[j].blockNumber > recentRecipients[i].blockNumber ||
					(recentRecipients[j].blockNumber == recentRecipients[i].blockNumber &&
						recentRecipients[j].logIndex > recentRecipients[i].logIndex) {
					recentRecipients[i], recentRecipients[j] = recentRecipients[j], recentRecipients[i]
				}
			}
		}

		// Limit to top N most recent recipients to avoid too many RPC calls
		if len(recentRecipients) > maxRecipientsToCheck {
			recentRecipients = recentRecipients[:maxRecipientsToCheck]
		}

		logger.InfoCtx(timeoutCtx, "Found recent ERC1155 transfers, checking recipient balances",
			zap.String("contract", contractAddress),
			zap.String("tokenNumber", tokenNumber),
			zap.Int("recipientsToCheck", len(recentRecipients)),
		)

		// Check balanceOf for each recipient until we find one with balance > 0
		// This handles partial burns and transfers between holders
		for i, recipient := range recentRecipients {
			balanceStr, err := f.ERC1155BalanceOf(timeoutCtx, contractAddress, recipient.address.Hex(), tokenNumber)
			if err != nil {
				logger.WarnCtx(timeoutCtx, "Failed to check balance for recipient, trying next",
					zap.String("recipient", recipient.address.Hex()),
					zap.Int("recipientIndex", i),
					zap.Error(err),
				)
				continue
			}

			// Parse balance string to big.Int
			balance, ok := new(big.Int).SetString(balanceStr, 10)
			if !ok {
				logger.WarnCtx(timeoutCtx, "Invalid balance returned, trying next recipient",
					zap.String("recipient", recipient.address.Hex()),
					zap.String("balance", balanceStr),
				)
				continue
			}

			// If this recipient has a balance > 0, token exists!
			if balance.Cmp(big.NewInt(0)) > 0 {
				logger.InfoCtx(timeoutCtx, "ERC1155 token existence confirmed",
					zap.String("contract", contractAddress),
					zap.String("tokenNumber", tokenNumber),
					zap.String("holder", recipient.address.Hex()),
					zap.String("balance", balanceStr),
				)
				return true, nil
			}
		}

		// All checked recipients have 0 balance - token likely doesn't exist or fully burned
		logger.InfoCtx(timeoutCtx, "All recent recipients have zero balance, assuming token doesn't exist or fully burned",
			zap.String("contract", contractAddress),
			zap.String("tokenNumber", tokenNumber),
			zap.Int("recipientsChecked", len(recentRecipients)),
		)
		return false, nil

	default:
		return false, fmt.Errorf("unsupported standard: %s", standard)
	}
}

// Close closes the connection
func (f *ethereumClient) Close() {
	f.client.Close()
}
