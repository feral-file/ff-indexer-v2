package ethereum

import (
	"context"
	"errors"
	"fmt"
	"math/big"
	"sort"
	"strings"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"go.uber.org/zap"

	"github.com/feral-file/ff-indexer-v2/internal/adapter"
	"github.com/feral-file/ff-indexer-v2/internal/block"
	"github.com/feral-file/ff-indexer-v2/internal/domain"
	"github.com/feral-file/ff-indexer-v2/internal/logger"
	"github.com/feral-file/ff-indexer-v2/internal/providers/ethereum/adapters"
	"github.com/feral-file/ff-indexer-v2/internal/providers/ethereum/contracts"
	"github.com/feral-file/ff-indexer-v2/internal/providers/ethereum/helpers"
	contractregistry "github.com/feral-file/ff-indexer-v2/internal/providers/ethereum/registry"
	"github.com/feral-file/ff-indexer-v2/internal/registry"
)

// ErrContractNotFound is re-exported from helpers so external callers share one sentinel.
var ErrContractNotFound = helpers.ErrContractNotFound

// ErrOriginationNotFound is returned when an origination is not found for a contract.
var ErrOriginationNotFound = errors.New("origination not found")

// EthereumClient is the public gateway for Ethereum provider operations.
//
// Callers interact only with this interface. Internally, methods fall into a few roles:
//   - Infrastructure: thin RPC/block wrappers used by ingestion and orchestration code.
//   - Adapter-routed: contract/standard-aware operations delegated to registry → adapters.
//   - Cross-standard orchestration: multi-standard log scans that stay on the client today.
//   - Contract lifecycle & registry introspection: deployer lookup and adapter registry access.
//
//go:generate mockgen -source=client.go -destination=../../mocks/ethereum_provider_client.go -package=mocks -mock_names=EthereumClient=MockEthereumProviderClient
type EthereumClient interface {
	// SubscribeFilterLogs subscribes to filter logs
	SubscribeFilterLogs(ctx context.Context, query ethereum.FilterQuery, ch chan<- types.Log) (ethereum.Subscription, error)

	// GetLatestBlock returns the latest block number
	GetLatestBlock(ctx context.Context) (uint64, error)

	// TokenBalances fetches all holder balances via the contract adapter registry.
	//
	// Best-effort owner-discovery path; for accurate full-provenance balances use
	// TokenBalancesForAddresses instead.
	TokenBalances(ctx context.Context, contractAddress, tokenNumber string, standard domain.ChainStandard) (map[string]string, error)

	// TokenBalancesForAddresses fetches balances for specific addresses via the adapter registry.
	//
	// ERC1155 uses balanceOfBatch. ERC721 checks whether a requested address is the
	// current owner. Configured contracts use configured owner lookup or event replay.
	//
	// Intended for full provenance indexing where accuracy matters.
	// Returns map[ownerAddress]balance, excluding zero balances.
	TokenBalancesForAddresses(
		ctx context.Context,
		contractAddress, tokenNumber string,
		standard domain.ChainStandard,
		addresses []string,
	) (map[string]string, error)

	// OwnerBalanceAndEvents fetches owner-specific balance and events via the adapter registry.
	OwnerBalanceAndEvents(
		ctx context.Context,
		contractAddress, tokenNumber, ownerAddress string,
		standard domain.ChainStandard,
	) (balance string, events []domain.BlockchainEvent, err error)

	// OwnershipModel reports single-owner vs multi-holder semantics for a contract.
	OwnershipModel(contractAddress string, standard domain.ChainStandard) (adapters.OwnershipModel, error)

	// GetTokenEvents fetches historical events for a specific token via the adapter registry.
	// Returns events in ascending block/log order.
	GetTokenEvents(ctx context.Context, contractAddress, tokenNumber string, standard domain.ChainStandard) ([]domain.BlockchainEvent, error)

	// ParseEventLog parses an Ethereum log into a standardized blockchain event.
	//
	// Returns (nil, nil) for intentionally skipped logs.
	ParseEventLog(ctx context.Context, vLog types.Log) (*domain.BlockchainEvent, error)

	// TokenExists checks whether a token exists via the contract adapter registry.
	TokenExists(ctx context.Context, contractAddress, tokenNumber string, standard domain.ChainStandard) (bool, error)

	// TokenOwner resolves the token owner via the contract adapter registry.
	TokenOwner(ctx context.Context, contractAddress, tokenNumber string, standard domain.ChainStandard) (string, error)

	// TokenURI resolves on-chain metadata URI via the contract adapter registry.
	TokenURI(ctx context.Context, contractAddress, tokenNumber string, standard domain.ChainStandard) (string, error)

	// IsVendorOnlyMetadata reports whether on-chain metadata fetch should be skipped for a contract.
	IsVendorOnlyMetadata(contractAddress string) bool

	// SupportsProvenance reports whether full on-chain provenance indexing is supported for a contract.
	//
	// Returns false when adapter lookup fails.
	SupportsProvenance(contractAddress string, standard domain.ChainStandard) bool

	// GetTokenCIDsByOwnerAndBlockRange retrieves token CIDs with block numbers for an owner within a block range.
	// It queries standard ERC721/ERC1155 adapters and configured generic contract adapters, merges logs,
	// and replays ownership with a single global held-token limit. Returns effectiveFromBlock/effectiveToBlock
	// for the range actually covered after limiting.
	GetTokenCIDsByOwnerAndBlockRange(
		ctx context.Context,
		ownerAddress string,
		requestedFromBlock uint64,
		requestedToBlock uint64,
		limit int,
		order domain.BlockScanOrder,
		blacklist registry.BlacklistRegistry,
	) (domain.TokenWithBlockRangeResult, error)

	// GetContractDeployer retrieves the deployer address for a contract
	// minBlock specifies the earliest block to search (0 = search from genesis)
	GetContractDeployer(ctx context.Context, contractAddress string, minBlock uint64) (string, error)

	// ContractAdapterRegistry returns the loaded contract adapter registry.
	ContractAdapterRegistry() *contractregistry.AdapterRegistry

	// Close closes the connection
	Close()
}

// ethereumClient implements EthereumClient. It wires RPC, pagination, block metadata,
// and the in-process adapter registry; it does not embed standard- or contract-specific logic.
type ethereumClient struct {
	chainID         domain.Chain
	client          adapter.EthClient
	clock           adapter.Clock
	blockProvider   block.BlockProvider
	pagination      *helpers.PaginationHelper
	adapterRegistry *contractregistry.AdapterRegistry
}

// NewClient constructs the Ethereum gateway: RPC client, pagination helper, and adapter registry.
//
// Returns an error if the contract adapter registry cannot be initialized (config validation failure,
// missing ABI files, etc.). Callers must handle this error to prevent silent startup failures.
func NewClient(chainID domain.Chain, client adapter.EthClient, clock adapter.Clock, blockProvider block.BlockProvider) (EthereumClient, error) {
	ec := &ethereumClient{
		chainID:       chainID,
		client:        client,
		clock:         clock,
		blockProvider: blockProvider,
	}
	ec.pagination = helpers.NewPaginationHelper(client, clock, blockProvider)

	registry, err := contractregistry.NewAdapterRegistry(
		contracts.Files,
		client,
		blockProvider,
		ec.pagination,
		chainID,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize contract adapter registry: %w", err)
	}
	ec.adapterRegistry = registry

	return ec, nil
}

// SubscribeFilterLogs subscribes to filter logs
func (f *ethereumClient) SubscribeFilterLogs(ctx context.Context, query ethereum.FilterQuery, ch chan<- types.Log) (ethereum.Subscription, error) {
	return f.client.SubscribeFilterLogs(ctx, query, ch)
}

// GetLatestBlock returns the latest block number using the cached provider
func (f *ethereumClient) GetLatestBlock(ctx context.Context) (uint64, error) {
	return f.blockProvider.GetLatestBlock(ctx)
}

// GetTokenEvents fetches all historical events for a specific token by routing to the appropriate adapter.
// Returns events in ascending order of timestamp.
func (f *ethereumClient) GetTokenEvents(ctx context.Context, contractAddress, tokenNumber string, standard domain.ChainStandard) ([]domain.BlockchainEvent, error) {
	adp, err := f.adapterRegistry.GetAdapter(f.chainID, contractAddress, standard)
	if err != nil {
		return nil, err
	}
	return adp.GetTokenEvents(ctx, contractAddress, tokenNumber)
}

// ParseEventLog parses an Ethereum log into a standardized blockchain event.
func (f *ethereumClient) ParseEventLog(ctx context.Context, vLog types.Log) (*domain.BlockchainEvent, error) {
	if len(vLog.Topics) == 0 {
		return nil, fmt.Errorf("event log has no topics")
	}

	return f.adapterRegistry.ParseEvent(ctx, vLog, f.chainID)
}

// TokenExists checks if a token exists on the blockchain via the contract adapter registry.
func (f *ethereumClient) TokenExists(ctx context.Context, contractAddress, tokenNumber string, standard domain.ChainStandard) (bool, error) {
	adp, err := f.adapterRegistry.GetAdapter(f.chainID, contractAddress, standard)
	if err != nil {
		return false, err
	}
	logger.DebugCtx(ctx, "Routing token existence check through contract adapter",
		zap.String("chain", string(f.chainID)),
		zap.String("contract", contractAddress),
		zap.String("standard", string(standard)),
		zap.String("adapter_type", fmt.Sprintf("%T", adp)),
	)

	return adp.TokenExists(ctx, contractAddress, tokenNumber)
}

// TokenOwner resolves the token owner via the contract adapter registry.
func (f *ethereumClient) TokenOwner(ctx context.Context, contractAddress, tokenNumber string, standard domain.ChainStandard) (string, error) {
	adp, err := f.adapterRegistry.GetAdapter(f.chainID, contractAddress, standard)
	if err != nil {
		return "", err
	}
	logger.DebugCtx(ctx, "Routing token owner lookup through contract adapter",
		zap.String("chain", string(f.chainID)),
		zap.String("contract", contractAddress),
		zap.String("standard", string(standard)),
		zap.String("adapter_type", fmt.Sprintf("%T", adp)),
	)

	return adp.TokenOwner(ctx, contractAddress, tokenNumber)
}

// TokenURI resolves on-chain metadata URI via the contract adapter registry.
func (f *ethereumClient) TokenURI(ctx context.Context, contractAddress, tokenNumber string, standard domain.ChainStandard) (string, error) {
	adp, err := f.adapterRegistry.GetAdapter(f.chainID, contractAddress, standard)
	if err != nil {
		return "", err
	}
	logger.DebugCtx(ctx, "Routing token URI lookup through contract adapter",
		zap.String("chain", string(f.chainID)),
		zap.String("contract", contractAddress),
		zap.String("standard", string(standard)),
		zap.String("adapter_type", fmt.Sprintf("%T", adp)),
	)

	return adp.TokenURI(ctx, contractAddress, tokenNumber)
}

// IsVendorOnlyMetadata reports whether on-chain metadata fetch should be skipped for a contract.
func (f *ethereumClient) IsVendorOnlyMetadata(contractAddress string) bool {
	return f.adapterRegistry.IsVendorOnlyMetadata(f.chainID, contractAddress)
}

// SupportsProvenance reports whether full on-chain provenance indexing is supported for a contract.
func (f *ethereumClient) SupportsProvenance(contractAddress string, standard domain.ChainStandard) bool {
	supported, err := f.adapterRegistry.SupportsProvenance(f.chainID, contractAddress, standard)
	if err != nil {
		return false
	}
	return supported
}

// OwnershipModel reports single-owner vs multi-holder semantics via the adapter registry.
func (f *ethereumClient) OwnershipModel(contractAddress string, standard domain.ChainStandard) (adapters.OwnershipModel, error) {
	adp, err := f.adapterRegistry.GetAdapter(f.chainID, contractAddress, standard)
	if err != nil {
		return "", err
	}
	return adp.OwnershipModel(), nil
}

// TokenBalances fetches all holder balances via the contract adapter registry.
func (f *ethereumClient) TokenBalances(
	ctx context.Context,
	contractAddress, tokenNumber string,
	standard domain.ChainStandard,
) (map[string]string, error) {
	adp, err := f.adapterRegistry.GetAdapter(f.chainID, contractAddress, standard)
	if err != nil {
		return nil, err
	}
	return adp.GetTokenBalances(ctx, contractAddress, tokenNumber)
}

// TokenBalancesForAddresses routes address-scoped balance queries through the adapter registry.
func (f *ethereumClient) TokenBalancesForAddresses(
	ctx context.Context,
	contractAddress, tokenNumber string,
	standard domain.ChainStandard,
	addresses []string,
) (map[string]string, error) {
	adp, err := f.adapterRegistry.GetAdapter(f.chainID, contractAddress, standard)
	if err != nil {
		return nil, err
	}
	return adp.GetTokenBalancesForAddresses(ctx, contractAddress, tokenNumber, addresses)
}

// OwnerBalanceAndEvents fetches owner-specific balance and events via the adapter registry.
func (f *ethereumClient) OwnerBalanceAndEvents(
	ctx context.Context,
	contractAddress, tokenNumber, ownerAddress string,
	standard domain.ChainStandard,
) (string, []domain.BlockchainEvent, error) {
	adp, err := f.adapterRegistry.GetAdapter(f.chainID, contractAddress, standard)
	if err != nil {
		return "", nil, err
	}
	return adp.GetOwnerBalanceAndEvents(ctx, contractAddress, tokenNumber, ownerAddress)
}

// GetTokenCIDsByOwnerAndBlockRange retrieves token CIDs with block numbers for an owner within a block range.
//
// Reason: Unified client-side replay preserves global heldCount semantics across ERC-721, ERC-1155,
// and configured legacy contracts. Adapters only fetch owner-scoped logs.
//
// Trade-offs:
//   - All adapter logs for the requested range are fetched before early-stop can trim replay work
//   - For large ranges (>100k blocks) with small limits, consider chunking at the workflow layer
//
// Constraints:
//   - Limit must be > 0 (validated on entry)
//   - Limit stops replay at a block boundary once heldCount reaches limit
//   - Blacklist filtering happens during replay, not after aggregation
//   - EffectiveFromBlock/EffectiveToBlock report the replay range after applying the limit
func (f *ethereumClient) GetTokenCIDsByOwnerAndBlockRange(
	ctx context.Context,
	ownerAddress string,
	requestedFromBlock uint64,
	requestedToBlock uint64,
	limit int,
	order domain.BlockScanOrder,
	blacklist registry.BlacklistRegistry,
) (domain.TokenWithBlockRangeResult, error) {
	if limit <= 0 {
		return domain.TokenWithBlockRangeResult{}, fmt.Errorf("limit must be > 0")
	}

	owner := common.HexToAddress(ownerAddress)
	configuredAdapters := f.adapterRegistry.GetProvenanceContractsForChain(f.chainID)
	adaptersToQuery := make([]adapters.ContractAdapter, 0, 2+len(configuredAdapters))

	if erc721Adapter, ok := f.adapterRegistry.GetStandardAdapter(domain.StandardERC721); ok {
		adaptersToQuery = append(adaptersToQuery, erc721Adapter)
	}
	if erc1155Adapter, ok := f.adapterRegistry.GetStandardAdapter(domain.StandardERC1155); ok {
		adaptersToQuery = append(adaptersToQuery, erc1155Adapter)
	}
	for _, adapter := range configuredAdapters {
		adaptersToQuery = append(adaptersToQuery, adapter)
	}

	type adapterResult struct {
		logs []types.Log
		err  error
	}

	resultsCh := make(chan adapterResult, len(adaptersToQuery))
	for _, adp := range adaptersToQuery {
		go func(adapter adapters.ContractAdapter) {
			logs, err := adapter.GetOwnerLogs(ctx, ownerAddress, requestedFromBlock, requestedToBlock)
			resultsCh <- adapterResult{logs: logs, err: err}
		}(adp)
	}

	var allLogs []types.Log
	for range adaptersToQuery {
		result := <-resultsCh
		if result.err != nil {
			return domain.TokenWithBlockRangeResult{}, fmt.Errorf("adapter owner log query failed: %w", result.err)
		}
		allLogs = append(allLogs, result.logs...)
	}

	allLogs = deduplicateOwnerLogs(allLogs)

	configuredStandards := make(map[string]domain.ChainStandard, len(configuredAdapters))
	for contractAddr := range configuredAdapters {
		if standard, ok := f.adapterRegistry.GetContractCIDStandard(f.chainID, contractAddr); ok {
			configuredStandards[strings.ToLower(common.HexToAddress(contractAddr).Hex())] = standard
		}
	}

	replayResult, err := adapters.ReplayOwnerTokensWithLimit(ctx, adapters.OwnerReplayParams{
		ChainID:                     f.chainID,
		Owner:                       owner,
		Logs:                        allLogs,
		Blacklist:                   blacklist,
		Limit:                       limit,
		Order:                       order,
		RequestedFromBlock:          requestedFromBlock,
		RequestedToBlock:            requestedToBlock,
		ConfiguredContractStandards: configuredStandards,
		ParseLog: func(ctx context.Context, vLog types.Log) (*domain.BlockchainEvent, error) {
			return f.adapterRegistry.ParseEvent(ctx, vLog, f.chainID)
		},
	})
	if err != nil {
		return domain.TokenWithBlockRangeResult{}, fmt.Errorf("owner ownership replay failed: %w", err)
	}

	sortTokensByBlockOrder(replayResult.Tokens, order)

	return domain.TokenWithBlockRangeResult{
		Tokens:             replayResult.Tokens,
		EffectiveFromBlock: replayResult.EffectiveFromBlock,
		EffectiveToBlock:   replayResult.EffectiveToBlock,
	}, nil
}

func deduplicateOwnerLogs(logs []types.Log) []types.Log {
	logMap := make(map[string]types.Log, len(logs))
	for _, vLog := range logs {
		key := fmt.Sprintf("%d-%s-%d", vLog.BlockNumber, vLog.TxHash.Hex(), vLog.Index)
		logMap[key] = vLog
	}

	result := make([]types.Log, 0, len(logMap))
	for _, vLog := range logMap {
		result = append(result, vLog)
	}
	return result
}

// sortTokensByBlockOrder sorts tokens by block number (respecting scan order), then by TokenCID.
// TokenCID tiebreaker ensures deterministic ordering when multiple tokens are in the same block.
func sortTokensByBlockOrder(tokens []domain.TokenWithBlock, order domain.BlockScanOrder) {
	if order.Desc() {
		sort.Slice(tokens, func(i, j int) bool {
			if tokens[i].BlockNumber != tokens[j].BlockNumber {
				return tokens[i].BlockNumber > tokens[j].BlockNumber
			}
			return tokens[i].TokenCID < tokens[j].TokenCID
		})
		return
	}

	sort.Slice(tokens, func(i, j int) bool {
		if tokens[i].BlockNumber != tokens[j].BlockNumber {
			return tokens[i].BlockNumber < tokens[j].BlockNumber
		}
		return tokens[i].TokenCID < tokens[j].TokenCID
	})
}

// applyOwnerTokenLimit applies the global limit at block boundaries and adjusts the effective range.
//
// When the limit is reached mid-block, includes all tokens from that block to preserve atomicity.
// This ensures the effective range always ends on a complete block boundary, not mid-block.
//
// For descending order, adjusts effectiveFromBlock to the cutoff block (earliest block kept).
// For ascending order, adjusts effectiveToBlock to the cutoff block (latest block kept).
//
// Returns the limited token slice and the adjusted effective range bounds.
func applyOwnerTokenLimit(
	tokens []domain.TokenWithBlock,
	limit int,
	order domain.BlockScanOrder,
	effectiveFromBlock uint64,
	effectiveToBlock uint64,
) ([]domain.TokenWithBlock, uint64, uint64) {
	if len(tokens) <= limit {
		return tokens, effectiveFromBlock, effectiveToBlock
	}

	tokenCount := 0
	var lastBlock uint64
	cutoffIndex := len(tokens)

	for i, token := range tokens {
		if i > 0 && token.BlockNumber != lastBlock && tokenCount >= limit {
			cutoffIndex = i
			break
		}
		lastBlock = token.BlockNumber
		tokenCount++
	}

	if cutoffIndex < len(tokens) {
		cutoffBlock := tokens[cutoffIndex-1].BlockNumber
		if order.Desc() {
			effectiveFromBlock = cutoffBlock
		} else {
			effectiveToBlock = cutoffBlock
		}
	}

	return tokens[:cutoffIndex], effectiveFromBlock, effectiveToBlock
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

// ContractAdapterRegistry returns the loaded contract adapter registry.
func (f *ethereumClient) ContractAdapterRegistry() *contractregistry.AdapterRegistry {
	return f.adapterRegistry
}

// Close closes the connection
func (f *ethereumClient) Close() {
	f.client.Close()
}
