package ethereum

import (
	"context"
	"errors"
	"fmt"
	"math/big"
	"sort"
	"time"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"go.uber.org/zap"

	"github.com/feral-file/ff-indexer-v2/internal/adapter"
	"github.com/feral-file/ff-indexer-v2/internal/block"
	"github.com/feral-file/ff-indexer-v2/internal/domain"
	"github.com/feral-file/ff-indexer-v2/internal/logger"
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
//   - Low-level ERC-1155: standard balance helpers (no adapter routing; kept for existing callers).
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

	// ERC1155BalanceOf fetches the balance of a specific token ID for an owner from an ERC1155 contract
	ERC1155BalanceOf(ctx context.Context, contractAddress, ownerAddress, tokenNumber string) (string, error)

	// ERC1155BalanceOfBatch fetches balances for multiple addresses for a specific token ID from an ERC1155 contract
	// Handles chunking internally to respect RPC limits (default 200 addresses per chunk)
	// Returns a map of address -> balance string
	ERC1155BalanceOfBatch(ctx context.Context, contractAddress, tokenNumber string, addresses []string) (map[string]string, error)

	// ERC1155Balances calculates all current ERC1155 token balances by replaying transfer events
	ERC1155Balances(ctx context.Context, contractAddress, tokenNumber string) (map[string]string, error)

	// GetERC1155BalanceAndEventsForOwner fetches the current balance and transfer events for a specific ERC1155 token and owner
	// This is optimized for owner-specific indexing by filtering events where owner is sender or receiver
	// Supports both TransferSingle and TransferBatch events
	GetERC1155BalanceAndEventsForOwner(ctx context.Context, contractAddress, tokenNumber, ownerAddress string) (balance string, events []domain.BlockchainEvent, err error)

	// GetTokenEvents fetches all historical events for a specific token
	// Returns events in ascending order of timestamp
	GetTokenEvents(ctx context.Context, contractAddress, tokenNumber string, standard domain.ChainStandard) ([]domain.BlockchainEvent, error)

	// ParseEventLog parses an Ethereum log into a standardized blockchain event
	ParseEventLog(ctx context.Context, vLog types.Log) (*domain.BlockchainEvent, error)

	// TokenExists checks if a token exists on the blockchain
	// For ERC721: uses ownerOf and catches execution revert errors
	// For ERC1155: checks mint and burn events in logs
	TokenExists(ctx context.Context, contractAddress, tokenNumber string, standard domain.ChainStandard) (bool, error)

	// TokenOwner resolves the token owner via the contract adapter registry.
	TokenOwner(ctx context.Context, contractAddress, tokenNumber string, standard domain.ChainStandard) (string, error)

	// TokenURI resolves on-chain metadata URI via the contract adapter registry.
	TokenURI(ctx context.Context, contractAddress, tokenNumber string, standard domain.ChainStandard) (string, error)

	// IsVendorOnlyMetadata reports whether on-chain metadata fetch should be skipped for a contract.
	IsVendorOnlyMetadata(contractAddress string) bool

	// SupportsProvenance reports whether full on-chain provenance indexing is supported for a contract.
	SupportsProvenance(contractAddress string, standard domain.ChainStandard) bool

	// GetTokenCIDsByOwnerAndBlockRange retrieves token CIDs with block numbers for an owner within a block range.
	// It queries both ERC721 and ERC1155 transfer events where the address is either sender or receiver.
	// limit is always applied; pass a very large value if you want "no cap". order controls scan direction for limit selection.
	// Returns effectiveFromBlock/effectiveToBlock for the range actually used after limiting.
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
func NewClient(chainID domain.Chain, client adapter.EthClient, clock adapter.Clock, blockProvider block.BlockProvider) EthereumClient {
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
		clock,
		blockProvider,
		ec.pagination,
		chainID,
	)
	if err != nil {
		panic(fmt.Sprintf("failed to initialize contract adapter registry: %v", err))
	}
	ec.adapterRegistry = registry

	return ec
}

// SubscribeFilterLogs subscribes to filter logs
func (f *ethereumClient) SubscribeFilterLogs(ctx context.Context, query ethereum.FilterQuery, ch chan<- types.Log) (ethereum.Subscription, error) {
	return f.client.SubscribeFilterLogs(ctx, query, ch)
}

func (f *ethereumClient) filterLogsWithPagination(ctx context.Context, query ethereum.FilterQuery) ([]types.Log, error) {
	return f.pagination.FilterLogsWithPagination(ctx, query)
}

// GetLatestBlock returns the latest block number using the cached provider
func (f *ethereumClient) GetLatestBlock(ctx context.Context) (uint64, error) {
	return f.blockProvider.GetLatestBlock(ctx)
}

// ERC1155BalanceOf fetches the balance of a specific token ID for an owner from an ERC1155 contract
func (f *ethereumClient) ERC1155BalanceOf(ctx context.Context, contractAddress, ownerAddress, tokenNumber string) (string, error) {
	return helpers.ERC1155BalanceOf(ctx, f.client, contractAddress, ownerAddress, tokenNumber)
}

// ERC1155BalanceOfBatch fetches balances for multiple addresses for a specific token ID from an ERC1155 contract
// It uses the ERC1155 balanceOfBatch function which allows querying multiple address-token pairs in a single RPC call
// Handles chunking internally to respect RPC limits (200 address-token pairs per chunk by default)
// Returns a map of address -> balance string (only includes addresses with non-zero balances)
func (f *ethereumClient) ERC1155BalanceOfBatch(ctx context.Context, contractAddress, tokenNumber string, addresses []string) (map[string]string, error) {
	return helpers.ERC1155BalanceOfBatch(ctx, f.client, contractAddress, tokenNumber, addresses)
}

// ERC1155Balances calculates all current ERC1155 token balances by replaying transfer events.
func (f *ethereumClient) ERC1155Balances(ctx context.Context, contractAddress, tokenNumber string) (map[string]string, error) {
	return helpers.ERC1155ReplayBalances(ctx, f.pagination, f.blockProvider, contractAddress, tokenNumber)
}

// GetERC1155BalanceAndEventsForOwner fetches the current balance and transfer events for a specific ERC1155 token and owner.
// This is optimized for owner-specific indexing by filtering events where owner is sender or receiver.
// Supports both TransferSingle and TransferBatch events.
func (f *ethereumClient) GetERC1155BalanceAndEventsForOwner(ctx context.Context, contractAddress, tokenNumber, ownerAddress string) (string, []domain.BlockchainEvent, error) {
	return helpers.ERC1155BalanceAndEventsForOwner(
		ctx,
		f.client,
		f.pagination,
		f.blockProvider,
		f.chainID,
		f.clock.Now,
		contractAddress,
		tokenNumber,
		ownerAddress,
	)
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

	blockHash := vLog.BlockHash.Hex()
	event := &domain.BlockchainEvent{
		Chain:           f.chainID,
		ContractAddress: vLog.Address.Hex(),
		TxHash:          vLog.TxHash.Hex(),
		BlockNumber:     vLog.BlockNumber,
		BlockHash:       &blockHash,
		TxIndex:         uint64(vLog.TxIndex),                     //nolint:gosec,G115
		LogIndex:        uint64(vLog.Index),                       //nolint:gosec,G115
		Timestamp:       time.Unix(int64(vLog.BlockTimestamp), 0), //nolint:gosec,G115
	}

	return f.adapterRegistry.ParseEvent(ctx, f.chainID, vLog, event)
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

// GetTokenCIDsByOwnerAndBlockRange retrieves token CIDs with block numbers for an owner within a block range.
// It queries both ERC721 and ERC1155 transfer events where the address is either sender or receiver.
// limit is respected strictly and must be > 0 (caller should pass a very large value to mimic no limit).
// If limit is reached during a block, the scan finishes that block and stops afterward.
// The result reflects only the scanned portion of the requested range.
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
	ownerHash := common.BytesToHash(owner.Bytes())

	// Define all query configurations
	// We need to query both ERC721 and ERC1155 transfers where the address is either sender or receiver
	queries := []ethereum.FilterQuery{
		// ERC721 Transfer where address is `from` (topic[1])
		{
			FromBlock: new(big.Int).SetUint64(requestedFromBlock),
			ToBlock:   new(big.Int).SetUint64(requestedToBlock),
			Topics: [][]common.Hash{
				{helpers.TransferEventSignature}, // Transfer event
				{ownerHash},                      // from address
			},
		},
		// ERC721 Transfer where address is `to` (topic[2])
		{
			FromBlock: new(big.Int).SetUint64(requestedFromBlock),
			ToBlock:   new(big.Int).SetUint64(requestedToBlock),
			Topics: [][]common.Hash{
				{helpers.TransferEventSignature}, // Transfer event
				nil,                              // any from address
				{ownerHash},                      // to address
			},
		},
		// ERC1155 TransferSingle where address is `from` (topic[2])
		{
			FromBlock: new(big.Int).SetUint64(requestedFromBlock),
			ToBlock:   new(big.Int).SetUint64(requestedToBlock),
			Topics: [][]common.Hash{
				{helpers.ERC1155TransferSingleEventSignature}, // TransferSingle event
				nil,         // any operator
				{ownerHash}, // from address
			},
		},
		// ERC1155 TransferSingle where address is `to` (topic[3])
		{
			FromBlock: new(big.Int).SetUint64(requestedFromBlock),
			ToBlock:   new(big.Int).SetUint64(requestedToBlock),
			Topics: [][]common.Hash{
				{helpers.ERC1155TransferSingleEventSignature}, // TransferSingle event
				nil,         // any operator
				nil,         // any from address
				{ownerHash}, // to address
			},
		},
		// ERC1155 TransferBatch where address is `from` (topic[2])
		{
			FromBlock: new(big.Int).SetUint64(requestedFromBlock),
			ToBlock:   new(big.Int).SetUint64(requestedToBlock),
			Topics: [][]common.Hash{
				{helpers.ERC1155TransferBatchEventSignature}, // TransferBatch event
				nil,         // any operator
				{ownerHash}, // from address
			},
		},
		// ERC1155 TransferBatch where address is `to` (topic[3])
		{
			FromBlock: new(big.Int).SetUint64(requestedFromBlock),
			ToBlock:   new(big.Int).SetUint64(requestedToBlock),
			Topics: [][]common.Hash{
				{helpers.ERC1155TransferBatchEventSignature}, // TransferBatch event
				nil,         // any operator
				nil,         // any from address
				{ownerHash}, // to address
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
			return domain.TokenWithBlockRangeResult{}, fmt.Errorf("failed to query logs: %w", result.err)
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

	// Sort logs by block number and log index based on requested order.
	// This ensures deterministic processing order for limit selection.
	if order.Desc() {
		sort.Slice(allLogs, func(i, j int) bool {
			if allLogs[i].BlockNumber != allLogs[j].BlockNumber {
				return allLogs[i].BlockNumber > allLogs[j].BlockNumber
			}
			return allLogs[i].Index > allLogs[j].Index
		})
	} else {
		sort.Slice(allLogs, func(i, j int) bool {
			if allLogs[i].BlockNumber != allLogs[j].BlockNumber {
				return allLogs[i].BlockNumber < allLogs[j].BlockNumber
			}
			return allLogs[i].Index < allLogs[j].Index
		})
	}

	// Track balance changes per token to determine ownership at the end of the processed range.
	// For ERC721: map stores the last transfer log (to determine final owner).
	// For ERC1155: map stores net balance change (incoming - outgoing).
	type tokenBalance struct {
		standard    domain.ChainStandard
		lastLog     *types.Log // For ERC721: last transfer log
		netBalance  *big.Int   // For ERC1155: net balance change
		blockNumber uint64     // Block number of last update
		logIndex    uint       // Log index of last update
		owned       bool       // True if owner holds token at end of processed range
	}

	updateHeldCount := func(prevOwned, newOwned bool, heldCount *int) {
		if prevOwned == newOwned {
			return
		}
		if newOwned {
			*heldCount = *heldCount + 1
		} else {
			*heldCount = *heldCount - 1
		}
	}

	isNewer := func(existing *tokenBalance, vLog types.Log) bool {
		return existing == nil ||
			vLog.BlockNumber > existing.blockNumber ||
			(vLog.BlockNumber == existing.blockNumber && vLog.Index > existing.logIndex)
	}

	tokensFromBalance := func(balanceMap map[domain.TokenCID]*tokenBalance) []domain.TokenWithBlock {
		tokensWithBlocks := make([]domain.TokenWithBlock, 0, len(balanceMap))
		for tokenCID, balance := range balanceMap {
			if balance.owned {
				tokensWithBlocks = append(tokensWithBlocks, domain.TokenWithBlock{
					TokenCID:    tokenCID,
					BlockNumber: balance.blockNumber,
				})
			}
		}
		return tokensWithBlocks
	}

	balanceMap := make(map[domain.TokenCID]*tokenBalance)
	heldCount := 0
	effectiveFromBlock := requestedFromBlock
	effectiveToBlock := requestedToBlock

	effectiveBoundary := &effectiveToBlock
	if order.Desc() {
		effectiveBoundary = &effectiveFromBlock
	}

	for logIndex := 0; logIndex < len(allLogs); {
		blockNumber := allLogs[logIndex].BlockNumber

		for logIndex < len(allLogs) && allLogs[logIndex].BlockNumber == blockNumber {
			vLog := allLogs[logIndex]
			logIndex++

			if len(vLog.Topics) < 1 {
				continue
			}

			switch vLog.Topics[0] {
			case helpers.TransferEventSignature:
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

				// Skip if token is blacklisted
				if blacklist != nil && blacklist.IsTokenCIDBlacklisted(tokenCID) {
					continue
				}

				existing := balanceMap[tokenCID]
				if isNewer(existing, vLog) {
					prevOwned := false
					if existing != nil {
						prevOwned = existing.owned
					}
					logCopy := vLog
					newOwned := toAddr == owner

					if existing == nil {
						balanceMap[tokenCID] = &tokenBalance{
							standard:    domain.StandardERC721,
							lastLog:     &logCopy,
							blockNumber: vLog.BlockNumber,
							logIndex:    vLog.Index,
							owned:       newOwned,
						}
					} else {
						existing.lastLog = &logCopy
						existing.blockNumber = vLog.BlockNumber
						existing.logIndex = vLog.Index
						existing.owned = newOwned
					}

					updateHeldCount(prevOwned, newOwned, &heldCount)
				}

			case helpers.ERC1155TransferSingleEventSignature:
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

				// Skip if token is blacklisted
				if blacklist != nil && blacklist.IsTokenCIDBlacklisted(tokenCID) {
					continue
				}

				existing := balanceMap[tokenCID]
				if existing == nil {
					existing = &tokenBalance{
						standard:   domain.StandardERC1155,
						netBalance: big.NewInt(0),
					}
					balanceMap[tokenCID] = existing
				}

				prevOwned := existing.netBalance.Sign() > 0
				if toAddr == owner {
					existing.netBalance.Add(existing.netBalance, amount)
				}
				if fromAddr == owner {
					existing.netBalance.Sub(existing.netBalance, amount)
				}
				newOwned := existing.netBalance.Sign() > 0
				existing.owned = newOwned
				updateHeldCount(prevOwned, newOwned, &heldCount)

				if isNewer(existing, vLog) {
					existing.blockNumber = vLog.BlockNumber
					existing.logIndex = vLog.Index
				}

			case helpers.ERC1155TransferBatchEventSignature:
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
				for j := range idsLength {
					idStart := idsStart + j*32
					valueStart := valuesStart + j*32

					tokenID := new(big.Int).SetBytes(vLog.Data[idStart : idStart+32])
					amount := new(big.Int).SetBytes(vLog.Data[valueStart : valueStart+32])

					tokenCID := domain.NewTokenCID(f.chainID, domain.StandardERC1155, vLog.Address.Hex(), tokenID.String())

					// Skip if token is blacklisted
					if blacklist != nil && blacklist.IsTokenCIDBlacklisted(tokenCID) {
						continue
					}

					existing := balanceMap[tokenCID]
					if existing == nil {
						existing = &tokenBalance{
							standard:   domain.StandardERC1155,
							netBalance: big.NewInt(0),
						}
						balanceMap[tokenCID] = existing
					}

					prevOwned := existing.netBalance.Sign() > 0
					if toAddr == owner {
						existing.netBalance.Add(existing.netBalance, amount)
					}
					if fromAddr == owner {
						existing.netBalance.Sub(existing.netBalance, amount)
					}
					newOwned := existing.netBalance.Sign() > 0
					existing.owned = newOwned
					updateHeldCount(prevOwned, newOwned, &heldCount)

					if isNewer(existing, vLog) {
						existing.blockNumber = vLog.BlockNumber
						existing.logIndex = vLog.Index
					}
				}
			}
		}

		if heldCount >= limit {
			// Only truncate if there are more logs beyond this block; if this is the last
			// block present in the result set, we've effectively scanned the whole requested range.
			if logIndex < len(allLogs) {
				*effectiveBoundary = blockNumber
				break
			}
		}
	}

	return domain.TokenWithBlockRangeResult{
		Tokens:             tokensFromBalance(balanceMap),
		EffectiveFromBlock: effectiveFromBlock,
		EffectiveToBlock:   effectiveToBlock,
	}, nil
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
