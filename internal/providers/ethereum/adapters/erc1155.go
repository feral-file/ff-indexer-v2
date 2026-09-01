package adapters

import (
	"context"
	"fmt"
	"math/big"
	"sort"
	"time"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"go.uber.org/zap"

	ethadapter "github.com/feral-file/ff-indexer-v2/internal/adapter"
	"github.com/feral-file/ff-indexer-v2/internal/block"
	"github.com/feral-file/ff-indexer-v2/internal/domain"
	"github.com/feral-file/ff-indexer-v2/internal/logger"
	"github.com/feral-file/ff-indexer-v2/internal/providers/ethereum/helpers"
	"github.com/feral-file/ff-indexer-v2/internal/registry"
)

// ERC1155Adapter handles standard ERC-1155 token operations and event parsing.
type ERC1155Adapter struct {
	ethClient     ethadapter.EthClient
	pagination    *helpers.PaginationHelper
	chainID       domain.Chain
	blockProvider block.BlockProvider
	// warehouseTokenFilter is true only when the warehouse for this chain is
	// verified to apply the erc1155Id filter (see
	// ethereum.ChainSupportsWarehouseERC1155Filter). The per-token hint is sent
	// only then; on any other chain the whole-contract fetch plus the
	// client-side token-id filter below is used instead, so an unverified
	// warehouse is never trusted to have filtered by id.
	warehouseTokenFilter bool
}

// NewERC1155Adapter creates an adapter for standard ERC-1155 contracts.
func NewERC1155Adapter(
	ethClient ethadapter.EthClient,
	pagination *helpers.PaginationHelper,
	chainID domain.Chain,
	blockProvider block.BlockProvider,
	warehouseTokenFilter bool,
) *ERC1155Adapter {
	return &ERC1155Adapter{
		ethClient:            ethClient,
		pagination:           pagination,
		chainID:              chainID,
		blockProvider:        blockProvider,
		warehouseTokenFilter: warehouseTokenFilter,
	}
}

// GetStandard returns the ERC-1155 chain standard.
func (a *ERC1155Adapter) GetStandard() domain.ChainStandard {
	return domain.StandardERC1155
}

// OwnershipModel returns multi-holder semantics for ERC-1155 tokens.
func (a *ERC1155Adapter) OwnershipModel() OwnershipModel {
	return OwnershipMultiHolder
}

// GetTokenBalances fetches all holder balances by replaying standard ERC-1155 transfer events.
func (a *ERC1155Adapter) GetTokenBalances(
	ctx context.Context,
	contractAddress, tokenNumber string,
) (map[string]string, error) {
	return helpers.ERC1155ReplayBalances(ctx, a.pagination, a.blockProvider, contractAddress, tokenNumber)
}

// GetTokenBalancesForAddresses fetches accurate on-chain balances for specific addresses.
//
// Uses the ERC-1155 balanceOfBatch contract call for accuracy, unlike GetTokenBalances
// which uses best-effort event replay. This method is designed for full provenance indexing
// where complete accuracy is required.
//
// Reason: Full provenance requires accurate current state via on-chain queries, not
// best-effort replay which has 10M block limits, 30s timeouts, and ignores TransferBatch.
//
// Trade-offs: Slightly more expensive (RPC calls) but provides 100% accurate balances.
//
// Constraints: Returns only non-zero balances. Processes 200 addresses per batch call.
func (a *ERC1155Adapter) GetTokenBalancesForAddresses(
	ctx context.Context,
	contractAddress, tokenNumber string,
	addresses []string,
) (map[string]string, error) {
	if len(addresses) == 0 {
		return make(map[string]string), nil
	}

	allBalances, err := helpers.ERC1155BalanceOfBatch(ctx, a.ethClient, contractAddress, tokenNumber, addresses)
	if err != nil {
		return nil, fmt.Errorf("failed to get ERC1155 balances for addresses: %w", err)
	}

	// Filter out zero balances
	filtered := make(map[string]string)
	for addr, balance := range allBalances {
		if balance != "0" {
			filtered[addr] = balance
		}
	}

	return filtered, nil
}

// GetOwnerBalanceAndEvents fetches balance and events for a specific ERC-1155 owner.
func (a *ERC1155Adapter) GetOwnerBalanceAndEvents(
	ctx context.Context,
	contractAddress, tokenNumber, ownerAddress string,
) (string, []domain.BlockchainEvent, error) {
	return helpers.ERC1155BalanceAndEventsForOwner(
		ctx,
		a.ethClient,
		a.pagination,
		a.blockProvider,
		a.chainID,
		time.Now,
		contractAddress,
		tokenNumber,
		ownerAddress,
	)
}

// TokenExists checks existence via recent transfer scan and balance checks.
func (a *ERC1155Adapter) TokenExists(ctx context.Context, contractAddress, tokenNumber string) (bool, error) {
	return helpers.ERC1155TokenExists(ctx, a.ethClient, a.pagination, a.blockProvider, contractAddress, tokenNumber)
}

// TokenOwner is unsupported for fungible ERC-1155 tokens.
func (a *ERC1155Adapter) TokenOwner(ctx context.Context, contractAddress, tokenNumber string) (string, error) {
	return "", fmt.Errorf("ERC1155 does not support single-owner lookup")
}

// TokenURI returns the ERC-1155 uri value.
func (a *ERC1155Adapter) TokenURI(ctx context.Context, contractAddress, tokenNumber string) (string, error) {
	return helpers.ERC1155URI(ctx, a.ethClient, contractAddress, tokenNumber)
}

// SupportsProvenance reports that standard ERC-1155 provenance indexing is supported.
func (a *ERC1155Adapter) SupportsProvenance() bool {
	return true
}

// GetEventSignatures returns standard ERC-1155 event topic hashes.
func (a *ERC1155Adapter) GetEventSignatures() []common.Hash {
	return []common.Hash{
		helpers.ERC1155TransferSingleEventSignature,
		helpers.ERC1155TransferBatchEventSignature,
		helpers.ERC1155URIEventSignature,
	}
}

// GetTokenEvents fetches all historical events for a specific ERC-1155 token.
// For ERC-1155, token ID is in data, not topics, so we fetch all events for this contract
// and filter by token ID client-side. Returns events in ascending order of timestamp.
func (a *ERC1155Adapter) GetTokenEvents(ctx context.Context, contractAddress, tokenNumber string) ([]domain.BlockchainEvent, error) {
	// Parse token number to big.Int
	tokenID, ok := new(big.Int).SetString(tokenNumber, 10)
	if !ok {
		return nil, fmt.Errorf("invalid token number: %s", tokenNumber)
	}

	contractAddr := common.HexToAddress(contractAddress)

	// For ERC1155, token ID is in data, not topics, so we fetch all events for this contract
	// and filter by token ID later
	query := ethereum.FilterQuery{
		FromBlock: big.NewInt(0),
		ToBlock:   nil,
		Addresses: []common.Address{contractAddr},
		Topics: [][]common.Hash{
			{
				helpers.ERC1155TransferSingleEventSignature, // TransferSingle events
				//helpers.ERC1155TransferBatchEventSignature,  // FIXME: Handle batch transfers properly
				helpers.ERC1155URIEventSignature, // URI events (metadata updates)
			},
		},
	}

	// Fetch logs with pagination to handle Infura's 10k limitation. When the
	// warehouse for this chain is verified to apply the erc1155Id filter, send
	// the token-id hint so the per-token walk is an index point lookup there
	// (TransferSingle by data word 0, URI by topic1); the vendor leg ignores it
	// and still returns the whole contract, so the token-id filter below remains
	// the correctness backstop. On a chain without that verified capability the
	// hint is omitted (never trusted unverified).
	var opts []helpers.FilterOption
	if a.warehouseTokenFilter {
		opts = append(opts, helpers.WithERC1155TokenID(common.BigToHash(tokenID)))
	}
	logs, err := a.pagination.FilterLogsWithPagination(ctx, query, opts...)
	if err != nil {
		return nil, fmt.Errorf("failed to filter logs: %w", err)
	}

	// Parse logs and convert to BlockchainEvent
	events := make([]domain.BlockchainEvent, 0)
	for _, vLog := range logs {
		parsed, err := a.ParseEvent(ctx, vLog)
		if err != nil {
			return nil, fmt.Errorf("parse event log at block %d index %d: %w", vLog.BlockNumber, vLog.Index, err)
		}
		if parsed == nil {
			continue
		}

		// Filter by token number
		if parsed.TokenNumber != tokenNumber {
			continue
		}

		events = append(events, *parsed)
	}

	// Sort events by block number, transaction index, and log index for deterministic ordering
	sort.SliceStable(events, func(i, j int) bool {
		if events[i].BlockNumber != events[j].BlockNumber {
			return events[i].BlockNumber < events[j].BlockNumber
		}
		if events[i].TxIndex != events[j].TxIndex {
			return events[i].TxIndex < events[j].TxIndex
		}
		return events[i].LogIndex < events[j].LogIndex
	})

	return events, nil
}

// OwnerQuerySpecs declares the ERC-1155 owner-scan shapes. TransferSingle and
// TransferBatch share the (operator, from, to) indexed-topic layout, so one spec
// per owner position covers both event types: owner as sender (topic 2) and as
// recipient (topic 3). Topic 1 is the operator, which does not affect ownership.
func (a *ERC1155Adapter) OwnerQuerySpecs() []OwnerQuerySpec {
	transferSigs := []common.Hash{
		helpers.ERC1155TransferSingleEventSignature,
		helpers.ERC1155TransferBatchEventSignature,
	}
	return []OwnerQuerySpec{
		{EventSigs: transferSigs, OwnerTopicIndex: 2},
		{EventSigs: transferSigs, OwnerTopicIndex: 3},
	}
}

// PostProcessOwnerLogs is a no-op: standard ERC-1155 needs no receipt-based repair.
func (a *ERC1155Adapter) PostProcessOwnerLogs(_ context.Context, _ common.Address, _ []types.Log) ([]types.Log, error) {
	return nil, nil
}

// GetTokensByOwner returns ERC1155 tokens owned by the address at the end of the block range.
func (a *ERC1155Adapter) GetTokensByOwner(
	ctx context.Context,
	ownerAddress string,
	fromBlock uint64,
	toBlock uint64,
	blacklist registry.BlacklistRegistry,
) ([]domain.TokenWithBlock, error) {
	owner := common.HexToAddress(ownerAddress)

	logs, err := FetchOwnerLogs(ctx, a.pagination, a.OwnerQuerySpecs(), common.BytesToHash(owner.Bytes()), nil, fromBlock, toBlock)
	if err != nil {
		return nil, fmt.Errorf("failed to query ERC1155 logs: %w", err)
	}

	logs = deduplicateLogs(logs)
	sortLogsAscending(logs)

	return trackERC1155OwnershipFromLogs(a.chainID, owner, logs, blacklist), nil
}

// ParseEvent parses standard ERC-1155 events.
//
// Shape checks run BEFORE the block-timestamp lookup and drop the log instead
// of failing, mirroring the ERC-721 adapter: the whole-chain topic0 filter lets
// any contract emit a log under these signatures, and a fatal parse error
// replays from the durable cursor and crash-loops ingestion (measured on the
// ERC-721 side, 2026-08-27). The former ordering also let a failing
// BlockProvider turn a log that was going to be dropped into that same crash.
func (a *ERC1155Adapter) ParseEvent(ctx context.Context, vLog types.Log) (*domain.BlockchainEvent, error) {
	if len(vLog.Topics) == 0 {
		return nil, fmt.Errorf("event log has no topics")
	}

	switch vLog.Topics[0] {
	case helpers.ERC1155TransferSingleEventSignature:
		if len(vLog.Topics) != 4 || len(vLog.Data) < 64 {
			skipMalformedStandardLog(ctx, "TransferSingle", vLog)
			return nil, nil
		}

		base, err := helpers.BaseEventFromLog(ctx, a.chainID, vLog, a.blockProvider)
		if err != nil {
			return nil, err
		}

		event := base
		event.Standard = domain.StandardERC1155
		fromAddress := common.BytesToAddress(vLog.Topics[2].Bytes()).Hex()
		event.FromAddress = &fromAddress
		toAddress := common.BytesToAddress(vLog.Topics[3].Bytes()).Hex()
		event.ToAddress = &toAddress
		event.TokenNumber = new(big.Int).SetBytes(vLog.Data[0:32]).String()
		event.Quantity = new(big.Int).SetBytes(vLog.Data[32:64]).String()
		event.EventType = domain.TransferEventType(event.FromAddress, event.ToAddress)
		return &event, nil
	case helpers.ERC1155TransferBatchEventSignature:
		logger.DebugCtx(ctx, "Skipping ERC1155 TransferBatch event",
			zap.String("contract", vLog.Address.Hex()),
			zap.String("txHash", vLog.TxHash.Hex()))
		return nil, nil
	case helpers.ERC1155URIEventSignature:
		if len(vLog.Topics) != 2 {
			skipMalformedStandardLog(ctx, "URI", vLog)
			return nil, nil
		}

		base, err := helpers.BaseEventFromLog(ctx, a.chainID, vLog, a.blockProvider)
		if err != nil {
			return nil, err
		}

		event := base
		event.Standard = domain.StandardERC1155
		event.TokenNumber = new(big.Int).SetBytes(vLog.Topics[1].Bytes()).String()
		event.EventType = domain.EventTypeMetadataUpdate
		event.Quantity = "1"
		return &event, nil
	default:
		return nil, ErrUnknownEvent
	}
}

// skipMalformedStandardLog records a log dropped because its shape does not
// match the standard event its topic0 claims. Warn level: unlike pre-standard
// Transfer shapes (continuous, debug-logged in the ERC-721 adapter), a foreign
// log under an ERC-1155 signature is rare enough to stay visible. Dropping it
// loses one event from a contract that is not emitting the standard anyway;
// failing on it would crash-loop ingestion from the durable cursor.
func skipMalformedStandardLog(ctx context.Context, eventName string, vLog types.Log) {
	logger.WarnCtx(ctx, "Skipping malformed standard-signature log",
		zap.String("event", eventName),
		zap.String("contract", vLog.Address.Hex()),
		zap.Uint64("block", vLog.BlockNumber),
		zap.Uint("logIndex", vLog.Index),
		zap.Int("topics", len(vLog.Topics)),
		zap.Int("dataLen", len(vLog.Data)))
}

var _ ContractAdapter = (*ERC1155Adapter)(nil)
