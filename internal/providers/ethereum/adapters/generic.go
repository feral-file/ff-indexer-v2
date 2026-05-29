package adapters

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"math/big"
	"sort"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"go.uber.org/zap"

	ethadapter "github.com/feral-file/ff-indexer-v2/internal/adapter"
	"github.com/feral-file/ff-indexer-v2/internal/block"
	"github.com/feral-file/ff-indexer-v2/internal/domain"
	"github.com/feral-file/ff-indexer-v2/internal/logger"
	"github.com/feral-file/ff-indexer-v2/internal/providers/ethereum/helpers"
	"github.com/feral-file/ff-indexer-v2/internal/registry"
)

// GenericAdapter executes configured contract calls for legacy or custom contracts.
type GenericAdapter struct {
	contractAddress  string
	ownershipModel   OwnershipModel
	cidStandard      domain.ChainStandard
	existence        *MethodCall
	owner            *MethodCall
	metadata         ContractMetadataConfig
	ethClient        ethadapter.EthClient
	pagination       *helpers.PaginationHelper
	blockProvider    block.BlockProvider
	provenance       bool
	provenanceEvents []EventConfig
	chainID          domain.Chain
}

// NewGenericAdapter builds a GenericAdapter from parsed contract configuration.
func NewGenericAdapter(
	contractAddress string,
	ownershipModel OwnershipModel,
	existence, owner *MethodCall,
	metadata ContractMetadataConfig,
	ethClient ethadapter.EthClient,
	pagination *helpers.PaginationHelper,
	blockProvider block.BlockProvider,
	supportsProvenance bool,
	provenanceEvents []EventConfig,
	chainID domain.Chain,
) *GenericAdapter {
	return &GenericAdapter{
		contractAddress:  contractAddress,
		ownershipModel:   ownershipModel,
		cidStandard:      CIDStandardFromOwnershipModel(ownershipModel),
		existence:        existence,
		owner:            owner,
		metadata:         metadata,
		ethClient:        ethClient,
		pagination:       pagination,
		blockProvider:    blockProvider,
		provenance:       supportsProvenance,
		provenanceEvents: provenanceEvents,
		chainID:          chainID,
	}
}

// GetStandard returns the CID/API standard label derived from ownership_model.
func (a *GenericAdapter) GetStandard() domain.ChainStandard {
	return a.cidStandard
}

// OwnershipModel returns the configured ownership semantics for this contract.
func (a *GenericAdapter) OwnershipModel() OwnershipModel {
	return a.ownershipModel
}

// TokenExists checks token existence using the configured existence method.
func (a *GenericAdapter) TokenExists(ctx context.Context, contractAddress, tokenNumber string) (bool, error) {
	if a.existence == nil {
		return false, fmt.Errorf("existence method not configured")
	}

	result, err := a.callMethod(ctx, contractAddress, tokenNumber, a.existence)
	if err != nil {
		if helpers.IsExecutionRevert(err) {
			return false, nil
		}
		return false, err
	}

	return evaluateSuccess(result, a.existence.SuccessCondition)
}

// TokenOwner resolves the current owner using the configured owner method.
func (a *GenericAdapter) TokenOwner(ctx context.Context, contractAddress, tokenNumber string) (string, error) {
	if a.owner == nil {
		return "", fmt.Errorf("owner method not configured")
	}

	result, err := a.callMethod(ctx, contractAddress, tokenNumber, a.owner)
	if err != nil {
		return "", err
	}

	owner, err := unpackAddress(a.owner.ABI, a.owner.Method, result)
	if err != nil {
		return "", err
	}

	if owner == common.HexToAddress(domain.ETHEREUM_ZERO_ADDRESS) &&
		a.existence != nil &&
		a.existence.SuccessCondition == SuccessAddressNonZero {
		return "", domain.ErrTokenNotFoundOnChain
	}

	return owner.Hex(), nil
}

// TokenURI returns on-chain metadata when configured; vendor-only contracts return empty.
func (a *GenericAdapter) TokenURI(ctx context.Context, contractAddress, tokenNumber string) (string, error) {
	switch a.metadata.Source {
	case MetadataSourceVendorOnly, "":
		return "", nil
	case MetadataSourceOnChain:
		if a.metadata.Method == nil {
			return "", fmt.Errorf("on-chain metadata method not configured")
		}
		result, err := a.callMethod(ctx, contractAddress, tokenNumber, a.metadata.Method)
		if err != nil {
			return "", err
		}
		var uri string
		if err := a.metadata.Method.ABI.UnpackIntoInterface(&uri, a.metadata.Method.Method, result); err != nil {
			return "", fmt.Errorf("failed to unpack metadata URI: %w", err)
		}
		return uri, nil
	default:
		return "", fmt.Errorf("unsupported metadata source: %s", a.metadata.Source)
	}
}

// SupportsProvenance reports whether full on-chain provenance indexing is supported.
func (a *GenericAdapter) SupportsProvenance() bool {
	return a.provenance
}

// GetTokenBalances replays configured custom events to compute all holder balances for a token.
func (a *GenericAdapter) GetTokenBalances(
	ctx context.Context,
	contractAddress, tokenNumber string,
) (map[string]string, error) {
	if a.ownershipModel != OwnershipMultiHolder {
		return nil, fmt.Errorf("GetTokenBalances not supported for single-owner contracts")
	}

	events, err := a.GetTokenEvents(ctx, contractAddress, tokenNumber)
	if err != nil {
		return nil, err
	}

	return replayBalancesFromEvents(events), nil
}

// GetTokenBalancesForAddresses fetches balances for specific addresses.
//
// For single-owner contracts: checks if any address matches the current owner.
// For multi-holder contracts: replays events filtered by provided addresses.
//
// Reason: Legacy contracts may not have balanceOf/balanceOfBatch methods, so we
// use the configured owner method for single-owner or event replay for multi-holder.
//
// Trade-offs: Multi-holder replay may miss transfers outside event coverage, but
// this is consistent with configured contract limitations. Full provenance using
// legacy multi-holder contracts should ensure events are properly configured.
//
// Constraints: Depends on configured custom events for multi-holder accuracy.
func (a *GenericAdapter) GetTokenBalancesForAddresses(
	ctx context.Context,
	contractAddress, tokenNumber string,
	addresses []string,
) (map[string]string, error) {
	if len(addresses) == 0 {
		return make(map[string]string), nil
	}

	switch a.ownershipModel {
	case OwnershipSingleOwner:
		// For single-owner, check if current owner is in the address list
		owner, err := a.TokenOwner(ctx, contractAddress, tokenNumber)
		if err != nil {
			if helpers.IsExecutionRevert(err) || errors.Is(err, domain.ErrTokenNotFoundOnChain) {
				// Token doesn't exist or is burned
				return make(map[string]string), nil
			}
			return nil, fmt.Errorf("failed to get token owner: %w", err)
		}

		ownerLower := common.HexToAddress(owner).Hex()
		for _, addr := range addresses {
			addrLower := common.HexToAddress(addr).Hex()
			if addrLower == ownerLower && ownerLower != domain.ETHEREUM_ZERO_ADDRESS {
				return map[string]string{ownerLower: "1"}, nil
			}
		}
		return make(map[string]string), nil

	case OwnershipMultiHolder:
		// For multi-holder, replay events and filter by addresses
		events, err := a.GetTokenEvents(ctx, contractAddress, tokenNumber)
		if err != nil {
			return nil, err
		}

		// Create address lookup map for efficient filtering
		addressMap := make(map[string]bool)
		for _, addr := range addresses {
			addressMap[common.HexToAddress(addr).Hex()] = true
		}

		// Replay balances and filter by requested addresses
		allBalances := replayBalancesFromEvents(events)
		filtered := make(map[string]string)
		for addr, balance := range allBalances {
			addrLower := common.HexToAddress(addr).Hex()
			if addressMap[addrLower] {
				filtered[addrLower] = balance
			}
		}

		return filtered, nil

	default:
		return nil, fmt.Errorf("unsupported ownership model: %s", a.ownershipModel)
	}
}

// GetOwnerBalanceAndEvents replays configured custom events for one owner and token.
func (a *GenericAdapter) GetOwnerBalanceAndEvents(
	ctx context.Context,
	contractAddress, tokenNumber, ownerAddress string,
) (string, []domain.BlockchainEvent, error) {
	if a.ownershipModel != OwnershipMultiHolder {
		return "", nil, fmt.Errorf("GetOwnerBalanceAndEvents not supported for single-owner contracts")
	}

	events, err := a.GetTokenEvents(ctx, contractAddress, tokenNumber)
	if err != nil {
		return "", nil, err
	}

	ownerEvents := filterOwnerEvents(events, ownerAddress)
	balance := replayOwnerBalanceFromEvents(ownerAddress, events)

	return balance, ownerEvents, nil
}

// GetEventSignatures returns configured custom event topic hashes.
func (a *GenericAdapter) GetEventSignatures() []common.Hash {
	signatures := make([]common.Hash, 0, len(a.provenanceEvents))
	for _, eventCfg := range a.provenanceEvents {
		signatures = append(signatures, crypto.Keccak256Hash([]byte(eventCfg.Signature)))
	}
	return signatures
}

// GetTokenEvents fetches all historical events for a specific token from this configured contract.
// Uses the custom event signatures from the contract configuration.
// Note: Client-side filtering by token number is required since configured events may not have indexed token IDs.
func (a *GenericAdapter) GetTokenEvents(ctx context.Context, contractAddress, tokenNumber string) ([]domain.BlockchainEvent, error) {
	customSignatures := a.GetEventSignatures()
	if len(customSignatures) == 0 {
		return nil, fmt.Errorf("configured contract has no event signatures")
	}

	// Build query for configured custom events
	contractAddr := common.HexToAddress(contractAddress)
	query := ethereum.FilterQuery{
		FromBlock: big.NewInt(0),
		ToBlock:   nil,
		Addresses: []common.Address{contractAddr},
		Topics:    [][]common.Hash{customSignatures},
	}

	// Fetch logs with pagination
	logs, err := a.pagination.FilterLogsWithPagination(ctx, query)
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

		// Filter by token number (client-side filtering required for configured contracts)
		if parsed.TokenNumber != tokenNumber {
			continue
		}

		events = append(events, *parsed)
	}

	var repairErr error
	events, repairErr = a.repairBrokenPunkBoughtEvents(ctx, events)
	if repairErr != nil {
		return nil, repairErr
	}

	events = deduplicateOwnershipEvents(events)

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

// GetOwnerLogs fetches configured provenance event logs where the owner is sender or recipient.
//
// For CryptoPunks, also queries internal Transfer(seller, buyer, 1) events to discover
// corrupted acceptBidForPunk purchases where PunkBought has zero indexed toAddress but
// the internal Transfer contains the real buyer. For each internal Transfer, fetches the
// transaction to find and include the corrupted PunkBought event.
func (a *GenericAdapter) GetOwnerLogs(
	ctx context.Context,
	ownerAddress string,
	fromBlock uint64,
	toBlock uint64,
) ([]types.Log, error) {
	if len(a.provenanceEvents) == 0 {
		return nil, nil
	}

	owner := common.HexToAddress(ownerAddress)
	ownerHash := common.BytesToHash(owner.Bytes())
	contractAddr := common.HexToAddress(a.contractAddress)

	queries := buildCustomOwnerTransferQueries(a.provenanceEvents, contractAddr, ownerHash, fromBlock, toBlock)

	// For CryptoPunks PunkBought repairs, also query internal Transfer events where owner is recipient.
	// This discovers corrupted acceptBidForPunk purchases where the PunkBought log has zero buyer
	// but the same-tx internal Transfer(seller, buyer, 1) reveals the real buyer.
	var internalTransferQuery *ethereum.FilterQuery
	if a.hasPunkBoughtEvent() {
		query := buildInternalTransferBuyerQuery(contractAddr, ownerHash, fromBlock, toBlock)
		internalTransferQuery = &query
		queries = append(queries, query)
	}

	logs, err := filterLogsInParallel(ctx, a.pagination, queries)
	if err != nil {
		return nil, fmt.Errorf("failed to query configured contract logs: %w", err)
	}

	// For CryptoPunks, process internal Transfer logs to find corrupted PunkBought events
	if internalTransferQuery != nil {
		additionalLogs, err := a.findCorruptedPunkBoughtFromInternalTransfers(ctx, logs, contractAddr)
		if err != nil {
			return nil, err
		}
		logs = append(logs, additionalLogs...)
	}

	return logs, nil
}

// findCorruptedPunkBoughtFromInternalTransfers extracts corrupted PunkBought events by
// fetching transaction receipts for internal Transfer logs and finding PunkBought events
// in the same transactions.
func (a *GenericAdapter) findCorruptedPunkBoughtFromInternalTransfers(
	ctx context.Context,
	logs []types.Log,
	contractAddr common.Address,
) ([]types.Log, error) {
	if a.ethClient == nil {
		return nil, nil
	}

	// Find internal Transfer logs
	var internalTransferLogs []types.Log
	for _, vLog := range logs {
		if vLog.Address == contractAddr &&
			len(vLog.Topics) == 3 &&
			vLog.Topics[0] == helpers.TransferEventSignature &&
			len(vLog.Data) >= 32 {
			value := new(big.Int).SetBytes(vLog.Data[:32])
			if value.Cmp(big.NewInt(1)) == 0 {
				internalTransferLogs = append(internalTransferLogs, vLog)
			}
		}
	}

	if len(internalTransferLogs) == 0 {
		return nil, nil
	}

	// Fetch receipts and extract PunkBought logs
	var additionalLogs []types.Log
	seenTxs := make(map[common.Hash]bool)
	punkBoughtSig := crypto.Keccak256Hash([]byte("PunkBought(uint256,uint256,address,address)"))

	for _, transferLog := range internalTransferLogs {
		if seenTxs[transferLog.TxHash] {
			continue
		}
		seenTxs[transferLog.TxHash] = true

		receipt, err := a.ethClient.TransactionReceipt(ctx, transferLog.TxHash)
		if err != nil {
			return nil, fmt.Errorf("fetch receipt for tx %s: %w", transferLog.TxHash.Hex(), err)
		}

		// Find PunkBought logs in the same transaction
		for _, receiptLog := range receipt.Logs {
			if receiptLog == nil {
				continue
			}
			if receiptLog.Address == contractAddr &&
				len(receiptLog.Topics) > 0 &&
				receiptLog.Topics[0] == punkBoughtSig {
				// Found a PunkBought in the same transaction as an internal Transfer
				additionalLogs = append(additionalLogs, *receiptLog)
			}
		}
	}

	return additionalLogs, nil
}

// GetTokensByOwner returns tokens owned by the address at the end of the block range for this configured contract.
//
// Queries custom provenance events from contracts.json, parses them into standardized events,
// then applies ownership tracking based on the configured standard field:
// - standard="erc721": last-transfer-wins logic
// - standard="erc1155": balance-accumulation logic
//
// Logs warnings for events that fail to parse (schema mismatch or malformed data) but continues
// processing remaining events. This prevents a single bad event from failing the entire scan.
//
// Returns empty result if no provenance events are configured.
func (a *GenericAdapter) GetTokensByOwner(
	ctx context.Context,
	ownerAddress string,
	fromBlock uint64,
	toBlock uint64,
	blacklist registry.BlacklistRegistry,
) ([]domain.TokenWithBlock, error) {
	if len(a.provenanceEvents) == 0 {
		return nil, nil
	}

	owner := common.HexToAddress(ownerAddress)

	logs, err := a.GetOwnerLogs(ctx, ownerAddress, fromBlock, toBlock)
	if err != nil {
		return nil, err
	}

	logs = deduplicateLogs(logs)
	sortLogsAscending(logs)

	events := make([]domain.BlockchainEvent, 0, len(logs))
	for _, vLog := range logs {
		parsed, err := a.ParseEvent(ctx, vLog)
		if err != nil {
			return nil, fmt.Errorf("parse event at block %d log %d: %w", vLog.BlockNumber, vLog.Index, err)
		}
		if parsed == nil {
			logger.WarnCtx(ctx, "ParseEvent returned nil in ownership scan",
				zap.String("contract", a.contractAddress),
				zap.Uint64("block", vLog.BlockNumber),
				zap.Uint("logIndex", vLog.Index))
			continue
		}

		events = append(events, *parsed)
	}

	var repairErr error
	events, repairErr = a.repairBrokenPunkBoughtEvents(ctx, events)
	if repairErr != nil {
		return nil, repairErr
	}

	events = deduplicateOwnershipEvents(events)

	return trackOwnershipFromParsedEvents(a.chainID, a.cidStandard, a.contractAddress, owner, events, blacklist), nil
}

// buildCustomOwnerTransferQueries constructs filter queries for configured provenance events
// where the owner address appears in indexed from/to parameters.
//
// For CryptoPunks PunkBought, also queries events where owner is the seller (FromAddress),
// because corrupted acceptBidForPunk events have zero in the indexed ToAddress but correct
// FromAddress. After fetching, repair logic will restore the buyer from internal Transfer logs.
//
// Only processes transfer/mint/burn events. Metadata update events do not affect ownership
// and are skipped.
//
// Returns separate queries for each (event, topic position) combination to maximize parallelism.
// Config validation ensures from/to addresses are indexed, so this always produces owner-scoped queries.
func buildCustomOwnerTransferQueries(
	provenanceEvents []EventConfig,
	contractAddr common.Address,
	ownerHash common.Hash,
	fromBlock, toBlock uint64,
) []ethereum.FilterQuery {
	var queries []ethereum.FilterQuery

	for _, eventCfg := range provenanceEvents {
		if !isOwnershipAffectingEvent(eventCfg.MapToStandardEvent) {
			continue
		}

		eventSig := crypto.Keccak256Hash([]byte(eventCfg.Signature))
		fromIndex, toIndex := indexedAddressTopicIndices(eventCfg)

		if fromIndex > 0 {
			queries = append(queries, buildCustomOwnerQuery(eventSig, contractAddr, ownerHash, fromIndex, fromBlock, toBlock))
		}
		if toIndex > 0 {
			queries = append(queries, buildCustomOwnerQuery(eventSig, contractAddr, ownerHash, toIndex, fromBlock, toBlock))
		}
	}

	return queries
}

// indexedAddressTopicIndices finds the topic positions of FromAddress and ToAddress in the event signature.
//
// Topic 0 is always the event signature hash. Indexed parameters occupy topics 1, 2, 3 in declaration order.
// Returns (-1, -1) if the addresses are not indexed or not mapped.
//
// Example: PunkTransfer(address indexed from, address indexed to, uint256 punkIndex)
// - from is at topic index 1
// - to is at topic index 2
func indexedAddressTopicIndices(eventCfg EventConfig) (fromIndex, toIndex int) {
	fromIndex = -1
	toIndex = -1

	for i, paramName := range eventCfg.IndexedParams {
		target, ok := eventCfg.ParameterMappings[paramName]
		if !ok {
			continue
		}

		topicIndex := i + 1
		switch target {
		case EventFieldFromAddress:
			fromIndex = topicIndex
		case EventFieldToAddress:
			toIndex = topicIndex
		}
	}

	return fromIndex, toIndex
}

// buildCustomOwnerQuery constructs a filter query for a specific event signature and topic position.
// The owner hash is placed at the specified topic index to filter for events where the owner
// is the sender or recipient.
func buildCustomOwnerQuery(
	eventSig common.Hash,
	contractAddr common.Address,
	ownerHash common.Hash,
	ownerTopicIndex int,
	fromBlock, toBlock uint64,
) ethereum.FilterQuery {
	topics := make([][]common.Hash, ownerTopicIndex+1)
	topics[0] = []common.Hash{eventSig}
	topics[ownerTopicIndex] = []common.Hash{ownerHash}

	return ethereum.FilterQuery{
		FromBlock: new(big.Int).SetUint64(fromBlock),
		ToBlock:   new(big.Int).SetUint64(toBlock),
		Addresses: []common.Address{contractAddr},
		Topics:    topics,
	}
}

// buildInternalTransferBuyerQuery constructs a query for CryptoPunks internal Transfer(seller, buyer, 1)
// events where the owner is the buyer (topic[2]). This discovers corrupted acceptBidForPunk purchases
// where the PunkBought log has zero buyer but the internal Transfer reveals the real buyer.
//
// Reason: acceptBidForPunk clears punkBids storage before emitting PunkBought under Solidity 0.4.8,
// so the emitted toAddress can be zero even though ownership transferred. The same transaction
// includes an internal Transfer(seller, buyer, 1) log with the correct buyer.
//
// Trade-offs: This adds one additional parallel query per owner sweep, but it's bounded by the
// same block range and is necessary to discover all CryptoPunks purchases for an owner.
//
// Constraints: Only applies to contracts with PunkBought provenance events configured.
func buildInternalTransferBuyerQuery(
	contractAddr common.Address,
	buyerHash common.Hash,
	fromBlock, toBlock uint64,
) ethereum.FilterQuery {
	// Internal Transfer uses standard ERC20-style Transfer(address indexed from, address indexed to, uint256 value)
	// Topic[0] = Transfer signature
	// Topic[1] = from (seller)
	// Topic[2] = to (buyer) <-- filter by owner here
	topics := [][]common.Hash{
		{helpers.TransferEventSignature}, // topic[0]: Transfer event
		nil,                              // topic[1]: from (any seller)
		{buyerHash},                      // topic[2]: to (specific buyer)
	}

	return ethereum.FilterQuery{
		FromBlock: new(big.Int).SetUint64(fromBlock),
		ToBlock:   new(big.Int).SetUint64(toBlock),
		Addresses: []common.Address{contractAddr},
		Topics:    topics,
	}
}

// ParseEvent parses a configured legacy contract event into a blockchain event.
//
// Returns (nil, nil) for intentionally skipped logs:
// - CryptoPunks internal Transfer(seller, buyer, 1) logs used only for buyer discovery
func (a *GenericAdapter) ParseEvent(ctx context.Context, vLog types.Log) (*domain.BlockchainEvent, error) {
	if len(vLog.Topics) == 0 {
		return nil, fmt.Errorf("event log has no topics")
	}

	// Skip CryptoPunks internal Transfer logs - these are queried for corrupted buyer discovery
	// but should not be parsed as standalone events. The real PunkBought/PunkTransfer events
	// will be repaired using these internal logs.
	if a.hasPunkBoughtEvent() && vLog.Topics[0] == helpers.TransferEventSignature {
		if len(vLog.Topics) == 3 && len(vLog.Data) >= 32 {
			value := new(big.Int).SetBytes(vLog.Data[:32])
			// Internal CryptoPunks transfers have value=1
			if value.Cmp(big.NewInt(1)) == 0 {
				return nil, nil
			}
		}
	}

	for _, eventCfg := range a.provenanceEvents {
		expectedSignature := crypto.Keccak256Hash([]byte(eventCfg.Signature))
		if vLog.Topics[0] != expectedSignature {
			continue
		}
		parsed, err := parseConfiguredEvent(ctx, vLog, eventCfg, a.cidStandard, a.chainID, a.blockProvider)
		if err != nil {
			return nil, err
		}
		// Skip single-event repair here; batch repair after token filtering handles this
		return parsed, nil
	}

	return nil, ErrUnknownEvent
}

func parseConfiguredEvent(
	ctx context.Context,
	vLog types.Log,
	eventCfg EventConfig,
	standard domain.ChainStandard,
	chain domain.Chain,
	blockProvider block.BlockProvider,
) (*domain.BlockchainEvent, error) {
	event, err := helpers.BaseEventFromLog(ctx, chain, vLog, blockProvider)
	if err != nil {
		return nil, err
	}
	event.Standard = standard
	event.EventType = eventCfg.MapToStandardEvent

	indexedValues := make(map[string]any, len(eventCfg.IndexedParams))
	topicIndex := 1
	for _, paramName := range eventCfg.IndexedParams {
		if topicIndex >= len(vLog.Topics) {
			return nil, fmt.Errorf("insufficient topics for indexed parameter %s", paramName)
		}
		indexedValues[paramName] = vLog.Topics[topicIndex]
		topicIndex++
	}

	dataValues := make(map[string]any, len(eventCfg.DataParams))
	dataOffset := 0
	for _, paramName := range eventCfg.DataParams {
		if dataOffset+32 > len(vLog.Data) {
			return nil, fmt.Errorf("insufficient data for parameter %s", paramName)
		}
		dataValues[paramName] = append([]byte(nil), vLog.Data[dataOffset:dataOffset+32]...)
		dataOffset += 32
	}

	allValues := make(map[string]any, len(indexedValues)+len(dataValues))
	maps.Copy(allValues, indexedValues)
	maps.Copy(allValues, dataValues)

	for paramName, eventField := range eventCfg.ParameterMappings {
		val, ok := allValues[paramName]
		if !ok {
			return nil, fmt.Errorf("parameter %s not found in event data", paramName)
		}
		if err := applyCustomEventField(&event, eventField, val); err != nil {
			return nil, fmt.Errorf("map parameter %s: %w", paramName, err)
		}
	}

	switch event.EventType {
	case domain.EventTypeTransfer, domain.EventTypeMint, domain.EventTypeBurn:
		if event.Quantity == "" {
			event.Quantity = "1"
		}
	case domain.EventTypeMetadataUpdate:
		if event.Quantity == "" {
			event.Quantity = "1"
		}
	}

	// Configured legacy events keep mapToStandardEvent as declared in contracts.json.
	// Do not reclassify via TransferEventType: CryptoPunks PunkBought can carry zero toAddress
	// before receipt repair, and mint/burn are modeled as separate configured events.

	logger.DebugCtx(ctx, "Parsed custom contract event",
		zap.String("contract", vLog.Address.Hex()),
		zap.String("signature", eventCfg.Signature),
		zap.String("eventType", string(event.EventType)),
		zap.String("tokenNumber", event.TokenNumber),
	)

	return &event, nil
}

func applyCustomEventField(event *domain.BlockchainEvent, eventField string, val any) error {
	switch eventField {
	case EventFieldFromAddress:
		addr, err := decodeCustomEventAddress(val)
		if err != nil {
			return err
		}
		event.FromAddress = &addr
	case EventFieldToAddress:
		addr, err := decodeCustomEventAddress(val)
		if err != nil {
			return err
		}
		event.ToAddress = &addr
	case EventFieldTokenNumber:
		tokenNumber, err := decodeCustomEventUint256(val)
		if err != nil {
			return err
		}
		event.TokenNumber = tokenNumber
	case EventFieldQuantity:
		quantity, err := decodeCustomEventUint256(val)
		if err != nil {
			return err
		}
		event.Quantity = quantity
	default:
		return fmt.Errorf("unsupported event field %q", eventField)
	}
	return nil
}

func decodeCustomEventAddress(val any) (string, error) {
	switch typed := val.(type) {
	case common.Hash:
		return common.BytesToAddress(typed.Bytes()).Hex(), nil
	case []byte:
		if len(typed) < common.AddressLength {
			return "", fmt.Errorf("insufficient bytes for address")
		}
		return common.BytesToAddress(typed[len(typed)-common.AddressLength:]).Hex(), nil
	default:
		return "", fmt.Errorf("unsupported address value type %T", val)
	}
}

func decodeCustomEventUint256(val any) (string, error) {
	switch typed := val.(type) {
	case common.Hash:
		return new(big.Int).SetBytes(typed.Bytes()).String(), nil
	case []byte:
		return new(big.Int).SetBytes(typed).String(), nil
	default:
		return "", fmt.Errorf("unsupported uint256 value type %T", val)
	}
}

func (a *GenericAdapter) callMethod(
	ctx context.Context,
	contractAddress, tokenNumber string,
	call *MethodCall,
) ([]byte, error) {
	args, err := resolveParams(call.Params, tokenNumber)
	if err != nil {
		return nil, err
	}

	data, err := call.ABI.Pack(call.Method, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to pack %s: %w", call.Method, err)
	}

	contractAddr := common.HexToAddress(contractAddress)
	result, err := a.ethClient.CallContract(ctx, ethereum.CallMsg{
		To:   &contractAddr,
		Data: data,
	}, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to call %s: %w", call.Method, err)
	}
	if len(result) == 0 {
		return nil, fmt.Errorf("empty result from %s", call.Method)
	}

	return result, nil
}

func resolveParams(params []string, tokenNumber string) ([]any, error) {
	tokenID, ok := new(big.Int).SetString(tokenNumber, 10)
	if !ok {
		return nil, fmt.Errorf("invalid token number: %s", tokenNumber)
	}

	resolved := make([]any, len(params))
	for i, param := range params {
		switch param {
		case "${tokenId}":
			resolved[i] = tokenID
		default:
			return nil, fmt.Errorf("unsupported parameter placeholder: %s", param)
		}
	}

	return resolved, nil
}

func evaluateSuccess(result []byte, condition SuccessCondition) (bool, error) {
	switch condition {
	case SuccessAddressNonZero:
		if len(result) < common.AddressLength {
			return false, nil
		}
		addr := common.BytesToAddress(result[len(result)-common.AddressLength:])
		return addr != common.HexToAddress(domain.ETHEREUM_ZERO_ADDRESS), nil
	case SuccessNoRevert, "":
		return true, nil
	default:
		return false, fmt.Errorf("unsupported success condition: %s", condition)
	}
}

func unpackAddress(contractABI abi.ABI, method string, result []byte) (common.Address, error) {
	values, err := contractABI.Unpack(method, result)
	if err != nil {
		return common.Address{}, fmt.Errorf("failed to unpack %s result: %w", method, err)
	}
	if len(values) == 0 {
		return common.Address{}, fmt.Errorf("no return values from %s", method)
	}

	addr, ok := values[0].(common.Address)
	if !ok {
		return common.Address{}, fmt.Errorf("expected address from %s", method)
	}

	return addr, nil
}

var _ ContractAdapter = (*GenericAdapter)(nil)
