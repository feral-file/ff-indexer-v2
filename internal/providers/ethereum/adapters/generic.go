package adapters

import (
	"context"
	"fmt"
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
	standard         domain.ChainStandard
	existence        *MethodCall
	owner            *MethodCall
	metadata         ContractMetadataConfig
	ethClient        ethadapter.EthClient
	pagination       *helpers.PaginationHelper
	provenance       bool
	provenanceEvents []EventConfig
	chainID          domain.Chain
	blockProvider    block.BlockProvider
	clock            ethadapter.Clock
}

// NewGenericAdapter builds a GenericAdapter from parsed contract configuration.
func NewGenericAdapter(
	contractAddress string,
	standard domain.ChainStandard,
	existence, owner *MethodCall,
	metadata ContractMetadataConfig,
	ethClient ethadapter.EthClient,
	pagination *helpers.PaginationHelper,
	supportsProvenance bool,
	provenanceEvents []EventConfig,
	chainID domain.Chain,
	blockProvider block.BlockProvider,
	clock ethadapter.Clock,
) *GenericAdapter {
	return &GenericAdapter{
		contractAddress:  contractAddress,
		standard:         standard,
		existence:        existence,
		owner:            owner,
		metadata:         metadata,
		ethClient:        ethClient,
		pagination:       pagination,
		provenance:       supportsProvenance,
		provenanceEvents: provenanceEvents,
		chainID:          chainID,
		blockProvider:    blockProvider,
		clock:            clock,
	}
}

// GetStandard returns the configured token standard for this contract override.
func (a *GenericAdapter) GetStandard() domain.ChainStandard {
	return a.standard
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
		blockHash := vLog.BlockHash.Hex()

		// Get block timestamp
		timestamp, err := a.blockProvider.GetBlockTimestamp(ctx, vLog.BlockNumber)
		if err != nil {
			// Fallback to current time if timestamp fetch fails
			timestamp = a.clock.Now()
			logger.WarnCtx(ctx, "Failed to fetch block timestamp, using current time",
				zap.Uint64("blockNumber", vLog.BlockNumber),
				zap.Error(err))
		}

		event := &domain.BlockchainEvent{
			Chain:           a.chainID,
			ContractAddress: vLog.Address.Hex(),
			TxHash:          vLog.TxHash.Hex(),
			BlockNumber:     vLog.BlockNumber,
			BlockHash:       &blockHash,
			TxIndex:         uint64(vLog.TxIndex), //nolint:gosec,G115
			LogIndex:        uint64(vLog.Index),   //nolint:gosec,G115
			Timestamp:       timestamp,
		}

		parsed, err := a.ParseEvent(ctx, vLog, event)
		if err != nil {
			// Log error but continue processing
			logger.WarnCtx(ctx, "Failed to parse event log", zap.Error(err))
			continue
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

// GetTokensByOwner returns tokens owned by the address within the block range for this configured contract.
// Ownership tracking uses the configured standard field: erc721 uses last-transfer-wins, erc1155 uses net balance.
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
	ownerHash := common.BytesToHash(owner.Bytes())
	contractAddr := common.HexToAddress(a.contractAddress)

	queries := buildCustomOwnerTransferQueries(a.provenanceEvents, contractAddr, ownerHash, fromBlock, toBlock)
	logs, err := filterLogsInParallel(ctx, a.pagination, queries)
	if err != nil {
		return nil, fmt.Errorf("failed to query configured contract logs: %w", err)
	}

	logs = deduplicateLogs(logs)
	sortLogsAscending(logs)

	events := make([]domain.BlockchainEvent, 0, len(logs))
	for _, vLog := range logs {
		event := &domain.BlockchainEvent{
			Chain:           a.chainID,
			ContractAddress: a.contractAddress,
			BlockNumber:     vLog.BlockNumber,
			LogIndex:        uint64(vLog.Index), //nolint:gosec,G115
		}

		parsed, err := a.ParseEvent(ctx, vLog, event)
		if err != nil {
			logger.WarnCtx(ctx, "Failed to parse event in ownership scan",
				zap.String("contract", a.contractAddress),
				zap.Uint64("block", vLog.BlockNumber),
				zap.Uint("logIndex", vLog.Index),
				zap.Error(err))
			continue
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

	return trackOwnershipFromParsedEvents(a.chainID, a.standard, a.contractAddress, owner, events, blacklist), nil
}

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

// ParseEvent parses a configured legacy contract event into a blockchain event.
func (a *GenericAdapter) ParseEvent(ctx context.Context, vLog types.Log, event *domain.BlockchainEvent) (*domain.BlockchainEvent, error) {
	if len(vLog.Topics) == 0 {
		return nil, fmt.Errorf("event log has no topics")
	}

	for _, eventCfg := range a.provenanceEvents {
		expectedSignature := crypto.Keccak256Hash([]byte(eventCfg.Signature))
		if vLog.Topics[0] != expectedSignature {
			continue
		}
		return parseConfiguredEvent(ctx, vLog, eventCfg, a.standard, event)
	}

	return nil, ErrUnknownEvent
}

func parseConfiguredEvent(
	ctx context.Context,
	vLog types.Log,
	eventCfg EventConfig,
	standard domain.ChainStandard,
	event *domain.BlockchainEvent,
) (*domain.BlockchainEvent, error) {
	if event.Standard == "" {
		event.Standard = standard
	}
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
	for k, v := range indexedValues {
		allValues[k] = v
	}
	for k, v := range dataValues {
		allValues[k] = v
	}

	for paramName, eventField := range eventCfg.ParameterMappings {
		val, ok := allValues[paramName]
		if !ok {
			return nil, fmt.Errorf("parameter %s not found in event data", paramName)
		}
		if err := applyCustomEventField(event, eventField, val); err != nil {
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

	if event.EventType == domain.EventTypeTransfer {
		event.EventType = domain.TransferEventType(event.FromAddress, event.ToAddress)
	}

	logger.DebugCtx(ctx, "Parsed custom contract event",
		zap.String("contract", vLog.Address.Hex()),
		zap.String("signature", eventCfg.Signature),
		zap.String("eventType", string(event.EventType)),
		zap.String("tokenNumber", event.TokenNumber),
	)

	return event, nil
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
