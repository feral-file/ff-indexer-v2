package adapters

import (
	"context"
	"fmt"
	"math/big"
	"sort"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"go.uber.org/zap"

	"github.com/feral-file/ff-indexer-v2/internal/domain"
	"github.com/feral-file/ff-indexer-v2/internal/providers/ethereum/helpers"
	"github.com/feral-file/ff-indexer-v2/internal/registry"
)

// deduplicateLogs removes duplicate logs that may appear when querying with overlapping filter topics.
// Uses block number, transaction hash, and log index as the uniqueness key.
func deduplicateLogs(logs []types.Log) []types.Log {
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

// sortLogsAscending orders logs chronologically by block number, then by log index within the block.
// Required for correct ownership tracking since later events override earlier ones.
func sortLogsAscending(logs []types.Log) {
	sort.Slice(logs, func(i, j int) bool {
		if logs[i].BlockNumber != logs[j].BlockNumber {
			return logs[i].BlockNumber < logs[j].BlockNumber
		}
		return logs[i].Index < logs[j].Index
	})
}

// filterLogsInParallel executes multiple filter queries concurrently and merges the results.
// All queries must succeed or the function returns an error.
// Deduplication is the caller's responsibility after merging.
func filterLogsInParallel(
	ctx context.Context,
	pagination *helpers.PaginationHelper,
	queries []ethereum.FilterQuery,
) ([]types.Log, error) {
	if len(queries) == 0 {
		return nil, nil
	}

	type queryResult struct {
		logs []types.Log
		err  error
	}

	resultsCh := make(chan queryResult, len(queries))
	for _, q := range queries {
		go func(query ethereum.FilterQuery) {
			logs, err := pagination.FilterLogsWithPagination(ctx, query)
			resultsCh <- queryResult{logs: logs, err: err}
		}(q)
	}

	var allLogs []types.Log
	for range queries {
		result := <-resultsCh
		if result.err != nil {
			return nil, result.err
		}
		allLogs = append(allLogs, result.logs...)
	}

	return allLogs, nil
}

// tokenOwnershipState tracks the current ownership status and last-seen block for a token.
// For ERC721, owned is a boolean derived from the last transfer's "to" address.
// For ERC1155, owned is true when netBalance > 0 after accumulating all transfers.
type tokenOwnershipState struct {
	owned       bool
	blockNumber uint64
	logIndex    uint
	netBalance  *big.Int
}

// isNewerOwnershipState returns true if vLog occurred after the existing state.
// Uses block number as primary sort key, log index as tiebreaker.
func isNewerOwnershipState(existing *tokenOwnershipState, vLog types.Log) bool {
	return existing == nil ||
		vLog.BlockNumber > existing.blockNumber ||
		(vLog.BlockNumber == existing.blockNumber && vLog.Index > existing.logIndex)
}

// isTokenBlacklisted checks if the token is in the blacklist registry.
// Returns false if blacklist is nil (no filtering).
func isTokenBlacklisted(blacklist registry.BlacklistRegistry, tokenCID domain.TokenCID) bool {
	return blacklist != nil && blacklist.IsTokenCIDBlacklisted(tokenCID)
}

// trackERC721OwnershipFromLogs applies last-transfer-wins ownership tracking for ERC721-style tokens.
//
// For each token, only the most recent Transfer event determines ownership. A token is owned if
// the latest transfer's "to" address matches the owner. Transfers are compared by (block, logIndex).
//
// Skips logs that don't involve the owner (neither from nor to) and filters out blacklisted tokens.
func trackERC721OwnershipFromLogs(
	chainID domain.Chain,
	owner common.Address,
	logs []types.Log,
	blacklist registry.BlacklistRegistry,
) []domain.TokenWithBlock {
	tokenMap := make(map[domain.TokenCID]*tokenOwnershipState)

	for _, vLog := range logs {
		if len(vLog.Topics) != 4 || vLog.Topics[0] != helpers.TransferEventSignature {
			continue
		}

		fromAddr := common.BytesToAddress(vLog.Topics[1].Bytes())
		toAddr := common.BytesToAddress(vLog.Topics[2].Bytes())
		if fromAddr != owner && toAddr != owner {
			continue
		}

		tokenID := new(big.Int).SetBytes(vLog.Topics[3].Bytes())
		tokenCID := domain.NewTokenCID(chainID, domain.StandardERC721, vLog.Address.Hex(), tokenID.String())
		if isTokenBlacklisted(blacklist, tokenCID) {
			continue
		}

		existing := tokenMap[tokenCID]
		if !isNewerOwnershipState(existing, vLog) {
			continue
		}

		tokenMap[tokenCID] = &tokenOwnershipState{
			owned:       toAddr == owner,
			blockNumber: vLog.BlockNumber,
			logIndex:    vLog.Index,
		}
	}

	return ownedTokensFromState(tokenMap)
}

// trackERC1155OwnershipFromLogs applies net-balance ownership tracking for ERC1155-style tokens.
//
// For each token, accumulates the net balance by processing TransferSingle and TransferBatch events.
// A token is owned if the net balance > 0 after all transfers. Balance increases when the owner
// receives tokens, decreases when the owner sends tokens.
//
// Skips logs that don't involve the owner and filters out blacklisted tokens.
func trackERC1155OwnershipFromLogs(
	chainID domain.Chain,
	owner common.Address,
	logs []types.Log,
	blacklist registry.BlacklistRegistry,
) []domain.TokenWithBlock {
	tokenMap := make(map[domain.TokenCID]*tokenOwnershipState)

	for _, vLog := range logs {
		if len(vLog.Topics) < 1 {
			continue
		}

		switch vLog.Topics[0] {
		case helpers.ERC1155TransferSingleEventSignature:
			applyERC1155SingleTransfer(chainID, owner, vLog, blacklist, tokenMap)
		case helpers.ERC1155TransferBatchEventSignature:
			applyERC1155BatchTransfer(chainID, owner, vLog, blacklist, tokenMap)
		}
	}

	return ownedTokensFromState(tokenMap)
}

// applyERC1155SingleTransfer processes an ERC1155 TransferSingle event and updates token ownership state.
// Extracts tokenID and amount from event data, then adjusts the owner's balance.
func applyERC1155SingleTransfer(
	chainID domain.Chain,
	owner common.Address,
	vLog types.Log,
	blacklist registry.BlacklistRegistry,
	tokenMap map[domain.TokenCID]*tokenOwnershipState,
) {
	if len(vLog.Topics) != 4 || len(vLog.Data) < 64 {
		return
	}

	fromAddr := common.BytesToAddress(vLog.Topics[2].Bytes())
	toAddr := common.BytesToAddress(vLog.Topics[3].Bytes())
	if fromAddr != owner && toAddr != owner {
		return
	}

	tokenID := new(big.Int).SetBytes(vLog.Data[0:32])
	amount := new(big.Int).SetBytes(vLog.Data[32:64])
	tokenCID := domain.NewTokenCID(chainID, domain.StandardERC1155, vLog.Address.Hex(), tokenID.String())
	if isTokenBlacklisted(blacklist, tokenCID) {
		return
	}

	applyERC1155BalanceChange(tokenMap, tokenCID, owner, fromAddr, toAddr, amount, vLog)
}

// applyERC1155BatchTransfer processes an ERC1155 TransferBatch event and updates token ownership state.
// Decodes ABI-encoded arrays of token IDs and amounts from event data, then adjusts the owner's balance
// for each token in the batch.
func applyERC1155BatchTransfer(
	chainID domain.Chain,
	owner common.Address,
	vLog types.Log,
	blacklist registry.BlacklistRegistry,
	tokenMap map[domain.TokenCID]*tokenOwnershipState,
) {
	if len(vLog.Topics) != 4 || len(vLog.Data) < 128 {
		return
	}

	fromAddr := common.BytesToAddress(vLog.Topics[2].Bytes())
	toAddr := common.BytesToAddress(vLog.Topics[3].Bytes())
	if fromAddr != owner && toAddr != owner {
		return
	}

	idsOffset := new(big.Int).SetBytes(vLog.Data[0:32]).Uint64()
	valuesOffset := new(big.Int).SetBytes(vLog.Data[32:64]).Uint64()
	if idsOffset+32 > uint64(len(vLog.Data)) || valuesOffset+32 > uint64(len(vLog.Data)) {
		return
	}

	idsLength := new(big.Int).SetBytes(vLog.Data[idsOffset : idsOffset+32]).Uint64()
	valuesLength := new(big.Int).SetBytes(vLog.Data[valuesOffset : valuesOffset+32]).Uint64()
	if idsLength != valuesLength {
		return
	}

	idsStart := idsOffset + 32
	valuesStart := valuesOffset + 32
	if idsStart+idsLength*32 > uint64(len(vLog.Data)) || valuesStart+valuesLength*32 > uint64(len(vLog.Data)) {
		return
	}

	for j := range idsLength {
		idStart := idsStart + j*32
		valueStart := valuesStart + j*32

		tokenID := new(big.Int).SetBytes(vLog.Data[idStart : idStart+32])
		amount := new(big.Int).SetBytes(vLog.Data[valueStart : valueStart+32])
		tokenCID := domain.NewTokenCID(chainID, domain.StandardERC1155, vLog.Address.Hex(), tokenID.String())
		if isTokenBlacklisted(blacklist, tokenCID) {
			continue
		}

		applyERC1155BalanceChange(tokenMap, tokenCID, owner, fromAddr, toAddr, amount, vLog)
	}
}

// applyERC1155BalanceChange updates the net balance for a token based on a transfer event.
// Adds amount if owner is the recipient, subtracts if owner is the sender.
// Updates owned status to true if netBalance > 0, false otherwise.
// Records the block and log index if this event is newer than the existing state.
func applyERC1155BalanceChange(
	tokenMap map[domain.TokenCID]*tokenOwnershipState,
	tokenCID domain.TokenCID,
	owner, fromAddr, toAddr common.Address,
	amount *big.Int,
	vLog types.Log,
) {
	existing := tokenMap[tokenCID]
	if existing == nil {
		existing = &tokenOwnershipState{netBalance: big.NewInt(0)}
		tokenMap[tokenCID] = existing
	}

	if toAddr == owner {
		existing.netBalance.Add(existing.netBalance, amount)
	}
	if fromAddr == owner {
		existing.netBalance.Sub(existing.netBalance, amount)
	}

	existing.owned = existing.netBalance.Sign() > 0
	if isNewerOwnershipState(existing, vLog) {
		existing.blockNumber = vLog.BlockNumber
		existing.logIndex = vLog.Index
	}
}

// ownedTokensFromState extracts owned tokens from the ownership state map.
// Returns only tokens where state.owned is true, with their last-seen block numbers.
func ownedTokensFromState(tokenMap map[domain.TokenCID]*tokenOwnershipState) []domain.TokenWithBlock {
	result := make([]domain.TokenWithBlock, 0, len(tokenMap))
	for tokenCID, state := range tokenMap {
		if state.owned {
			result = append(result, domain.TokenWithBlock{
				TokenCID:    tokenCID,
				BlockNumber: state.blockNumber,
			})
		}
	}
	return result
}

// trackOwnershipFromParsedEvents applies ownership tracking to parsed blockchain events.
//
// The standard field (from contracts.json) determines which tracking logic to use:
// - ERC721: last-transfer-wins
// - ERC1155: balance-accumulation
//
// This function is used by GenericAdapter after parsing custom contract events into standardized
// domain.BlockchainEvent structs. The caller is responsible for sorting events chronologically
// before calling this function.
func trackOwnershipFromParsedEvents(
	chainID domain.Chain,
	standard domain.ChainStandard,
	contractAddress string,
	owner common.Address,
	events []domain.BlockchainEvent,
	blacklist registry.BlacklistRegistry,
) []domain.TokenWithBlock {
	switch standard {
	case domain.StandardERC721:
		return trackERC721OwnershipFromEvents(chainID, contractAddress, owner, events, blacklist)
	case domain.StandardERC1155:
		return trackERC1155OwnershipFromEvents(chainID, contractAddress, owner, events, blacklist)
	default:
		return nil
	}
}

// trackERC721OwnershipFromEvents applies last-transfer-wins tracking to parsed events.
// Used by GenericAdapter for contracts configured with standard="erc721".
// Only processes transfer/mint/burn events that involve the owner.
func trackERC721OwnershipFromEvents(
	chainID domain.Chain,
	contractAddress string,
	owner common.Address,
	events []domain.BlockchainEvent,
	blacklist registry.BlacklistRegistry,
) []domain.TokenWithBlock {
	tokenMap := make(map[domain.TokenCID]*tokenOwnershipState)

	for _, event := range events {
		if !isOwnershipAffectingEvent(event.EventType) {
			continue
		}

		_, toAddr, ok := ownershipAddressesFromEvent(event, owner)
		if !ok {
			continue
		}

		tokenCID := domain.NewTokenCID(chainID, domain.StandardERC721, contractAddress, event.TokenNumber)
		if isTokenBlacklisted(blacklist, tokenCID) {
			continue
		}

		vLog := types.Log{BlockNumber: event.BlockNumber, Index: uint(event.LogIndex)} //nolint:gosec,G115
		existing := tokenMap[tokenCID]
		if !isNewerOwnershipState(existing, vLog) {
			continue
		}

		tokenMap[tokenCID] = &tokenOwnershipState{
			owned:       toAddr == owner,
			blockNumber: event.BlockNumber,
			logIndex:    uint(event.LogIndex), //nolint:gosec,G115
		}
	}

	return ownedTokensFromState(tokenMap)
}

// trackERC1155OwnershipFromEvents applies balance-accumulation tracking to parsed events.
// Used by GenericAdapter for contracts configured with standard="erc1155".
// Only processes transfer/mint/burn events that involve the owner.
// Logs a warning if quantity parsing fails, defaulting to 1 for that event.
func trackERC1155OwnershipFromEvents(
	chainID domain.Chain,
	contractAddress string,
	owner common.Address,
	events []domain.BlockchainEvent,
	blacklist registry.BlacklistRegistry,
) []domain.TokenWithBlock {
	tokenMap := make(map[domain.TokenCID]*tokenOwnershipState)

	for _, event := range events {
		if !isOwnershipAffectingEvent(event.EventType) {
			continue
		}

		fromAddr, toAddr, ok := ownershipAddressesFromEvent(event, owner)
		if !ok {
			continue
		}

		tokenCID := domain.NewTokenCID(chainID, domain.StandardERC1155, contractAddress, event.TokenNumber)
		if isTokenBlacklisted(blacklist, tokenCID) {
			continue
		}

		amount, ok := new(big.Int).SetString(event.Quantity, 10)
		if !ok {
			// This should be rare with proper validation, but log for debugging
			zap.L().Warn("Failed to parse quantity in ERC1155 ownership tracking, defaulting to 1",
				zap.String("chain", string(chainID)),
				zap.String("contract", contractAddress),
				zap.String("tokenNumber", event.TokenNumber),
				zap.String("quantity", event.Quantity),
				zap.Uint64("block", event.BlockNumber))
			amount = big.NewInt(1)
		}

		vLog := types.Log{BlockNumber: event.BlockNumber, Index: uint(event.LogIndex)} //nolint:gosec,G115
		applyERC1155BalanceChange(tokenMap, tokenCID, owner, fromAddr, toAddr, amount, vLog)
	}

	return ownedTokensFromState(tokenMap)
}

// isOwnershipAffectingEvent returns true if the event type can change token ownership.
// Only transfer, mint, and burn events affect ownership; metadata updates do not.
func isOwnershipAffectingEvent(eventType domain.EventType) bool {
	switch eventType {
	case domain.EventTypeTransfer, domain.EventTypeMint, domain.EventTypeBurn:
		return true
	default:
		return false
	}
}

// ownershipAddressesFromEvent extracts from and to addresses from a parsed event.
// Returns (from, to, true) if the event involves the owner as sender or recipient.
// Returns (zero, zero, false) if neither address matches the owner.
// Handles nil addresses (for mint: from=nil, for burn: to=nil).
func ownershipAddressesFromEvent(event domain.BlockchainEvent, owner common.Address) (common.Address, common.Address, bool) {
	var fromAddr common.Address
	if event.FromAddress != nil {
		fromAddr = common.HexToAddress(*event.FromAddress)
	}

	var toAddr common.Address
	if event.ToAddress != nil {
		toAddr = common.HexToAddress(*event.ToAddress)
	}

	if fromAddr != owner && toAddr != owner {
		return common.Address{}, common.Address{}, false
	}

	return fromAddr, toAddr, true
}

// replayBalancesFromEvents computes net holder balances from ownership-affecting parsed events.
func replayBalancesFromEvents(events []domain.BlockchainEvent) map[string]string {
	balances := make(map[string]*big.Int)

	for _, event := range events {
		if !isOwnershipAffectingEvent(event.EventType) {
			continue
		}

		amount, ok := eventQuantityAmount(event)
		if !ok {
			continue
		}

		if event.FromAddress != nil && *event.FromAddress != "" && *event.FromAddress != domain.ETHEREUM_ZERO_ADDRESS {
			applyAddressBalanceDelta(balances, *event.FromAddress, new(big.Int).Neg(amount))
		}
		if event.ToAddress != nil && *event.ToAddress != "" && *event.ToAddress != domain.ETHEREUM_ZERO_ADDRESS {
			applyAddressBalanceDelta(balances, *event.ToAddress, amount)
		}
	}

	result := make(map[string]string, len(balances))
	for addr, balance := range balances {
		if balance.Sign() > 0 {
			result[addr] = balance.String()
		}
	}

	return result
}

// replayOwnerBalanceFromEvents computes net balance for one owner from parsed events.
func replayOwnerBalanceFromEvents(ownerAddress string, events []domain.BlockchainEvent) string {
	owner := common.HexToAddress(ownerAddress)
	balance := big.NewInt(0)

	for _, event := range events {
		if !isOwnershipAffectingEvent(event.EventType) {
			continue
		}

		fromAddr, toAddr, ok := ownershipAddressesFromEvent(event, owner)
		if !ok {
			continue
		}

		amount, ok := eventQuantityAmount(event)
		if !ok {
			continue
		}

		if toAddr == owner {
			balance.Add(balance, amount)
		}
		if fromAddr == owner {
			balance.Sub(balance, amount)
		}
	}

	if balance.Sign() <= 0 {
		return "0"
	}

	return balance.String()
}

// filterOwnerEvents returns ownership-affecting events where the owner is sender or recipient.
func filterOwnerEvents(events []domain.BlockchainEvent, ownerAddress string) []domain.BlockchainEvent {
	owner := common.HexToAddress(ownerAddress)
	filtered := make([]domain.BlockchainEvent, 0, len(events))

	for _, event := range events {
		if !isOwnershipAffectingEvent(event.EventType) {
			continue
		}
		if _, _, ok := ownershipAddressesFromEvent(event, owner); ok {
			filtered = append(filtered, event)
		}
	}

	return filtered
}

func eventQuantityAmount(event domain.BlockchainEvent) (*big.Int, bool) {
	quantity := event.Quantity
	if quantity == "" {
		quantity = "1"
	}

	amount, ok := new(big.Int).SetString(quantity, 10)
	if !ok {
		return nil, false
	}

	return amount, true
}

func applyAddressBalanceDelta(balances map[string]*big.Int, address string, delta *big.Int) {
	normalized := domain.NormalizeAddress(address)
	current, ok := balances[normalized]
	if !ok {
		current = big.NewInt(0)
		balances[normalized] = current
	}
	current.Add(current, delta)
}
