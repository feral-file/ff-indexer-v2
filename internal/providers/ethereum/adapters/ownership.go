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

// deduplicateLogs removes duplicate logs using block number, transaction hash, and log index.
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

// sortLogsAscending orders logs by block number then log index.
func sortLogsAscending(logs []types.Log) {
	sort.Slice(logs, func(i, j int) bool {
		if logs[i].BlockNumber != logs[j].BlockNumber {
			return logs[i].BlockNumber < logs[j].BlockNumber
		}
		return logs[i].Index < logs[j].Index
	})
}

// filterLogsInParallel executes filter queries concurrently and merges the results.
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

type tokenOwnershipState struct {
	owned       bool
	blockNumber uint64
	logIndex    uint
	netBalance  *big.Int
}

func isNewerOwnershipState(existing *tokenOwnershipState, vLog types.Log) bool {
	return existing == nil ||
		vLog.BlockNumber > existing.blockNumber ||
		(vLog.BlockNumber == existing.blockNumber && vLog.Index > existing.logIndex)
}

func isTokenBlacklisted(blacklist registry.BlacklistRegistry, tokenCID domain.TokenCID) bool {
	return blacklist != nil && blacklist.IsTokenCIDBlacklisted(tokenCID)
}

// trackERC721OwnershipFromLogs applies last-transfer-wins ownership tracking for ERC721-style tokens.
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
// The configured standard field determines whether last-transfer-wins or balance accumulation is used.
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

func isOwnershipAffectingEvent(eventType domain.EventType) bool {
	switch eventType {
	case domain.EventTypeTransfer, domain.EventTypeMint, domain.EventTypeBurn:
		return true
	default:
		return false
	}
}

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
