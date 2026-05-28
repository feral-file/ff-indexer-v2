package adapters

import (
	"context"
	"fmt"
	"math/big"
	"sort"
	"strings"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"

	"github.com/feral-file/ff-indexer-v2/internal/domain"
	"github.com/feral-file/ff-indexer-v2/internal/providers/ethereum/helpers"
	"github.com/feral-file/ff-indexer-v2/internal/registry"
)

// OwnerReplayParams configures a unified cross-standard ownership replay with a global held-token limit.
type OwnerReplayParams struct {
	ChainID                     domain.Chain
	Owner                       common.Address
	Logs                        []types.Log
	Blacklist                   registry.BlacklistRegistry
	Limit                       int
	Order                       domain.BlockScanOrder
	RequestedFromBlock          uint64
	RequestedToBlock            uint64
	ConfiguredContractStandards map[string]domain.ChainStandard
	ParseLog                    func(ctx context.Context, vLog types.Log) (*domain.BlockchainEvent, error)
}

// OwnerReplayResult is the output of unified ownership replay.
type OwnerReplayResult struct {
	Tokens             []domain.TokenWithBlock
	EffectiveFromBlock uint64
	EffectiveToBlock   uint64
}

// ReplayOwnerTokensWithLimit replays merged owner-scoped logs in scan order and stops once the
// global held-token count reaches limit at a block boundary.
//
// Reason: Single interleaved replay across standards with a global heldCount matches pre-adapter-centric
// client semantics. For ASC scans, ownership reflects state as of the cutoff block. For DESC scans,
// ownership reflects last-transfer-wins from newest events first (newest-N-active-tokens semantics).
//
// Trade-offs: Parses configured-contract logs via ParseLog (including timestamp lookup). Standard
// ERC-721/1155 paths use raw log decoding only. Logs are replayed in the requested scan order
// (ascending or descending), matching legacy main client behavior for limit cutoffs.
func ReplayOwnerTokensWithLimit(ctx context.Context, params OwnerReplayParams) (OwnerReplayResult, error) {
	if params.Limit <= 0 {
		return OwnerReplayResult{}, fmt.Errorf("limit must be > 0")
	}

	logs := append([]types.Log(nil), params.Logs...)
	sortLogsByOrder(logs, params.Order)

	tokenMap := make(map[domain.TokenCID]*tokenOwnershipState)
	heldCount := 0
	effectiveFromBlock := params.RequestedFromBlock
	effectiveToBlock := params.RequestedToBlock

	effectiveBoundary := &effectiveToBlock
	if params.Order.Desc() {
		effectiveBoundary = &effectiveFromBlock
	}

	for logIndex := 0; logIndex < len(logs); {
		blockNumber := logs[logIndex].BlockNumber

		for logIndex < len(logs) && logs[logIndex].BlockNumber == blockNumber {
			vLog := logs[logIndex]
			logIndex++

			if err := applyOwnerLog(ctx, params, vLog, tokenMap, &heldCount); err != nil {
				return OwnerReplayResult{}, err
			}
		}

		if heldCount >= params.Limit {
			if logIndex < len(logs) {
				*effectiveBoundary = blockNumber
				break
			}
		}
	}

	return OwnerReplayResult{
		Tokens:             ownedTokensFromState(tokenMap),
		EffectiveFromBlock: effectiveFromBlock,
		EffectiveToBlock:   effectiveToBlock,
	}, nil
}

func sortLogsByOrder(logs []types.Log, order domain.BlockScanOrder) {
	if order.Desc() {
		sort.Slice(logs, func(i, j int) bool {
			if logs[i].BlockNumber != logs[j].BlockNumber {
				return logs[i].BlockNumber > logs[j].BlockNumber
			}
			return logs[i].Index > logs[j].Index
		})
		return
	}
	sortLogsAscending(logs)
}

func applyOwnerLog(
	ctx context.Context,
	params OwnerReplayParams,
	vLog types.Log,
	tokenMap map[domain.TokenCID]*tokenOwnershipState,
	heldCount *int,
) error {
	if len(vLog.Topics) < 1 {
		return nil
	}

	if _, ok := params.ConfiguredContractStandards[strings.ToLower(vLog.Address.Hex())]; ok {
		return applyOwnerConfiguredContractLog(ctx, params, vLog, tokenMap, heldCount)
	}

	switch vLog.Topics[0] {
	case helpers.TransferEventSignature:
		if len(vLog.Topics) == 4 {
			applyOwnerERC721Transfer(params, vLog, tokenMap, heldCount)
		}
		return nil
	case helpers.ERC1155TransferSingleEventSignature:
		applyOwnerERC1155SingleTransfer(params, vLog, tokenMap, heldCount)
		return nil
	case helpers.ERC1155TransferBatchEventSignature:
		applyOwnerERC1155BatchTransfer(params, vLog, tokenMap, heldCount)
		return nil
	default:
		return applyOwnerConfiguredContractLog(ctx, params, vLog, tokenMap, heldCount)
	}
}

func applyOwnerERC721Transfer(
	params OwnerReplayParams,
	vLog types.Log,
	tokenMap map[domain.TokenCID]*tokenOwnershipState,
	heldCount *int,
) {
	fromAddr := common.BytesToAddress(vLog.Topics[1].Bytes())
	toAddr := common.BytesToAddress(vLog.Topics[2].Bytes())
	if fromAddr != params.Owner && toAddr != params.Owner {
		return
	}

	tokenID := new(big.Int).SetBytes(vLog.Topics[3].Bytes())
	tokenCID := domain.NewTokenCID(params.ChainID, domain.StandardERC721, vLog.Address.Hex(), tokenID.String())
	if isTokenBlacklisted(params.Blacklist, tokenCID) {
		return
	}

	existing := tokenMap[tokenCID]
	if !isNewerOwnershipState(existing, vLog) {
		return
	}

	prevOwned := existing != nil && existing.owned
	newOwned := toAddr == params.Owner
	tokenMap[tokenCID] = &tokenOwnershipState{
		owned:       newOwned,
		blockNumber: vLog.BlockNumber,
		logIndex:    vLog.Index,
	}
	updateHeldCount(prevOwned, newOwned, heldCount)
}

func applyOwnerERC1155SingleTransfer(
	params OwnerReplayParams,
	vLog types.Log,
	tokenMap map[domain.TokenCID]*tokenOwnershipState,
	heldCount *int,
) {
	if len(vLog.Topics) != 4 || len(vLog.Data) < 64 {
		return
	}

	fromAddr := common.BytesToAddress(vLog.Topics[2].Bytes())
	toAddr := common.BytesToAddress(vLog.Topics[3].Bytes())
	if fromAddr != params.Owner && toAddr != params.Owner {
		return
	}

	tokenID := new(big.Int).SetBytes(vLog.Data[0:32])
	amount := new(big.Int).SetBytes(vLog.Data[32:64])
	tokenCID := domain.NewTokenCID(params.ChainID, domain.StandardERC1155, vLog.Address.Hex(), tokenID.String())
	if isTokenBlacklisted(params.Blacklist, tokenCID) {
		return
	}

	applyOwnerERC1155BalanceChange(params.Owner, fromAddr, toAddr, amount, vLog, tokenMap, tokenCID, heldCount)
}

func applyOwnerERC1155BatchTransfer(
	params OwnerReplayParams,
	vLog types.Log,
	tokenMap map[domain.TokenCID]*tokenOwnershipState,
	heldCount *int,
) {
	if len(vLog.Topics) != 4 || len(vLog.Data) < 128 {
		return
	}

	fromAddr := common.BytesToAddress(vLog.Topics[2].Bytes())
	toAddr := common.BytesToAddress(vLog.Topics[3].Bytes())
	if fromAddr != params.Owner && toAddr != params.Owner {
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
		tokenCID := domain.NewTokenCID(params.ChainID, domain.StandardERC1155, vLog.Address.Hex(), tokenID.String())
		if isTokenBlacklisted(params.Blacklist, tokenCID) {
			continue
		}

		applyOwnerERC1155BalanceChange(params.Owner, fromAddr, toAddr, amount, vLog, tokenMap, tokenCID, heldCount)
	}
}

func applyOwnerERC1155BalanceChange(
	owner, fromAddr, toAddr common.Address,
	amount *big.Int,
	vLog types.Log,
	tokenMap map[domain.TokenCID]*tokenOwnershipState,
	tokenCID domain.TokenCID,
	heldCount *int,
) {
	existing := tokenMap[tokenCID]
	if existing == nil {
		existing = &tokenOwnershipState{netBalance: big.NewInt(0)}
		tokenMap[tokenCID] = existing
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
	updateHeldCount(prevOwned, newOwned, heldCount)

	if isNewerOwnershipState(existing, vLog) {
		existing.blockNumber = vLog.BlockNumber
		existing.logIndex = vLog.Index
	}
}

func applyOwnerConfiguredContractLog(
	ctx context.Context,
	params OwnerReplayParams,
	vLog types.Log,
	tokenMap map[domain.TokenCID]*tokenOwnershipState,
	heldCount *int,
) error {
	if params.ParseLog == nil || len(params.ConfiguredContractStandards) == 0 {
		return nil
	}

	contractAddr := strings.ToLower(vLog.Address.Hex())
	standard, ok := params.ConfiguredContractStandards[contractAddr]
	if !ok {
		return nil
	}

	parsed, err := params.ParseLog(ctx, vLog)
	if err != nil {
		return fmt.Errorf("parse configured contract log at block %d index %d: %w", vLog.BlockNumber, vLog.Index, err)
	}
	if parsed == nil || !isOwnershipAffectingEvent(parsed.EventType) {
		return nil
	}

	fromAddr, toAddr, ok := ownershipAddressesFromEvent(*parsed, params.Owner)
	if !ok {
		return nil
	}

	tokenCID := domain.NewTokenCID(params.ChainID, standard, vLog.Address.Hex(), parsed.TokenNumber)
	if isTokenBlacklisted(params.Blacklist, tokenCID) {
		return nil
	}

	switch standard {
	case domain.StandardERC721:
		applyOwnerParsedERC721Transfer(params.Owner, toAddr, *parsed, tokenMap, tokenCID, heldCount)
	case domain.StandardERC1155:
		amount, ok := eventQuantityAmount(*parsed)
		if !ok {
			return fmt.Errorf("parse quantity for configured contract log at block %d: invalid quantity %q",
				vLog.BlockNumber, parsed.Quantity)
		}
		syntheticLog := types.Log{BlockNumber: parsed.BlockNumber, Index: uint(parsed.LogIndex)} //nolint:gosec,G115
		applyOwnerERC1155BalanceChange(params.Owner, fromAddr, toAddr, amount, syntheticLog, tokenMap, tokenCID, heldCount)
	}

	return nil
}

func applyOwnerParsedERC721Transfer(
	owner, toAddr common.Address,
	event domain.BlockchainEvent,
	tokenMap map[domain.TokenCID]*tokenOwnershipState,
	tokenCID domain.TokenCID,
	heldCount *int,
) {
	vLog := types.Log{BlockNumber: event.BlockNumber, Index: uint(event.LogIndex)} //nolint:gosec,G115
	existing := tokenMap[tokenCID]
	if !isNewerOwnershipState(existing, vLog) {
		return
	}

	prevOwned := existing != nil && existing.owned
	newOwned := toAddr == owner
	tokenMap[tokenCID] = &tokenOwnershipState{
		owned:       newOwned,
		blockNumber: event.BlockNumber,
		logIndex:    uint(event.LogIndex), //nolint:gosec,G115
	}
	updateHeldCount(prevOwned, newOwned, heldCount)
}

func updateHeldCount(prevOwned, newOwned bool, heldCount *int) {
	if prevOwned == newOwned {
		return
	}
	if newOwned {
		*heldCount++
		return
	}
	*heldCount--
}
