package adapters

import (
	"context"
	"fmt"
	"math/big"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"

	"github.com/feral-file/ff-indexer-v2/internal/domain"
	"github.com/feral-file/ff-indexer-v2/internal/providers/ethereum/helpers"
)

// punkBoughtEventSignature is the topic0 for CryptoPunks PunkBought marketplace sales.
var punkBoughtEventSignature = crypto.Keccak256Hash([]byte("PunkBought(uint256,uint256,address,address)"))

// hasPunkBoughtEvent reports whether this adapter indexes PunkBought provenance events.
func (a *GenericAdapter) hasPunkBoughtEvent() bool {
	for _, eventCfg := range a.provenanceEvents {
		if crypto.Keccak256Hash([]byte(eventCfg.Signature)) == punkBoughtEventSignature {
			return true
		}
	}
	return false
}

// needsPunkBoughtRepair reports whether a parsed ownership event likely came from a corrupted
// acceptBidForPunk PunkBought log (zero buyer/value) that must be repaired from the same-tx
// internal CryptoPunks Transfer(seller, buyer, 1) log.
func needsPunkBoughtRepair(event domain.BlockchainEvent) bool {
	if event.EventType != domain.EventTypeTransfer && event.EventType != domain.EventTypeBurn {
		return false
	}
	if event.FromAddress == nil || *event.FromAddress == "" || *event.FromAddress == domain.ETHEREUM_ZERO_ADDRESS {
		return false
	}
	return event.ToAddress == nil || *event.ToAddress == "" || *event.ToAddress == domain.ETHEREUM_ZERO_ADDRESS
}

// repairBrokenPunkBoughtEvents fetches transaction receipts for corrupted PunkBought events
// and restores buyer/recipient addresses from the companion internal Transfer log.
func (a *GenericAdapter) repairBrokenPunkBoughtEvents(
	ctx context.Context,
	events []domain.BlockchainEvent,
) ([]domain.BlockchainEvent, error) {
	if !a.hasPunkBoughtEvent() || a.ethClient == nil || len(events) == 0 {
		return events, nil
	}

	indicesByTx := make(map[string][]int)
	for i := range events {
		if needsPunkBoughtRepair(events[i]) {
			indicesByTx[events[i].TxHash] = append(indicesByTx[events[i].TxHash], i)
		}
	}
	if len(indicesByTx) == 0 {
		return events, nil
	}

	contractAddr := common.HexToAddress(a.contractAddress)
	for txHash, indices := range indicesByTx {
		receipt, err := a.ethClient.TransactionReceipt(ctx, common.HexToHash(txHash))
		if err != nil {
			return nil, fmt.Errorf("fetch receipt for PunkBought repair tx %s: %w", txHash, err)
		}
		receiptLogs := receiptLogsAsValues(receipt.Logs)
		for _, i := range indices {
			if !repairPunkBoughtFromReceiptLogs(receiptLogs, &events[i], contractAddr) {
				return nil, fmt.Errorf("repair PunkBought at block %d tx %s: missing internal Transfer buyer",
					events[i].BlockNumber, txHash)
			}
		}
	}

	return events, nil
}

// repairPunkBoughtFromReceiptLogs restores buyer/recipient on a corrupted PunkBought event
// using CryptoPunks' internal ERC20-style Transfer(seller, buyer, 1) log from the same tx.
//
// Reason: acceptBidForPunk clears punkBids storage before emitting PunkBought under Solidity
// 0.4.8, so the emitted toAddress/value can be zero even though ownership transferred.
func repairPunkBoughtFromReceiptLogs(
	receiptLogs []types.Log,
	event *domain.BlockchainEvent,
	contractAddr common.Address,
) bool {
	if event == nil || event.FromAddress == nil {
		return false
	}

	buyer, ok := cryptoPunksInternalTransferBuyer(receiptLogs, contractAddr, common.HexToAddress(*event.FromAddress))
	if !ok {
		return false
	}

	buyerHex := buyer.Hex()
	event.ToAddress = &buyerHex
	event.Quantity = "1"
	event.EventType = domain.EventTypeTransfer
	return true
}

// cryptoPunksInternalTransferBuyer finds the non-zero recipient of CryptoPunks' internal
// 3-topic Transfer log emitted from the seller during marketplace/bid-accept flows.
func cryptoPunksInternalTransferBuyer(
	logs []types.Log,
	contractAddr common.Address,
	from common.Address,
) (common.Address, bool) {
	fromTopic := common.BytesToHash(from.Bytes())
	for _, vLog := range logs {
		if vLog.Address != contractAddr {
			continue
		}
		if len(vLog.Topics) != 3 || vLog.Topics[0] != helpers.TransferEventSignature {
			continue
		}
		if vLog.Topics[1] != fromTopic {
			continue
		}

		to := common.BytesToAddress(vLog.Topics[2].Bytes())
		if to == (common.Address{}) {
			continue
		}
		if len(vLog.Data) >= 32 {
			value := new(big.Int).SetBytes(vLog.Data[:32])
			if value.Sign() != 0 && value.Cmp(big.NewInt(1)) != 0 {
				continue
			}
		}

		return to, true
	}

	return common.Address{}, false
}

func receiptLogsAsValues(logs []*types.Log) []types.Log {
	if len(logs) == 0 {
		return nil
	}
	out := make([]types.Log, 0, len(logs))
	for _, vLog := range logs {
		if vLog == nil {
			continue
		}
		out = append(out, *vLog)
	}
	return out
}
