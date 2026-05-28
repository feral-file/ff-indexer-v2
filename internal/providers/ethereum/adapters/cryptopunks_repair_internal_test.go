package adapters

import (
	"math/big"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/stretchr/testify/require"

	"github.com/feral-file/ff-indexer-v2/internal/domain"
	"github.com/feral-file/ff-indexer-v2/internal/providers/ethereum/helpers"
)

func TestRepairPunkBoughtFromReceiptLogs_AcceptBidFixture(t *testing.T) {
	t.Parallel()

	const contractAddr = "0xb47e3cd837ddf8e4c57f05d70ab865de6e193bbb"
	seller := common.HexToAddress("0xc480fb0ebea2591470f571436926785be5ebcd22")
	buyer := common.HexToAddress("0x9df6a358688ccdc2a955568a05aacac7a998a319")
	punkBoughtSig := crypto.Keccak256Hash([]byte("PunkBought(uint256,uint256,address,address)"))

	receiptLogs := []types.Log{
		{
			Address: common.HexToAddress(contractAddr),
			Topics: []common.Hash{
				helpers.TransferEventSignature,
				common.BytesToHash(seller.Bytes()),
				common.BytesToHash(buyer.Bytes()),
			},
			Data: common.LeftPadBytes(big.NewInt(1).Bytes(), 32),
		},
		{
			Address: common.HexToAddress(contractAddr),
			Topics: []common.Hash{
				punkBoughtSig,
				common.BigToHash(big.NewInt(6690)),
				common.BytesToHash(seller.Bytes()),
				{},
			},
			Data: common.Hash{}.Bytes(),
		},
	}

	from := seller.Hex()
	event := domain.BlockchainEvent{
		EventType:   domain.EventTypeTransfer,
		FromAddress: &from,
		ToAddress:   ptr(domain.ETHEREUM_ZERO_ADDRESS),
		TokenNumber: "6690",
		Quantity:    "0",
	}

	repaired := repairPunkBoughtFromReceiptLogs(receiptLogs, &event, common.HexToAddress(contractAddr))
	require.True(t, repaired)
	require.Equal(t, domain.EventTypeTransfer, event.EventType)
	require.NotNil(t, event.ToAddress)
	require.Equal(t, buyer.Hex(), *event.ToAddress)
	require.Equal(t, "1", event.Quantity)
}

func TestNeedsPunkBoughtRepair_DoesNotReclassifyValidTransfer(t *testing.T) {
	t.Parallel()

	to := "0x1111111111111111111111111111111111111111"
	from := "0x2222222222222222222222222222222222222222"
	require.False(t, needsPunkBoughtRepair(domain.BlockchainEvent{
		EventType:   domain.EventTypeTransfer,
		FromAddress: &from,
		ToAddress:   &to,
	}))
}

func ptr(v string) *string {
	return &v
}
