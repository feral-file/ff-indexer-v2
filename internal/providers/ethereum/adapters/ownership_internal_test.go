package adapters

import (
	"math/big"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/stretchr/testify/require"

	"github.com/feral-file/ff-indexer-v2/internal/domain"
	"github.com/feral-file/ff-indexer-v2/internal/providers/ethereum/helpers"
	internalTypes "github.com/feral-file/ff-indexer-v2/internal/types"
)

func TestTrackERC721OwnershipFromLogs_LastTransferWins(t *testing.T) {
	t.Parallel()

	owner := common.HexToAddress("0x00000000000000000000000000000000000000aa")
	other := common.HexToAddress("0x00000000000000000000000000000000000000bb")
	contract := common.HexToAddress("0x0000000000000000000000000000000000000001")

	logs := []types.Log{
		{
			Address:     contract,
			BlockNumber: 100,
			Index:       1,
			Topics: []common.Hash{
				helpers.TransferEventSignature,
				common.BytesToHash(other.Bytes()),
				common.BytesToHash(owner.Bytes()),
				common.BigToHash(big.NewInt(1)),
			},
		},
		{
			Address:     contract,
			BlockNumber: 200,
			Index:       2,
			Topics: []common.Hash{
				helpers.TransferEventSignature,
				common.BytesToHash(owner.Bytes()),
				common.BytesToHash(other.Bytes()),
				common.BigToHash(big.NewInt(1)),
			},
		},
		{
			Address:     contract,
			BlockNumber: 300,
			Index:       3,
			Topics: []common.Hash{
				helpers.TransferEventSignature,
				common.BytesToHash(other.Bytes()),
				common.BytesToHash(owner.Bytes()),
				common.BigToHash(big.NewInt(2)),
			},
		},
	}

	tokens := trackERC721OwnershipFromLogs(domain.ChainEthereumMainnet, owner, logs, nil)
	require.Len(t, tokens, 1)

	tokenCID := domain.NewTokenCID(domain.ChainEthereumMainnet, domain.StandardERC721, contract.Hex(), "2")
	require.Equal(t, tokenCID, tokens[0].TokenCID)
	require.Equal(t, uint64(300), tokens[0].BlockNumber)
}

func TestTrackERC1155OwnershipFromLogs_NetBalance(t *testing.T) {
	t.Parallel()

	owner := common.HexToAddress("0x00000000000000000000000000000000000000aa")
	other := common.HexToAddress("0x00000000000000000000000000000000000000bb")
	contract := common.HexToAddress("0x0000000000000000000000000000000000000001")

	logs := []types.Log{
		erc1155SingleLog(contract, 100, 1, other, owner, big.NewInt(1), big.NewInt(2)),
		erc1155SingleLog(contract, 200, 2, owner, other, big.NewInt(1), big.NewInt(1)),
	}

	tokens := trackERC1155OwnershipFromLogs(domain.ChainEthereumMainnet, owner, logs, nil)
	require.Len(t, tokens, 1)
	require.Equal(t, uint64(200), tokens[0].BlockNumber)
}

func TestTrackOwnershipFromParsedEvents_GenericERC721(t *testing.T) {
	t.Parallel()

	owner := common.HexToAddress("0x00000000000000000000000000000000000000aa")
	other := "0x00000000000000000000000000000000000000bb"
	contract := "0xb47e3cd837ddf8e4c57f05d70ab865de6e193bbb"

	events := []domain.BlockchainEvent{
		{
			EventType:   domain.EventTypeMint,
			TokenNumber: "1",
			ToAddress:   internalTypes.StringPtr(owner.Hex()),
			BlockNumber: 100,
			LogIndex:    1,
		},
		{
			EventType:   domain.EventTypeTransfer,
			TokenNumber: "1",
			FromAddress: internalTypes.StringPtr(owner.Hex()),
			ToAddress:   &other,
			BlockNumber: 200,
			LogIndex:    2,
		},
	}

	tokens := trackOwnershipFromParsedEvents(domain.ChainEthereumMainnet, domain.StandardERC721, contract, owner, events, nil)
	require.Empty(t, tokens)
}

func TestIndexedAddressTopicIndices(t *testing.T) {
	t.Parallel()

	fromIndex, toIndex := indexedAddressTopicIndices(EventConfig{
		IndexedParams: []string{"from", "to"},
		ParameterMappings: map[string]string{
			"from": EventFieldFromAddress,
			"to":   EventFieldToAddress,
		},
	})

	require.Equal(t, 1, fromIndex)
	require.Equal(t, 2, toIndex)
}

func TestReplayBalancesFromEvents(t *testing.T) {
	t.Parallel()

	from := "0x1111111111111111111111111111111111111111"
	to := "0x2222222222222222222222222222222222222222"

	events := []domain.BlockchainEvent{
		{
			EventType:   domain.EventTypeMint,
			ToAddress:   &to,
			TokenNumber: "1",
			Quantity:    "5",
		},
		{
			EventType:   domain.EventTypeTransfer,
			FromAddress: &to,
			ToAddress:   &from,
			TokenNumber: "1",
			Quantity:    "2",
		},
	}

	balances := replayBalancesFromEvents(events)
	require.Equal(t, map[string]string{
		domain.NormalizeAddress(from): "2",
		domain.NormalizeAddress(to):   "3",
	}, balances)
}

func TestReplayOwnerBalanceFromEvents(t *testing.T) {
	t.Parallel()

	owner := "0x2222222222222222222222222222222222222222"
	other := "0x1111111111111111111111111111111111111111"

	events := []domain.BlockchainEvent{
		{
			EventType:   domain.EventTypeMint,
			ToAddress:   &owner,
			TokenNumber: "1",
			Quantity:    "5",
		},
		{
			EventType:   domain.EventTypeTransfer,
			FromAddress: &owner,
			ToAddress:   &other,
			TokenNumber: "1",
			Quantity:    "2",
		},
	}

	require.Equal(t, "3", replayOwnerBalanceFromEvents(owner, events))
}

func TestGenericAdapter_GetTokenBalances_UnsupportedForSingleOwner(t *testing.T) {
	t.Parallel()

	adp := NewGenericAdapter(
		"0xabc",
		OwnershipSingleOwner,
		nil,
		nil,
		ContractMetadataConfig{Source: MetadataSourceVendorOnly},
		nil,
		nil,
		nil,
		false,
		nil,
		domain.ChainEthereumMainnet,
	)

	_, err := adp.GetTokenBalances(t.Context(), "0xabc", "1")
	require.Error(t, err)
	require.Contains(t, err.Error(), "not supported for single-owner")
}

func erc1155SingleLog(
	contract common.Address,
	blockNumber uint64,
	index uint,
	from, to common.Address,
	tokenID, amount *big.Int,
) types.Log {
	data := make([]byte, 64)
	copy(data[0:32], common.LeftPadBytes(tokenID.Bytes(), 32))
	copy(data[32:64], common.LeftPadBytes(amount.Bytes(), 32))

	return types.Log{
		Address:     contract,
		BlockNumber: blockNumber,
		Index:       index,
		Topics: []common.Hash{
			helpers.ERC1155TransferSingleEventSignature,
			{},
			common.BytesToHash(from.Bytes()),
			common.BytesToHash(to.Bytes()),
		},
		Data: data,
	}
}
