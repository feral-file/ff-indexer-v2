package adapters

import (
	"context"
	"math/big"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/stretchr/testify/require"

	"github.com/feral-file/ff-indexer-v2/internal/domain"
	"github.com/feral-file/ff-indexer-v2/internal/providers/ethereum/helpers"
)

func TestReplayOwnerTokensWithLimit_StopsBeforeLaterSell(t *testing.T) {
	t.Parallel()

	owner := common.HexToAddress("0x00000000000000000000000000000000000000aa")
	other := common.HexToAddress("0x00000000000000000000000000000000000000bb")
	contract := common.HexToAddress("0x0000000000000000000000000000000000000001")

	logs := make([]types.Log, 0, 101)
	for i := range 99 {
		tokenID := int64(i + 1)
		blockNumber := uint64(1000 + i) //nolint:gosec,G115 // test fixture: i is bounded to 98
		logIndex := uint(i + 1)         //nolint:gosec,G115 // test fixture: i is bounded to 98
		logs = append(logs, erc721TransferLog(contract, blockNumber, logIndex, other, owner, big.NewInt(tokenID)))
	}

	logs = append(logs,
		erc721TransferLog(contract, 1400, 100, other, owner, big.NewInt(100)),
		erc721TransferLog(contract, 1800, 101, owner, other, big.NewInt(100)),
	)

	result, err := ReplayOwnerTokensWithLimit(context.Background(), OwnerReplayParams{
		ChainID:            domain.ChainEthereumMainnet,
		Owner:              owner,
		Logs:               logs,
		Limit:              100,
		Order:              domain.BlockScanOrderAsc,
		RequestedFromBlock: 1000,
		RequestedToBlock:   2000,
	})
	require.NoError(t, err)
	require.Equal(t, uint64(1400), result.EffectiveToBlock)
	require.Len(t, result.Tokens, 100)

	tokenCID := domain.NewTokenCID(domain.ChainEthereumMainnet, domain.StandardERC721, contract.Hex(), "100")
	found := false
	for _, token := range result.Tokens {
		if token.TokenCID == tokenCID {
			found = true
			require.Equal(t, uint64(1400), token.BlockNumber)
			break
		}
	}
	require.True(t, found, "token acquired before cutoff and sold after must remain in result")
}

func TestReplayOwnerTokensWithLimit_EndStateWouldDropToken(t *testing.T) {
	t.Parallel()

	owner := common.HexToAddress("0x00000000000000000000000000000000000000aa")
	other := common.HexToAddress("0x00000000000000000000000000000000000000bb")
	contract := common.HexToAddress("0x0000000000000000000000000000000000000001")

	logs := []types.Log{
		erc721TransferLog(contract, 1400, 1, other, owner, big.NewInt(1)),
		erc721TransferLog(contract, 1800, 2, owner, other, big.NewInt(1)),
	}

	fullRange, err := ReplayOwnerTokensWithLimit(context.Background(), OwnerReplayParams{
		ChainID:            domain.ChainEthereumMainnet,
		Owner:              owner,
		Logs:               logs,
		Limit:              1_000,
		Order:              domain.BlockScanOrderAsc,
		RequestedFromBlock: 1000,
		RequestedToBlock:   2000,
	})
	require.NoError(t, err)
	require.Empty(t, fullRange.Tokens, "end-of-range replay excludes token sold before range end")

	limited, err := ReplayOwnerTokensWithLimit(context.Background(), OwnerReplayParams{
		ChainID:            domain.ChainEthereumMainnet,
		Owner:              owner,
		Logs:               logs,
		Limit:              1,
		Order:              domain.BlockScanOrderAsc,
		RequestedFromBlock: 1000,
		RequestedToBlock:   2000,
	})
	require.NoError(t, err)
	require.Len(t, limited.Tokens, 1)
	require.Equal(t, uint64(1400), limited.EffectiveToBlock)
}

func erc721TransferLog(
	contract common.Address,
	blockNumber uint64,
	index uint,
	from, to common.Address,
	tokenID *big.Int,
) types.Log {
	return types.Log{
		Address:     contract,
		BlockNumber: blockNumber,
		Index:       index,
		Topics: []common.Hash{
			helpers.TransferEventSignature,
			common.BytesToHash(from.Bytes()),
			common.BytesToHash(to.Bytes()),
			common.BigToHash(tokenID),
		},
	}
}
