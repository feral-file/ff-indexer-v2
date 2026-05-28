package adapters

import (
	"context"
	"math/big"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/stretchr/testify/require"

	"github.com/feral-file/ff-indexer-v2/internal/domain"
)

// TestReplayOwnerTokensWithLimit_DescNewestActivity verifies that DESC scans produce
// "newest N tokens by activity" semantics, not "ownership as of cutoff block."
//
// DESC replay processes logs from newest to oldest. For ERC-721, last-transfer-wins
// means older logs are ignored once a newer transfer is seen. This produces the N most
// recently active tokens, matching legacy main client behavior.
func TestReplayOwnerTokensWithLimit_DescNewestActivity(t *testing.T) {
	t.Parallel()

	owner := common.HexToAddress("0x00000000000000000000000000000000000000aa")
	other := common.HexToAddress("0x00000000000000000000000000000000000000bb")
	contract := common.HexToAddress("0x0000000000000000000000000000000000000001")

	// Token bought at 1400, sold at 1700
	// When scanning DESC from 2000 with limit 1:
	// - Sell @1700 processed first → token marked as not-owned
	// - Buy @1400 ignored (older than existing state)
	// Result: token NOT in output (newest activity was a sell)
	logs := []types.Log{
		erc721TransferLog(contract, 1400, 1, other, owner, big.NewInt(1)),
		erc721TransferLog(contract, 1700, 2, owner, other, big.NewInt(1)),
	}

	result, err := ReplayOwnerTokensWithLimit(context.Background(), OwnerReplayParams{
		ChainID:            domain.ChainEthereumMainnet,
		Owner:              owner,
		Logs:               logs,
		Limit:              10,
		Order:              domain.BlockScanOrderDesc,
		RequestedFromBlock: 1000,
		RequestedToBlock:   2000,
	})
	require.NoError(t, err)
	require.Empty(t, result.Tokens, "DESC replay with sell after buy excludes token (newest activity wins)")
	require.Equal(t, uint64(2000), result.EffectiveToBlock)
	require.Equal(t, uint64(1000), result.EffectiveFromBlock)
}

// TestReplayOwnerTokensWithLimit_DescWithCutoff demonstrates DESC cutoff behavior.
//
// DESC scans find the N newest active tokens. When limit is reached, EffectiveFromBlock
// moves to the cutoff boundary (earliest block kept), but ownership state is still based
// on newest-first replay.
func TestReplayOwnerTokensWithLimit_DescWithCutoff(t *testing.T) {
	t.Parallel()

	owner := common.HexToAddress("0x00000000000000000000000000000000000000aa")
	other := common.HexToAddress("0x00000000000000000000000000000000000000bb")
	contract := common.HexToAddress("0x0000000000000000000000000000000000000001")

	// Create tokens with activity at different blocks (DESC: newest first)
	logs := []types.Log{
		erc721TransferLog(contract, 1900, 1, other, owner, big.NewInt(99)), // newest
		erc721TransferLog(contract, 1800, 2, other, owner, big.NewInt(98)),
		erc721TransferLog(contract, 1700, 3, other, owner, big.NewInt(97)),
		erc721TransferLog(contract, 1600, 4, other, owner, big.NewInt(96)),
		erc721TransferLog(contract, 1500, 5, other, owner, big.NewInt(95)),
		erc721TransferLog(contract, 1400, 6, other, owner, big.NewInt(94)), // will be cutoff
		erc721TransferLog(contract, 1300, 7, other, owner, big.NewInt(93)), // older than cutoff
	}

	result, err := ReplayOwnerTokensWithLimit(context.Background(), OwnerReplayParams{
		ChainID:            domain.ChainEthereumMainnet,
		Owner:              owner,
		Logs:               logs,
		Limit:              5,
		Order:              domain.BlockScanOrderDesc,
		RequestedFromBlock: 1000,
		RequestedToBlock:   2000,
	})
	require.NoError(t, err)
	require.Len(t, result.Tokens, 5)
	require.Equal(t, uint64(1500), result.EffectiveFromBlock, "cutoff at earliest kept block")
	require.Equal(t, uint64(2000), result.EffectiveToBlock)

	// Verify we kept the 5 newest tokens (99, 98, 97, 96, 95)
	tokenIDs := make([]string, len(result.Tokens))
	for i, token := range result.Tokens {
		// Extract token number from CID
		tokenIDs[i] = string(token.TokenCID[len(token.TokenCID)-2:])
	}
	require.Contains(t, tokenIDs, "99")
	require.Contains(t, tokenIDs, "95")
	require.NotContains(t, tokenIDs, "94", "token older than cutoff excluded")
	require.NotContains(t, tokenIDs, "93", "token older than cutoff excluded")
}
