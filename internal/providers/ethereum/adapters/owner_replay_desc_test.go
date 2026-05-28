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

// TestReplayOwnerTokensWithLimit_DescNewestActivity_ERC1155 verifies DESC replay
// semantics for ERC-1155/multi-holder tokens where balance accumulation matters.
//
// DESC replay for ERC-1155 processes logs from newest to oldest, accumulating balances
// to produce "newest N tokens with non-zero balance" semantics. Unlike ERC-721's
// last-transfer-wins, ERC-1155 sums all transfers to determine final balance.
func TestReplayOwnerTokensWithLimit_DescNewestActivity_ERC1155(t *testing.T) {
	t.Parallel()

	owner := common.HexToAddress("0x00000000000000000000000000000000000000aa")
	other := common.HexToAddress("0x00000000000000000000000000000000000000bb")
	contract := common.HexToAddress("0x0000000000000000000000000000000000000001")

	// Token acquired in two batches, then partially sold:
	// - Block 1400: received 10 units
	// - Block 1600: received 5 more units (total 15)
	// - Block 1700: sold 8 units (remaining 7 > 0)
	// When scanning DESC from 2000, owner should still own the token (balance > 0)
	logs := []types.Log{
		erc1155TransferSingleLog(contract, 1400, 1, other, owner, big.NewInt(1), big.NewInt(10)),
		erc1155TransferSingleLog(contract, 1600, 2, other, owner, big.NewInt(1), big.NewInt(5)),
		erc1155TransferSingleLog(contract, 1700, 3, owner, other, big.NewInt(1), big.NewInt(8)),
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
	require.Len(t, result.Tokens, 1, "DESC replay keeps token with non-zero balance after partial sale")
	require.Equal(t, uint64(1700), result.Tokens[0].BlockNumber, "block number reflects newest activity")
	require.Equal(t, uint64(2000), result.EffectiveToBlock)
	require.Equal(t, uint64(1000), result.EffectiveFromBlock)
}

// TestReplayOwnerTokensWithLimit_DescZeroBalance_ERC1155 verifies that ERC-1155
// tokens with zero final balance are excluded from DESC replay results.
//
// When all units are sold/transferred, the token should not appear in the output,
// similar to ERC-721 behavior but through balance accumulation.
func TestReplayOwnerTokensWithLimit_DescZeroBalance_ERC1155(t *testing.T) {
	t.Parallel()

	owner := common.HexToAddress("0x00000000000000000000000000000000000000aa")
	other := common.HexToAddress("0x00000000000000000000000000000000000000bb")
	contract := common.HexToAddress("0x0000000000000000000000000000000000000001")

	// Token fully acquired then fully sold:
	// - Block 1400: received 10 units
	// - Block 1700: sold all 10 units (balance 0)
	logs := []types.Log{
		erc1155TransferSingleLog(contract, 1400, 1, other, owner, big.NewInt(1), big.NewInt(10)),
		erc1155TransferSingleLog(contract, 1700, 2, owner, other, big.NewInt(1), big.NewInt(10)),
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
	require.Empty(t, result.Tokens, "DESC replay excludes token with zero balance")
	require.Equal(t, uint64(2000), result.EffectiveToBlock)
	require.Equal(t, uint64(1000), result.EffectiveFromBlock)
}

// TestReplayOwnerTokensWithLimit_DescWithCutoff_ERC1155 demonstrates DESC cutoff
// behavior for multi-holder tokens where limit is reached mid-replay.
//
// For ERC-1155, the cutoff logic still applies: we keep the N newest tokens with
// non-zero balance, and EffectiveFromBlock moves to the earliest kept block.
func TestReplayOwnerTokensWithLimit_DescWithCutoff_ERC1155(t *testing.T) {
	t.Parallel()

	owner := common.HexToAddress("0x00000000000000000000000000000000000000aa")
	other := common.HexToAddress("0x00000000000000000000000000000000000000bb")
	contract := common.HexToAddress("0x0000000000000000000000000000000000000001")

	// Create 7 tokens with activity at different blocks (DESC: newest first)
	// Each token receives varying amounts to verify balance tracking
	logs := []types.Log{
		erc1155TransferSingleLog(contract, 1900, 1, other, owner, big.NewInt(99), big.NewInt(100)), // newest
		erc1155TransferSingleLog(contract, 1800, 2, other, owner, big.NewInt(98), big.NewInt(50)),
		erc1155TransferSingleLog(contract, 1700, 3, other, owner, big.NewInt(97), big.NewInt(25)),
		erc1155TransferSingleLog(contract, 1600, 4, other, owner, big.NewInt(96), big.NewInt(10)),
		erc1155TransferSingleLog(contract, 1500, 5, other, owner, big.NewInt(95), big.NewInt(5)),
		erc1155TransferSingleLog(contract, 1400, 6, other, owner, big.NewInt(94), big.NewInt(1)), // will be cutoff
		erc1155TransferSingleLog(contract, 1300, 7, other, owner, big.NewInt(93), big.NewInt(1)), // older than cutoff
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
	require.Len(t, result.Tokens, 5, "DESC replay respects limit for ERC-1155")
	require.Equal(t, uint64(1500), result.EffectiveFromBlock, "cutoff at earliest kept block")
	require.Equal(t, uint64(2000), result.EffectiveToBlock)

	// Verify we kept the 5 newest tokens (99, 98, 97, 96, 95)
	tokenIDs := make([]string, len(result.Tokens))
	for i, token := range result.Tokens {
		// Extract token number from CID
		_, _, _, tokenNum := token.TokenCID.Parse()
		tokenIDs[i] = tokenNum
	}

	require.Contains(t, tokenIDs, "99")
	require.Contains(t, tokenIDs, "98")
	require.Contains(t, tokenIDs, "97")
	require.Contains(t, tokenIDs, "96")
	require.Contains(t, tokenIDs, "95")
	require.NotContains(t, tokenIDs, "94", "token 94 older than cutoff excluded")
	require.NotContains(t, tokenIDs, "93", "token 93 older than cutoff excluded")
}
