package ethereum

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/feral-file/ff-indexer-v2/internal/domain"
)

func TestApplyOwnerTokenLimit_BlockBoundary(t *testing.T) {
	t.Parallel()

	tokens := []domain.TokenWithBlock{
		{TokenCID: "a", BlockNumber: 100},
		{TokenCID: "b", BlockNumber: 100},
		{TokenCID: "c", BlockNumber: 200},
		{TokenCID: "d", BlockNumber: 300},
	}

	limited, from, to := applyOwnerTokenLimit(tokens, 2, domain.BlockScanOrderAsc, 1, 500)
	require.Len(t, limited, 2)
	require.Equal(t, uint64(1), from)
	require.Equal(t, uint64(100), to)
	require.Equal(t, "a", string(limited[0].TokenCID))
	require.Equal(t, "b", string(limited[1].TokenCID))
}

func TestApplyOwnerTokenLimit_EmptyInput(t *testing.T) {
	t.Parallel()

	limited, from, to := applyOwnerTokenLimit(nil, 10, domain.BlockScanOrderAsc, 1, 500)
	require.Empty(t, limited)
	require.Equal(t, uint64(1), from)
	require.Equal(t, uint64(500), to)
}

func TestApplyOwnerTokenLimit_ZeroLimit(t *testing.T) {
	t.Parallel()

	tokens := []domain.TokenWithBlock{
		{TokenCID: "a", BlockNumber: 100},
		{TokenCID: "b", BlockNumber: 200},
	}

	// Zero limit means stop at first block boundary after reaching limit
	// Since limit is 0, we stop after the first block
	limited, from, to := applyOwnerTokenLimit(tokens, 0, domain.BlockScanOrderAsc, 1, 500)
	require.Len(t, limited, 1)
	require.Equal(t, uint64(1), from)
	require.Equal(t, uint64(100), to) // Cuts at first block
}

func TestApplyOwnerTokenLimit_Descending(t *testing.T) {
	t.Parallel()

	tokens := []domain.TokenWithBlock{
		{TokenCID: "d", BlockNumber: 400},
		{TokenCID: "c", BlockNumber: 300},
		{TokenCID: "b", BlockNumber: 200},
		{TokenCID: "a", BlockNumber: 100},
	}

	limited, from, to := applyOwnerTokenLimit(tokens, 3, domain.BlockScanOrderDesc, 1, 500)
	require.Len(t, limited, 3)
	require.Equal(t, uint64(200), from)
	require.Equal(t, uint64(500), to)
	require.Equal(t, "d", string(limited[0].TokenCID))
	require.Equal(t, "c", string(limited[1].TokenCID))
	require.Equal(t, "b", string(limited[2].TokenCID))
}

func TestApplyOwnerTokenLimit_LimitExceedsTokens(t *testing.T) {
	t.Parallel()

	tokens := []domain.TokenWithBlock{
		{TokenCID: "a", BlockNumber: 100},
		{TokenCID: "b", BlockNumber: 200},
	}

	limited, from, to := applyOwnerTokenLimit(tokens, 100, domain.BlockScanOrderAsc, 1, 500)
	require.Len(t, limited, 2)
	require.Equal(t, uint64(1), from)
	require.Equal(t, uint64(500), to) // Effective range unchanged when limit not reached
}

func TestSortTokensByBlockOrder_Ascending(t *testing.T) {
	t.Parallel()

	tokens := []domain.TokenWithBlock{
		{TokenCID: "a", BlockNumber: 200},
		{TokenCID: "b", BlockNumber: 100},
		{TokenCID: "c", BlockNumber: 300},
	}

	sortTokensByBlockOrder(tokens, domain.BlockScanOrderAsc)
	require.Equal(t, uint64(100), tokens[0].BlockNumber)
	require.Equal(t, uint64(200), tokens[1].BlockNumber)
	require.Equal(t, uint64(300), tokens[2].BlockNumber)
}

func TestSortTokensByBlockOrder_Descending(t *testing.T) {
	t.Parallel()

	tokens := []domain.TokenWithBlock{
		{TokenCID: "b", BlockNumber: 100},
		{TokenCID: "a", BlockNumber: 200},
	}

	sortTokensByBlockOrder(tokens, domain.BlockScanOrderDesc)
	require.Equal(t, uint64(200), tokens[0].BlockNumber)
	require.Equal(t, uint64(100), tokens[1].BlockNumber)
}

func TestSortTokensByBlockOrder_StableSortSameBlock(t *testing.T) {
	t.Parallel()

	// When multiple tokens are in the same block, the sort should also consider TokenCID
	tokens := []domain.TokenWithBlock{
		{TokenCID: "z", BlockNumber: 100},
		{TokenCID: "a", BlockNumber: 100},
		{TokenCID: "m", BlockNumber: 100},
	}

	sortTokensByBlockOrder(tokens, domain.BlockScanOrderAsc)
	require.Equal(t, uint64(100), tokens[0].BlockNumber)
	require.Equal(t, uint64(100), tokens[1].BlockNumber)
	require.Equal(t, uint64(100), tokens[2].BlockNumber)
	// Should be sorted by TokenCID within same block
	require.Equal(t, "a", string(tokens[0].TokenCID))
	require.Equal(t, "m", string(tokens[1].TokenCID))
	require.Equal(t, "z", string(tokens[2].TokenCID))
}
