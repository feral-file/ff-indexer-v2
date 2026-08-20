package ethereum

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/feral-file/ff-indexer-v2/internal/domain"
)

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
