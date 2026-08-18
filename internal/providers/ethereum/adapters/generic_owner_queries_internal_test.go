package adapters

import (
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/stretchr/testify/require"

	"github.com/feral-file/ff-indexer-v2/internal/domain"
)

// TestBuildCustomOwnerTransferQueries_GroupsSignaturesByOwnerPosition pins the
// credit-guard query shape for configured contracts: signatures sharing an owner
// topic position are OR'd into one query instead of one query per (event,
// position) pair. With the CryptoPunks event set this is 3 queries instead of 5;
// each query walks the full block range on a span-capped provider, so ungrouping
// silently raises the RPC cost of every wallet scan.
func TestBuildCustomOwnerTransferQueries_GroupsSignaturesByOwnerPosition(t *testing.T) {
	t.Parallel()

	// The CryptoPunks provenance event set from contracts.json:
	// PunkTransfer(from@1, to@2), Assign(to@1), PunkBought(punkIndex@1, from@2, to@3).
	punkTransfer := EventConfig{
		Signature:          "PunkTransfer(address,address,uint256)",
		MapToStandardEvent: domain.EventTypeTransfer,
		IndexedParams:      []string{"from", "to"},
		ParameterMappings: map[string]string{
			"from": EventFieldFromAddress,
			"to":   EventFieldToAddress,
		},
	}
	assign := EventConfig{
		Signature:          "Assign(address,uint256)",
		MapToStandardEvent: domain.EventTypeMint,
		IndexedParams:      []string{"to"},
		ParameterMappings: map[string]string{
			"to": EventFieldToAddress,
		},
	}
	punkBought := EventConfig{
		Signature:          "PunkBought(uint256,uint256,address,address)",
		MapToStandardEvent: domain.EventTypeTransfer,
		IndexedParams:      []string{"punkIndex", "fromAddress", "toAddress"},
		ParameterMappings: map[string]string{
			"fromAddress": EventFieldFromAddress,
			"toAddress":   EventFieldToAddress,
		},
	}
	// Non-ownership events must not contribute queries.
	metadataUpdate := EventConfig{
		Signature:          "MetadataUpdated(uint256)",
		MapToStandardEvent: domain.EventTypeMetadataUpdate,
	}

	contractAddr := common.HexToAddress("0x00000000000000000000000000000000000000bb")
	owner := common.HexToAddress("0x00000000000000000000000000000000000000aa")
	ownerHash := common.BytesToHash(owner.Bytes())

	const (
		fromBlock uint64 = 5
		toBlock   uint64 = 100
	)

	queries := buildCustomOwnerTransferQueries(
		[]EventConfig{punkTransfer, assign, punkBought, metadataUpdate},
		contractAddr, ownerHash, fromBlock, toBlock,
	)

	punkTransferSig := crypto.Keccak256Hash([]byte(punkTransfer.Signature))
	assignSig := crypto.Keccak256Hash([]byte(assign.Signature))
	punkBoughtSig := crypto.Keccak256Hash([]byte(punkBought.Signature))

	require.Len(t, queries, 3, "one query per owner topic position, not per (event, position) pair")

	// Queries are emitted in ascending owner-topic-position order.
	require.ElementsMatch(t, []common.Hash{punkTransferSig, assignSig}, queries[0].Topics[0],
		"position 1 groups PunkTransfer.from with Assign.to")
	require.Equal(t, []common.Hash{ownerHash}, queries[0].Topics[1])

	require.ElementsMatch(t, []common.Hash{punkTransferSig, punkBoughtSig}, queries[1].Topics[0],
		"position 2 groups PunkTransfer.to with PunkBought.from")
	require.Equal(t, []common.Hash{ownerHash}, queries[1].Topics[2])

	require.Equal(t, []common.Hash{punkBoughtSig}, queries[2].Topics[0])
	require.Equal(t, []common.Hash{ownerHash}, queries[2].Topics[3])

	for _, q := range queries {
		require.Equal(t, []common.Address{contractAddr}, q.Addresses)
		require.Equal(t, fromBlock, q.FromBlock.Uint64())
		require.Equal(t, toBlock, q.ToBlock.Uint64())
	}
}
