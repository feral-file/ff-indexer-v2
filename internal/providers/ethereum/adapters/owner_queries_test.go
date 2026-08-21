package adapters_test

import (
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/stretchr/testify/require"

	"github.com/feral-file/ff-indexer-v2/internal/providers/ethereum/adapters"
	"github.com/feral-file/ff-indexer-v2/internal/providers/ethereum/helpers"
)

var (
	punkTransferSig = crypto.Keccak256Hash([]byte("PunkTransfer(address,address,uint256)"))
	punkAssignSig   = crypto.Keccak256Hash([]byte("Assign(address,uint256)"))
	punkBoughtSig   = crypto.Keccak256Hash([]byte("PunkBought(uint256,uint256,address,address)"))
)

// cryptoPunksSpecs mirrors the specs the CryptoPunks GenericAdapter derives from
// contracts.json: grouped provenance events plus the internal Transfer buyer leg.
func cryptoPunksSpecs() []adapters.OwnerQuerySpec {
	return []adapters.OwnerQuerySpec{
		{EventSigs: []common.Hash{punkTransferSig, punkAssignSig}, OwnerTopicIndex: 1},
		{EventSigs: []common.Hash{punkTransferSig, punkBoughtSig}, OwnerTopicIndex: 2},
		{EventSigs: []common.Hash{punkBoughtSig}, OwnerTopicIndex: 3},
		{EventSigs: []common.Hash{helpers.TransferEventSignature}, OwnerTopicIndex: 2},
	}
}

// TestMergeOwnerQuerySpecs_CrossAdapterCreditGuard pins the whole-scan query
// count: merging the ERC-721, ERC-1155, and CryptoPunks specs yields exactly
// THREE queries (one per owner topic position) with exactly these signature
// sets. Each query is a full-range pagination walk on a span-capped provider
// (~2,500 calls for a mainnet history scan), so a regression from 3 back to 8
// walks silently multiplies the Infura cost of every wallet scan by ~2.7x.
func TestMergeOwnerQuerySpecs_CrossAdapterCreditGuard(t *testing.T) {
	t.Parallel()

	var specs []adapters.OwnerQuerySpec
	specs = append(specs,
		adapters.OwnerQuerySpec{EventSigs: []common.Hash{helpers.TransferEventSignature}, OwnerTopicIndex: 1},
		adapters.OwnerQuerySpec{EventSigs: []common.Hash{helpers.TransferEventSignature}, OwnerTopicIndex: 2},
	)
	specs = append(specs,
		adapters.OwnerQuerySpec{
			EventSigs:       []common.Hash{helpers.ERC1155TransferSingleEventSignature, helpers.ERC1155TransferBatchEventSignature},
			OwnerTopicIndex: 2,
		},
		adapters.OwnerQuerySpec{
			EventSigs:       []common.Hash{helpers.ERC1155TransferSingleEventSignature, helpers.ERC1155TransferBatchEventSignature},
			OwnerTopicIndex: 3,
		},
	)
	specs = append(specs, cryptoPunksSpecs()...)

	merged := adapters.MergeOwnerQuerySpecs(specs)
	require.Len(t, merged, 3, "one merged query per owner topic position")

	require.Equal(t, 1, merged[0].OwnerTopicIndex)
	require.ElementsMatch(t, []common.Hash{
		helpers.TransferEventSignature, // ERC-721 from / punks internal seller
		punkTransferSig,
		punkAssignSig,
	}, merged[0].EventSigs)

	require.Equal(t, 2, merged[1].OwnerTopicIndex)
	require.ElementsMatch(t, []common.Hash{
		helpers.TransferEventSignature, // ERC-721 to; subsumes the punks internal buyer leg
		helpers.ERC1155TransferSingleEventSignature,
		helpers.ERC1155TransferBatchEventSignature,
		punkTransferSig,
		punkBoughtSig,
	}, merged[1].EventSigs)

	require.Equal(t, 3, merged[2].OwnerTopicIndex)
	require.ElementsMatch(t, []common.Hash{
		helpers.ERC1155TransferSingleEventSignature,
		helpers.ERC1155TransferBatchEventSignature,
		punkBoughtSig,
	}, merged[2].EventSigs,
		"Transfer must NOT appear at position 3: ERC-721 topic 3 is tokenId, and an owner hash there matches garbage")
}

// TestMergeOwnerQuerySpecs_DedupesAndDropsInvalid verifies duplicate signatures
// union to one entry and malformed specs (empty sigs, out-of-range positions)
// are dropped instead of producing broken queries.
func TestMergeOwnerQuerySpecs_DedupesAndDropsInvalid(t *testing.T) {
	t.Parallel()

	merged := adapters.MergeOwnerQuerySpecs([]adapters.OwnerQuerySpec{
		{EventSigs: []common.Hash{helpers.TransferEventSignature}, OwnerTopicIndex: 2},
		{EventSigs: []common.Hash{helpers.TransferEventSignature}, OwnerTopicIndex: 2},
		{EventSigs: nil, OwnerTopicIndex: 1},
		{EventSigs: []common.Hash{punkBoughtSig}, OwnerTopicIndex: 0},
		{EventSigs: []common.Hash{punkBoughtSig}, OwnerTopicIndex: 4},
	})

	require.Len(t, merged, 1)
	require.Equal(t, 2, merged[0].OwnerTopicIndex)
	require.Equal(t, []common.Hash{helpers.TransferEventSignature}, merged[0].EventSigs)
}

// TestBuildOwnerQueries_TopicLayoutAndScope pins the concrete FilterQuery shape:
// signatures OR'd in topics[0], owner hash alone at its position, intermediate
// positions nil (wildcard), topics sliced to exactly ownerTopicIndex+1, and the
// optional contract scope applied verbatim.
func TestBuildOwnerQueries_TopicLayoutAndScope(t *testing.T) {
	t.Parallel()

	owner := common.HexToAddress("0x00000000000000000000000000000000000000aa")
	ownerHash := common.BytesToHash(owner.Bytes())
	contract := common.HexToAddress("0x00000000000000000000000000000000000000bb")

	queries := adapters.BuildOwnerQueries(
		[]adapters.OwnerQuerySpec{
			{EventSigs: []common.Hash{punkTransferSig}, OwnerTopicIndex: 1},
			{EventSigs: []common.Hash{punkBoughtSig}, OwnerTopicIndex: 3},
		},
		ownerHash,
		[]common.Address{contract},
		5, 100,
	)

	require.Len(t, queries, 2)

	require.Len(t, queries[0].Topics, 2)
	require.Equal(t, []common.Hash{punkTransferSig}, queries[0].Topics[0])
	require.Equal(t, []common.Hash{ownerHash}, queries[0].Topics[1])

	require.Len(t, queries[1].Topics, 4)
	require.Equal(t, []common.Hash{punkBoughtSig}, queries[1].Topics[0])
	require.Nil(t, queries[1].Topics[1])
	require.Nil(t, queries[1].Topics[2])
	require.Equal(t, []common.Hash{ownerHash}, queries[1].Topics[3])

	for _, q := range queries {
		require.Equal(t, []common.Address{contract}, q.Addresses)
		require.Equal(t, uint64(5), q.FromBlock.Uint64())
		require.Equal(t, uint64(100), q.ToBlock.Uint64())
	}
}
