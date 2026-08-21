package adapters

import (
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/stretchr/testify/require"

	"github.com/feral-file/ff-indexer-v2/internal/domain"
	"github.com/feral-file/ff-indexer-v2/internal/providers/ethereum/helpers"
)

// punksProvenanceEvents is the CryptoPunks provenance event set from contracts.json:
// PunkTransfer(from@1, to@2), Assign(to@1), PunkBought(punkIndex@1, from@2, to@3),
// plus a non-ownership metadata event that must not contribute specs.
func punksProvenanceEvents() []EventConfig {
	return []EventConfig{
		{
			Signature:          "PunkTransfer(address,address,uint256)",
			MapToStandardEvent: domain.EventTypeTransfer,
			IndexedParams:      []string{"from", "to"},
			ParameterMappings: map[string]string{
				"from": EventFieldFromAddress,
				"to":   EventFieldToAddress,
			},
		},
		{
			Signature:          "Assign(address,uint256)",
			MapToStandardEvent: domain.EventTypeMint,
			IndexedParams:      []string{"to"},
			ParameterMappings: map[string]string{
				"to": EventFieldToAddress,
			},
		},
		{
			Signature:          "PunkBought(uint256,uint256,address,address)",
			MapToStandardEvent: domain.EventTypeTransfer,
			IndexedParams:      []string{"punkIndex", "fromAddress", "toAddress"},
			ParameterMappings: map[string]string{
				"fromAddress": EventFieldFromAddress,
				"toAddress":   EventFieldToAddress,
			},
		},
		{
			Signature:          "MetadataUpdated(uint256)",
			MapToStandardEvent: domain.EventTypeMetadataUpdate,
		},
	}
}

// TestGenericOwnerQuerySpecs_GroupsSignaturesByOwnerPosition pins the
// credit-guard spec shape for configured contracts: signatures sharing an owner
// topic position are OR'd into one spec instead of one per (event, position)
// pair, and the CryptoPunks internal Transfer buyer leg is declared as a spec so
// the client's cross-adapter merge can dissolve it into the ERC-721 leg. With
// the CryptoPunks event set this is 3 grouped specs + 1 internal Transfer spec;
// each spec that survives the merge walks the full block range on a span-capped
// provider, so ungrouping silently raises the RPC cost of every wallet scan.
func TestGenericOwnerQuerySpecs_GroupsSignaturesByOwnerPosition(t *testing.T) {
	t.Parallel()

	adp := &GenericAdapter{provenanceEvents: punksProvenanceEvents()}

	punkTransferSig := crypto.Keccak256Hash([]byte("PunkTransfer(address,address,uint256)"))
	assignSig := crypto.Keccak256Hash([]byte("Assign(address,uint256)"))
	punkBoughtSig := crypto.Keccak256Hash([]byte("PunkBought(uint256,uint256,address,address)"))

	specs := adp.OwnerQuerySpecs()
	require.Len(t, specs, 4, "3 grouped provenance specs + internal Transfer buyer spec")

	// Grouped specs are emitted in ascending owner-topic-position order.
	require.Equal(t, 1, specs[0].OwnerTopicIndex)
	require.ElementsMatch(t, []common.Hash{punkTransferSig, assignSig}, specs[0].EventSigs,
		"position 1 groups PunkTransfer.from with Assign.to")

	require.Equal(t, 2, specs[1].OwnerTopicIndex)
	require.ElementsMatch(t, []common.Hash{punkTransferSig, punkBoughtSig}, specs[1].EventSigs,
		"position 2 groups PunkTransfer.to with PunkBought.from")

	require.Equal(t, 3, specs[2].OwnerTopicIndex)
	require.ElementsMatch(t, []common.Hash{punkBoughtSig}, specs[2].EventSigs)

	require.Equal(t, 2, specs[3].OwnerTopicIndex,
		"internal Transfer buyer leg targets topic 2 (ERC-20-style to address)")
	require.ElementsMatch(t, []common.Hash{helpers.TransferEventSignature}, specs[3].EventSigs)
}

// TestGenericOwnerQuerySpecs_NoPunkBoughtNoInternalTransferLeg verifies the
// internal Transfer spec is punks-repair-specific: contracts without a
// PunkBought event must not pay an extra Transfer walk.
func TestGenericOwnerQuerySpecs_NoPunkBoughtNoInternalTransferLeg(t *testing.T) {
	t.Parallel()

	events := punksProvenanceEvents()[:2] // PunkTransfer + Assign only
	adp := &GenericAdapter{provenanceEvents: events}

	specs := adp.OwnerQuerySpecs()
	require.Len(t, specs, 2)
	for _, spec := range specs {
		for _, sig := range spec.EventSigs {
			require.NotEqual(t, helpers.TransferEventSignature, sig)
		}
	}
}
