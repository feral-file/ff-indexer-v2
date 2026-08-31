package ethereum

import (
	"context"
	"fmt"
	"math/big"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"go.uber.org/zap"

	"github.com/feral-file/ff-indexer-v2/internal/adapter"
	"github.com/feral-file/ff-indexer-v2/internal/domain"
	"github.com/feral-file/ff-indexer-v2/internal/logger"
	"github.com/feral-file/ff-indexer-v2/internal/providers/ethereum/helpers"
)

// cryptoPunksAddress is the mainnet CryptoPunks market contract (also the
// configured override in contracts/contracts.json).
var cryptoPunksAddress = common.HexToAddress("0xb47e3cd837ddf8e4c57f05d70ab865de6e193bbb")

// cryptoPunksInternalTransferProbeBlock is a mainnet block that holds a
// CryptoPunks internal Transfer(seller, buyer, 1) log — a 3-topic log under the
// ERC-721 Transfer signature (tx 0xb28b5f2c…ba52; verified against the vendor
// on 2026-08-30, one of 45 such logs in blocks 3,914,623–3,924,623).
const cryptoPunksInternalTransferProbeBlock = 3_919_706

// openSeaSharedStorefrontAddress is the OpenSea Shared Storefront (a large,
// multi-token ERC-1155), used to probe the warehouse's erc1155Id filter.
var openSeaSharedStorefrontAddress = common.HexToAddress("0x495f947276749Ce646f68AC8c248420045cb7b5e")

// erc1155IDProbeBlock is a mainnet block in which the storefront emitted five
// TransferSingle logs across four distinct token ids — erc1155ProbeTokenID
// (twice) and three siblings (verified live against the warehouse 2026-08-31).
// Probing the block with erc1155ProbeTokenID must return only its two logs: a
// warehouse that ignores the unknown field answers with the siblings too,
// which the probe rejects.
const erc1155IDProbeBlock = 14_045_001

// erc1155ProbeTokenID is a token transferred (twice) in erc1155IDProbeBlock; its
// 32-byte id is the first data word of those TransferSingle logs.
var erc1155ProbeTokenID = common.HexToHash("0x57b090ba902578996db810e9f3140bd73ea8495e000000000000010000000032")

// LogWarehouseRequirements returns what the warehouse must satisfy before the
// indexer routes history through it for the given chain: the chain id, and on
// mainnet a probe proving it stores the CryptoPunks internal Transfer.
//
// Reason: the owner scan discovers corrupted acceptBidForPunk purchases (a
// PunkBought whose indexed buyer is zero) only through that internal Transfer
// (adapters.GenericAdapter.OwnerQuerySpecs / PostProcessOwnerLogs). It is a
// 3-topic log under the ERC-721 Transfer signature — the shape a warehouse
// build drops as "ERC-20". A warehouse without it would answer the owner
// query completely, minus the one log that triggers the repair, and the buyer
// would vanish from owner discovery with no fall-through to catch it. The
// probe turns that silent omission into a refused warehouse.
func LogWarehouseRequirements(chain domain.Chain) (adapter.LogWarehouseRequirements, error) {
	id, ok := chain.EIP155NumericID()
	if !ok || id < 0 {
		return adapter.LogWarehouseRequirements{}, fmt.Errorf("log warehouse requires an eip155 chain, got %q", chain)
	}
	reqs := adapter.LogWarehouseRequirements{ChainID: uint64(id)}
	if chain == domain.ChainEthereumMainnet {
		reqs.Probes = append(reqs.Probes, cryptoPunksInternalTransferProbe(), erc1155IDFilterProbe())
	}
	return reqs, nil
}

// cryptoPunksInternalTransferProbe asks for the Transfer-signature logs of the
// CryptoPunks contract in the probe block and accepts only a 3-topic one.
func cryptoPunksInternalTransferProbe() adapter.LogWarehouseProbe {
	block := new(big.Int).SetUint64(cryptoPunksInternalTransferProbeBlock)
	return adapter.LogWarehouseProbe{
		Name: "CryptoPunks internal Transfer",
		Query: ethereum.FilterQuery{
			FromBlock: block,
			ToBlock:   block,
			Addresses: []common.Address{cryptoPunksAddress},
			Topics:    [][]common.Hash{{helpers.TransferEventSignature}},
		},
		Accept: func(logs []types.Log) bool {
			for _, l := range logs {
				if l.Address == cryptoPunksAddress && len(l.Topics) == 3 {
					return true
				}
			}
			return false
		},
	}
}

// erc1155IDFilterProbe verifies the warehouse actually applies the erc1155Id
// filter (ff-eth-logs api_design.md 3.8), on which per-token ERC-1155
// provenance depends: GetTokenEvents sends the token id so the walk is an
// index point lookup instead of a whole-contract scan.
//
// Reason: the field is a warehouse-only extension a standard node — and an
// older warehouse build — ignores, answering the whole query. The indexer
// trusts the warehouse's answer for the covered range with no vendor backstop,
// so a warehouse that ignores or misapplies the filter would either resume the
// full-contract scan (an ignored field) or silently drop the token's history
// (a broken filter). The probe turns both into a refused warehouse. It asks
// erc1155IDProbeBlock (two distinct tokens) for erc1155ProbeTokenID and accepts
// only when every returned TransferSingle carries that id in data word 0 and at
// least one does: a warehouse that ignored the field returns the sibling token
// too (rejected), and one that dropped everything returns nothing (rejected).
func erc1155IDFilterProbe() adapter.LogWarehouseProbe {
	block := new(big.Int).SetUint64(erc1155IDProbeBlock)
	id := erc1155ProbeTokenID
	return adapter.LogWarehouseProbe{
		Name: "ERC-1155 erc1155Id filter",
		Query: ethereum.FilterQuery{
			FromBlock: block,
			ToBlock:   block,
			Addresses: []common.Address{openSeaSharedStorefrontAddress},
			Topics:    [][]common.Hash{{helpers.ERC1155TransferSingleEventSignature}},
		},
		ERC1155ID: &id,
		Accept: func(logs []types.Log) bool {
			matched := 0
			for _, l := range logs {
				// TransferSingle carries the token id in data word 0.
				if len(l.Data) < 32 || common.BytesToHash(l.Data[:32]) != id {
					return false // a foreign token id proves the filter was not applied
				}
				matched++
			}
			return matched > 0
		},
	}
}

// LogWarehouseHead returns the warehouse head and true when a log warehouse is
// configured and answers; (0, false) when none is configured, it is
// unreachable, or it was refused. Callers plan work around the head (the
// owner scan sizes its windows by it) but never depend on it: every log fetch
// re-checks the head and falls through to the vendor on its own.
func (f *ethereumClient) LogWarehouseHead(ctx context.Context) (uint64, bool) {
	if f.guards.LogWarehouse == nil {
		return 0, false
	}
	head, err := f.guards.LogWarehouse.Head(ctx)
	if err != nil {
		logger.WarnCtx(ctx, "Log warehouse head unavailable, planning without it", zap.Error(err))
		return 0, false
	}
	return head, true
}
