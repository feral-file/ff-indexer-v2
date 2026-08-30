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
		reqs.Probes = append(reqs.Probes, cryptoPunksInternalTransferProbe())
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
