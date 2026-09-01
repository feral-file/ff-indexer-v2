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

// erc1155IDProbeBlock (0xd65e29) is a mainnet block in which the storefront
// emitted five TransferSingle logs across four distinct token ids — erc1155ProbeTokenID
// (twice) and three siblings (verified live against the warehouse 2026-08-31).
// Probing the block with erc1155ProbeTokenID must return only its two logs: a
// warehouse that ignores the unknown field answers with the siblings too,
// which the probe rejects.
const erc1155IDProbeBlock = 14_048_809

// erc1155ProbeTokenID is a token transferred (twice) in erc1155IDProbeBlock; its
// 32-byte id is the first data word of those TransferSingle logs.
var erc1155ProbeTokenID = common.HexToHash("0x57b090ba902578996db810e9f3140bd73ea8495e000000000000010000000032")

// erc1155ProbeTokenLogs is how many TransferSingle logs erc1155ProbeTokenID has
// in erc1155IDProbeBlock (verified live); the probe requires exactly this many.
const erc1155ProbeTokenLogs = 2

// uriProbeContract emitted 25 URI logs — one per token id 1..25 — in a single
// block, used to probe the erc1155Id filter's URI arm (the id is in topic1, not
// data). Verified live 2026-08-31.
var uriProbeContract = common.HexToAddress("0xd0e4847359ae76c2786d242e5f45c4f6f1abd752")

// uriProbeBlock holds uriProbeContract's 25 URI logs.
const uriProbeBlock = 6_938_761

// uriProbeTokenID is one of the token ids with a URI log in uriProbeBlock; for
// URI the id is the indexed topic1.
var uriProbeTokenID = common.BigToHash(big.NewInt(5))

// uriProbeTokenLogs is how many URI logs uriProbeTokenID has in uriProbeBlock
// (verified live); the probe requires exactly this many.
const uriProbeTokenLogs = 1

// ChainSupportsWarehouseERC1155Filter reports whether a warehouse for chain is
// verified to apply the erc1155Id filter — i.e. whether LogWarehouseRequirements
// adds the capability probes for it. It is the single source the indexer uses
// both to add those probes and to decide whether to send the erc1155Id hint
// (adapters.NewERC1155Adapter): the hint must never be sent on a chain whose
// warehouse the probes do not cover, or it would be trusted unverified.
func ChainSupportsWarehouseERC1155Filter(chain domain.Chain) bool {
	return chain == domain.ChainEthereumMainnet
}

// LogWarehouseRequirements returns what the warehouse must satisfy before the
// indexer routes history through it for the given chain: the chain id, and on
// mainnet three capability probes — the CryptoPunks internal Transfer, and the
// erc1155Id filter's TransferSingle and URI arms.
//
// Reason: each probe guards a call site whose correctness depends on a shape or
// capability the warehouse's answer must include, and which a routing client
// cannot otherwise detect (the warehouse-covered range has no vendor backstop):
//   - CryptoPunks internal Transfer: the owner scan discovers corrupted
//     acceptBidForPunk purchases (a PunkBought whose indexed buyer is zero)
//     only through that 3-topic log under the ERC-721 Transfer signature — the
//     shape a warehouse build drops as "ERC-20".
//   - erc1155Id TransferSingle / URI arms: GetTokenEvents sends the token id so
//     a per-token ERC-1155 history walk is an index point lookup, not a
//     whole-contract scan. A warehouse that ignores the warehouse-only field
//     (an older build) would restore the scan; one that misfilters would drop
//     the token's history. Both arms are probed because the id lives in a
//     different place per signature (TransferSingle data word 0, URI topic1).
//
// A failed probe is fatal at startup (dialLogWarehouse returns the error and
// worker init stops) — the same "fail loud on a misconfigured warehouse" rule
// the chain-id and CryptoPunks checks already use. So a warehouse that predates
// the erc1155Id filter (ff-eth-logs #8) fails these probes and the indexer will
// not start: #8 must be deployed to the warehouse BEFORE an indexer image that
// carries these requirements. (A warehouse that is merely unreachable at
// startup is tolerated and re-verified on first use; only an answered-but-failed
// probe is fatal.)
func LogWarehouseRequirements(chain domain.Chain) (adapter.LogWarehouseRequirements, error) {
	id, ok := chain.EIP155NumericID()
	if !ok || id < 0 {
		return adapter.LogWarehouseRequirements{}, fmt.Errorf("log warehouse requires an eip155 chain, got %q", chain)
	}
	reqs := adapter.LogWarehouseRequirements{ChainID: uint64(id)}
	if ChainSupportsWarehouseERC1155Filter(chain) {
		reqs.Probes = append(reqs.Probes, cryptoPunksInternalTransferProbe(), erc1155IDFilterProbe(), erc1155URIFilterProbe())
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
			// The block holds exactly two TransferSingle logs for this token;
			// require both so a partially populated index that returns only one
			// is refused, not trusted.
			return matched == erc1155ProbeTokenLogs
		},
	}
}

// erc1155URIFilterProbe verifies the erc1155Id filter's URI arm: a URI log
// carries its token id in the indexed topic1, so the warehouse must filter it
// there (not by a data word). GetTokenEvents sends one erc1155Id-filtered query
// covering both TransferSingle and URI, so a warehouse that filters transfers
// but drops or misfilters URIs would silently omit metadata-update provenance
// on the covered range. The probe asks uriProbeBlock (25 URI logs across 25
// token ids) for uriProbeTokenID and accepts only when every returned URI
// carries that id in topic1 and at least one does: an ignoring warehouse
// returns the other 24 ids (rejected), a dropping one returns nothing
// (rejected).
func erc1155URIFilterProbe() adapter.LogWarehouseProbe {
	block := new(big.Int).SetUint64(uriProbeBlock)
	id := uriProbeTokenID
	return adapter.LogWarehouseProbe{
		Name: "ERC-1155 URI erc1155Id filter",
		Query: ethereum.FilterQuery{
			FromBlock: block,
			ToBlock:   block,
			Addresses: []common.Address{uriProbeContract},
			Topics:    [][]common.Hash{{helpers.ERC1155URIEventSignature}},
		},
		ERC1155ID: &id,
		Accept: func(logs []types.Log) bool {
			matched := 0
			for _, l := range logs {
				// URI carries the token id in the indexed topic1.
				if len(l.Topics) < 2 || l.Topics[1] != id {
					return false
				}
				matched++
			}
			// Exactly one URI log for this token in the block.
			return matched == uriProbeTokenLogs
		},
	}
}

// LogWarehouseHead returns the warehouse head and true when a log warehouse is
// configured and answers; (0, false) when none is configured, it is
// unreachable, or it was refused. Callers plan work around the head (the
// owner scan sizes its windows by it) but never depend on it: every log fetch
// re-checks the head and applies the configured outage policy on its own (fail
// in strict mode, or fall through to the vendor — see
// helpers.PaginationGuards.LogWarehouseVendorFallthrough).
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
