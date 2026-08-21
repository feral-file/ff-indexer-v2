package adapters

import (
	"bytes"
	"context"
	"math/big"
	"sort"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"

	"github.com/feral-file/ff-indexer-v2/internal/providers/ethereum/helpers"
)

// OwnerQuerySpec declares one owner-scoped eth_getLogs shape an adapter needs for
// owner discovery: the event signatures to OR in topics[0] and the topic position
// where the owner address appears.
//
// Reason: eth_getLogs ORs values within a topic position and ANDs across positions,
// so specs sharing OwnerTopicIndex can be merged into ONE query by unioning their
// signatures — the union returns exactly the union of the per-spec results. On a
// span-capped provider (Infura: 10k blocks) every query walks the full block range
// regardless of matches, so the number of merged queries IS the RPC cost multiplier
// of a wallet scan. Declaring shapes instead of fetching per adapter lets the client
// merge across adapters: 8 full-range walks (2 ERC-721 + 2 ERC-1155 + 4 CryptoPunks)
// collapse into 3.
//
// Constraints: OwnerTopicIndex must be 1..3 (topic 0 is the signature hash). Merged
// queries drop per-contract address scoping, so any log consumer must tolerate
// same-signature events from foreign contracts (the replay and repair paths already
// filter by contract and topic shape).
type OwnerQuerySpec struct {
	// EventSigs are the topic-0 signature hashes OR'd in one query.
	EventSigs []common.Hash
	// OwnerTopicIndex is the 1-based topic position holding the owner address.
	OwnerTopicIndex int
}

// MergeOwnerQuerySpecs groups specs by owner topic position and unions their
// signatures, producing at most one spec per position (so at most 3).
//
// Reason: this is the cross-adapter credit guard. Each returned spec becomes one
// full-range pagination walk; without merging, every adapter pays its own walks.
//
// Constraints: output is deterministic — positions ascending, signatures sorted by
// byte value — so query shapes are stable across runs and pinnable in tests.
// Specs with no signatures or an out-of-range position are dropped.
func MergeOwnerQuerySpecs(specs []OwnerQuerySpec) []OwnerQuerySpec {
	sigsByIndex := make(map[int]map[common.Hash]struct{})
	for _, spec := range specs {
		if spec.OwnerTopicIndex < 1 || spec.OwnerTopicIndex > 3 {
			continue
		}
		set := sigsByIndex[spec.OwnerTopicIndex]
		if set == nil {
			set = make(map[common.Hash]struct{})
			sigsByIndex[spec.OwnerTopicIndex] = set
		}
		for _, sig := range spec.EventSigs {
			set[sig] = struct{}{}
		}
	}

	merged := make([]OwnerQuerySpec, 0, len(sigsByIndex))
	for index := 1; index <= 3; index++ {
		set := sigsByIndex[index]
		if len(set) == 0 {
			continue
		}
		sigs := make([]common.Hash, 0, len(set))
		for sig := range set {
			sigs = append(sigs, sig)
		}
		sort.Slice(sigs, func(i, j int) bool {
			return bytes.Compare(sigs[i][:], sigs[j][:]) < 0
		})
		merged = append(merged, OwnerQuerySpec{EventSigs: sigs, OwnerTopicIndex: index})
	}
	return merged
}

// BuildOwnerQueries turns specs into concrete filter queries for one owner and
// block range. A non-empty scope restricts matches to those contract addresses;
// the client's merged cross-adapter scan passes no scope, while a standalone
// adapter scan (GetTokensByOwner) scopes to its own contract.
func BuildOwnerQueries(
	specs []OwnerQuerySpec,
	ownerHash common.Hash,
	scope []common.Address,
	fromBlock, toBlock uint64,
) []ethereum.FilterQuery {
	queries := make([]ethereum.FilterQuery, 0, len(specs))
	for _, spec := range specs {
		if len(spec.EventSigs) == 0 || spec.OwnerTopicIndex < 1 || spec.OwnerTopicIndex > 3 {
			continue
		}
		topics := make([][]common.Hash, spec.OwnerTopicIndex+1)
		topics[0] = spec.EventSigs
		topics[spec.OwnerTopicIndex] = []common.Hash{ownerHash}

		queries = append(queries, ethereum.FilterQuery{
			FromBlock: new(big.Int).SetUint64(fromBlock),
			ToBlock:   new(big.Int).SetUint64(toBlock),
			Addresses: scope,
			Topics:    topics,
		})
	}
	return queries
}

// FetchOwnerLogs builds queries from specs and runs them as concurrent pagination
// walks, returning the merged raw logs.
//
// Reason: this is the single fetch entrypoint for owner scans — the client calls it
// once with cross-adapter merged specs, and adapter-level GetTokensByOwner calls it
// with the adapter's own specs. One entrypoint keeps the query shape and the
// fail-fast cancellation semantics (first error cancels sibling walks) in one place.
//
// Constraints: results are unsorted and may contain duplicates when a log matches
// more than one query; callers dedupe and sort.
func FetchOwnerLogs(
	ctx context.Context,
	pagination *helpers.PaginationHelper,
	specs []OwnerQuerySpec,
	ownerHash common.Hash,
	scope []common.Address,
	fromBlock, toBlock uint64,
) ([]types.Log, error) {
	queries := BuildOwnerQueries(specs, ownerHash, scope, fromBlock, toBlock)
	return filterLogsInParallel(ctx, pagination, queries)
}
