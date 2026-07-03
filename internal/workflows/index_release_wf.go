package workflows

// IndexRelease workflow: derives token CIDs for a vendor release within a mint range and
// fans them into chunked IndexTokens jobs.
//
// This file implements the IndexRelease job handler registered in worker_core.go.
// Phase 1 (this handler) is fast: it derives CIDs and enqueues child jobs, then exits.
// Phase 2 (IndexTokens children) runs independently on the same queue.
//
// Vendor CID derivation strategies:
//
//   artblocks: deterministic — vendor_release_id="{chainID}-{contract}-{projectID}";
//              token_number = projectID*1_000_000 + (mintNumber-1);
//              CID format: eip155:{chainID}:erc721:{contract}:{token_number}
//
//   objkt:     deterministic — vendor_release_id = KT1 contract address;
//              token_number = mintNumber (1-based direct equality);
//              CID format: tezos:mainnet:fa2:{contract}:{mint_number}
//
//   fxhash:    requires GetGentksByIteration API call (gentk IDs are global, not derived by math);
//              CID format: tezos:mainnet:fa2:{contract}:{tokenID}
//
//   feralfile: requires GetSeriesArtworks API call (artworks sorted by index);
//              CID format depends on resolved chain: eip155:1:erc721 or tezos:mainnet:fa2;
//              artworks still on Bitmark (chain=="bitmark") are skipped.

import (
	"context"
	"fmt"
	"math"
	"strconv"
	"strings"

	"go.uber.org/zap"

	"github.com/feral-file/ff-indexer-v2/internal/domain"
	"github.com/feral-file/ff-indexer-v2/internal/logger"
	"github.com/feral-file/ff-indexer-v2/internal/providers/jobs"
	"github.com/feral-file/ff-indexer-v2/internal/store/schema"
)

// indexReleaseChunkSize is the number of token CIDs per IndexTokens child job.
// Matches the MAX_TOKEN_CIDS_PER_REQUEST cap so each child job is within the
// same bound as a manual triggerTokenIndexing call.
const indexReleaseChunkSize = 50

// IndexRelease is the job handler for the "IndexRelease" job kind.
//
// It derives token CIDs for [mintFrom, mintTo] using per-vendor strategies,
// skips any CIDs that cannot be derived (logging the count), then chunks the
// valid CIDs into IndexTokens child jobs of indexReleaseChunkSize each.
//
// Reason this is a separate job (not inline in the HTTP handler): fxhash and
// Feral File require external API calls to resolve mint numbers to on-chain IDs.
// Doing that synchronously in an HTTP request risks timeouts for large ranges.
// The job model also provides retry semantics for transient API failures.
func (w *coreWorkflows) IndexRelease(ctx context.Context, vendor string, vendorReleaseID string, mintFrom int64, mintTo int64) error {
	logger.InfoCtx(ctx, "IndexRelease started",
		zap.String("vendor", vendor),
		zap.String("vendorReleaseID", vendorReleaseID),
		zap.Int64("mintFrom", mintFrom),
		zap.Int64("mintTo", mintTo),
	)

	cids, skipped, err := w.deriveReleaseCIDs(ctx, schema.Vendor(vendor), vendorReleaseID, mintFrom, mintTo)
	if err != nil {
		return fmt.Errorf("IndexRelease: CID derivation failed for %s/%s [%d..%d]: %w", vendor, vendorReleaseID, mintFrom, mintTo, err)
	}

	logger.InfoCtx(ctx, "IndexRelease: CIDs derived",
		zap.String("vendor", vendor),
		zap.String("vendorReleaseID", vendorReleaseID),
		zap.Int("tokensQueued", len(cids)),
		zap.Int("tokensSkipped", skipped),
	)

	if len(cids) == 0 {
		logger.InfoCtx(ctx, "IndexRelease: no CIDs to index, exiting",
			zap.String("vendor", vendor),
			zap.String("vendorReleaseID", vendorReleaseID),
		)
		return nil
	}

	// Enqueue IndexTokens child jobs in chunks.
	for i := 0; i < len(cids); i += indexReleaseChunkSize {
		end := i + indexReleaseChunkSize
		if end > len(cids) {
			end = len(cids)
		}
		chunk := cids[i:end]

		uk := fmt.Sprintf("index-release-%s-%s-%d-%d", vendor, vendorReleaseID, i, end)
		_, _, err := w.jobQueue.Enqueue(ctx, jobs.EnqueueOptions{
			Queue:     w.config.TokenTaskQueue,
			Kind:      "IndexTokens",
			Args:      []any{chunk, nil},
			UniqueKey: &uk,
		})
		if err != nil {
			return fmt.Errorf("IndexRelease: failed to enqueue IndexTokens chunk [%d:%d]: %w", i, end, err)
		}
	}

	logger.InfoCtx(ctx, "IndexRelease: all chunks enqueued",
		zap.String("vendor", vendor),
		zap.String("vendorReleaseID", vendorReleaseID),
		zap.Int("chunks", int(math.Ceil(float64(len(cids))/float64(indexReleaseChunkSize)))),
		zap.Int("tokensQueued", len(cids)),
		zap.Int("tokensSkipped", skipped),
	)

	return nil
}

// deriveReleaseCIDs resolves the full set of token CIDs for [mintFrom, mintTo] using
// the per-vendor strategy. Returns (cids, skippedCount, error).
//
// skippedCount counts mint positions for which a CID could not be derived:
// Bitmark-origin FF artworks not yet swapped, or vendor API gaps.
func (w *coreWorkflows) deriveReleaseCIDs(ctx context.Context, vendor schema.Vendor, vendorReleaseID string, mintFrom, mintTo int64) ([]domain.TokenCID, int, error) {
	switch vendor {
	case schema.VendorArtBlocks:
		return deriveArtBlocksCIDs(vendorReleaseID, mintFrom, mintTo)
	case schema.VendorObjkt:
		return deriveObjktCIDs(vendorReleaseID, mintFrom, mintTo)
	case schema.VendorFXHash:
		return w.deriveFxhashCIDs(ctx, vendorReleaseID, mintFrom, mintTo)
	case schema.VendorFeralFile:
		return w.deriveFeralFileCIDs(ctx, vendorReleaseID, mintFrom, mintTo)
	default:
		return nil, 0, fmt.Errorf("unsupported vendor for release indexing: %s", vendor)
	}
}

// deriveArtBlocksCIDs computes Art Blocks token CIDs from vendor_release_id and mint range.
//
// Art Blocks vendor_release_id format: "{chainID}-{contract}-{projectID}"
// Token number formula: projectID * 1_000_000 + (mintNumber - 1)
// CID format: eip155:{chainID}:erc721:{lowercaseContract}:{tokenNumber}
//
// This is fully deterministic — zero API calls required. The formula is the inverse
// of ParseArtBlocksTokenID used in the metadata enhancer.
func deriveArtBlocksCIDs(vendorReleaseID string, mintFrom, mintTo int64) ([]domain.TokenCID, int, error) {
	// Parse: first "-" separates chainID from the rest; last "-" separates contract from projectID.
	firstSep := strings.Index(vendorReleaseID, "-")
	if firstSep < 0 {
		return nil, 0, fmt.Errorf("invalid artblocks vendor_release_id (no separator): %q", vendorReleaseID)
	}
	chainIDStr := vendorReleaseID[:firstSep]
	rest := vendorReleaseID[firstSep+1:]

	lastSep := strings.LastIndex(rest, "-")
	if lastSep < 0 {
		return nil, 0, fmt.Errorf("invalid artblocks vendor_release_id (missing project separator): %q", vendorReleaseID)
	}
	contract := rest[:lastSep]
	projectIDStr := rest[lastSep+1:]

	chainID, err := strconv.ParseInt(chainIDStr, 10, 64)
	if err != nil {
		return nil, 0, fmt.Errorf("invalid artblocks vendor_release_id chain ID %q: %w", chainIDStr, err)
	}
	projectID, err := strconv.ParseInt(projectIDStr, 10, 64)
	if err != nil {
		return nil, 0, fmt.Errorf("invalid artblocks vendor_release_id project ID %q: %w", projectIDStr, err)
	}

	chain := domain.Chain(fmt.Sprintf("eip155:%d", chainID))

	cids := make([]domain.TokenCID, 0, mintTo-mintFrom+1)
	for mintNum := mintFrom; mintNum <= mintTo; mintNum++ {
		tokenNumber := projectID*artblocksTokenIDMultiplier + (mintNum - 1)
		cid := domain.NewTokenCID(chain, domain.StandardERC721, contract, strconv.FormatInt(tokenNumber, 10))
		cids = append(cids, cid)
	}
	return cids, 0, nil
}

// artblocksTokenIDMultiplier is the multiplier used by Art Blocks to encode (projectID, mintIndex)
// into a single token number: tokenNumber = projectID * 1_000_000 + mintIndex.
// Defined here to avoid importing the artblocks vendor package (which would create a circular dep).
const artblocksTokenIDMultiplier = int64(1_000_000)

// deriveObjktCIDs computes objkt token CIDs from vendor_release_id (KT1 address) and mint range.
//
// objkt vendor_release_id is the FA2 contract address (KT1...).
// Token number equals mint number directly (1-based, confirmed from metadata enhancer).
// CID format: tezos:mainnet:fa2:{contract}:{mint_number}
func deriveObjktCIDs(vendorReleaseID string, mintFrom, mintTo int64) ([]domain.TokenCID, int, error) {
	cids := make([]domain.TokenCID, 0, mintTo-mintFrom+1)
	for mintNum := mintFrom; mintNum <= mintTo; mintNum++ {
		cid := domain.NewTokenCID(domain.ChainTezosMainnet, domain.StandardFA2, vendorReleaseID, strconv.FormatInt(mintNum, 10))
		cids = append(cids, cid)
	}
	return cids, 0, nil
}

// deriveFxhashCIDs resolves fxhash token CIDs via GetGentksByIteration.
//
// fxhash gentk token IDs are global integers assigned at mint time and cannot be
// derived from iteration numbers by math. The API call is required.
// CID format: tezos:mainnet:fa2:{contract}:{tokenID}
func (w *coreWorkflows) deriveFxhashCIDs(ctx context.Context, vendorReleaseID string, mintFrom, mintTo int64) ([]domain.TokenCID, int, error) {
	if w.fxhashClient == nil {
		return nil, 0, fmt.Errorf("fxhash client not configured: cannot index fxhash release %q", vendorReleaseID)
	}

	refs, err := w.fxhashClient.GetGentksByIteration(ctx, vendorReleaseID, mintFrom, mintTo)
	if err != nil {
		return nil, 0, fmt.Errorf("fxhash GetGentksByIteration failed for %q [%d..%d]: %w", vendorReleaseID, mintFrom, mintTo, err)
	}

	cids := make([]domain.TokenCID, 0, len(refs))
	for _, ref := range refs {
		cid := domain.NewTokenCID(domain.ChainTezosMainnet, domain.StandardFA2, ref.ContractAddress, ref.TokenID)
		cids = append(cids, cid)
	}

	// skipped = range size minus resolved count (gaps or API returns fewer than requested).
	skipped := int(mintTo-mintFrom+1) - len(refs)
	if skipped < 0 {
		skipped = 0
	}

	return cids, skipped, nil
}

// deriveFeralFileCIDs resolves Feral File token CIDs via GetSeriesArtworks.
//
// Feral File artworks carry a resolved chain/contractAddress/tokenID from the API.
// Artworks still on Bitmark (chain=="bitmark") have no EVM/Tezos identity and are
// skipped; the caller is informed via tokensSkipped.
//
// Chain mapping: "ethereum" → eip155:1:erc721; "tezos" → tezos:mainnet:fa2
func (w *coreWorkflows) deriveFeralFileCIDs(ctx context.Context, vendorReleaseID string, mintFrom, mintTo int64) ([]domain.TokenCID, int, error) {
	if w.feralfileClient == nil {
		return nil, 0, fmt.Errorf("Feral File client not configured: cannot index Feral File release %q", vendorReleaseID)
	}

	artworks, err := w.feralfileClient.GetSeriesArtworks(ctx, vendorReleaseID, mintFrom, mintTo)
	if err != nil {
		return nil, 0, fmt.Errorf("Feral File GetSeriesArtworks failed for series %q [%d..%d]: %w", vendorReleaseID, mintFrom, mintTo, err)
	}

	cids := make([]domain.TokenCID, 0, len(artworks))
	skipped := 0

	for _, artwork := range artworks {
		chain, standard, skip := mapFeralFileChain(artwork.Chain)
		if skip {
			// Artwork is still on Bitmark — no EVM/Tezos identity yet.
			logger.InfoCtx(ctx, "IndexRelease: skipping unswapped Bitmark artwork",
				zap.String("seriesID", vendorReleaseID),
				zap.Int64("index", artwork.Index),
			)
			skipped++
			continue
		}
		if artwork.ContractAddress == "" || artwork.TokenID == "" {
			logger.WarnCtx(ctx, "IndexRelease: skipping artwork with missing contract or token ID",
				zap.String("seriesID", vendorReleaseID),
				zap.Int64("index", artwork.Index),
				zap.String("chain", artwork.Chain),
			)
			skipped++
			continue
		}

		cid := domain.NewTokenCID(chain, standard, artwork.ContractAddress, artwork.TokenID)
		cids = append(cids, cid)
	}

	// Account for artworks not returned by the API (range beyond current supply).
	apiSkipped := int(mintTo-mintFrom+1) - len(artworks)
	if apiSkipped > 0 {
		skipped += apiSkipped
	}

	return cids, skipped, nil
}

// mapFeralFileChain maps a Feral File API chain string to the indexer's domain.Chain
// and domain.ChainStandard values.
//
// FF uses "ethereum" for mainnet ERC-721 deployments. The function returns skip=true
// for "bitmark" because Bitmark-origin artworks have no EVM/Tezos on-chain identity
// until a swap completes.
//
// Constraint: only mainnet chains are supported here. FF does not currently list
// testnet deployments through the production artworks API.
func mapFeralFileChain(chain string) (domain.Chain, domain.ChainStandard, bool) {
	switch strings.ToLower(chain) {
	case "ethereum":
		return domain.ChainEthereumMainnet, domain.StandardERC721, false
	case "tezos":
		return domain.ChainTezosMainnet, domain.StandardFA2, false
	default:
		// Includes "bitmark" and any unrecognised chain string.
		return "", "", true
	}
}

