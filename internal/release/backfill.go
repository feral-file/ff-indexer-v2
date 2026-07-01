package release

import (
	"context"
	"fmt"
	"strings"

	"go.uber.org/zap"

	"github.com/feral-file/ff-indexer-v2/internal/adapter"
	"github.com/feral-file/ff-indexer-v2/internal/domain"
	"github.com/feral-file/ff-indexer-v2/internal/logger"
	"github.com/feral-file/ff-indexer-v2/internal/providers/vendors/artblocks"
	"github.com/feral-file/ff-indexer-v2/internal/providers/vendors/feralfile"
	"github.com/feral-file/ff-indexer-v2/internal/store"
	"github.com/feral-file/ff-indexer-v2/internal/store/schema"
)

const defaultBatchSize = 500

// Backfiller populates releases and release_members from existing enrichment sources.
type Backfiller struct {
	store           store.Store
	feralfileClient feralfile.Client
	json            adapter.JSON
	batchSize       int
}

// NewBackfiller creates a release backfill runner.
func NewBackfiller(store store.Store, feralfileClient feralfile.Client, json adapter.JSON, batchSize int) *Backfiller {
	if batchSize <= 0 {
		batchSize = defaultBatchSize
	}
	return &Backfiller{
		store:           store,
		feralfileClient: feralfileClient,
		json:            json,
		batchSize:       batchSize,
	}
}

type ffVendorArtwork struct {
	SeriesID string `json:"seriesID"`
	Index    int64  `json:"index"`
	Series   struct {
		ID string `json:"ID"`
	} `json:"series"`
}

// Run backfills all artblocks and feralfile enrichment sources.
//
// Best-effort: per-row failures are logged and counted rather than aborting the run,
// so a transient API error or malformed vendor JSON does not prevent other tokens from
// being processed. However, a non-nil error is returned when any row fails so that the
// caller (cmd/backfill-releases) exits non-zero and the operator can detect partial
// backfills and re-run. The returned int is always the number of successfully processed
// rows regardless of failures.
func (b *Backfiller) Run(ctx context.Context) (int, error) {
	vendors := []schema.Vendor{schema.VendorArtBlocks, schema.VendorFeralFile}
	var offset uint64
	processed := 0
	var failed int

	for {
		sources, err := b.store.ListEnrichmentSourcesByVendors(ctx, vendors, b.batchSize, offset)
		if err != nil {
			return processed, err
		}
		if len(sources) == 0 {
			break
		}

		for _, source := range sources {
			if err := b.backfillSource(ctx, source); err != nil {
				logger.WarnCtx(ctx, "Failed to backfill release for enrichment source",
					zap.Uint64("token_id", source.TokenID),
					zap.String("vendor", string(source.Vendor)),
					zap.Error(err))
				failed++
				continue
			}
			processed++
		}

		if len(sources) < b.batchSize {
			break
		}
		offset += uint64(len(sources))
	}

	if failed > 0 {
		return processed, fmt.Errorf("partial backfill: %d succeeded, %d failed", processed, failed)
	}

	return processed, nil
}

func (b *Backfiller) backfillSource(ctx context.Context, source schema.EnrichmentSource) error {
	token, err := b.store.GetTokenByID(ctx, source.TokenID)
	if err != nil {
		return fmt.Errorf("get token: %w", err)
	}
	if token == nil {
		return fmt.Errorf("token %d not found", source.TokenID)
	}

	var vendorReleaseID string
	var mintNumber int64
	var releaseMeta *Metadata

	switch source.Vendor {
	case schema.VendorArtBlocks:
		vendorReleaseID, mintNumber, err = ReleaseInfoFromArtBlocks(token.Chain, token.ContractAddress, token.TokenNumber)
		if err == nil {
			releaseMeta = MetadataFromArtBlocksVendorJSON(source.VendorJSON, b.json)
		}
	case schema.VendorFeralFile:
		vendorReleaseID, mintNumber, releaseMeta, err = b.feralFileReleaseInfo(ctx, source, token.TokenNumber)
	default:
		return nil
	}
	if err != nil {
		return err
	}
	if vendorReleaseID == "" {
		return fmt.Errorf("missing vendor release id")
	}

	var name *string
	var totalMints *int64
	if releaseMeta != nil {
		name = releaseMeta.Name
		totalMints = releaseMeta.TotalMints
	}

	release, err := b.store.UpsertRelease(ctx, source.Vendor, vendorReleaseID, name, totalMints)
	if err != nil {
		return fmt.Errorf("upsert release: %w", err)
	}
	if err := b.store.UpsertReleaseMember(ctx, release.ID, source.TokenID, mintNumber); err != nil {
		return fmt.Errorf("upsert release member: %w", err)
	}
	return nil
}

// ReleaseInfoFromArtBlocks derives AB release membership from the on-chain token ID encoding.
//
// ParseArtBlocksTokenID returns a 0-based mint index (tokenID % 1_000_000).
// The returned mintNumber is 1-based to match the schema contract and FF convention
// (first token of a project is mint_number 1, not 0).
//
// The vendor_release_id includes the EIP-155 chain ID so that the same contract/project
// on different EVM chains (e.g. mainnet vs L2) produces distinct release rows and does
// not collide on the UNIQUE (vendor, vendor_release_id) constraint.
func ReleaseInfoFromArtBlocks(chain domain.Chain, contractAddress, tokenNumber string) (string, int64, error) {
	evmChainID, ok := chain.EIP155NumericID()
	if !ok {
		return "", 0, fmt.Errorf("art blocks release requires an eip155 chain, got %q", chain)
	}
	projectID, rawMintIndex, err := artblocks.ParseArtBlocksTokenID(tokenNumber)
	if err != nil {
		return "", 0, err
	}
	vendorReleaseID := fmt.Sprintf("%d-%s-%d", evmChainID, strings.ToLower(contractAddress), projectID)
	return vendorReleaseID, rawMintIndex + 1, nil
}

// ReleaseInfoFromFeralFile derives FF release membership from stored vendor JSON or the FF API.
func (b *Backfiller) ReleaseInfoFromFeralFile(ctx context.Context, source schema.EnrichmentSource, tokenNumber string) (string, int64, error) {
	vendorReleaseID, mintNumber, _, err := b.feralFileReleaseInfo(ctx, source, tokenNumber)
	return vendorReleaseID, mintNumber, err
}

func (b *Backfiller) feralFileReleaseInfo(ctx context.Context, source schema.EnrichmentSource, tokenNumber string) (string, int64, *Metadata, error) {
	if len(source.VendorJSON) > 0 {
		var artwork ffVendorArtwork
		if err := b.json.Unmarshal(source.VendorJSON, &artwork); err == nil {
			seriesID := artwork.SeriesID
			if seriesID == "" {
				seriesID = artwork.Series.ID
			}
			if seriesID != "" {
				return seriesID, artwork.Index + 1, MetadataFromFeralFileVendorJSON(source.VendorJSON, b.json), nil
			}
		}
	}

	artwork, err := b.feralfileClient.GetArtwork(ctx, tokenNumber)
	if err != nil {
		return "", 0, nil, fmt.Errorf("fetch feral file artwork: %w", err)
	}
	seriesID := artwork.SeriesIDOrFallback()
	if seriesID == "" {
		return "", 0, nil, fmt.Errorf("missing series id")
	}
	return seriesID, artwork.Index + 1, MetadataFromFeralFileArtwork(artwork), nil
}
