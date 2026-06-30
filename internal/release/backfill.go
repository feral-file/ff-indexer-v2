package release

import (
	"context"
	"fmt"
	"strings"

	"go.uber.org/zap"

	"github.com/feral-file/ff-indexer-v2/internal/adapter"
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
func (b *Backfiller) Run(ctx context.Context) (int, error) {
	vendors := []schema.Vendor{schema.VendorArtBlocks, schema.VendorFeralFile}
	var offset uint64
	processed := 0

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
				continue
			}
			processed++
		}

		if len(sources) < b.batchSize {
			break
		}
		offset += uint64(len(sources))
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

	switch source.Vendor {
	case schema.VendorArtBlocks:
		vendorReleaseID, mintNumber, err = ReleaseInfoFromArtBlocks(token.ContractAddress, token.TokenNumber)
	case schema.VendorFeralFile:
		vendorReleaseID, mintNumber, err = b.ReleaseInfoFromFeralFile(ctx, source, token.TokenNumber)
	default:
		return nil
	}
	if err != nil {
		return err
	}
	if vendorReleaseID == "" {
		return fmt.Errorf("missing vendor release id")
	}

	release, err := b.store.UpsertRelease(ctx, source.Vendor, vendorReleaseID)
	if err != nil {
		return fmt.Errorf("upsert release: %w", err)
	}
	if err := b.store.UpsertReleaseMember(ctx, release.ID, source.TokenID, mintNumber); err != nil {
		return fmt.Errorf("upsert release member: %w", err)
	}
	return nil
}

// ReleaseInfoFromArtBlocks derives AB release membership from on-chain token id encoding.
func ReleaseInfoFromArtBlocks(contractAddress, tokenNumber string) (string, int64, error) {
	projectID, mintNumber, err := artblocks.ParseArtBlocksTokenID(tokenNumber)
	if err != nil {
		return "", 0, err
	}
	vendorReleaseID := fmt.Sprintf("%s-%d", strings.ToLower(contractAddress), projectID)
	return vendorReleaseID, mintNumber, nil
}

// ReleaseInfoFromFeralFile derives FF release membership from stored vendor JSON or the FF API.
func (b *Backfiller) ReleaseInfoFromFeralFile(ctx context.Context, source schema.EnrichmentSource, tokenNumber string) (string, int64, error) {
	if len(source.VendorJSON) > 0 {
		var artwork ffVendorArtwork
		if err := b.json.Unmarshal(source.VendorJSON, &artwork); err == nil {
			seriesID := artwork.SeriesID
			if seriesID == "" {
				seriesID = artwork.Series.ID
			}
			if seriesID != "" {
				return seriesID, artwork.Index + 1, nil
			}
		}
	}

	artwork, err := b.feralfileClient.GetArtwork(ctx, tokenNumber)
	if err != nil {
		return "", 0, fmt.Errorf("fetch feral file artwork: %w", err)
	}
	seriesID := artwork.SeriesIDOrFallback()
	if seriesID == "" {
		return "", 0, fmt.Errorf("missing series id")
	}
	return seriesID, artwork.Index + 1, nil
}
