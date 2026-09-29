package workflows

import (
	"context"
	"fmt"

	"github.com/feral-file/ff-indexer-v2/internal/metadata"
	"github.com/feral-file/ff-indexer-v2/internal/store/schema"
	"github.com/feral-file/ff-indexer-v2/internal/types"
	"github.com/feral-file/ff-indexer-v2/internal/uri"
)

// keepStoredMetadataMedia keeps the token's stored image and animation URLs when the
// freshly resolved ones address the same IPFS content through another gateway.
//
// Reason: see uri.PreferStoredGatewayURL. It runs before the metadata upsert so an
// unchanged token writes unchanged URLs and its health rows and media assets stay
// attached.
// Constraints: each field is compared with its own stored value only, so a re-index
// of unchanged metadata changes nothing at all.
func keepStoredMetadataMedia(resolved *metadata.NormalizedMetadata, stored *schema.TokenMetadata) {
	if stored == nil {
		return
	}
	resolved.Image = uri.PreferStoredGatewayURL(resolved.Image, stored.ImageURL)
	resolved.Animation = uri.PreferStoredGatewayURL(resolved.Animation, stored.AnimationURL)
}

// keepStoredEnrichmentMedia is keepStoredMetadataMedia for vendor enrichment: it
// keeps the URLs stored on the token's enrichment source.
//
// Trade-offs: the stored enrichment source is read only when the vendor media is an
// IPFS gateway URL, so vendors serving plain HTTP media cost no extra query.
// Constraints: a failed read is returned rather than ignored, because continuing
// would silently store the race winner and detach the token's media assets.
func (e *coreExecutor) keepStoredEnrichmentMedia(ctx context.Context, tokenID uint64, enhanced *metadata.EnhancedMetadata) error {
	if !isIPFSGatewayURL(enhanced.ImageURL) && !isIPFSGatewayURL(enhanced.AnimationURL) {
		return nil
	}
	stored, err := e.store.GetEnrichmentSourceByTokenID(ctx, tokenID)
	if err != nil {
		return fmt.Errorf("failed to get stored enrichment source: %w", err)
	}
	if stored == nil {
		return nil
	}
	if enhanced.ImageURL != nil {
		enhanced.ImageURL = types.StringPtr(uri.PreferStoredGatewayURL(*enhanced.ImageURL, stored.ImageURL))
	}
	if enhanced.AnimationURL != nil {
		enhanced.AnimationURL = types.StringPtr(uri.PreferStoredGatewayURL(*enhanced.AnimationURL, stored.AnimationURL))
	}
	return nil
}

// isIPFSGatewayURL reports whether url is set and is an IPFS gateway URL.
func isIPFSGatewayURL(url *string) bool {
	if url == nil {
		return false
	}
	ok, _ := types.IsIPFSGatewayURL(*url)
	return ok
}
