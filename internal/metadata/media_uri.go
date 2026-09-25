package metadata

import (
	"context"
	"errors"
	"fmt"

	"go.uber.org/zap"

	"github.com/feral-file/ff-indexer-v2/internal/domain"
	"github.com/feral-file/ff-indexer-v2/internal/logger"
	"github.com/feral-file/ff-indexer-v2/internal/types"
	"github.com/feral-file/ff-indexer-v2/internal/uri"
)

// resolveMediaURI selects a normalized media URL without restoring retired hosts.
// Reason: UriToGateway leaves HTTP URLs unchanged, so its generic fallback would
// undo retirement during metadata rebuilds when no replacement validates.
// Trade-offs: fail that rebuild and retry later, preserving existing metadata
// and enrichment. The typed retirement error survives normalization wrappers.
// Constraints: only a validated replacement may migrate a retired gateway;
// native IPFS and other URI failures retain their existing default fallback.
func resolveMediaURI(ctx context.Context, resolver uri.Resolver, source string) (string, error) {
	if source == "" {
		return "", nil
	}
	resolved, err := resolver.Resolve(ctx, source)
	if err == nil {
		return resolved, nil
	}
	if types.IsBrowserIPFSGateway(source) && !errors.Is(err, uri.ErrRetiredGatewayReplacement) {
		err = fmt.Errorf("%w: %w", uri.ErrRetiredGatewayReplacement, err)
	}
	if errors.Is(err, uri.ErrRetiredGatewayReplacement) {
		return "", err
	}
	logger.WarnCtx(ctx, "failed to resolve media URI, fallback to default gateway", zap.Error(err), zap.String("uri", source))
	return domain.UriToGateway(source), nil
}

// resolveRetiredMedia validates every retired HTTP gateway in vendor media and
// moves Feral File CDN URLs to the current origin.
// Reason: vendor HTTP URLs can bypass per-vendor URI resolution, and enrichment
// takes precedence over normalized metadata in display selection.
// Trade-offs: a gateway outage delays other new vendor facts until the next
// refresh, preserving the entire existing enrichment record without an upsert.
// The CDN move is a pure rewrite (same paths, new host), so it needs no probe.
// Constraints: never overwrite migrated playable media with an unvalidated URL.
func (e *enhancer) resolveRetiredMedia(ctx context.Context, enhanced *EnhancedMetadata) error {
	for _, target := range []**string{&enhanced.ImageURL, &enhanced.AnimationURL} {
		if *target == nil {
			continue
		}
		if moved := types.MigrateFeralFileCDN(**target); moved != **target {
			*target = &moved
		}
		if !types.IsBrowserIPFSGateway(**target) {
			continue
		}
		resolved, err := resolveMediaURI(ctx, e.uriResolver, **target)
		if err != nil {
			return fmt.Errorf("failed to replace retired vendor media URI: %w", err)
		}
		*target = &resolved
	}
	return nil
}
