package metadata

import (
	"context"
	"strings"

	"go.uber.org/zap"

	"github.com/gabriel-vasile/mimetype"

	"github.com/feral-file/ff-indexer-v2/internal/adapter"
	"github.com/feral-file/ff-indexer-v2/internal/logger"
	"github.com/feral-file/ff-indexer-v2/internal/types"
	"github.com/feral-file/ff-indexer-v2/internal/uri"
)

// detectMimeType detects the MIME type of the artwork from the resolved URL
// It first checks animation_url, and if not present, falls back to image_url
// Returns nil if no URL is available or if detection fails
func detectMimeType(
	ctx context.Context,
	httpClient adapter.HTTPClient,
	uriResolver uri.Resolver,
	animationURL, imageURL *string,
) *string {
	if animationURL == nil && imageURL == nil {
		return nil
	}

	// Determine which URL to use - prefer animation_url over image_url
	var targetURL string
	if !types.StringNilOrEmpty(animationURL) {
		targetURL = *animationURL
		logger.DebugCtx(ctx, "Using animation_url for mime type detection", zap.String("url", targetURL))
	} else if !types.StringNilOrEmpty(imageURL) {
		targetURL = *imageURL
		logger.DebugCtx(ctx, "Using image_url for mime type detection", zap.String("url", targetURL))
	} else {
		logger.DebugCtx(ctx, "No URL available for mime type detection")
		return nil
	}

	// Resolve the URI to a canonical URL
	resolvedURL, err := uriResolver.Resolve(ctx, targetURL)
	if err != nil {
		logger.WarnCtx(ctx, "Failed to resolve URI for mime type detection",
			zap.String("url", targetURL),
			zap.Error(err))
		return nil
	}

	logger.DebugCtx(ctx, "Resolved URL for mime type detection",
		zap.String("original", targetURL),
		zap.String("resolved", resolvedURL))

	// Try HEAD request first to get content-type
	// If HEAD fails, fallback to partial GET request
	var mimeType string

	// Cut the query params from the URL
	// Some gateways like https://onchfs.fxhash2.xyz don't support HEAD requests with query params
	// so cut the query params from the URL will be safe to get the content-type
	headURL := resolvedURL
	if parts := strings.Split(headURL, "?"); len(parts) > 0 {
		headURL = parts[0]
	}
	resp, err := httpClient.Head(ctx, headURL)
	if err == nil && resp.StatusCode >= 200 && resp.StatusCode < 300 {
		mimeType = resp.Header.Get("Content-Type")
	}

	if mimeType == "" {
		// Download first 512 bytes for mime type detection
		const maxBytes = 512
		content, err := httpClient.GetPartialBytes(ctx, resolvedURL, maxBytes)
		if err != nil {
			logger.WarnCtx(ctx, "Failed to download content for mime type detection",
				zap.String("url", resolvedURL),
				zap.Error(err))
			return nil
		}

		// Detect mime type using the mimetype library
		mtype := mimetype.Detect(content)
		if mtype == nil {
			logger.WarnCtx(ctx, "Failed to detect mime type",
				zap.String("url", resolvedURL))
			return nil
		}

		mimeType = mtype.String()
	}

	logger.InfoCtx(ctx, "Detected mime type",
		zap.String("url", resolvedURL),
		zap.String("mimeType", mimeType))

	return &mimeType
}
