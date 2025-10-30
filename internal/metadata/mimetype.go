package metadata

import (
	"context"

	logger "github.com/bitmark-inc/autonomy-logger"
	"github.com/gabriel-vasile/mimetype"
	"go.uber.org/zap"

	"github.com/feral-file/ff-indexer-v2/internal/adapter"
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
		logger.Debug("Using animation_url for mime type detection", zap.String("url", targetURL))
	} else if !types.StringNilOrEmpty(imageURL) {
		targetURL = *imageURL
		logger.Debug("Using image_url for mime type detection", zap.String("url", targetURL))
	} else {
		logger.Debug("No URL available for mime type detection")
		return nil
	}

	// Resolve the URI to a canonical URL
	resolvedURL, err := uriResolver.Resolve(ctx, targetURL)
	if err != nil {
		logger.Warn("Failed to resolve URI for mime type detection",
			zap.String("url", targetURL),
			zap.Error(err))
		return nil
	}

	logger.Debug("Resolved URL for mime type detection",
		zap.String("original", targetURL),
		zap.String("resolved", resolvedURL))

	// Download first 512 bytes for mime type detection
	const maxBytes = 512
	content, err := httpClient.GetPartialContent(ctx, resolvedURL, maxBytes)
	if err != nil {
		logger.Warn("Failed to download content for mime type detection",
			zap.String("url", resolvedURL),
			zap.Error(err))
		return nil
	}

	// Detect mime type using the mimetype library
	mtype := mimetype.Detect(content)
	if mtype == nil {
		logger.Warn("Failed to detect mime type",
			zap.String("url", resolvedURL))
		return nil
	}

	mimeTypeStr := mtype.String()
	logger.Info("Detected mime type",
		zap.String("url", resolvedURL),
		zap.String("mimeType", mimeTypeStr))

	return &mimeTypeStr
}
