//go:build cgo

package workflowsmedia

import (
	"context"
	"errors"
	"fmt"

	"go.uber.org/zap"

	"github.com/feral-file/ff-indexer-v2/internal/domain"
	"github.com/feral-file/ff-indexer-v2/internal/logger"
	mediaprocessor "github.com/feral-file/ff-indexer-v2/internal/media/processor"
	"github.com/feral-file/ff-indexer-v2/internal/providers/cloudflare"
	"github.com/feral-file/ff-indexer-v2/internal/store"
	"github.com/feral-file/ff-indexer-v2/internal/store/schema"
	"github.com/feral-file/ff-indexer-v2/internal/types"
)

// Executor defines the interface for executing media-related activities
//
//go:generate mockgen -source=executor.go -destination=../../mocks/executor_media.go -package=mocks -mock_names=Executor=MockMediaExecutor
type Executor interface {
	// IndexMediaFile processes a media file by downloading, uploading to storage, and storing metadata
	IndexMediaFile(ctx context.Context, url string) error
}

// executor implements the media Executor interface
type executor struct {
	store          store.Store
	mediaProcessor mediaprocessor.Processor
}

// NewExecutor creates a new media executor instance
func NewExecutor(
	st store.Store,
	mediaProc mediaprocessor.Processor,
) Executor {
	return &executor{
		store:          st,
		mediaProcessor: mediaProc,
	}
}

// IndexMediaFile processes a media file by downloading, uploading to storage, and storing metadata
func (e *executor) IndexMediaFile(ctx context.Context, url string) error {
	if url == "" {
		return fmt.Errorf("media URL is empty")
	}
	if !types.IsHTTPSURL(url) {
		return fmt.Errorf("only HTTPS URLs are supported: %s", url)
	}

	// Check if media asset already exists
	existingAsset, err := e.store.GetMediaAssetBySourceURL(ctx, url, e.toSchemaStorageProvider())
	if err != nil {
		return fmt.Errorf("failed to check existing media asset: %w", err)
	}

	if existingAsset != nil {
		logger.InfoCtx(ctx, "Media asset already exists, skipping", zap.String("url", url))
		return nil
	}

	// Process the media file
	if err := e.mediaProcessor.Process(ctx, url); err != nil {
		if errors.Is(err, domain.ErrUnsupportedMediaFile) ||
			errors.Is(err, domain.ErrExceededMaxFileSize) ||
			errors.Is(err, domain.ErrMissingContentLength) {
			// Skip known errors
			return nil
		}

		return fmt.Errorf("failed to process media file: %w", err)
	}

	logger.InfoCtx(ctx, "Successfully indexed media file", zap.String("url", url))
	return nil
}

// toSchemaStorageProvider converts the provider name to a schema storage provider
func (e *executor) toSchemaStorageProvider() schema.StorageProvider {
	switch e.mediaProcessor.Provider() {
	case cloudflare.CLOUDFLARE_PROVIDER_NAME:
		return schema.StorageProviderCloudflare
	default:
		return schema.StorageProviderSelfHosted
	}
}
