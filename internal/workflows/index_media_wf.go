//go:build cgo

package workflows

import (
	"context"
	"fmt"

	"go.uber.org/zap"

	"github.com/feral-file/ff-indexer-v2/internal/logger"
	"github.com/feral-file/ff-indexer-v2/internal/providers/jobs"
	"github.com/feral-file/ff-indexer-v2/internal/types"
)

// IndexMultipleMediaWorkflow handles the media processing for a list of URLs
// It triggers child workflows concurrently (fire and forget) for each URL
func (w *mediaWorkflows) IndexMultipleMediaWorkflow(ctx context.Context, urls []string) error {
	logger.InfoCtx(ctx, "Starting multiple media indexing", zap.Int("count", len(urls)))

	// Remove duplicate URLs
	uniqueURLs := make(map[string]bool)
	for _, url := range urls {
		if types.IsValidURL(url) || types.IsDataURI(url) {
			uniqueURLs[url] = true
		}
	}

	if len(uniqueURLs) == 0 {
		logger.InfoCtx(ctx, "No media URLs to process")
		return nil
	}

	logger.InfoCtx(ctx, "Processing unique media URLs", zap.Int("count", len(uniqueURLs)))

	// Start all child jobs concurrently (fire and forget)
	for url := range uniqueURLs {
		uk := "media-" + types.MD5Hash(url)
		_, _, err := w.jobQueue.Enqueue(ctx, jobs.EnqueueOptions{
			Queue:     w.config.MediaTaskQueue,
			Kind:      "IndexMediaWorkflow",
			Args:      []any{url},
			UniqueKey: &uk,
		})
		if err != nil {
			logger.WarnCtx(ctx, "Failed to start child workflow for media URL",
				zap.String("url", url),
				zap.Error(err),
			)
			// Continue with other URLs even if one fails to start
			continue
		}

		logger.InfoCtx(ctx, "Media indexing job enqueued",
			zap.String("url", url),
			zap.String("uniqueKey", uk),
		)
	}

	logger.InfoCtx(ctx, "All media child workflows triggered", zap.Int("count", len(uniqueURLs)))
	return nil
}

// IndexMediaWorkflow handles the media processing for a single URL
// This workflow uses a separate task queue with higher execution time
func (w *mediaWorkflows) IndexMediaWorkflow(ctx context.Context, url string) error {
	logger.InfoCtx(ctx, "Starting media indexing", zap.String("url", url))

	// Execute the media indexing activity
	err := w.executor.IndexMediaFile(ctx, url)
	if err != nil {
		logger.ErrorCtx(ctx,
			fmt.Errorf("failed to index media file"),
			zap.Error(err),
			zap.String("url", url),
		)
		return err
	}

	logger.InfoCtx(ctx, "Media indexed successfully", zap.String("url", url))
	return nil
}
