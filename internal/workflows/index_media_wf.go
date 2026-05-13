//go:build cgo

package workflows

import (
	"context"
	"errors"
	"fmt"
	"strings"

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

// IndexMediaWorkflow handles media processing for a single URL.
//
// Reason: Media jobs should still fail when processing cannot finish so operators can inspect
// jobs.last_error, but expected third-party source timeouts should not page engineers through Sentry.
// Trade-offs: Known source-fetch timeouts are warning logs; unexpected failures remain error logs.
// Constraints: Returning the original error preserves the worker's failed-job behavior.
func (w *mediaWorkflows) IndexMediaWorkflow(ctx context.Context, url string) error {
	logger.InfoCtx(ctx, "Starting media indexing", zap.String("url", url))

	// Execute the media indexing activity
	err := w.executor.IndexMediaFile(ctx, url)
	if err != nil {
		logMediaIndexFailure(ctx, url, err)
		return err
	}

	logger.InfoCtx(ctx, "Media indexed successfully", zap.String("url", url))
	return nil
}

// logMediaIndexFailure records a failed media job at the appropriate operational severity.
//
// Reason: Sentry should capture unexpected application failures, not routine source gateway
// timeouts from externally hosted media. Trade-offs: The job still fails and stores last_error,
// so operators retain a durable signal without one Sentry event per unavailable media URL.
func logMediaIndexFailure(ctx context.Context, url string, err error) {
	if isExpectedMediaSourceTimeout(err) {
		logger.WarnCtx(ctx,
			"Media source fetch timed out during indexing",
			zap.Error(err),
			zap.String("url", url),
		)
		return
	}

	logger.ErrorCtx(ctx,
		fmt.Errorf("failed to index media file"),
		zap.Error(err),
		zap.String("url", url),
	)
}

// isExpectedMediaSourceTimeout reports whether err is the known transform-source fetch timeout.
//
// Reason: The transformer wraps HTTP GET failures with "failed to get response"; combining that
// marker with context.DeadlineExceeded limits Sentry suppression to external media fetches instead
// of every possible media workflow deadline, such as database or provider API timeouts.
func isExpectedMediaSourceTimeout(err error) bool {
	return errors.Is(err, context.DeadlineExceeded) && strings.Contains(err.Error(), "failed to get response")
}
