//go:build !cgo

package main

import (
	"context"
	"errors"

	"go.temporal.io/sdk/client"
	"gorm.io/gorm"

	"github.com/feral-file/ff-indexer-v2/internal/config"
	"github.com/feral-file/ff-indexer-v2/internal/logger"
)

// registerWorkerMedia is a no-op when CGO is disabled (media indexing requires CGO).
func registerWorkerMedia(
	_ context.Context,
	cfg *config.AppConfig,
	_ *gorm.DB,
	_ client.Client,
) (run func(context.Context) error, cleanup func(context.Context) error, err error) {
	if cfg != nil && cfg.MediaEnabled {
		return nil, nil, errors.New("media worker requires CGO_ENABLED=1 when media_enabled=true")
	}

	logger.Warn("CGO is disabled: media Temporal worker is not started (rebuild with CGO_ENABLED=1 to enable)")
	run = func(ctx context.Context) error {
		<-ctx.Done()
		return ctx.Err()
	}
	cleanup = func(context.Context) error { return nil }
	return run, cleanup, nil
}
