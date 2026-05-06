//go:build !cgo

package main

import (
	"context"
	"errors"

	"gorm.io/gorm"

	"github.com/feral-file/ff-indexer-v2/internal/config"
	"github.com/feral-file/ff-indexer-v2/internal/logger"
)

// registerWorkerMedia is a no-op when CGO is disabled (media indexing requires CGO).
func registerWorkerMedia(
	_ context.Context,
	wcfg *config.WorkerMediaConfig,
	_ *gorm.DB,
) (run func(context.Context) error, cleanup func(context.Context) error, err error) {
	if wcfg != nil && wcfg.MediaEnabled {
		return nil, nil, errors.New("media worker requires CGO_ENABLED=1 when media_enabled=true")
	}

	logger.Warn("CGO is disabled: media job worker is not started (rebuild with CGO_ENABLED=1 to enable)")
	run = func(ctx context.Context) error {
		<-ctx.Done()
		return ctx.Err()
	}
	cleanup = func(context.Context) error { return nil }
	return run, cleanup, nil
}
