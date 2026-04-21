//go:build !cgo

package main

import (
	"context"

	"go.temporal.io/sdk/client"
	"gorm.io/gorm"

	"github.com/feral-file/ff-indexer-v2/internal/config"
)

// registerWorkerMedia is a no-op when CGO is disabled (media indexing requires CGO).
func registerWorkerMedia(
	_ context.Context,
	_ *config.AppConfig,
	_ *gorm.DB,
	_ client.Client,
) (run func(context.Context) error, cleanup func(context.Context) error, err error) {
	return noOpMediaWorker("CGO is disabled: media Temporal worker is not started (rebuild with CGO_ENABLED=1 to enable)")
}
