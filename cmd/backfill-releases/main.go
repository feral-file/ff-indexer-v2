package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"go.uber.org/zap"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"

	"github.com/feral-file/ff-indexer-v2/internal/adapter"
	"github.com/feral-file/ff-indexer-v2/internal/config"
	"github.com/feral-file/ff-indexer-v2/internal/logger"
	"github.com/feral-file/ff-indexer-v2/internal/providers/vendors/feralfile"
	releasebackfill "github.com/feral-file/ff-indexer-v2/internal/release"
	"github.com/feral-file/ff-indexer-v2/internal/store"
)

var (
	configFile = flag.String("config", "", "Path to configuration file")
	envPath    = flag.String("env", "config/", "Path to environment files")
	batchSize  = flag.Int("batch-size", 500, "Number of enrichment sources to process per batch")
)

func main() {
	os.Exit(run())
}

func run() int {
	flag.Parse()
	config.ChdirRepoRoot()

	cfg, err := config.LoadAppConfig(*configFile, *envPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to load config: %v\n", err)
		return 1
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := logger.Initialize(logger.Config{Debug: cfg.Debug}); err != nil {
		fmt.Fprintf(os.Stderr, "Failed to initialize logger: %v\n", err)
		return 1
	}
	defer logger.Flush(2 * time.Second)

	db, err := gorm.Open(postgres.Open(cfg.Database.DSN()), &gorm.Config{})
	if err != nil {
		logger.FatalCtx(ctx, "Failed to connect to database", zap.Error(err))
	}
	sqlDB, err := db.DB()
	if err != nil {
		logger.FatalCtx(ctx, "Failed to get sql DB", zap.Error(err))
	}
	defer func() { _ = sqlDB.Close() }()

	pgStore := store.NewPGStore(db)
	httpClient := adapter.NewHTTPClient(30 * time.Second)
	jsonAdapter := adapter.NewJSON()
	// Use the configured FF API URL so backfill targets the same endpoint as normal
	// enrichment (staging, proxy, or custom). The hardcoded feralfile.API_ENDPOINT
	// constant is intentionally not used here to match worker_core.go behavior.
	ffClient := feralfile.NewClient(httpClient, cfg.Vendors.FeralFileURL)

	backfiller := releasebackfill.NewBackfiller(pgStore, ffClient, jsonAdapter, *batchSize)
	processed, err := backfiller.Run(ctx)
	if err != nil {
		logger.ErrorCtx(ctx, err, zap.String("message", "Release backfill failed"), zap.Int("processed", processed))
		return 1
	}

	logger.InfoCtx(ctx, "Release backfill completed", zap.Int("processed", processed))
	return 0
}
