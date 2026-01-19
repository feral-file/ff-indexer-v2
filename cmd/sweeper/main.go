package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"go.temporal.io/sdk/client"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"

	"github.com/feral-file/ff-indexer-v2/internal/adapter"
	"github.com/feral-file/ff-indexer-v2/internal/config"
	"github.com/feral-file/ff-indexer-v2/internal/logger"
	temporal "github.com/feral-file/ff-indexer-v2/internal/providers/temporal"
	"github.com/feral-file/ff-indexer-v2/internal/store"
	"github.com/feral-file/ff-indexer-v2/internal/sweeper"
	"github.com/feral-file/ff-indexer-v2/internal/uri"
)

var (
	configFile = flag.String("config", "", "Path to configuration file")
	envPath    = flag.String("env", "config/", "Path to environment files")
)

func main() {
	flag.Parse()

	// Load configuration
	config.ChdirRepoRoot()
	cfg, err := config.LoadSweeperConfig(*configFile, *envPath)
	if err != nil {
		panic(fmt.Sprintf("Failed to load config: %v", err))
	}

	// Create context with cancellation
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Initialize logger with sentry integration
	err = logger.Initialize(logger.Config{
		Debug:           cfg.Debug,
		SentryDSN:       cfg.SentryDSN,
		BreadcrumbLevel: zapcore.InfoLevel,
		Tags: map[string]string{
			"service": "sweeper",
		},
	})
	if err != nil {
		panic(fmt.Sprintf("Failed to initialize logger: %v", err))
	}
	defer logger.Flush(2 * time.Second)
	logger.InfoCtx(ctx, "Starting Sweeper")

	// Connect to database
	db, err := gorm.Open(postgres.Open(cfg.Database.DSN()), &gorm.Config{})
	if err != nil {
		logger.FatalCtx(ctx, "Failed to connect to database", zap.Error(err), zap.String("dsn", cfg.Database.DSN()))
	}

	// Configure connection pool
	if err := store.ConfigureConnectionPool(db, cfg.Database.MaxOpenConns, cfg.Database.MaxIdleConns, cfg.Database.ConnMaxLifetime, cfg.Database.ConnMaxIdleTime); err != nil {
		logger.FatalCtx(ctx, "Failed to configure connection pool", zap.Error(err))
	}
	logger.InfoCtx(ctx, "Connected to database",
		zap.Int("max_open_conns", cfg.Database.MaxOpenConns),
		zap.Int("max_idle_conns", cfg.Database.MaxIdleConns),
	)

	// Initialize store
	dataStore := store.NewPGStore(db)

	// Initialize HTTP client
	httpClient := adapter.NewHTTPClient(cfg.MediaHealthSweeper.HTTPTimeout)

	// Initialize IO adapter
	ioAdapter := adapter.NewIO()

	// Initialize clock adapter
	clock := adapter.NewClock()

	// Initialize URI resolver config
	uriResolverConfig := &uri.Config{
		IPFSGateways:    cfg.MediaHealthSweeper.URI.IPFSGateways,
		ArweaveGateways: cfg.MediaHealthSweeper.URI.ArweaveGateways,
		OnChFSGateways:  cfg.MediaHealthSweeper.URI.OnchfsGateways,
	}

	// Initialize health checker
	urlHealthChecker := uri.NewURLChecker(httpClient, ioAdapter, uriResolverConfig)

	// Connect to Temporal for webhook notifications
	temporalLogger := temporal.NewZapLoggerAdapter(logger.Default())
	temporalClient, err := client.Dial(client.Options{
		HostPort:  cfg.Temporal.HostPort,
		Namespace: cfg.Temporal.Namespace,
		Logger:    temporalLogger,
	})
	if err != nil {
		logger.FatalCtx(ctx, "Failed to connect to Temporal", zap.Error(err), zap.String("host_port", cfg.Temporal.HostPort))
	}
	defer temporalClient.Close()
	logger.InfoCtx(ctx, "Connected to Temporal", zap.String("namespace", cfg.Temporal.Namespace))

	// Initialize media health sweeper
	mediaSweeperConfig := &sweeper.MediaHealthSweeperConfig{
		BatchSize:      cfg.MediaHealthSweeper.BatchSize,
		WorkerPoolSize: cfg.MediaHealthSweeper.Worker.WorkerPoolSize,
		RecheckAfter:   cfg.MediaHealthSweeper.RecheckAfter,
	}
	mediaSweeper := sweeper.NewMediaHealthSweeper(mediaSweeperConfig, dataStore, urlHealthChecker, clock, temporalClient, cfg.Temporal.TokenTaskQueue)

	logger.InfoCtx(ctx, "Initialized media health sweeper (continuous mode)",
		zap.Int("batch_size", cfg.MediaHealthSweeper.BatchSize),
		zap.Int("worker_pool_size", cfg.MediaHealthSweeper.Worker.WorkerPoolSize),
		zap.Duration("recheck_after", cfg.MediaHealthSweeper.RecheckAfter),
	)

	// Start the sweeper in a goroutine
	errChan := make(chan error, 1)
	go func() {
		if err := mediaSweeper.Start(ctx); err != nil {
			errChan <- err
		}
	}()

	// Wait for interrupt signal or error
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)

	select {
	case sig := <-sigCh:
		logger.InfoCtx(ctx, "Received shutdown signal", zap.String("signal", sig.String()))
	case err := <-errChan:
		logger.ErrorCtx(ctx, err)
	}

	// Cancel context to stop the sweeper
	cancel()

	// Give the sweeper time to shut down gracefully
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer shutdownCancel()

	if err := mediaSweeper.Stop(shutdownCtx); err != nil {
		logger.ErrorCtx(shutdownCtx, err)
	}

	logger.InfoCtx(shutdownCtx, "Sweeper stopped")
}
