package main

import (
	"context"
	"errors"
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
	"github.com/feral-file/ff-indexer-v2/internal/bridge"
	"github.com/feral-file/ff-indexer-v2/internal/config"
	"github.com/feral-file/ff-indexer-v2/internal/logger"
	temporal "github.com/feral-file/ff-indexer-v2/internal/providers/temporal"
	"github.com/feral-file/ff-indexer-v2/internal/registry"
	"github.com/feral-file/ff-indexer-v2/internal/store"
)

var (
	configPath = flag.String("config", "", "Path to configuration file")
)

func main() {
	flag.Parse()

	// Load configuration
	config.ChdirRepoRoot()
	cfg, err := config.LoadEventBridgeConfig(*configPath)
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
			"service": "event-bridge",
		},
	})
	if err != nil {
		panic(fmt.Sprintf("Failed to initialize logger: %v", err))
	}
	defer logger.Flush(2 * time.Second)
	logger.InfoCtx(ctx, "Starting Event Bridge")

	// Connect to database
	db, err := gorm.Open(postgres.Open(cfg.Database.DSN()), &gorm.Config{})
	if err != nil {
		logger.FatalCtx(ctx, "Failed to connect to database", zap.Error(err), zap.String("dsn", cfg.Database.DSN()))
	}
	logger.InfoCtx(ctx, "Connected to database")

	// Initialize store
	dataStore := store.NewPGStore(db)

	// Initialize adapters
	jsonAdapter := adapter.NewJSON()
	natsJS := adapter.NewNatsJetStream()
	fs := adapter.NewFileSystem()

	// Connect to Temporal (for triggering workflows remotely) with logger integration
	temporalLogger := temporal.NewZapLoggerAdapter(logger.Default())
	temporalClient, err := client.Dial(client.Options{
		HostPort:  cfg.Temporal.HostPort,
		Namespace: cfg.Temporal.Namespace,
		Logger:    temporalLogger, // Use zap logger adapter for Temporal client
	})
	if err != nil {
		logger.FatalCtx(ctx, "Failed to connect to Temporal", zap.Error(err), zap.String("host_port", cfg.Temporal.HostPort))
	}
	defer temporalClient.Close()
	logger.InfoCtx(ctx, "Connected to Temporal", zap.String("namespace", cfg.Temporal.Namespace))

	// Load blacklist registry
	var blacklistRegistry registry.BlacklistRegistry
	if cfg.BlacklistPath != "" {
		blacklistLoader := registry.NewBlacklistRegistryLoader(fs, jsonAdapter)
		blacklistRegistry, err = blacklistLoader.Load(cfg.BlacklistPath)
		if err != nil {
			logger.FatalCtx(ctx, "Failed to load blacklist registry",
				zap.Error(err),
				zap.String("path", cfg.BlacklistPath))
		}
		logger.InfoCtx(ctx, "Loaded blacklist registry", zap.String("path", cfg.BlacklistPath))
	} else {
		logger.WarnCtx(ctx, "Blacklist registry path not configured, all contracts will be allowed")
	}

	// Create bridge
	eventBridge, err := bridge.NewBridge(
		ctx,
		bridge.Config{
			URL:               cfg.NATS.URL,
			StreamName:        cfg.NATS.StreamName,
			ConsumerName:      cfg.NATS.ConsumerName,
			MaxReconnects:     cfg.NATS.MaxReconnects,
			ReconnectWait:     cfg.NATS.ReconnectWait,
			ConnectionName:    cfg.NATS.ConnectionName,
			AckWaitTimeout:    cfg.NATS.AckWait,
			MaxDeliver:        cfg.NATS.MaxDeliver,
			TemporalTaskQueue: cfg.Temporal.TaskQueue,
		},
		natsJS,
		dataStore,
		temporalClient,
		jsonAdapter,
		blacklistRegistry,
	)
	if err != nil {
		logger.FatalCtx(ctx, "Failed to create event bridge", zap.Error(err))
	}
	defer eventBridge.Close()
	logger.InfoCtx(ctx, "Event bridge created", zap.String("stream", cfg.NATS.StreamName), zap.String("consumer", cfg.NATS.ConsumerName))

	// Setup signal handling
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	// Channel for bridge errors
	errCh := make(chan error, 1)

	// Start the bridge
	go func() {
		if err := eventBridge.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
			errCh <- err
		}
	}()

	// Wait for shutdown signal or error
	select {
	case sig := <-sigCh:
		logger.InfoCtx(ctx, "Received shutdown signal", zap.String("signal", sig.String()))
		cancel()
	case err := <-errCh:
		logger.ErrorCtx(ctx, err, zap.String("component", "bridge"))
		cancel()
	}

	// Give some time for graceful shutdown
	time.Sleep(time.Second)

	logger.Info("Event Bridge stopped")
}
