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

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"

	"github.com/feral-file/ff-indexer-v2/internal/adapter"
	"github.com/feral-file/ff-indexer-v2/internal/config"
	"github.com/feral-file/ff-indexer-v2/internal/emitter"
	"github.com/feral-file/ff-indexer-v2/internal/logger"
	"github.com/feral-file/ff-indexer-v2/internal/providers/ethereum"
	"github.com/feral-file/ff-indexer-v2/internal/providers/jetstream"
	"github.com/feral-file/ff-indexer-v2/internal/store"
)

var (
	configFile = flag.String("config", "", "Path to configuration file")
	envPath    = flag.String("env", "config/", "Path to environment files")
)

func main() {
	flag.Parse()

	// Load configuration
	config.ChdirRepoRoot()
	cfg, err := config.LoadEthereumEmitterConfig(*configFile, *envPath)
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
			"service": "ethereum-event-emitter",
		},
	})
	if err != nil {
		panic(fmt.Sprintf("Failed to initialize logger: %v", err))
	}
	defer logger.Flush(2 * time.Second)
	logger.InfoCtx(ctx, "Starting Ethereum Event Emitter")

	// Connect to database
	db, err := gorm.Open(postgres.Open(cfg.Database.DSN()), &gorm.Config{})
	if err != nil {
		logger.FatalCtx(ctx, "Failed to connect to database", zap.Error(err), zap.String("dsn", cfg.Database.DSN()))
	}
	logger.InfoCtx(ctx, "Connected to database")

	// Initialize store
	dataStore := store.NewPGStore(db)

	// Initialize adapters
	clockAdapter := adapter.NewClock()
	jsonAdapter := adapter.NewJSON()
	natsJS := adapter.NewNatsJetStream()

	// Initialize ethereum client
	ethDialer := adapter.NewEthClientDialer()
	adapterEthClient, err := ethDialer.Dial(ctx, cfg.Ethereum.WebSocketURL)
	if err != nil {
		logger.FatalCtx(ctx, "Failed to dial Ethereum RPC", zap.Error(err), zap.String("rpc_url", cfg.Ethereum.RPCURL))
	}
	defer adapterEthClient.Close()
	ethereumClient := ethereum.NewClient(cfg.Ethereum.ChainID, adapterEthClient, clockAdapter)

	// Initialize NATS publisher
	natsPublisher, err := jetstream.NewPublisher(
		ctx,
		jetstream.Config{
			URL:            cfg.NATS.URL,
			StreamName:     cfg.NATS.StreamName,
			MaxReconnects:  cfg.NATS.MaxReconnects,
			ReconnectWait:  cfg.NATS.ReconnectWait,
			ConnectionName: cfg.NATS.ConnectionName,
		}, natsJS, jsonAdapter)
	if err != nil {
		logger.FatalCtx(ctx, "Failed to create NATS publisher", zap.Error(err), zap.String("url", cfg.NATS.URL))
	}
	defer natsPublisher.Close()
	logger.InfoCtx(ctx, "Connected to NATS JetStream")

	// Initialize Ethereum subscriber
	ethSubscriber, err := ethereum.NewSubscriber(ctx, ethereum.Config{
		WebSocketURL:    cfg.Ethereum.WebSocketURL,
		ChainID:         cfg.Ethereum.ChainID,
		WorkerPoolSize:  cfg.Worker.WorkerPoolSize,
		WorkerQueueSize: cfg.Worker.WorkerQueueSize,
	}, ethereumClient, clockAdapter)
	if err != nil {
		logger.FatalCtx(ctx, "Failed to create Ethereum subscriber", zap.Error(err), zap.String("websocket_url", cfg.Ethereum.WebSocketURL))
	}
	defer ethSubscriber.Close()
	logger.InfoCtx(ctx, "Connected to Ethereum WebSocket")

	// Setup signal handling
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	// Create emitter with common logic
	emitterCfg := emitter.Config{
		ChainID:         cfg.Ethereum.ChainID,
		StartBlock:      cfg.Ethereum.StartBlock,
		CursorSaveFreq:  2,                // Save every 2 blocks
		CursorSaveDelay: 30 * time.Second, // Or every 30 seconds
	}

	eventEmitter := emitter.NewEmitter(
		ethSubscriber,
		natsPublisher,
		dataStore,
		emitterCfg,
		clockAdapter,
	)
	defer eventEmitter.Close()

	// Channel for emitter errors
	errCh := make(chan error, 1)

	// Start the emitter
	go func() {
		if err := eventEmitter.Run(ctx); err != nil && errors.Is(err, context.Canceled) {
			errCh <- err
		}
	}()

	// Wait for shutdown signal or error
	select {
	case sig := <-sigCh:
		logger.InfoCtx(ctx, "Received shutdown signal", zap.String("signal", sig.String()))
		cancel()
	case <-natsPublisher.CloseChan():
		logger.InfoCtx(ctx, "NATS connection closed unexpectedly")
		cancel()
	case err := <-errCh:
		logger.ErrorCtx(ctx, err, zap.String("component", "emitter"))
		cancel()
	}

	// Give some time for graceful shutdown
	time.Sleep(time.Second)

	// Use non-context logger for final shutdown message since context is already canceled
	logger.Info("Ethereum Event Emitter stopped")
}
