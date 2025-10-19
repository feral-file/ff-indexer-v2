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

	logger "github.com/bitmark-inc/autonomy-logger"
	"github.com/getsentry/sentry-go"
	"go.uber.org/zap"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"

	"github.com/feral-file/ff-indexer-v2/internal/adapter"
	"github.com/feral-file/ff-indexer-v2/internal/config"
	"github.com/feral-file/ff-indexer-v2/internal/domain"
	"github.com/feral-file/ff-indexer-v2/internal/emitter"
	"github.com/feral-file/ff-indexer-v2/internal/providers/jetstream"
	"github.com/feral-file/ff-indexer-v2/internal/providers/tezos"
	"github.com/feral-file/ff-indexer-v2/internal/store"
)

var (
	configPath = flag.String("config", "config.yaml", "Path to configuration file")
)

func main() {
	flag.Parse()

	// Load configuration
	cfg, err := config.LoadTezosEmitterConfig(*configPath)
	if err != nil {
		panic(fmt.Sprintf("Failed to load config: %v", err))
	}

	// Initialize logger
	err = logger.Initialize(cfg.Debug,
		&sentry.ClientOptions{
			Dsn:   cfg.SentryDSN,
			Debug: cfg.Debug,
		})
	if err != nil {
		panic(fmt.Sprintf("Failed to initialize logger: %v", err))
	}
	logger.Info("Starting Tezos Event Emitter")

	// Connect to database
	db, err := gorm.Open(postgres.Open(cfg.Database.DSN()), &gorm.Config{})
	if err != nil {
		logger.Fatal("Failed to connect to database", zap.Error(err), zap.String("dsn", cfg.Database.DSN()))
	}
	logger.Info("Connected to database")

	// Initialize store
	dataStore := store.NewPGStore(db)

	// Initialize adapters
	clockAdapter := adapter.NewClock()
	jsonAdapter := adapter.NewJSON()
	natsJS := adapter.NewNatsJetStream()
	signalR := adapter.NewSignalR()
	httpClient := adapter.NewHTTPClient(30 * time.Second)

	// Initialize NATS publisher
	natsPublisher, err := jetstream.NewPublisher(
		jetstream.Config{
			URL:            cfg.NATS.URL,
			StreamName:     cfg.NATS.StreamName,
			MaxReconnects:  cfg.NATS.MaxReconnects,
			ReconnectWait:  cfg.NATS.ReconnectWait,
			ConnectionName: cfg.NATS.ConnectionName,
		}, natsJS, jsonAdapter)
	if err != nil {
		logger.Fatal("Failed to create NATS publisher", zap.Error(err), zap.String("url", cfg.NATS.URL))
	}
	defer natsPublisher.Close()
	logger.Info("Connected to NATS JetStream")

	// Initialize TzKT client
	tzktClient := tezos.NewTzKTClient(cfg.Tezos.APIURL, httpClient)

	// Initialize Tezos subscriber
	tezosSubscriber, err := tezos.NewSubscriber(tezos.Config{
		APIURL:       cfg.Tezos.APIURL,
		WebSocketURL: cfg.Tezos.WebSocketURL,
		ChainID:      domain.Chain(cfg.Tezos.ChainID),
	}, signalR, clockAdapter, tzktClient)
	if err != nil {
		logger.Fatal("Failed to create Tezos subscriber", zap.Error(err), zap.String("websocket_url", cfg.Tezos.WebSocketURL))
	}
	defer tezosSubscriber.Close()
	logger.Info("Connected to TzKT WebSocket")

	// Create context with cancellation
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Setup signal handling
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	// Create emitter with common logic
	emitterCfg := emitter.Config{
		ChainID:         domain.Chain(cfg.Tezos.ChainID),
		StartBlock:      cfg.Tezos.StartLevel,
		CursorSaveFreq:  2,                // Save every 2 levels
		CursorSaveDelay: 30 * time.Second, // Or every 30 seconds
	}

	eventEmitter := emitter.NewEmitter(
		tezosSubscriber,
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
		logger.Info("Received shutdown signal", zap.String("signal", sig.String()))
		cancel()
	case err := <-errCh:
		logger.Error(err, zap.String("component", "emitter"))
		cancel()
	}

	// Give some time for graceful shutdown
	time.Sleep(2 * time.Second)

	logger.Info("Tezos Event Emitter stopped")
}
