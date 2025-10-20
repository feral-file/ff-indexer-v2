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
	"go.temporal.io/sdk/client"
	"go.uber.org/zap"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"

	"github.com/feral-file/ff-indexer-v2/internal/adapter"
	"github.com/feral-file/ff-indexer-v2/internal/bridge"
	"github.com/feral-file/ff-indexer-v2/internal/config"
	"github.com/feral-file/ff-indexer-v2/internal/store"
)

var (
	configPath = flag.String("config", "config.yaml", "Path to configuration file")
)

func main() {
	flag.Parse()

	// Load configuration
	cfg, err := config.LoadEventBridgeConfig(*configPath)
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
	logger.Info("Starting Event Bridge")

	// Connect to database
	db, err := gorm.Open(postgres.Open(cfg.Database.DSN()), &gorm.Config{})
	if err != nil {
		logger.Fatal("Failed to connect to database", zap.Error(err), zap.String("dsn", cfg.Database.DSN()))
	}
	logger.Info("Connected to database")

	// Initialize store
	dataStore := store.NewPGStore(db)

	// Initialize adapters
	jsonAdapter := adapter.NewJSON()
	natsJS := adapter.NewNatsJetStream()

	// Connect to Temporal (for triggering workflows remotely)
	temporalClient, err := client.Dial(client.Options{
		HostPort:  cfg.Temporal.HostPort,
		Namespace: cfg.Temporal.Namespace,
	})
	if err != nil {
		logger.Fatal("Failed to connect to Temporal", zap.Error(err), zap.String("host_port", cfg.Temporal.HostPort))
	}
	defer temporalClient.Close()
	logger.Info("Connected to Temporal", zap.String("namespace", cfg.Temporal.Namespace))

	// Create bridge
	eventBridge, err := bridge.NewBridge(
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
	)
	if err != nil {
		logger.Fatal("Failed to create event bridge", zap.Error(err))
	}
	defer eventBridge.Close()
	logger.Info("Event bridge created", zap.String("stream", cfg.NATS.StreamName), zap.String("consumer", cfg.NATS.ConsumerName))

	// Create context with cancellation
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

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
		logger.Info("Received shutdown signal", zap.String("signal", sig.String()))
		cancel()
	case err := <-errCh:
		logger.Error(err, zap.String("component", "bridge"))
		cancel()
	}

	// Give some time for graceful shutdown
	time.Sleep(time.Second)

	logger.Info("Event Bridge stopped")
}
