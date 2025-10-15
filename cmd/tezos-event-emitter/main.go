package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	logger "github.com/bitmark-inc/autonomy-logger"
	"github.com/feral-file/ff-indexer-v2/internal/config"
	"github.com/feral-file/ff-indexer-v2/internal/domain"
	"github.com/feral-file/ff-indexer-v2/internal/providers/jetstream"
	"github.com/feral-file/ff-indexer-v2/internal/providers/tezos"
	"github.com/feral-file/ff-indexer-v2/internal/store"
	"github.com/getsentry/sentry-go"
	"go.uber.org/zap"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

var (
	configPath = flag.String("config", "config.yaml", "Path to configuration file")
)

func main() {
	flag.Parse()

	// Load configuration
	cfg, err := config.LoadTezosEmitterConfig(*configPath)
	if err != nil {
		logger.Fatal("Failed to load config", zap.Error(err), zap.String("config_path", *configPath))
	}

	// Initialize logger
	err = logger.Initialize(cfg.Debug,
		&sentry.ClientOptions{
			Dsn:   cfg.SentryDSN,
			Debug: cfg.Debug,
		})
	if err != nil {
		logger.Fatal("Failed to initialize logger", zap.Error(err), zap.String("sentry_dsn", cfg.SentryDSN))
	}
	logger.Info("Starting Ethereum Event Emitter")

	// Connect to database
	db, err := gorm.Open(postgres.Open(cfg.Database.DSN()), &gorm.Config{})
	if err != nil {
		logger.Fatal("Failed to connect to database", zap.Error(err), zap.String("dsn", cfg.Database.DSN()))
	}
	logger.Info("Connected to database")

	// Initialize cursor store
	cursorStore := store.NewCursorStore(db)

	// Initialize NATS publisher
	natsPublisher, err := jetstream.NewPublisher(jetstream.Config{
		URL:            cfg.NATS.URL,
		StreamName:     cfg.NATS.StreamName,
		MaxReconnects:  cfg.NATS.MaxReconnects,
		ReconnectWait:  cfg.NATS.ReconnectWait,
		ConnectionName: cfg.NATS.ConnectionName,
	})
	if err != nil {
		logger.Fatal("Failed to create NATS publisher", zap.Error(err), zap.String("url", cfg.NATS.URL))
	}
	defer natsPublisher.Close()
	logger.Info("Connected to NATS JetStream")

	// Initialize Tezos subscriber
	tezosSubscriber, err := tezos.NewSubscriber(tezos.Config{
		APIURL:       cfg.Tezos.APIURL,
		WebSocketURL: cfg.Tezos.WebSocketURL,
		ChainID:      domain.Chain(cfg.Tezos.ChainID),
	})
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

	// Get last processed level from cursor store
	lastLevel, err := cursorStore.GetBlockCursor(ctx, cfg.Tezos.ChainID)
	if err != nil {
		logger.Fatal("Failed to get block cursor", zap.Error(err), zap.String("chain_id", cfg.Tezos.ChainID))
	}

	if lastLevel > 0 {
		logger.Info("Last processed level", zap.Uint64("level", lastLevel))
	} else {
		logger.Info("No previous cursor found, starting from current")
	}

	// Channel for events
	errCh := make(chan error, 1)

	// Start subscribing to events
	go func() {
		logger.Info("Starting event subscription")

		lastSavedLevel := uint64(0)
		lastSaveTime := time.Now()

		handler := func(event *domain.BlockchainEvent) error {
			// Log the event
			logger.Info("Received transfer event",
				zap.String("contract", event.ContractAddress),
				zap.String("token_number", event.TokenNumber),
				zap.String("event_type", string(event.EventType)),
				zap.String("from", event.FromAddress),
				zap.String("to", event.ToAddress),
				zap.Uint64("level", event.BlockNumber),
				zap.String("tx_hash", event.TxHash))

			// Publish to NATS
			if err := natsPublisher.PublishEvent(ctx, event); err != nil {
				logger.Error(fmt.Errorf("failed to publish event: %w", err), zap.String("tx_hash", event.TxHash))
				return err
			}

			// Save cursor periodically (every 10 levels or 30 seconds)
			if event.BlockNumber-lastSavedLevel >= 10 || time.Since(lastSaveTime) >= 30*time.Second {
				if err := cursorStore.SetBlockCursor(ctx, cfg.Tezos.ChainID, event.BlockNumber); err != nil {
					logger.Error(fmt.Errorf("failed to save block cursor: %w", err), zap.String("chain_id", cfg.Tezos.ChainID))
				} else {
					lastSavedLevel = event.BlockNumber
					lastSaveTime = time.Now()
					logger.Debug("Saved level cursor", zap.Uint64("level", event.BlockNumber))
				}
			}

			return nil
		}

		err := tezosSubscriber.SubscribeEvents(ctx, handler)
		if err != nil {
			errCh <- err
		}
	}()

	// Wait for shutdown signal or error
	select {
	case sig := <-sigCh:
		logger.Info("Received shutdown signal", zap.String("signal", sig.String()))
		cancel()
	case err := <-errCh:
		logger.Error(fmt.Errorf("subscription error: %w", err))
		cancel()
	}

	// Give some time for graceful shutdown
	time.Sleep(2 * time.Second)

	logger.Info("Tezos Event Emitter stopped")
}
