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
	"github.com/feral-file/ff-indexer-v2/internal/providers/ethereum"
	"github.com/feral-file/ff-indexer-v2/internal/providers/jetstream"
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
	cfg, err := config.LoadEthereumEmitterConfig(*configPath)
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

	// Initialize Ethereum subscriber
	ethSubscriber, err := ethereum.NewSubscriber(ethereum.Config{
		WebSocketURL: cfg.Ethereum.WebSocketURL,
		ChainID:      domain.Chain(cfg.Ethereum.ChainID),
	})
	if err != nil {
		logger.Fatal("Failed to create Ethereum subscriber", zap.Error(err), zap.String("websocket_url", cfg.Ethereum.WebSocketURL))
	}
	defer ethSubscriber.Close()
	logger.Info("Connected to Ethereum WebSocket")

	// Create context with cancellation
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Setup signal handling
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	// Determine starting block
	startBlock := cfg.Ethereum.StartBlock
	if startBlock == 0 {
		// Get last processed block from cursor store
		lastBlock, err := cursorStore.GetBlockCursor(ctx, cfg.Ethereum.ChainID)
		if err != nil {
			logger.Fatal("Failed to get block cursor", zap.Error(err), zap.String("chain_id", cfg.Ethereum.ChainID))
		}

		if lastBlock > 0 {
			startBlock = lastBlock + 1
			logger.Info("Resuming from last processed block", zap.Uint64("block", startBlock))
		} else {
			// Start from latest block
			latestBlock, err := ethSubscriber.GetLatestBlockNumber(ctx)
			if err != nil {
				logger.Fatal("Failed to get latest block number", zap.Error(err), zap.String("chain_id", cfg.Ethereum.ChainID))
			}
			startBlock = latestBlock
			logger.Info("Starting from latest block", zap.Uint64("block", startBlock))
		}
	} else {
		logger.Info("Starting from configured block", zap.Uint64("block", startBlock))
	}

	// Channel for events
	errCh := make(chan error, 1)

	// Start subscribing to events
	go func() {
		logger.Info("Starting event subscription")

		lastSavedBlock := uint64(0)
		lastSaveTime := time.Now()

		handler := func(event *domain.BlockchainEvent) error {
			// Log the event
			logger.Info("Received transfer event",
				zap.String("contract", event.ContractAddress),
				zap.String("token_number", event.TokenNumber),
				zap.String("event_type", string(event.EventType)),
				zap.String("from", event.FromAddress), zap.String("to", event.ToAddress),
				zap.Uint64("block", event.BlockNumber),
				zap.String("tx_hash", event.TxHash))

			// Publish to NATS
			if err := natsPublisher.PublishEvent(ctx, event); err != nil {
				logger.Error(fmt.Errorf("failed to publish event: %w", err), zap.String("tx_hash", event.TxHash))
				return err
			}

			// Save cursor periodically (every 10 blocks or 30 seconds)
			if event.BlockNumber-lastSavedBlock >= 10 || time.Since(lastSaveTime) >= 30*time.Second {
				if err := cursorStore.SetBlockCursor(ctx, cfg.Ethereum.ChainID, event.BlockNumber); err != nil {
					logger.Error(fmt.Errorf("failed to save block cursor: %w", err), zap.String("chain_id", cfg.Ethereum.ChainID))
				} else {
					lastSavedBlock = event.BlockNumber
					lastSaveTime = time.Now()
					logger.Debug("Saved block cursor", zap.Uint64("block", event.BlockNumber))
				}
			}

			return nil
		}

		err := ethSubscriber.SubscribeEvents(ctx, startBlock, handler)
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

	logger.Info("Ethereum Event Emitter stopped")
}
