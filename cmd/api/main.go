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
	"github.com/getsentry/sentry-go"
	"go.temporal.io/sdk/client"
	"go.uber.org/zap"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"

	"github.com/feral-file/ff-indexer-v2/internal/api/middleware"
	"github.com/feral-file/ff-indexer-v2/internal/api/server"
	"github.com/feral-file/ff-indexer-v2/internal/config"
	"github.com/feral-file/ff-indexer-v2/internal/registry"
	"github.com/feral-file/ff-indexer-v2/internal/store"
)

var (
	configPath = flag.String("config", "config.yaml", "Path to configuration file")
)

func main() {
	flag.Parse()

	// Load configuration
	config.ChdirRepoRoot()
	cfg, err := config.LoadAPIConfig(*configPath)
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
	logger.Info("Starting Feral File Indexer API")

	// Connect to database
	db, err := gorm.Open(postgres.Open(cfg.Database.DSN()), &gorm.Config{})
	if err != nil {
		logger.Fatal("Failed to connect to database", zap.Error(err), zap.String("dsn", cfg.Database.DSN()))
	}
	logger.Info("Connected to database")

	// Initialize store
	dataStore := store.NewPGStore(db)

	// Connect to Temporal
	temporalClient, err := client.Dial(client.Options{
		HostPort:  cfg.Temporal.HostPort,
		Namespace: cfg.Temporal.Namespace,
	})
	if err != nil {
		logger.Fatal("Failed to connect to Temporal", zap.Error(err))
	}
	defer temporalClient.Close()
	logger.Info("Connected to Temporal", zap.String("host_port", cfg.Temporal.HostPort))

	// Load blacklist registry
	var blacklistRegistry registry.BlacklistRegistry
	if cfg.BlacklistPath != "" {
		blacklistRegistry, err = registry.LoadBlacklist(cfg.BlacklistPath)
		if err != nil {
			logger.Fatal("Failed to load blacklist registry",
				zap.Error(err),
				zap.String("path", cfg.BlacklistPath))
		}
		logger.Info("Loaded blacklist registry", zap.String("path", cfg.BlacklistPath))
	} else {
		logger.Warn("Blacklist registry path not configured, all contracts will be allowed")
	}

	// Create server config
	serverConfig := server.Config{
		Debug:                 cfg.Debug,
		Host:                  cfg.Server.Host,
		Port:                  cfg.Server.Port,
		ReadTimeout:           time.Duration(cfg.Server.ReadTimeout) * time.Second,
		WriteTimeout:          time.Duration(cfg.Server.WriteTimeout) * time.Second,
		IdleTimeout:           time.Duration(cfg.Server.IdleTimeout) * time.Second,
		OrchestratorTaskQueue: cfg.Temporal.TaskQueue,
		Auth: middleware.AuthConfig{
			JWTPublicKey: cfg.Auth.JWTPublicKey,
			APIKeys:      cfg.Auth.APIKeys,
		},
	}

	// Create and start server
	srv := server.New(serverConfig, dataStore, temporalClient, blacklistRegistry)

	// Start server in a goroutine
	go func() {
		if err := srv.Start(); err != nil {
			logger.Fatal("Server failed", zap.Error(err))
		}
	}()

	// Wait for interrupt signal to gracefully shutdown the server
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh

	logger.Info("Shutting down server...")

	// Create shutdown context with timeout
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Shutdown server
	if err := srv.Shutdown(ctx); err != nil {
		logger.Fatal("Server forced to shutdown", zap.Error(err))
	}

	logger.Info("API server stopped")
}
