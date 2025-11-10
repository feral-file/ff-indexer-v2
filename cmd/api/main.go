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
	"github.com/feral-file/ff-indexer-v2/internal/api/middleware"
	"github.com/feral-file/ff-indexer-v2/internal/api/server"
	"github.com/feral-file/ff-indexer-v2/internal/config"
	"github.com/feral-file/ff-indexer-v2/internal/logger"
	temporal "github.com/feral-file/ff-indexer-v2/internal/providers/temporal"
	"github.com/feral-file/ff-indexer-v2/internal/registry"
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
	cfg, err := config.LoadAPIConfig(*configFile, *envPath)
	if err != nil {
		panic(fmt.Sprintf("Failed to load config: %v", err))
	}

	// Create shutdown context with timeout
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Initialize logger with sentry integration
	err = logger.Initialize(logger.Config{
		Debug:           cfg.Debug,
		SentryDSN:       cfg.SentryDSN,
		BreadcrumbLevel: zapcore.InfoLevel,
		Tags: map[string]string{
			"service": "api-server",
		},
	})
	if err != nil {
		panic(fmt.Sprintf("Failed to initialize logger: %v", err))
	}
	defer logger.Flush(2 * time.Second)
	logger.InfoCtx(ctx, "Starting Feral File Indexer API")

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

	// Initialize adapters
	fs := adapter.NewFileSystem()
	jsonAdapter := adapter.NewJSON()

	// Connect to Temporal with logger integration
	temporalLogger := temporal.NewZapLoggerAdapter(logger.Default())
	temporalClient, err := client.Dial(client.Options{
		HostPort:  cfg.Temporal.HostPort,
		Namespace: cfg.Temporal.Namespace,
		Logger:    temporalLogger, // Use zap logger adapter for Temporal client
	})
	if err != nil {
		logger.FatalCtx(ctx, "Failed to connect to Temporal", zap.Error(err))
	}
	defer temporalClient.Close()
	logger.InfoCtx(ctx, "Connected to Temporal", zap.String("host_port", cfg.Temporal.HostPort))

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

	// Create server config
	serverConfig := server.Config{
		Debug:                 cfg.Debug,
		Host:                  cfg.Server.Host,
		Port:                  cfg.Server.Port,
		ReadTimeout:           time.Duration(cfg.Server.ReadTimeout) * time.Second,
		WriteTimeout:          time.Duration(cfg.Server.WriteTimeout) * time.Second,
		IdleTimeout:           time.Duration(cfg.Server.IdleTimeout) * time.Second,
		OrchestratorTaskQueue: cfg.Temporal.TokenTaskQueue,
		Auth: middleware.AuthConfig{
			JWTPublicKey: cfg.Auth.JWTPublicKey,
			APIKeys:      cfg.Auth.APIKeys,
		},
	}

	// Create and start server
	srv := server.New(serverConfig, dataStore, temporalClient, blacklistRegistry)

	// Start server in a goroutine
	errCh := make(chan error, 1)
	go func() {
		if err := srv.Start(ctx); err != nil {
			errCh <- err
		}
	}()

	// Wait for interrupt signal to gracefully shutdown the server
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	select {
	case sig := <-sigCh:
		logger.InfoCtx(ctx, "Received shutdown signal", zap.String("signal", sig.String()))
		cancel()
	case err := <-errCh:
		logger.ErrorCtx(ctx, err, zap.String("component", "server"))
		cancel()
	}

	// Create shutdown context with timeout (don't use canceled ctx)
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()

	logger.InfoCtx(shutdownCtx, "Shutting down server...")

	// Shutdown server
	if err := srv.Shutdown(shutdownCtx); err != nil {
		logger.FatalCtx(shutdownCtx, "Server forced to shutdown", zap.Error(err))
	}

	// Use non-context logger for final message since original ctx is canceled
	logger.Info("API server stopped")
}
