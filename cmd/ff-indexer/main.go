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
	"golang.org/x/sync/errgroup"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/plugin/dbresolver"

	"github.com/feral-file/ff-indexer-v2/internal/adapter"
	"github.com/feral-file/ff-indexer-v2/internal/api/middleware"
	"github.com/feral-file/ff-indexer-v2/internal/api/server"
	"github.com/feral-file/ff-indexer-v2/internal/config"
	"github.com/feral-file/ff-indexer-v2/internal/logger"
	"github.com/feral-file/ff-indexer-v2/internal/providers/jobs"
	"github.com/feral-file/ff-indexer-v2/internal/ratelimit"
	"github.com/feral-file/ff-indexer-v2/internal/registry"
	"github.com/feral-file/ff-indexer-v2/internal/store"
	"github.com/feral-file/ff-indexer-v2/internal/sweeper"
	"github.com/feral-file/ff-indexer-v2/internal/uri"
)

var (
	configFile = flag.String("config", "", "Path to configuration file")
	envPath    = flag.String("env", "config/", "Path to environment files")
)

func main() {
	os.Exit(run())
}

func run() int {
	flag.Parse()
	config.ChdirRepoRoot()

	cfg, err := config.LoadAppConfig(*configFile, *envPath)
	if err != nil {
		panic(fmt.Sprintf("Failed to load config: %v", err))
	}

	rootCtx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	err = logger.Initialize(logger.Config{
		Debug:           cfg.Debug,
		SentryDSN:       cfg.SentryDSN,
		BreadcrumbLevel: zapcore.InfoLevel,
		Tags: map[string]string{
			"service": "ff-indexer",
		},
	})
	if err != nil {
		panic(fmt.Sprintf("Failed to initialize logger: %v", err))
	}
	defer logger.Flush(2 * time.Second)

	logger.InfoCtx(rootCtx, "Starting ff-indexer (unified process)")

	// Postgres (writer + optional read replica).
	db, err := gorm.Open(postgres.Open(cfg.Database.DSN()), &gorm.Config{})
	if err != nil {
		logger.FatalCtx(rootCtx, "Failed to connect to database", zap.Error(err), zap.String("dsn", cfg.Database.DSN()))
	}
	sqlDB, err := db.DB()
	if err != nil {
		logger.FatalCtx(rootCtx, "Failed to get sql DB", zap.Error(err))
	}
	defer func() { _ = sqlDB.Close() }()

	maxOpenConns, maxIdleConns, connMaxLifetime, connMaxIdleTime :=
		store.NormalizeConnectionPoolSettings(cfg.Database.MaxOpenConns, cfg.Database.MaxIdleConns, cfg.Database.ConnMaxLifetime, cfg.Database.ConnMaxIdleTime)

	if cfg.Database.ReadHost != "" {
		resolver := dbresolver.Register(dbresolver.Config{
			Replicas: []gorm.Dialector{
				postgres.Open(cfg.Database.ReadDSN()),
			},
		}).
			SetMaxOpenConns(maxOpenConns).
			SetMaxIdleConns(maxIdleConns).
			SetConnMaxLifetime(connMaxLifetime).
			SetConnMaxIdleTime(connMaxIdleTime)

		if err := db.Use(resolver); err != nil {
			logger.FatalCtx(rootCtx, "Failed to configure database read replica", zap.Error(err))
		}
		logger.InfoCtx(rootCtx, "Configured database read replica", zap.String("read_host", cfg.Database.ReadHost))
	}

	if err := store.ConfigureConnectionPool(db, maxOpenConns, maxIdleConns, connMaxLifetime, connMaxIdleTime); err != nil {
		logger.FatalCtx(rootCtx, "Failed to configure connection pool", zap.Error(err))
	}

	dataStore := store.NewPGStore(db)

	jsonAdapter := adapter.NewJSON()
	jobQueue := jobs.NewJobQueue(dataStore, jsonAdapter)

	// Process-local rate limiter for vendor APIs and Tezos paths.
	rateLimiter, err := ratelimit.NewLimiter(cfg.RateLimiter)
	if err != nil {
		logger.FatalCtx(rootCtx, "Failed to initialize rate limiter", zap.Error(err))
	}
	defer func() { _ = rateLimiter.Close() }()

	fs := adapter.NewFileSystem()
	var blacklistRegistry registry.BlacklistRegistry
	if cfg.BlacklistPath != "" {
		blacklistLoader := registry.NewBlacklistRegistryLoader(fs, jsonAdapter)
		blacklistRegistry, err = blacklistLoader.Load(cfg.BlacklistPath)
		if err != nil {
			logger.FatalCtx(rootCtx, "Failed to load blacklist registry", zap.Error(err), zap.String("path", cfg.BlacklistPath))
		}
		logger.InfoCtx(rootCtx, "Loaded blacklist registry", zap.String("path", cfg.BlacklistPath))
	} else {
		logger.WarnCtx(rootCtx, "Blacklist registry path not configured, all contracts will be allowed")
	}

	// Worker-core: token job queue + dedicated Ethereum RPC client for handlers.
	wCoreCfg := cfg.ToWorkerCoreConfig()
	runWorkerCore, cleanupWorkerCore, err := registerWorkerCore(rootCtx, wCoreCfg, db, rateLimiter)
	if err != nil {
		logger.FatalCtx(rootCtx, "Failed to init worker-core", zap.Error(err))
	}

	// REST/GraphQL API.
	apiCfg := cfg.ToAPIConfig()
	srv := newAPIServer(apiCfg, dataStore, jobQueue, blacklistRegistry)

	// Media URL health sweeper (scheduled batch checks; may enqueue jobs).
	sweeperCfg := cfg.ToSweeperConfig()
	ssrfValidator, err := config.SSRFValidatorFromProtection(cfg.Security.SSRFProtection)
	if err != nil {
		logger.FatalCtx(rootCtx, "Invalid SSRF security configuration", zap.Error(err))
	}
	httpClient := adapter.NewHTTPClientWithSSRF(sweeperCfg.MediaHealthSweeper.HTTPTimeout, ssrfValidator, cfg.Security.SSRFProtection.MaxRedirects)
	if ssrfValidator != nil {
		logger.InfoCtx(rootCtx, "Outbound media HTTP client uses SSRF validation (sweeper; token worker; media worker when CGO enabled)",
			zap.Int("max_redirects", cfg.Security.SSRFProtection.MaxRedirects),
			zap.Bool("block_multicast", cfg.Security.SSRFProtection.BlockMulticast),
			zap.Int("ssrf_allowlist_domains", len(cfg.Security.SSRFProtection.Allowlist.Domains)),
			zap.Int("ssrf_allowlist_ips", len(cfg.Security.SSRFProtection.Allowlist.IPs)),
		)
	} else {
		logger.WarnCtx(rootCtx, "Outbound media HTTP SSRF validation is DISABLED (security.ssrf_protection.enabled=false)")
	}
	ioAdapter := adapter.NewIO()
	clock := adapter.NewClock()
	uriResolverConfig := &uri.Config{
		IPFSGateways:    sweeperCfg.MediaHealthSweeper.URI.IPFSGateways,
		ArweaveGateways: sweeperCfg.MediaHealthSweeper.URI.ArweaveGateways,
		OnChFSGateways:  sweeperCfg.MediaHealthSweeper.URI.OnchfsGateways,
	}
	urlHealthChecker := uri.NewURLChecker(httpClient, ioAdapter, uriResolverConfig)
	dataURIChecker := uri.NewDataURIChecker()
	mediaSweeperConfig := &sweeper.MediaHealthSweeperConfig{
		BatchSize:      sweeperCfg.MediaHealthSweeper.BatchSize,
		WorkerPoolSize: sweeperCfg.MediaHealthSweeper.Worker.WorkerPoolSize,
		RecheckAfter:   sweeperCfg.MediaHealthSweeper.RecheckAfter,
	}
	mediaSweeper := sweeper.NewMediaHealthSweeper(mediaSweeperConfig, dataStore, urlHealthChecker, dataURIChecker, clock, jobQueue, cfg.Jobs.TokenQueue)

	// Worker-media: media task queue (requires CGO build).
	wMediaCfg := cfg.ToWorkerMediaConfig()
	runWorkerMedia, cleanupWorkerMedia, err := registerWorkerMedia(rootCtx, wMediaCfg, db)
	if err != nil {
		logger.FatalCtx(rootCtx, "Failed to init worker-media", zap.Error(err))
	}

	// Subsystems: HTTP, chain listeners, postgres job workers, sweeper.
	g, ctx := errgroup.WithContext(rootCtx)

	g.Go(func() error {
		componentCtx := logger.WithComponent(ctx, logger.ComponentHTTPServer)
		return runHTTPServer(componentCtx, srv)
	})

	g.Go(func() error {
		componentCtx := logger.WithComponent(ctx, logger.ComponentEthereumIngestion)
		return runEthereumIngestion(componentCtx, cfg, dataStore, jobQueue, blacklistRegistry)
	})

	g.Go(func() error {
		componentCtx := logger.WithComponent(ctx, logger.ComponentTezosIngestion)
		return runTezosIngestion(componentCtx, cfg, dataStore, jobQueue, blacklistRegistry, rateLimiter)
	})

	g.Go(func() error {
		componentCtx := logger.WithComponent(ctx, logger.ComponentWorkerCore)
		return runWorkerCore(componentCtx)
	})

	g.Go(func() error {
		componentCtx := logger.WithComponent(ctx, logger.ComponentWorkerMedia)
		return runWorkerMedia(componentCtx)
	})

	g.Go(func() error {
		componentCtx := logger.WithComponent(ctx, logger.ComponentSweeper)
		return runSweeper(componentCtx, mediaSweeper)
	})

	if err := waitForSubsystems(rootCtx, g, cleanupWorkerMedia, cleanupWorkerCore); err != nil {
		logger.ErrorCtx(rootCtx, errors.New("ff-indexer stopped with error"), zap.Error(err))
		return 1
	}

	return 0
}

type waitGroup interface {
	Wait() error
}

func waitForSubsystems(
	rootCtx context.Context,
	g waitGroup,
	cleanupWorkerMedia func(context.Context) error,
	cleanupWorkerCore func(context.Context) error,
) error {
	err := g.Wait()

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := cleanupWorkerMedia(shutdownCtx); err != nil {
		logger.WarnCtx(rootCtx, "worker-media cleanup", zap.Error(err))
	}
	if err := cleanupWorkerCore(shutdownCtx); err != nil {
		logger.WarnCtx(rootCtx, "worker-core cleanup", zap.Error(err))
	}

	if err != nil && !errors.Is(err, context.Canceled) {
		return err
	}

	logger.Info("ff-indexer stopped")
	return nil
}

func newAPIServer(
	cfg *config.APIConfig,
	dataStore store.Store,
	jq jobs.JobQueue,
	blacklistRegistry registry.BlacklistRegistry,
) *server.Server {
	jsonAdapter := adapter.NewJSON()
	clockAdapter := adapter.NewClock()
	serverConfig := server.Config{
		Debug:        cfg.Debug,
		Host:         cfg.Server.Host,
		Port:         cfg.Server.Port,
		ReadTimeout:  time.Duration(cfg.Server.ReadTimeout) * time.Second,
		WriteTimeout: time.Duration(cfg.Server.WriteTimeout) * time.Second,
		IdleTimeout:  time.Duration(cfg.Server.IdleTimeout) * time.Second,
		TokenQueue:   cfg.Jobs.TokenQueue,
		Auth: middleware.AuthConfig{
			JWTPublicKey: cfg.Auth.JWTPublicKey,
			APIKeys:      cfg.Auth.APIKeys,
		},
		TezosChainID:    cfg.Tezos.ChainID,
		EthereumChainID: cfg.Ethereum.ChainID,
	}
	return server.New(serverConfig, dataStore, jq, blacklistRegistry, jsonAdapter, clockAdapter)
}

func runHTTPServer(ctx context.Context, srv *server.Server) error {
	errCh := make(chan error, 1)
	go func() {
		errCh <- srv.Start(ctx)
	}()
	select {
	case err := <-errCh:
		return err
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := srv.Shutdown(shutdownCtx); err != nil {
			return err
		}
		<-errCh
		return ctx.Err()
	}
}

func runSweeper(ctx context.Context, mediaSweeper sweeper.Sweeper) error {
	errCh := make(chan error, 1)
	go func() {
		errCh <- mediaSweeper.Start(ctx)
	}()
	select {
	case err := <-errCh:
		return err
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = mediaSweeper.Stop(shutdownCtx)
		return ctx.Err()
	}
}
