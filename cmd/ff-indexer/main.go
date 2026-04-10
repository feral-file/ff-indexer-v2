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
	"golang.org/x/sync/errgroup"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/plugin/dbresolver"

	"github.com/feral-file/ff-indexer-v2/internal/adapter"
	"github.com/feral-file/ff-indexer-v2/internal/api/middleware"
	"github.com/feral-file/ff-indexer-v2/internal/api/server"
	"github.com/feral-file/ff-indexer-v2/internal/bridge"
	"github.com/feral-file/ff-indexer-v2/internal/config"
	"github.com/feral-file/ff-indexer-v2/internal/logger"
	temporal "github.com/feral-file/ff-indexer-v2/internal/providers/temporal"
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

	// Temporal client (shared by API, bridge, workers, sweeper).
	temporalLogger := temporal.NewZapLoggerAdapter(logger.Default())
	temporalClient, err := client.Dial(client.Options{
		HostPort:  cfg.Temporal.HostPort,
		Namespace: cfg.Temporal.Namespace,
		Logger:    temporalLogger,
	})
	if err != nil {
		logger.FatalCtx(rootCtx, "Failed to connect to Temporal", zap.Error(err))
	}
	defer temporalClient.Close()

	// Redis-backed rate limiter (vendor APIs, Tezos paths, worker-core).
	clockAdapter := adapter.NewClock()
	redisAdapter := adapter.NewRedisClient(cfg.RateLimiter.RedisAddr, cfg.RateLimiter.RedisPassword, cfg.RateLimiter.RedisDB)
	defer func() { _ = redisAdapter.Close() }()

	rateLimitProxy, err := ratelimit.NewProxy(cfg.RateLimiter, redisAdapter, clockAdapter)
	if err != nil {
		logger.FatalCtx(rootCtx, "Failed to initialize rate limit proxy", zap.Error(err))
	}
	defer func() { _ = rateLimitProxy.Close() }()

	jsonAdapter := adapter.NewJSON()

	// NATS JetStream publishers (emitters) and consumer bridge — one connection each.
	ethNatsPub, err := newEmitterNATSPublisher(rootCtx, cfg, jsonAdapter, "-ethereum-emitter")
	if err != nil {
		logger.FatalCtx(rootCtx, "Failed to create Ethereum NATS publisher", zap.Error(err))
	}
	defer ethNatsPub.Close()

	tezNatsPub, err := newEmitterNATSPublisher(rootCtx, cfg, jsonAdapter, "-tezos-emitter")
	if err != nil {
		logger.FatalCtx(rootCtx, "Failed to create Tezos NATS publisher", zap.Error(err))
	}
	defer tezNatsPub.Close()

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

	eventBridge, err := bridge.NewBridge(
		rootCtx,
		bridge.Config{
			URL:               cfg.NATS.URL,
			StreamName:        cfg.NATS.StreamName,
			ConsumerName:      cfg.NATS.ConsumerName,
			MaxReconnects:     cfg.NATS.MaxReconnects,
			ReconnectWait:     cfg.NATS.ReconnectWait,
			ConnectionName:    cfg.NATS.ConnectionName + "-event-bridge",
			AckWaitTimeout:    cfg.NATS.AckWait,
			MaxDeliver:        cfg.NATS.MaxDeliver,
			TemporalTaskQueue: cfg.Temporal.TokenTaskQueue,
		},
		adapter.NewNatsJetStream(),
		dataStore,
		temporalClient,
		jsonAdapter,
		blacklistRegistry,
	)
	if err != nil {
		logger.FatalCtx(rootCtx, "Failed to create event bridge", zap.Error(err))
	}
	defer eventBridge.Close()

	// Worker-core: token task queue + dedicated Ethereum RPC client for activities.
	wCoreCfg := cfg.ToWorkerCoreConfig()
	runWorkerCore, cleanupWorkerCore, err := registerWorkerCore(rootCtx, wCoreCfg, db, temporalClient, rateLimitProxy)
	if err != nil {
		logger.FatalCtx(rootCtx, "Failed to init worker-core", zap.Error(err))
	}

	// REST/GraphQL API.
	apiCfg := cfg.ToAPIConfig()
	srv := newAPIServer(apiCfg, dataStore, temporalClient, blacklistRegistry)

	// Media URL health sweeper (Temporal-driven batch checks).
	sweeperCfg := cfg.ToSweeperConfig()
	httpClient := adapter.NewHTTPClient(sweeperCfg.MediaHealthSweeper.HTTPTimeout)
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
	mediaSweeper := sweeper.NewMediaHealthSweeper(mediaSweeperConfig, dataStore, urlHealthChecker, dataURIChecker, clock, temporalClient, sweeperCfg.Temporal.TokenTaskQueue)

	// Worker-media: media task queue (requires CGO build).
	runWorkerMedia, cleanupWorkerMedia, err := registerWorkerMedia(rootCtx, cfg, db, temporalClient)
	if err != nil {
		logger.FatalCtx(rootCtx, "Failed to init worker-media", zap.Error(err))
	}

	// Subsystems: HTTP, chain emitters, bridge, Temporal workers, sweeper.
	g, ctx := errgroup.WithContext(rootCtx)

	g.Go(func() error {
		return runHTTPServer(ctx, srv)
	})

	g.Go(func() error {
		return runEthereumEmitter(ctx, cfg, dataStore, ethNatsPub)
	})

	g.Go(func() error {
		return runTezosEmitter(ctx, cfg, dataStore, tezNatsPub, rateLimitProxy)
	})

	g.Go(func() error {
		errCh := make(chan error, 1)
		go func() {
			err := eventBridge.Run(ctx)
			errCh <- err
		}()
		select {
		case err := <-errCh:
			if err != nil && !errors.Is(err, context.Canceled) {
				return err
			}
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	})

	g.Go(func() error {
		return runWorkerCore(ctx)
	})

	g.Go(func() error {
		return runWorkerMedia(ctx)
	})

	g.Go(func() error {
		return runSweeper(ctx, mediaSweeper)
	})

	if err := g.Wait(); err != nil && !errors.Is(err, context.Canceled) {
		logger.ErrorCtx(rootCtx, errors.New("ff-indexer stopped with error"), zap.Error(err))
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := cleanupWorkerMedia(shutdownCtx); err != nil {
		logger.WarnCtx(rootCtx, "worker-media cleanup", zap.Error(err))
	}
	if err := cleanupWorkerCore(shutdownCtx); err != nil {
		logger.WarnCtx(rootCtx, "worker-core cleanup", zap.Error(err))
	}

	logger.Info("ff-indexer stopped")
}

func newAPIServer(
	cfg *config.APIConfig,
	dataStore store.Store,
	temporalClient client.Client,
	blacklistRegistry registry.BlacklistRegistry,
) *server.Server {
	jsonAdapter := adapter.NewJSON()
	clockAdapter := adapter.NewClock()
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
		TezosChainID:    cfg.Tezos.ChainID,
		EthereumChainID: cfg.Ethereum.ChainID,
	}
	return server.New(serverConfig, dataStore, temporalClient, blacklistRegistry, jsonAdapter, clockAdapter)
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
