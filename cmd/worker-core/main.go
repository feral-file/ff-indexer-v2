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
	"go.temporal.io/sdk/interceptor"
	"go.temporal.io/sdk/worker"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"

	"github.com/feral-file/ff-indexer-v2/internal/adapter"
	"github.com/feral-file/ff-indexer-v2/internal/config"
	"github.com/feral-file/ff-indexer-v2/internal/logger"
	"github.com/feral-file/ff-indexer-v2/internal/metadata"
	"github.com/feral-file/ff-indexer-v2/internal/providers/ethereum"
	temporal "github.com/feral-file/ff-indexer-v2/internal/providers/temporal"
	"github.com/feral-file/ff-indexer-v2/internal/providers/tezos"
	"github.com/feral-file/ff-indexer-v2/internal/providers/vendors/artblocks"
	"github.com/feral-file/ff-indexer-v2/internal/providers/vendors/fxhash"
	"github.com/feral-file/ff-indexer-v2/internal/registry"
	"github.com/feral-file/ff-indexer-v2/internal/store"
	"github.com/feral-file/ff-indexer-v2/internal/uri"
	"github.com/feral-file/ff-indexer-v2/internal/workflows"
)

var (
	configFile = flag.String("config", "", "Path to configuration file")
	envPath    = flag.String("env", "config/", "Path to environment files")
)

func main() {
	flag.Parse()

	// Load configuration
	config.ChdirRepoRoot()
	cfg, err := config.LoadWorkerCoreConfig(*configFile, *envPath)
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
			"service": "worker-core",
		},
	})
	if err != nil {
		panic(fmt.Sprintf("Failed to initialize logger: %v", err))
	}
	defer logger.Flush(2 * time.Second)
	logger.InfoCtx(ctx, "Starting Worker Core")

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
	jsonAdapter := adapter.NewJSON()
	jcsAdapter := adapter.NewJCS()
	clockAdapter := adapter.NewClock()
	fs := adapter.NewFileSystem()
	base64Adapter := adapter.NewBase64()

	// Initialize ethereum client
	httpClient := adapter.NewHTTPClient(30 * time.Second)
	ethDialer := adapter.NewEthClientDialer()
	adapterEthClient, err := ethDialer.Dial(ctx, cfg.Ethereum.RPCURL)
	if err != nil {
		logger.FatalCtx(ctx, "Failed to dial Ethereum RPC", zap.Error(err), zap.String("rpc_url", cfg.Ethereum.RPCURL))
	}
	defer adapterEthClient.Close()
	ethereumClient := ethereum.NewClient(cfg.Ethereum.ChainID, adapterEthClient, clockAdapter)

	logger.InfoCtx(ctx, "Connected to Ethereum RPC", zap.String("rpc_url", cfg.Ethereum.RPCURL))

	// Initialize Tezos client
	tzktClient := tezos.NewTzKTClient(cfg.Tezos.ChainID, cfg.Tezos.APIURL, httpClient, clockAdapter)

	// Initialize vendors
	artblocksClient := artblocks.NewClient(httpClient, cfg.Vendors.ArtBlocksURL)
	fxhashClient := fxhash.NewClient(httpClient)

	// Initialize registry loaders
	publisherLoader := registry.NewPublisherRegistryLoader(fs, jsonAdapter)
	blacklistLoader := registry.NewBlacklistRegistryLoader(fs, jsonAdapter)

	// Load publisher registry
	var publisherRegistry registry.PublisherRegistry
	if cfg.PublisherRegistryPath != "" {
		publisherRegistry, err = publisherLoader.Load(cfg.PublisherRegistryPath)
		if err != nil {
			logger.FatalCtx(ctx, "Failed to load publisher registry",
				zap.Error(err),
				zap.String("path", cfg.PublisherRegistryPath))
		}
		logger.InfoCtx(ctx, "Loaded publisher registry", zap.String("path", cfg.PublisherRegistryPath))
	} else {
		logger.WarnCtx(ctx, "Publisher registry path not configured, publisher resolution will be disabled")
	}

	// Load blacklist registry
	var blacklistRegistry registry.BlacklistRegistry
	if cfg.BlacklistPath != "" {
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

	// Initialize URI resolver
	uriResolver := uri.NewResolver(httpClient, &uri.Config{
		IPFSGateways:    cfg.URI.IPFSGateways,
		ArweaveGateways: cfg.URI.ArweaveGateways,
		OnChFSGateways:  cfg.URI.OnchfsGateways,
	})

	// Initialize metadata enhancer and resolver
	metadataEnhancer := metadata.NewEnhancer(httpClient, uriResolver, artblocksClient, fxhashClient, jsonAdapter, jcsAdapter)
	metadataResolver := metadata.NewResolver(ethereumClient, tzktClient, httpClient, uriResolver, jsonAdapter, clockAdapter, jcsAdapter, base64Adapter, dataStore, publisherRegistry)

	// Load deployer cache from DB if resolver has store and registry
	if publisherRegistry != nil {
		if err := metadataResolver.LoadDeployerCacheFromDB(ctx); err != nil {
			logger.WarnCtx(ctx, "Failed to load deployer cache from DB", zap.Error(err))
		}
	}

	// Initialize executor for activities
	executor := workflows.NewExecutor(dataStore, metadataResolver, metadataEnhancer, ethereumClient, tzktClient, nil, jsonAdapter, clockAdapter)

	// Connect to Temporal with logger integration
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

	// Create Temporal worker with logger and Sentry interceptor
	sentryInterceptor := temporal.NewSentryActivityInterceptor()
	temporalWorker := worker.New(
		temporalClient,
		cfg.Temporal.TokenTaskQueue,
		worker.Options{
			MaxConcurrentActivityExecutionSize: cfg.Temporal.MaxConcurrentActivityExecutionSize,
			WorkerActivitiesPerSecond:          cfg.Temporal.WorkerActivitiesPerSecond,
			Interceptors: []interceptor.WorkerInterceptor{
				sentryInterceptor,
			},
		})
	logger.InfoCtx(ctx, "Created Temporal worker", zap.String("taskQueue", cfg.Temporal.TokenTaskQueue))

	// Create worker core instance
	workerCore := workflows.NewWorkerCore(executor,
		workflows.WorkerCoreConfig{
			EthereumTokenSweepStartBlock: cfg.EthereumTokenSweepStartBlock,
			TezosTokenSweepStartBlock:    cfg.TezosTokenSweepStartBlock,
			EthereumChainID:              cfg.Ethereum.ChainID,
			TezosChainID:                 cfg.Tezos.ChainID,
			MediaTaskQueue:               cfg.Temporal.MediaTaskQueue,
		}, blacklistRegistry)

	// Register workflows
	temporalWorker.RegisterWorkflow(workerCore.IndexTokenMint)
	temporalWorker.RegisterWorkflow(workerCore.IndexTokenTransfer)
	temporalWorker.RegisterWorkflow(workerCore.IndexTokenBurn)
	temporalWorker.RegisterWorkflow(workerCore.IndexMetadataUpdate)
	temporalWorker.RegisterWorkflow(workerCore.IndexTokenMetadata)
	temporalWorker.RegisterWorkflow(workerCore.IndexTokenFromEvent)
	temporalWorker.RegisterWorkflow(workerCore.IndexTokenProvenances)
	temporalWorker.RegisterWorkflow(workerCore.IndexTokens)
	temporalWorker.RegisterWorkflow(workerCore.IndexToken)
	temporalWorker.RegisterWorkflow(workerCore.IndexTokenOwners)
	temporalWorker.RegisterWorkflow(workerCore.IndexTokenOwner)
	temporalWorker.RegisterWorkflow(workerCore.IndexTezosTokenOwner)
	temporalWorker.RegisterWorkflow(workerCore.IndexEthereumTokenOwner)
	logger.InfoCtx(ctx, "Registered workflows")

	// Register activities
	// Activities will be called by workflows
	temporalWorker.RegisterActivity(executor.CreateTokenMint)
	temporalWorker.RegisterActivity(executor.FetchTokenMetadata)
	temporalWorker.RegisterActivity(executor.UpsertTokenMetadata)
	temporalWorker.RegisterActivity(executor.EnhanceTokenMetadata)
	temporalWorker.RegisterActivity(executor.UpdateTokenTransfer)
	temporalWorker.RegisterActivity(executor.UpdateTokenBurn)
	temporalWorker.RegisterActivity(executor.CreateMetadataUpdate)
	temporalWorker.RegisterActivity(executor.IndexTokenWithMinimalProvenancesByBlockchainEvent)
	temporalWorker.RegisterActivity(executor.IndexTokenWithFullProvenancesByTokenCID)
	temporalWorker.RegisterActivity(executor.CheckTokenExists)
	temporalWorker.RegisterActivity(executor.GetEthereumTokenCIDsByOwnerWithinBlockRange)
	temporalWorker.RegisterActivity(executor.GetLatestEthereumBlock)
	temporalWorker.RegisterActivity(executor.GetLatestTezosBlock)
	temporalWorker.RegisterActivity(executor.IndexTokenWithMinimalProvenancesByTokenCID)
	temporalWorker.RegisterActivity(executor.GetTezosTokenCIDsByAccountWithinBlockRange)
	temporalWorker.RegisterActivity(executor.GetIndexingBlockRangeForAddress)
	temporalWorker.RegisterActivity(executor.UpdateIndexingBlockRangeForAddress)
	temporalWorker.RegisterActivity(executor.EnsureWatchedAddressExists)
	logger.InfoCtx(ctx, "Registered activities")

	// Start worker
	err = temporalWorker.Start()
	if err != nil {
		logger.FatalCtx(ctx, "Failed to start worker", zap.Error(err))
	}
	logger.InfoCtx(ctx, "Worker started and listening for tasks")

	// Wait for interrupt signal
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh

	logger.InfoCtx(ctx, "Shutting down worker...")
	temporalWorker.Stop()
	logger.InfoCtx(ctx, "Worker stopped")
}
