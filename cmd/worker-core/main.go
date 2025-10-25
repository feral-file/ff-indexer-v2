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
	"go.temporal.io/sdk/worker"
	"go.uber.org/zap"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"

	"github.com/feral-file/ff-indexer-v2/internal/adapter"
	"github.com/feral-file/ff-indexer-v2/internal/config"
	"github.com/feral-file/ff-indexer-v2/internal/metadata"
	"github.com/feral-file/ff-indexer-v2/internal/providers/ethereum"
	"github.com/feral-file/ff-indexer-v2/internal/providers/tezos"
	"github.com/feral-file/ff-indexer-v2/internal/providers/vendors/artblocks"
	"github.com/feral-file/ff-indexer-v2/internal/providers/vendors/fxhash"
	"github.com/feral-file/ff-indexer-v2/internal/store"
	"github.com/feral-file/ff-indexer-v2/internal/workflows"
)

var (
	configPath = flag.String("config", "config.yaml", "Path to configuration file")
)

func main() {
	flag.Parse()

	// Load configuration
	cfg, err := config.LoadWorkerCoreConfig(*configPath)
	if err != nil {
		panic(fmt.Sprintf("Failed to load config: %v", err))
	}

	// Create context with cancellation
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Initialize logger
	err = logger.Initialize(cfg.Debug,
		&sentry.ClientOptions{
			Dsn:   cfg.SentryDSN,
			Debug: cfg.Debug,
		})
	if err != nil {
		panic(fmt.Sprintf("Failed to initialize logger: %v", err))
	}
	logger.Info("Starting Worker Core")

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
	clockAdapter := adapter.NewClock()

	// Initialize ethereum client
	httpClient := adapter.NewHTTPClient(30 * time.Second)
	ethDialer := adapter.NewEthClientDialer()
	adapterEthClient, err := ethDialer.Dial(ctx, cfg.Ethereum.RPCURL)
	if err != nil {
		logger.Fatal("Failed to dial Ethereum RPC", zap.Error(err), zap.String("rpc_url", cfg.Ethereum.RPCURL))
	}
	defer adapterEthClient.Close()
	ethereumClient := ethereum.NewClient(cfg.Ethereum.ChainID, adapterEthClient, clockAdapter)

	logger.Info("Connected to Ethereum RPC", zap.String("rpc_url", cfg.Ethereum.RPCURL))

	// Initialize Tezos client
	tzktClient := tezos.NewTzKTClient(cfg.Tezos.ChainID, cfg.Tezos.APIURL, httpClient, clockAdapter)

	// Initialize vendors
	artblocksClient := artblocks.NewClient(httpClient)
	fxhashClient := fxhash.NewClient(httpClient)

	// Load publisher registry
	var publisherRegistry *metadata.PublisherRegistry
	if cfg.PublisherRegistryPath != "" {
		registry, err := metadata.LoadPublisherRegistry(cfg.PublisherRegistryPath)
		if err != nil {
			logger.Fatal("Failed to load publisher registry",
				zap.Error(err),
				zap.String("path", cfg.PublisherRegistryPath))
		}
		publisherRegistry = registry
		logger.Info("Loaded publisher registry", zap.String("path", cfg.PublisherRegistryPath))
	} else {
		logger.Warn("Publisher registry path not configured, publisher resolution will be disabled")
	}

	// Initialize metadata enhancer and resolver
	metadataEnhancer := metadata.NewEnhancer(artblocksClient, fxhashClient)
	metadataResolver := metadata.NewResolver(ethereumClient, tzktClient, httpClient, jsonAdapter, clockAdapter, dataStore, publisherRegistry)

	// Load deployer cache from DB if resolver has store and registry
	if publisherRegistry != nil {
		if err := metadataResolver.LoadDeployerCacheFromDB(ctx); err != nil {
			logger.Warn("Failed to load deployer cache from DB", zap.Error(err))
		}
	}

	// Initialize executor for activities
	executor := workflows.NewExecutor(dataStore, metadataResolver, metadataEnhancer, ethereumClient, tzktClient, jsonAdapter, clockAdapter)

	// Connect to Temporal
	temporalClient, err := client.Dial(client.Options{
		HostPort:  cfg.Temporal.HostPort,
		Namespace: cfg.Temporal.Namespace,
	})
	if err != nil {
		logger.Fatal("Failed to connect to Temporal", zap.Error(err), zap.String("host_port", cfg.Temporal.HostPort))
	}
	defer temporalClient.Close()
	logger.Info("Connected to Temporal", zap.String("namespace", cfg.Temporal.Namespace))

	// Create Temporal worker
	temporalWorker := worker.New(
		temporalClient,
		cfg.Temporal.TaskQueue,
		worker.Options{
			MaxConcurrentActivityExecutionSize: cfg.Temporal.MaxConcurrentActivityExecutionSize,
			WorkerActivitiesPerSecond:          cfg.Temporal.WorkerActivitiesPerSecond,
		})
	logger.Info("Created Temporal worker", zap.String("taskQueue", cfg.Temporal.TaskQueue))

	// Create worker core instance
	workerCore := workflows.NewWorkerCore(executor,
		workflows.WorkerCoreConfig{
			EthereumTokenSweepStartBlock:     cfg.EthereumTokenSweepStartBlock,
			EthereumTokenSweepBlockChunkSize: cfg.EthereumTokenSweepBlockChunkSize,
			TezosTokenSweepStartBlock:        cfg.TezosTokenSweepStartBlock,
			TezosTokenSweepBlockChunkSize:    cfg.TezosTokenSweepBlockChunkSize,
			EthereumChainID:                  cfg.Ethereum.ChainID,
			TezosChainID:                     cfg.Tezos.ChainID,
		})

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
	logger.Info("Registered workflows")

	// Register activities
	// Activities will be called by workflows
	temporalWorker.RegisterActivity(executor.CreateTokenMint)
	temporalWorker.RegisterActivity(executor.FetchTokenMetadata)
	temporalWorker.RegisterActivity(executor.UpsertTokenMetadata)
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
	logger.Info("Registered activities")

	// Start worker
	err = temporalWorker.Start()
	if err != nil {
		logger.Fatal("Failed to start worker", zap.Error(err))
	}
	logger.Info("Worker started and listening for tasks")

	// Wait for interrupt signal
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh

	logger.Info("Shutting down worker...")
	temporalWorker.Stop()
	logger.Info("Worker stopped")
}
