package main

import (
	"context"
	"time"

	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/interceptor"
	"go.temporal.io/sdk/worker"
	"go.uber.org/zap"
	"gorm.io/gorm"

	"github.com/feral-file/ff-indexer-v2/internal/adapter"
	"github.com/feral-file/ff-indexer-v2/internal/block"
	"github.com/feral-file/ff-indexer-v2/internal/config"
	"github.com/feral-file/ff-indexer-v2/internal/logger"
	"github.com/feral-file/ff-indexer-v2/internal/metadata"
	"github.com/feral-file/ff-indexer-v2/internal/providers/ethereum"
	temporal "github.com/feral-file/ff-indexer-v2/internal/providers/temporal"
	"github.com/feral-file/ff-indexer-v2/internal/providers/tezos"
	"github.com/feral-file/ff-indexer-v2/internal/providers/vendors/artblocks"
	"github.com/feral-file/ff-indexer-v2/internal/providers/vendors/feralfile"
	"github.com/feral-file/ff-indexer-v2/internal/providers/vendors/objkt"
	"github.com/feral-file/ff-indexer-v2/internal/providers/vendors/opensea"
	"github.com/feral-file/ff-indexer-v2/internal/ratelimit"
	"github.com/feral-file/ff-indexer-v2/internal/registry"
	"github.com/feral-file/ff-indexer-v2/internal/store"
	"github.com/feral-file/ff-indexer-v2/internal/uri"
	"github.com/feral-file/ff-indexer-v2/internal/workflows"
)

// registerWorkerCore wires the token-indexing Temporal worker (worker-core / token task queue).
func registerWorkerCore(
	ctx context.Context,
	cfg *config.WorkerCoreConfig,
	db *gorm.DB,
	temporalClient client.Client,
	rateLimitProxy ratelimit.Proxy,
) (run func(context.Context) error, cleanup func(context.Context) error, err error) {
	// Store and shared adapters.
	dataStore := store.NewPGStore(db)

	jsonAdapter := adapter.NewJSON()
	jcsAdapter := adapter.NewJCS()
	clockAdapter := adapter.NewClock()
	fs := adapter.NewFileSystem()
	base64Adapter := adapter.NewBase64()
	ioAdapter := adapter.NewIO()
	temporalActivityAdapter := adapter.NewActivity()
	temporalWorkflowAdapter := adapter.NewWorkflow()

	httpClient := adapter.NewHTTPClient(15 * time.Second)

	// Chain clients (Ethereum RPC + Tezos TzKT) and vendor APIs.
	ethDialer := adapter.NewEthClientDialer()
	adapterEthClient, err := ethDialer.Dial(ctx, cfg.Ethereum.RPCURL)
	if err != nil {
		return nil, nil, err
	}

	ethBlockFetcher := ethereum.NewEthereumBlockFetcher(adapterEthClient)
	ethBlockProvider := block.NewBlockProvider(ethBlockFetcher,
		block.Config{
			TTL:               cfg.Ethereum.BlockHeadTTL * time.Second,
			StaleWindow:       cfg.Ethereum.BlockHeadStaleWindow * time.Second,
			BlockTimestampTTL: 0,
		}, clockAdapter)
	ethereumClient := ethereum.NewClient(cfg.Ethereum.ChainID, adapterEthClient, clockAdapter, ethBlockProvider)

	tzBlockFetcher := tezos.NewTezosBlockFetcher(cfg.Tezos.APIURL, httpClient, clockAdapter)
	tzBlockProvider := block.NewBlockProvider(tzBlockFetcher,
		block.Config{
			TTL:               cfg.Tezos.BlockHeadTTL * time.Second,
			StaleWindow:       cfg.Tezos.BlockHeadStaleWindow * time.Second,
			BlockTimestampTTL: 0,
		}, clockAdapter)
	tzktClient := tezos.NewTzKTClient(cfg.Tezos.ChainID, cfg.Tezos.APIURL, httpClient, rateLimitProxy, clockAdapter, tzBlockProvider)

	artblocksClient := artblocks.NewClient(httpClient, cfg.Vendors.ArtBlocksURL, jsonAdapter)
	feralfileClient := feralfile.NewClient(httpClient, cfg.Vendors.FeralFileURL)
	objktClient := objkt.NewClient(httpClient, rateLimitProxy, cfg.Vendors.ObjktURL, cfg.Vendors.ObjktAPIKey, jsonAdapter)
	openseaClient := opensea.NewClient(httpClient, rateLimitProxy, cfg.Vendors.OpenSeaURL, cfg.Vendors.OpenSeaAPIKey, jsonAdapter)

	publisherLoader := registry.NewPublisherRegistryLoader(fs, jsonAdapter)
	blacklistLoader := registry.NewBlacklistRegistryLoader(fs, jsonAdapter)

	var publisherRegistry registry.PublisherRegistry
	if cfg.PublisherRegistryPath != "" {
		publisherRegistry, err = publisherLoader.Load(cfg.PublisherRegistryPath)
		if err != nil {
			adapterEthClient.Close()
			return nil, nil, err
		}
		logger.InfoCtx(ctx, "Loaded publisher registry", zap.String("path", cfg.PublisherRegistryPath))
	} else {
		logger.WarnCtx(ctx, "Publisher registry path not configured, publisher resolution will be disabled")
	}

	var blacklistRegistry registry.BlacklistRegistry
	if cfg.BlacklistPath != "" {
		blacklistRegistry, err = blacklistLoader.Load(cfg.BlacklistPath)
		if err != nil {
			adapterEthClient.Close()
			return nil, nil, err
		}
		logger.InfoCtx(ctx, "Loaded blacklist registry", zap.String("path", cfg.BlacklistPath))
	} else {
		logger.WarnCtx(ctx, "Blacklist registry path not configured, all contracts will be allowed")
	}

	// Metadata pipeline and workflow executor.
	uriResolver := uri.NewResolver(httpClient, &uri.Config{
		IPFSGateways:    cfg.URI.IPFSGateways,
		ArweaveGateways: cfg.URI.ArweaveGateways,
		OnChFSGateways:  cfg.URI.OnchfsGateways,
	})
	uriConfig := &uri.Config{
		IPFSGateways:    cfg.URI.IPFSGateways,
		ArweaveGateways: cfg.URI.ArweaveGateways,
		OnChFSGateways:  cfg.URI.OnchfsGateways,
	}
	urlChecker := uri.NewURLChecker(httpClient, ioAdapter, uriConfig)
	dataURIChecker := uri.NewDataURIChecker()

	metadataEnhancer := metadata.NewEnhancer(httpClient, uriResolver, artblocksClient, feralfileClient, objktClient, openseaClient, jsonAdapter, jcsAdapter)
	metadataResolver := metadata.NewResolver(ethereumClient, tzktClient, httpClient, uriResolver, jsonAdapter, clockAdapter, jcsAdapter, base64Adapter, dataStore, publisherRegistry)

	if publisherRegistry != nil {
		if err := metadataResolver.LoadDeployerCacheFromDB(ctx); err != nil {
			logger.WarnCtx(ctx, "Failed to load deployer cache from DB", zap.Error(err))
		}
	}

	executor := workflows.NewExecutor(
		dataStore,
		metadataResolver,
		metadataEnhancer,
		ethereumClient,
		tzktClient,
		jsonAdapter,
		clockAdapter,
		httpClient,
		ioAdapter,
		temporalActivityAdapter,
		blacklistRegistry,
		urlChecker,
		dataURIChecker)

	// Temporal worker: register workflows and activities on the token task queue.
	sentryInterceptor := temporal.NewSentryActivityInterceptor()
	temporalWorker := worker.New(
		temporalClient,
		cfg.Temporal.TokenTaskQueue,
		worker.Options{
			MaxConcurrentActivityExecutionSize: cfg.Temporal.MaxConcurrentActivityExecutionSize,
			WorkerActivitiesPerSecond:          cfg.Temporal.WorkerActivitiesPerSecond,
			MaxConcurrentActivityTaskPollers:   cfg.Temporal.MaxConcurrentActivityTaskPollers,
			Interceptors: []interceptor.WorkerInterceptor{
				sentryInterceptor,
			},
		})

	workerCore := workflows.NewWorkerCore(executor,
		workflows.WorkerCoreConfig{
			EthereumTokenSweepStartBlock:       cfg.EthereumTokenSweepStartBlock,
			TezosTokenSweepStartBlock:          cfg.TezosTokenSweepStartBlock,
			EthereumChainID:                    cfg.Ethereum.ChainID,
			TezosChainID:                       cfg.Tezos.ChainID,
			EthereumOwnerFirstBatchTarget:      cfg.EthereumOwnerFirstBatchTarget,
			EthereumOwnerSubsequentBatchTarget: cfg.EthereumOwnerSubsequentBatchTarget,
			TezosOwnerFirstBatchTarget:         cfg.TezosOwnerFirstBatchTarget,
			TezosOwnerSubsequentBatchTarget:    cfg.TezosOwnerSubsequentBatchTarget,
			MediaTaskQueue:                     cfg.Temporal.MediaTaskQueue,
			BudgetedIndexingModeEnabled:        cfg.BudgetedIndexingEnabled,
			BudgetedIndexingDefaultDailyQuota:  cfg.BudgetedIndexingDefaultDailyQuota,
		}, blacklistRegistry, temporalWorkflowAdapter)

	temporalWorker.RegisterWorkflow(workerCore.IndexTokenMint)
	temporalWorker.RegisterWorkflow(workerCore.IndexTokenTransfer)
	temporalWorker.RegisterWorkflow(workerCore.IndexTokenBurn)
	temporalWorker.RegisterWorkflow(workerCore.IndexMetadataUpdate)
	temporalWorker.RegisterWorkflow(workerCore.IndexTokenMetadata)
	temporalWorker.RegisterWorkflow(workerCore.IndexTokenFromEvent)
	temporalWorker.RegisterWorkflow(workerCore.IndexTokenProvenances)
	temporalWorker.RegisterWorkflow(workerCore.IndexTokens)
	temporalWorker.RegisterWorkflow(workerCore.IndexToken)
	temporalWorker.RegisterWorkflow(workerCore.IndexTokenOwner)
	temporalWorker.RegisterWorkflow(workerCore.IndexTezosTokenOwner)
	temporalWorker.RegisterWorkflow(workerCore.IndexEthereumTokenOwner)
	temporalWorker.RegisterWorkflow(workerCore.IndexMultipleTokensMetadata)
	temporalWorker.RegisterWorkflow(workerCore.NotifyWebhookClients)
	temporalWorker.RegisterWorkflow(workerCore.DeliverWebhook)

	temporalWorker.RegisterActivity(executor.CreateTokenMint)
	temporalWorker.RegisterActivity(executor.ResolveTokenMetadata)
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
	temporalWorker.RegisterActivity(executor.GetActiveWebhookClientsByEventType)
	temporalWorker.RegisterActivity(executor.GetWebhookClientByID)
	temporalWorker.RegisterActivity(executor.CreateWebhookDeliveryRecord)
	temporalWorker.RegisterActivity(executor.DeliverWebhookHTTP)
	temporalWorker.RegisterActivity(executor.GetQuotaInfo)
	temporalWorker.RegisterActivity(executor.IncrementTokensIndexed)
	temporalWorker.RegisterActivity(executor.CreateIndexingJob)
	temporalWorker.RegisterActivity(executor.UpdateIndexingJobStatus)
	temporalWorker.RegisterActivity(executor.UpdateIndexingJobProgress)
	temporalWorker.RegisterActivity(executor.CheckMediaURLsHealthAndUpdateViewability)

	// Run blocks until worker exits or ctx is canceled; cleanup closes the Ethereum RPC client.
	run = func(ctx context.Context) error {
		errCh := make(chan error, 1)
		go func() {
			errCh <- temporalWorker.Start()
		}()
		select {
		case err := <-errCh:
			return err
		case <-ctx.Done():
			temporalWorker.Stop()
			<-errCh
			return ctx.Err()
		}
	}
	cleanup = func(context.Context) error {
		adapterEthClient.Close()
		return nil
	}
	return run, cleanup, nil
}
