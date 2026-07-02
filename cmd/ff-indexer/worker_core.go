package main

import (
	"context"
	"fmt"
	"time"

	"go.uber.org/zap"
	"gorm.io/gorm"

	"github.com/feral-file/ff-indexer-v2/internal/adapter"
	"github.com/feral-file/ff-indexer-v2/internal/block"
	"github.com/feral-file/ff-indexer-v2/internal/config"
	"github.com/feral-file/ff-indexer-v2/internal/logger"
	"github.com/feral-file/ff-indexer-v2/internal/metadata"
	"github.com/feral-file/ff-indexer-v2/internal/providers/ethereum"
	"github.com/feral-file/ff-indexer-v2/internal/providers/jobs"
	"github.com/feral-file/ff-indexer-v2/internal/providers/tezos"
	"github.com/feral-file/ff-indexer-v2/internal/providers/vendors/artblocks"
	"github.com/feral-file/ff-indexer-v2/internal/providers/vendors/feralfile"
	"github.com/feral-file/ff-indexer-v2/internal/providers/vendors/fxhash"
	"github.com/feral-file/ff-indexer-v2/internal/providers/vendors/objkt"
	"github.com/feral-file/ff-indexer-v2/internal/providers/vendors/opensea"
	"github.com/feral-file/ff-indexer-v2/internal/ratelimit"
	"github.com/feral-file/ff-indexer-v2/internal/registry"
	"github.com/feral-file/ff-indexer-v2/internal/store"
	"github.com/feral-file/ff-indexer-v2/internal/uri"
	"github.com/feral-file/ff-indexer-v2/internal/workflows"
)

// registerWorkerCore wires the token-indexing jobs.Worker (worker-core / token_index queue).
func registerWorkerCore(
	ctx context.Context,
	cfg *config.WorkerCoreConfig,
	db *gorm.DB,
	rateLimiter ratelimit.Limiter,
) (run func(context.Context) error, cleanup func(context.Context) error, err error) {
	// Store and shared adapters.
	dataStore := store.NewPGStore(db)

	jsonAdapter := adapter.NewJSON()
	jcsAdapter := adapter.NewJCS()
	clockAdapter := adapter.NewClock()
	fs := adapter.NewFileSystem()
	base64Adapter := adapter.NewBase64()
	ioAdapter := adapter.NewIO()

	ssrfValidator, err := config.SSRFValidatorFromProtection(cfg.Security.SSRFProtection)
	if err != nil {
		return nil, nil, fmt.Errorf("SSRF security configuration: %w", err)
	}
	httpClient := adapter.NewHTTPClientWithSSRF(15*time.Second, ssrfValidator, cfg.Security.SSRFProtection.MaxRedirects)
	if ssrfValidator != nil {
		logger.InfoCtx(ctx, "Token worker outbound HTTP uses SSRF validation",
			zap.Int("max_redirects", cfg.Security.SSRFProtection.MaxRedirects),
		)
	}

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
	ethereumClient, err := ethereum.NewClient(cfg.Ethereum.ChainID, adapterEthClient, clockAdapter, ethBlockProvider)
	if err != nil {
		return nil, nil, fmt.Errorf("initialize ethereum client: %w", err)
	}

	tzBlockFetcher := tezos.NewTezosBlockFetcher(cfg.Tezos.APIURL, httpClient, clockAdapter)
	tzBlockProvider := block.NewBlockProvider(tzBlockFetcher,
		block.Config{
			TTL:               cfg.Tezos.BlockHeadTTL * time.Second,
			StaleWindow:       cfg.Tezos.BlockHeadStaleWindow * time.Second,
			BlockTimestampTTL: 0,
		}, clockAdapter)
	tzktClient := tezos.NewTzKTClient(cfg.Tezos.ChainID, cfg.Tezos.APIURL, httpClient, rateLimiter, clockAdapter, tzBlockProvider)

	artblocksClient := artblocks.NewClient(httpClient, cfg.Vendors.ArtBlocksURL, jsonAdapter)
	feralfileClient := feralfile.NewClient(httpClient, cfg.Vendors.FeralFileURL)
	fxhashClient := fxhash.NewClient(httpClient, rateLimiter, cfg.Vendors.FxhashURL, jsonAdapter)
	objktClient := objkt.NewClient(httpClient, rateLimiter, cfg.Vendors.ObjktURL, cfg.Vendors.ObjktAPIKey, jsonAdapter)
	openseaClient := opensea.NewClient(httpClient, rateLimiter, cfg.Vendors.OpenSeaURL, cfg.Vendors.OpenSeaAPIKey, jsonAdapter)

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

	metadataEnhancer := metadata.NewEnhancer(httpClient, uriResolver, artblocksClient, feralfileClient, fxhashClient, objktClient, openseaClient, jsonAdapter, jcsAdapter)
	metadataResolver := metadata.NewResolver(ethereumClient, tzktClient, httpClient, uriResolver, jsonAdapter, clockAdapter, jcsAdapter, base64Adapter, dataStore, publisherRegistry)

	if publisherRegistry != nil {
		if err := metadataResolver.LoadDeployerCacheFromDB(ctx); err != nil {
			logger.WarnCtx(ctx, "Failed to load deployer cache from DB", zap.Error(err))
		}
	}

	executor := workflows.NewCoreExecutor(
		dataStore,
		metadataResolver,
		metadataEnhancer,
		ethereumClient,
		tzktClient,
		jsonAdapter,
		clockAdapter,
		httpClient,
		ioAdapter,
		blacklistRegistry,
		urlChecker,
		dataURIChecker)

	jobQueue := jobs.NewJobQueue(dataStore, jsonAdapter)

	core := workflows.NewCoreWorkflows(executor,
		workflows.CoreWorkflowsConfig{
			EthereumTokenSweepStartBlock:       cfg.EthereumTokenSweepStartBlock,
			TezosTokenSweepStartBlock:          cfg.TezosTokenSweepStartBlock,
			EthereumChainID:                    cfg.Ethereum.ChainID,
			TezosChainID:                       cfg.Tezos.ChainID,
			EthereumOwnerFirstBatchTarget:      cfg.EthereumOwnerFirstBatchTarget,
			EthereumOwnerSubsequentBatchTarget: cfg.EthereumOwnerSubsequentBatchTarget,
			TezosOwnerFirstBatchTarget:         cfg.TezosOwnerFirstBatchTarget,
			TezosOwnerSubsequentBatchTarget:    cfg.TezosOwnerSubsequentBatchTarget,
			TokenTaskQueue:                     cfg.Jobs.TokenQueue,
			MediaEnabled:                       cfg.MediaEnabled,
			MediaTaskQueue:                     cfg.Jobs.MediaQueue,
			BudgetedIndexingModeEnabled:        cfg.BudgetedIndexingEnabled,
			BudgetedIndexingDefaultDailyQuota:  cfg.BudgetedIndexingDefaultDailyQuota,
		}, blacklistRegistry, jobQueue)

	reg := jobs.NewRegistry(jsonAdapter)
	reg.Register("IndexTokenMint", core.IndexTokenMint)
	reg.Register("IndexTokenTransfer", core.IndexTokenTransfer)
	reg.Register("IndexTokenBurn", core.IndexTokenBurn)
	reg.Register("IndexMetadataUpdate", core.IndexMetadataUpdate)
	reg.Register("IndexToken", core.IndexToken)
	reg.Register("IndexTokens", core.IndexTokens)
	reg.Register("IndexTokenFromEvent", core.IndexTokenFromEvent)
	reg.Register("IndexTokenMetadata", core.IndexTokenMetadata)
	reg.Register("IndexMultipleTokensMetadata", core.IndexMultipleTokensMetadata)
	reg.Register("IndexTokenProvenances", core.IndexTokenProvenances)
	reg.Register("IndexTokenOwner", core.IndexTokenOwner)
	reg.Register("IndexTezosTokenOwner", core.IndexTezosTokenOwner)
	reg.Register("IndexEthereumTokenOwner", core.IndexEthereumTokenOwner)
	reg.Register("NotifyWebhookClients", core.NotifyWebhookClients)
	reg.Register("DeliverWebhook", core.DeliverWebhook)

	tw := cfg.Jobs.TokenWorker
	jWorker := jobs.NewWorker(dataStore, reg, jobs.WorkerConfig{
		Queue:          cfg.Jobs.TokenQueue,
		Concurrency:    tw.Concurrency,
		PollInterval:   tw.PollInterval,
		BatchSize:      tw.BatchSize,
		CancelInterval: tw.CancelInterval,
	})

	// Run blocks until worker exits or ctx is canceled; cleanup closes the Ethereum RPC client.
	run = func(ctx context.Context) error {
		return jWorker.Run(ctx)
	}
	cleanup = func(context.Context) error {
		adapterEthClient.Close()
		return nil
	}
	return run, cleanup, nil
}
