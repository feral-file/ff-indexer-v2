package main

import (
	"context"
	"errors"
	"time"

	"go.temporal.io/sdk/client"
	"go.uber.org/zap"

	"github.com/feral-file/ff-indexer-v2/internal/adapter"
	"github.com/feral-file/ff-indexer-v2/internal/block"
	"github.com/feral-file/ff-indexer-v2/internal/config"
	"github.com/feral-file/ff-indexer-v2/internal/ingestion"
	"github.com/feral-file/ff-indexer-v2/internal/logger"
	"github.com/feral-file/ff-indexer-v2/internal/providers/ethereum"
	"github.com/feral-file/ff-indexer-v2/internal/providers/tezos"
	"github.com/feral-file/ff-indexer-v2/internal/ratelimit"
	"github.com/feral-file/ff-indexer-v2/internal/registry"
	"github.com/feral-file/ff-indexer-v2/internal/store"
)

// runEthereumIngestion runs Ethereum chain ingestion until ctx is done.
func runEthereumIngestion(
	ctx context.Context,
	cfg *config.AppConfig,
	dataStore store.Store,
	temporalClient client.Client,
	blacklistRegistry registry.BlacklistRegistry,
) error {
	clockAdapter := adapter.NewClock()
	ethDialer := adapter.NewEthClientDialer()
	adapterEthClient, err := ethDialer.Dial(ctx, cfg.Ethereum.WebSocketURL)
	if err != nil {
		return err
	}
	defer adapterEthClient.Close()

	ethBlockFetcher := ethereum.NewEthereumBlockFetcher(adapterEthClient)
	ethBlockProvider := block.NewBlockProvider(ethBlockFetcher,
		block.Config{
			TTL:               cfg.Ethereum.BlockHeadTTL * time.Second,
			StaleWindow:       cfg.Ethereum.BlockHeadStaleWindow * time.Second,
			BlockTimestampTTL: 0,
		}, clockAdapter)
	ethereumClient := ethereum.NewClient(cfg.Ethereum.ChainID, adapterEthClient, clockAdapter, ethBlockProvider)

	source, err := ethereum.NewSubscriber(ethereum.Config{
		WebSocketURL: cfg.Ethereum.WebSocketURL,
		ChainID:      cfg.Ethereum.ChainID,
	}, ethereumClient)
	if err != nil {
		return err
	}

	runner := ingestion.NewRunner(
		ctx,
		source,
		dataStore,
		temporalClient,
		blacklistRegistry,
		ingestion.Config{
			ChainID:           cfg.Ethereum.ChainID,
			StartBlock:        cfg.Ethereum.StartBlock,
			TemporalTaskQueue: cfg.Temporal.TokenTaskQueue,
		},
		clockAdapter,
	)
	defer func() { _ = runner.Close() }()

	return runIngestion(ctx, runner, "ethereum")
}

// runTezosIngestion runs Tezos chain ingestion until ctx is done.
func runTezosIngestion(
	ctx context.Context,
	cfg *config.AppConfig,
	dataStore store.Store,
	temporalClient client.Client,
	blacklistRegistry registry.BlacklistRegistry,
	rateLimiter ratelimit.Limiter,
) error {
	clockAdapter := adapter.NewClock()
	signalR := adapter.NewSignalR()
	httpClient := adapter.NewHTTPClient(15 * time.Second)

	tzBlockFetcher := tezos.NewTezosBlockFetcher(cfg.Tezos.APIURL, httpClient, clockAdapter)
	tzBlockProvider := block.NewBlockProvider(tzBlockFetcher,
		block.Config{
			TTL:               cfg.Tezos.BlockHeadTTL * time.Second,
			StaleWindow:       cfg.Tezos.BlockHeadStaleWindow * time.Second,
			BlockTimestampTTL: 0,
		}, clockAdapter)
	tzktClient := tezos.NewTzKTClient(cfg.Tezos.ChainID, cfg.Tezos.APIURL, httpClient, rateLimiter, clockAdapter, tzBlockProvider)

	source, err := tezos.NewSubscriber(tezos.Config{
		WebSocketURL: cfg.Tezos.WebSocketURL,
		ChainID:      cfg.Tezos.ChainID,
	}, signalR, clockAdapter, tzktClient)
	if err != nil {
		return err
	}

	runner := ingestion.NewRunner(
		ctx,
		source,
		dataStore,
		temporalClient,
		blacklistRegistry,
		ingestion.Config{
			ChainID:           cfg.Tezos.ChainID,
			StartBlock:        cfg.Tezos.StartLevel,
			TemporalTaskQueue: cfg.Temporal.TokenTaskQueue,
		},
		clockAdapter,
	)
	defer func() { _ = runner.Close() }()

	return runIngestion(ctx, runner, "tezos")
}

func runIngestion(ctx context.Context, runner ingestion.Runner, component string) error {
	errCh := make(chan error, 1)
	go func() {
		errCh <- runner.Run(ctx)
	}()

	select {
	case err := <-errCh:
		logIngestionError(ctx, component, err)
		return err
	case <-ctx.Done():
		err := <-errCh
		if err != nil && !errors.Is(err, context.Canceled) {
			logIngestionError(ctx, component, err)
			return err
		}
		return ctx.Err()
	}
}

func logIngestionError(ctx context.Context, component string, err error) {
	if err != nil && !errors.Is(err, context.Canceled) {
		logger.ErrorCtx(ctx, errors.New(component+" ingestion error"), zap.Error(err))
	}
}
