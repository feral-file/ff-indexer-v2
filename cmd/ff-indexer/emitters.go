package main

import (
	"context"
	"errors"
	"time"

	"go.uber.org/zap"

	"github.com/feral-file/ff-indexer-v2/internal/adapter"
	"github.com/feral-file/ff-indexer-v2/internal/block"
	"github.com/feral-file/ff-indexer-v2/internal/config"
	"github.com/feral-file/ff-indexer-v2/internal/emitter"
	"github.com/feral-file/ff-indexer-v2/internal/logger"
	"github.com/feral-file/ff-indexer-v2/internal/messaging"
	"github.com/feral-file/ff-indexer-v2/internal/providers/ethereum"
	"github.com/feral-file/ff-indexer-v2/internal/providers/jetstream"
	"github.com/feral-file/ff-indexer-v2/internal/providers/tezos"
	"github.com/feral-file/ff-indexer-v2/internal/ratelimit"
	"github.com/feral-file/ff-indexer-v2/internal/store"
)

// newEmitterNATSPublisher opens a dedicated NATS JetStream publisher for chain emitters (separate from bridge).
func newEmitterNATSPublisher(ctx context.Context, cfg *config.AppConfig, jsonAdapter adapter.JSON, connectionSuffix string) (messaging.Publisher, error) {
	return jetstream.NewPublisher(ctx, jetstream.Config{
		URL:            cfg.NATS.URL,
		StreamName:     cfg.NATS.StreamName,
		MaxReconnects:  cfg.NATS.MaxReconnects,
		ReconnectWait:  cfg.NATS.ReconnectWait,
		ConnectionName: cfg.NATS.ConnectionName + connectionSuffix,
	}, adapter.NewNatsJetStream(), jsonAdapter)
}

// runEthereumEmitter runs the Ethereum chain → NATS emitter until ctx is done.
func runEthereumEmitter(
	ctx context.Context,
	cfg *config.AppConfig,
	dataStore store.Store,
	natsPub messaging.Publisher,
) error {
	// WebSocket client, block cache, subscriber, then emitter loop until shutdown or NATS closes.
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

	ethSubscriber, err := ethereum.NewSubscriber(ctx, ethereum.Config{
		WebSocketURL:    cfg.Ethereum.WebSocketURL,
		ChainID:         cfg.Ethereum.ChainID,
		WorkerPoolSize:  cfg.Worker.WorkerPoolSize,
		WorkerQueueSize: cfg.Worker.WorkerQueueSize,
	}, ethereumClient, clockAdapter)
	if err != nil {
		return err
	}
	defer ethSubscriber.Close()

	emitterCfg := emitter.Config{
		ChainID:         cfg.Ethereum.ChainID,
		StartBlock:      cfg.Ethereum.StartBlock,
		CursorSaveFreq:  2,
		CursorSaveDelay: 30 * time.Second,
	}
	eventEmitter := emitter.NewEmitter(ethSubscriber, natsPub, dataStore, emitterCfg, clockAdapter)
	defer eventEmitter.Close()

	errCh := make(chan error, 1)
	go func() {
		errCh <- eventEmitter.Run(ctx)
	}()

	select {
	case err := <-errCh:
		if err != nil && !errors.Is(err, context.Canceled) {
			logger.ErrorCtx(ctx, errors.New("ethereum emitter error"), zap.Error(err))
		}
		return err
	case <-natsPub.CloseChan():
		logger.ErrorCtx(ctx, errors.New("NATS closed (ethereum emitter)"), zap.Error(err))
		return errors.New("nats publisher closed")
	case <-ctx.Done():
		return ctx.Err()
	}
}

// runTezosEmitter runs the Tezos chain → NATS emitter until ctx is done.
func runTezosEmitter(
	ctx context.Context,
	cfg *config.AppConfig,
	dataStore store.Store,
	natsPub messaging.Publisher,
	rateLimitProxy ratelimit.Proxy,
) error {
	// TzKT + SignalR subscriber, then emitter loop until shutdown or NATS closes.
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
	tzktClient := tezos.NewTzKTClient(cfg.Tezos.ChainID, cfg.Tezos.APIURL, httpClient, rateLimitProxy, clockAdapter, tzBlockProvider)

	tezosSubscriber, err := tezos.NewSubscriber(tezos.Config{
		WebSocketURL:    cfg.Tezos.WebSocketURL,
		ChainID:         cfg.Tezos.ChainID,
		WorkerPoolSize:  cfg.Worker.WorkerPoolSize,
		WorkerQueueSize: cfg.Worker.WorkerQueueSize,
	}, signalR, clockAdapter, tzktClient)
	if err != nil {
		return err
	}
	defer tezosSubscriber.Close()

	emitterCfg := emitter.Config{
		ChainID:         cfg.Tezos.ChainID,
		StartBlock:      cfg.Tezos.StartLevel,
		CursorSaveFreq:  2,
		CursorSaveDelay: 30 * time.Second,
	}
	eventEmitter := emitter.NewEmitter(tezosSubscriber, natsPub, dataStore, emitterCfg, clockAdapter)
	defer eventEmitter.Close()

	errCh := make(chan error, 1)
	go func() {
		errCh <- eventEmitter.Run(ctx)
	}()

	select {
	case err := <-errCh:
		if err != nil && !errors.Is(err, context.Canceled) {
			logger.ErrorCtx(ctx, errors.New("tezos emitter error"), zap.Error(err))
		}
		return err
	case <-natsPub.CloseChan():
		logger.ErrorCtx(ctx, errors.New("NATS closed (tezos emitter)"), zap.Error(err))
		return errors.New("nats publisher closed")
	case <-ctx.Done():
		return ctx.Err()
	}
}
