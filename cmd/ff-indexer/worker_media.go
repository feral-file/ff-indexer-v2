//go:build cgo

package main

import (
	"context"
	"fmt"
	"time"

	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/interceptor"
	"go.temporal.io/sdk/worker"
	"gorm.io/gorm"

	"github.com/feral-file/ff-indexer-v2/internal/adapter"
	"github.com/feral-file/ff-indexer-v2/internal/config"
	"github.com/feral-file/ff-indexer-v2/internal/downloader"
	"github.com/feral-file/ff-indexer-v2/internal/media/processor"
	"github.com/feral-file/ff-indexer-v2/internal/media/rasterizer"
	"github.com/feral-file/ff-indexer-v2/internal/media/transformer"
	"github.com/feral-file/ff-indexer-v2/internal/providers/cloudflare"
	temporal "github.com/feral-file/ff-indexer-v2/internal/providers/temporal"
	"github.com/feral-file/ff-indexer-v2/internal/store"
	"github.com/feral-file/ff-indexer-v2/internal/uri"
	workflowsmedia "github.com/feral-file/ff-indexer-v2/internal/workflows/media"
)

// registerWorkerMedia wires the media-indexing Temporal worker (worker-media).
func registerWorkerMedia(
	_ context.Context,
	cfg *config.AppConfig,
	db *gorm.DB,
	temporalClient client.Client,
) (run func(context.Context) error, cleanup func(context.Context) error, err error) {
	wcfg := cfg.ToWorkerMediaConfig()

	// Store and I/O adapters.
	dataStore := store.NewPGStore(db)

	ioAdapter := adapter.NewIO()
	jsonAdapter := adapter.NewJSON()
	fileSystem := adapter.NewFileSystem()
	resvgClient := adapter.NewResvgClient()
	imageEncoder := adapter.NewImageEncoder()
	chromedpClient := adapter.NewChromedpClient()
	xml := adapter.NewXML()

	httpClient := adapter.NewHTTPClient(15 * time.Second)

	uriResolverConfig := &uri.Config{
		IPFSGateways:    wcfg.URI.IPFSGateways,
		ArweaveGateways: wcfg.URI.ArweaveGateways,
		OnChFSGateways:  wcfg.URI.OnchfsGateways,
	}
	uriResolver := uri.NewResolver(httpClient, uriResolverConfig)

	// Cloudflare media + download path; rasterizer and image transform pipeline.
	cfClient, err := adapter.NewCloudflareClient(wcfg.Cloudflare.APIToken)
	if err != nil {
		return nil, nil, fmt.Errorf("cloudflare client: %w", err)
	}

	mediaDownloaderHTTPClient := adapter.NewHTTPClient(15 * time.Minute)
	mediaDownloader := downloader.NewDownloader(mediaDownloaderHTTPClient, fileSystem)

	cloudflareConfig := &cloudflare.Config{
		AccountID: wcfg.Cloudflare.AccountID,
		APIToken:  wcfg.Cloudflare.APIToken,
	}
	mediaProvider := cloudflare.NewMediaProvider(cfClient, cloudflareConfig, mediaDownloader, fileSystem)

	browserRasterizer := rasterizer.NewBrowserRasterizer(
		chromedpClient,
		xml,
		adapter.NewFileSystem(),
		&rasterizer.BrowserRasterizerConfig{
			Width:     wcfg.Rasterizer.Width,
			TimeoutMs: wcfg.Rasterizer.TimeoutMs,
		})
	svgRasterizer := rasterizer.NewRasterizer(resvgClient, imageEncoder, browserRasterizer,
		&rasterizer.Config{
			Width:                 wcfg.Rasterizer.Width,
			EnableBrowserFallback: wcfg.Rasterizer.BrowserFallbackEnabled,
		})

	vipsClient := adapter.NewVipsClient()
	imageTransformer := transformer.NewTransformer(wcfg.Transform, httpClient, ioAdapter, vipsClient)

	dataURIChecker := uri.NewDataURIChecker()

	mediaProcessor := processor.NewProcessor(httpClient, uriResolver, dataURIChecker, mediaProvider, dataStore, svgRasterizer, fileSystem, ioAdapter, jsonAdapter, mediaDownloader, imageTransformer, wcfg.MaxImageSize, wcfg.MaxVideoSize)

	mediaExecutor := workflowsmedia.NewExecutor(dataStore, mediaProcessor)

	sentryInterceptor := temporal.NewSentryActivityInterceptor()
	temporalWorker := worker.New(temporalClient,
		wcfg.Temporal.MediaTaskQueue,
		worker.Options{
			MaxConcurrentActivityExecutionSize: wcfg.Temporal.MaxConcurrentActivityExecutionSize,
			WorkerActivitiesPerSecond:          wcfg.Temporal.WorkerActivitiesPerSecond,
			MaxConcurrentActivityTaskPollers:   wcfg.Temporal.MaxConcurrentActivityTaskPollers,
			Interceptors: []interceptor.WorkerInterceptor{
				sentryInterceptor,
			},
		})

	mediaWorker := workflowsmedia.NewWorker(mediaExecutor)

	temporalWorker.RegisterWorkflow(mediaWorker.IndexMediaWorkflow)
	temporalWorker.RegisterWorkflow(mediaWorker.IndexMultipleMediaWorkflow)
	temporalWorker.RegisterActivity(mediaExecutor.IndexMediaFile)

	// Run until worker stops or ctx is canceled; cleanup closes transform resources.
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

	cleanup = func(ctx context.Context) error {
		_ = ctx
		if err := imageTransformer.Close(); err != nil {
			return err
		}
		return nil
	}

	return run, cleanup, nil
}
