//go:build cgo

package main

import (
	"context"
	"errors"
	"fmt"
	"time"

	"gorm.io/gorm"

	"github.com/feral-file/ff-indexer-v2/internal/adapter"
	"github.com/feral-file/ff-indexer-v2/internal/config"
	"github.com/feral-file/ff-indexer-v2/internal/downloader"
	"github.com/feral-file/ff-indexer-v2/internal/logger"
	"github.com/feral-file/ff-indexer-v2/internal/media/processor"
	"github.com/feral-file/ff-indexer-v2/internal/media/rasterizer"
	"github.com/feral-file/ff-indexer-v2/internal/media/transformer"
	"github.com/feral-file/ff-indexer-v2/internal/providers/cloudflare"
	"github.com/feral-file/ff-indexer-v2/internal/providers/jobs"
	"github.com/feral-file/ff-indexer-v2/internal/store"
	"github.com/feral-file/ff-indexer-v2/internal/uri"
	"github.com/feral-file/ff-indexer-v2/internal/workflows"
)

// registerWorkerMedia wires the media-indexing jobs.Worker (worker-media / media_index queue).
func registerWorkerMedia(
	_ context.Context,
	cfg *config.AppConfig,
	db *gorm.DB,
) (run func(context.Context) error, cleanup func(context.Context) error, err error) {
	wcfg := cfg.ToWorkerMediaConfig()
	if !wcfg.MediaEnabled {
		logger.Warn("Media job worker disabled by config (FF_INDEXER_MEDIA_ENABLED=false)")
		run = func(ctx context.Context) error {
			<-ctx.Done()
			return ctx.Err()
		}
		cleanup = func(context.Context) error { return nil }
		return run, cleanup, nil
	}
	if wcfg.Cloudflare.AccountID == "" {
		return nil, nil, errors.New("cloudflare.account_id is required when media worker is enabled")
	}

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

	mediaExecutor := workflows.NewMediaExecutor(dataStore, mediaProcessor)
	jobQueue := jobs.NewJobQueue(dataStore, jsonAdapter)

	mediaWf := workflows.NewMediaWorkflows(mediaExecutor, jobQueue, workflows.MediaWorkflowsConfig{
		MediaTaskQueue: wcfg.Jobs.MediaQueue,
	})

	reg := jobs.NewRegistry(jsonAdapter)
	reg.Register("IndexMediaWorkflow", mediaWf.IndexMediaWorkflow)
	reg.Register("IndexMultipleMediaWorkflow", mediaWf.IndexMultipleMediaWorkflow)

	mw := wcfg.Jobs.MediaWorker
	jWorker := jobs.NewWorker(dataStore, reg, jobs.WorkerConfig{
		Queue:          wcfg.Jobs.MediaQueue,
		Concurrency:    mw.Concurrency,
		PollInterval:   mw.PollInterval,
		BatchSize:      mw.BatchSize,
		CancelInterval: mw.CancelInterval,
	})

	// Run until worker stops or ctx is canceled; cleanup closes transform resources.
	run = func(ctx context.Context) error {
		return jWorker.Run(ctx)
	}

	cleanup = func(ctx context.Context) error {
		_ = ctx
		return errors.Join(
			imageTransformer.Close(),
			browserRasterizer.Close(),
		)
	}

	return run, cleanup, nil
}
