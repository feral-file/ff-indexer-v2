//go:build cgo

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
	"github.com/feral-file/ff-indexer-v2/internal/downloader"
	"github.com/feral-file/ff-indexer-v2/internal/logger"
	"github.com/feral-file/ff-indexer-v2/internal/media/processor"
	"github.com/feral-file/ff-indexer-v2/internal/media/rasterizer"
	"github.com/feral-file/ff-indexer-v2/internal/providers/cloudflare"
	temporal "github.com/feral-file/ff-indexer-v2/internal/providers/temporal"
	"github.com/feral-file/ff-indexer-v2/internal/store"
	"github.com/feral-file/ff-indexer-v2/internal/uri"
	workflowsmedia "github.com/feral-file/ff-indexer-v2/internal/workflows/media"
)

var (
	configFile = flag.String("config", "", "Path to configuration file")
	envPath    = flag.String("env", "config/", "Path to environment files")
)

func main() {
	flag.Parse()

	// Load configuration
	config.ChdirRepoRoot()
	cfg, err := config.LoadWorkerMediaConfig(*configFile, *envPath)
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
			"service": "worker-media",
		},
	})
	if err != nil {
		panic(fmt.Sprintf("Failed to initialize logger: %v", err))
	}
	defer logger.Flush(2 * time.Second)
	logger.InfoCtx(ctx, "Starting Worker Media")

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
	ioAdapter := adapter.NewIO()
	jsonAdapter := adapter.NewJSON()
	fileSystem := adapter.NewFileSystem()
	resvgClient := adapter.NewResvgClient()
	imageEncoder := adapter.NewImageEncoder()

	// Initialize HTTP client
	httpClient := adapter.NewHTTPClient(15 * time.Second)

	// Initialize URI resolver
	uriResolverConfig := &uri.Config{
		IPFSGateways:    cfg.URI.IPFSGateways,
		ArweaveGateways: cfg.URI.ArweaveGateways,
		OnChFSGateways:  cfg.URI.OnchfsGateways,
	}
	uriResolver := uri.NewResolver(httpClient, uriResolverConfig)

	// Initialize Cloudflare client
	cfClient, err := adapter.NewCloudflareClient(cfg.Cloudflare.APIToken)
	if err != nil {
		logger.FatalCtx(ctx, "Failed to create Cloudflare client", zap.Error(err))
	}

	// Initialize downloader for media file downloads
	mediaDownloader := downloader.NewDownloader(httpClient, fileSystem)

	// Initialize Cloudflare media provider (handles both Images and Stream)
	cloudflareConfig := &cloudflare.Config{
		AccountID: cfg.Cloudflare.AccountID,
		APIToken:  cfg.Cloudflare.APIToken,
	}
	mediaProvider := cloudflare.NewMediaProvider(cfClient, cloudflareConfig, mediaDownloader, fileSystem)

	logger.InfoCtx(ctx, "Initialized Cloudflare media provider",
		zap.String("accountID", cfg.Cloudflare.AccountID),
	)

	// Initialize SVG rasterizer with adapters and config
	svgRasterizer := rasterizer.NewRasterizer(resvgClient, imageEncoder, &rasterizer.Config{
		Width: cfg.Rasterizer.Width,
	})

	// Initialize media processor
	mediaProcessor := processor.NewProcessor(httpClient, uriResolver, mediaProvider, dataStore, svgRasterizer, fileSystem, ioAdapter, jsonAdapter, mediaDownloader, cfg.MaxStaticImageSize, cfg.MaxAnimatedImageSize, cfg.MaxVideoSize)

	// Initialize media executor for media processing activities
	mediaExecutor := workflowsmedia.NewExecutor(dataStore, mediaProcessor)

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

	logger.InfoCtx(ctx, "Connected to Temporal",
		zap.String("host_port", cfg.Temporal.HostPort),
		zap.String("namespace", cfg.Temporal.Namespace),
	)

	// Create Temporal worker with logger and Sentry interceptor
	sentryInterceptor := temporal.NewSentryActivityInterceptor()
	temporalWorker := worker.New(temporalClient,
		cfg.Temporal.MediaTaskQueue,
		worker.Options{
			MaxConcurrentActivityExecutionSize: cfg.Temporal.MaxConcurrentActivityExecutionSize,
			WorkerActivitiesPerSecond:          cfg.Temporal.WorkerActivitiesPerSecond,
			MaxConcurrentActivityTaskPollers:   cfg.Temporal.MaxConcurrentActivityTaskPollers,
			Interceptors: []interceptor.WorkerInterceptor{
				sentryInterceptor,
			},
		})

	// Create media worker instance
	mediaWorker := workflowsmedia.NewWorker(mediaExecutor)

	// Register media workflows
	temporalWorker.RegisterWorkflow(mediaWorker.IndexMediaWorkflow)
	temporalWorker.RegisterWorkflow(mediaWorker.IndexMultipleMediaWorkflow)
	logger.InfoCtx(ctx, "Registered media workflows")

	// Register media processing activity
	temporalWorker.RegisterActivity(mediaExecutor.IndexMediaFile)
	logger.InfoCtx(ctx, "Registered media processing activity")

	// Start the worker
	err = temporalWorker.Start()
	if err != nil {
		logger.FatalCtx(ctx, "Failed to start Temporal worker", zap.Error(err))
	}

	logger.InfoCtx(ctx, "Worker Media started successfully",
		zap.String("token_task_queue", cfg.Temporal.TokenTaskQueue),
	)

	// Wait for interrupt signal
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	<-sigCh

	logger.InfoCtx(ctx, "Shutting down Worker Media...")

	// Stop the worker
	temporalWorker.Stop()

	logger.InfoCtx(ctx, "Worker Media stopped")
}
