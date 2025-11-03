package main

import (
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
	"github.com/feral-file/ff-indexer-v2/internal/downloader"
	"github.com/feral-file/ff-indexer-v2/internal/media/processor"
	"github.com/feral-file/ff-indexer-v2/internal/providers/cloudflare"
	"github.com/feral-file/ff-indexer-v2/internal/store"
	"github.com/feral-file/ff-indexer-v2/internal/uri"
	"github.com/feral-file/ff-indexer-v2/internal/workflows"
)

var (
	configPath = flag.String("config", "config.yaml", "Path to configuration file")
)

func main() {
	flag.Parse()

	// Load configuration
	config.ChdirRepoRoot()
	cfg, err := config.LoadWorkerMediaConfig(*configPath)
	if err != nil {
		panic(fmt.Sprintf("Failed to load config: %v", err))
	}

	// Initialize logger
	err = logger.Initialize(cfg.Debug,
		&sentry.ClientOptions{
			Dsn:   cfg.SentryDSN,
			Debug: cfg.Debug,
		})
	if err != nil {
		panic(fmt.Sprintf("Failed to initialize logger: %v", err))
	}
	logger.Info("Starting Worker Media")

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
	fileSystem := adapter.NewFileSystem()

	// Initialize HTTP client
	httpClient := adapter.NewHTTPClient(30 * time.Second)

	// Initialize URI resolver
	uriResolverConfig := &uri.Config{
		IPFSGateways:    cfg.URI.IPFSGateways,
		ArweaveGateways: cfg.URI.ArweaveGateways,
	}
	uriResolver := uri.NewResolver(httpClient, uriResolverConfig)

	// Initialize Cloudflare client
	cfClient, err := adapter.NewCloudflareClient(cfg.Cloudflare.APIToken)
	if err != nil {
		logger.Fatal("Failed to create Cloudflare client", zap.Error(err))
	}

	// Initialize downloader for media file downloads
	mediaDownloader := downloader.NewDownloader(httpClient, fileSystem)

	// Initialize Cloudflare media provider (handles both Images and Stream)
	cloudflareConfig := &cloudflare.Config{
		AccountID: cfg.Cloudflare.AccountID,
		APIToken:  cfg.Cloudflare.APIToken,
	}
	mediaProvider := cloudflare.NewMediaProvider(cfClient, cloudflareConfig, mediaDownloader, fileSystem)

	logger.Info("Initialized Cloudflare media provider",
		zap.String("accountID", cfg.Cloudflare.AccountID),
	)

	// Initialize media processor
	mediaProcessor := processor.NewProcessor(httpClient, uriResolver, mediaProvider, dataStore, cfg.MaxStaticImageSize, cfg.MaxAnimatedImageSize, cfg.MaxVideoSize)

	// Initialize executor for activities (minimal setup for media processing only)
	// We pass nil for unused dependencies since worker-media only handles media activities
	executor := workflows.NewExecutor(
		dataStore,
		nil, // metadataResolver - not needed for media worker
		nil, // metadataEnhancer - not needed for media worker
		nil, // ethereumClient - not needed for media worker
		nil, // tzktClient - not needed for media worker
		mediaProcessor,
		jsonAdapter,
		clockAdapter,
	)

	// Connect to Temporal
	temporalClient, err := client.Dial(client.Options{
		HostPort:  cfg.Temporal.HostPort,
		Namespace: cfg.Temporal.Namespace,
	})
	if err != nil {
		logger.Fatal("Failed to connect to Temporal", zap.Error(err), zap.String("host_port", cfg.Temporal.HostPort))
	}
	defer temporalClient.Close()

	logger.Info("Connected to Temporal",
		zap.String("host_port", cfg.Temporal.HostPort),
		zap.String("namespace", cfg.Temporal.Namespace),
	)

	// Create Temporal worker for media processing
	temporalWorker := worker.New(temporalClient, cfg.Temporal.TaskQueue, worker.Options{
		MaxConcurrentActivityExecutionSize: cfg.Temporal.MaxConcurrentActivityExecutionSize,
		WorkerActivitiesPerSecond:          cfg.Temporal.WorkerActivitiesPerSecond,
	})

	// Create worker media instance
	workerMedia := workflows.NewWorkerMedia(executor)

	// Register media workflows
	temporalWorker.RegisterWorkflow(workerMedia.IndexMediaWorkflow)
	temporalWorker.RegisterWorkflow(workerMedia.IndexMultipleMediaWorkflow)
	logger.Info("Registered media workflows")

	// Register media processing activity
	temporalWorker.RegisterActivity(executor.IndexMediaFile)
	logger.Info("Registered media processing activity")

	// Start the worker
	err = temporalWorker.Start()
	if err != nil {
		logger.Fatal("Failed to start Temporal worker", zap.Error(err))
	}

	logger.Info("Worker Media started successfully",
		zap.String("task_queue", cfg.Temporal.TaskQueue),
	)

	// Wait for interrupt signal
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	<-sigCh

	logger.Info("Shutting down Worker Media...")

	// Stop the worker
	temporalWorker.Stop()

	logger.Info("Worker Media stopped")
}
