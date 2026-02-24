package main

import (
	"bytes"
	"context"
	"encoding/base64"
	"flag"
	"fmt"
	"image"
	"image/color"
	"image/jpeg"
	"strings"
	"time"

	"go.temporal.io/sdk/client"
	"go.uber.org/zap/zapcore"

	"github.com/feral-file/ff-indexer-v2/internal/config"
	"github.com/feral-file/ff-indexer-v2/internal/logger"
)

var (
	configFile = flag.String("config", "", "Path to configuration file")
	envPath    = flag.String("env", "config/", "Path to environment files")
	urlFlag    = flag.String("url", "", "Media URL or data URI to index")
	urlsFlag   = flag.String("urls", "", "Comma-separated list of media URLs/data URIs")
)

func main() {
	flag.Parse()

	config.ChdirRepoRoot()
	cfg, err := config.LoadWorkerMediaConfig(*configFile, *envPath)
	if err != nil {
		panic(fmt.Sprintf("Failed to load config: %v", err))
	}

	if err := logger.Initialize(logger.Config{Debug: cfg.Debug, BreadcrumbLevel: zapcore.InfoLevel}); err != nil {
		panic(fmt.Sprintf("Failed to initialize logger: %v", err))
	}
	defer logger.Flush(2 * time.Second)

	ctx := context.Background()

	urls := parseURLs(*urlFlag, *urlsFlag)
	if len(urls) == 0 {
		urls = []string{sampleDataURI()}
	}

	clientOptions := client.Options{
		HostPort:  cfg.Temporal.HostPort,
		Namespace: cfg.Temporal.Namespace,
	}
	temporalClient, err := client.Dial(clientOptions)
	if err != nil {
		panic(fmt.Sprintf("Failed to connect to Temporal: %v", err))
	}
	defer temporalClient.Close()

	if len(urls) == 1 {
		workflowID := fmt.Sprintf("media-cli-%d", time.Now().UnixNano())
		workflowOptions := client.StartWorkflowOptions{
			ID:        workflowID,
			TaskQueue: cfg.Temporal.MediaTaskQueue,
		}

		we, err := temporalClient.ExecuteWorkflow(ctx, workflowOptions, "IndexMediaWorkflow", urls[0])
		if err != nil {
			panic(fmt.Sprintf("Failed to start workflow: %v", err))
		}

		fmt.Printf("Started workflow %s (run %s) for URL: %s\n", we.GetID(), we.GetRunID(), urls[0])
		return
	}

	workflowID := fmt.Sprintf("media-cli-batch-%d", time.Now().UnixNano())
	workflowOptions := client.StartWorkflowOptions{
		ID:        workflowID,
		TaskQueue: cfg.Temporal.MediaTaskQueue,
	}

	we, err := temporalClient.ExecuteWorkflow(ctx, workflowOptions, "IndexMultipleMediaWorkflow", urls)
	if err != nil {
		panic(fmt.Sprintf("Failed to start workflow: %v", err))
	}

	fmt.Printf("Started workflow %s (run %s) for %d URLs\n", we.GetID(), we.GetRunID(), len(urls))
}

func sampleDataURI() string {
	img := image.NewRGBA(image.Rect(0, 0, 1, 1))
	img.Set(0, 0, color.RGBA{R: 0, G: 0, B: 0, A: 255})

	var buf bytes.Buffer
	_ = jpeg.Encode(&buf, img, &jpeg.Options{Quality: 80})

	encoded := base64.StdEncoding.EncodeToString(buf.Bytes())
	return "data:image/jpeg;base64," + encoded
}

func parseURLs(single string, multiple string) []string {
	if strings.TrimSpace(multiple) != "" {
		parts := strings.Split(multiple, ",")
		urls := make([]string, 0, len(parts))
		for _, part := range parts {
			trimmed := strings.TrimSpace(part)
			if trimmed != "" {
				urls = append(urls, trimmed)
			}
		}
		return urls
	}

	if strings.TrimSpace(single) != "" {
		return []string{strings.TrimSpace(single)}
	}

	return nil
}
