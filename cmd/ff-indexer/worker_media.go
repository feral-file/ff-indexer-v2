//go:build cgo

package main

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/chromedp/chromedp"
	"gorm.io/gorm"

	"github.com/feral-file/ff-indexer-v2/internal/adapter"
	"github.com/feral-file/ff-indexer-v2/internal/config"
	"github.com/feral-file/ff-indexer-v2/internal/downloader"
	"github.com/feral-file/ff-indexer-v2/internal/logger"
	"github.com/feral-file/ff-indexer-v2/internal/media/probe"
	"github.com/feral-file/ff-indexer-v2/internal/media/processor"
	"github.com/feral-file/ff-indexer-v2/internal/media/rasterizer"
	"github.com/feral-file/ff-indexer-v2/internal/media/transformer"
	"github.com/feral-file/ff-indexer-v2/internal/providers/cloudflare"
	"github.com/feral-file/ff-indexer-v2/internal/providers/jobs"
	"github.com/feral-file/ff-indexer-v2/internal/security/ssrf"
	"github.com/feral-file/ff-indexer-v2/internal/store"
	"github.com/feral-file/ff-indexer-v2/internal/uri"
	"github.com/feral-file/ff-indexer-v2/internal/workflows"
)

// probeAllocatorOptions picks sandboxed or unsandboxed chromium flags for the render
// probe, warning loudly when the sandbox is off since that is a real loss of isolation
// for untrusted artwork.
func probeAllocatorOptions(noSandbox bool) []chromedp.ExecAllocatorOption {
	if noSandbox {
		logger.Warn("Render probe chromium sandbox is DISABLED (render_probe.no_sandbox=true); untrusted artwork runs with reduced isolation")
		return probe.AllocatorOptionsNoSandbox()
	}
	return probe.AllocatorOptions()
}

// ssrfValidatorOrNil converts a possibly-nil *ssrf.Validator into an interface value
// that is untyped-nil when protection is disabled, so downstream `!= nil` checks behave
// (a typed-nil interface would pass the check and panic on first use).
func ssrfValidatorOrNil(v *ssrf.Validator) adapter.SSRFValidator {
	if v == nil {
		return nil
	}
	return v
}

// registerWorkerMedia wires the media-indexing jobs.Worker (worker-media / media_index queue).
func registerWorkerMedia(
	ctx context.Context,
	wcfg *config.WorkerMediaConfig,
	db *gorm.DB,
) (run func(context.Context) error, cleanup func(context.Context) error, err error) {
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

	ssrfValidator, err := config.SSRFValidatorFromProtection(wcfg.Security.SSRFProtection)
	if err != nil {
		return nil, nil, fmt.Errorf("SSRF security configuration: %w", err)
	}
	httpClient := adapter.NewHTTPClientWithSSRF(15*time.Second, ssrfValidator, wcfg.Security.SSRFProtection.MaxRedirects)
	mediaDownloaderHTTPClient := adapter.NewHTTPClientWithSSRF(15*time.Minute, ssrfValidator, wcfg.Security.SSRFProtection.MaxRedirects)

	uriResolverConfig := &uri.Config{
		IPFSGateways:        wcfg.URI.IPFSGateways,
		ArweaveGateways:     wcfg.URI.ArweaveGateways,
		OnChFSGateways:      wcfg.URI.OnchfsGateways,
		ProbeMaxBytes:       wcfg.URI.ProbeMaxBytes,
		KnownBadPageMarkers: wcfg.URI.KnownBadPageMarkers,
	}
	uriResolver := uri.NewResolver(httpClient, ioAdapter, uriResolverConfig)

	// Cloudflare media + download path; rasterizer and image transform pipeline.
	cfClient, err := adapter.NewCloudflareClient(wcfg.Cloudflare.APIToken)
	if err != nil {
		return nil, nil, fmt.Errorf("cloudflare client: %w", err)
	}

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

	mediaProcessor := processor.NewProcessor(httpClient, uriResolver, dataURIChecker, mediaProvider, dataStore, svgRasterizer, fileSystem, ioAdapter, jsonAdapter, mediaDownloader, imageTransformer, wcfg.MaxImageSize, wcfg.MaxVideoSize, wcfg.VideoProcessingEnabled)

	mediaExecutor := workflows.NewMediaExecutor(dataStore, mediaProcessor)
	jobQueue := jobs.NewJobQueue(dataStore, jsonAdapter)

	// L1 render probe: optional, chromium-backed. A nil executor makes RenderMediaProbe
	// jobs no-ops so a disable does not strand already-enqueued jobs.
	var renderProbeExecutor workflows.RenderProbeExecutor
	var probeRenderer probe.Renderer
	if wcfg.RenderProbe.Enabled {
		fingerprints := make([]probe.Fingerprint, 0, len(wcfg.RenderProbe.KnownBadFingerprints))
		for _, fp := range wcfg.RenderProbe.KnownBadFingerprints {
			parsed, err := probe.ParseFingerprint(fp.Phash, fp.MaxDistance, fp.Label)
			if err != nil {
				return nil, nil, fmt.Errorf("render_probe.known_bad_fingerprints: %w", err)
			}
			fingerprints = append(fingerprints, parsed)
		}

		probeRenderer = probe.NewRenderer(chromedpClient, &probe.RendererConfig{
			ViewportWidth:  wcfg.RenderProbe.ViewportWidth,
			ViewportHeight: wcfg.RenderProbe.ViewportHeight,
			TimeoutMs:      wcfg.RenderProbe.TimeoutMs,
			SettleMs:       wcfg.RenderProbe.SettleMs,
			// The probe runs untrusted remote pages, so it uses its own launch flags
			// (no disable-web-security) rather than the SVG rasterizer's, and validates
			// every browser-initiated request against the SSRF policy.
			AllocatorOptions: probeAllocatorOptions(wcfg.RenderProbe.NoSandbox),
			SSRFValidator:    ssrfValidatorOrNil(ssrfValidator),
		})

		// Startup self-verification: the probe may not activate on an unproven runtime.
		// egress_restricted is an operator attestation; the metadata endpoint is the one
		// destination whose reachability falsifies it outright, so it is cross-checked
		// here UNCONDITIONALLY — the attestation is required to enable the probe
		// regardless of application-level SSRF settings, and with ssrf_protection
		// disabled the renderer runs with a nil validator (no request interception at
		// all), which makes the network-level restriction the ONLY control and this
		// check more critical, not less. An earlier revision skipped it in that case;
		// that had the logic exactly backwards. The render self-check then proves the
		// deployed image's capture path, software WebGL backend, and blank detection
		// against built-in known-good/known-bad fixtures. Either failing fails worker
		// startup: a runtime that misjudges the fixtures would not error in production —
		// it would silently misclassify artworks and gate healthy media after the
		// debounce.
		if err := probe.VerifyNoMetadataEgress(ctx); err != nil {
			return nil, nil, err
		}
		selfCheckCtx, cancelSelfCheck := context.WithTimeout(ctx, 2*time.Minute)
		err := probe.SelfCheck(selfCheckCtx, probeRenderer, wcfg.RenderProbe.BlankVarianceThreshold)
		cancelSelfCheck()
		if err != nil {
			return nil, nil, err
		}
		logger.Info("Render probe self-check passed: capture path, WebGL backend, and blank detection verified in this runtime")

		// ssrfValidator is nil when SSRF protection is disabled — the executor treats
		// nil as "no policy" (chromium bypasses the Go HTTP client, so this is the only
		// SSRF check on the render path).
		renderProbeExecutor = workflows.NewRenderProbeExecutor(
			dataStore,
			probeRenderer,
			ssrfValidatorOrNil(ssrfValidator),
			jobQueue,
			wcfg.Jobs.TokenQueue,
			adapter.NewClock(),
			workflows.RenderProbeExecutorConfig{
				BlankVarianceThreshold: wcfg.RenderProbe.BlankVarianceThreshold,
				FailureGateThreshold:   wcfg.RenderProbe.FailureGateThreshold,
				RecheckInterval:        wcfg.RenderProbe.RecheckInterval,
				RetryInterval:          wcfg.RenderProbe.RetryInterval,
				BrokenRecheckInterval:  wcfg.RenderProbe.BrokenRecheckInterval,
				Enforce:                wcfg.RenderProbe.Enforce,
				ImageSettleMs:          wcfg.RenderProbe.ImageSettleMs,
				Fingerprints:           fingerprints,
			},
		)
	}

	mediaWf := workflows.NewMediaWorkflows(mediaExecutor, renderProbeExecutor, jobQueue, workflows.MediaWorkflowsConfig{
		MediaTaskQueue: wcfg.Jobs.MediaQueue,
	})

	reg := jobs.NewRegistry(jsonAdapter)
	reg.Register("IndexMediaWorkflow", mediaWf.IndexMediaWorkflow)
	reg.Register("IndexMultipleMediaWorkflow", mediaWf.IndexMultipleMediaWorkflow)
	reg.Register("RenderMediaProbe", mediaWf.RenderMediaProbe)

	mw := wcfg.Jobs.MediaWorker
	jWorker := jobs.NewWorker(dataStore, reg, jobs.WorkerConfig{
		Queue:          wcfg.Jobs.MediaQueue,
		Concurrency:    mw.Concurrency,
		PollInterval:   mw.PollInterval,
		BatchSize:      mw.BatchSize,
		CancelInterval: mw.CancelInterval,
		MaxAttempts:    mw.MaxAttempts,
	})

	// Run until worker stops or ctx is canceled; cleanup closes transform resources.
	run = func(ctx context.Context) error {
		return jWorker.Run(ctx)
	}

	cleanup = func(ctx context.Context) error {
		_ = ctx
		errs := []error{
			imageTransformer.Close(),
			browserRasterizer.Close(),
		}
		if probeRenderer != nil {
			errs = append(errs, probeRenderer.Close())
		}
		return errors.Join(errs...)
	}

	return run, cleanup, nil
}
