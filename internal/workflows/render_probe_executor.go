//go:build cgo

package workflows

import (
	"context"
	"fmt"
	"time"

	"github.com/oklog/ulid/v2"
	"go.uber.org/zap"

	"github.com/feral-file/ff-indexer-v2/internal/adapter"
	"github.com/feral-file/ff-indexer-v2/internal/domain"
	"github.com/feral-file/ff-indexer-v2/internal/logger"
	"github.com/feral-file/ff-indexer-v2/internal/media/probe"
	"github.com/feral-file/ff-indexer-v2/internal/providers/jobs"
	"github.com/feral-file/ff-indexer-v2/internal/store"
	"github.com/feral-file/ff-indexer-v2/internal/store/schema"
	"github.com/feral-file/ff-indexer-v2/internal/webhook"
)

// RenderProbeExecutor runs one L1 render probe for a media URL: render, classify,
// debounce, and gate viewability on confirmed failures.
//
//go:generate mockgen -source=render_probe_executor.go -destination=../mocks/render_probe_executor.go -package=mocks -mock_names=RenderProbeExecutor=MockRenderProbeExecutor
type RenderProbeExecutor interface {
	// ExecuteRenderProbe renders url and records the observation. It returns an error
	// only for infrastructure failures (store unreachable); render failures are recorded
	// state, not job errors — a URL that renders blank is a data point, and failing the
	// job would put it on the queue's retry/backoff path instead of the probe's own
	// debounce cadence.
	ExecuteRenderProbe(ctx context.Context, url string) error
}

// RenderProbeExecutorConfig holds classification thresholds and scheduling intervals.
type RenderProbeExecutorConfig struct {
	// BlankVarianceThreshold: frames with normalized luminance variance below this are blank.
	BlankVarianceThreshold float64
	// FailureGateThreshold is how many consecutive blank/stalled probes gate viewability
	// (known-bad fingerprint matches gate immediately, ignoring this).
	FailureGateThreshold int
	// RecheckInterval schedules the next probe after rendered_ok.
	RecheckInterval time.Duration
	// RetryInterval schedules the next probe after a not-yet-gating blank/stalled (the
	// debounce window).
	RetryInterval time.Duration
	// BrokenRecheckInterval schedules the next probe after gating; the probe is the only
	// healer of render-gated rows (L0 skips them), so this also bounds heal latency.
	BrokenRecheckInterval time.Duration
	// ImageSettleMs shortens the render settle for URLs whose every health-row signal
	// says static raster image (IsStaticImageRenderClass). Static images paint on
	// decode, so holding a browser slot through the full generative-work settle for the
	// image majority of the corpus (~60% at rollout) roughly halves total render
	// throughput for nothing. <= 0 disables the shortcut (full settle for everything).
	ImageSettleMs int
	// Fingerprints are known-bad render pHashes (directory listings, error pages,
	// placeholders); a match gates immediately.
	Fingerprints []probe.Fingerprint
}

// renderProbeExecutor implements RenderProbeExecutor.
type renderProbeExecutor struct {
	store         store.Store
	renderer      probe.Renderer
	ssrfValidator adapter.SSRFValidator // nil when SSRF protection is disabled
	jobQueue      jobs.JobQueue
	tokenQueue    string
	clock         adapter.Clock
	cfg           RenderProbeExecutorConfig
}

// NewRenderProbeExecutor creates a render probe executor.
//
// ssrfValidator may be nil (SSRF protection disabled); when set, every URL is validated
// before Navigate because chromium fetches it outside the SSRF-protected Go HTTP client.
func NewRenderProbeExecutor(
	st store.Store,
	renderer probe.Renderer,
	ssrfValidator adapter.SSRFValidator,
	jobQueue jobs.JobQueue,
	tokenQueue string,
	clock adapter.Clock,
	cfg RenderProbeExecutorConfig,
) RenderProbeExecutor {
	return &renderProbeExecutor{
		store:         st,
		renderer:      renderer,
		ssrfValidator: ssrfValidator,
		jobQueue:      jobQueue,
		tokenQueue:    tokenQueue,
		clock:         clock,
		cfg:           cfg,
	}
}

// gated reports whether a previous probe already gated viewability for this URL.
//
// Reads the durable HealthGated marker rather than re-deriving from the verdict and
// failure counter: those are overwritten by every probe, so a fingerprint gate followed
// by a stall below the debounce threshold would look ungated while the health row is
// still broken — and since L0 never re-checks render_% rows, nothing would ever heal it.
func (e *renderProbeExecutor) gated(prev *schema.MediaRenderProbe) bool {
	return prev != nil && prev.HealthGated
}

// ExecuteRenderProbe implements RenderProbeExecutor.
//
// Reason: the debounce state machine lives here, not in SQL — fingerprint gates
// immediately (unambiguous), blank/stalled gate only at FailureGateThreshold consecutive
// failures because slow WebGL under software GL and intentionally dark works produce
// false blanks on a single observation. Trade-offs: a URL gated by the probe is healed
// only by the probe (L0 excludes render_% rows), so BrokenRecheckInterval bounds how long
// a false gate can last. Constraints: baseline_phash is passed on every successful
// capture but the store upsert never overwrites an existing baseline (capture-only
// contract, feral-file#3485).
func (e *renderProbeExecutor) ExecuteRenderProbe(ctx context.Context, url string) error {
	now := e.clock.Now()

	// Load prior state first: every path below (including a policy refusal) must
	// preserve the verdict and failure counter that `gated` reads, or a URL whose gate
	// state is erased can never be recognized as needing healing later.
	prev, err := e.store.GetMediaRenderProbe(ctx, url)
	if err != nil {
		return fmt.Errorf("failed to load previous render probe: %w", err)
	}
	wasGated := e.gated(prev)

	// Chromium performs its own network I/O outside the SSRF-protected HTTP client. The
	// renderer validates every browser request, but refusing an obviously blocked URL
	// here avoids launching a browser context at all.
	if e.ssrfValidator != nil {
		if err := e.ssrfValidator.ValidateHTTPURL(ctx, url); err != nil {
			errMsg := fmt.Sprintf("ssrf policy refused render: %v", err)
			logger.WarnCtx(ctx, "Render probe refused by SSRF policy", zap.String("url", url), zap.Error(err))
			// Record the refusal without disturbing the render verdict: a policy block
			// is not evidence about what the artwork renders, and overwriting a prior
			// gate's verdict/counter here would strand a gated URL as permanently
			// broken (a later successful render would not recognize it as gated and so
			// would never heal the health row).
			refusal := schema.MediaRenderProbe{
				MediaURL:    url,
				Verdict:     schema.RenderProbeVerdictStalled,
				LastError:   &errMsg,
				NextCheckAt: now.Add(e.cfg.BrokenRecheckInterval),
			}
			if prev != nil {
				refusal.Verdict = prev.Verdict
				refusal.ConsecutiveFailures = prev.ConsecutiveFailures
				refusal.HealthGated = prev.HealthGated
				refusal.Phash = prev.Phash
				refusal.BaselinePhash = prev.BaselinePhash
				refusal.EngineVersion = prev.EngineVersion
				refusal.Viewport = prev.Viewport
				refusal.CapturedAt = prev.CapturedAt
			}
			return e.store.UpsertMediaRenderProbe(ctx, refusal)
		}
	}

	row := schema.MediaRenderProbe{
		MediaURL: url,
	}
	if prev != nil {
		row.ConsecutiveFailures = prev.ConsecutiveFailures
		row.BaselinePhash = prev.BaselinePhash
		row.CapturedAt = prev.CapturedAt
		// Carry the gate marker forward; only a successful release clears it.
		row.HealthGated = prev.HealthGated
	}

	// Static raster images paint on decode; everything else (HTML, animation sources,
	// SVG, mixed or unknown signals) keeps the renderer's full settle. A store failure
	// here is infrastructure, same as the probe-row load above — fail the job rather
	// than guess a class.
	settleMs := 0 // renderer default
	if e.cfg.ImageSettleMs > 0 {
		staticImage, err := e.store.IsStaticImageRenderClass(ctx, url)
		if err != nil {
			return fmt.Errorf("failed to classify render class: %w", err)
		}
		if staticImage {
			settleMs = e.cfg.ImageSettleMs
		}
	}

	capture, renderErr := e.renderer.RenderProbe(ctx, url, settleMs)
	if renderErr != nil {
		// Job cancellation / worker shutdown says nothing about the artwork: leave all
		// probe state untouched so the URL stays due and the next run judges it. The job
		// error surfaces to the queue for normal retry handling.
		if ctxErr := ctx.Err(); ctxErr != nil {
			logger.InfoCtx(ctx, "Render probe canceled, leaving probe state unchanged",
				zap.String("url", url),
				zap.Error(ctxErr),
			)
			return ctxErr
		}
		// Load/timeout failure: the "stalled" verdict.
		errMsg := renderErr.Error()
		row.Verdict = schema.RenderProbeVerdictStalled
		row.LastError = &errMsg
		row.ConsecutiveFailures++
		return e.finishFailure(ctx, url, row, wasGated, now)
	}

	classification, err := probe.Classify(capture.Image, e.cfg.Fingerprints, e.cfg.BlankVarianceThreshold)
	if err != nil {
		return fmt.Errorf("failed to classify render capture: %w", err)
	}

	phashValue := int64(classification.Phash) // #nosec G115 -- deliberate bit-pattern reinterpretation for BIGINT storage
	row.Phash = &phashValue
	if row.BaselinePhash == nil {
		row.BaselinePhash = &phashValue
	}
	row.EngineVersion = &capture.EngineVersion
	row.Viewport = &capture.Viewport
	capturedAt := now
	row.CapturedAt = &capturedAt
	row.Verdict = classification.Verdict

	switch classification.Verdict {
	case schema.RenderProbeVerdictRenderedOK:
		row.ConsecutiveFailures = 0
		row.NextCheckAt = now.Add(e.cfg.RecheckInterval)
		if wasGated {
			// The URL renders again: release the gate. Deliberately released to
			// "unknown", not "healthy" — a successful screenshot says the page painted,
			// it says nothing about the bytes L0 validates (content type, container
			// integrity). Clearing the render_% reason hands the row back to the L0
			// sweep, which marks it healthy on its next pass and only then makes the
			// token viewable again. Costs one sweep cycle of recovery latency in
			// exchange for never restoring viewability on a screenshot alone.
			//
			// The health rows and the probe marker are cleared in one transaction: as
			// two writes, a token indexed in between would inherit a gate the release
			// then skips, leaving it broken with no probe that recognizes it as gated.
			// A failure leaves the marker set, so the next probe retries the release.
			tokenIDs, err := e.store.ReleaseRenderGate(ctx, row)
			if err != nil {
				return fmt.Errorf("failed to release render gate: %w", err)
			}
			return e.propagateViewability(ctx, tokenIDs)
		}
		if err := e.store.UpsertMediaRenderProbe(ctx, row); err != nil {
			return fmt.Errorf("failed to upsert render probe: %w", err)
		}
		return nil

	case schema.RenderProbeVerdictKnownBadFingerprint:
		errMsg := fmt.Sprintf("render matched known-bad fingerprint %q", classification.MatchedLabel)
		row.LastError = &errMsg
		row.ConsecutiveFailures++
		row.NextCheckAt = now.Add(e.cfg.BrokenRecheckInterval)
		// Unambiguous: gate immediately regardless of debounce state. The marker and the
		// health rows are written in one locked transaction so a token indexed in between
		// cannot land ungated.
		tokenIDs, err := e.store.AcquireRenderGate(ctx, row, store.MediaHealthUpdate{
			Status:           schema.MediaHealthStatusBroken,
			LastError:        &errMsg,
			FailureReason:    strPtr(schema.RenderFailureKnownBad),
			RenderProbeWrite: true,
		})
		if err != nil {
			return fmt.Errorf("failed to acquire render gate: %w", err)
		}
		return e.propagateViewability(ctx, tokenIDs)

	default: // blank
		errMsg := fmt.Sprintf("blank frame (variance %.6f below threshold %.6f)",
			classification.Variance, e.cfg.BlankVarianceThreshold)
		row.LastError = &errMsg
		row.ConsecutiveFailures++
		return e.finishFailure(ctx, url, row, wasGated, now)
	}
}

// finishFailure applies the debounce policy for blank/stalled outcomes: record below the
// threshold, gate at it. wasGated keeps an already-gated row gated (and re-broadcasts
// nothing — the health row is already broken, so the update is idempotent).
func (e *renderProbeExecutor) finishFailure(ctx context.Context, url string, row schema.MediaRenderProbe, wasGated bool, now time.Time) error {
	// An already-gated URL stays gated: a further failure never releases it, and the
	// marker must survive verdict changes (a fingerprint gate followed by a stall below
	// the threshold) or the health row could never be healed.
	gate := wasGated || row.ConsecutiveFailures >= e.cfg.FailureGateThreshold
	if gate {
		row.NextCheckAt = now.Add(e.cfg.BrokenRecheckInterval)
		row.HealthGated = true
	} else {
		row.NextCheckAt = now.Add(e.cfg.RetryInterval)
	}

	if !gate {
		if err := e.store.UpsertMediaRenderProbe(ctx, row); err != nil {
			return fmt.Errorf("failed to upsert render probe: %w", err)
		}
		logger.InfoCtx(ctx, "Render probe failure recorded below gate threshold",
			zap.String("url", url),
			zap.String("verdict", row.Verdict.String()),
			zap.Int("consecutive_failures", row.ConsecutiveFailures),
			zap.Int("gate_threshold", e.cfg.FailureGateThreshold),
		)
		return nil
	}

	reason := schema.RenderFailureBlank
	if row.Verdict == schema.RenderProbeVerdictStalled {
		reason = schema.RenderFailureStalled
	}
	// Idempotent for an already-gated row: it refreshes the reason to match the newest
	// verdict without changing whether the URL is gated. Marker and health rows go in one
	// locked transaction so concurrent indexing cannot slip an ungated row in between.
	tokenIDs, err := e.store.AcquireRenderGate(ctx, row, store.MediaHealthUpdate{
		Status:           schema.MediaHealthStatusBroken,
		LastError:        row.LastError,
		FailureReason:    &reason,
		RenderProbeWrite: true,
	})
	if err != nil {
		return fmt.Errorf("failed to acquire render gate: %w", err)
	}
	return e.propagateViewability(ctx, tokenIDs)
}

// reconcileRetryDelay is how long a probe job waits before retrying after a
// post-gate/release viewability reconciliation failure. Short on purpose: until the
// retry lands, tokens.is_viewable disagrees with a durable gate or release, and nothing
// else corrects it (see propagateViewability). The cost of a retry is one re-render.
const reconcileRetryDelay = time.Minute

// propagateViewability recomputes viewability for the given tokens and emits a webhook
// for each one whose visibility actually changed, matching the sweeper's behavior.
//
// Failures return jobs.ErrReschedule rather than a plain error, because by the time this
// runs the gate or release is already durable and the probe's own cadence will not save
// us: a plain error is MarkJobFailed — permanent, no queue retry — and the probe row's
// next_check_at is already pushed a full interval out (24h gated, 168h released), so
// tokens.is_viewable would disagree with browser-confirmed health state for that entire
// window, served through default API results. An earlier version logged and continued on
// the theory that the next sweep would reconcile — wrong for exactly the case that
// matters: once a URL is gated, GetURLsForChecking excludes its render_% rows, so no
// sweep revisits them. The reschedule re-runs the whole probe (render included); the
// gate/release re-applies idempotently and reconciliation is retried. Re-rendering is
// the cheaper mistake, and a persistent store failure here means the queue itself is
// struggling — one render per reconcileRetryDelay per stuck URL is bounded load.
func (e *renderProbeExecutor) propagateViewability(ctx context.Context, tokenIDs []uint64) error {
	if len(tokenIDs) == 0 {
		return nil
	}

	changes, err := e.store.BatchUpdateTokensViewability(ctx, tokenIDs)
	if err != nil {
		logger.ErrorCtx(ctx, err, zap.Uint64s("token_ids", tokenIDs))
		// Both causes are wrapped: errors.As finds the RescheduleError (checked before
		// context.Canceled in the worker's mapping, so a store error that wraps a
		// canceled context still reschedules), and the store failure stays inspectable.
		return fmt.Errorf("viewability reconciliation failed after durable gate state (%w); rescheduling: %w",
			err, jobs.ErrReschedule(e.clock.Now().Add(reconcileRetryDelay)))
	}

	for _, change := range changes {
		logger.InfoCtx(ctx, "Render probe changed token viewability",
			zap.String("token_cid", change.TokenCID),
			zap.Bool("was_viewable", change.OldViewable),
			zap.Bool("is_viewable", change.NewViewable),
		)
		e.enqueueViewabilityWebhook(ctx, change.TokenCID, change.NewViewable)
	}
	return nil
}

// enqueueViewabilityWebhook mirrors the sweeper's triggerWebhook: same event shape, same
// unique key, same queue, so subscribers see one consistent viewability stream.
func (e *renderProbeExecutor) enqueueViewabilityWebhook(ctx context.Context, tokenCID string, isViewable bool) {
	parsedTokenCID := domain.TokenCID(tokenCID)
	chain, standard, contract, tokenNumber := parsedTokenCID.Parse()

	eventID := ulid.MustNewDefault(e.clock.Now()).String()
	webhookEvent := webhook.WebhookEvent{
		EventID:   eventID,
		EventType: webhook.EventTypeTokenViewabilityChanged,
		Timestamp: e.clock.Now(),
		Data: webhook.TokenViewabilityChanged{
			EventData: webhook.EventData{
				TokenCID:    tokenCID,
				Chain:       string(chain),
				Standard:    string(standard),
				Contract:    contract,
				TokenNumber: tokenNumber,
			},
			IsViewable: isViewable,
		},
	}

	uk := jobs.WebhookNotifyUniqueKey(eventID)
	if _, _, err := e.jobQueue.Enqueue(ctx, jobs.EnqueueOptions{
		Queue:     e.tokenQueue,
		Kind:      "NotifyWebhookClients",
		Args:      []any{webhookEvent},
		UniqueKey: &uk,
	}); err != nil {
		logger.ErrorCtx(ctx, err, zap.String("token_cid", tokenCID))
	}
}

// strPtr returns a pointer to s.
func strPtr(s string) *string {
	return &s
}
