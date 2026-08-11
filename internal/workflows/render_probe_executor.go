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

	capture, renderErr := e.renderer.RenderProbe(ctx, url)
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
			// Ordering matters: release health first and only persist the cleared marker
			// afterwards. Writing rendered_ok first would clear the marker even when the
			// release fails, stranding a broken health row that L0 never re-checks.
			if err := e.setHealthAndPropagate(ctx, url, store.MediaHealthUpdate{
				Status:           schema.MediaHealthStatusUnknown,
				RenderProbeWrite: true,
			}); err != nil {
				// Keep the marker set so the next probe retries the release.
				row.HealthGated = true
				if upsertErr := e.store.UpsertMediaRenderProbe(ctx, row); upsertErr != nil {
					logger.ErrorCtx(ctx, upsertErr, zap.String("url", url))
				}
				return err
			}
			row.HealthGated = false
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
		row.HealthGated = true
		if err := e.store.UpsertMediaRenderProbe(ctx, row); err != nil {
			return fmt.Errorf("failed to upsert render probe: %w", err)
		}
		// Unambiguous: gate immediately regardless of debounce state.
		return e.setHealthAndPropagate(ctx, url, store.MediaHealthUpdate{
			Status:           schema.MediaHealthStatusBroken,
			LastError:        &errMsg,
			FailureReason:    strPtr(schema.RenderFailureKnownBad),
			RenderProbeWrite: true,
		})

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

	if err := e.store.UpsertMediaRenderProbe(ctx, row); err != nil {
		return fmt.Errorf("failed to upsert render probe: %w", err)
	}

	if !gate {
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
	// verdict without changing whether the URL is gated.
	return e.setHealthAndPropagate(ctx, url, store.MediaHealthUpdate{
		Status:           schema.MediaHealthStatusBroken,
		LastError:        row.LastError,
		FailureReason:    &reason,
		RenderProbeWrite: true,
	})
}

// setHealthAndPropagate writes the URL-wide health verdict, recomputes viewability for
// every affected token, and enqueues viewability webhooks for tokens that changed —
// mirroring the media health sweeper's flushViewabilityUpdates/triggerWebhook flow so
// downstream consumers cannot tell which probe layer produced the change.
func (e *renderProbeExecutor) setHealthAndPropagate(ctx context.Context, url string, update store.MediaHealthUpdate) error {
	if err := e.store.UpdateTokenMediaHealthByURL(ctx, url, update); err != nil {
		return fmt.Errorf("failed to update media health: %w", err)
	}

	tokenIDs, err := e.store.GetTokenIDsByMediaURL(ctx, url)
	if err != nil {
		return fmt.Errorf("failed to get token IDs for URL: %w", err)
	}
	if len(tokenIDs) == 0 {
		return nil
	}

	changes, err := e.store.BatchUpdateTokensViewability(ctx, tokenIDs)
	if err != nil {
		return fmt.Errorf("failed to update tokens viewability: %w", err)
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
