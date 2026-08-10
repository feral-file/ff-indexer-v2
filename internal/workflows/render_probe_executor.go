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

// gated reports whether a previous probe row had already gated viewability: fingerprint
// verdicts gate on sight, blank/stalled gate at the debounce threshold. Derived from the
// probe row itself so no extra health-row lookup is needed.
func (e *renderProbeExecutor) gated(prev *schema.MediaRenderProbe) bool {
	if prev == nil {
		return false
	}
	switch prev.Verdict {
	case schema.RenderProbeVerdictKnownBadFingerprint:
		return true
	case schema.RenderProbeVerdictBlank, schema.RenderProbeVerdictStalled:
		return prev.ConsecutiveFailures >= e.cfg.FailureGateThreshold
	default:
		return false
	}
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

	// Chromium bypasses the SSRF-protected HTTP client entirely; refuse policy-blocked
	// URLs before Navigate. Recorded as stalled without counting toward the gate: the
	// block is policy, not evidence about what the artwork renders.
	if e.ssrfValidator != nil {
		if err := e.ssrfValidator.ValidateHTTPURL(ctx, url); err != nil {
			errMsg := fmt.Sprintf("ssrf policy refused render: %v", err)
			logger.WarnCtx(ctx, "Render probe refused by SSRF policy", zap.String("url", url), zap.Error(err))
			return e.store.UpsertMediaRenderProbe(ctx, schema.MediaRenderProbe{
				MediaURL:    url,
				Verdict:     schema.RenderProbeVerdictStalled,
				LastError:   &errMsg,
				NextCheckAt: now.Add(e.cfg.BrokenRecheckInterval),
			})
		}
	}

	prev, err := e.store.GetMediaRenderProbe(ctx, url)
	if err != nil {
		return fmt.Errorf("failed to load previous render probe: %w", err)
	}
	wasGated := e.gated(prev)

	row := schema.MediaRenderProbe{
		MediaURL: url,
	}
	if prev != nil {
		row.ConsecutiveFailures = prev.ConsecutiveFailures
		row.BaselinePhash = prev.BaselinePhash
		row.CapturedAt = prev.CapturedAt
	}

	capture, renderErr := e.renderer.RenderProbe(ctx, url)
	if renderErr != nil {
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
		if err := e.store.UpsertMediaRenderProbe(ctx, row); err != nil {
			return fmt.Errorf("failed to upsert render probe: %w", err)
		}
		if wasGated {
			// The URL renders again: heal the health row we gated. The next L0 sweep
			// re-populates observed/sniffed content types (the row's failure_reason is
			// cleared, so it re-enters the byte-level sweep).
			return e.setHealthAndPropagate(ctx, url, store.MediaHealthUpdate{
				Status: schema.MediaHealthStatusHealthy,
			})
		}
		return nil

	case schema.RenderProbeVerdictKnownBadFingerprint:
		errMsg := fmt.Sprintf("render matched known-bad fingerprint %q", classification.MatchedLabel)
		row.LastError = &errMsg
		row.ConsecutiveFailures++
		row.NextCheckAt = now.Add(e.cfg.BrokenRecheckInterval)
		if err := e.store.UpsertMediaRenderProbe(ctx, row); err != nil {
			return fmt.Errorf("failed to upsert render probe: %w", err)
		}
		// Unambiguous: gate immediately regardless of debounce state.
		return e.setHealthAndPropagate(ctx, url, store.MediaHealthUpdate{
			Status:        schema.MediaHealthStatusBroken,
			LastError:     &errMsg,
			FailureReason: strPtr(schema.RenderFailureKnownBad),
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
	gate := row.ConsecutiveFailures >= e.cfg.FailureGateThreshold
	if gate {
		row.NextCheckAt = now.Add(e.cfg.BrokenRecheckInterval)
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
	_ = wasGated // the health update below is idempotent for already-gated rows
	return e.setHealthAndPropagate(ctx, url, store.MediaHealthUpdate{
		Status:        schema.MediaHealthStatusBroken,
		LastError:     row.LastError,
		FailureReason: &reason,
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
