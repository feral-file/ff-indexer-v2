//go:build cgo

package workflows

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/oklog/ulid/v2"
	"go.uber.org/zap"

	"github.com/feral-file/ff-indexer-v2/internal/adapter"
	"github.com/feral-file/ff-indexer-v2/internal/domain"
	"github.com/feral-file/ff-indexer-v2/internal/logger"
	"github.com/feral-file/ff-indexer-v2/internal/media/probe"
	"github.com/feral-file/ff-indexer-v2/internal/providers/jobs"
	"github.com/feral-file/ff-indexer-v2/internal/security/ssrf"
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
	// NoEvidenceRecheckInterval schedules the next probe after a no-evidence outcome (a
	// non-2xx served error page or an SSRF policy refusal). These outcomes never gate, so
	// this bounds nothing but wasted work: a public gateway's persistent HTTP 410 does not
	// lift on the gated cadence, and rechecking the blocked population daily starved
	// coverage of never-probed URLs. Longer than BrokenRecheckInterval on purpose.
	NoEvidenceRecheckInterval time.Duration
	// Enforce turns render verdicts into viewability gates. False is shadow mode: the
	// probe renders, classifies, debounces, and records everything exactly as
	// enforcement would — verdicts, counters, pHashes — but never writes a gate, so
	// nothing is ever hidden. The rollout contract: watch media_render_probes for a
	// while, hand-verify a sample of would-be-gated URLs, and only then flip this on.
	// The counters being identical in both modes is what makes the shadow data an
	// honest preview of enforcement rather than an approximation.
	Enforce bool
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
// false blanks on a single observation. Outcomes that carry no evidence about the
// artwork never touch that counter: a browser that failed to launch reschedules the job
// (worker-host failure), and a main document served non-2xx or an SSRF refusal is
// recorded via recordNoEvidence. Trade-offs: a URL gated by the probe is healed
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
			// A resolver failure is transient infrastructure, not a policy verdict —
			// the ssrf package keeps ErrResolutionFailed distinct from ErrBlocked for
			// exactly this attribution. Both paths preserve all probe state; only the
			// recheck cadence differs: a policy block is durable (slow reprobe), a DNS
			// blip is not (an L0-healthy URL must not lose a week of L1 coverage to it).
			interval := e.cfg.NoEvidenceRecheckInterval
			if errors.Is(err, ssrf.ErrResolutionFailed) {
				interval = e.cfg.RetryInterval
			}
			logger.WarnCtx(ctx, "Render probe refused by SSRF policy", zap.String("url", url), zap.Error(err))
			return e.recordNoEvidence(ctx, url, fmt.Sprintf("ssrf policy refused render: %v", err), prev, now, interval)
		}
	}

	row := schema.MediaRenderProbe{
		MediaURL: url,
	}
	if prev != nil {
		row.ConsecutiveFailures = prev.ConsecutiveFailures
		row.BaselinePhash = prev.BaselinePhash
		// The last successful capture is carried forward whole: pHash, engine, viewport,
		// and timestamp describe one observation and must survive or vanish together. A
		// failure path that kept the timestamp but nulled the rest (an earlier revision
		// did) claims a capture happened while deleting its comparability data — the
		// capture-only record #3485 exists to accumulate. A successful classification
		// below overwrites all four with the fresh capture.
		row.Phash = prev.Phash
		row.EngineVersion = prev.EngineVersion
		row.Viewport = prev.Viewport
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
		// A browser that never started is a worker-host failure, not render evidence:
		// leave all probe state untouched and retry the job after a short delay.
		// Recording it as stalled would let a host incident march healthy URLs to the
		// gate threshold — the 2026-08-17 fork-exhaustion incident drove ~2,100 URLs
		// to would-gate counters exactly that way.
		if errors.Is(renderErr, probe.ErrBrowserUnavailable) {
			logger.WarnCtx(ctx, "Render probe browser unavailable, rescheduling job",
				zap.String("url", url),
				zap.Error(renderErr),
			)
			return fmt.Errorf("browser unavailable (%w); rescheduling: %w",
				renderErr, jobs.ErrReschedule(now.Add(browserUnavailableRetryDelay)))
		}
		// Load/timeout failure: the "stalled" verdict.
		errMsg := renderErr.Error()
		row.Verdict = schema.RenderProbeVerdictStalled
		row.LastError = &errMsg
		row.ConsecutiveFailures++
		return e.finishFailure(ctx, url, row, wasGated, now)
	}

	// A non-2xx main document means the server sent an error body instead of the
	// artwork; chromium paints it like any page, so the frame must not be classified.
	// Measured in production: ipfs.io's HTTP 410 bot-block page (one line of text on
	// white) classified 1,692 healthy artworks as blank. L0 owns byte-level judgment of
	// HTTP failures on its own cadence — L1 records the attempt and moves on.
	if capture.MainStatus != 0 && (capture.MainStatus < 200 || capture.MainStatus >= 300) {
		logger.WarnCtx(ctx, "Render probe main document returned non-2xx, frame not classified",
			zap.String("url", url),
			zap.Int("main_status", capture.MainStatus),
		)
		return e.recordNoEvidence(ctx, url,
			fmt.Sprintf("main document returned HTTP %d; frame not classified", capture.MainStatus),
			prev, now, e.cfg.NoEvidenceRecheckInterval)
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
		// The counter is blank/stalled debounce state and a fingerprint match is not a
		// blank/stalled observation, so it resets rather than increments. Incrementing
		// bypassed the debounce after a rollback: release-on-disable preserves the probe
		// row, so a retained fingerprint count of 1 plus one transient blank after
		// re-enable reached the threshold and gated healthy media on a single
		// observation.
		row.ConsecutiveFailures = 0
		row.NextCheckAt = now.Add(e.cfg.BrokenRecheckInterval)
		// Shadow mode: record the verdict on the enforcement cadence and log what would
		// have happened, but never gate. Any stale marker is preserved untouched on the
		// row — clearing health rows is the release transaction's job, and the sweeper
		// runs releases while enforcement is off.
		if !e.cfg.Enforce {
			logger.InfoCtx(ctx, "Shadow mode: fingerprint match would gate viewability",
				zap.String("url", url),
				zap.String("fingerprint", classification.MatchedLabel),
			)
			if err := e.store.UpsertMediaRenderProbe(ctx, row); err != nil {
				return fmt.Errorf("failed to upsert render probe: %w", err)
			}
			return nil
		}
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
		// Shadow mode: the row records that the threshold was reached (counter, verdict,
		// enforcement cadence) but the gate is not taken and the marker is not set —
		// carrying a stale marker forward unchanged is fine, setting a new one here
		// would be hiding. The log line is the operator's watch signal.
		if !e.cfg.Enforce {
			logger.InfoCtx(ctx, "Shadow mode: failure threshold reached, would gate viewability",
				zap.String("url", url),
				zap.String("verdict", row.Verdict.String()),
				zap.Int("consecutive_failures", row.ConsecutiveFailures),
			)
			if err := e.store.UpsertMediaRenderProbe(ctx, row); err != nil {
				return fmt.Errorf("failed to upsert render probe: %w", err)
			}
			return nil
		}
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

// browserUnavailableRetryDelay is how long a probe job waits before retrying after the
// browser failed to launch. Long enough that a host under fork/memory pressure is not
// hammered by immediate retries, short enough that a transient blip costs one delay.
const browserUnavailableRetryDelay = 5 * time.Minute

// recordNoEvidence persists a probe attempt that produced no evidence about the artwork
// (SSRF policy refusal, or a main document served with a non-2xx status): the attempt
// and its reason are recorded, but verdict, failure counter, gate marker, and the last
// successful capture's metadata are preserved from the previous row.
//
// Reason: overwriting a prior gate's verdict/counter here would strand a gated URL as
// permanently broken — a later successful render would not recognize it as gated and so
// would never heal the health row. First-ever attempts record a stalled verdict (the
// closest existing category; a new enum value is not worth a migration) with the counter
// still at zero, so no-evidence outcomes can never accumulate toward the gate threshold.
// Constraints: the caller picks the recheck interval because it knows the condition's
// durability — NoEvidenceRecheckInterval for persistent conditions (ipfs.io's bot-block
// of headless browsers, policy refusals) where a faster cadence only burns render
// capacity re-confirming the same outcome, RetryInterval for transient ones (resolver
// failures) where a slow cadence would cost healthy URLs a week of L1 coverage.
func (e *renderProbeExecutor) recordNoEvidence(ctx context.Context, url, errMsg string, prev *schema.MediaRenderProbe, now time.Time, recheckIn time.Duration) error {
	row := schema.MediaRenderProbe{
		MediaURL:    url,
		Verdict:     schema.RenderProbeVerdictStalled,
		LastError:   &errMsg,
		NextCheckAt: now.Add(recheckIn),
	}
	if prev != nil {
		row.Verdict = prev.Verdict
		row.ConsecutiveFailures = prev.ConsecutiveFailures
		row.HealthGated = prev.HealthGated
		row.Phash = prev.Phash
		row.BaselinePhash = prev.BaselinePhash
		row.EngineVersion = prev.EngineVersion
		row.Viewport = prev.Viewport
		row.CapturedAt = prev.CapturedAt
	}
	if err := e.store.UpsertMediaRenderProbe(ctx, row); err != nil {
		return fmt.Errorf("failed to upsert render probe: %w", err)
	}
	return nil
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
