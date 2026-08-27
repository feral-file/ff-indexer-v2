//go:build cgo

package workflows

import (
	"context"
	"errors"
	"fmt"
	"strings"
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
	// FailureGateThreshold is how many consecutive blank probes gate viewability
	// (known-bad fingerprint matches gate immediately, ignoring this; a stall never
	// counts — see ExecuteRenderProbe).
	FailureGateThreshold int
	// RecheckInterval schedules the next probe after rendered_ok.
	RecheckInterval time.Duration
	// RetryInterval schedules the next probe after a not-yet-gating blank (the debounce
	// window) and after a first stall (a stall under load deserves a soon retry).
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
	// ConfirmSettleMs is the settle for a confirmation probe: one whose blank would reach
	// FailureGateThreshold (the second probe at the default 2; every probe at 1) or whose
	// render would heal a gate. It replaces the class settle for those probes, and they
	// also render alone (see renderProbeExecutor.lane). <= 0 keeps the class settle.
	ConfirmSettleMs int
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
	// lane admits renders: first looks share it, a confirmation takes it alone, and a
	// render that cannot enter is rescheduled rather than parked in a worker slot.
	lane renderLane
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
// immediately (unambiguous), blank gates only at FailureGateThreshold consecutive
// failures because slow WebGL under software GL and intentionally dark works produce
// false blanks on a single observation. Outcomes that carry no evidence about the
// artwork never touch that counter: a browser that failed to launch reschedules the job
// (worker-host failure); a main document served non-2xx or an SSRF refusal is recorded
// via recordNoEvidence; and a stall (load failure or timeout) is recorded via
// recordStalled — "the prober could not finish looking" is not "the prober saw it
// broken". Trade-offs: a URL gated by the probe is healed only by the probe (L0 excludes
// render_% rows), so BrokenRecheckInterval bounds how long a false gate can last; a page
// that hangs for every viewer is no longer gated by L1 at all, the same conservative
// contract L0 lives by (a false broken hides real art). Constraints: baseline_phash is
// passed on every successful capture but the store upsert never overwrites an existing
// baseline (capture-only contract, feral-file#3485).
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
	confirming := e.confirming(prev, wasGated)

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
			interval := e.noEvidenceInterval(wasGated)
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

	settleMs, err := e.settleFor(ctx, url, confirming)
	if err != nil {
		return err
	}

	capture, renderErr := e.render(ctx, url, settleMs, confirming, now)
	if renderErr != nil {
		// The lane is busy: nothing rendered, nothing is known, no state is written. The
		// job comes back after laneBusyRetryDelay and the slot runs other work meanwhile.
		if errors.Is(renderErr, errRenderLaneBusy) {
			logger.InfoCtx(ctx, "Render lane busy, rescheduling probe",
				zap.String("url", url),
				zap.Bool("confirming", confirming),
			)
			return fmt.Errorf("%w; rescheduling: %w", renderErr, jobs.ErrReschedule(now.Add(laneBusyRetryDelay)))
		}
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
		// Load/timeout failure: the "stalled" verdict, recorded as telemetry, never as
		// evidence. Measured 2026-08-25 by re-probing 40 production would-gate stalls on
		// unloaded hardware in production's own configuration: 29 rendered (20 inside
		// the OLD 45s budget), and the 28 that rendered in every configuration carried
		// counters of 9–13 — the debounce had re-measured the loaded prober, not the
		// page, nine times over.
		logger.WarnCtx(ctx, "Render probe stalled, recorded without evidence",
			zap.String("url", url),
			zap.Error(renderErr),
		)
		return e.recordStalled(ctx, url, renderErr.Error(), prev, wasGated, now)
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
			prev, now, e.noEvidenceInterval(wasGated))
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
		// The counter is blank debounce state and a fingerprint match is not a blank
		// observation, so it resets rather than increments. Incrementing
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

// finishFailure applies the debounce policy for a blank outcome: record below the
// threshold, gate at it. wasGated keeps an already-gated row gated (and re-broadcasts
// nothing — the health row is already broken, so the update is idempotent).
func (e *renderProbeExecutor) finishFailure(ctx context.Context, url string, row schema.MediaRenderProbe, wasGated bool, now time.Time) error {
	// An already-gated URL stays gated: a further failure never releases it, and the
	// marker must survive verdict changes (a fingerprint gate followed by a blank below
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

	// Only blank reaches here now; RenderFailureStalled stays defined for rows gated
	// before stalls stopped counting, until their healing render clears them.
	reason := schema.RenderFailureBlank
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

// noEvidenceInterval picks the recheck cadence for a no-evidence outcome by what the
// row's gate state makes of it. Ungated: NoEvidenceRecheckInterval — the condition is
// durable (a fingerprint-keyed gateway block) and a faster cadence only re-confirms it.
// Gated: BrokenRecheckInterval — the probe is a gated row's ONLY healer, and that
// interval is the documented bound on heal latency; stretching a gated row's recheck to
// the slow no-evidence cadence would keep recovered artwork hidden for up to a week
// after a transient gateway failure (#138 bot F2).
func (e *renderProbeExecutor) noEvidenceInterval(wasGated bool) time.Duration {
	if wasGated {
		return e.cfg.BrokenRecheckInterval
	}
	return e.cfg.NoEvidenceRecheckInterval
}

// confirming reports whether this probe is the look whose outcome changes gate state: a
// blank now would reach FailureGateThreshold, or a render now would heal a held gate.
//
// Reason: derived from the threshold, not from "counter > 0" (the first cut), because
// failure_gate_threshold accepts 1 — under which a never-probed URL's first blank gates,
// and keying on the counter would have run that deciding look on the shared lane at the
// first-look settle (#142 bot round 2). Constraints: at a threshold above 2 the looks
// between the first and the deciding one stay ordinary first looks by design; only the
// look that can gate needs the confirmation conditions.
func (e *renderProbeExecutor) confirming(prev *schema.MediaRenderProbe, wasGated bool) bool {
	if wasGated {
		return true
	}
	failures := 0
	if prev != nil {
		failures = prev.ConsecutiveFailures
	}
	return failures+1 >= e.cfg.FailureGateThreshold
}

// settleFor picks the settle window for one probe.
//
// Reason: a confirmation probe takes ConfirmSettleMs regardless of render class — the
// confirming look is only evidence if it can disagree with the first look, and one that
// re-runs the first look's window under the first look's load cannot (31/40 production
// would-gate blanks re-probed on unloaded hardware rendered fine, every one at counter
// 4). Otherwise static raster images paint on decode and take ImageSettleMs; everything
// else (HTML, animation sources, SVG, mixed or unknown signals) keeps the renderer's
// default. Constraints: a class-lookup store failure is infrastructure, same as the
// probe-row load — fail the job rather than guess a class.
func (e *renderProbeExecutor) settleFor(ctx context.Context, url string, confirming bool) (int, error) {
	if confirming && e.cfg.ConfirmSettleMs > 0 {
		return e.cfg.ConfirmSettleMs, nil
	}
	if e.cfg.ImageSettleMs <= 0 {
		return 0, nil // renderer default
	}
	staticImage, err := e.store.IsStaticImageRenderClass(ctx, url)
	if err != nil {
		return 0, fmt.Errorf("failed to classify render class: %w", err)
	}
	if staticImage {
		return e.cfg.ImageSettleMs, nil
	}
	return 0, nil
}

// errRenderLaneBusy reports that the render lane turned the probe away; the caller
// reschedules the job with all probe state untouched.
var errRenderLaneBusy = errors.New("render lane busy")

// render runs the browser under the render lane: first looks share the lane, a
// confirmation holds it exclusively and so renders with no other probe from this worker
// in flight. A render the lane cannot admit returns errRenderLaneBusy without rendering.
//
// Reason: the threshold-2 debounce failed in production not because two looks are too
// few but because the second look ran under the same conditions as the first. prod-01
// is a shared 4-vCPU droplet rendering under software GL with media_worker.concurrency
// 4, so a confirming probe rendered beside three others exactly like the first one did
// and reproduced the CPU-starved blank every time. Trade-offs: a turned-away probe costs
// a queue round-trip (laneBusyRetryDelay) instead of holding its worker slot, so media
// indexing on the shared pool keeps running however many confirmations are due — see
// renderLane for why blocking was rejected. Constraints: the lane is per executor, hence
// per worker process; it cannot see other processes' renders.
func (e *renderProbeExecutor) render(ctx context.Context, url string, settleMs int, confirming bool, now time.Time) (*probe.Capture, error) {
	if !e.lane.tryEnter(confirming, now) {
		return nil, errRenderLaneBusy
	}
	defer e.lane.leave(confirming)
	return e.renderer.RenderProbe(ctx, url, settleMs)
}

// carriedRow builds the probe row for an attempt that produced no evidence about the
// artwork: the attempt (error, next check) is new; verdict, failure counter, gate
// marker, and the last successful capture's metadata are carried from the previous row.
//
// Reason: overwriting a prior gate's verdict/counter would strand a gated URL as
// permanently broken — a later successful render would not recognize it as gated and so
// would never heal the health row. First-ever attempts get a stalled verdict (the
// closest existing category; a new enum value is not worth a migration) with the
// counter still at zero, so no-evidence outcomes can never accumulate toward the gate
// threshold.
func carriedRow(url, errMsg string, prev *schema.MediaRenderProbe, nextCheckAt time.Time) schema.MediaRenderProbe {
	row := schema.MediaRenderProbe{
		MediaURL:    url,
		Verdict:     schema.RenderProbeVerdictStalled,
		LastError:   &errMsg,
		NextCheckAt: nextCheckAt,
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
	return row
}

// recordNoEvidence persists a probe attempt that produced no evidence about the artwork
// (SSRF policy refusal, or a main document served with a non-2xx status) — carriedRow's
// contract, at the caller's cadence. Constraints: the caller picks the recheck interval
// because it knows the condition's durability — noEvidenceInterval for served error
// pages and policy refusals (slow for ungated rows, heal-cadence for gated ones),
// RetryInterval for transient resolver failures where a slow cadence would cost healthy
// URLs a week of L1 coverage.
func (e *renderProbeExecutor) recordNoEvidence(ctx context.Context, url, errMsg string, prev *schema.MediaRenderProbe, now time.Time, recheckIn time.Duration) error {
	if err := e.store.UpsertMediaRenderProbe(ctx, carriedRow(url, errMsg, prev, now.Add(recheckIn))); err != nil {
		return fmt.Errorf("failed to upsert render probe: %w", err)
	}
	return nil
}

// stallErrorPrefix marks a last_error written by an actual render stall, as opposed to
// the no-evidence attempts that also wear the stalled label on first-ever rows.
const stallErrorPrefix = "render stalled: "

// wasRenderStall reports whether the previous attempt was an actual render stall.
//
// Reason: the verdict cannot say — carriedRow gives a first-ever no-evidence attempt
// (served 410, SSRF refusal) the stalled label, and a gated row keeps its gate's verdict
// through a stall (recordStalled) — and treating either as a prior stall, or missing
// one, would send a URL's FIRST real timeout straight to the week-long cadence or never
// back a persistent one off (#142 bot round 4). The discriminator lives in last_error
// because a new enum value would cost a migration for one cadence decision. Pre-#142
// stall rows lack the prefix and so read as no prior stall: they get one RetryInterval
// look before backing off.
func wasRenderStall(prev *schema.MediaRenderProbe) bool {
	return prev != nil && prev.LastError != nil && strings.HasPrefix(*prev.LastError, stallErrorPrefix)
}

// recordStalled persists a load failure or timeout as the stalled verdict (on an ungated
// row) under the no-evidence contract: the label and error are recorded for telemetry,
// but the failure counter, gate marker, and last capture are carried forward untouched,
// so a stall can neither gate nor march a URL toward the threshold.
//
// Reason: a timeout is a fact about the prober's budget and load, not about the page —
// see ExecuteRenderProbe. Cadence: a first stall retries on RetryInterval, because a
// stall under load is the transient case worth a soon second look; a stall following an
// actual stall (wasRenderStall) is treated as durable and moves to noEvidenceInterval,
// or ~1,300 persistently stalling URLs on an hourly retry would out-spend the whole
// daily render budget and starve coverage of never-probed URLs. Trade-offs: a page that
// recovers after two stalls waits the slow cadence for L1 to notice — it is not hidden
// meanwhile, since stalls never gate.
func (e *renderProbeExecutor) recordStalled(ctx context.Context, url, errMsg string, prev *schema.MediaRenderProbe, wasGated bool, now time.Time) error {
	interval := e.cfg.RetryInterval
	if wasRenderStall(prev) {
		interval = e.noEvidenceInterval(wasGated)
	}
	row := carriedRow(url, stallErrorPrefix+errMsg, prev, now.Add(interval))
	// A gated row keeps the verdict that acquired its gate: a token that later inherits
	// the gate with no sibling health row derives its failure reason from that verdict
	// (store.activeRenderGate), and a stall must not relabel a blank or fingerprint gate
	// as render_stalled — a reason the probe no longer issues (#142 bot round 6). The
	// stall itself is on the row in last_error, prefixed.
	if !row.HealthGated {
		row.Verdict = schema.RenderProbeVerdictStalled
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
