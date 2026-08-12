//go:build cgo

package workflows_test

import (
	"context"
	"errors"
	"image"
	"image/color"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/feral-file/ff-indexer-v2/internal/media/probe"
	"github.com/feral-file/ff-indexer-v2/internal/mocks"
	"github.com/feral-file/ff-indexer-v2/internal/providers/jobs"
	"github.com/feral-file/ff-indexer-v2/internal/store"
	"github.com/feral-file/ff-indexer-v2/internal/store/schema"
	"github.com/feral-file/ff-indexer-v2/internal/workflows"
)

// renderProbeTestConfig is the shared executor config for these tests: gate at 2
// consecutive failures, distinct intervals so next_check_at assertions can tell which
// path scheduled the row.
var renderProbeTestConfig = workflows.RenderProbeExecutorConfig{
	BlankVarianceThreshold: 0.001,
	FailureGateThreshold:   2,
	RecheckInterval:        168 * time.Hour,
	RetryInterval:          time.Hour,
	BrokenRecheckInterval:  24 * time.Hour,
}

type renderProbeMocks struct {
	store    *mocks.MockStore
	renderer *mocks.MockRenderProbeRenderer
	ssrf     *mocks.MockSSRFValidator
	jobQueue *mocks.MockJobQueue
	clock    *mocks.MockClock
	now      time.Time
}

func setupRenderProbe(t *testing.T, cfg workflows.RenderProbeExecutorConfig) (renderProbeMocks, workflows.RenderProbeExecutor) {
	ctrl := gomock.NewController(t)
	t.Cleanup(ctrl.Finish)

	m := renderProbeMocks{
		store:    mocks.NewMockStore(ctrl),
		renderer: mocks.NewMockRenderProbeRenderer(ctrl),
		ssrf:     mocks.NewMockSSRFValidator(ctrl),
		jobQueue: mocks.NewMockJobQueue(ctrl),
		clock:    mocks.NewMockClock(ctrl),
		now:      time.Date(2026, 8, 10, 12, 0, 0, 0, time.UTC),
	}
	m.clock.EXPECT().Now().Return(m.now).AnyTimes()

	exec := workflows.NewRenderProbeExecutor(m.store, m.renderer, m.ssrf, m.jobQueue, "token_index", m.clock, cfg)
	return m, exec
}

// gradientByte converts a gradient step to a color byte. The single conversion lives
// here so one suppression covers every fixture call site.
func gradientByte(v int) uint8 {
	return uint8(v) // #nosec G115 -- callers pass values bounded to 0-255 by construction
}

// contentFrame renders with variance far above the blank threshold.
func contentFrame() *probe.Capture {
	img := image.NewRGBA(image.Rect(0, 0, 64, 64))
	for y := range 64 {
		for x := range 64 {
			img.Set(x, y, color.RGBA{gradientByte(x * 4), gradientByte(y * 4), 128, 255})
		}
	}
	return &probe.Capture{Image: img, EngineVersion: "HeadlessChrome/123.0", Viewport: "1024x1024"}
}

// blankFrame is uniform black.
func blankFrame() *probe.Capture {
	img := image.NewRGBA(image.Rect(0, 0, 64, 64))
	for y := range 64 {
		for x := range 64 {
			img.Set(x, y, color.Black)
		}
	}
	return &probe.Capture{Image: img, EngineVersion: "HeadlessChrome/123.0", Viewport: "1024x1024"}
}

func TestExecuteRenderProbe_renderedOK_firstCapture(t *testing.T) {
	m, exec := setupRenderProbe(t, renderProbeTestConfig)
	url := "https://example.com/work.html"

	m.ssrf.EXPECT().ValidateHTTPURL(gomock.Any(), url).Return(nil)
	m.store.EXPECT().GetMediaRenderProbe(gomock.Any(), url).Return(nil, nil)
	m.renderer.EXPECT().RenderProbe(gomock.Any(), url, 0).Return(contentFrame(), nil)

	m.store.EXPECT().
		UpsertMediaRenderProbe(gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, row schema.MediaRenderProbe) error {
			assert.Equal(t, schema.RenderProbeVerdictRenderedOK, row.Verdict)
			assert.Equal(t, 0, row.ConsecutiveFailures)
			require.NotNil(t, row.Phash)
			require.NotNil(t, row.BaselinePhash)
			assert.Equal(t, *row.Phash, *row.BaselinePhash, "first capture becomes baseline")
			require.NotNil(t, row.EngineVersion)
			assert.Equal(t, "HeadlessChrome/123.0", *row.EngineVersion)
			assert.Equal(t, m.now.Add(renderProbeTestConfig.RecheckInterval), row.NextCheckAt)
			assert.Nil(t, row.LastError)
			return nil
		})

	// No health write: nothing was gated, nothing to heal.
	require.NoError(t, exec.ExecuteRenderProbe(context.Background(), url))
}

func TestExecuteRenderProbe_firstBlankIsDebounced(t *testing.T) {
	m, exec := setupRenderProbe(t, renderProbeTestConfig)
	url := "https://example.com/dark.html"

	m.ssrf.EXPECT().ValidateHTTPURL(gomock.Any(), url).Return(nil)
	m.store.EXPECT().GetMediaRenderProbe(gomock.Any(), url).Return(nil, nil)
	m.renderer.EXPECT().RenderProbe(gomock.Any(), url, 0).Return(blankFrame(), nil)

	m.store.EXPECT().
		UpsertMediaRenderProbe(gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, row schema.MediaRenderProbe) error {
			assert.Equal(t, schema.RenderProbeVerdictBlank, row.Verdict)
			assert.Equal(t, 1, row.ConsecutiveFailures)
			assert.Equal(t, m.now.Add(renderProbeTestConfig.RetryInterval), row.NextCheckAt, "debounce window")
			require.NotNil(t, row.Phash, "blank frames still record their phash")
			return nil
		})

	// Below the gate threshold: no health update, no viewability recompute, no webhook.
	require.NoError(t, exec.ExecuteRenderProbe(context.Background(), url))
}

func TestExecuteRenderProbe_secondBlankGatesViewability(t *testing.T) {
	m, exec := setupRenderProbe(t, renderProbeTestConfig)
	url := "https://example.com/dead.html"

	m.ssrf.EXPECT().ValidateHTTPURL(gomock.Any(), url).Return(nil)
	m.store.EXPECT().GetMediaRenderProbe(gomock.Any(), url).Return(&schema.MediaRenderProbe{
		MediaURL:            url,
		Verdict:             schema.RenderProbeVerdictBlank,
		ConsecutiveFailures: 1,
	}, nil)
	m.renderer.EXPECT().RenderProbe(gomock.Any(), url, 0).Return(blankFrame(), nil)

	// Gating is one atomic call: marker and health rows together, returning the tokens
	// to recompute. Separate writes would let a token indexed in between land ungated.
	m.store.EXPECT().
		AcquireRenderGate(gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, row schema.MediaRenderProbe, upd store.MediaHealthUpdate) ([]uint64, error) {
			assert.Equal(t, 2, row.ConsecutiveFailures)
			assert.True(t, row.HealthGated)
			assert.Equal(t, m.now.Add(renderProbeTestConfig.BrokenRecheckInterval), row.NextCheckAt)
			assert.Equal(t, schema.MediaHealthStatusBroken, upd.Status)
			require.NotNil(t, upd.FailureReason)
			assert.Equal(t, schema.RenderFailureBlank, *upd.FailureReason)
			return []uint64{7}, nil
		})
	cid := "eip155:1:erc721:0xabc:7"
	m.store.EXPECT().BatchUpdateTokensViewability(gomock.Any(), []uint64{7}).Return([]store.TokenViewabilityChange{
		{TokenID: 7, TokenCID: cid, OldViewable: true, NewViewable: false},
	}, nil)
	m.jobQueue.EXPECT().
		Enqueue(gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, opts jobs.EnqueueOptions) (*schema.Job, bool, error) {
			assert.Equal(t, "token_index", opts.Queue)
			assert.Equal(t, "NotifyWebhookClients", opts.Kind)
			return &schema.Job{ID: 1}, true, nil
		})

	require.NoError(t, exec.ExecuteRenderProbe(context.Background(), url))
}

func TestExecuteRenderProbe_stalledRenderCountsTowardGate(t *testing.T) {
	m, exec := setupRenderProbe(t, renderProbeTestConfig)
	url := "https://example.com/hangs.html"

	m.ssrf.EXPECT().ValidateHTTPURL(gomock.Any(), url).Return(nil)
	m.store.EXPECT().GetMediaRenderProbe(gomock.Any(), url).Return(&schema.MediaRenderProbe{
		MediaURL:            url,
		Verdict:             schema.RenderProbeVerdictStalled,
		ConsecutiveFailures: 1,
	}, nil)
	m.renderer.EXPECT().RenderProbe(gomock.Any(), url, 0).Return(nil, errors.New("context deadline exceeded"))

	m.store.EXPECT().
		AcquireRenderGate(gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, row schema.MediaRenderProbe, upd store.MediaHealthUpdate) ([]uint64, error) {
			assert.Equal(t, schema.RenderProbeVerdictStalled, row.Verdict)
			assert.Equal(t, 2, row.ConsecutiveFailures)
			assert.Nil(t, row.Phash, "no frame, no phash")
			require.NotNil(t, upd.FailureReason)
			assert.Equal(t, schema.RenderFailureStalled, *upd.FailureReason)
			return []uint64{9}, nil
		})
	m.store.EXPECT().BatchUpdateTokensViewability(gomock.Any(), []uint64{9}).Return(nil, nil)

	require.NoError(t, exec.ExecuteRenderProbe(context.Background(), url))
}

func TestExecuteRenderProbe_fingerprintGatesImmediately(t *testing.T) {
	// Compute the frame's own hash so it matches a configured fingerprint exactly.
	frame := contentFrame()
	cls, err := probe.Classify(frame.Image, nil, 0.001)
	require.NoError(t, err)

	cfg := renderProbeTestConfig
	cfg.Fingerprints = []probe.Fingerprint{{Hash: cls.Phash, MaxDistance: 4, Label: "kubo-dir-listing"}}
	m, exec := setupRenderProbe(t, cfg)
	url := "https://example.com/dir-cid"

	m.ssrf.EXPECT().ValidateHTTPURL(gomock.Any(), url).Return(nil)
	m.store.EXPECT().GetMediaRenderProbe(gomock.Any(), url).Return(nil, nil) // FIRST observation
	m.renderer.EXPECT().RenderProbe(gomock.Any(), url, 0).Return(frame, nil)

	// Gates on first observation — no debounce for unambiguous matches — in one atomic call.
	m.store.EXPECT().
		AcquireRenderGate(gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, row schema.MediaRenderProbe, upd store.MediaHealthUpdate) ([]uint64, error) {
			assert.Equal(t, schema.RenderProbeVerdictKnownBadFingerprint, row.Verdict)
			require.NotNil(t, row.LastError)
			assert.Contains(t, *row.LastError, "kubo-dir-listing")
			require.NotNil(t, upd.FailureReason)
			assert.Equal(t, schema.RenderFailureKnownBad, *upd.FailureReason)
			return []uint64{3}, nil
		})
	m.store.EXPECT().BatchUpdateTokensViewability(gomock.Any(), []uint64{3}).Return(nil, nil)

	require.NoError(t, exec.ExecuteRenderProbe(context.Background(), url))
}

func TestExecuteRenderProbe_renderedOKAfterGateHeals(t *testing.T) {
	m, exec := setupRenderProbe(t, renderProbeTestConfig)
	url := "https://example.com/recovered.html"

	baseline := int64(42)
	m.store.EXPECT().GetMediaRenderProbe(gomock.Any(), url).Return(&schema.MediaRenderProbe{
		MediaURL:            url,
		Verdict:             schema.RenderProbeVerdictBlank,
		ConsecutiveFailures: 2,
		HealthGated:         true, // durable marker, not re-derived from the counter
		BaselinePhash:       &baseline,
	}, nil)
	m.ssrf.EXPECT().ValidateHTTPURL(gomock.Any(), url).Return(nil)
	m.renderer.EXPECT().RenderProbe(gomock.Any(), url, 0).Return(contentFrame(), nil)

	// One atomic release: health rows and the probe marker clear together, and the
	// affected tokens come back for the viewability recompute. A separate
	// UpsertMediaRenderProbe here would reintroduce the window a token can be indexed in.
	m.store.EXPECT().
		ReleaseRenderGate(gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, row schema.MediaRenderProbe) ([]uint64, error) {
			assert.Equal(t, schema.RenderProbeVerdictRenderedOK, row.Verdict)
			assert.Equal(t, 0, row.ConsecutiveFailures, "recovery resets the counter")
			require.NotNil(t, row.BaselinePhash)
			assert.Equal(t, baseline, *row.BaselinePhash, "existing baseline carried through")
			return []uint64{5}, nil
		})

	healedCID := "eip155:1:erc721:0xabc:5"
	m.store.EXPECT().BatchUpdateTokensViewability(gomock.Any(), []uint64{5}).Return([]store.TokenViewabilityChange{
		{TokenID: 5, TokenCID: healedCID, OldViewable: false, NewViewable: true},
	}, nil)
	m.jobQueue.EXPECT().Enqueue(gomock.Any(), gomock.Any()).Return(&schema.Job{ID: 2}, true, nil)

	require.NoError(t, exec.ExecuteRenderProbe(context.Background(), url))
}

func TestExecuteRenderProbe_ssrfRefusalRecordsWithoutCounting(t *testing.T) {
	m, exec := setupRenderProbe(t, renderProbeTestConfig)
	url := "http://127.0.0.1/internal"

	m.store.EXPECT().GetMediaRenderProbe(gomock.Any(), url).Return(nil, nil)
	m.ssrf.EXPECT().ValidateHTTPURL(gomock.Any(), url).Return(errors.New("blocked: private address"))

	// Records a stalled row without rendering or gating.
	m.store.EXPECT().
		UpsertMediaRenderProbe(gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, row schema.MediaRenderProbe) error {
			assert.Equal(t, schema.RenderProbeVerdictStalled, row.Verdict)
			assert.Equal(t, 0, row.ConsecutiveFailures, "policy refusal is not render evidence")
			require.NotNil(t, row.LastError)
			assert.Contains(t, *row.LastError, "ssrf policy refused")
			return nil
		})

	require.NoError(t, exec.ExecuteRenderProbe(context.Background(), url))
}

// TestExecuteRenderProbe_ssrfRefusalPreservesExistingGate pins the recovery path: a
// temporary policy refusal must not erase the verdict/counter that identify a URL as
// gated. Losing them would make a later successful render fail to recognize the gate,
// leaving the health row broken forever (L0 never re-checks render_% rows).
func TestExecuteRenderProbe_ssrfRefusalPreservesExistingGate(t *testing.T) {
	m, exec := setupRenderProbe(t, renderProbeTestConfig)
	url := "https://example.com/temporarily-blocked.html"

	baseline := int64(4242)
	captured := m.now.Add(-24 * time.Hour)
	m.store.EXPECT().GetMediaRenderProbe(gomock.Any(), url).Return(&schema.MediaRenderProbe{
		MediaURL:            url,
		Verdict:             schema.RenderProbeVerdictBlank,
		ConsecutiveFailures: 2, // gated
		BaselinePhash:       &baseline,
		CapturedAt:          &captured,
	}, nil)
	m.ssrf.EXPECT().ValidateHTTPURL(gomock.Any(), url).Return(errors.New("blocked: resolves to private address"))

	m.store.EXPECT().
		UpsertMediaRenderProbe(gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, row schema.MediaRenderProbe) error {
			assert.Equal(t, schema.RenderProbeVerdictBlank, row.Verdict, "prior verdict retained")
			assert.Equal(t, 2, row.ConsecutiveFailures, "gate counter retained")
			require.NotNil(t, row.BaselinePhash)
			assert.Equal(t, baseline, *row.BaselinePhash)
			require.NotNil(t, row.LastError)
			assert.Contains(t, *row.LastError, "ssrf policy refused")
			return nil
		})

	require.NoError(t, exec.ExecuteRenderProbe(context.Background(), url))
}

// TestExecuteRenderProbe_cancellationLeavesStateUntouched: worker shutdown or job
// cancellation says nothing about the artwork, so no probe row is written and no gate
// counter advances — the URL stays due for the next run.
func TestExecuteRenderProbe_cancellationLeavesStateUntouched(t *testing.T) {
	m, exec := setupRenderProbe(t, renderProbeTestConfig)
	url := "https://example.com/canceled.html"

	ctx, cancel := context.WithCancel(context.Background())

	m.store.EXPECT().GetMediaRenderProbe(gomock.Any(), url).Return(nil, nil)
	m.ssrf.EXPECT().ValidateHTTPURL(gomock.Any(), url).Return(nil)
	m.renderer.EXPECT().
		RenderProbe(gomock.Any(), url, 0).
		DoAndReturn(func(context.Context, string, int) (*probe.Capture, error) {
			cancel()
			return nil, context.Canceled
		})

	// No UpsertMediaRenderProbe, no health write: strict mock enforces it.
	err := exec.ExecuteRenderProbe(ctx, url)
	require.ErrorIs(t, err, context.Canceled)
}

// TestExecuteRenderProbe_healthWritesAreRenderProbeWrites pins the ownership flag: L1's
// health writes must be marked so the store lets them set/clear render_% rows while
// every L0 writer is blocked from clearing them.
func TestExecuteRenderProbe_healthWritesAreRenderProbeWrites(t *testing.T) {
	m, exec := setupRenderProbe(t, renderProbeTestConfig)
	url := "https://example.com/gates.html"

	m.store.EXPECT().GetMediaRenderProbe(gomock.Any(), url).Return(&schema.MediaRenderProbe{
		MediaURL:            url,
		Verdict:             schema.RenderProbeVerdictBlank,
		ConsecutiveFailures: 1,
	}, nil)
	m.ssrf.EXPECT().ValidateHTTPURL(gomock.Any(), url).Return(nil)
	m.renderer.EXPECT().RenderProbe(gomock.Any(), url, 0).Return(blankFrame(), nil)
	m.store.EXPECT().
		AcquireRenderGate(gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, _ schema.MediaRenderProbe, upd store.MediaHealthUpdate) ([]uint64, error) {
			assert.True(t, upd.RenderProbeWrite, "L1 health writes must be marked as render-probe writes")
			return []uint64{1}, nil
		})
	m.store.EXPECT().BatchUpdateTokensViewability(gomock.Any(), []uint64{1}).Return(nil, nil)

	require.NoError(t, exec.ExecuteRenderProbe(context.Background(), url))
}

// TestExecuteRenderProbe_gateSurvivesVerdictChange pins the durable marker against the
// sequence that loses it when "is it gated" is re-derived from the verdict and counter:
// a fingerprint gate (count 1) followed by a stall that lands below a threshold of 3.
// The health row is gated from the first probe, so a later success must still heal it —
// otherwise L0, which never re-checks render_% rows, leaves it broken forever.
func TestExecuteRenderProbe_gateSurvivesVerdictChange(t *testing.T) {
	cfg := renderProbeTestConfig
	cfg.FailureGateThreshold = 3
	m, exec := setupRenderProbe(t, cfg)
	url := "https://example.com/fingerprint-then-stall.html"

	// Prior state: fingerprint gate recorded one failure and gated health.
	m.store.EXPECT().GetMediaRenderProbe(gomock.Any(), url).Return(&schema.MediaRenderProbe{
		MediaURL:            url,
		Verdict:             schema.RenderProbeVerdictKnownBadFingerprint,
		ConsecutiveFailures: 1,
		HealthGated:         true,
	}, nil)
	m.ssrf.EXPECT().ValidateHTTPURL(gomock.Any(), url).Return(nil)
	m.renderer.EXPECT().RenderProbe(gomock.Any(), url, 0).Return(nil, errors.New("context deadline exceeded"))

	// The stall lands at count 2, below the threshold of 3 — but the URL is already
	// gated, so the marker must persist and the row stay on the broken cadence.
	m.store.EXPECT().
		AcquireRenderGate(gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, row schema.MediaRenderProbe, _ store.MediaHealthUpdate) ([]uint64, error) {
			assert.Equal(t, schema.RenderProbeVerdictStalled, row.Verdict)
			assert.Equal(t, 2, row.ConsecutiveFailures)
			assert.True(t, row.HealthGated, "an existing gate must survive a verdict change below threshold")
			assert.Equal(t, m.now.Add(cfg.BrokenRecheckInterval), row.NextCheckAt)
			return []uint64{1}, nil
		})
	m.store.EXPECT().BatchUpdateTokensViewability(gomock.Any(), []uint64{1}).Return(nil, nil)

	require.NoError(t, exec.ExecuteRenderProbe(context.Background(), url))
}

// TestExecuteRenderProbe_failedReleaseRetainsGate pins recovery durability: when the
// atomic release fails nothing is cleared — neither the health rows nor the marker — so
// the next probe still sees the gate and retries. Recording rendered_ok separately would
// strand a broken health row that L0 never re-checks.
func TestExecuteRenderProbe_failedReleaseRetainsGate(t *testing.T) {
	m, exec := setupRenderProbe(t, renderProbeTestConfig)
	url := "https://example.com/release-fails.html"

	m.store.EXPECT().GetMediaRenderProbe(gomock.Any(), url).Return(&schema.MediaRenderProbe{
		MediaURL:            url,
		Verdict:             schema.RenderProbeVerdictBlank,
		ConsecutiveFailures: 2,
		HealthGated:         true,
	}, nil)
	m.ssrf.EXPECT().ValidateHTTPURL(gomock.Any(), url).Return(nil)
	m.renderer.EXPECT().RenderProbe(gomock.Any(), url, 0).Return(contentFrame(), nil)

	m.store.EXPECT().
		ReleaseRenderGate(gomock.Any(), gomock.Any()).
		Return(nil, assert.AnError)

	// Strict mock: no UpsertMediaRenderProbe and no viewability recompute may follow a
	// failed release — the gate must be left exactly as it was.
	err := exec.ExecuteRenderProbe(context.Background(), url)
	require.Error(t, err, "the job fails so the queue retries the release")
}

// TestExecuteRenderProbe_reconciliationFailureReschedulesJob pins the durability of the
// step after the gate commits. Health state is already broken at that point, but until
// BatchUpdateTokensViewability runs the tokens still read is_viewable=true — and nothing
// else fixes that: no sweep revisits render_% rows, a plain error is MarkJobFailed with
// no queue retry, and the probe row's next_check_at is already a full interval out. The
// error must therefore be a jobs.RescheduleError with a short delay, so the worker moves
// the job back to pending instead of permanently failing it.
func TestExecuteRenderProbe_reconciliationFailureReschedulesJob(t *testing.T) {
	m, exec := setupRenderProbe(t, renderProbeTestConfig)
	url := "https://example.com/reconcile-fails.html"

	m.store.EXPECT().GetMediaRenderProbe(gomock.Any(), url).Return(&schema.MediaRenderProbe{
		MediaURL:            url,
		Verdict:             schema.RenderProbeVerdictBlank,
		ConsecutiveFailures: 1,
	}, nil)
	m.ssrf.EXPECT().ValidateHTTPURL(gomock.Any(), url).Return(nil)
	m.renderer.EXPECT().RenderProbe(gomock.Any(), url, 0).Return(blankFrame(), nil)
	m.store.EXPECT().
		AcquireRenderGate(gomock.Any(), gomock.Any(), gomock.Any()).
		Return([]uint64{1}, nil)
	m.store.EXPECT().
		BatchUpdateTokensViewability(gomock.Any(), []uint64{1}).
		Return(nil, assert.AnError)

	// Strict mock: no webhook may be enqueued for a recompute that did not happen.
	err := exec.ExecuteRenderProbe(context.Background(), url)
	require.Error(t, err)
	var re *jobs.RescheduleError
	require.ErrorAs(t, err, &re, "reconciliation failure must reschedule, not permanently fail")
	assert.Equal(t, m.now.Add(time.Minute).UTC(), re.At, "short retry delay: is_viewable disagrees with durable health state until it lands")
}

// TestExecuteRenderProbe_reconciliationFailureFailsRecovery is the same contract on the
// healing path: the gate is already released (next_check_at a full RecheckInterval out),
// so tokens are still hidden until the recompute lands. Nothing else revisits them.
func TestExecuteRenderProbe_reconciliationFailureFailsRecovery(t *testing.T) {
	m, exec := setupRenderProbe(t, renderProbeTestConfig)
	url := "https://example.com/reconcile-fails-recovery.html"

	m.store.EXPECT().GetMediaRenderProbe(gomock.Any(), url).Return(&schema.MediaRenderProbe{
		MediaURL:            url,
		Verdict:             schema.RenderProbeVerdictBlank,
		ConsecutiveFailures: 2,
		HealthGated:         true,
	}, nil)
	m.ssrf.EXPECT().ValidateHTTPURL(gomock.Any(), url).Return(nil)
	m.renderer.EXPECT().RenderProbe(gomock.Any(), url, 0).Return(contentFrame(), nil)
	m.store.EXPECT().ReleaseRenderGate(gomock.Any(), gomock.Any()).Return([]uint64{4}, nil)
	m.store.EXPECT().
		BatchUpdateTokensViewability(gomock.Any(), []uint64{4}).
		Return(nil, assert.AnError)

	err := exec.ExecuteRenderProbe(context.Background(), url)
	require.Error(t, err)
	var re *jobs.RescheduleError
	require.ErrorAs(t, err, &re, "recovery reconciliation failure must reschedule, not permanently fail")
}

// TestExecuteRenderProbe_imageSettleShortcut pins the per-class settle contract: the
// shortened window is used only when ImageSettleMs is configured AND the store says every
// signal for the URL is a static raster image. Every other combination — shortcut
// disabled, non-image class — must pass 0 so the renderer applies its full default; a
// shortcut applied to a generative work is a manufactured blank verdict.
func TestExecuteRenderProbe_imageSettleShortcut(t *testing.T) {
	withImageSettle := renderProbeTestConfig
	withImageSettle.ImageSettleMs = 2000

	cases := []struct {
		name         string
		cfg          workflows.RenderProbeExecutorConfig
		classLookups func(m renderProbeMocks, url string)
		wantSettleMs int
	}{
		{
			name: "static image gets the shortened settle",
			cfg:  withImageSettle,
			classLookups: func(m renderProbeMocks, url string) {
				m.store.EXPECT().IsStaticImageRenderClass(gomock.Any(), url).Return(true, nil)
			},
			wantSettleMs: 2000,
		},
		{
			name: "non-image class keeps the full settle",
			cfg:  withImageSettle,
			classLookups: func(m renderProbeMocks, url string) {
				m.store.EXPECT().IsStaticImageRenderClass(gomock.Any(), url).Return(false, nil)
			},
			wantSettleMs: 0,
		},
		{
			name:         "shortcut disabled: no class lookup at all",
			cfg:          renderProbeTestConfig, // ImageSettleMs zero
			classLookups: func(m renderProbeMocks, url string) {},
			wantSettleMs: 0,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			m, exec := setupRenderProbe(t, tc.cfg)
			url := "https://example.com/classed-media"

			m.ssrf.EXPECT().ValidateHTTPURL(gomock.Any(), url).Return(nil)
			m.store.EXPECT().GetMediaRenderProbe(gomock.Any(), url).Return(nil, nil)
			tc.classLookups(m, url)
			m.renderer.EXPECT().RenderProbe(gomock.Any(), url, tc.wantSettleMs).Return(contentFrame(), nil)
			m.store.EXPECT().UpsertMediaRenderProbe(gomock.Any(), gomock.Any()).Return(nil)

			require.NoError(t, exec.ExecuteRenderProbe(context.Background(), url))
		})
	}
}

// TestExecuteRenderProbe_classLookupFailureFailsJob pins that a store failure during
// class lookup fails the job (queue retries) instead of guessing a class: guessing short
// on a generative work manufactures a blank verdict, and guessing long silently costs
// the throughput the shortcut exists to reclaim.
func TestExecuteRenderProbe_classLookupFailureFailsJob(t *testing.T) {
	cfg := renderProbeTestConfig
	cfg.ImageSettleMs = 2000
	m, exec := setupRenderProbe(t, cfg)
	url := "https://example.com/class-lookup-fails"

	m.ssrf.EXPECT().ValidateHTTPURL(gomock.Any(), url).Return(nil)
	m.store.EXPECT().GetMediaRenderProbe(gomock.Any(), url).Return(nil, nil)
	m.store.EXPECT().IsStaticImageRenderClass(gomock.Any(), url).Return(false, assert.AnError)

	err := exec.ExecuteRenderProbe(context.Background(), url)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "render class")
}
