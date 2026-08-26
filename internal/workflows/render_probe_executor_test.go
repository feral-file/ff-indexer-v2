//go:build cgo

package workflows_test

import (
	"context"
	"errors"
	"fmt"
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
	"github.com/feral-file/ff-indexer-v2/internal/security/ssrf"
	"github.com/feral-file/ff-indexer-v2/internal/store"
	"github.com/feral-file/ff-indexer-v2/internal/store/schema"
	"github.com/feral-file/ff-indexer-v2/internal/workflows"
)

// renderProbeTestConfig is the shared executor config for these tests: gate at 2
// consecutive failures, distinct intervals so next_check_at assertions can tell which
// path scheduled the row.
var renderProbeTestConfig = workflows.RenderProbeExecutorConfig{
	BlankVarianceThreshold:    0.001,
	FailureGateThreshold:      2,
	RecheckInterval:           168 * time.Hour,
	RetryInterval:             time.Hour,
	BrokenRecheckInterval:     24 * time.Hour,
	NoEvidenceRecheckInterval: 72 * time.Hour, // distinct from the others so no-evidence rows are identifiable
	Enforce:                   true,           // most tests assert enforcement; shadow has its own suite
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

// TestExecuteRenderProbe_stalledRecordsNoEvidence pins the stall contract: a timeout is
// recorded under its own label but carries the counter and gate state forward untouched,
// so it can neither gate nor march a URL toward the threshold. Before this, 40 audited
// production would-gate stalls rendered 29 times on unloaded hardware in production's own
// configuration, at counters of 9–13.
func TestExecuteRenderProbe_stalledRecordsNoEvidence(t *testing.T) {
	t.Run("a stall after a blank keeps the counter and retries soon", func(t *testing.T) {
		m, exec := setupRenderProbe(t, renderProbeTestConfig)
		url := "https://example.com/hangs.html"

		m.ssrf.EXPECT().ValidateHTTPURL(gomock.Any(), url).Return(nil)
		m.store.EXPECT().GetMediaRenderProbe(gomock.Any(), url).Return(&schema.MediaRenderProbe{
			MediaURL:            url,
			Verdict:             schema.RenderProbeVerdictBlank,
			ConsecutiveFailures: 1,
		}, nil)
		m.renderer.EXPECT().RenderProbe(gomock.Any(), url, 0).Return(nil, errors.New("context deadline exceeded"))

		// No AcquireRenderGate: the strict mock enforces that a stall at the threshold
		// does not gate.
		m.store.EXPECT().
			UpsertMediaRenderProbe(gomock.Any(), gomock.Any()).
			DoAndReturn(func(_ context.Context, row schema.MediaRenderProbe) error {
				assert.Equal(t, schema.RenderProbeVerdictStalled, row.Verdict, "the label is kept for telemetry")
				assert.Equal(t, 1, row.ConsecutiveFailures, "a stall never advances the counter")
				assert.False(t, row.HealthGated)
				assert.Nil(t, row.Phash, "no frame, no phash")
				require.NotNil(t, row.LastError)
				assert.Contains(t, *row.LastError, "context deadline exceeded")
				assert.Equal(t, m.now.Add(renderProbeTestConfig.RetryInterval), row.NextCheckAt,
					"a first stall is the transient case: soon retry")
				return nil
			})

		require.NoError(t, exec.ExecuteRenderProbe(context.Background(), url))
	})

	t.Run("a stall after a stall is durable and moves to the no-evidence cadence", func(t *testing.T) {
		m, exec := setupRenderProbe(t, renderProbeTestConfig)
		url := "https://example.com/always-hangs.html"

		m.ssrf.EXPECT().ValidateHTTPURL(gomock.Any(), url).Return(nil)
		m.store.EXPECT().GetMediaRenderProbe(gomock.Any(), url).Return(&schema.MediaRenderProbe{
			MediaURL: url,
			Verdict:  schema.RenderProbeVerdictStalled,
		}, nil)
		m.renderer.EXPECT().RenderProbe(gomock.Any(), url, 0).Return(nil, errors.New("context deadline exceeded"))
		m.store.EXPECT().
			UpsertMediaRenderProbe(gomock.Any(), gomock.Any()).
			DoAndReturn(func(_ context.Context, row schema.MediaRenderProbe) error {
				assert.Equal(t, 0, row.ConsecutiveFailures)
				assert.Equal(t, m.now.Add(renderProbeTestConfig.NoEvidenceRecheckInterval), row.NextCheckAt,
					"repeat stalls must not hold an hourly retry that would out-spend the render budget")
				return nil
			})

		require.NoError(t, exec.ExecuteRenderProbe(context.Background(), url))
	})
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
			assert.Equal(t, 0, row.ConsecutiveFailures,
				"the counter is blank debounce state; a fingerprint match is not a blank observation")
			require.NotNil(t, row.LastError)
			assert.Contains(t, *row.LastError, "kubo-dir-listing")
			require.NotNil(t, upd.FailureReason)
			assert.Equal(t, schema.RenderFailureKnownBad, *upd.FailureReason)
			return []uint64{3}, nil
		})
	m.store.EXPECT().BatchUpdateTokensViewability(gomock.Any(), []uint64{3}).Return(nil, nil)

	require.NoError(t, exec.ExecuteRenderProbe(context.Background(), url))
}

// TestExecuteRenderProbe_blankAfterReleasedFingerprintGateIsDebounced pins the rollback
// chain the counter reset exists for: fingerprint gate -> probe disabled (the sweeper
// releases the gate but the probe row, counter included, survives) -> probe re-enabled
// -> one transient blank. With the old increment-on-fingerprint behavior the retained
// count of 1 plus this single blank reached the threshold of 2 and gated healthy media
// on one observation; the correct outcome is a recorded first failure on the debounce
// path.
func TestExecuteRenderProbe_blankAfterReleasedFingerprintGateIsDebounced(t *testing.T) {
	m, exec := setupRenderProbe(t, renderProbeTestConfig)
	url := "https://example.com/rollback-debounce.html"

	m.ssrf.EXPECT().ValidateHTTPURL(gomock.Any(), url).Return(nil)
	// The released row: fingerprint verdict retained, gate cleared by the disable-path
	// release, counter as the fixed code leaves it (0).
	m.store.EXPECT().GetMediaRenderProbe(gomock.Any(), url).Return(&schema.MediaRenderProbe{
		MediaURL:    url,
		Verdict:     schema.RenderProbeVerdictKnownBadFingerprint,
		HealthGated: false,
	}, nil)
	m.renderer.EXPECT().RenderProbe(gomock.Any(), url, 0).Return(blankFrame(), nil)

	// One blank after the release is a FIRST observation: recorded below the threshold,
	// no gate. The strict mock proves AcquireRenderGate is never called.
	m.store.EXPECT().
		UpsertMediaRenderProbe(gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, row schema.MediaRenderProbe) error {
			assert.Equal(t, schema.RenderProbeVerdictBlank, row.Verdict)
			assert.Equal(t, 1, row.ConsecutiveFailures, "first blank after a released fingerprint gate")
			assert.False(t, row.HealthGated)
			assert.Equal(t, m.now.Add(renderProbeTestConfig.RetryInterval), row.NextCheckAt, "debounce window, not a gate")
			return nil
		})

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
			assert.Equal(t, m.now.Add(renderProbeTestConfig.NoEvidenceRecheckInterval), row.NextCheckAt,
				"a policy block is durable — slow reprobe cadence")
			return nil
		})

	require.NoError(t, exec.ExecuteRenderProbe(context.Background(), url))
}

// TestExecuteRenderProbe_dnsResolutionFailureRetriesSoon pins the cadence split inside
// the SSRF validation path (bot finding F2 on #138): ssrf.ErrResolutionFailed is a
// transient resolver failure, not a policy verdict, so it must ride the short
// RetryInterval — the long no-evidence cadence would cost an L0-healthy URL a week of
// L1 coverage per DNS blip. State preservation is identical to a policy refusal.
func TestExecuteRenderProbe_dnsResolutionFailureRetriesSoon(t *testing.T) {
	m, exec := setupRenderProbe(t, renderProbeTestConfig)
	url := "https://flaky-resolver.example/work.html"

	m.store.EXPECT().GetMediaRenderProbe(gomock.Any(), url).Return(nil, nil)
	m.ssrf.EXPECT().ValidateHTTPURL(gomock.Any(), url).
		Return(fmt.Errorf("%w: DNS resolution failed for host %q", ssrf.ErrResolutionFailed, "flaky-resolver.example"))

	m.store.EXPECT().
		UpsertMediaRenderProbe(gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, row schema.MediaRenderProbe) error {
			assert.Equal(t, 0, row.ConsecutiveFailures, "a resolver failure is not render evidence")
			assert.Equal(t, m.now.Add(renderProbeTestConfig.RetryInterval), row.NextCheckAt,
				"transient resolver failure — short retry, not the no-evidence cadence")
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

	// The stall changes the label but carries the counter and the marker forward: the
	// gate must persist through the verdict change or the health row could never heal.
	m.store.EXPECT().
		UpsertMediaRenderProbe(gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, row schema.MediaRenderProbe) error {
			assert.Equal(t, schema.RenderProbeVerdictStalled, row.Verdict)
			assert.Equal(t, 1, row.ConsecutiveFailures)
			assert.True(t, row.HealthGated, "an existing gate must survive a verdict change")
			assert.Equal(t, m.now.Add(cfg.RetryInterval), row.NextCheckAt,
				"a gated row's first stall retries soon: the probe is its only healer")
			return nil
		})

	require.NoError(t, exec.ExecuteRenderProbe(context.Background(), url))
}

// TestExecuteRenderProbe_confirmationSettle pins which probes are confirmations and what
// they render with: a non-zero counter or a held gate takes ConfirmSettleMs regardless of
// render class (the class lookup is skipped), a first look keeps the class settle.
func TestExecuteRenderProbe_confirmationSettle(t *testing.T) {
	cfg := renderProbeTestConfig
	cfg.ImageSettleMs = 2000
	cfg.ConfirmSettleMs = 30000

	cases := []struct {
		name          string
		prev          *schema.MediaRenderProbe
		wantSettleMs  int
		wantClassCall bool
	}{
		{"first look uses the class settle", nil, 2000, true},
		{"counter above zero confirms", &schema.MediaRenderProbe{Verdict: schema.RenderProbeVerdictBlank, ConsecutiveFailures: 1}, 30000, false},
		{"a held gate confirms", &schema.MediaRenderProbe{Verdict: schema.RenderProbeVerdictKnownBadFingerprint, HealthGated: true}, 30000, false},
		{"a clean prior render is a first look", &schema.MediaRenderProbe{Verdict: schema.RenderProbeVerdictRenderedOK}, 2000, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			m, exec := setupRenderProbe(t, cfg)
			url := "https://example.com/confirm.jpg"
			if tc.prev != nil {
				tc.prev.MediaURL = url
			}
			m.ssrf.EXPECT().ValidateHTTPURL(gomock.Any(), url).Return(nil)
			m.store.EXPECT().GetMediaRenderProbe(gomock.Any(), url).Return(tc.prev, nil)
			if tc.wantClassCall {
				m.store.EXPECT().IsStaticImageRenderClass(gomock.Any(), url).Return(true, nil)
			}
			m.renderer.EXPECT().RenderProbe(gomock.Any(), url, tc.wantSettleMs).Return(contentFrame(), nil)
			if tc.prev != nil && tc.prev.HealthGated {
				m.store.EXPECT().ReleaseRenderGate(gomock.Any(), gomock.Any()).Return(nil, nil)
			} else {
				m.store.EXPECT().UpsertMediaRenderProbe(gomock.Any(), gomock.Any()).Return(nil)
			}
			require.NoError(t, exec.ExecuteRenderProbe(context.Background(), url))
		})
	}
}

// TestExecuteRenderProbe_confirmationRendersAlone pins the lane: a confirmation waits for
// every in-flight first look on the executor to finish and renders with none beside it.
// The second look is only evidence if it can disagree with the first, and one that runs
// under the first look's contention cannot.
func TestExecuteRenderProbe_confirmationRendersAlone(t *testing.T) {
	m, exec := setupRenderProbe(t, renderProbeTestConfig)
	first := "https://example.com/first-look.html"
	confirm := "https://example.com/confirm.html"

	m.ssrf.EXPECT().ValidateHTTPURL(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
	m.store.EXPECT().GetMediaRenderProbe(gomock.Any(), first).Return(nil, nil)
	m.store.EXPECT().GetMediaRenderProbe(gomock.Any(), confirm).Return(&schema.MediaRenderProbe{
		MediaURL:            confirm,
		Verdict:             schema.RenderProbeVerdictBlank,
		ConsecutiveFailures: 1,
	}, nil)
	m.store.EXPECT().UpsertMediaRenderProbe(gomock.Any(), gomock.Any()).Return(nil).Times(2)

	firstStarted := make(chan struct{})
	releaseFirst := make(chan struct{})
	confirmRendered := make(chan struct{})
	m.renderer.EXPECT().RenderProbe(gomock.Any(), first, 0).
		DoAndReturn(func(context.Context, string, int) (*probe.Capture, error) {
			close(firstStarted)
			<-releaseFirst
			return contentFrame(), nil
		})
	m.renderer.EXPECT().RenderProbe(gomock.Any(), confirm, 0).
		DoAndReturn(func(context.Context, string, int) (*probe.Capture, error) {
			close(confirmRendered)
			return contentFrame(), nil
		})

	firstDone := make(chan error, 1)
	go func() { firstDone <- exec.ExecuteRenderProbe(context.Background(), first) }()
	<-firstStarted
	confirmDone := make(chan error, 1)
	go func() { confirmDone <- exec.ExecuteRenderProbe(context.Background(), confirm) }()

	select {
	case <-confirmRendered:
		t.Fatal("confirmation rendered while a first look was in flight")
	case <-time.After(100 * time.Millisecond):
	}
	close(releaseFirst)
	require.NoError(t, <-firstDone)
	require.NoError(t, <-confirmDone)
	<-confirmRendered
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

// TestExecuteRenderProbe_shadowMode pins the shadow contract: with Enforce false the
// probe records everything enforcement would — verdict, counter, enforcement cadence —
// but never touches health rows or viewability. Strict mocks prove no
// AcquireRenderGate, no BatchUpdateTokensViewability, and no webhook on any would-gate
// path.
func TestExecuteRenderProbe_shadowMode(t *testing.T) {
	shadow := renderProbeTestConfig
	shadow.Enforce = false

	t.Run("blank at threshold records but does not gate", func(t *testing.T) {
		m, exec := setupRenderProbe(t, shadow)
		url := "https://example.com/shadow/dead.html"

		m.ssrf.EXPECT().ValidateHTTPURL(gomock.Any(), url).Return(nil)
		m.store.EXPECT().GetMediaRenderProbe(gomock.Any(), url).Return(&schema.MediaRenderProbe{
			MediaURL:            url,
			Verdict:             schema.RenderProbeVerdictBlank,
			ConsecutiveFailures: 1,
		}, nil)
		m.renderer.EXPECT().RenderProbe(gomock.Any(), url, 0).Return(blankFrame(), nil)

		m.store.EXPECT().
			UpsertMediaRenderProbe(gomock.Any(), gomock.Any()).
			DoAndReturn(func(_ context.Context, row schema.MediaRenderProbe) error {
				assert.Equal(t, 2, row.ConsecutiveFailures, "counter identical to enforcement")
				assert.False(t, row.HealthGated, "shadow never sets the marker")
				assert.Equal(t, m.now.Add(renderProbeTestConfig.BrokenRecheckInterval), row.NextCheckAt,
					"would-gated rows keep the enforcement cadence so shadow data stays fresh")
				return nil
			})

		require.NoError(t, exec.ExecuteRenderProbe(context.Background(), url))
	})

	t.Run("fingerprint match records but does not gate", func(t *testing.T) {
		frame := contentFrame()
		cls, err := probe.Classify(frame.Image, nil, 0.001)
		require.NoError(t, err)
		cfg := shadow
		cfg.Fingerprints = []probe.Fingerprint{{Hash: cls.Phash, MaxDistance: 4, Label: "kubo-dir-listing"}}
		m, exec := setupRenderProbe(t, cfg)
		url := "https://example.com/shadow/dir-cid"

		m.ssrf.EXPECT().ValidateHTTPURL(gomock.Any(), url).Return(nil)
		m.store.EXPECT().GetMediaRenderProbe(gomock.Any(), url).Return(nil, nil)
		m.renderer.EXPECT().RenderProbe(gomock.Any(), url, 0).Return(frame, nil)

		m.store.EXPECT().
			UpsertMediaRenderProbe(gomock.Any(), gomock.Any()).
			DoAndReturn(func(_ context.Context, row schema.MediaRenderProbe) error {
				assert.Equal(t, schema.RenderProbeVerdictKnownBadFingerprint, row.Verdict)
				assert.False(t, row.HealthGated)
				require.NotNil(t, row.LastError)
				assert.Contains(t, *row.LastError, "kubo-dir-listing")
				return nil
			})

		require.NoError(t, exec.ExecuteRenderProbe(context.Background(), url))
	})

	t.Run("a stale gate is not re-acquired by a shadow failure", func(t *testing.T) {
		// A gate left over from an enforcing deployment: the sweeper releases it, and
		// meanwhile a shadow failure must carry the marker forward untouched rather
		// than re-acquire the gate.
		m, exec := setupRenderProbe(t, shadow)
		url := "https://example.com/shadow/stale-gate.html"

		m.ssrf.EXPECT().ValidateHTTPURL(gomock.Any(), url).Return(nil)
		m.store.EXPECT().GetMediaRenderProbe(gomock.Any(), url).Return(&schema.MediaRenderProbe{
			MediaURL:            url,
			Verdict:             schema.RenderProbeVerdictBlank,
			ConsecutiveFailures: 2,
			HealthGated:         true,
		}, nil)
		m.renderer.EXPECT().RenderProbe(gomock.Any(), url, 0).Return(blankFrame(), nil)

		m.store.EXPECT().
			UpsertMediaRenderProbe(gomock.Any(), gomock.Any()).
			DoAndReturn(func(_ context.Context, row schema.MediaRenderProbe) error {
				assert.True(t, row.HealthGated, "the marker survives for the sweeper's release; only release may clear it")
				return nil
			})

		require.NoError(t, exec.ExecuteRenderProbe(context.Background(), url))
	})

	t.Run("a stale gate is still released by a successful shadow render", func(t *testing.T) {
		// Releasing is un-hiding — allowed and wanted in shadow.
		m, exec := setupRenderProbe(t, shadow)
		url := "https://example.com/shadow/heals.html"

		m.ssrf.EXPECT().ValidateHTTPURL(gomock.Any(), url).Return(nil)
		m.store.EXPECT().GetMediaRenderProbe(gomock.Any(), url).Return(&schema.MediaRenderProbe{
			MediaURL:            url,
			Verdict:             schema.RenderProbeVerdictBlank,
			ConsecutiveFailures: 2,
			HealthGated:         true,
		}, nil)
		m.renderer.EXPECT().RenderProbe(gomock.Any(), url, 0).Return(contentFrame(), nil)
		m.store.EXPECT().ReleaseRenderGate(gomock.Any(), gomock.Any()).Return([]uint64{9}, nil)
		m.store.EXPECT().BatchUpdateTokensViewability(gomock.Any(), []uint64{9}).Return(nil, nil)

		require.NoError(t, exec.ExecuteRenderProbe(context.Background(), url))
	})
}

// TestExecuteRenderProbe_stallAfterCapturePreservesCaptureMetadata pins the capture-only
// record's integrity across failures: a stall after a successful capture must carry the
// whole prior observation forward — pHash, engine, viewport, timestamp — not just the
// timestamp. Keeping CapturedAt while nulling the rest (an earlier revision did) claims
// a capture happened while deleting its comparability data.
func TestExecuteRenderProbe_stallAfterCapturePreservesCaptureMetadata(t *testing.T) {
	m, exec := setupRenderProbe(t, renderProbeTestConfig)
	url := "https://example.com/stall-after-capture.html"

	prevPhash := int64(0x1234567890ABCDEF)
	prevEngine := "HeadlessChrome/122.0"
	prevViewport := "1024x1024"
	prevCaptured := m.now.Add(-24 * time.Hour)
	m.ssrf.EXPECT().ValidateHTTPURL(gomock.Any(), url).Return(nil)
	m.store.EXPECT().GetMediaRenderProbe(gomock.Any(), url).Return(&schema.MediaRenderProbe{
		MediaURL:      url,
		Verdict:       schema.RenderProbeVerdictRenderedOK,
		Phash:         &prevPhash,
		BaselinePhash: &prevPhash,
		EngineVersion: &prevEngine,
		Viewport:      &prevViewport,
		CapturedAt:    &prevCaptured,
	}, nil)
	m.renderer.EXPECT().RenderProbe(gomock.Any(), url, 0).Return(nil, errors.New("context deadline exceeded"))

	m.store.EXPECT().
		UpsertMediaRenderProbe(gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, row schema.MediaRenderProbe) error {
			assert.Equal(t, schema.RenderProbeVerdictStalled, row.Verdict)
			assert.Equal(t, 0, row.ConsecutiveFailures, "a stall is no evidence; the counter is carried, not advanced")
			require.NotNil(t, row.Phash, "the last capture's pHash survives a stall")
			assert.Equal(t, prevPhash, *row.Phash)
			require.NotNil(t, row.EngineVersion)
			assert.Equal(t, prevEngine, *row.EngineVersion)
			require.NotNil(t, row.Viewport)
			assert.Equal(t, prevViewport, *row.Viewport)
			require.NotNil(t, row.CapturedAt)
			assert.Equal(t, prevCaptured, *row.CapturedAt)
			return nil
		})

	require.NoError(t, exec.ExecuteRenderProbe(context.Background(), url))
}

// TestExecuteRenderProbe_browserUnavailableReschedulesJob pins the infrastructure/
// evidence split for launch failures: a browser that never started (fork exhaustion,
// startup crash) says nothing about the artwork, so no probe row is written, no counter
// advances, and the job is rescheduled to retry after the backoff delay. Recording these
// as stalled let the 2026-08-17 host incident march ~2,100 healthy URLs to would-gate
// counters.
func TestExecuteRenderProbe_browserUnavailableReschedulesJob(t *testing.T) {
	m, exec := setupRenderProbe(t, renderProbeTestConfig)
	url := "https://example.com/work.html"

	m.store.EXPECT().GetMediaRenderProbe(gomock.Any(), url).Return(nil, nil)
	m.ssrf.EXPECT().ValidateHTTPURL(gomock.Any(), url).Return(nil)
	m.renderer.EXPECT().
		RenderProbe(gomock.Any(), url, 0).
		Return(nil, fmt.Errorf("render probe browser launch failed for %s: %w: fork/exec /usr/bin/chromium: resource temporarily unavailable",
			url, probe.ErrBrowserUnavailable))

	// No UpsertMediaRenderProbe, no health write: the strict mock enforces that probe
	// state stays untouched.
	err := exec.ExecuteRenderProbe(context.Background(), url)
	require.Error(t, err)
	var re *jobs.RescheduleError
	require.ErrorAs(t, err, &re, "launch failure must reschedule the job, not fail it")
	assert.Equal(t, m.now.Add(5*time.Minute), re.At)
}

// TestExecuteRenderProbe_non2xxMainStatusRecordsWithoutCounting pins the no-evidence
// contract for served error pages: chromium paints an HTTP error body like a normal page
// (production measurement: ipfs.io's 410 bot-block page classified 1,692 healthy
// artworks as blank), so a non-2xx main document must never be classified — no phash, no
// baseline seeding, no counter movement — only recorded with its status.
func TestExecuteRenderProbe_non2xxMainStatusRecordsWithoutCounting(t *testing.T) {
	m, exec := setupRenderProbe(t, renderProbeTestConfig)
	url := "https://example.com/bot-blocked.html"

	m.store.EXPECT().GetMediaRenderProbe(gomock.Any(), url).Return(nil, nil)
	m.ssrf.EXPECT().ValidateHTTPURL(gomock.Any(), url).Return(nil)
	blocked := contentFrame()
	blocked.MainStatus = 410
	m.renderer.EXPECT().RenderProbe(gomock.Any(), url, 0).Return(blocked, nil)

	m.store.EXPECT().
		UpsertMediaRenderProbe(gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, row schema.MediaRenderProbe) error {
			assert.Equal(t, schema.RenderProbeVerdictStalled, row.Verdict)
			assert.Equal(t, 0, row.ConsecutiveFailures, "a served error page is not render evidence")
			assert.Nil(t, row.Phash, "an error page's frame must not be hashed")
			assert.Nil(t, row.BaselinePhash, "an error page must never seed the baseline")
			require.NotNil(t, row.LastError)
			assert.Contains(t, *row.LastError, "HTTP 410")
			assert.Equal(t, m.now.Add(renderProbeTestConfig.NoEvidenceRecheckInterval), row.NextCheckAt)
			return nil
		})

	require.NoError(t, exec.ExecuteRenderProbe(context.Background(), url))
}

// TestExecuteRenderProbe_non2xxMainStatusPreservesExistingState mirrors the SSRF-refusal
// preservation contract: a gateway serving an error page must not erase the verdict,
// counter, or capture metadata that identify the URL's real probe history — losing them
// would strand a gated URL with no healer.
func TestExecuteRenderProbe_non2xxMainStatusPreservesExistingState(t *testing.T) {
	m, exec := setupRenderProbe(t, renderProbeTestConfig)
	url := "https://example.com/blocked-after-history.html"

	prevPhash := int64(1234)
	baseline := int64(4242)
	captured := m.now.Add(-24 * time.Hour)
	m.store.EXPECT().GetMediaRenderProbe(gomock.Any(), url).Return(&schema.MediaRenderProbe{
		MediaURL:            url,
		Verdict:             schema.RenderProbeVerdictBlank,
		ConsecutiveFailures: 2, // gated
		HealthGated:         true,
		Phash:               &prevPhash,
		BaselinePhash:       &baseline,
		CapturedAt:          &captured,
	}, nil)
	m.ssrf.EXPECT().ValidateHTTPURL(gomock.Any(), url).Return(nil)
	blocked := contentFrame()
	blocked.MainStatus = 429
	m.renderer.EXPECT().RenderProbe(gomock.Any(), url, 0).Return(blocked, nil)

	m.store.EXPECT().
		UpsertMediaRenderProbe(gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, row schema.MediaRenderProbe) error {
			assert.Equal(t, schema.RenderProbeVerdictBlank, row.Verdict, "prior verdict retained")
			assert.Equal(t, 2, row.ConsecutiveFailures, "gate counter retained")
			assert.True(t, row.HealthGated, "gate marker retained")
			require.NotNil(t, row.Phash)
			assert.Equal(t, prevPhash, *row.Phash, "last successful capture retained")
			require.NotNil(t, row.BaselinePhash)
			assert.Equal(t, baseline, *row.BaselinePhash)
			require.NotNil(t, row.CapturedAt)
			assert.Equal(t, captured, *row.CapturedAt)
			require.NotNil(t, row.LastError)
			assert.Contains(t, *row.LastError, "HTTP 429")
			assert.Equal(t, m.now.Add(renderProbeTestConfig.BrokenRecheckInterval), row.NextCheckAt,
				"a gated row keeps its heal cadence — the probe is its only healer, so the "+
					"slow no-evidence interval would hide recovered artwork for a week")
			return nil
		})

	require.NoError(t, exec.ExecuteRenderProbe(context.Background(), url))
}
