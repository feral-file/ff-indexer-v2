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

// contentFrame renders with variance far above the blank threshold.
func contentFrame() *probe.Capture {
	img := image.NewRGBA(image.Rect(0, 0, 64, 64))
	for y := range 64 {
		for x := range 64 {
			img.Set(x, y, color.RGBA{uint8(x * 4), uint8(y * 4), 128, 255})
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
	m.renderer.EXPECT().RenderProbe(gomock.Any(), url).Return(contentFrame(), nil)

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
	m.renderer.EXPECT().RenderProbe(gomock.Any(), url).Return(blankFrame(), nil)

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
	m.renderer.EXPECT().RenderProbe(gomock.Any(), url).Return(blankFrame(), nil)

	m.store.EXPECT().
		UpsertMediaRenderProbe(gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, row schema.MediaRenderProbe) error {
			assert.Equal(t, 2, row.ConsecutiveFailures)
			assert.Equal(t, m.now.Add(renderProbeTestConfig.BrokenRecheckInterval), row.NextCheckAt)
			return nil
		})

	m.store.EXPECT().
		UpdateTokenMediaHealthByURL(gomock.Any(), url, gomock.Any()).
		DoAndReturn(func(_ context.Context, _ string, upd store.MediaHealthUpdate) error {
			assert.Equal(t, schema.MediaHealthStatusBroken, upd.Status)
			require.NotNil(t, upd.FailureReason)
			assert.Equal(t, schema.RenderFailureBlank, *upd.FailureReason)
			return nil
		})
	cid := "eip155:1:erc721:0xabc:7"
	m.store.EXPECT().GetTokenIDsByMediaURL(gomock.Any(), url).Return([]uint64{7}, nil)
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
	m.renderer.EXPECT().RenderProbe(gomock.Any(), url).Return(nil, errors.New("context deadline exceeded"))

	m.store.EXPECT().
		UpsertMediaRenderProbe(gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, row schema.MediaRenderProbe) error {
			assert.Equal(t, schema.RenderProbeVerdictStalled, row.Verdict)
			assert.Equal(t, 2, row.ConsecutiveFailures)
			assert.Nil(t, row.Phash, "no frame, no phash")
			return nil
		})

	m.store.EXPECT().
		UpdateTokenMediaHealthByURL(gomock.Any(), url, gomock.Any()).
		DoAndReturn(func(_ context.Context, _ string, upd store.MediaHealthUpdate) error {
			require.NotNil(t, upd.FailureReason)
			assert.Equal(t, schema.RenderFailureStalled, *upd.FailureReason)
			return nil
		})
	m.store.EXPECT().GetTokenIDsByMediaURL(gomock.Any(), url).Return([]uint64{9}, nil)
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
	m.renderer.EXPECT().RenderProbe(gomock.Any(), url).Return(frame, nil)

	m.store.EXPECT().
		UpsertMediaRenderProbe(gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, row schema.MediaRenderProbe) error {
			assert.Equal(t, schema.RenderProbeVerdictKnownBadFingerprint, row.Verdict)
			require.NotNil(t, row.LastError)
			assert.Contains(t, *row.LastError, "kubo-dir-listing")
			return nil
		})

	// Gates on first observation — no debounce for unambiguous matches.
	m.store.EXPECT().
		UpdateTokenMediaHealthByURL(gomock.Any(), url, gomock.Any()).
		DoAndReturn(func(_ context.Context, _ string, upd store.MediaHealthUpdate) error {
			require.NotNil(t, upd.FailureReason)
			assert.Equal(t, schema.RenderFailureKnownBad, *upd.FailureReason)
			return nil
		})
	m.store.EXPECT().GetTokenIDsByMediaURL(gomock.Any(), url).Return([]uint64{3}, nil)
	m.store.EXPECT().BatchUpdateTokensViewability(gomock.Any(), []uint64{3}).Return(nil, nil)

	require.NoError(t, exec.ExecuteRenderProbe(context.Background(), url))
}

func TestExecuteRenderProbe_renderedOKAfterGateHeals(t *testing.T) {
	m, exec := setupRenderProbe(t, renderProbeTestConfig)
	url := "https://example.com/recovered.html"

	baseline := int64(42)
	m.ssrf.EXPECT().ValidateHTTPURL(gomock.Any(), url).Return(nil)
	m.store.EXPECT().GetMediaRenderProbe(gomock.Any(), url).Return(&schema.MediaRenderProbe{
		MediaURL:            url,
		Verdict:             schema.RenderProbeVerdictBlank,
		ConsecutiveFailures: 2, // >= threshold: this row had gated
		BaselinePhash:       &baseline,
	}, nil)
	m.renderer.EXPECT().RenderProbe(gomock.Any(), url).Return(contentFrame(), nil)

	m.store.EXPECT().
		UpsertMediaRenderProbe(gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, row schema.MediaRenderProbe) error {
			assert.Equal(t, schema.RenderProbeVerdictRenderedOK, row.Verdict)
			assert.Equal(t, 0, row.ConsecutiveFailures, "recovery resets the counter")
			require.NotNil(t, row.BaselinePhash)
			assert.Equal(t, baseline, *row.BaselinePhash, "existing baseline carried through")
			return nil
		})

	// Heal: the render probe is the only healer of render-gated rows.
	m.store.EXPECT().
		UpdateTokenMediaHealthByURL(gomock.Any(), url, gomock.Any()).
		DoAndReturn(func(_ context.Context, _ string, upd store.MediaHealthUpdate) error {
			assert.Equal(t, schema.MediaHealthStatusHealthy, upd.Status)
			assert.Nil(t, upd.FailureReason)
			return nil
		})
	cid := "eip155:1:erc721:0xabc:5"
	m.store.EXPECT().GetTokenIDsByMediaURL(gomock.Any(), url).Return([]uint64{5}, nil)
	m.store.EXPECT().BatchUpdateTokensViewability(gomock.Any(), []uint64{5}).Return([]store.TokenViewabilityChange{
		{TokenID: 5, TokenCID: cid, OldViewable: false, NewViewable: true},
	}, nil)
	m.jobQueue.EXPECT().Enqueue(gomock.Any(), gomock.Any()).Return(&schema.Job{ID: 2}, true, nil)

	require.NoError(t, exec.ExecuteRenderProbe(context.Background(), url))
}

func TestExecuteRenderProbe_ssrfRefusalRecordsWithoutCounting(t *testing.T) {
	m, exec := setupRenderProbe(t, renderProbeTestConfig)
	url := "http://127.0.0.1/internal"

	m.ssrf.EXPECT().ValidateHTTPURL(gomock.Any(), url).Return(errors.New("blocked: private address"))

	// Records a stalled row without loading previous state, rendering, or gating.
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
