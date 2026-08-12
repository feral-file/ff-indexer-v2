package probe_test

import (
	"context"
	"image"
	"image/color"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/feral-file/ff-indexer-v2/internal/media/probe"
	"github.com/feral-file/ff-indexer-v2/internal/mocks"
)

// contentCapture returns a capture whose frame classifies rendered_ok; blankCapture one
// that classifies blank. The self-check tests run the REAL classifier on these frames,
// so they exercise SelfCheck's actual judgment rather than stubbed verdicts.
func contentCapture() *probe.Capture {
	return &probe.Capture{Image: gradientFrame(), EngineVersion: "HeadlessChrome/123.0", Viewport: "512x512"}
}

func blankCapture() *probe.Capture {
	img := image.NewRGBA(image.Rect(0, 0, 128, 128))
	for y := range 128 {
		for x := range 128 {
			img.Set(x, y, color.Black)
		}
	}
	return &probe.Capture{Image: img, EngineVersion: "HeadlessChrome/123.0", Viewport: "512x512"}
}

// expectSelfCheckRenders wires the mock renderer to return frames in SelfCheck's fixed
// fixture order: known-good, webgl, known-bad.
func expectSelfCheckRenders(m *mocks.MockRenderProbeRenderer, frames []*probe.Capture) {
	calls := make([]any, 0, len(frames))
	for _, f := range frames {
		calls = append(calls,
			m.EXPECT().
				RenderProbe(gomock.Any(), gomock.Any(), gomock.Any()).
				Return(f, nil))
	}
	gomock.InOrder(calls...)
}

func TestSelfCheck_passesOnHealthyRuntime(t *testing.T) {
	ctrl := gomock.NewController(t)
	m := mocks.NewMockRenderProbeRenderer(ctrl)
	expectSelfCheckRenders(m, []*probe.Capture{contentCapture(), contentCapture(), blankCapture()})

	require.NoError(t, probe.SelfCheck(context.Background(), m, 0.001))
}

func TestSelfCheck_failsWhenWebGLPaintsNothing(t *testing.T) {
	ctrl := gomock.NewController(t)
	m := mocks.NewMockRenderProbeRenderer(ctrl)
	// The good fixture paints, the WebGL fixture comes back uniform — the
	// disable-software-rasterizer regression class. SelfCheck must stop there: a strict
	// mock proves the blank fixture never renders.
	expectSelfCheckRenders(m, []*probe.Capture{contentCapture(), blankCapture()})

	err := probe.SelfCheck(context.Background(), m, 0.001)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "WebGL")
}

func TestSelfCheck_failsWhenBlankDetectionIsBroken(t *testing.T) {
	ctrl := gomock.NewController(t)
	m := mocks.NewMockRenderProbeRenderer(ctrl)
	// The blank fixture classifying rendered_ok means blank detection is not working in
	// this runtime: the probe would silently never gate anything.
	expectSelfCheckRenders(m, []*probe.Capture{contentCapture(), contentCapture(), contentCapture()})

	err := probe.SelfCheck(context.Background(), m, 0.001)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "blank detection")
}

func TestSelfCheck_zeroThresholdSkipsBlankFixture(t *testing.T) {
	ctrl := gomock.NewController(t)
	m := mocks.NewMockRenderProbeRenderer(ctrl)
	// blankThreshold <= 0 turns blank classification off by definition; only the two
	// rendered_ok fixtures run, and the strict mock proves no third render happens.
	expectSelfCheckRenders(m, []*probe.Capture{contentCapture(), contentCapture()})

	require.NoError(t, probe.SelfCheck(context.Background(), m, 0))
}

func TestSelfCheck_renderFailureNamesTheFixture(t *testing.T) {
	ctrl := gomock.NewController(t)
	m := mocks.NewMockRenderProbeRenderer(ctrl)
	m.EXPECT().
		RenderProbe(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(nil, assert.AnError)

	err := probe.SelfCheck(context.Background(), m, 0.001)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "known-good")
}

func TestVerifyNoEgress_reachableEndpointFailsWithRemediation(t *testing.T) {
	// A local listener stands in for a reachable metadata endpoint: the check must fail
	// and tell the operator what to do — "attestation false" without remediation is
	// just an outage.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer func() { _ = ln.Close() }()

	err = probe.VerifyNoEgressToForTest(context.Background(), []string{ln.Addr().String()}, time.Second)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "egress_restricted attestation is false")
	assert.Contains(t, err.Error(), "DOCKER-USER")
}

func TestVerifyNoEgress_unreachableEndpointPasses(t *testing.T) {
	// Grab a port, then close it so nothing listens there.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	addr := ln.Addr().String()
	require.NoError(t, ln.Close())

	require.NoError(t, probe.VerifyNoEgressToForTest(context.Background(), []string{addr}, time.Second))
}
