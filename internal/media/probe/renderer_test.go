package probe_test

import (
	"bytes"
	"context"
	"image/png"
	"testing"
	"time"

	"github.com/chromedp/chromedp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/feral-file/ff-indexer-v2/internal/media/probe"
	"github.com/feral-file/ff-indexer-v2/internal/mocks"
)

// encodePNG encodes the shared gradient frame so the renderer's decode path runs on
// real bytes.
func encodePNG(t *testing.T) []byte {
	var buf bytes.Buffer
	require.NoError(t, png.Encode(&buf, gradientFrame()))
	return buf.Bytes()
}

func newMockedRenderer(t *testing.T) (*mocks.MockChromedpClient, probe.Renderer) {
	ctrl := gomock.NewController(t)
	t.Cleanup(ctrl.Finish)
	mockChromedp := mocks.NewMockChromedpClient(ctrl)

	mockChromedp.EXPECT().
		NewExecAllocator(gomock.Any(), gomock.Any()).
		DoAndReturn(func(ctx context.Context, _ []chromedp.ExecAllocatorOption) (context.Context, context.CancelFunc) {
			return context.WithCancel(ctx)
		})

	r := probe.NewRenderer(mockChromedp, &probe.RendererConfig{
		ViewportWidth:  640,
		ViewportHeight: 480,
		TimeoutMs:      5000,
		SettleMs:       10,
	})
	t.Cleanup(func() { _ = r.Close() })
	return mockChromedp, r
}

func TestRenderProbe_capturesFrameAndEngine(t *testing.T) {
	mockChromedp, r := newMockedRenderer(t)
	url := "https://example.com/work.html"
	pngBytes := encodePNG(t)

	mockChromedp.EXPECT().
		NewContext(gomock.Any()).
		DoAndReturn(func(ctx context.Context) (context.Context, context.CancelFunc) {
			return context.WithCancel(ctx)
		})

	// The action-builder mocks populate the output pointers directly (builders run
	// before Run in the real flow too; the renderer only reads them after Run returns).
	mockChromedp.EXPECT().EmulateViewport(int64(640), int64(480)).Return(nil)
	mockChromedp.EXPECT().Navigate(url).Return(nil)
	mockChromedp.EXPECT().WaitReady("body").Return(nil)
	mockChromedp.EXPECT().
		Evaluate("navigator.userAgent", gomock.Any()).
		DoAndReturn(func(_ string, res any, _ ...chromedp.EvaluateOption) chromedp.EvaluateAction {
			*(res.(*string)) = "HeadlessChrome/123.0"
			return nil
		})
	mockChromedp.EXPECT().Sleep(10 * time.Millisecond).Return(nil)
	mockChromedp.EXPECT().
		FullScreenshot(gomock.Any(), 100).
		DoAndReturn(func(res *[]byte, _ int) chromedp.Action {
			*res = pngBytes
			return nil
		})
	mockChromedp.EXPECT().
		Run(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		Return(nil)

	capture, err := r.RenderProbe(context.Background(), url)
	require.NoError(t, err)
	require.NotNil(t, capture)
	assert.Equal(t, "HeadlessChrome/123.0", capture.EngineVersion)
	assert.Equal(t, "640x480", capture.Viewport)
	require.NotNil(t, capture.Image)
	assert.Equal(t, 128, capture.Image.Bounds().Dx(), "decoded frame keeps source dimensions")
}

func TestRenderProbe_runErrorIsStalledSignal(t *testing.T) {
	mockChromedp, r := newMockedRenderer(t)
	url := "https://example.com/hangs.html"

	mockChromedp.EXPECT().
		NewContext(gomock.Any()).
		DoAndReturn(func(ctx context.Context) (context.Context, context.CancelFunc) {
			return context.WithCancel(ctx)
		})
	mockChromedp.EXPECT().EmulateViewport(gomock.Any(), gomock.Any()).Return(nil)
	mockChromedp.EXPECT().Navigate(url).Return(nil)
	mockChromedp.EXPECT().WaitReady("body").Return(nil)
	mockChromedp.EXPECT().Evaluate(gomock.Any(), gomock.Any()).Return(nil)
	mockChromedp.EXPECT().Sleep(gomock.Any()).Return(nil)
	mockChromedp.EXPECT().FullScreenshot(gomock.Any(), 100).Return(nil)
	mockChromedp.EXPECT().
		Run(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		Return(context.DeadlineExceeded)

	capture, err := r.RenderProbe(context.Background(), url)
	require.Error(t, err)
	assert.Nil(t, capture)
	assert.ErrorIs(t, err, context.DeadlineExceeded)
}

func TestRenderProbe_undecodableScreenshotErrors(t *testing.T) {
	mockChromedp, r := newMockedRenderer(t)
	url := "https://example.com/garbage.html"

	mockChromedp.EXPECT().
		NewContext(gomock.Any()).
		DoAndReturn(func(ctx context.Context) (context.Context, context.CancelFunc) {
			return context.WithCancel(ctx)
		})
	mockChromedp.EXPECT().EmulateViewport(gomock.Any(), gomock.Any()).Return(nil)
	mockChromedp.EXPECT().Navigate(url).Return(nil)
	mockChromedp.EXPECT().WaitReady("body").Return(nil)
	mockChromedp.EXPECT().Evaluate(gomock.Any(), gomock.Any()).Return(nil)
	mockChromedp.EXPECT().Sleep(gomock.Any()).Return(nil)
	mockChromedp.EXPECT().
		FullScreenshot(gomock.Any(), 100).
		DoAndReturn(func(res *[]byte, _ int) chromedp.Action {
			*res = []byte("not a png")
			return nil
		})
	mockChromedp.EXPECT().
		Run(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		Return(nil)

	capture, err := r.RenderProbe(context.Background(), url)
	require.Error(t, err)
	assert.Nil(t, capture)
	assert.Contains(t, err.Error(), "decoding screenshot")
}
