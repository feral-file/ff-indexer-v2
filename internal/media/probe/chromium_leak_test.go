//go:build chromium && unix

// Live-browser regression test for issue #136: a probe teardown must not leak chromium
// child processes or the chromedp-runner user-data dir.
//
// Reason: the mocked tests can prove the contexts are cancelled, but the incident showed
// that cancellation alone was never the problem — chromedp's default cancel kills only
// the browser process, and a renderer wedged in artwork JS survives it. Only a real
// chromium can regress-test the process-group kill, so this lives in the chromium-tagged
// suite next to the smoke tests.
package probe_test

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/chromedp/chromedp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/feral-file/ff-indexer-v2/internal/adapter"
	"github.com/feral-file/ff-indexer-v2/internal/media/browserproc"
	"github.com/feral-file/ff-indexer-v2/internal/media/probe"
)

// cmdRecorder captures the launched browser command so the test can address its process
// group after teardown. chromedp keeps a single ModifyCmdFunc — appending this option
// after probe.AllocatorOptions() replaces the browserproc one, so it must re-apply
// ConfigureCmd itself to test the production configuration.
type cmdRecorder struct {
	mu   sync.Mutex
	cmds []*exec.Cmd
}

func (r *cmdRecorder) option() chromedp.ExecAllocatorOption {
	return chromedp.ModifyCmdFunc(func(cmd *exec.Cmd) {
		browserproc.ConfigureCmd(cmd)
		r.mu.Lock()
		defer r.mu.Unlock()
		r.cmds = append(r.cmds, cmd)
	})
}

func (r *cmdRecorder) recorded(t *testing.T) *exec.Cmd {
	t.Helper()
	r.mu.Lock()
	defer r.mu.Unlock()
	require.Len(t, r.cmds, 1, "expected exactly one browser launch")
	return r.cmds[0]
}

// userDataDir extracts the chromedp-runner temp dir from the recorded launch args.
func userDataDir(t *testing.T, cmd *exec.Cmd) string {
	t.Helper()
	for _, arg := range cmd.Args {
		if dir, ok := strings.CutPrefix(arg, "--user-data-dir="); ok {
			return dir
		}
	}
	t.Fatal("no --user-data-dir in browser args")
	return ""
}

// requireTreeAndDirGone polls until the browser's whole process group has no members
// (zombies included) and its user-data dir is removed. Polling bounds the small windows
// that are legitimate: zombies linger until init reaps them, and chromedp removes the
// dir 10ms after reaping the browser.
func requireTreeAndDirGone(t *testing.T, cmd *exec.Cmd, dir string) {
	t.Helper()
	require.NotNil(t, cmd.Process)
	pgid := cmd.Process.Pid // Setpgid(0): the browser leads its own group
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		groupGone := errors.Is(syscall.Kill(-pgid, 0), syscall.ESRCH)
		_, statErr := os.Stat(dir)
		if groupGone && os.IsNotExist(statErr) {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	err := syscall.Kill(-pgid, 0)
	_, statErr := os.Stat(dir)
	t.Fatalf("leak after teardown: group %d kill(0) err=%v (want ESRCH), dir %s stat err=%v (want not-exist)",
		pgid, err, dir, statErr)
}

func newLeakTestRenderer(rec *cmdRecorder, timeoutMs, settleMs int) probe.Renderer {
	return probe.NewRenderer(adapter.NewChromedpClient(), &probe.RendererConfig{
		ViewportWidth:    256,
		ViewportHeight:   256,
		TimeoutMs:        timeoutMs,
		SettleMs:         settleMs,
		AllocatorOptions: append(probe.AllocatorOptions(), rec.option()),
	})
}

// TestChromiumTeardownWedgedRenderer is the incident scenario: artwork JS wedges the
// renderer in an infinite loop, the probe burns its timeout, and teardown must still
// take down every chromium process and the user-data dir.
func TestChromiumTeardownWedgedRenderer(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/html")
		_, _ = w.Write([]byte(`<html><body style="background:#123"><script>for(;;){}</script></body></html>`))
	}))
	defer srv.Close()

	rec := &cmdRecorder{}
	renderer := newLeakTestRenderer(rec, 8000, 500)
	defer func() { _ = renderer.Close() }()

	_, err := renderer.RenderProbe(context.Background(), srv.URL, 0)
	require.Error(t, err, "a wedged renderer must time out, not capture")

	cmd := rec.recorded(t)
	requireTreeAndDirGone(t, cmd, userDataDir(t, cmd))
}

// TestChromiumTeardownAfterSuccess proves the ordinary path leaks nothing either: every
// probe launches its own browser, so a successful capture is followed by a full-tree
// teardown too.
func TestChromiumTeardownAfterSuccess(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/html")
		_, _ = w.Write([]byte(`<html><body style="margin:0">` +
			`<div style="width:100vw;height:100vh;background:linear-gradient(135deg,#0af,#f0a)"></div>` +
			`</body></html>`))
	}))
	defer srv.Close()

	rec := &cmdRecorder{}
	renderer := newLeakTestRenderer(rec, 30000, 500)
	defer func() { _ = renderer.Close() }()

	capture, err := renderer.RenderProbe(context.Background(), srv.URL, 0)
	require.NoError(t, err)
	assert.NotNil(t, capture.Image)

	cmd := rec.recorded(t)
	requireTreeAndDirGone(t, cmd, userDataDir(t, cmd))
}
