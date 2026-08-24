//go:build chromium && linux

// The faithful issue-#136 regression: a chromium child that cannot run its channel-close
// detection must still die with the browser.
//
// Reason: a renderer wedged by page JS alone does NOT reproduce the prod leak — its IO
// thread still notices the dead IPC channel and self-exits (measured on both darwin and
// linux). The leaked processes were children that dodged that detection (mid-abort while
// dumping core, blocked in the kernel). SIGSTOP models that class deterministically: a
// stopped process runs no threads at all, so with chromedp's stock browser-only kill it
// outlives teardown forever — while the process-group SIGKILL takes it down regardless,
// because SIGKILL acts on stopped processes too. Linux-only: it walks /proc to find the
// browser's descendant tree.
package probe_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// descendantsOf returns the transitive child pids of root, walking /proc once.
func descendantsOf(t *testing.T, root int) []int {
	t.Helper()
	children := map[int][]int{}
	entries, err := os.ReadDir("/proc")
	require.NoError(t, err)
	for _, e := range entries {
		pid, err := strconv.Atoi(e.Name())
		if err != nil {
			continue
		}
		stat, err := os.ReadFile(filepath.Join("/proc", e.Name(), "stat"))
		if err != nil {
			continue // process raced away
		}
		// Field 4 (ppid) sits after the parenthesized comm, which may contain spaces.
		rest := string(stat[strings.LastIndexByte(string(stat), ')')+2:])
		fields := strings.Fields(rest)
		if len(fields) < 2 {
			continue
		}
		ppid, err := strconv.Atoi(fields[1])
		if err != nil {
			continue
		}
		children[ppid] = append(children[ppid], pid)
	}
	var out []int
	queue := []int{root}
	for len(queue) > 0 {
		p := queue[0]
		queue = queue[1:]
		for _, c := range children[p] {
			out = append(out, c)
			queue = append(queue, c)
		}
	}
	return out
}

// stopOneRenderer polls the browser's descendant tree for a --type=renderer process and
// SIGSTOPs the first one found, returning its pid.
func stopOneRenderer(t *testing.T, browserPid int, deadline time.Time) int {
	t.Helper()
	for time.Now().Before(deadline) {
		for _, pid := range descendantsOf(t, browserPid) {
			cmdline, err := os.ReadFile(filepath.Join("/proc", strconv.Itoa(pid), "cmdline"))
			if err != nil || !strings.Contains(string(cmdline), "--type=renderer") {
				continue
			}
			if err := syscall.Kill(pid, syscall.SIGSTOP); err == nil {
				return pid
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatal("no renderer child appeared to SIGSTOP")
	return 0
}

// TestChromiumTeardownStoppedRenderer: a renderer that cannot observe the browser's
// death (SIGSTOP — the deterministic stand-in for the mid-crash children of the
// incident) must still be killed by teardown, and the user-data dir must be removed.
// With chromedp's stock cancel this leaks the stopped renderer unconditionally.
func TestChromiumTeardownStoppedRenderer(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/html")
		_, _ = w.Write([]byte(`<html><body style="margin:0">` +
			`<div style="width:100vw;height:100vh;background:linear-gradient(135deg,#0af,#f0a)"></div>` +
			`</body></html>`))
	}))
	defer srv.Close()

	rec := &cmdRecorder{}
	// Settle long enough (4s) that the probe is still mid-render while the test finds
	// and stops the renderer; timeout comfortably above it so the probe still succeeds
	// or times out without racing the assertion — either end state must be leak-free.
	renderer := newLeakTestRenderer(rec, 20000, 4000)
	defer func() { _ = renderer.Close() }()

	probeDone := make(chan error, 1)
	go func() {
		_, err := renderer.RenderProbe(context.Background(), srv.URL, 0)
		probeDone <- err
	}()

	// The browser launches shortly after RenderProbe enters chromedp.Run.
	var browserPid int
	require.Eventually(t, func() bool {
		rec.mu.Lock()
		defer rec.mu.Unlock()
		if len(rec.cmds) == 1 && rec.cmds[0].Process != nil {
			browserPid = rec.cmds[0].Process.Pid
			return true
		}
		return false
	}, 10*time.Second, 50*time.Millisecond, "browser never launched")

	stopped := stopOneRenderer(t, browserPid, time.Now().Add(10*time.Second))
	t.Logf("SIGSTOPped renderer pid %d under browser %d", stopped, browserPid)

	<-probeDone // capture result irrelevant; only teardown hygiene is under test

	// The load-bearing assertion: the stopped renderer itself must be dead. The group
	// check below is vacuous without Setpgid (no such pgid exists → ESRCH), so this
	// per-pid check is what actually discriminates the fix — verified: with the group
	// kill disabled, the stopped renderer survives teardown in state T and this fails.
	require.Eventually(t, func() bool { return pidDeadOrZombie(stopped) },
		15*time.Second, 100*time.Millisecond,
		"SIGSTOPped renderer pid %d survived browser teardown — the process-group kill regressed", stopped)

	cmd := rec.recorded(t)
	requireTreeAndDirGone(t, cmd, userDataDir(t, cmd))
}

// pidDeadOrZombie reports pid as gone: fully reaped (ESRCH) or a zombie awaiting reap.
//
// Reason: after the group SIGKILL the renderer's parent chain is already dead, so the
// corpse waits for whatever PID 1 the test runs under; treating state Z as dead keeps
// the assertion independent of that environment's reaping latency.
func pidDeadOrZombie(pid int) bool {
	stat, err := os.ReadFile(filepath.Join("/proc", strconv.Itoa(pid), "stat"))
	if err != nil {
		return true // /proc entry gone: reaped
	}
	rest := string(stat[strings.LastIndexByte(string(stat), ')')+2:])
	fields := strings.Fields(rest)
	return len(fields) > 0 && fields[0] == "Z"
}
