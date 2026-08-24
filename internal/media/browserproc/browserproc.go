// Package browserproc hardens the teardown of headless-chromium subprocess trees.
//
// Reason (issue #136): chromedp launches chromium via exec.CommandContext, whose
// cancellation SIGKILLs only the browser process itself. Chromium's renderer/gpu/utility
// children normally self-exit when the browser dies, but a renderer wedged in untrusted
// artwork JS survives the browser's death, reparents to container PID 1 (the indexer
// binary, which does not reap), and keeps writing into the chromedp-runner user-data dir
// — so chromedp's post-exit os.RemoveAll fails ENOTEMPTY and the dir leaks too. On
// prod-01 this accumulated 433 chromium processes and 780 user-data dirs in 36 h.
//
// The fix: launch chromium in its own process group and redirect the command's context
// cancellation at the whole group, so the timeout/shutdown path kills the entire tree at
// once. Trade-off: chromium never gets a graceful shutdown — but it never did (the
// default cancel was already SIGKILL); this only widens the same signal to the children.
// Constraint: cmd.Cancel fires only when the context kills the browser; a browser that
// crashes on its own can still strand an already-wedged child, which is why the deploy
// runs the container with an init reaper as backstop (ff-deploy `init: true`).
package browserproc

import (
	"os/exec"

	"github.com/chromedp/chromedp"
)

// AllocatorOption returns the chromedp allocator option applying ConfigureCmd to the
// browser launch. It must be part of every chromedp allocator's option set.
//
// Constraint: chromedp keeps a single ModifyCmdFunc — a later ModifyCmdFunc in the same
// option slice replaces this one, so any wrapper (tests record the launched cmd this
// way) must call ConfigureCmd itself.
func AllocatorOption() chromedp.ExecAllocatorOption {
	return chromedp.ModifyCmdFunc(ConfigureCmd)
}

// ConfigureCmd places the browser in its own process group and points the command's
// context cancellation at the whole group (SIGKILL). On Linux it also re-applies
// chromedp's default parent-death signal, which setting ModifyCmdFunc otherwise
// discards. On non-unix platforms it is a no-op.
func ConfigureCmd(cmd *exec.Cmd) {
	configureCmd(cmd)
}
