//go:build !unix

package browserproc

import "os/exec"

// configureCmd is a no-op on non-unix platforms: syscall.SysProcAttr has no process-group
// fields there, and no supported deployment runs the media worker outside Linux
// containers (darwin is dev-only, also unix). Behavior falls back to chromedp's default
// browser-only kill.
func configureCmd(_ *exec.Cmd) {}
