//go:build unix && !linux

package browserproc

import "syscall"

// setPdeathsig is a no-op outside Linux: PDEATHSIG is a Linux-only prctl, and chromedp's
// own defaults set nothing on these platforms either (allocate_other.go).
func setPdeathsig(_ *syscall.SysProcAttr) {}
