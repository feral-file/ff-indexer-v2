//go:build linux

package browserproc

import "syscall"

// setPdeathsig re-applies chromedp's default: kill the browser if the Go process dies.
//
// Reason: chromedp sets this in allocateCmdOptions, but supplying a ModifyCmdFunc
// replaces that default entirely — dropping it would trade one leak (children outliving
// the browser) for another (browsers outliving a crashed worker). Note it guards the
// direct child only, and only against our death; the group kill in configureCmd is what
// covers the children on normal teardown.
func setPdeathsig(attr *syscall.SysProcAttr) {
	attr.Pdeathsig = syscall.SIGKILL
}
