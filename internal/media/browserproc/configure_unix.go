//go:build unix

package browserproc

import (
	"errors"
	"os"
	"os/exec"
	"syscall"
)

func configureCmd(cmd *exec.Cmd) {
	if cmd.SysProcAttr == nil {
		cmd.SysProcAttr = new(syscall.SysProcAttr)
	}
	// Setpgid with Pgid 0: the browser becomes leader of a fresh process group (pgid ==
	// its pid), and every chromium child it spawns inherits that group — the unit the
	// cancel below can address as one target.
	cmd.SysProcAttr.Setpgid = true
	setPdeathsig(cmd.SysProcAttr)
	cmd.Cancel = func() error {
		return killGroup(cmd)
	}
}

// killGroup SIGKILLs the browser's entire process group.
//
// Reason: SIGKILL rather than a graceful signal because this replaces exec's default
// cancel, which was already Process.Kill() — the browser has no graceful path here, and
// a wedged renderer would ignore anything catchable anyway. ESRCH (group already fully
// exited) maps to os.ErrProcessDone so exec.Cmd.Wait treats it as a clean kill instead
// of surfacing a spurious error.
func killGroup(cmd *exec.Cmd) error {
	p := cmd.Process
	if p == nil || p.Pid <= 0 {
		return os.ErrProcessDone
	}
	err := syscall.Kill(-p.Pid, syscall.SIGKILL)
	if errors.Is(err, syscall.ESRCH) {
		return os.ErrProcessDone
	}
	return err
}
