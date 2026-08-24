//go:build unix

package browserproc_test

import (
	"bufio"
	"context"
	"errors"
	"os/exec"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/feral-file/ff-indexer-v2/internal/media/browserproc"
)

// groupGone reports whether no process (zombies included) remains in pgid's group.
func groupGone(pgid int) bool {
	return errors.Is(syscall.Kill(-pgid, 0), syscall.ESRCH)
}

// waitGroupGone polls for the whole group to disappear; direct members die with the
// SIGKILL, but a grandchild's zombie lingers until launchd/init reaps it.
func waitGroupGone(t *testing.T, pgid int) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if groupGone(pgid) {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("process group %d still has members after teardown", pgid)
}

// TestConfigureCmd_ContextCancelKillsWholeTree is the incident scenario in miniature:
// the direct child (the "browser") spawns a long-lived grandchild (the "wedged
// renderer"), the context is canceled (the probe timeout), and the whole tree must die
// — with exec's default cancel, only the direct child would.
func TestConfigureCmd_ContextCancelKillsWholeTree(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// "echo started" after the background spawn so the test can synchronize on the
	// grandchild existing before it kills anything.
	cmd := exec.CommandContext(ctx, "sh", "-c", "sleep 300 & echo started; wait")
	browserproc.ConfigureCmd(cmd)

	stdout, err := cmd.StdoutPipe()
	require.NoError(t, err)
	require.NoError(t, cmd.Start())

	line, err := bufio.NewReader(stdout).ReadString('\n')
	require.NoError(t, err)
	require.Contains(t, line, "started")

	pgid := cmd.Process.Pid // Setpgid(0) makes the child its own group leader
	require.NoError(t, syscall.Kill(-pgid, 0), "group should be alive before cancel")

	cancel()
	// Wait returns the context error for a context-killed process; the important part is
	// that it returns at all and the group is then empty.
	_ = cmd.Wait()
	waitGroupGone(t, pgid)
}

// TestConfigureCmd_CancelAfterNaturalExitIsClean: a group that already fully exited must
// map ESRCH to os/exec's "process done" sentinel, not surface a spurious error from Wait.
func TestConfigureCmd_CancelAfterNaturalExitIsClean(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cmd := exec.CommandContext(ctx, "true")
	browserproc.ConfigureCmd(cmd)
	require.NoError(t, cmd.Start())
	require.NoError(t, cmd.Wait())

	// The group leader is reaped and had no children: canceling now exercises the
	// ESRCH path inside the Cancel func.
	cancel()
	waitGroupGone(t, cmd.Process.Pid)
}

// TestConfigureCmd_PreservesExistingSysProcAttr guards against clobbering attrs another
// ModifyCmdFunc-style caller may have set before us.
func TestConfigureCmd_PreservesExistingSysProcAttr(t *testing.T) {
	cmd := exec.Command("true")
	cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: false}
	browserproc.ConfigureCmd(cmd)
	assert.True(t, cmd.SysProcAttr.Setpgid)
	assert.NotNil(t, cmd.Cancel)
}
