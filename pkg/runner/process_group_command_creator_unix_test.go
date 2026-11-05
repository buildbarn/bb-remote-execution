//go:build darwin || freebsd || linux
// +build darwin freebsd linux

package runner_test

import (
	"bufio"
	"context"
	"io"
	"os"
	"os/exec"
	"syscall"
	"testing"
	"time"

	"github.com/buildbarn/bb-remote-execution/pkg/runner"
	"github.com/buildbarn/bb-storage/pkg/filesystem/path"
	"github.com/buildbarn/bb-storage/pkg/testutil"
	"github.com/stretchr/testify/require"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestProcessGroupCommandCreatorAttributes(t *testing.T) {
	for name, chroot := range map[string]bool{
		"Plain":  false,
		"Chroot": true,
	} {
		t.Run(name, func(t *testing.T) {
			attributes := &syscall.SysProcAttr{
				Credential: &syscall.Credential{
					Uid: 123,
					Gid: 456,
				},
				Noctty: true,
			}
			original := *attributes
			creator := runner.NewPlainCommandCreator(attributes)
			if chroot {
				var err error
				creator, err = runner.NewChrootedCommandCreator(attributes)
				require.NoError(t, err)
			}
			creator, err := runner.NewProcessGroupCommandCreator(creator)
			require.NoError(t, err)
			cmd, err := creator(context.Background(), []string{"/bin/sh"}, &path.RootBuilder, path.UNIXFormat.NewParser(""), "")
			require.NoError(t, err)
			require.Equal(t, original, *attributes)
			expected := original
			expected.Setpgid = true
			if chroot {
				expected.Chroot = "/"
			}
			require.Equal(t, expected, *cmd.SysProcAttr)
		})
	}
}

func TestProcessGroupCommandCreatorInvalidAttributes(t *testing.T) {
	for name, attributes := range map[string]*syscall.SysProcAttr{
		"Session":             {Setsid: true},
		"ControllingTerminal": {Setctty: true},
		"ExistingGroup":       {Pgid: 123},
	} {
		t.Run(name, func(t *testing.T) {
			creator, err := runner.NewProcessGroupCommandCreator(runner.NewPlainCommandCreator(attributes))
			require.NoError(t, err)
			_, err = creator(context.Background(), []string{"/bin/sh"}, &path.RootBuilder, path.UNIXFormat.NewParser(""), "")
			testutil.RequireEqualStatus(t, status.Error(codes.InvalidArgument, "Process group cancellation requires a new process group without Setsid, Setctty or a nonzero Pgid"), err)
		})
	}
}

func TestProcessGroupCommandCreatorError(t *testing.T) {
	expected := status.Error(codes.Internal, "Failed to create command")
	creator, err := runner.NewProcessGroupCommandCreator(func(context.Context, []string, *path.Builder, path.Parser, string) (*exec.Cmd, error) {
		return nil, expected
	})
	require.NoError(t, err)
	_, err = creator(context.Background(), nil, nil, nil, "")
	testutil.RequireEqualStatus(t, expected, err)
}

func TestProcessGroupCommandCreatorMissingGroup(t *testing.T) {
	creator, err := runner.NewProcessGroupCommandCreator(runner.NewPlainCommandCreator(nil))
	require.NoError(t, err)
	cmd, err := creator(context.Background(), []string{"/bin/sh"}, &path.RootBuilder, path.UNIXFormat.NewParser(""), "")
	require.NoError(t, err)
	// No process is started. A negative PID makes Kill receive a
	// positive, invalid PID, so this cannot accidentally target a live
	// process group.
	cmd.Process = &os.Process{Pid: -1 << 30}
	require.Equal(t, os.ErrProcessDone, cmd.Cancel())
}

func TestProcessGroupCommandCreatorCancellation(t *testing.T) {
	creator, err := runner.NewProcessGroupCommandCreator(runner.NewPlainCommandCreator(nil))
	require.NoError(t, err)
	// A second command remains running while the first
	// command is canceled.
	otherContext, cancelOther := context.WithCancel(context.Background())
	defer cancelOther()
	_, otherDone := startProcessGroupFixture(t, creator, otherContext)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	output, done := startProcessGroupFixture(t, creator, ctx)
	cancel()
	select {
	case err := <-done:
		require.Error(t, err)
	case <-time.After(10 * time.Second):
		t.Fatal("command did not stop on cancellation")
	}
	// The ordinary grandchild inherits stdout. EOF proves
	// that it also closed the pipe, without relying on PID
	// polling or zombie reaping.
	require.NoError(t, output.SetReadDeadline(time.Now().Add(5*time.Second)))
	_, err = output.Read(make([]byte, 1))
	require.Equal(t, io.EOF, err)
	select {
	case err := <-otherDone:
		t.Fatalf("unrelated command exited: %v", err)
	default:
	}
	cancelOther()
	select {
	case err := <-otherDone:
		require.Error(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("unrelated command did not stop")
	}
}

// The grandchild inherits ignored SIGTERM and signals readiness through
// stdout. Its ancestors wait for their children, keeping it in the
// command's group. The bounded sleep also expires independently if the
// cancellation under test fails.
func startProcessGroupFixture(t *testing.T, creator runner.CommandCreator, ctx context.Context) (*os.File, <-chan error) {
	t.Helper()
	ctx, cancel := context.WithCancel(ctx)
	t.Cleanup(cancel)
	cmd, err := creator(ctx, []string{"/bin/sh", "-c", `trap '' TERM
/bin/sh -c '
  /bin/sh -c "echo ready; exec sleep 30" &
  wait
' &
wait`}, &path.RootBuilder, path.UNIXFormat.NewParser(""), "")
	require.NoError(t, err)
	output, writer, err := os.Pipe()
	require.NoError(t, err)
	t.Cleanup(func() {
		output.Close()
		writer.Close()
	})
	cmd.Stdout = writer
	require.NoError(t, cmd.Start())
	done := make(chan error, 1)
	waited := make(chan struct{})
	go func() {
		done <- cmd.Wait()
		close(waited)
	}()
	t.Cleanup(func() {
		cancel()
		select {
		case <-waited:
		case <-time.After(5 * time.Second):
			t.Error("command cleanup timed out")
		}
	})
	require.NoError(t, writer.Close())
	require.NoError(t, output.SetReadDeadline(time.Now().Add(5*time.Second)))
	line, err := bufio.NewReader(output).ReadString('\n')
	require.NoError(t, err)
	require.Equal(t, "ready\n", line)
	return output, done
}
