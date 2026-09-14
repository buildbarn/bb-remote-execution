package runner_test

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/bazelbuild/rules_go/go/runfiles"
	"github.com/buildbarn/bb-remote-execution/pkg/runner"
	"github.com/buildbarn/bb-storage/pkg/filesystem/path"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func fakeWorkerExecutable(t *testing.T) string {
	t.Helper()
	executable, err := runfiles.Rlocation(os.Getenv("FAKE_WORKER_BINARY"))
	require.NoError(t, err)
	executable, err = filepath.Abs(executable)
	require.NoError(t, err)
	return executable
}

func newPersistentWorkerProcessCommand(t *testing.T, mode string) (context.Context, *exec.Cmd) {
	t.Helper()
	executable := fakeWorkerExecutable(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	t.Cleanup(cancel)
	directory, scopeWalker := path.EmptyBuilder.Join(path.VoidScopeWalker)
	require.NoError(t, path.Resolve(path.LocalFormat.NewParser(t.TempDir()), scopeWalker))
	command, err := runner.NewPlainCommandCreator(&syscall.SysProcAttr{})(
		ctx,
		[]string{executable, "--mode=" + mode},
		directory,
		path.UNIXFormat.NewParser("."),
		"",
	)
	require.NoError(t, err)
	command.Env = []string{}
	return ctx, command
}

func startPersistentWorkerProcess(t *testing.T, command *exec.Cmd) *runner.PersistentWorkerProcess {
	t.Helper()
	process, err := runner.StartPersistentWorkerProcess(command, 1024*1024)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, process.Close())
	})
	return process
}

func TestPersistentWorkerProcessReuse(t *testing.T) {
	ctx, command := newPersistentWorkerProcessCommand(t, "echo")
	process := startPersistentWorkerProcess(t, command)
	var workerID uuid.UUID
	for requestIndex, request := range [][]byte{{0xff, 0x00, 0x80}, {}, []byte("another request")} {
		actionContext, cancelAction := context.WithCancel(ctx)
		response, err := process.Execute(actionContext, request)
		cancelAction()
		require.NoError(t, err)
		responseID, _, found := strings.Cut(string(response), ":")
		require.True(t, found)
		currentWorkerID, err := uuid.Parse(responseID)
		require.NoError(t, err)
		if requestIndex == 0 {
			workerID = currentWorkerID
		}
		require.Equal(t, workerID, currentWorkerID)
	}
	require.NoError(t, process.Close())
	require.NoError(t, process.Close())
	require.NotNil(t, command.ProcessState)
	response, err := process.Execute(ctx, nil)
	require.Error(t, err)
	require.Nil(t, response)
}

func TestPersistentWorkerProcessStartFailure(t *testing.T) {
	command := exec.Command(filepath.Join(t.TempDir(), "missing-compiler"))
	process, err := runner.StartPersistentWorkerProcess(command, 1024)
	require.Error(t, err)
	require.Nil(t, process)
	_, err = command.Stdin.(*os.File).Stat()
	require.Error(t, err)
	_, err = command.Stdout.(*os.File).Stat()
	require.Error(t, err)
}

func TestPersistentWorkerProcessPreCancelledRequest(t *testing.T) {
	ctx, command := newPersistentWorkerProcessCommand(t, "echo")
	process := startPersistentWorkerProcess(t, command)
	actionContext, cancelAction := context.WithCancel(ctx)
	cancelAction()
	response, err := process.Execute(actionContext, []byte("cancelled"))
	require.Error(t, err)
	require.Nil(t, response)
	response, err = process.Execute(ctx, []byte("first request"))
	require.NoError(t, err)
	responseID, payload, found := strings.Cut(string(response), ":")
	require.True(t, found)
	_, err = uuid.Parse(responseID)
	require.NoError(t, err)
	require.Equal(t, "1:first request", payload)
}

func TestPersistentWorkerProcessFailure(t *testing.T) {
	for _, mode := range []string{"exit", "truncated", "invalid-length", "oversized"} {
		t.Run(mode, func(t *testing.T) {
			ctx, command := newPersistentWorkerProcessCommand(t, mode)
			process := startPersistentWorkerProcess(t, command)
			response, err := process.Execute(ctx, nil)
			require.Error(t, err)
			if mode == "exit" {
				var exitError *exec.ExitError
				require.ErrorAs(t, err, &exitError)
				require.Equal(t, 23, exitError.ExitCode())
			}
			require.Nil(t, response)
			require.NoError(t, ctx.Err())
			require.NotNil(t, command.ProcessState)
			response, err = process.Execute(ctx, nil)
			require.Error(t, err)
			require.Nil(t, response)
		})
	}
}

func TestPersistentWorkerProcessIdleExit(t *testing.T) {
	ctx, command := newPersistentWorkerProcessCommand(t, "exit-idle")
	process := startPersistentWorkerProcess(t, command)
	select {
	case <-process.Done():
	case <-ctx.Done():
		t.Fatal("compiler did not exit")
	}
	require.NoError(t, process.Wait())
	require.NotNil(t, command.ProcessState)
	response, err := process.Execute(ctx, nil)
	require.Error(t, err)
	require.Nil(t, response)
}

func TestPersistentWorkerProcessLifetimeCancellation(t *testing.T) {
	ctx, command := newPersistentWorkerProcessCommand(t, "echo")
	lifetimeContext, cancelLifetime := context.WithCancel(ctx)
	defer cancelLifetime()
	lifetimeCommand := exec.CommandContext(lifetimeContext, command.Path, command.Args[1:]...)
	lifetimeCommand.Env = command.Env
	lifetimeCommand.Dir = command.Dir
	process := startPersistentWorkerProcess(t, lifetimeCommand)
	_, err := process.Execute(ctx, nil)
	require.NoError(t, err)
	cancelLifetime()
	require.NoError(t, process.Close())
	select {
	case <-process.Done():
	case <-ctx.Done():
		t.Fatal("compiler did not stop")
	}
	require.Error(t, process.Wait())
	require.NotNil(t, lifetimeCommand.ProcessState)
}
