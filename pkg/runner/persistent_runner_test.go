package runner_test

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	runner_pb "github.com/buildbarn/bb-remote-execution/pkg/proto/runner"
	"github.com/buildbarn/bb-remote-execution/pkg/runner"
	"github.com/buildbarn/bb-storage/pkg/filesystem"
	"github.com/buildbarn/bb-storage/pkg/filesystem/path"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func newPersistentRunner(t *testing.T, ctx context.Context, commandCreator runner.CommandCreator) (*runner.PersistentRunner, string) {
	t.Helper()
	directoryPath := t.TempDir()
	directory, err := filesystem.NewLocalDirectory(path.LocalFormat.NewParser(directoryPath))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, directory.Close()) })
	directoryBuilder, scopeWalker := path.EmptyBuilder.Join(path.VoidScopeWalker)
	require.NoError(t, path.Resolve(path.LocalFormat.NewParser(directoryPath), scopeWalker))
	server := runner.NewPersistentRunner(ctx, directory, directoryBuilder, commandCreator, false, 1024)
	t.Cleanup(func() { require.NoError(t, server.Close()) })
	return server, directoryPath
}

func createPersistentRunnerSession(t *testing.T, server *runner.PersistentRunner, ctx context.Context, mode string) string {
	t.Helper()
	response, err := server.CreateSession(ctx, &runner_pb.CreateSessionRequest{
		Arguments:         []string{fakeWorkerExecutable(t), "--mode=" + mode},
		ProcessStderrPath: uuid.NewString() + ".stderr",
	})
	require.NoError(t, err)
	_, err = uuid.Parse(response.SessionId)
	require.NoError(t, err)
	return response.SessionId
}

func TestPersistentRunnerReadinessAndInvalidRequests(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	server, _ := newPersistentRunner(t, ctx, runner.NewPlainCommandCreator(&syscall.SysProcAttr{}))
	_, err := server.CheckReadiness(ctx, &runner_pb.CheckReadinessRequest{Path: "."})
	require.NoError(t, err)
	_, err = server.CheckReadiness(ctx, &runner_pb.CheckReadinessRequest{Path: "missing"})
	require.Error(t, err)
	for _, request := range []*runner_pb.CreateSessionRequest{
		{},
		{Arguments: []string{fakeWorkerExecutable(t)}, ProcessStderrPath: "missing/stderr"},
		{Arguments: []string{"./missing"}, ProcessStderrPath: "stderr"},
	} {
		response, err := server.CreateSession(ctx, request)
		require.Error(t, err)
		require.Nil(t, response)
	}
	for _, sessionID := range []string{"", "unknown"} {
		response, err := server.ExecuteSession(ctx, &runner_pb.ExecuteSessionRequest{SessionId: sessionID})
		require.Error(t, err)
		require.Nil(t, response)
	}
	_, err = server.CloseSession(ctx, &runner_pb.SessionRequest{})
	require.Error(t, err)
	_, err = server.CloseSession(ctx, &runner_pb.SessionRequest{SessionId: "unknown"})
	require.NoError(t, err)
}

func TestPersistentRunnerReuse(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	server, _ := newPersistentRunner(t, ctx, runner.NewPlainCommandCreator(&syscall.SysProcAttr{}))
	createContext, cancelCreate := context.WithCancel(ctx)
	sessionID := createPersistentRunnerSession(t, server, createContext, "echo")
	cancelCreate()
	var workerID uuid.UUID
	for requestIndex := range 3 {
		actionContext, cancelAction := context.WithCancel(ctx)
		response, err := server.ExecuteSession(actionContext, &runner_pb.ExecuteSessionRequest{
			SessionId: sessionID, SerializedWorkRequest: []byte{0xff, 0x00, 0x80},
		})
		cancelAction()
		require.NoError(t, err)
		parts := strings.SplitN(string(response.SerializedWorkResponse), ":", 3)
		require.Len(t, parts, 3)
		currentWorkerID, err := uuid.Parse(parts[0])
		require.NoError(t, err)
		if requestIndex == 0 {
			workerID = currentWorkerID
		}
		require.Equal(t, workerID, currentWorkerID)
		require.Equal(t, []byte{0xff, 0x00, 0x80}, []byte(parts[2]))
	}
	for range 2 {
		_, err := server.CloseSession(ctx, &runner_pb.SessionRequest{SessionId: sessionID})
		require.NoError(t, err)
	}
	_, err := server.ExecuteSession(ctx, &runner_pb.ExecuteSessionRequest{SessionId: sessionID})
	require.Error(t, err)
}

func TestPersistentRunnerFailedSession(t *testing.T) {
	for _, mode := range []string{"exit", "truncated", "invalid-length", "oversized"} {
		t.Run(mode, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			server, _ := newPersistentRunner(t, ctx, runner.NewPlainCommandCreator(&syscall.SysProcAttr{}))
			sessionID := createPersistentRunnerSession(t, server, ctx, mode)
			for range 2 {
				response, err := server.ExecuteSession(ctx, &runner_pb.ExecuteSessionRequest{SessionId: sessionID})
				require.Error(t, err)
				require.Nil(t, response)
			}
			_, err := server.CloseSession(ctx, &runner_pb.SessionRequest{SessionId: sessionID})
			require.NoError(t, err)
		})
	}
}

func TestPersistentRunnerConcurrentSessions(t *testing.T) {
	for _, finish := range []string{"CloseSession", "CancelAction", "Shutdown"} {
		t.Run(finish, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			server, directoryPath := newPersistentRunner(t, ctx, runner.NewPlainCommandCreator(&syscall.SysProcAttr{}))
			waiting, err := server.CreateSession(ctx, &runner_pb.CreateSessionRequest{
				Arguments: []string{fakeWorkerExecutable(t), "--mode=wait"}, ProcessStderrPath: "waiting.stderr",
			})
			require.NoError(t, err)
			otherSessionID := createPersistentRunnerSession(t, server, ctx, "echo")
			actionContext, cancelAction := context.WithCancel(ctx)
			defer cancelAction()
			executionDone := make(chan error, 1)
			go func() {
				_, err := server.ExecuteSession(actionContext, &runner_pb.ExecuteSessionRequest{SessionId: waiting.SessionId})
				executionDone <- err
			}()
			require.Eventually(t, func() bool {
				info, err := os.Stat(filepath.Join(directoryPath, "waiting.stderr"))
				return err == nil && info.Size() > 0
			}, 5*time.Second, time.Millisecond)
			_, err = server.ExecuteSession(ctx, &runner_pb.ExecuteSessionRequest{SessionId: waiting.SessionId})
			require.Error(t, err)
			_, err = server.ExecuteSession(ctx, &runner_pb.ExecuteSessionRequest{SessionId: otherSessionID})
			require.NoError(t, err)
			switch finish {
			case "CloseSession":
				closeResults := make(chan error, 2)
				for range 2 {
					go func() {
						_, err := server.CloseSession(ctx, &runner_pb.SessionRequest{SessionId: waiting.SessionId})
						closeResults <- err
					}()
				}
				for range 2 {
					require.NoError(t, <-closeResults)
				}
			case "CancelAction":
				cancelAction()
			case "Shutdown":
				require.NoError(t, server.Close())
			}
			select {
			case err := <-executionDone:
				require.Error(t, err)
			case <-ctx.Done():
				t.Fatal("Execution did not finish")
			}
			_, err = server.ExecuteSession(ctx, &runner_pb.ExecuteSessionRequest{SessionId: waiting.SessionId})
			require.Error(t, err)
		})
	}
}

func TestPersistentRunnerShutdownDuringCreation(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	started := make(chan struct{})
	server, _ := newPersistentRunner(t, ctx, func(ctx context.Context, arguments []string, inputRoot *path.Builder, workingDirectory path.Parser, pathVariable string) (*exec.Cmd, error) {
		close(started)
		<-ctx.Done()
		return nil, ctx.Err()
	})
	creationDone := make(chan error, 1)
	go func() {
		_, err := server.CreateSession(ctx, &runner_pb.CreateSessionRequest{Arguments: []string{"compiler"}})
		creationDone <- err
	}()
	select {
	case <-started:
	case <-ctx.Done():
		t.Fatal("Creation did not start")
	}
	var shutdowns sync.WaitGroup
	for range 2 {
		shutdowns.Go(func() { server.Close() })
	}
	shutdowns.Wait()
	require.Error(t, <-creationDone)
	_, err := server.CheckReadiness(ctx, &runner_pb.CheckReadinessRequest{})
	require.Error(t, err)
	_, err = server.CreateSession(ctx, &runner_pb.CreateSessionRequest{Arguments: []string{"compiler"}})
	require.Error(t, err)
}
