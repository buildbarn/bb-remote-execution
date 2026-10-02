package runner_test

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/buildbarn/bb-remote-execution/internal/mock"
	"github.com/buildbarn/bb-remote-execution/pkg/cleaner"
	runner_pb "github.com/buildbarn/bb-remote-execution/pkg/proto/runner"
	"github.com/buildbarn/bb-remote-execution/pkg/runner"
	"github.com/buildbarn/bb-storage/pkg/filesystem"
	"github.com/buildbarn/bb-storage/pkg/filesystem/path"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"go.uber.org/mock/gomock"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
)

func newPersistentRunner(t *testing.T, ctx context.Context, commandCreator runner.CommandCreator) (*runner.PersistentRunner, string) {
	t.Helper()
	return newPersistentRunnerWithCleaner(t, ctx, commandCreator, nil)
}

func newPersistentRunnerWithCleaner(t *testing.T, ctx context.Context, commandCreator runner.CommandCreator, idleInvoker *cleaner.IdleInvoker) (*runner.PersistentRunner, string) {
	t.Helper()
	directoryPath := t.TempDir()
	directory, err := filesystem.NewLocalDirectory(path.LocalFormat.NewParser(directoryPath))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, directory.Close()) })
	directoryBuilder, scopeWalker := path.EmptyBuilder.Join(path.VoidScopeWalker)
	require.NoError(t, path.Resolve(path.LocalFormat.NewParser(directoryPath), scopeWalker))
	server := runner.NewPersistentRunner(ctx, directory, directoryBuilder, commandCreator, false, 1024, idleInvoker)
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
		require.Len(t, status.Convert(err).Details(), 1)
		require.IsType(t, &runner_pb.CreateSessionFailure{}, status.Convert(err).Details()[0])
		require.Nil(t, response)
	}
	for _, sessionID := range []string{"", "unknown"} {
		response, err := server.ExecuteInPersistentWorker(ctx, &runner_pb.ExecuteInPersistentWorkerRequest{SessionId: sessionID})
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
		response, err := server.ExecuteInPersistentWorker(actionContext, &runner_pb.ExecuteInPersistentWorkerRequest{
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
	_, err := server.ExecuteInPersistentWorker(ctx, &runner_pb.ExecuteInPersistentWorkerRequest{SessionId: sessionID})
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
				response, err := server.ExecuteInPersistentWorker(ctx, &runner_pb.ExecuteInPersistentWorkerRequest{SessionId: sessionID})
				require.Error(t, err)
				require.Nil(t, response)
			}
			_, err := server.CloseSession(ctx, &runner_pb.SessionRequest{SessionId: sessionID})
			require.NoError(t, err)
		})
	}
}

func TestPersistentRunnerExitDiagnostics(t *testing.T) {
	logFile, err := os.CreateTemp(t.TempDir(), "runner-log")
	require.NoError(t, err)
	previousOutput := log.Writer()
	log.SetOutput(logFile)
	t.Cleanup(func() {
		log.SetOutput(previousOutput)
		require.NoError(t, logFile.Close())
	})
	for _, mode := range []string{"exit", "exit-startup"} {
		t.Run(mode, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			server, _ := newPersistentRunner(t, ctx, runner.NewPlainCommandCreator(&syscall.SysProcAttr{}))
			created, err := server.CreateSession(ctx, &runner_pb.CreateSessionRequest{
				Arguments: []string{fakeWorkerExecutable(t), "--mode=" + mode}, ProcessStderrPath: "stderr",
			})
			if err != nil {
				require.Len(t, status.Convert(err).Details(), 1)
				require.IsType(t, &runner_pb.CreateSessionFailure{}, status.Convert(err).Details()[0])
			} else {
				_, err = server.ExecuteInPersistentWorker(ctx, &runner_pb.ExecuteInPersistentWorkerRequest{SessionId: created.SessionId})
				require.Error(t, err)
			}
			require.Eventually(t, func() bool {
				contents, err := os.ReadFile(logFile.Name())
				require.NoError(t, err)
				return strings.Contains(string(contents), fmt.Sprintf("stderr_tail=%q", strings.Repeat("x", 8192-len(mode)-1)+mode+"\n"))
			}, 5*time.Second, time.Millisecond)
			require.NoError(t, server.Close())
			contents, err := os.ReadFile(logFile.Name())
			require.NoError(t, err)
			require.Contains(t, string(contents), `exit_status="exit status 23"`)
			require.NotContains(t, string(contents), "discarded prefix")
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
				_, err := server.ExecuteInPersistentWorker(actionContext, &runner_pb.ExecuteInPersistentWorkerRequest{SessionId: waiting.SessionId})
				executionDone <- err
			}()
			require.Eventually(t, func() bool {
				info, err := os.Stat(filepath.Join(directoryPath, "waiting.stderr"))
				return err == nil && info.Size() > 0
			}, 5*time.Second, time.Millisecond)
			_, err = server.ExecuteInPersistentWorker(ctx, &runner_pb.ExecuteInPersistentWorkerRequest{SessionId: waiting.SessionId})
			require.Error(t, err)
			_, err = server.ExecuteInPersistentWorker(ctx, &runner_pb.ExecuteInPersistentWorkerRequest{SessionId: otherSessionID})
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
			_, err = server.ExecuteInPersistentWorker(ctx, &runner_pb.ExecuteInPersistentWorkerRequest{SessionId: waiting.SessionId})
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

func TestPersistentRunnerCleanerLifetime(test *testing.T) {
	for _, finish := range []string{"CloseSession", "Exit", "Shutdown"} {
		test.Run(finish, func(test *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			var cleanings atomic.Int32
			idleInvoker := cleaner.NewIdleInvoker(func(ctx context.Context) error {
				require.NoError(test, ctx.Err())
				cleanings.Add(1)
				return nil
			})
			server, _ := newPersistentRunnerWithCleaner(test, ctx, runner.NewPlainCommandCreator(&syscall.SysProcAttr{}), idleInvoker)
			first := createPersistentRunnerSession(test, server, ctx, "echo")
			mode := "echo"
			if finish == "Exit" {
				mode = "exit"
			}
			second := createPersistentRunnerSession(test, server, ctx, mode)
			base := mock.NewMockRunnerServer(gomock.NewController(test))
			ordinary := runner.NewCleanRunner(base, idleInvoker)
			base.EXPECT().Run(ctx, gomock.Any()).Return(&runner_pb.RunResponse{}, nil)
			_, err := ordinary.Run(ctx, &runner_pb.RunRequest{})
			require.NoError(test, err)
			base.EXPECT().CheckReadiness(ctx, gomock.Any()).Return(&emptypb.Empty{}, nil)
			_, err = ordinary.CheckReadiness(ctx, &runner_pb.CheckReadinessRequest{})
			require.NoError(test, err)
			_, err = server.CheckReadiness(ctx, &runner_pb.CheckReadinessRequest{Path: "."})
			require.NoError(test, err)
			require.Equal(test, int32(1), cleanings.Load())
			_, err = server.CloseSession(ctx, &runner_pb.SessionRequest{SessionId: first})
			require.NoError(test, err)
			require.Equal(test, int32(1), cleanings.Load())
			switch finish {
			case "CloseSession":
				_, err = server.CloseSession(ctx, &runner_pb.SessionRequest{SessionId: second})
				require.NoError(test, err)
			case "Exit":
				_, err = server.ExecuteInPersistentWorker(ctx, &runner_pb.ExecuteInPersistentWorkerRequest{SessionId: second})
				require.Error(test, err)
				require.Eventually(test, func() bool { return cleanings.Load() == 2 }, time.Second, time.Millisecond)
			case "Shutdown":
				require.NoError(test, server.Close())
			}
			require.Equal(test, int32(2), cleanings.Load())
		})
	}
}

func TestPersistentRunnerCleanerCanceledCreation(test *testing.T) {
	var cleanings atomic.Int32
	idleInvoker := cleaner.NewIdleInvoker(func(ctx context.Context) error {
		require.NoError(test, ctx.Err())
		cleanings.Add(1)
		return nil
	})
	started := make(chan struct{})
	server, _ := newPersistentRunnerWithCleaner(test, context.Background(), func(ctx context.Context, arguments []string, inputRoot *path.Builder, workingDirectory path.Parser, pathVariable string) (*exec.Cmd, error) {
		close(started)
		<-ctx.Done()
		return nil, ctx.Err()
	}, idleInvoker)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	finished := make(chan error, 1)
	go func() {
		_, err := server.CreateSession(ctx, &runner_pb.CreateSessionRequest{Arguments: []string{"compiler"}})
		finished <- err
	}()
	<-started
	cancel()
	require.Error(test, <-finished)
	require.Equal(test, int32(2), cleanings.Load())
}

func TestPersistentRunnerCleanerAcquireFailure(test *testing.T) {
	idleInvoker := cleaner.NewIdleInvoker(func(ctx context.Context) error {
		return status.Error(codes.Unavailable, "Cleaning failed")
	})
	server, _ := newPersistentRunnerWithCleaner(test, context.Background(), runner.NewPlainCommandCreator(&syscall.SysProcAttr{}), idleInvoker)
	_, err := server.CreateSession(context.Background(), &runner_pb.CreateSessionRequest{Arguments: []string{"compiler"}})
	require.Error(test, err)
	_, err = server.CheckReadiness(context.Background(), &runner_pb.CheckReadinessRequest{})
	require.Error(test, err)
}
