package main

import (
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/bazelbuild/rules_go/go/runfiles"
	runner_pb "github.com/buildbarn/bb-remote-execution/pkg/proto/runner"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

func TestPersistentRunnerBinary(test *testing.T) {
	executable := func(name string) string {
		test.Helper()
		pathname, err := runfiles.Rlocation(os.Getenv(name))
		require.NoError(test, err)
		pathname, err = filepath.Abs(pathname)
		require.NoError(test, err)
		return pathname
	}
	directory := test.TempDir()
	temporaryDirectory := test.TempDir()
	socketDirectory, err := os.MkdirTemp("", "bb-runner-")
	require.NoError(test, err)
	test.Cleanup(func() { require.NoError(test, os.RemoveAll(socketDirectory)) })
	socketPath := filepath.Join(socketDirectory, "grpc")
	configuration, err := json.Marshal(map[string]any{
		"buildDirectoryPath": directory,
		"grpcServers": []any{map[string]any{
			"listenPaths":          []string{socketPath},
			"authenticationPolicy": map[string]any{"allow": map[string]any{}},
		}},
		"persistentWorkers":            map[string]any{"maximumWorkResponseSizeBytes": 1024},
		"setTmpdirEnvironmentVariable": true,
		"cleanTemporaryDirectories":    []string{temporaryDirectory},
		"readinessCheckingPathnames":   []string{directory},
	})
	require.NoError(test, err)
	configurationPath := filepath.Join(directory, "runner.json")
	require.NoError(test, os.WriteFile(configurationPath, configuration, 0o600))
	logFile, err := os.Create(filepath.Join(directory, "runner.log"))
	require.NoError(test, err)
	test.Cleanup(func() { require.NoError(test, logFile.Close()) })
	command := exec.Command(executable("RUNNER_BINARY"), configurationPath)
	command.Stdout = logFile
	command.Stderr = logFile
	require.NoError(test, command.Start())
	done := make(chan struct{})
	var waitError error
	go func() {
		waitError = command.Wait()
		close(done)
	}()
	test.Cleanup(func() {
		command.Process.Signal(syscall.SIGTERM)
		select {
		case <-done:
		case <-time.After(10 * time.Second):
			command.Process.Kill()
			<-done
			test.Error("Runner shutdown timed out")
		}
		if test.Failed() {
			contents, err := os.ReadFile(logFile.Name())
			require.NoError(test, err)
			test.Log(string(contents))
		}
	})
	connection, err := grpc.NewClient("unix://"+socketPath, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(test, err)
	defer connection.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	ordinary := runner_pb.NewRunnerClient(connection)
	persistent := runner_pb.NewPersistentRunnerClient(connection)
	_, err = ordinary.CheckReadiness(ctx, &runner_pb.CheckReadinessRequest{Path: "."}, grpc.WaitForReady(true))
	require.NoError(test, err)
	_, err = persistent.CreateSession(ctx, &runner_pb.CreateSessionRequest{
		Arguments: []string{"./missing-compiler"}, ProcessStderrPath: "failed.stderr",
	})
	require.Error(test, err)
	require.Len(test, status.Convert(err).Details(), 1)
	require.IsType(test, &runner_pb.CreateSessionFailure{}, status.Convert(err).Details()[0])
	createSession := func(stderrPath string) string {
		test.Helper()
		response, err := persistent.CreateSession(ctx, &runner_pb.CreateSessionRequest{
			Arguments: []string{executable("FAKE_WORKER_BINARY"), "--persistent_worker"}, ProcessStderrPath: stderrPath,
		})
		require.NoError(test, err)
		return response.SessionId
	}
	sessionID := createSession("session.stderr")
	marker := filepath.Join(temporaryDirectory, "retained")
	require.NoError(test, os.WriteFile(marker, nil, 0o600))
	var workerID uuid.UUID
	for requestIndex := range 2 {
		response, err := persistent.ExecuteInPersistentWorker(ctx, &runner_pb.ExecuteInPersistentWorkerRequest{
			SessionId: sessionID, SerializedWorkRequest: []byte("request"),
		})
		require.NoError(test, err)
		identity, _, found := strings.Cut(string(response.SerializedWorkResponse), ":")
		require.True(test, found)
		currentWorkerID, err := uuid.Parse(identity)
		require.NoError(test, err)
		if requestIndex == 0 {
			workerID = currentWorkerID
		}
		require.Equal(test, workerID, currentWorkerID)
		_, err = ordinary.CheckReadiness(ctx, &runner_pb.CheckReadinessRequest{Path: "."})
		require.NoError(test, err)
		_, err = persistent.CheckReadiness(ctx, &runner_pb.CheckReadinessRequest{Path: "."})
		require.NoError(test, err)
		require.FileExists(test, marker)
	}
	result, err := ordinary.Run(ctx, &runner_pb.RunRequest{
		Arguments: []string{executable("FAKE_WORKER_BINARY"), "--mode=exit-idle"}, StdoutPath: "ordinary.stdout", StderrPath: "ordinary.stderr",
	})
	require.NoError(test, err)
	require.Zero(test, result.ExitCode)
	require.FileExists(test, marker)
	_, err = persistent.CloseSession(ctx, &runner_pb.SessionRequest{SessionId: sessionID})
	require.NoError(test, err)
	require.NoFileExists(test, marker)
	createSession("shutdown.stderr")
	require.NoError(test, os.WriteFile(marker, nil, 0o600))
	require.NoError(test, command.Process.Signal(syscall.SIGTERM))
	select {
	case <-done:
	case <-ctx.Done():
		test.Fatal(ctx.Err())
	}
	var exitError *exec.ExitError
	require.ErrorAs(test, waitError, &exitError)
	require.NoFileExists(test, marker)
}
