package builder_test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"net"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/bazelbuild/rules_go/go/runfiles"
	"github.com/buildbarn/bb-remote-execution/pkg/builder"
	runner_pb "github.com/buildbarn/bb-remote-execution/pkg/proto/runner"
	"github.com/buildbarn/bb-remote-execution/pkg/runner"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/filesystem"
	"github.com/buildbarn/bb-storage/pkg/filesystem/path"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

func TestPersistentWorkerGRPCRoundTrip(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	executable, err := runfiles.Rlocation(os.Getenv("FAKE_WORKER_BINARY"))
	require.NoError(t, err)
	compiler, err := os.ReadFile(executable)
	require.NoError(t, err)
	directoryPath := t.TempDir()
	require.NoError(t, os.Mkdir(filepath.Join(directoryPath, "root"), 0o777))
	require.NoError(t, os.WriteFile(filepath.Join(directoryPath, "root", "compiler"), compiler, 0o777))
	directory, err := filesystem.NewLocalDirectory(path.LocalFormat.NewParser(directoryPath))
	require.NoError(t, err)
	defer directory.Close()
	directoryBuilder, scopeWalker := path.EmptyBuilder.Join(path.VoidScopeWalker)
	require.NoError(t, path.Resolve(path.LocalFormat.NewParser(directoryPath), scopeWalker))
	persistentRunner := runner.NewPersistentRunner(ctx, directory, directoryBuilder, runner.NewPlainCommandCreator(&syscall.SysProcAttr{}), false, 1024*1024, nil)
	defer persistentRunner.Close()
	listener := bufconn.Listen(1024 * 1024)
	defer listener.Close()
	server := grpc.NewServer()
	runner_pb.RegisterPersistentRunnerServer(server, persistentRunner)
	defer server.Stop()
	serveDone := make(chan error, 1)
	go func() { serveDone <- server.Serve(listener) }()
	connection, err := grpc.NewClient(
		"passthrough:///persistent-runner",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(ctx context.Context, address string) (net.Conn, error) { return listener.DialContext(ctx) }),
	)
	require.NoError(t, err)
	defer connection.Close()
	client := runner_pb.NewPersistentRunnerClient(connection)
	_, err = client.CheckReadiness(ctx, &runner_pb.CheckReadinessRequest{Path: "root"})
	require.NoError(t, err)

	action := &remoteexecution.Action{Platform: &remoteexecution.Platform{Properties: []*remoteexecution.Platform_Property{
		{Name: "persistentWorkerKey", Value: "test-tool"},
	}}}
	command := &remoteexecution.Command{Arguments: []string{"./compiler", "--mode=proto", "@args"}}
	compilerHash := sha256.Sum256(compiler)
	inputs := map[string]*remoteexecution.FileNode{
		"compiler": {
			Digest:         &remoteexecution.Digest{Hash: hex.EncodeToString(compilerHash[:]), SizeBytes: int64(len(compiler))},
			IsExecutable:   true,
			NodeProperties: &remoteexecution.NodeProperties{Properties: []*remoteexecution.NodeProperty{{Name: "bazel_tool_input"}}},
		},
	}
	var sessionID string
	var workerID uuid.UUID
	var compatibilityKey [sha256.Size]byte
	for requestIndex, test := range []struct {
		source    string
		arguments string
		exitCode  int32
	}{
		{source: "first source", arguments: "source\noutput\n"},
		{source: "second source", arguments: "source\noutput\n"},
		{source: "third source", arguments: "missing\noutput\n", exitCode: 1},
		{source: "recovered source", arguments: "source\noutput\n"},
	} {
		for name, contents := range map[string]string{"source": test.source, "args": test.arguments} {
			require.NoError(t, os.WriteFile(filepath.Join(directoryPath, "root", name), []byte(contents), 0o666))
			hash := sha256.Sum256([]byte(contents))
			inputs[name] = &remoteexecution.FileNode{Digest: &remoteexecution.Digest{Hash: hex.EncodeToString(hash[:]), SizeBytes: int64(len(contents))}}
		}
		prepared, err := builder.NewPersistentWorkerCommand(action.Platform, command, digest.MustNewFunction("instance", remoteexecution.DigestFunction_SHA256), inputs, nil)
		require.NoError(t, err)
		require.NotNil(t, prepared)
		if requestIndex == 0 {
			compatibilityKey = prepared.CompatibilityKey
			createContext, cancelCreate := context.WithCancel(ctx)
			session, err := client.CreateSession(createContext, &runner_pb.CreateSessionRequest{
				Arguments: prepared.Arguments, EnvironmentVariables: prepared.EnvironmentVariables,
				WorkingDirectory: prepared.WorkingDirectory, InputRootDirectory: "root", ProcessStderrPath: "compiler.stderr",
			})
			cancelCreate()
			require.NoError(t, err)
			sessionID = session.SessionId
		}
		require.Equal(t, compatibilityKey, prepared.CompatibilityKey)
		request, err := prepared.NewExecuteInPersistentWorkerRequest(ctx, sessionID, func(ctx context.Context, name string) ([]byte, error) {
			return os.ReadFile(filepath.Join(directoryPath, "root", name))
		})
		require.NoError(t, err)
		response, err := client.ExecuteInPersistentWorker(ctx, request)
		require.NoError(t, err)
		workResponse, err := builder.DecodePersistentWorkerResponse(response)
		require.NoError(t, err)
		require.Equal(t, test.exitCode, workResponse.ExitCode)
		responseID, _, _ := strings.Cut(workResponse.Output, "\n")
		currentWorkerID, err := uuid.Parse(responseID)
		require.NoError(t, err)
		if requestIndex == 0 {
			workerID = currentWorkerID
		}
		require.Equal(t, workerID, currentWorkerID)
		output, err := os.ReadFile(filepath.Join(directoryPath, "root", "output"))
		if test.exitCode == 0 {
			require.NoError(t, err)
			require.Equal(t, strings.ToUpper(test.source), string(output))
			require.NoError(t, os.Remove(filepath.Join(directoryPath, "root", "output")))
		} else {
			require.Error(t, err)
		}
	}
	for range 2 {
		_, err := client.CloseSession(ctx, &runner_pb.SessionRequest{SessionId: sessionID})
		require.NoError(t, err)
	}
	_, err = client.ExecuteInPersistentWorker(ctx, &runner_pb.ExecuteInPersistentWorkerRequest{SessionId: sessionID})
	require.Error(t, err)
	server.Stop()
	require.NoError(t, <-serveDone)
}
