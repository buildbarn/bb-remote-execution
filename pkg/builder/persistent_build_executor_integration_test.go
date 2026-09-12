package builder_test

import (
	"context"
	"math"
	"net"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/bazelbuild/rules_go/go/runfiles"
	"github.com/buildbarn/bb-remote-execution/internal/mock"
	"github.com/buildbarn/bb-remote-execution/pkg/builder"
	re_cas "github.com/buildbarn/bb-remote-execution/pkg/cas"
	"github.com/buildbarn/bb-remote-execution/pkg/filesystem/pool"
	"github.com/buildbarn/bb-remote-execution/pkg/proto/remoteworker"
	runner_pb "github.com/buildbarn/bb-remote-execution/pkg/proto/runner"
	"github.com/buildbarn/bb-remote-execution/pkg/runner"
	"github.com/buildbarn/bb-storage/pkg/blobstore/buffer"
	"github.com/buildbarn/bb-storage/pkg/cas"
	"github.com/buildbarn/bb-storage/pkg/clock"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/filesystem"
	"github.com/buildbarn/bb-storage/pkg/filesystem/path"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"go.uber.org/mock/gomock"
	"golang.org/x/sync/semaphore"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
)

func TestPersistentBuildExecutorGRPCRoundTrip(test *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	ctrl := gomock.NewController(test)
	digestFunction := digest.MustNewFunction("instance", remoteexecution.DigestFunction_SHA256)
	var storageLock sync.Mutex
	contents := map[digest.Digest][]byte{}
	addBytes := func(data []byte) *remoteexecution.Digest {
		generator := digestFunction.NewGenerator(int64(len(data)))
		_, err := generator.Write(data)
		require.NoError(test, err)
		blobDigest := generator.Sum()
		storageLock.Lock()
		contents[blobDigest] = data
		storageLock.Unlock()
		return blobDigest.GetProto()
	}
	addMessage := func(message proto.Message) *remoteexecution.Digest {
		data, err := proto.Marshal(message)
		require.NoError(test, err)
		return addBytes(data)
	}
	blobs := mock.NewMockBlobAccess(ctrl)
	blobs.EXPECT().Get(gomock.Any(), gomock.Any()).DoAndReturn(func(ctx context.Context, blobDigest digest.Digest) buffer.Buffer {
		if err := ctx.Err(); err != nil {
			return buffer.NewBufferFromError(err)
		}
		storageLock.Lock()
		defer storageLock.Unlock()
		if data, ok := contents[blobDigest]; ok {
			return buffer.NewValidatedBufferFromByteSlice(data)
		}
		return buffer.NewBufferFromError(status.Error(codes.NotFound, "Missing blob"))
	}).AnyTimes()
	blobs.EXPECT().Put(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(func(ctx context.Context, blobDigest digest.Digest, data buffer.Buffer) error {
		serialized, err := data.ToByteSlice(math.MaxInt)
		if err == nil {
			storageLock.Lock()
			contents[blobDigest] = serialized
			storageLock.Unlock()
		}
		return err
	}).AnyTimes()
	directoryPath := test.TempDir()
	directory, err := filesystem.NewLocalDirectory(path.LocalFormat.NewParser(directoryPath))
	require.NoError(test, err)
	defer directory.Close()
	directoryBuilder, walker := path.EmptyBuilder.Join(path.VoidScopeWalker)
	require.NoError(test, path.Resolve(path.LocalFormat.NewParser(directoryPath), walker))
	persistentRunner := runner.NewPersistentRunner(ctx, directory, directoryBuilder, runner.NewPlainCommandCreator(&syscall.SysProcAttr{}), true, 1024*1024)
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
	require.NoError(test, err)
	defer connection.Close()
	directoryFetcher := re_cas.NewBlobAccessDirectoryFetcher(blobs, cas.NewBlobAccessMessageReader[remoteexecution.Directory](blobs, 1024*1024), nil, 1024*1024, 1024*1024)
	buildDirectory := builder.NewNaiveBuildDirectory(directory, directoryFetcher, re_cas.NewBlobAccessFileFetcher(blobs), semaphore.NewWeighted(4), blobs)
	var nextID atomic.Uint64
	executor := builder.NewPersistentBuildExecutor(ctx, mock.NewMockBuildExecutor(ctrl), blobs,
		cas.NewBlobAccessMessageReader[remoteexecution.Command](blobs, 1024*1024), directoryFetcher,
		builder.NewSharedBuildDirectoryCreator(builder.NewRootBuildDirectoryCreator(buildDirectory), &nextID),
		runner_pb.NewPersistentRunnerClient(connection), pool.EmptyFilePool, clock.SystemClock,
		time.Second, time.Second, nil, nil, false)
	defer func() { require.NoError(test, executor.Close(context.Background())) }()
	executable, err := runfiles.Rlocation(os.Getenv("FAKE_WORKER_BINARY"))
	require.NoError(test, err)
	compiler, err := os.ReadFile(executable)
	require.NoError(test, err)
	compilerDigest := addBytes(compiler)
	var previousWorkerID uuid.UUID
	for actionIndex, source := range []string{"first", "second", "failure", "recovered", "different environment"} {
		arguments := "source\nout/result\n"
		if actionIndex == 2 {
			arguments = "missing\nout/result\n"
		}
		inputDirectory := &remoteexecution.Directory{Files: []*remoteexecution.FileNode{
			{Name: "args", Digest: addBytes([]byte(arguments))},
			{
				Name: "compiler", Digest: compilerDigest, IsExecutable: true,
				NodeProperties: &remoteexecution.NodeProperties{Properties: []*remoteexecution.NodeProperty{{Name: "bazel_tool_input"}}},
			},
			{Name: "source", Digest: addBytes([]byte(source))},
		}}
		command := &remoteexecution.Command{Arguments: []string{"./compiler", "--mode=proto", "@args"}, WorkingDirectory: "work", OutputPaths: []string{"out/result"}}
		if actionIndex == 4 {
			command.EnvironmentVariables = []*remoteexecution.Command_EnvironmentVariable{{Name: "CHANGED", Value: "true"}}
		}
		action := &remoteexecution.Action{
			CommandDigest: addMessage(command),
			InputRootDigest: addMessage(&remoteexecution.Directory{Directories: []*remoteexecution.DirectoryNode{
				{Name: "work", Digest: addMessage(inputDirectory)},
			}}),
			Timeout: durationpb.New(10 * time.Second),
			Platform: &remoteexecution.Platform{Properties: []*remoteexecution.Platform_Property{
				{Name: "persistentWorkerKey", Value: "fake-compiler"},
			}},
		}
		actionContext, cancelAction := context.WithCancel(ctx)
		updates := make(chan *remoteworker.CurrentState_Executing, 10)
		response := executor.Execute(actionContext, pool.EmptyFilePool, nil, digestFunction,
			&remoteworker.DesiredState_Executing{Action: action, ActionDigest: addMessage(action)}, updates)
		cancelAction()
		require.NoError(test, status.ErrorProto(response.Status))
		diagnosticsDigest, err := digestFunction.NewDigestFromProto(response.Result.StderrDigest)
		require.NoError(test, err)
		diagnostics, err := blobs.Get(ctx, diagnosticsDigest).ToByteSlice(1024 * 1024)
		require.NoError(test, err)
		workerIDString, _, _ := strings.Cut(string(diagnostics), "\n")
		workerID, err := uuid.Parse(workerIDString)
		require.NoError(test, err)
		if actionIndex > 0 && actionIndex < 4 {
			require.Equal(test, previousWorkerID, workerID)
		} else {
			require.NotEqual(test, previousWorkerID, workerID)
		}
		previousWorkerID = workerID
		if actionIndex == 2 {
			require.Equal(test, int32(1), response.Result.ExitCode)
			require.Empty(test, response.Result.OutputFiles)
		} else {
			require.Equal(test, int32(0), response.Result.ExitCode)
			require.Len(test, response.Result.OutputFiles, 1)
			outputDigest, err := digestFunction.NewDigestFromProto(response.Result.OutputFiles[0].Digest)
			require.NoError(test, err)
			output, err := blobs.Get(ctx, outputDigest).ToByteSlice(1024)
			require.NoError(test, err)
			require.Equal(test, strings.ToUpper(source), string(output))
		}
	}
	require.NoError(test, executor.Close(context.Background()))
	children, err := os.ReadDir(directoryPath)
	require.NoError(test, err)
	require.Empty(test, children)
	server.Stop()
	require.NoError(test, <-serveDone)
}
