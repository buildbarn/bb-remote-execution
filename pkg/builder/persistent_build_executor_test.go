package builder_test

import (
	"context"
	"math"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-remote-execution/internal/mock"
	"github.com/buildbarn/bb-remote-execution/pkg/builder"
	"github.com/buildbarn/bb-remote-execution/pkg/filesystem/pool"
	"github.com/buildbarn/bb-remote-execution/pkg/proto/remoteworker"
	runner_pb "github.com/buildbarn/bb-remote-execution/pkg/proto/runner"
	worker_pb "github.com/buildbarn/bb-remote-execution/pkg/proto/worker"
	"github.com/buildbarn/bb-storage/pkg/blobstore/buffer"
	"github.com/buildbarn/bb-storage/pkg/clock"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/filesystem/path"
	"github.com/buildbarn/bb-storage/pkg/testutil"
	"github.com/stretchr/testify/require"

	"go.uber.org/mock/gomock"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/emptypb"
)

type persistentExecutorTestFixture struct {
	test          *testing.T
	storage       *persistentWorkspaceTestStorage
	nativePath    string
	commandReader *mock.MockMessageReader[*remoteexecution.Command]
	ordinary      *mock.MockBuildExecutor
	runner        *mock.MockPersistentRunnerClient
	executor      *builder.PersistentBuildExecutor
	cancel        context.CancelFunc
	hooks         []*persistentWorkspaceHookDirectory
	storeOutputs  *gomock.Call
}

func newPersistentExecutorTestFixture(test *testing.T) *persistentExecutorTestFixture {
	ctrl := gomock.NewController(test)
	storage := newPersistentWorkspaceTestStorage(test)
	rootDirectory, nativePath := storage.newNativeRoot()
	var nextID atomic.Uint64
	lifetimeContext, cancel := context.WithCancel(context.Background())
	test.Cleanup(cancel)
	fixture := &persistentExecutorTestFixture{
		test:          test,
		storage:       storage,
		nativePath:    nativePath,
		commandReader: mock.NewMockMessageReader[*remoteexecution.Command](ctrl),
		ordinary:      mock.NewMockBuildExecutor(ctrl),
		runner:        mock.NewMockPersistentRunnerClient(ctrl),
		cancel:        cancel,
	}
	fixture.commandReader.EXPECT().ReadMessage(gomock.Any(), gomock.Any()).DoAndReturn(func(ctx context.Context, commandDigest digest.Digest) (*remoteexecution.Command, error) {
		command := &remoteexecution.Command{}
		if err := proto.Unmarshal(storage.contents[commandDigest], command); err != nil {
			return nil, err
		}
		return command, nil
	}).AnyTimes()
	fixture.storeOutputs = storage.blobs.EXPECT().Put(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(func(ctx context.Context, blobDigest digest.Digest, contents buffer.Buffer) error {
		if err := ctx.Err(); err != nil {
			contents.Discard()
			return status.FromContextError(err).Err()
		}
		data, err := contents.ToByteSlice(math.MaxInt)
		if err == nil {
			storage.contents[blobDigest] = data
		}
		return err
	}).AnyTimes()
	baseCreator := builder.NewSharedBuildDirectoryCreator(builder.NewRootBuildDirectoryCreator(rootDirectory), &nextID)
	creator := mock.NewMockBuildDirectoryCreator(ctrl)
	creator.EXPECT().GetBuildDirectory(gomock.Any(), nil).DoAndReturn(func(ctx context.Context, actionDigest *digest.Digest) (builder.BuildDirectory, *path.Trace, error) {
		directory, directoryPath, err := baseCreator.GetBuildDirectory(ctx, actionDigest)
		if err != nil {
			return nil, nil, err
		}
		hooks := &persistentWorkspaceHookDirectory{BuildDirectory: directory}
		fixture.hooks = append(fixture.hooks, hooks)
		return hooks, directoryPath, nil
	}).AnyTimes()
	fixture.executor = builder.NewPersistentBuildExecutor(
		lifetimeContext, fixture.ordinary, storage.blobs, fixture.commandReader, storage.fetcher,
		creator,
		fixture.runner, pool.EmptyFilePool, clock.SystemClock, time.Second, time.Second, nil,
		map[string]string{"PATH": "default", "DEFAULT": "retained"}, false,
	)
	return fixture
}

func (fixture *persistentExecutorTestFixture) request(tool, source string, modify func(*remoteexecution.Action, *remoteexecution.Command, *remoteexecution.Directory)) *remoteworker.DesiredState_Executing {
	storage := fixture.storage
	command := &remoteexecution.Command{
		Arguments:            []string{"./compiler", "--startup", "@args"},
		WorkingDirectory:     "pkg",
		EnvironmentVariables: []*remoteexecution.Command_EnvironmentVariable{{Name: "PATH", Value: "command"}},
		OutputPaths:          []string{"out/result"},
	}
	directory := &remoteexecution.Directory{Files: []*remoteexecution.FileNode{
		{Name: "args", Digest: storage.addBytes([]byte("source\nout/result\n")).GetProto()},
		{
			Name: "compiler", Digest: storage.addBytes([]byte(tool)).GetProto(), IsExecutable: true,
			NodeProperties: &remoteexecution.NodeProperties{Properties: []*remoteexecution.NodeProperty{{Name: "bazel_tool_input"}}},
		},
		{Name: "source", Digest: storage.addBytes([]byte(source)).GetProto()},
	}}
	action := &remoteexecution.Action{
		Timeout: durationpb.New(time.Minute),
		Platform: &remoteexecution.Platform{Properties: []*remoteexecution.Platform_Property{
			{Name: "persistentWorkerKey", Value: "compiler"},
		}},
	}
	if modify != nil {
		modify(action, command, directory)
	}
	action.InputRootDigest = storage.addDirectory(&remoteexecution.Directory{Directories: []*remoteexecution.DirectoryNode{
		{Name: "pkg", Digest: storage.addDirectory(directory).GetProto()},
	}}).GetProto()
	commandBytes, err := proto.Marshal(command)
	require.NoError(fixture.test, err)
	action.CommandDigest = storage.addBytes(commandBytes).GetProto()
	actionBytes, err := proto.Marshal(action)
	require.NoError(fixture.test, err)
	return &remoteworker.DesiredState_Executing{
		Action: action, ActionDigest: storage.addBytes(actionBytes).GetProto(),
	}
}

func (fixture *persistentExecutorTestFixture) execute(ctx context.Context, request *remoteworker.DesiredState_Executing) (*remoteexecution.ExecuteResponse, []*remoteworker.CurrentState_Executing) {
	updates := make(chan *remoteworker.CurrentState_Executing, 10)
	response := fixture.executor.Execute(ctx, pool.EmptyFilePool, nil, fixture.storage.function, request, updates)
	close(updates)
	var states []*remoteworker.CurrentState_Executing
	for update := range updates {
		states = append(states, update)
	}
	return response, states
}

func persistentExecutorTestResponse(test *testing.T, response *worker_pb.WorkResponse) *runner_pb.ExecuteInPersistentWorkerResponse {
	serialized, err := proto.Marshal(response)
	require.NoError(test, err)
	return &runner_pb.ExecuteInPersistentWorkerResponse{SerializedWorkResponse: serialized}
}

func TestPersistentBuildExecutorReuse(test *testing.T) {
	fixture := newPersistentExecutorTestFixture(test)
	var workingPath string
	var workingDirectory *os.File
	fixture.runner.EXPECT().CreateSession(gomock.Any(), gomock.Any()).DoAndReturn(func(ctx context.Context, request *runner_pb.CreateSessionRequest, options ...grpc.CallOption) (*runner_pb.CreateSessionResponse, error) {
		require.Equal(test, []string{"./compiler", "--startup", "--persistent_worker"}, request.Arguments)
		require.Equal(test, map[string]string{"PATH": "command", "DEFAULT": "retained"}, request.EnvironmentVariables)
		require.Equal(test, "pkg", request.WorkingDirectory)
		require.Equal(test, "1/tmp", request.TemporaryDirectory)
		require.Equal(test, "1/session_logs/stderr", request.ProcessStderrPath)
		require.Equal(test, "1/session_logs", request.ServerLogsDirectory)
		workingPath = filepath.Join(fixture.nativePath, request.InputRootDirectory, request.WorkingDirectory)
		var err error
		workingDirectory, err = os.Open(workingPath)
		require.NoError(test, err)
		test.Cleanup(func() {
			if workingDirectory != nil {
				require.NoError(test, workingDirectory.Close())
			}
		})
		require.NoError(test, os.WriteFile(filepath.Join(fixture.nativePath, request.ProcessStderrPath), []byte("lifetime diagnostics"), 0o666))
		require.NoError(test, os.WriteFile(filepath.Join(fixture.nativePath, request.TemporaryDirectory, "cache"), []byte("cached"), 0o666))
		return &runner_pb.CreateSessionResponse{SessionId: "session"}, nil
	})
	resourceUsage, err := anypb.New(&emptypb.Empty{})
	require.NoError(test, err)
	for requestIndex, source := range []string{"first", "second", "compile failure", "recovered"} {
		fixture.runner.EXPECT().ExecuteInPersistentWorker(gomock.Any(), gomock.Any()).DoAndReturn(func(ctx context.Context, request *runner_pb.ExecuteInPersistentWorkerRequest, options ...grpc.CallOption) (*runner_pb.ExecuteInPersistentWorkerResponse, error) {
			require.Equal(test, "session", request.SessionId)
			workRequest := &worker_pb.WorkRequest{}
			require.NoError(test, proto.Unmarshal(request.SerializedWorkRequest, workRequest))
			require.Equal(test, []string{"source", "out/result"}, workRequest.Arguments)
			require.Len(test, workRequest.Inputs, 3)
			require.Equal(test, "pkg/args", workRequest.Inputs[0].Path)
			require.Equal(test, int32(0), workRequest.RequestId)
			contents, err := os.ReadFile(filepath.Join(workingPath, "source"))
			require.NoError(test, err)
			require.Equal(test, source, string(contents))
			for _, stale := range []string{"out/result", "undeclared", "old-directory"} {
				_, err := os.Stat(filepath.Join(workingPath, stale))
				require.ErrorIs(test, err, os.ErrNotExist)
			}
			heldInfo, err := workingDirectory.Stat()
			require.NoError(test, err)
			currentInfo, err := os.Stat(workingPath)
			require.NoError(test, err)
			require.True(test, os.SameFile(heldInfo, currentInfo))
			require.FileExists(test, filepath.Join(fixture.nativePath, "1/tmp/cache"))
			require.NoError(test, os.WriteFile(filepath.Join(workingPath, "undeclared"), []byte("stale"), 0o666))
			require.NoError(test, os.Mkdir(filepath.Join(workingPath, "old-directory"), 0o777))
			response := &worker_pb.WorkResponse{Output: "request diagnostics: " + source}
			if requestIndex == 2 {
				response.ExitCode = 7
			} else {
				require.NoError(test, os.WriteFile(filepath.Join(workingPath, "out/result"), []byte(source), 0o666))
			}
			serialized := persistentExecutorTestResponse(test, response)
			serialized.ResourceUsage = []*anypb.Any{resourceUsage}
			return serialized, nil
		})
		ctx, cancel := context.WithCancel(context.Background())
		response, states := fixture.execute(ctx, fixture.request("tool", source, nil))
		cancel()
		require.NoError(test, status.ErrorProto(response.Status))
		require.Len(test, states, 3)
		require.NotNil(test, states[0].GetFetchingInputs())
		require.NotNil(test, states[1].GetRunning())
		require.NotNil(test, states[2].GetUploadingOutputs())
		require.Nil(test, response.Result.StdoutDigest)
		require.Empty(test, response.ServerLogs)
		diagnostics, err := fixture.storage.function.NewDigestFromProto(response.Result.StderrDigest)
		require.NoError(test, err)
		require.Equal(test, "request diagnostics: "+source, string(fixture.storage.contents[diagnostics]))
		testutil.RequireEqualProto(test, resourceUsage, response.Result.ExecutionMetadata.AuxiliaryMetadata[0])
		if requestIndex == 2 {
			require.Equal(test, int32(7), response.Result.ExitCode)
			require.Empty(test, response.Result.OutputFiles)
		} else {
			require.Len(test, response.Result.OutputFiles, 1)
			outputDigest, err := fixture.storage.function.NewDigestFromProto(response.Result.OutputFiles[0].Digest)
			require.NoError(test, err)
			require.Equal(test, source, string(fixture.storage.contents[outputDigest]))
		}
	}
	fixture.runner.EXPECT().CloseSession(gomock.Any(), &runner_pb.SessionRequest{SessionId: "session"}).DoAndReturn(func(ctx context.Context, request *runner_pb.SessionRequest, options ...grpc.CallOption) (*emptypb.Empty, error) {
		require.DirExists(test, workingPath)
		require.NoError(test, workingDirectory.Close())
		workingDirectory = nil
		return &emptypb.Empty{}, nil
	})
	for range 2 {
		require.NoError(test, fixture.executor.Close(context.Background()))
	}
	require.NoDirExists(test, filepath.Join(fixture.nativePath, "1"))
}

func TestPersistentBuildExecutorFallback(test *testing.T) {
	for _, testCase := range []struct {
		name   string
		modify func(*remoteexecution.Action, *remoteexecution.Command, *remoteexecution.Directory)
	}{
		{name: "Ordinary", modify: func(action *remoteexecution.Action, command *remoteexecution.Command, directory *remoteexecution.Directory) {
			action.Platform = nil
		}},
		{name: "JSON", modify: func(action *remoteexecution.Action, command *remoteexecution.Command, directory *remoteexecution.Directory) {
			action.Platform.Properties = append(action.Platform.Properties, &remoteexecution.Platform_Property{Name: "persistentWorkerProtocol", Value: "json"})
		}},
		{name: "UnknownProtocol", modify: func(action *remoteexecution.Action, command *remoteexecution.Command, directory *remoteexecution.Directory) {
			action.Platform.Properties = append(action.Platform.Properties, &remoteexecution.Platform_Property{Name: "persistentWorkerProtocol", Value: "unknown"})
		}},
		{name: "ActionPlatformOverridesCommand", modify: func(action *remoteexecution.Action, command *remoteexecution.Command, directory *remoteexecution.Directory) {
			command.Platform = action.Platform
			action.Platform = &remoteexecution.Platform{}
		}},
		{name: "UnmarkedTool", modify: func(action *remoteexecution.Action, command *remoteexecution.Command, directory *remoteexecution.Directory) {
			directory.Files[1].NodeProperties = nil
		}},
	} {
		test.Run(testCase.name, func(test *testing.T) {
			fixture := newPersistentExecutorTestFixture(test)
			request := fixture.request("tool", "source", testCase.modify)
			ctrl := gomock.NewController(test)
			filePool := mock.NewMockFilePool(ctrl)
			monitor := mock.NewMockUnreadDirectoryMonitor(ctrl)
			updates := make(chan *remoteworker.CurrentState_Executing, 10)
			expected := builder.NewDefaultExecuteResponse(request)
			fixture.ordinary.EXPECT().Execute(gomock.Any(), filePool, monitor, fixture.storage.function, request, updates).Return(expected)
			require.Same(test, expected, fixture.executor.Execute(context.Background(), filePool, monitor, fixture.storage.function, request, updates))
			require.NoError(test, fixture.executor.Close(context.Background()))
			children, err := os.ReadDir(fixture.nativePath)
			require.NoError(test, err)
			require.Empty(test, children)
		})
	}
}

func TestPersistentBuildExecutorInputSymlink(test *testing.T) {
	fixture := newPersistentExecutorTestFixture(test)
	fixture.runner.EXPECT().CreateSession(gomock.Any(), gomock.Any()).Return(&runner_pb.CreateSessionResponse{SessionId: "session"}, nil)

	for _, source := range []string{"first", "second"} {
		request := fixture.request("tool", source, func(action *remoteexecution.Action, command *remoteexecution.Command, directory *remoteexecution.Directory) {
			directory.Symlinks = []*remoteexecution.SymlinkNode{{Name: "alias", Target: "source"}}
		})
		fixture.runner.EXPECT().ExecuteInPersistentWorker(gomock.Any(), gomock.Any()).DoAndReturn(func(ctx context.Context, request *runner_pb.ExecuteInPersistentWorkerRequest, options ...grpc.CallOption) (*runner_pb.ExecuteInPersistentWorkerResponse, error) {
			require.Equal(test, "session", request.SessionId)
			aliasPath := filepath.Join(fixture.nativePath, "1/root/pkg/alias")
			target, err := os.Readlink(aliasPath)
			require.NoError(test, err)
			require.Equal(test, "source", target)
			contents, err := os.ReadFile(aliasPath)
			require.NoError(test, err)
			require.Equal(test, source, string(contents))
			return persistentExecutorTestResponse(test, &worker_pb.WorkResponse{}), nil
		})
		response, _ := fixture.execute(context.Background(), request)
		require.NoError(test, status.ErrorProto(response.Status))
	}

	fixture.runner.EXPECT().CloseSession(gomock.Any(), &runner_pb.SessionRequest{SessionId: "session"}).Return(&emptypb.Empty{}, nil)
	require.NoError(test, fixture.executor.Close(context.Background()))
}

func TestPersistentBuildExecutorPlatform(test *testing.T) {
	for _, testCase := range []struct {
		name   string
		modify func(*remoteexecution.Action, *remoteexecution.Command, *remoteexecution.Directory)
	}{
		{name: "DefaultProtocol"},
		{name: "Proto", modify: func(action *remoteexecution.Action, command *remoteexecution.Command, directory *remoteexecution.Directory) {
			action.Platform.Properties = append(action.Platform.Properties, &remoteexecution.Platform_Property{Name: "persistentWorkerProtocol", Value: "proto"})
		}},
		{name: "EmptyProtocol", modify: func(action *remoteexecution.Action, command *remoteexecution.Command, directory *remoteexecution.Directory) {
			action.Platform.Properties = append(action.Platform.Properties, &remoteexecution.Platform_Property{Name: "persistentWorkerProtocol"})
		}},
		{name: "LegacyCommandPlatform", modify: func(action *remoteexecution.Action, command *remoteexecution.Command, directory *remoteexecution.Directory) {
			command.Platform, action.Platform = action.Platform, nil
		}},
		{name: "ActionPlatformOverridesCommand", modify: func(action *remoteexecution.Action, command *remoteexecution.Command, directory *remoteexecution.Directory) {
			command.Platform = &remoteexecution.Platform{}
		}},
	} {
		test.Run(testCase.name, func(test *testing.T) {
			fixture := newPersistentExecutorTestFixture(test)
			fixture.runner.EXPECT().CreateSession(gomock.Any(), gomock.Any()).Return(&runner_pb.CreateSessionResponse{SessionId: "session"}, nil)
			fixture.runner.EXPECT().ExecuteInPersistentWorker(gomock.Any(), gomock.Any()).Return(persistentExecutorTestResponse(test, &worker_pb.WorkResponse{}), nil)
			response, _ := fixture.execute(context.Background(), fixture.request("tool", "source", testCase.modify))
			require.NoError(test, status.ErrorProto(response.Status))
			fixture.runner.EXPECT().CloseSession(gomock.Any(), &runner_pb.SessionRequest{SessionId: "session"}).Return(&emptypb.Empty{}, nil)
			require.NoError(test, fixture.executor.Close(context.Background()))
		})
	}
}

func TestPersistentBuildExecutorToolChange(test *testing.T) {
	fixture := newPersistentExecutorTestFixture(test)
	for _, sessionID := range []string{"first", "second"} {
		fixture.runner.EXPECT().CreateSession(gomock.Any(), gomock.Any()).Return(&runner_pb.CreateSessionResponse{SessionId: sessionID}, nil)
		fixture.runner.EXPECT().ExecuteInPersistentWorker(gomock.Any(), gomock.Any()).Return(persistentExecutorTestResponse(test, &worker_pb.WorkResponse{}), nil)
		response, _ := fixture.execute(context.Background(), fixture.request(sessionID, "source", nil))
		require.NoError(test, status.ErrorProto(response.Status))
		fixture.runner.EXPECT().CloseSession(gomock.Any(), &runner_pb.SessionRequest{SessionId: sessionID}).DoAndReturn(func(ctx context.Context, request *runner_pb.SessionRequest, options ...grpc.CallOption) (*emptypb.Empty, error) {
			if sessionID == "first" {
				require.DirExists(test, filepath.Join(fixture.nativePath, "1/root"))
				require.NoDirExists(test, filepath.Join(fixture.nativePath, "2"))
			} else {
				require.NoDirExists(test, filepath.Join(fixture.nativePath, "1"))
				require.DirExists(test, filepath.Join(fixture.nativePath, "2/root"))
			}
			return &emptypb.Empty{}, nil
		})
	}
	require.NoError(test, fixture.executor.Close(context.Background()))
}

func TestPersistentBuildExecutorInvalidInputs(test *testing.T) {
	for _, testCase := range []struct {
		name   string
		modify func(*remoteexecution.Action, *remoteexecution.Command, *remoteexecution.Directory)
	}{
		{name: "Duplicate", modify: func(action *remoteexecution.Action, command *remoteexecution.Command, directory *remoteexecution.Directory) {
			directory.Files = append(directory.Files, directory.Files[0])
		}},
		{name: "ConflictingTypes", modify: func(action *remoteexecution.Action, command *remoteexecution.Command, directory *remoteexecution.Directory) {
			directory.Symlinks = []*remoteexecution.SymlinkNode{{Name: "compiler", Target: "source"}}
		}},
		{name: "InvalidName", modify: func(action *remoteexecution.Action, command *remoteexecution.Command, directory *remoteexecution.Directory) {
			directory.Files[0].Name = "../args"
		}},
		{name: "InvalidDigest", modify: func(action *remoteexecution.Action, command *remoteexecution.Command, directory *remoteexecution.Directory) {
			directory.Files[0].Digest = nil
		}},
		{name: "InvalidTimeout", modify: func(action *remoteexecution.Action, command *remoteexecution.Command, directory *remoteexecution.Directory) {
			action.Timeout = nil
		}},
		{name: "InvalidCommand", modify: func(action *remoteexecution.Action, command *remoteexecution.Command, directory *remoteexecution.Directory) {
			command.Arguments = nil
		}},
		{name: "DuplicateWorkerKey", modify: func(action *remoteexecution.Action, command *remoteexecution.Command, directory *remoteexecution.Directory) {
			action.Platform.Properties = append(action.Platform.Properties, action.Platform.Properties[0])
		}},
		{name: "DuplicateProtocol", modify: func(action *remoteexecution.Action, command *remoteexecution.Command, directory *remoteexecution.Directory) {
			action.Platform.Properties = append(action.Platform.Properties,
				&remoteexecution.Platform_Property{Name: "persistentWorkerProtocol", Value: "proto"},
				&remoteexecution.Platform_Property{Name: "persistentWorkerProtocol", Value: "proto"})
		}},
	} {
		test.Run(testCase.name, func(test *testing.T) {
			fixture := newPersistentExecutorTestFixture(test)
			response, _ := fixture.execute(context.Background(), fixture.request("tool", "source", testCase.modify))
			require.Error(test, status.ErrorProto(response.Status))
			require.NoError(test, fixture.executor.Close(context.Background()))
		})
	}
}

func TestPersistentBuildExecutorFailedExchange(test *testing.T) {
	for _, testCase := range []struct {
		name     string
		response *runner_pb.ExecuteInPersistentWorkerResponse
		err      error
	}{
		{name: "Transport", err: status.Error(codes.Unavailable, "Connection lost")},
		{name: "DeadSession", err: status.Error(codes.NotFound, "Session exited")},
		{name: "Malformed", response: &runner_pb.ExecuteInPersistentWorkerResponse{SerializedWorkResponse: []byte{0xff}}},
		{name: "MissingResponse"},
		{name: "WrongRequestID", response: persistentExecutorTestResponse(test, &worker_pb.WorkResponse{RequestId: 42})},
	} {
		test.Run(testCase.name, func(test *testing.T) {
			fixture := newPersistentExecutorTestFixture(test)
			request := fixture.request("tool", "source", nil)
			fixture.runner.EXPECT().CreateSession(gomock.Any(), gomock.Any()).Return(&runner_pb.CreateSessionResponse{SessionId: "first"}, nil)
			fixture.runner.EXPECT().ExecuteInPersistentWorker(gomock.Any(), gomock.Any()).Return(testCase.response, testCase.err)
			fixture.runner.EXPECT().CloseSession(gomock.Any(), &runner_pb.SessionRequest{SessionId: "first"}).DoAndReturn(func(ctx context.Context, request *runner_pb.SessionRequest, options ...grpc.CallOption) (*emptypb.Empty, error) {
				require.NoError(test, ctx.Err())
				require.DirExists(test, filepath.Join(fixture.nativePath, "1/root"))
				return &emptypb.Empty{}, nil
			})
			response, _ := fixture.execute(context.Background(), request)
			require.Error(test, status.ErrorProto(response.Status))
			require.NoDirExists(test, filepath.Join(fixture.nativePath, "1"))
			fixture.runner.EXPECT().CreateSession(gomock.Any(), gomock.Any()).Return(&runner_pb.CreateSessionResponse{SessionId: "second"}, nil)
			fixture.runner.EXPECT().ExecuteInPersistentWorker(gomock.Any(), gomock.Any()).Return(persistentExecutorTestResponse(test, &worker_pb.WorkResponse{}), nil)
			response, _ = fixture.execute(context.Background(), request)
			require.NoError(test, status.ErrorProto(response.Status))
			fixture.runner.EXPECT().CloseSession(gomock.Any(), &runner_pb.SessionRequest{SessionId: "second"}).Return(&emptypb.Empty{}, nil)
			require.NoError(test, fixture.executor.Close(context.Background()))
		})
	}
}

func TestPersistentBuildExecutorCancellation(test *testing.T) {
	for _, mode := range []string{"Action", "Lifetime", "Timeout"} {
		test.Run(mode, func(test *testing.T) {
			fixture := newPersistentExecutorTestFixture(test)
			request := fixture.request("tool", "source", nil)
			if mode == "Timeout" {
				request.Action.Timeout = durationpb.New(50 * time.Millisecond)
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			fixture.runner.EXPECT().CreateSession(gomock.Any(), gomock.Any()).Return(&runner_pb.CreateSessionResponse{SessionId: "session"}, nil)
			fixture.runner.EXPECT().ExecuteInPersistentWorker(gomock.Any(), gomock.Any()).DoAndReturn(func(ctx context.Context, request *runner_pb.ExecuteInPersistentWorkerRequest, options ...grpc.CallOption) (*runner_pb.ExecuteInPersistentWorkerResponse, error) {
				switch mode {
				case "Action":
					cancel()
				case "Lifetime":
					fixture.cancel()
				}
				<-ctx.Done()
				return nil, status.FromContextError(ctx.Err()).Err()
			})
			fixture.runner.EXPECT().CloseSession(gomock.Any(), &runner_pb.SessionRequest{SessionId: "session"}).DoAndReturn(func(ctx context.Context, request *runner_pb.SessionRequest, options ...grpc.CallOption) (*emptypb.Empty, error) {
				require.NoError(test, ctx.Err())
				_, hasDeadline := ctx.Deadline()
				require.True(test, hasDeadline)
				return &emptypb.Empty{}, nil
			})
			response, _ := fixture.execute(ctx, request)
			require.Error(test, status.ErrorProto(response.Status))
			require.NoDirExists(test, filepath.Join(fixture.nativePath, "1"))
			require.NoError(test, fixture.executor.Close(context.Background()))
		})
	}
}

func TestPersistentBuildExecutorConfirmedCreationFailure(test *testing.T) {
	fixture := newPersistentExecutorTestFixture(test)
	request := fixture.request("tool", "source", nil)
	creationStatus, err := status.New(codes.Unavailable, "Startup failed").WithDetails(&runner_pb.CreateSessionFailure{})
	require.NoError(test, err)
	fixture.runner.EXPECT().CreateSession(gomock.Any(), gomock.Any()).Return(nil, creationStatus.Err())
	response, _ := fixture.execute(context.Background(), request)
	require.Error(test, status.ErrorProto(response.Status))
	require.NoDirExists(test, filepath.Join(fixture.nativePath, "1"))
	fixture.ordinary.EXPECT().CheckReadiness(gomock.Any()).Return(nil)
	fixture.runner.EXPECT().CheckReadiness(gomock.Any(), gomock.Any()).Return(&emptypb.Empty{}, nil)
	require.NoError(test, fixture.executor.CheckReadiness(context.Background()))
	fixture.runner.EXPECT().CreateSession(gomock.Any(), gomock.Any()).Return(&runner_pb.CreateSessionResponse{SessionId: "recovered"}, nil)
	fixture.runner.EXPECT().ExecuteInPersistentWorker(gomock.Any(), gomock.Any()).Return(persistentExecutorTestResponse(test, &worker_pb.WorkResponse{}), nil)
	response, _ = fixture.execute(context.Background(), request)
	require.NoError(test, status.ErrorProto(response.Status))
	fixture.runner.EXPECT().CloseSession(gomock.Any(), &runner_pb.SessionRequest{SessionId: "recovered"}).Return(&emptypb.Empty{}, nil)
	require.NoError(test, fixture.executor.Close(context.Background()))
}

func TestPersistentBuildExecutorConcurrentExecution(test *testing.T) {
	fixture := newPersistentExecutorTestFixture(test)
	request := fixture.request("tool", "source", nil)
	started := make(chan struct{})
	finish := make(chan struct{})
	fixture.runner.EXPECT().CreateSession(gomock.Any(), gomock.Any()).Return(&runner_pb.CreateSessionResponse{SessionId: "session"}, nil)
	fixture.runner.EXPECT().ExecuteInPersistentWorker(gomock.Any(), gomock.Any()).DoAndReturn(func(ctx context.Context, request *runner_pb.ExecuteInPersistentWorkerRequest, options ...grpc.CallOption) (*runner_pb.ExecuteInPersistentWorkerResponse, error) {
		close(started)
		<-finish
		return persistentExecutorTestResponse(test, &worker_pb.WorkResponse{}), nil
	})
	done := make(chan *remoteexecution.ExecuteResponse, 1)
	go func() {
		response, _ := fixture.execute(context.Background(), request)
		done <- response
	}()
	<-started
	response, _ := fixture.execute(context.Background(), request)
	require.Error(test, status.ErrorProto(response.Status))
	require.Error(test, fixture.executor.Close(context.Background()))
	require.Error(test, fixture.executor.CheckReadiness(context.Background()))
	close(finish)
	require.NoError(test, status.ErrorProto((<-done).Status))
	fixture.runner.EXPECT().CloseSession(gomock.Any(), &runner_pb.SessionRequest{SessionId: "session"}).Return(&emptypb.Empty{}, nil)
	require.NoError(test, fixture.executor.Close(context.Background()))
	response, _ = fixture.execute(context.Background(), request)
	require.Error(test, status.ErrorProto(response.Status))
}

func TestPersistentBuildExecutorReadiness(test *testing.T) {
	fixture := newPersistentExecutorTestFixture(test)
	fixture.ordinary.EXPECT().CheckReadiness(gomock.Any()).Return(nil)
	fixture.runner.EXPECT().CheckReadiness(gomock.Any(), &runner_pb.CheckReadinessRequest{Path: "1/check_readiness"}).DoAndReturn(func(ctx context.Context, request *runner_pb.CheckReadinessRequest, options ...grpc.CallOption) (*emptypb.Empty, error) {
		require.DirExists(test, filepath.Join(fixture.nativePath, request.Path))
		return &emptypb.Empty{}, nil
	})
	require.NoError(test, fixture.executor.CheckReadiness(context.Background()))
	require.NoDirExists(test, filepath.Join(fixture.nativePath, "1"))
	require.NoError(test, fixture.executor.Close(context.Background()))
}

func TestPersistentBuildExecutorSharedInputDirectory(test *testing.T) {
	fixture := newPersistentExecutorTestFixture(test)
	request := fixture.request("tool", "source", nil)
	rootDigest, err := fixture.storage.function.NewDigestFromProto(request.Action.InputRootDigest)
	require.NoError(test, err)
	rootDirectory := fixture.storage.directories[rootDigest]
	rootDirectory.Directories = append(rootDirectory.Directories, &remoteexecution.DirectoryNode{
		Name: "alias", Digest: rootDirectory.Directories[0].Digest,
	})
	request.Action.InputRootDigest = fixture.storage.addDirectory(rootDirectory).GetProto()
	fixture.runner.EXPECT().CreateSession(gomock.Any(), gomock.Any()).Return(&runner_pb.CreateSessionResponse{SessionId: "session"}, nil)
	fixture.runner.EXPECT().ExecuteInPersistentWorker(gomock.Any(), gomock.Any()).DoAndReturn(func(ctx context.Context, request *runner_pb.ExecuteInPersistentWorkerRequest, options ...grpc.CallOption) (*runner_pb.ExecuteInPersistentWorkerResponse, error) {
		workRequest := &worker_pb.WorkRequest{}
		require.NoError(test, proto.Unmarshal(request.SerializedWorkRequest, workRequest))
		require.Len(test, workRequest.Inputs, 6)
		require.Equal(test, "alias/args", workRequest.Inputs[0].Path)
		require.Equal(test, "pkg/args", workRequest.Inputs[3].Path)
		return persistentExecutorTestResponse(test, &worker_pb.WorkResponse{}), nil
	})
	response, _ := fixture.execute(context.Background(), request)
	require.NoError(test, status.ErrorProto(response.Status))
	fixture.runner.EXPECT().CloseSession(gomock.Any(), &runner_pb.SessionRequest{SessionId: "session"}).Return(&emptypb.Empty{}, nil)
	require.NoError(test, fixture.executor.Close(context.Background()))
}

func TestPersistentBuildExecutorCyclicInputDirectory(test *testing.T) {
	fixture := newPersistentExecutorTestFixture(test)
	request := fixture.request("tool", "source", nil)
	rootDigest, err := fixture.storage.function.NewDigestFromProto(request.Action.InputRootDigest)
	require.NoError(test, err)
	fixture.storage.directories[rootDigest].Directories[0].Digest = request.Action.InputRootDigest
	response, _ := fixture.execute(context.Background(), request)
	require.Error(test, status.ErrorProto(response.Status))
	require.NoError(test, fixture.executor.Close(context.Background()))
}

func TestPersistentBuildExecutorPreparationFailure(test *testing.T) {
	fixture := newPersistentExecutorTestFixture(test)
	request := fixture.request("tool", "source", nil)
	fixture.runner.EXPECT().CreateSession(gomock.Any(), gomock.Any()).Return(&runner_pb.CreateSessionResponse{SessionId: "session"}, nil)
	fixture.runner.EXPECT().ExecuteInPersistentWorker(gomock.Any(), gomock.Any()).Return(persistentExecutorTestResponse(test, &worker_pb.WorkResponse{}), nil)
	response, _ := fixture.execute(context.Background(), request)
	require.NoError(test, status.ErrorProto(response.Status))
	require.NoError(test, os.RemoveAll(filepath.Join(fixture.nativePath, "1/root/pkg")))
	fixture.runner.EXPECT().CloseSession(gomock.Any(), &runner_pb.SessionRequest{SessionId: "session"}).Return(&emptypb.Empty{}, nil)
	response, _ = fixture.execute(context.Background(), request)
	require.Error(test, status.ErrorProto(response.Status))
	require.NoDirExists(test, filepath.Join(fixture.nativePath, "1"))
	require.NoError(test, fixture.executor.Close(context.Background()))
}

func TestPersistentBuildExecutorFlagFileFailure(test *testing.T) {
	fixture := newPersistentExecutorTestFixture(test)
	request := fixture.request("tool", "source", func(action *remoteexecution.Action, command *remoteexecution.Command, directory *remoteexecution.Directory) {
		command.Arguments[2] = "@undeclared"
	})
	fixture.runner.EXPECT().CreateSession(gomock.Any(), gomock.Any()).Return(&runner_pb.CreateSessionResponse{SessionId: "session"}, nil)
	fixture.runner.EXPECT().CloseSession(gomock.Any(), &runner_pb.SessionRequest{SessionId: "session"}).Return(&emptypb.Empty{}, nil)
	response, _ := fixture.execute(context.Background(), request)
	require.Error(test, status.ErrorProto(response.Status))
	require.NoDirExists(test, filepath.Join(fixture.nativePath, "1"))
	require.NoError(test, fixture.executor.Close(context.Background()))
}

func TestPersistentBuildExecutorOutputFailure(test *testing.T) {
	fixture := newPersistentExecutorTestFixture(test)
	request := fixture.request("tool", "source", nil)
	fixture.runner.EXPECT().CreateSession(gomock.Any(), gomock.Any()).Return(&runner_pb.CreateSessionResponse{SessionId: "session"}, nil)
	fixture.runner.EXPECT().ExecuteInPersistentWorker(gomock.Any(), gomock.Any()).Return(persistentExecutorTestResponse(test, &worker_pb.WorkResponse{Output: "diagnostics"}), nil)
	fixture.storeOutputs.DoAndReturn(func(ctx context.Context, blobDigest digest.Digest, contents buffer.Buffer) error {
		contents.Discard()
		return status.Error(codes.Unavailable, "Storage unavailable")
	})
	fixture.runner.EXPECT().CloseSession(gomock.Any(), &runner_pb.SessionRequest{SessionId: "session"}).Return(&emptypb.Empty{}, nil)
	response, _ := fixture.execute(context.Background(), request)
	require.Error(test, status.ErrorProto(response.Status))
	require.NoDirExists(test, filepath.Join(fixture.nativePath, "1"))
	require.NoError(test, fixture.executor.Close(context.Background()))
}

func TestPersistentBuildExecutorIOFailure(test *testing.T) {
	for _, mode := range []string{"Executing", "Idle"} {
		test.Run(mode, func(test *testing.T) {
			fixture := newPersistentExecutorTestFixture(test)
			request := fixture.request("tool", "source", nil)
			ioError := status.Error(codes.DataLoss, "CAS read failed")
			fixture.runner.EXPECT().CreateSession(gomock.Any(), gomock.Any()).Return(&runner_pb.CreateSessionResponse{SessionId: "first"}, nil)
			fixture.runner.EXPECT().ExecuteInPersistentWorker(gomock.Any(), gomock.Any()).DoAndReturn(func(ctx context.Context, request *runner_pb.ExecuteInPersistentWorkerRequest, options ...grpc.CallOption) (*runner_pb.ExecuteInPersistentWorkerResponse, error) {
				if mode == "Executing" {
					fixture.hooks[0].logger.Log(ioError)
					<-ctx.Done()
					return nil, status.FromContextError(ctx.Err()).Err()
				}
				return persistentExecutorTestResponse(test, &worker_pb.WorkResponse{}), nil
			})
			fixture.runner.EXPECT().CloseSession(gomock.Any(), &runner_pb.SessionRequest{SessionId: "first"}).Return(&emptypb.Empty{}, nil)
			response, _ := fixture.execute(context.Background(), request)
			if mode == "Executing" {
				require.Equal(test, codes.DataLoss, status.Code(status.ErrorProto(response.Status)))
			} else {
				require.NoError(test, status.ErrorProto(response.Status))
				fixture.hooks[0].logger.Log(ioError)
			}
			fixture.runner.EXPECT().CreateSession(gomock.Any(), gomock.Any()).Return(&runner_pb.CreateSessionResponse{SessionId: "second"}, nil)
			fixture.runner.EXPECT().ExecuteInPersistentWorker(gomock.Any(), gomock.Any()).Return(persistentExecutorTestResponse(test, &worker_pb.WorkResponse{}), nil)
			response, _ = fixture.execute(context.Background(), request)
			require.NoError(test, status.ErrorProto(response.Status))
			require.NoDirExists(test, filepath.Join(fixture.nativePath, "1"))
			fixture.runner.EXPECT().CloseSession(gomock.Any(), &runner_pb.SessionRequest{SessionId: "second"}).Return(&emptypb.Empty{}, nil)
			require.NoError(test, fixture.executor.Close(context.Background()))
		})
	}
}
