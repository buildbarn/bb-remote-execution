package builder_test

import (
	"context"
	"os"
	"syscall"
	"testing"
	"time"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-remote-execution/internal/mock"
	"github.com/buildbarn/bb-remote-execution/pkg/builder"
	"github.com/buildbarn/bb-remote-execution/pkg/proto/remoteworker"
	runner_pb "github.com/buildbarn/bb-remote-execution/pkg/proto/runner"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/filesystem"
	"github.com/buildbarn/bb-storage/pkg/util"
	"github.com/golang/mock/gomock"
	"github.com/golang/protobuf/ptypes"
	"github.com/golang/protobuf/ptypes/any"
	"github.com/golang/protobuf/ptypes/duration"
	"github.com/golang/protobuf/ptypes/empty"
	"github.com/stretchr/testify/require"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestLocalBuildExecutorInvalidActionDigest(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)
	defer ctrl.Finish()
	contentAddressableStorage := mock.NewMockContentAddressableStorage(ctrl)
	buildDirectoryCreator := mock.NewMockBuildDirectoryCreator(ctrl)
	runner := mock.NewMockRunner(ctrl)
	clock := mock.NewMockClock(ctrl)
	localBuildExecutor := builder.NewLocalBuildExecutor(contentAddressableStorage, buildDirectoryCreator, runner, clock, time.Hour, time.Hour, nil)

	filePool := mock.NewMockFilePool(ctrl)
	metadata := make(chan *remoteworker.CurrentState_Executing, 10)
	executeResponse := localBuildExecutor.Execute(
		ctx,
		filePool,
		"netbsd",
		&remoteworker.DesiredState_Executing{
			ActionDigest: &remoteexecution.Digest{
				Hash:      "This is not a valid action digest!",
				SizeBytes: 7,
			},
			Action: &remoteexecution.Action{
				InputRootDigest: &remoteexecution.Digest{
					Hash:      "7777777777777777777777777777777777777777777777777777777777777777",
					SizeBytes: 42,
				},
			},
			Command: &remoteexecution.Command{
				Arguments: []string{"touch", "foo"},
				EnvironmentVariables: []*remoteexecution.Command_EnvironmentVariable{
					{Name: "PATH", Value: "/bin:/usr/bin"},
				},
				OutputFiles: []string{"foo"},
			},
		},
		metadata)
	require.Equal(t, &remoteexecution.ExecuteResponse{
		Result: &remoteexecution.ActionResult{
			ExecutionMetadata: &remoteexecution.ExecutedActionMetadata{},
		},
		Status: status.New(codes.InvalidArgument, "Failed to extract digest for action: Unknown digest hash length: 34 characters").Proto(),
	}, executeResponse)
}

func TestLocalBuildExecutorMissingAction(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)
	defer ctrl.Finish()
	contentAddressableStorage := mock.NewMockContentAddressableStorage(ctrl)
	buildDirectoryCreator := mock.NewMockBuildDirectoryCreator(ctrl)
	runner := mock.NewMockRunner(ctrl)
	clock := mock.NewMockClock(ctrl)
	localBuildExecutor := builder.NewLocalBuildExecutor(contentAddressableStorage, buildDirectoryCreator, runner, clock, time.Hour, time.Hour, nil)

	filePool := mock.NewMockFilePool(ctrl)
	metadata := make(chan *remoteworker.CurrentState_Executing, 10)
	executeResponse := localBuildExecutor.Execute(
		ctx,
		filePool,
		"netbsd",
		&remoteworker.DesiredState_Executing{
			ActionDigest: &remoteexecution.Digest{
				Hash:      "5555555555555555555555555555555555555555555555555555555555555555",
				SizeBytes: 7,
			},
			Command: &remoteexecution.Command{
				Arguments: []string{"touch", "foo"},
				EnvironmentVariables: []*remoteexecution.Command_EnvironmentVariable{
					{Name: "PATH", Value: "/bin:/usr/bin"},
				},
				OutputFiles: []string{"foo"},
			},
		},
		metadata)
	require.Equal(t, &remoteexecution.ExecuteResponse{
		Result: &remoteexecution.ActionResult{
			ExecutionMetadata: &remoteexecution.ExecutedActionMetadata{},
		},
		Status: status.New(codes.InvalidArgument, "Request does not contain an action").Proto(),
	}, executeResponse)
}

func TestLocalBuildExecutorMissingCommand(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)
	defer ctrl.Finish()
	contentAddressableStorage := mock.NewMockContentAddressableStorage(ctrl)
	buildDirectoryCreator := mock.NewMockBuildDirectoryCreator(ctrl)
	runner := mock.NewMockRunner(ctrl)
	clock := mock.NewMockClock(ctrl)
	localBuildExecutor := builder.NewLocalBuildExecutor(contentAddressableStorage, buildDirectoryCreator, runner, clock, time.Hour, time.Hour, nil)

	filePool := mock.NewMockFilePool(ctrl)
	metadata := make(chan *remoteworker.CurrentState_Executing, 10)
	executeResponse := localBuildExecutor.Execute(
		ctx,
		filePool,
		"netbsd",
		&remoteworker.DesiredState_Executing{
			ActionDigest: &remoteexecution.Digest{
				Hash:      "5555555555555555555555555555555555555555555555555555555555555555",
				SizeBytes: 7,
			},
			Action: &remoteexecution.Action{
				InputRootDigest: &remoteexecution.Digest{
					Hash:      "7777777777777777777777777777777777777777777777777777777777777777",
					SizeBytes: 42,
				},
			},
		},
		metadata)
	require.Equal(t, &remoteexecution.ExecuteResponse{
		Result: &remoteexecution.ActionResult{
			ExecutionMetadata: &remoteexecution.ExecutedActionMetadata{},
		},
		Status: status.New(codes.InvalidArgument, "Request does not contain a command").Proto(),
	}, executeResponse)
}

func TestLocalBuildExecutorBuildDirectoryCreatorFailedFailed(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)
	defer ctrl.Finish()
	contentAddressableStorage := mock.NewMockContentAddressableStorage(ctrl)
	buildDirectoryCreator := mock.NewMockBuildDirectoryCreator(ctrl)
	buildDirectoryCreator.EXPECT().GetBuildDirectory(
		digest.MustNewDigest("netbsd", "5555555555555555555555555555555555555555555555555555555555555555", 7),
		false,
	).Return(nil, "", status.Error(codes.InvalidArgument, "Platform requirements not provided"))
	runner := mock.NewMockRunner(ctrl)
	clock := mock.NewMockClock(ctrl)
	localBuildExecutor := builder.NewLocalBuildExecutor(contentAddressableStorage, buildDirectoryCreator, runner, clock, time.Hour, time.Hour, nil)

	filePool := mock.NewMockFilePool(ctrl)
	metadata := make(chan *remoteworker.CurrentState_Executing, 10)
	executeResponse := localBuildExecutor.Execute(
		ctx,
		filePool,
		"netbsd",
		&remoteworker.DesiredState_Executing{
			ActionDigest: &remoteexecution.Digest{
				Hash:      "5555555555555555555555555555555555555555555555555555555555555555",
				SizeBytes: 7,
			},
			Action: &remoteexecution.Action{
				InputRootDigest: &remoteexecution.Digest{
					Hash:      "7777777777777777777777777777777777777777777777777777777777777777",
					SizeBytes: 42,
				},
			},
			Command: &remoteexecution.Command{
				Arguments: []string{"touch", "foo"},
				EnvironmentVariables: []*remoteexecution.Command_EnvironmentVariable{
					{Name: "PATH", Value: "/bin:/usr/bin"},
				},
				OutputFiles: []string{"foo"},
			},
		},
		metadata)
	require.Equal(t, &remoteexecution.ExecuteResponse{
		Result: &remoteexecution.ActionResult{
			ExecutionMetadata: &remoteexecution.ExecutedActionMetadata{},
		},
		Status: status.New(codes.InvalidArgument, "Failed to acquire build environment: Platform requirements not provided").Proto(),
	}, executeResponse)
}

func TestLocalBuildExecutorInputRootPopulationFailed(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)
	defer ctrl.Finish()
	contentAddressableStorage := mock.NewMockContentAddressableStorage(ctrl)
	buildDirectoryCreator := mock.NewMockBuildDirectoryCreator(ctrl)
	buildDirectory := mock.NewMockBuildDirectory(ctrl)
	buildDirectoryCreator.EXPECT().GetBuildDirectory(
		digest.MustNewDigest("netbsd", "5555555555555555555555555555555555555555555555555555555555555555", 7),
		false,
	).Return(buildDirectory, ".", nil)
	filePool := mock.NewMockFilePool(ctrl)
	buildDirectory.EXPECT().InstallHooks(filePool)
	buildDirectory.EXPECT().Mkdir("root", os.FileMode(0777))
	inputRootDirectory := mock.NewMockBuildDirectory(ctrl)
	buildDirectory.EXPECT().EnterBuildDirectory("root").Return(inputRootDirectory, nil)
	inputRootDirectory.EXPECT().MergeDirectoryContents(
		ctx,
		digest.MustNewDigest("netbsd", "7777777777777777777777777777777777777777777777777777777777777777", 42),
	).Return(status.Error(codes.FailedPrecondition, "Some input files could not be found"))
	inputRootDirectory.EXPECT().Close()
	buildDirectory.EXPECT().Close()
	runner := mock.NewMockRunner(ctrl)
	clock := mock.NewMockClock(ctrl)
	localBuildExecutor := builder.NewLocalBuildExecutor(contentAddressableStorage, buildDirectoryCreator, runner, clock, time.Hour, time.Hour, nil)

	metadata := make(chan *remoteworker.CurrentState_Executing, 10)
	executeResponse := localBuildExecutor.Execute(
		ctx,
		filePool,
		"netbsd",
		&remoteworker.DesiredState_Executing{
			ActionDigest: &remoteexecution.Digest{
				Hash:      "5555555555555555555555555555555555555555555555555555555555555555",
				SizeBytes: 7,
			},
			Action: &remoteexecution.Action{
				InputRootDigest: &remoteexecution.Digest{
					Hash:      "7777777777777777777777777777777777777777777777777777777777777777",
					SizeBytes: 42,
				},
			},
			Command: &remoteexecution.Command{
				Arguments: []string{"touch", "foo"},
				EnvironmentVariables: []*remoteexecution.Command_EnvironmentVariable{
					{Name: "PATH", Value: "/bin:/usr/bin"},
				},
				OutputFiles: []string{"foo"},
			},
		},
		metadata)
	require.Equal(t, &remoteexecution.ExecuteResponse{
		Result: &remoteexecution.ActionResult{
			ExecutionMetadata: &remoteexecution.ExecutedActionMetadata{},
		},
		Status: status.New(codes.FailedPrecondition, "Some input files could not be found").Proto(),
	}, executeResponse)
}

func TestLocalBuildExecutorOutputDirectoryCreationFailure(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)
	defer ctrl.Finish()
	contentAddressableStorage := mock.NewMockContentAddressableStorage(ctrl)
	buildDirectoryCreator := mock.NewMockBuildDirectoryCreator(ctrl)
	buildDirectory := mock.NewMockBuildDirectory(ctrl)
	buildDirectoryCreator.EXPECT().GetBuildDirectory(
		digest.MustNewDigest("fedora", "5555555555555555555555555555555555555555555555555555555555555555", 7),
		false,
	).Return(buildDirectory, ".", nil)
	filePool := mock.NewMockFilePool(ctrl)
	buildDirectory.EXPECT().InstallHooks(filePool)
	buildDirectory.EXPECT().Mkdir("root", os.FileMode(0777))
	inputRootDirectory := mock.NewMockBuildDirectory(ctrl)
	buildDirectory.EXPECT().EnterBuildDirectory("root").Return(inputRootDirectory, nil)
	inputRootDirectory.EXPECT().MergeDirectoryContents(
		ctx,
		digest.MustNewDigest("fedora", "7777777777777777777777777777777777777777777777777777777777777777", 42),
	).Return(nil)
	inputRootDirectory.EXPECT().Mkdir("foo", os.FileMode(0777)).Return(status.Error(codes.Internal, "Out of disk space"))
	inputRootDirectory.EXPECT().Close()
	buildDirectory.EXPECT().Close()
	runner := mock.NewMockRunner(ctrl)
	clock := mock.NewMockClock(ctrl)
	localBuildExecutor := builder.NewLocalBuildExecutor(contentAddressableStorage, buildDirectoryCreator, runner, clock, time.Hour, time.Hour, nil)

	metadata := make(chan *remoteworker.CurrentState_Executing, 10)
	executeResponse := localBuildExecutor.Execute(
		ctx,
		filePool,
		"fedora",
		&remoteworker.DesiredState_Executing{
			ActionDigest: &remoteexecution.Digest{
				Hash:      "5555555555555555555555555555555555555555555555555555555555555555",
				SizeBytes: 7,
			},
			Action: &remoteexecution.Action{
				InputRootDigest: &remoteexecution.Digest{
					Hash:      "7777777777777777777777777777777777777777777777777777777777777777",
					SizeBytes: 42,
				},
			},
			Command: &remoteexecution.Command{
				Arguments: []string{"touch", "foo"},
				EnvironmentVariables: []*remoteexecution.Command_EnvironmentVariable{
					{Name: "PATH", Value: "/bin:/usr/bin"},
				},
				OutputFiles: []string{"foo/bar/baz"},
			},
		},
		metadata)
	require.Equal(t, &remoteexecution.ExecuteResponse{
		Result: &remoteexecution.ActionResult{
			ExecutionMetadata: &remoteexecution.ExecutedActionMetadata{},
		},
		Status: status.New(codes.Internal, "Failed to create output directory \"foo\": Out of disk space").Proto(),
	}, executeResponse)
}

func TestLocalBuildExecutorOutputSymlinkReadingFailure(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)
	defer ctrl.Finish()
	contentAddressableStorage := mock.NewMockContentAddressableStorage(ctrl)
	buildDirectory := mock.NewMockBuildDirectory(ctrl)
	contentAddressableStorage.EXPECT().PutFile(ctx, buildDirectory, "stdout", gomock.Any()).Return(
		digest.MustNewDigest("nintendo64", "0000000000000000000000000000000000000000000000000000000000000005", 567),
		nil)
	contentAddressableStorage.EXPECT().PutFile(ctx, buildDirectory, "stderr", gomock.Any()).Return(
		digest.MustNewDigest("nintendo64", "0000000000000000000000000000000000000000000000000000000000000006", 678),
		nil)
	buildDirectoryCreator := mock.NewMockBuildDirectoryCreator(ctrl)
	buildDirectoryCreator.EXPECT().GetBuildDirectory(
		digest.MustNewDigest("nintendo64", "5555555555555555555555555555555555555555555555555555555555555555", 7),
		false,
	).Return(buildDirectory, ".", nil)
	filePool := mock.NewMockFilePool(ctrl)
	buildDirectory.EXPECT().InstallHooks(filePool)
	buildDirectory.EXPECT().Mkdir("root", os.FileMode(0777))
	inputRootDirectory := mock.NewMockBuildDirectory(ctrl)
	buildDirectory.EXPECT().EnterBuildDirectory("root").Return(inputRootDirectory, nil)
	inputRootDirectory.EXPECT().MergeDirectoryContents(
		ctx,
		digest.MustNewDigest("nintendo64", "7777777777777777777777777777777777777777777777777777777777777777", 42),
	).Return(nil)
	inputRootDirectory.EXPECT().Mkdir("foo", os.FileMode(0777)).Return(nil)
	buildDirectory.EXPECT().Mkdir("tmp", os.FileMode(0777))
	runner := mock.NewMockRunner(ctrl)
	runner.EXPECT().Run(gomock.Any(), &runner_pb.RunRequest{
		Arguments:            []string{"touch", "foo"},
		EnvironmentVariables: map[string]string{"PATH": "/bin:/usr/bin"},
		WorkingDirectory:     "",
		StdoutPath:           "stdout",
		StderrPath:           "stderr",
		InputRootDirectory:   "root",
		TemporaryDirectory:   "tmp",
	}).Return(&runner_pb.RunResponse{
		ExitCode: 0,
	}, nil)
	fooDirectory := mock.NewMockDirectoryCloser(ctrl)
	inputRootDirectory.EXPECT().EnterDirectory("foo").Return(fooDirectory, nil)
	fooDirectory.EXPECT().ReadDir().Return([]filesystem.FileInfo{
		filesystem.NewFileInfo("bar", filesystem.FileTypeSymlink),
	}, nil)
	fooDirectory.EXPECT().Readlink("bar").Return("", status.Error(codes.Internal, "Cosmic rays caused interference"))
	fooDirectory.EXPECT().Close()
	inputRootDirectory.EXPECT().Close()
	buildDirectory.EXPECT().Close()
	clock := mock.NewMockClock(ctrl)
	clock.EXPECT().NewContextWithTimeout(gomock.Any(), time.Hour).DoAndReturn(func(parent context.Context, timeout time.Duration) (context.Context, context.CancelFunc) {
		return context.WithCancel(parent)
	})
	localBuildExecutor := builder.NewLocalBuildExecutor(contentAddressableStorage, buildDirectoryCreator, runner, clock, time.Hour, time.Hour, nil)

	metadata := make(chan *remoteworker.CurrentState_Executing, 10)
	executeResponse := localBuildExecutor.Execute(
		ctx,
		filePool,
		"nintendo64",
		&remoteworker.DesiredState_Executing{
			ActionDigest: &remoteexecution.Digest{
				Hash:      "5555555555555555555555555555555555555555555555555555555555555555",
				SizeBytes: 7,
			},
			Action: &remoteexecution.Action{
				InputRootDigest: &remoteexecution.Digest{
					Hash:      "7777777777777777777777777777777777777777777777777777777777777777",
					SizeBytes: 42,
				},
			},
			Command: &remoteexecution.Command{
				Arguments: []string{"touch", "foo"},
				EnvironmentVariables: []*remoteexecution.Command_EnvironmentVariable{
					{Name: "PATH", Value: "/bin:/usr/bin"},
				},
				OutputDirectories: []string{"foo"},
			},
		},
		metadata)
	require.Equal(t, &remoteexecution.ExecuteResponse{
		Result: &remoteexecution.ActionResult{
			StdoutDigest: &remoteexecution.Digest{
				Hash:      "0000000000000000000000000000000000000000000000000000000000000005",
				SizeBytes: 567,
			},
			StderrDigest: &remoteexecution.Digest{
				Hash:      "0000000000000000000000000000000000000000000000000000000000000006",
				SizeBytes: 678,
			},
			ExecutionMetadata: &remoteexecution.ExecutedActionMetadata{},
		},
		Status: status.New(codes.Internal, "Failed to read output symlink \"foo/bar\": Cosmic rays caused interference").Proto(),
	}, executeResponse)
}

// TestLocalBuildExecutorSuccess tests a full invocation of a simple
// build step, equivalent to compiling a simple C++ file.
func TestLocalBuildExecutorSuccess(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)
	defer ctrl.Finish()

	// File system operations that should occur against the input
	// root directory. Creation of
	// bazel-out/k8-fastbuild/bin/_objs/hello.
	inputRootDirectory := mock.NewMockBuildDirectory(ctrl)
	inputRootDirectory.EXPECT().Mkdir("bazel-out", os.FileMode(0777)).Return(nil)
	bazelOutDirectory := mock.NewMockDirectoryCloser(ctrl)
	inputRootDirectory.EXPECT().EnterDirectory("bazel-out").Return(bazelOutDirectory, nil)
	bazelOutDirectory.EXPECT().Close()
	bazelOutDirectory.EXPECT().Mkdir("k8-fastbuild", os.FileMode(0777)).Return(nil)
	k8FastbuildDirectory := mock.NewMockBuildDirectory(ctrl)
	bazelOutDirectory.EXPECT().EnterDirectory("k8-fastbuild").Return(k8FastbuildDirectory, nil)
	k8FastbuildDirectory.EXPECT().Close()
	k8FastbuildDirectory.EXPECT().Mkdir("bin", os.FileMode(0777)).Return(nil)
	binDirectory := mock.NewMockDirectoryCloser(ctrl)
	k8FastbuildDirectory.EXPECT().EnterDirectory("bin").Return(binDirectory, nil)
	binDirectory.EXPECT().Close()
	binDirectory.EXPECT().Mkdir("_objs", os.FileMode(0777)).Return(nil)
	objsDirectory := mock.NewMockDirectoryCloser(ctrl)
	binDirectory.EXPECT().EnterDirectory("_objs").Return(objsDirectory, nil)
	objsDirectory.EXPECT().Close()
	objsDirectory.EXPECT().Mkdir("hello", os.FileMode(0777)).Return(nil)
	helloDirectory := mock.NewMockDirectoryCloser(ctrl)
	objsDirectory.EXPECT().EnterDirectory("hello").Return(helloDirectory, nil)
	helloDirectory.EXPECT().Close()
	helloDirectory.EXPECT().Lstat("hello.pic.d").Return(filesystem.NewFileInfo("hello.pic.d", filesystem.FileTypeRegularFile), nil)
	helloDirectory.EXPECT().Lstat("hello.pic.o").Return(filesystem.NewFileInfo("hello.pic.o", filesystem.FileTypeExecutableFile), nil)

	// Read operations against the Content Addressable Storage.
	contentAddressableStorage := mock.NewMockContentAddressableStorage(ctrl)

	// Write operations against the Content Addressable Storage.
	buildDirectory := mock.NewMockBuildDirectory(ctrl)
	contentAddressableStorage.EXPECT().PutFile(ctx, buildDirectory, "stdout", gomock.Any()).Return(
		digest.MustNewDigest("ubuntu1804", "0000000000000000000000000000000000000000000000000000000000000005", 567),
		nil)
	contentAddressableStorage.EXPECT().PutFile(ctx, buildDirectory, "stderr", gomock.Any()).Return(
		digest.MustNewDigest("ubuntu1804", "0000000000000000000000000000000000000000000000000000000000000006", 678),
		nil)
	contentAddressableStorage.EXPECT().PutFile(ctx, helloDirectory, "hello.pic.d", gomock.Any()).Return(
		digest.MustNewDigest("ubuntu1804", "0000000000000000000000000000000000000000000000000000000000000007", 789),
		nil)
	contentAddressableStorage.EXPECT().PutFile(ctx, helloDirectory, "hello.pic.o", gomock.Any()).Return(
		digest.MustNewDigest("ubuntu1804", "0000000000000000000000000000000000000000000000000000000000000008", 890),
		nil)

	// Command execution.
	buildDirectoryCreator := mock.NewMockBuildDirectoryCreator(ctrl)
	buildDirectoryCreator.EXPECT().GetBuildDirectory(
		digest.MustNewDigest("ubuntu1804", "0000000000000000000000000000000000000000000000000000000000000001", 123),
		false,
	).Return(buildDirectory, "0000000000000000", nil)
	filePool := mock.NewMockFilePool(ctrl)
	buildDirectory.EXPECT().InstallHooks(filePool)
	buildDirectory.EXPECT().Mkdir("root", os.FileMode(0777))
	buildDirectory.EXPECT().EnterBuildDirectory("root").Return(inputRootDirectory, nil)
	inputRootDirectory.EXPECT().MergeDirectoryContents(
		ctx,
		digest.MustNewDigest("ubuntu1804", "0000000000000000000000000000000000000000000000000000000000000003", 345),
	).Return(nil)
	inputRootDirectory.EXPECT().Mkdir("dev", os.FileMode(0777))
	inputRootDevDirectory := mock.NewMockDirectoryCloser(ctrl)
	inputRootDirectory.EXPECT().EnterDirectory("dev").Return(inputRootDevDirectory, nil)
	inputRootDevDirectory.EXPECT().Mknod("null", os.FileMode(os.ModeDevice|os.ModeCharDevice|0666), 259).Return(nil)
	inputRootDevDirectory.EXPECT().Close()
	buildDirectory.EXPECT().Mkdir("tmp", os.FileMode(0777))
	resourceUsage, err := ptypes.MarshalAny(&empty.Empty{})
	require.NoError(t, err)
	runner := mock.NewMockRunner(ctrl)
	runner.EXPECT().Run(gomock.Any(), &runner_pb.RunRequest{
		Arguments: []string{
			"/usr/local/bin/clang",
			"-MD",
			"-MF",
			"bazel-out/k8-fastbuild/bin/_objs/hello/hello.pic.d",
			"-c",
			"hello.cc",
			"-o",
			"bazel-out/k8-fastbuild/bin/_objs/hello/hello.pic.o",
		},
		EnvironmentVariables: map[string]string{
			"BAZEL_DO_NOT_DETECT_CPP_TOOLCHAIN": "1",
			"PATH":                              "/bin:/usr/bin",
			"PWD":                               "/proc/self/cwd",
		},
		WorkingDirectory:   "",
		StdoutPath:         "0000000000000000/stdout",
		StderrPath:         "0000000000000000/stderr",
		InputRootDirectory: "0000000000000000/root",
		TemporaryDirectory: "0000000000000000/tmp",
	}).Return(&runner_pb.RunResponse{
		ExitCode:      0,
		ResourceUsage: []*any.Any{resourceUsage},
	}, nil)
	inputRootDirectory.EXPECT().Close()
	buildDirectory.EXPECT().Close()
	clock := mock.NewMockClock(ctrl)
	clock.EXPECT().NewContextWithTimeout(gomock.Any(), time.Hour).DoAndReturn(func(parent context.Context, timeout time.Duration) (context.Context, context.CancelFunc) {
		return context.WithCancel(parent)
	})
	inputRootCharacterDevices := map[string]int{"null": 259}
	localBuildExecutor := builder.NewLocalBuildExecutor(contentAddressableStorage, buildDirectoryCreator, runner, clock, time.Hour, time.Hour, inputRootCharacterDevices)

	metadata := make(chan *remoteworker.CurrentState_Executing, 10)
	executeResponse := localBuildExecutor.Execute(
		ctx,
		filePool,
		"ubuntu1804",
		&remoteworker.DesiredState_Executing{
			ActionDigest: &remoteexecution.Digest{
				Hash:      "0000000000000000000000000000000000000000000000000000000000000001",
				SizeBytes: 123,
			},
			Action: &remoteexecution.Action{
				InputRootDigest: &remoteexecution.Digest{
					Hash:      "0000000000000000000000000000000000000000000000000000000000000003",
					SizeBytes: 345,
				},
			},
			Command: &remoteexecution.Command{
				Arguments: []string{
					"/usr/local/bin/clang",
					"-MD",
					"-MF",
					"bazel-out/k8-fastbuild/bin/_objs/hello/hello.pic.d",
					"-c",
					"hello.cc",
					"-o",
					"bazel-out/k8-fastbuild/bin/_objs/hello/hello.pic.o",
				},
				EnvironmentVariables: []*remoteexecution.Command_EnvironmentVariable{
					{Name: "BAZEL_DO_NOT_DETECT_CPP_TOOLCHAIN", Value: "1"},
					{Name: "PATH", Value: "/bin:/usr/bin"},
					{Name: "PWD", Value: "/proc/self/cwd"},
				},
				OutputFiles: []string{
					"bazel-out/k8-fastbuild/bin/_objs/hello/hello.pic.d",
					"bazel-out/k8-fastbuild/bin/_objs/hello/hello.pic.o",
				},
				Platform: &remoteexecution.Platform{
					Properties: []*remoteexecution.Platform_Property{
						{
							Name:  "container-image",
							Value: "docker://gcr.io/cloud-marketplace/google/rbe-debian8@sha256:4893599fb00089edc8351d9c26b31d3f600774cb5addefb00c70fdb6ca797abf",
						},
					},
				},
			},
		},
		metadata)
	require.Equal(t, &remoteexecution.ExecuteResponse{
		Result: &remoteexecution.ActionResult{
			OutputFiles: []*remoteexecution.OutputFile{
				{
					Path: "bazel-out/k8-fastbuild/bin/_objs/hello/hello.pic.d",
					Digest: &remoteexecution.Digest{
						Hash:      "0000000000000000000000000000000000000000000000000000000000000007",
						SizeBytes: 789,
					},
				},
				{
					Path: "bazel-out/k8-fastbuild/bin/_objs/hello/hello.pic.o",
					Digest: &remoteexecution.Digest{
						Hash:      "0000000000000000000000000000000000000000000000000000000000000008",
						SizeBytes: 890,
					},
					IsExecutable: true,
				},
			},
			StdoutDigest: &remoteexecution.Digest{
				Hash:      "0000000000000000000000000000000000000000000000000000000000000005",
				SizeBytes: 567,
			},
			StderrDigest: &remoteexecution.Digest{
				Hash:      "0000000000000000000000000000000000000000000000000000000000000006",
				SizeBytes: 678,
			},
			ExecutionMetadata: &remoteexecution.ExecutedActionMetadata{
				AuxiliaryMetadata: []*any.Any{resourceUsage},
			},
		},
	}, executeResponse)
}

// TestLocalBuildExecutorSuccess tests a full invocation of a simple
// build step, equivalent to compiling a simple C++ file.
// This adds a working directory to the commands, with output files
// produced relative to that working directory.
func TestLocalBuildExecutorWithWorkingDirectorySuccess(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)
	defer ctrl.Finish()

	// File system operations that should occur against the build directory.

	// Creation of output directory's parent and the directory itself
	// This will be the same directory used by the first output file
	inputRootDirectory := mock.NewMockBuildDirectory(ctrl)
	inputRootDirectory.EXPECT().Mkdir("outputParent", os.FileMode(0777)).Return(nil)
	outputParentDirectory0 := mock.NewMockDirectoryCloser(ctrl)
	inputRootDirectory.EXPECT().EnterDirectory("outputParent").Return(outputParentDirectory0, nil)
	outputParentDirectory0.EXPECT().Close()
	outputParentDirectory0.EXPECT().Mkdir("output", os.FileMode(0777)).Return(nil)
	outputDirectory0 := mock.NewMockDirectoryCloser(ctrl)
	outputParentDirectory0.EXPECT().EnterDirectory("output").Return(outputDirectory0, nil)
	outputDirectory0.EXPECT().Close()

	// Creation of the parent directory of second output file
	inputRootDirectory.EXPECT().Mkdir("outputParent", os.FileMode(0777)).Return(syscall.EEXIST)
	outputParentDirectory1 := mock.NewMockDirectoryCloser(ctrl)
	inputRootDirectory.EXPECT().EnterDirectory("outputParent").Return(outputParentDirectory1, nil)
	outputParentDirectory1.EXPECT().Mkdir("output", os.FileMode(0777)).Return(syscall.EEXIST)
	outputDirectory1 := mock.NewMockDirectoryCloser(ctrl)
	outputParentDirectory1.EXPECT().EnterDirectory("output").Return(outputDirectory1, nil)
	outputParentDirectory1.EXPECT().Close()
	outputDirectory1.EXPECT().Close()
	outputDirectory1.EXPECT().Mkdir("foo", os.FileMode(0777)).Return(nil)
	fooDirectory := mock.NewMockDirectoryCloser(ctrl)
	outputDirectory1.EXPECT().EnterDirectory("foo").Return(fooDirectory, nil)
	fooDirectory.EXPECT().Close()
	fooDirectory.EXPECT().Mkdir("objects", os.FileMode(0777)).Return(nil)
	objectsDirectory := mock.NewMockDirectoryCloser(ctrl)
	fooDirectory.EXPECT().EnterDirectory("objects").Return(objectsDirectory, nil)
	objectsDirectory.EXPECT().Close()

	outputDirectory0.EXPECT().ReadDir().Return(
		[]filesystem.FileInfo{
			filesystem.NewFileInfo("output.file", filesystem.FileTypeRegularFile),
		}, nil)
	outputDirectory0.EXPECT().Lstat("hello.pic.d").Return(
		filesystem.NewFileInfo("hello.pic.d", filesystem.FileTypeRegularFile),
		nil)
	objectsDirectory.EXPECT().Lstat("hello.pic.o").Return(
		filesystem.NewFileInfo("hello.pic.o", filesystem.FileTypeExecutableFile),
		nil)

	// Read operations against the Content Addressable Storage.
	contentAddressableStorage := mock.NewMockContentAddressableStorage(ctrl)

	// Write operations against the Content Addressable Storage.
	buildDirectory := mock.NewMockBuildDirectory(ctrl)
	contentAddressableStorage.EXPECT().PutFile(ctx, buildDirectory, "stdout", gomock.Any()).Return(
		digest.MustNewDigest("ubuntu1804", "0000000000000000000000000000000000000000000000000000000000000005", 567),
		nil)
	contentAddressableStorage.EXPECT().PutFile(ctx, buildDirectory, "stderr", gomock.Any()).Return(
		digest.MustNewDigest("ubuntu1804", "0000000000000000000000000000000000000000000000000000000000000006", 678),
		nil)
	contentAddressableStorage.EXPECT().PutFile(ctx, outputDirectory0, "hello.pic.d", gomock.Any()).Return(
		digest.MustNewDigest("ubuntu1804", "0000000000000000000000000000000000000000000000000000000000000007", 789),
		nil)
	contentAddressableStorage.EXPECT().PutFile(ctx, objectsDirectory, "hello.pic.o", gomock.Any()).Return(
		digest.MustNewDigest("ubuntu1804", "0000000000000000000000000000000000000000000000000000000000000008", 890),
		nil)
	contentAddressableStorage.EXPECT().PutFile(ctx, outputDirectory0, "output.file", gomock.Any()).Return(
		digest.MustNewDigest("ubuntu1804", "0000000000000000000000000000000000000000000000000000000000000009", 901),
		nil)
	contentAddressableStorage.EXPECT().PutTree(ctx,
		&remoteexecution.Tree{
			Root: &remoteexecution.Directory{
				Files: []*remoteexecution.FileNode{
					{
						Name: "output.file",
						Digest: &remoteexecution.Digest{
							Hash:      "0000000000000000000000000000000000000000000000000000000000000009",
							SizeBytes: 901,
						},
					},
				},
			},
		}, gomock.Any()).Return(
		digest.MustNewDigest("ubuntu1804", "0000000000000000000000000000000000000000000000000000000000000010", 902),
		nil)

	// Command execution.
	buildDirectoryCreator := mock.NewMockBuildDirectoryCreator(ctrl)
	buildDirectoryCreator.EXPECT().GetBuildDirectory(
		digest.MustNewDigest("ubuntu1804", "0000000000000000000000000000000000000000000000000000000000000001", 123),
		false,
	).Return(buildDirectory, ".", nil)
	filePool := mock.NewMockFilePool(ctrl)
	buildDirectory.EXPECT().InstallHooks(filePool)
	buildDirectory.EXPECT().Mkdir("root", os.FileMode(0777))
	buildDirectory.EXPECT().EnterBuildDirectory("root").Return(inputRootDirectory, nil)
	inputRootDirectory.EXPECT().MergeDirectoryContents(
		ctx,
		digest.MustNewDigest("ubuntu1804", "0000000000000000000000000000000000000000000000000000000000000003", 345),
	).Return(nil)
	buildDirectory.EXPECT().Mkdir("tmp", os.FileMode(0777))
	resourceUsage, err := ptypes.MarshalAny(&empty.Empty{})
	require.NoError(t, err)
	runner := mock.NewMockRunner(ctrl)
	runner.EXPECT().Run(gomock.Any(), &runner_pb.RunRequest{
		Arguments: []string{
			"/usr/local/bin/clang",
			"-MD",
			"-MF",
			"../hello.pic.d",
			"-c",
			"hello.cc",
			"-o",
			"objects/hello.pic.o",
		},
		EnvironmentVariables: map[string]string{
			"PATH": "/bin:/usr/bin",
			"PWD":  "/proc/self/cwd",
		},
		WorkingDirectory:   "outputParent/output/foo",
		StdoutPath:         "stdout",
		StderrPath:         "stderr",
		InputRootDirectory: "root",
		TemporaryDirectory: "tmp",
	}).Return(&runner_pb.RunResponse{
		ExitCode:      0,
		ResourceUsage: []*any.Any{resourceUsage},
	}, nil)
	inputRootDirectory.EXPECT().Close()
	buildDirectory.EXPECT().Close()
	clock := mock.NewMockClock(ctrl)
	clock.EXPECT().NewContextWithTimeout(gomock.Any(), time.Hour).DoAndReturn(func(parent context.Context, timeout time.Duration) (context.Context, context.CancelFunc) {
		return context.WithCancel(parent)
	})
	localBuildExecutor := builder.NewLocalBuildExecutor(contentAddressableStorage, buildDirectoryCreator, runner, clock, time.Hour, time.Hour, nil)

	metadata := make(chan *remoteworker.CurrentState_Executing, 10)
	executeResponse := localBuildExecutor.Execute(
		ctx,
		filePool,
		"ubuntu1804",
		&remoteworker.DesiredState_Executing{
			ActionDigest: &remoteexecution.Digest{
				Hash:      "0000000000000000000000000000000000000000000000000000000000000001",
				SizeBytes: 123,
			},
			Action: &remoteexecution.Action{
				InputRootDigest: &remoteexecution.Digest{
					Hash:      "0000000000000000000000000000000000000000000000000000000000000003",
					SizeBytes: 345,
				},
			},
			Command: &remoteexecution.Command{
				Arguments: []string{
					"/usr/local/bin/clang",
					"-MD",
					"-MF",
					"../hello.pic.d",
					"-c",
					"hello.cc",
					"-o",
					"objects/hello.pic.o",
				},
				EnvironmentVariables: []*remoteexecution.Command_EnvironmentVariable{
					{Name: "PATH", Value: "/bin:/usr/bin"},
					{Name: "PWD", Value: "/proc/self/cwd"},
				},
				OutputFiles: []string{
					"../hello.pic.d",
					"objects/hello.pic.o",
				},
				OutputDirectories: []string{
					"..",
				},
				Platform: &remoteexecution.Platform{
					Properties: []*remoteexecution.Platform_Property{
						{
							Name:  "container-image",
							Value: "docker://gcr.io/cloud-marketplace/google/rbe-debian8@sha256:4893599fb00089edc8351d9c26b31d3f600774cb5addefb00c70fdb6ca797abf",
						},
					},
				},
				WorkingDirectory: "outputParent/output/foo",
			},
		},
		metadata)
	require.Equal(t, &remoteexecution.ExecuteResponse{
		Result: &remoteexecution.ActionResult{
			OutputFiles: []*remoteexecution.OutputFile{
				{
					Path: "../hello.pic.d",
					Digest: &remoteexecution.Digest{
						Hash:      "0000000000000000000000000000000000000000000000000000000000000007",
						SizeBytes: 789,
					},
				},
				{
					Path: "objects/hello.pic.o",
					Digest: &remoteexecution.Digest{
						Hash:      "0000000000000000000000000000000000000000000000000000000000000008",
						SizeBytes: 890,
					},
					IsExecutable: true,
				},
			},
			OutputDirectories: []*remoteexecution.OutputDirectory{
				{
					Path: "..",
					TreeDigest: &remoteexecution.Digest{
						Hash:      "0000000000000000000000000000000000000000000000000000000000000010",
						SizeBytes: 902,
					},
				},
			},
			StdoutDigest: &remoteexecution.Digest{
				Hash:      "0000000000000000000000000000000000000000000000000000000000000005",
				SizeBytes: 567,
			},
			StderrDigest: &remoteexecution.Digest{
				Hash:      "0000000000000000000000000000000000000000000000000000000000000006",
				SizeBytes: 678,
			},
			ExecutionMetadata: &remoteexecution.ExecutedActionMetadata{
				AuxiliaryMetadata: []*any.Any{resourceUsage},
			},
		},
	}, executeResponse)
}

func TestLocalBuildExecutorCachingInvalidTimeout(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)
	defer ctrl.Finish()

	contentAddressableStorage := mock.NewMockContentAddressableStorage(ctrl)
	buildDirectoryCreator := mock.NewMockBuildDirectoryCreator(ctrl)
	runner := mock.NewMockRunner(ctrl)
	clock := mock.NewMockClock(ctrl)
	localBuildExecutor := builder.NewLocalBuildExecutor(contentAddressableStorage, buildDirectoryCreator, runner, clock, time.Hour, time.Hour, nil)

	// Execution should fail, as the number of nanoseconds in the
	// timeout is not within bounds.
	filePool := mock.NewMockFilePool(ctrl)
	metadata := make(chan *remoteworker.CurrentState_Executing, 10)
	executeResponse := localBuildExecutor.Execute(
		ctx,
		filePool,
		"ubuntu1804",
		&remoteworker.DesiredState_Executing{
			ActionDigest: &remoteexecution.Digest{
				Hash:      "0000000000000000000000000000000000000000000000000000000000000001",
				SizeBytes: 123,
			},
			Action: &remoteexecution.Action{
				InputRootDigest: &remoteexecution.Digest{
					Hash:      "0000000000000000000000000000000000000000000000000000000000000003",
					SizeBytes: 345,
				},
				Timeout: &duration.Duration{
					Nanos: 1000000000,
				},
			},
		},
		metadata)
	require.Equal(t, &remoteexecution.ExecuteResponse{
		Result: &remoteexecution.ActionResult{
			ExecutionMetadata: &remoteexecution.ExecutedActionMetadata{},
		},
		Status: status.New(codes.InvalidArgument, "Invalid execution timeout: duration: nanos:1000000000 : nanos out of range").Proto(),
	}, executeResponse)
}

func TestLocalBuildExecutorCachingTimeoutTooHigh(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)
	defer ctrl.Finish()

	contentAddressableStorage := mock.NewMockContentAddressableStorage(ctrl)
	buildDirectoryCreator := mock.NewMockBuildDirectoryCreator(ctrl)
	runner := mock.NewMockRunner(ctrl)
	clock := mock.NewMockClock(ctrl)
	localBuildExecutor := builder.NewLocalBuildExecutor(contentAddressableStorage, buildDirectoryCreator, runner, clock, time.Hour, time.Hour, nil)

	// The protocol states that we must deny requests that have a
	// timeout that is longer than the server's maximum.
	filePool := mock.NewMockFilePool(ctrl)
	metadata := make(chan *remoteworker.CurrentState_Executing, 10)
	executeResponse := localBuildExecutor.Execute(
		ctx,
		filePool,
		"ubuntu1804",
		&remoteworker.DesiredState_Executing{
			ActionDigest: &remoteexecution.Digest{
				Hash:      "0000000000000000000000000000000000000000000000000000000000000001",
				SizeBytes: 123,
			},
			Action: &remoteexecution.Action{
				InputRootDigest: &remoteexecution.Digest{
					Hash:      "0000000000000000000000000000000000000000000000000000000000000003",
					SizeBytes: 345,
				},
				Timeout: &duration.Duration{
					Seconds: 7200,
				},
			},
		},
		metadata)
	require.Equal(t, &remoteexecution.ExecuteResponse{
		Result: &remoteexecution.ActionResult{
			ExecutionMetadata: &remoteexecution.ExecutedActionMetadata{},
		},
		Status: status.New(codes.InvalidArgument, "Execution timeout of 2h0m0s exceeds maximum permitted value of 1h0m0s").Proto(),
	}, executeResponse)
}

func TestLocalBuildExecutorTimeoutDuringExecution(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)
	defer ctrl.Finish()

	// Build directory.
	buildDirectory := mock.NewMockBuildDirectory(ctrl)
	contentAddressableStorage := mock.NewMockContentAddressableStorage(ctrl)
	contentAddressableStorage.EXPECT().PutFile(ctx, buildDirectory, "stdout", gomock.Any()).Return(
		digest.MustNewDigest("ubuntu1804", "0000000000000000000000000000000000000000000000000000000000000005", 567),
		nil)
	contentAddressableStorage.EXPECT().PutFile(ctx, buildDirectory, "stderr", gomock.Any()).Return(
		digest.MustNewDigest("ubuntu1804", "0000000000000000000000000000000000000000000000000000000000000006", 678),
		nil)

	// Build environment.
	buildDirectoryCreator := mock.NewMockBuildDirectoryCreator(ctrl)
	buildDirectoryCreator.EXPECT().GetBuildDirectory(
		digest.MustNewDigest("ubuntu1804", "0000000000000000000000000000000000000000000000000000000000000001", 123),
		false,
	).Return(buildDirectory, ".", nil)
	filePool := mock.NewMockFilePool(ctrl)
	buildDirectory.EXPECT().InstallHooks(filePool)

	// Input root creation.
	buildDirectory.EXPECT().Mkdir("root", os.FileMode(0777))
	inputRootDirectory := mock.NewMockBuildDirectory(ctrl)
	buildDirectory.EXPECT().EnterBuildDirectory("root").Return(inputRootDirectory, nil)
	inputRootDirectory.EXPECT().MergeDirectoryContents(
		ctx,
		digest.MustNewDigest("ubuntu1804", "0000000000000000000000000000000000000000000000000000000000000003", 345),
	).Return(nil)
	buildDirectory.EXPECT().Mkdir("tmp", os.FileMode(0777))

	// Simulate a timeout by running the command with a timeout of
	// zero seconds. This should cause an immediate build failure.
	runner := mock.NewMockRunner(ctrl)
	runner.EXPECT().Run(gomock.Any(), &runner_pb.RunRequest{
		Arguments:            []string{"clang"},
		EnvironmentVariables: map[string]string{},
		WorkingDirectory:     "",
		StdoutPath:           "stdout",
		StderrPath:           "stderr",
		InputRootDirectory:   "root",
		TemporaryDirectory:   "tmp",
	}).DoAndReturn(func(ctx context.Context, request *runner_pb.RunRequest) (*runner_pb.RunResponse, error) {
		<-ctx.Done()
		return nil, util.StatusFromContext(ctx)
	})
	inputRootDirectory.EXPECT().Close()
	buildDirectory.EXPECT().Close()
	clock := mock.NewMockClock(ctrl)
	clock.EXPECT().NewContextWithTimeout(gomock.Any(), time.Hour).DoAndReturn(func(parent context.Context, timeout time.Duration) (context.Context, context.CancelFunc) {
		return context.WithTimeout(parent, 0)
	})
	localBuildExecutor := builder.NewLocalBuildExecutor(contentAddressableStorage, buildDirectoryCreator, runner, clock, time.Hour, time.Hour, nil)

	metadata := make(chan *remoteworker.CurrentState_Executing, 10)
	executeResponse := localBuildExecutor.Execute(
		ctx,
		filePool,
		"ubuntu1804",
		&remoteworker.DesiredState_Executing{
			ActionDigest: &remoteexecution.Digest{
				Hash:      "0000000000000000000000000000000000000000000000000000000000000001",
				SizeBytes: 123,
			},
			Action: &remoteexecution.Action{
				InputRootDigest: &remoteexecution.Digest{
					Hash:      "0000000000000000000000000000000000000000000000000000000000000003",
					SizeBytes: 345,
				},
			},
			Command: &remoteexecution.Command{
				Arguments: []string{"clang"},
			},
		},
		metadata)
	require.Equal(t, &remoteexecution.ExecuteResponse{
		Result: &remoteexecution.ActionResult{
			StdoutDigest: &remoteexecution.Digest{
				Hash:      "0000000000000000000000000000000000000000000000000000000000000005",
				SizeBytes: 567,
			},
			StderrDigest: &remoteexecution.Digest{
				Hash:      "0000000000000000000000000000000000000000000000000000000000000006",
				SizeBytes: 678,
			},
			ExecutionMetadata: &remoteexecution.ExecutedActionMetadata{},
		},
		Status: status.New(codes.DeadlineExceeded, "Failed to run command: context deadline exceeded").Proto(),
	}, executeResponse)
}

func TestLocalBuildExecutorCharacterDeviceNodeCreationFailed(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)
	defer ctrl.Finish()

	// Build directory.
	buildDirectory := mock.NewMockBuildDirectory(ctrl)
	contentAddressableStorage := mock.NewMockContentAddressableStorage(ctrl)

	// Build environment.
	buildDirectoryCreator := mock.NewMockBuildDirectoryCreator(ctrl)
	buildDirectoryCreator.EXPECT().GetBuildDirectory(
		digest.MustNewDigest("ubuntu1804", "0000000000000000000000000000000000000000000000000000000000000001", 123),
		false,
	).Return(buildDirectory, ".", nil)
	filePool := mock.NewMockFilePool(ctrl)
	buildDirectory.EXPECT().InstallHooks(filePool)

	// Input root creation.
	buildDirectory.EXPECT().Mkdir("root", os.FileMode(0777))
	inputRootDirectory := mock.NewMockBuildDirectory(ctrl)
	buildDirectory.EXPECT().EnterBuildDirectory("root").Return(inputRootDirectory, nil)
	inputRootDirectory.EXPECT().MergeDirectoryContents(
		ctx,
		digest.MustNewDigest("ubuntu1804", "0000000000000000000000000000000000000000000000000000000000000003", 345),
	).Return(nil)
	inputRootDirectory.EXPECT().Mkdir("dev", os.FileMode(0777))
	inputRootDevDirectory := mock.NewMockDirectoryCloser(ctrl)
	inputRootDirectory.EXPECT().EnterDirectory("dev").Return(inputRootDevDirectory, nil)
	inputRootDevDirectory.EXPECT().Mknod("null", os.FileMode(os.ModeDevice|os.ModeCharDevice|0666), 259).Return(status.Error(codes.Internal, "Device node creation failed"))
	inputRootDevDirectory.EXPECT().Close()
	inputRootDirectory.EXPECT().Close()
	buildDirectory.EXPECT().Close()
	runner := mock.NewMockRunner(ctrl)
	clock := mock.NewMockClock(ctrl)
	inputRootCharacterDevices := map[string]int{"null": 259}
	localBuildExecutor := builder.NewLocalBuildExecutor(contentAddressableStorage, buildDirectoryCreator, runner, clock, time.Hour, time.Hour, inputRootCharacterDevices)

	metadata := make(chan *remoteworker.CurrentState_Executing, 10)
	executeResponse := localBuildExecutor.Execute(
		ctx,
		filePool,
		"ubuntu1804",
		&remoteworker.DesiredState_Executing{
			ActionDigest: &remoteexecution.Digest{
				Hash:      "0000000000000000000000000000000000000000000000000000000000000001",
				SizeBytes: 123,
			},
			Action: &remoteexecution.Action{
				InputRootDigest: &remoteexecution.Digest{
					Hash:      "0000000000000000000000000000000000000000000000000000000000000003",
					SizeBytes: 345,
				},
			},
			Command: &remoteexecution.Command{
				Arguments: []string{"clang"},
			},
		},
		metadata)
	require.Equal(t, &remoteexecution.ExecuteResponse{
		Result: &remoteexecution.ActionResult{
			ExecutionMetadata: &remoteexecution.ExecutedActionMetadata{},
		},
		Status: status.New(codes.Internal, "Failed to create character device \"null\": Device node creation failed").Proto(),
	}, executeResponse)
}
