package builder_test

import (
	"context"
	"os"
	"testing"
	"time"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-remote-execution/internal/mock"
	"github.com/buildbarn/bb-remote-execution/pkg/builder"
	"github.com/buildbarn/bb-remote-execution/pkg/proto/remoteworker"
	"github.com/buildbarn/bb-remote-execution/pkg/proto/runner"
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

func mustStatus(s *status.Status, err error) *status.Status {
	if err != nil {
		panic("Failed to create status")
	}
	return s
}

func TestLocalBuildExecutorEnvironmentInvalidActionDigest(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)
	defer ctrl.Finish()
	contentAddressableStorage := mock.NewMockContentAddressableStorage(ctrl)
	environmentManager := mock.NewMockManager(ctrl)
	inputRootPopulator := mock.NewMockInputRootPopulator(ctrl)
	clock := mock.NewMockClock(ctrl)
	localBuildExecutor := builder.NewLocalBuildExecutor(contentAddressableStorage, environmentManager, inputRootPopulator, clock, time.Hour, time.Hour)

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

func TestLocalBuildExecutorEnvironmentMissingAction(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)
	defer ctrl.Finish()
	contentAddressableStorage := mock.NewMockContentAddressableStorage(ctrl)
	environmentManager := mock.NewMockManager(ctrl)
	inputRootPopulator := mock.NewMockInputRootPopulator(ctrl)
	clock := mock.NewMockClock(ctrl)
	localBuildExecutor := builder.NewLocalBuildExecutor(contentAddressableStorage, environmentManager, inputRootPopulator, clock, time.Hour, time.Hour)

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

func TestLocalBuildExecutorEnvironmentMissingCommand(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)
	defer ctrl.Finish()
	contentAddressableStorage := mock.NewMockContentAddressableStorage(ctrl)
	environmentManager := mock.NewMockManager(ctrl)
	inputRootPopulator := mock.NewMockInputRootPopulator(ctrl)
	clock := mock.NewMockClock(ctrl)
	localBuildExecutor := builder.NewLocalBuildExecutor(contentAddressableStorage, environmentManager, inputRootPopulator, clock, time.Hour, time.Hour)

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

func TestLocalBuildExecutorEnvironmentAcquireFailed(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)
	defer ctrl.Finish()
	contentAddressableStorage := mock.NewMockContentAddressableStorage(ctrl)
	environmentManager := mock.NewMockManager(ctrl)
	environmentManager.EXPECT().Acquire(
		digest.MustNewDigest("netbsd", "5555555555555555555555555555555555555555555555555555555555555555", 7),
	).Return(nil, status.Error(codes.InvalidArgument, "Platform requirements not provided"))
	inputRootPopulator := mock.NewMockInputRootPopulator(ctrl)
	clock := mock.NewMockClock(ctrl)
	localBuildExecutor := builder.NewLocalBuildExecutor(contentAddressableStorage, environmentManager, inputRootPopulator, clock, time.Hour, time.Hour)

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
	environmentManager := mock.NewMockManager(ctrl)
	environment := mock.NewMockManagedEnvironment(ctrl)
	environmentManager.EXPECT().Acquire(
		digest.MustNewDigest("netbsd", "5555555555555555555555555555555555555555555555555555555555555555", 7),
	).Return(environment, nil)
	inputRootPopulator := mock.NewMockInputRootPopulator(ctrl)
	buildDirectory := mock.NewMockDirectory(ctrl)
	environment.EXPECT().GetBuildDirectory().Return(buildDirectory)
	filePool := mock.NewMockFilePool(ctrl)
	inputRootPopulator.EXPECT().PopulateInputRoot(
		ctx,
		filePool,
		digest.MustNewDigest("netbsd", "7777777777777777777777777777777777777777777777777777777777777777", 42),
		buildDirectory).Return(status.Error(codes.FailedPrecondition, "Some input files could not be found"))
	environment.EXPECT().Release()
	clock := mock.NewMockClock(ctrl)
	localBuildExecutor := builder.NewLocalBuildExecutor(contentAddressableStorage, environmentManager, inputRootPopulator, clock, time.Hour, time.Hour)

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
	environmentManager := mock.NewMockManager(ctrl)
	environment := mock.NewMockManagedEnvironment(ctrl)
	environmentManager.EXPECT().Acquire(
		digest.MustNewDigest("fedora", "5555555555555555555555555555555555555555555555555555555555555555", 7),
	).Return(environment, nil)
	inputRootPopulator := mock.NewMockInputRootPopulator(ctrl)
	buildDirectory := mock.NewMockDirectory(ctrl)
	environment.EXPECT().GetBuildDirectory().Return(buildDirectory)
	filePool := mock.NewMockFilePool(ctrl)
	inputRootPopulator.EXPECT().PopulateInputRoot(
		ctx,
		filePool,
		digest.MustNewDigest("fedora", "7777777777777777777777777777777777777777777777777777777777777777", 42),
		buildDirectory).Return(nil)
	buildDirectory.EXPECT().Mkdir("foo", os.FileMode(0777)).Return(status.Error(codes.Internal, "Out of disk space"))
	environment.EXPECT().Release()
	clock := mock.NewMockClock(ctrl)
	localBuildExecutor := builder.NewLocalBuildExecutor(contentAddressableStorage, environmentManager, inputRootPopulator, clock, time.Hour, time.Hour)

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
	buildDirectory := mock.NewMockDirectory(ctrl)
	contentAddressableStorage.EXPECT().PutFile(ctx, buildDirectory, ".stdout.txt", gomock.Any()).Return(
		digest.MustNewDigest("nintendo64", "0000000000000000000000000000000000000000000000000000000000000005", 567),
		nil)
	contentAddressableStorage.EXPECT().PutFile(ctx, buildDirectory, ".stderr.txt", gomock.Any()).Return(
		digest.MustNewDigest("nintendo64", "0000000000000000000000000000000000000000000000000000000000000006", 678),
		nil)
	environmentManager := mock.NewMockManager(ctrl)
	environment := mock.NewMockManagedEnvironment(ctrl)
	environmentManager.EXPECT().Acquire(
		digest.MustNewDigest("nintendo64", "5555555555555555555555555555555555555555555555555555555555555555", 7),
	).Return(environment, nil)
	inputRootPopulator := mock.NewMockInputRootPopulator(ctrl)
	environment.EXPECT().GetBuildDirectory().Return(buildDirectory)
	filePool := mock.NewMockFilePool(ctrl)
	inputRootPopulator.EXPECT().PopulateInputRoot(
		ctx,
		filePool,
		digest.MustNewDigest("nintendo64", "7777777777777777777777777777777777777777777777777777777777777777", 42),
		buildDirectory).Return(nil)
	buildDirectory.EXPECT().Mkdir("foo", os.FileMode(0777)).Return(nil)
	environment.EXPECT().Run(gomock.Any(), &runner.RunRequest{
		Arguments:            []string{"touch", "foo"},
		EnvironmentVariables: map[string]string{"PATH": "/bin:/usr/bin"},
		WorkingDirectory:     "",
		StdoutPath:           ".stdout.txt",
		StderrPath:           ".stderr.txt",
	}).Return(&runner.RunResponse{
		ExitCode: 0,
	}, nil)
	environment.EXPECT().Release()
	buildDirectory.EXPECT().Lstat("foo").Return(filesystem.NewFileInfo("foo", filesystem.FileTypeDirectory), nil)
	fooDirectory := mock.NewMockDirectory(ctrl)
	buildDirectory.EXPECT().Enter("foo").Return(fooDirectory, nil)
	fooDirectory.EXPECT().ReadDir().Return([]filesystem.FileInfo{
		filesystem.NewFileInfo("bar", filesystem.FileTypeSymlink),
	}, nil)
	fooDirectory.EXPECT().Readlink("bar").Return("", status.Error(codes.Internal, "Cosmic rays caused interference"))
	fooDirectory.EXPECT().Close()
	clock := mock.NewMockClock(ctrl)
	clock.EXPECT().NewContextWithTimeout(gomock.Any(), time.Hour).DoAndReturn(func(parent context.Context, timeout time.Duration) (context.Context, context.CancelFunc) {
		return context.WithCancel(parent)
	})
	localBuildExecutor := builder.NewLocalBuildExecutor(contentAddressableStorage, environmentManager, inputRootPopulator, clock, time.Hour, time.Hour)

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

	// File system operations that should occur against the build directory.
	// Creation of bazel-out/k8-fastbuild/bin/_objs/hello.
	buildDirectory := mock.NewMockDirectory(ctrl)
	buildDirectory.EXPECT().Mkdir("bazel-out", os.FileMode(0777)).Return(nil)
	bazelOutDirectory := mock.NewMockDirectory(ctrl)
	buildDirectory.EXPECT().Enter("bazel-out").Return(bazelOutDirectory, nil)
	bazelOutDirectory.EXPECT().Close()
	bazelOutDirectory.EXPECT().Mkdir("k8-fastbuild", os.FileMode(0777)).Return(nil)
	k8FastbuildDirectory := mock.NewMockDirectory(ctrl)
	bazelOutDirectory.EXPECT().Enter("k8-fastbuild").Return(k8FastbuildDirectory, nil)
	k8FastbuildDirectory.EXPECT().Close()
	k8FastbuildDirectory.EXPECT().Mkdir("bin", os.FileMode(0777)).Return(nil)
	binDirectory := mock.NewMockDirectory(ctrl)
	k8FastbuildDirectory.EXPECT().Enter("bin").Return(binDirectory, nil)
	binDirectory.EXPECT().Close()
	binDirectory.EXPECT().Mkdir("_objs", os.FileMode(0777)).Return(nil)
	objsDirectory := mock.NewMockDirectory(ctrl)
	binDirectory.EXPECT().Enter("_objs").Return(objsDirectory, nil)
	objsDirectory.EXPECT().Close()
	objsDirectory.EXPECT().Mkdir("hello", os.FileMode(0777)).Return(nil)
	helloDirectory := mock.NewMockDirectory(ctrl)
	objsDirectory.EXPECT().Enter("hello").Return(helloDirectory, nil)
	helloDirectory.EXPECT().Close()
	helloDirectory.EXPECT().Lstat("hello.pic.d").Return(filesystem.NewFileInfo("hello.pic.d", filesystem.FileTypeRegularFile), nil)
	helloDirectory.EXPECT().Lstat("hello.pic.o").Return(filesystem.NewFileInfo("hello.pic.o", filesystem.FileTypeExecutableFile), nil)

	// Read operations against the Content Addressable Storage.
	contentAddressableStorage := mock.NewMockContentAddressableStorage(ctrl)

	// Write operations against the Content Addressable Storage.
	contentAddressableStorage.EXPECT().PutFile(ctx, buildDirectory, ".stdout.txt", gomock.Any()).Return(
		digest.MustNewDigest("ubuntu1804", "0000000000000000000000000000000000000000000000000000000000000005", 567),
		nil)
	contentAddressableStorage.EXPECT().PutFile(ctx, buildDirectory, ".stderr.txt", gomock.Any()).Return(
		digest.MustNewDigest("ubuntu1804", "0000000000000000000000000000000000000000000000000000000000000006", 678),
		nil)
	contentAddressableStorage.EXPECT().PutFile(ctx, helloDirectory, "hello.pic.d", gomock.Any()).Return(
		digest.MustNewDigest("ubuntu1804", "0000000000000000000000000000000000000000000000000000000000000007", 789),
		nil)
	contentAddressableStorage.EXPECT().PutFile(ctx, helloDirectory, "hello.pic.o", gomock.Any()).Return(
		digest.MustNewDigest("ubuntu1804", "0000000000000000000000000000000000000000000000000000000000000008", 890),
		nil)

	// Command execution.
	environmentManager := mock.NewMockManager(ctrl)
	environment := mock.NewMockManagedEnvironment(ctrl)
	environmentManager.EXPECT().Acquire(
		digest.MustNewDigest("ubuntu1804", "0000000000000000000000000000000000000000000000000000000000000001", 123),
	).Return(environment, nil)
	environment.EXPECT().GetBuildDirectory().Return(buildDirectory)
	inputRootPopulator := mock.NewMockInputRootPopulator(ctrl)
	filePool := mock.NewMockFilePool(ctrl)
	inputRootPopulator.EXPECT().PopulateInputRoot(
		ctx,
		filePool,
		digest.MustNewDigest("ubuntu1804", "0000000000000000000000000000000000000000000000000000000000000003", 345),
		buildDirectory).Return(nil)
	resourceUsage, err := ptypes.MarshalAny(&empty.Empty{})
	require.NoError(t, err)
	environment.EXPECT().Run(gomock.Any(), &runner.RunRequest{
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
		WorkingDirectory: "",
		StdoutPath:       ".stdout.txt",
		StderrPath:       ".stderr.txt",
	}).Return(&runner.RunResponse{
		ExitCode:      0,
		ResourceUsage: []*any.Any{resourceUsage},
	}, nil)
	environment.EXPECT().Release()
	clock := mock.NewMockClock(ctrl)
	clock.EXPECT().NewContextWithTimeout(gomock.Any(), time.Hour).DoAndReturn(func(parent context.Context, timeout time.Duration) (context.Context, context.CancelFunc) {
		return context.WithCancel(parent)
	})
	localBuildExecutor := builder.NewLocalBuildExecutor(contentAddressableStorage, environmentManager, inputRootPopulator, clock, time.Hour, time.Hour)

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

func TestLocalBuildExecutorCachingInvalidTimeout(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)
	defer ctrl.Finish()

	contentAddressableStorage := mock.NewMockContentAddressableStorage(ctrl)
	environmentManager := mock.NewMockManager(ctrl)
	inputRootPopulator := mock.NewMockInputRootPopulator(ctrl)
	clock := mock.NewMockClock(ctrl)
	localBuildExecutor := builder.NewLocalBuildExecutor(contentAddressableStorage, environmentManager, inputRootPopulator, clock, time.Hour, time.Hour)

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
	environmentManager := mock.NewMockManager(ctrl)
	inputRootPopulator := mock.NewMockInputRootPopulator(ctrl)
	clock := mock.NewMockClock(ctrl)
	localBuildExecutor := builder.NewLocalBuildExecutor(contentAddressableStorage, environmentManager, inputRootPopulator, clock, time.Hour, time.Hour)

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
	buildDirectory := mock.NewMockDirectory(ctrl)
	contentAddressableStorage := mock.NewMockContentAddressableStorage(ctrl)
	contentAddressableStorage.EXPECT().PutFile(ctx, buildDirectory, ".stdout.txt", gomock.Any()).Return(
		digest.MustNewDigest("ubuntu1804", "0000000000000000000000000000000000000000000000000000000000000005", 567),
		nil)
	contentAddressableStorage.EXPECT().PutFile(ctx, buildDirectory, ".stderr.txt", gomock.Any()).Return(
		digest.MustNewDigest("ubuntu1804", "0000000000000000000000000000000000000000000000000000000000000006", 678),
		nil)

	// Build environment.
	environmentManager := mock.NewMockManager(ctrl)
	environment := mock.NewMockManagedEnvironment(ctrl)
	environmentManager.EXPECT().Acquire(
		digest.MustNewDigest("ubuntu1804", "0000000000000000000000000000000000000000000000000000000000000001", 123),
	).Return(environment, nil)
	environment.EXPECT().GetBuildDirectory().Return(buildDirectory)

	// Input root creation.
	inputRootPopulator := mock.NewMockInputRootPopulator(ctrl)
	filePool := mock.NewMockFilePool(ctrl)
	inputRootPopulator.EXPECT().PopulateInputRoot(
		ctx,
		filePool,
		digest.MustNewDigest("ubuntu1804", "0000000000000000000000000000000000000000000000000000000000000003", 345),
		buildDirectory).Return(nil)

	// Simulate a timeout by running the command with a timeout of
	// zero seconds. This should cause an immediate build failure.
	environment.EXPECT().Run(gomock.Any(), &runner.RunRequest{
		Arguments:            []string{"clang"},
		EnvironmentVariables: map[string]string{},
		WorkingDirectory:     "",
		StdoutPath:           ".stdout.txt",
		StderrPath:           ".stderr.txt",
	}).DoAndReturn(func(ctx context.Context, request *runner.RunRequest) (*runner.RunResponse, error) {
		<-ctx.Done()
		return nil, util.StatusFromContext(ctx)
	})
	environment.EXPECT().Release()
	clock := mock.NewMockClock(ctrl)
	clock.EXPECT().NewContextWithTimeout(gomock.Any(), time.Hour).DoAndReturn(func(parent context.Context, timeout time.Duration) (context.Context, context.CancelFunc) {
		return context.WithTimeout(parent, 0)
	})
	localBuildExecutor := builder.NewLocalBuildExecutor(contentAddressableStorage, environmentManager, inputRootPopulator, clock, time.Hour, time.Hour)

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

// TODO: Test aspects of execution not covered above (e.g., output
// directories, symlinks).
