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
	runner_pb "github.com/buildbarn/bb-remote-execution/pkg/proto/runner"
	"github.com/buildbarn/bb-storage/pkg/blobstore/buffer"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/filesystem"
	"github.com/buildbarn/bb-storage/pkg/filesystem/path"
	"github.com/buildbarn/bb-storage/pkg/testutil"
	"github.com/buildbarn/bb-storage/pkg/util"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/emptypb"
)

func TestLocalBuildExecutorInvalidActionDigest(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	contentAddressableStorage := mock.NewMockBlobAccess(ctrl)
	buildDirectoryCreator := mock.NewMockBuildDirectoryCreator(ctrl)
	runner := mock.NewMockRunner(ctrl)
	clock := mock.NewMockClock(ctrl)
	localBuildExecutor := builder.NewLocalBuildExecutor(contentAddressableStorage, buildDirectoryCreator, runner, clock, time.Hour, time.Hour, nil, 10000, map[string]string{})

	filePool := mock.NewMockFilePool(ctrl)
	metadata := make(chan *remoteworker.CurrentState_Executing, 10)
	executeResponse := localBuildExecutor.Execute(
		ctx,
		filePool,
		digest.MustNewInstanceName("netbsd"),
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
		},
		metadata)
	testutil.RequireEqualProto(t, &remoteexecution.ExecuteResponse{
		Result: &remoteexecution.ActionResult{
			ExecutionMetadata: &remoteexecution.ExecutedActionMetadata{},
		},
		Status: status.New(codes.InvalidArgument, "Failed to extract digest for action: Unknown digest hash length: 34 characters").Proto(),
	}, executeResponse)
}

func TestLocalBuildExecutorMissingAction(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	contentAddressableStorage := mock.NewMockBlobAccess(ctrl)
	buildDirectoryCreator := mock.NewMockBuildDirectoryCreator(ctrl)
	runner := mock.NewMockRunner(ctrl)
	clock := mock.NewMockClock(ctrl)
	localBuildExecutor := builder.NewLocalBuildExecutor(contentAddressableStorage, buildDirectoryCreator, runner, clock, time.Hour, time.Hour, nil, 10000, map[string]string{})

	filePool := mock.NewMockFilePool(ctrl)
	metadata := make(chan *remoteworker.CurrentState_Executing, 10)
	executeResponse := localBuildExecutor.Execute(
		ctx,
		filePool,
		digest.MustNewInstanceName("netbsd"),
		&remoteworker.DesiredState_Executing{
			ActionDigest: &remoteexecution.Digest{
				Hash:      "5555555555555555555555555555555555555555555555555555555555555555",
				SizeBytes: 7,
			},
		},
		metadata)
	testutil.RequireEqualProto(t, &remoteexecution.ExecuteResponse{
		Result: &remoteexecution.ActionResult{
			ExecutionMetadata: &remoteexecution.ExecutedActionMetadata{},
		},
		Status: status.New(codes.InvalidArgument, "Request does not contain an action").Proto(),
	}, executeResponse)
}

func TestLocalBuildExecutorBuildDirectoryCreatorFailedFailed(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	contentAddressableStorage := mock.NewMockBlobAccess(ctrl)
	buildDirectoryCreator := mock.NewMockBuildDirectoryCreator(ctrl)
	buildDirectoryCreator.EXPECT().GetBuildDirectory(
		digest.MustNewDigest("netbsd", "5555555555555555555555555555555555555555555555555555555555555555", 7),
		false,
	).Return(nil, nil, status.Error(codes.InvalidArgument, "Platform requirements not provided"))
	runner := mock.NewMockRunner(ctrl)
	clock := mock.NewMockClock(ctrl)
	localBuildExecutor := builder.NewLocalBuildExecutor(contentAddressableStorage, buildDirectoryCreator, runner, clock, time.Hour, time.Hour, nil, 10000, map[string]string{})

	filePool := mock.NewMockFilePool(ctrl)
	metadata := make(chan *remoteworker.CurrentState_Executing, 10)
	executeResponse := localBuildExecutor.Execute(
		ctx,
		filePool,
		digest.MustNewInstanceName("netbsd"),
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
	testutil.RequireEqualProto(t, &remoteexecution.ExecuteResponse{
		Result: &remoteexecution.ActionResult{
			ExecutionMetadata: &remoteexecution.ExecutedActionMetadata{},
		},
		Status: status.New(codes.InvalidArgument, "Failed to acquire build environment: Platform requirements not provided").Proto(),
	}, executeResponse)
}

func TestLocalBuildExecutorInputRootPopulationFailed(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	contentAddressableStorage := mock.NewMockBlobAccess(ctrl)
	buildDirectoryCreator := mock.NewMockBuildDirectoryCreator(ctrl)
	buildDirectory := mock.NewMockBuildDirectory(ctrl)
	buildDirectoryCreator.EXPECT().GetBuildDirectory(
		digest.MustNewDigest("netbsd", "5555555555555555555555555555555555555555555555555555555555555555", 7),
		false,
	).Return(buildDirectory, nil, nil)
	filePool := mock.NewMockFilePool(ctrl)
	buildDirectory.EXPECT().InstallHooks(filePool, gomock.Any())
	buildDirectory.EXPECT().Mkdir(path.MustNewComponent("root"), os.FileMode(0o777))
	inputRootDirectory := mock.NewMockBuildDirectory(ctrl)
	buildDirectory.EXPECT().EnterBuildDirectory(path.MustNewComponent("root")).Return(inputRootDirectory, nil)
	inputRootDirectory.EXPECT().MergeDirectoryContents(
		ctx,
		gomock.Any(),
		digest.MustNewDigest("netbsd", "7777777777777777777777777777777777777777777777777777777777777777", 42),
	).Return(status.Error(codes.FailedPrecondition, "Some input files could not be found"))
	inputRootDirectory.EXPECT().Close()
	buildDirectory.EXPECT().Close()
	runner := mock.NewMockRunner(ctrl)
	clock := mock.NewMockClock(ctrl)
	localBuildExecutor := builder.NewLocalBuildExecutor(contentAddressableStorage, buildDirectoryCreator, runner, clock, time.Hour, time.Hour, nil, 10000, map[string]string{})

	metadata := make(chan *remoteworker.CurrentState_Executing, 10)
	executeResponse := localBuildExecutor.Execute(
		ctx,
		filePool,
		digest.MustNewInstanceName("netbsd"),
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
	testutil.RequireEqualProto(t, &remoteexecution.ExecuteResponse{
		Result: &remoteexecution.ActionResult{
			ExecutionMetadata: &remoteexecution.ExecutedActionMetadata{},
		},
		Status: status.New(codes.FailedPrecondition, "Some input files could not be found").Proto(),
	}, executeResponse)
}

func TestLocalBuildExecutorOutputDirectoryCreationFailure(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	contentAddressableStorage := mock.NewMockBlobAccess(ctrl)
	contentAddressableStorage.EXPECT().Get(
		gomock.Any(),
		digest.MustNewDigest("fedora", "6666666666666666666666666666666666666666666666666666666666666666", 234),
	).Return(buffer.NewProtoBufferFromProto(&remoteexecution.Command{
		Arguments: []string{"touch", "foo"},
		EnvironmentVariables: []*remoteexecution.Command_EnvironmentVariable{
			{Name: "PATH", Value: "/bin:/usr/bin"},
		},
		OutputFiles: []string{"foo/bar/baz"},
	}, buffer.UserProvided))
	buildDirectoryCreator := mock.NewMockBuildDirectoryCreator(ctrl)
	buildDirectory := mock.NewMockBuildDirectory(ctrl)
	buildDirectoryCreator.EXPECT().GetBuildDirectory(
		digest.MustNewDigest("fedora", "5555555555555555555555555555555555555555555555555555555555555555", 7),
		false,
	).Return(buildDirectory, nil, nil)
	filePool := mock.NewMockFilePool(ctrl)
	buildDirectory.EXPECT().InstallHooks(filePool, gomock.Any())
	buildDirectory.EXPECT().Mkdir(path.MustNewComponent("root"), os.FileMode(0o777))
	inputRootDirectory := mock.NewMockBuildDirectory(ctrl)
	buildDirectory.EXPECT().EnterBuildDirectory(path.MustNewComponent("root")).Return(inputRootDirectory, nil)
	inputRootDirectory.EXPECT().MergeDirectoryContents(
		ctx,
		gomock.Any(),
		digest.MustNewDigest("fedora", "7777777777777777777777777777777777777777777777777777777777777777", 42),
	).Return(nil)
	inputRootDirectory.EXPECT().Mkdir(path.MustNewComponent("foo"), os.FileMode(0o777)).Return(status.Error(codes.Internal, "Out of disk space"))
	inputRootDirectory.EXPECT().Close()
	buildDirectory.EXPECT().Close()
	runner := mock.NewMockRunner(ctrl)
	clock := mock.NewMockClock(ctrl)
	localBuildExecutor := builder.NewLocalBuildExecutor(contentAddressableStorage, buildDirectoryCreator, runner, clock, time.Hour, time.Hour, nil, 10000, map[string]string{})

	metadata := make(chan *remoteworker.CurrentState_Executing, 10)
	executeResponse := localBuildExecutor.Execute(
		ctx,
		filePool,
		digest.MustNewInstanceName("fedora"),
		&remoteworker.DesiredState_Executing{
			ActionDigest: &remoteexecution.Digest{
				Hash:      "5555555555555555555555555555555555555555555555555555555555555555",
				SizeBytes: 7,
			},
			Action: &remoteexecution.Action{
				CommandDigest: &remoteexecution.Digest{
					Hash:      "6666666666666666666666666666666666666666666666666666666666666666",
					SizeBytes: 234,
				},
				InputRootDigest: &remoteexecution.Digest{
					Hash:      "7777777777777777777777777777777777777777777777777777777777777777",
					SizeBytes: 42,
				},
			},
		},
		metadata)
	testutil.RequireEqualProto(t, &remoteexecution.ExecuteResponse{
		Result: &remoteexecution.ActionResult{
			ExecutionMetadata: &remoteexecution.ExecutedActionMetadata{},
		},
		Status: status.New(codes.Internal, "Failed to create output parent directory \"foo\": Out of disk space").Proto(),
	}, executeResponse)
}

func TestLocalBuildExecutorMissingCommand(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	contentAddressableStorage := mock.NewMockBlobAccess(ctrl)
	buildDirectoryCreator := mock.NewMockBuildDirectoryCreator(ctrl)
	buildDirectory := mock.NewMockBuildDirectory(ctrl)
	buildDirectoryCreator.EXPECT().GetBuildDirectory(
		digest.MustNewDigest("netbsd", "5555555555555555555555555555555555555555555555555555555555555555", 7),
		false,
	).Return(buildDirectory, nil, nil)
	filePool := mock.NewMockFilePool(ctrl)
	buildDirectory.EXPECT().InstallHooks(filePool, gomock.Any())
	buildDirectory.EXPECT().Mkdir(path.MustNewComponent("root"), os.FileMode(0o777))
	inputRootDirectory := mock.NewMockBuildDirectory(ctrl)
	buildDirectory.EXPECT().EnterBuildDirectory(path.MustNewComponent("root")).Return(inputRootDirectory, nil)
	inputRootDirectory.EXPECT().MergeDirectoryContents(
		ctx,
		gomock.Any(),
		digest.MustNewDigest("netbsd", "7777777777777777777777777777777777777777777777777777777777777777", 42),
	).Return(nil)
	inputRootDirectory.EXPECT().Close()
	buildDirectory.EXPECT().Close()
	runner := mock.NewMockRunner(ctrl)
	clock := mock.NewMockClock(ctrl)
	localBuildExecutor := builder.NewLocalBuildExecutor(contentAddressableStorage, buildDirectoryCreator, runner, clock, time.Hour, time.Hour, nil, 10000, map[string]string{})

	metadata := make(chan *remoteworker.CurrentState_Executing, 10)
	executeResponse := localBuildExecutor.Execute(
		ctx,
		filePool,
		digest.MustNewInstanceName("netbsd"),
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
	testutil.RequireEqualProto(t, &remoteexecution.ExecuteResponse{
		Result: &remoteexecution.ActionResult{
			ExecutionMetadata: &remoteexecution.ExecutedActionMetadata{},
		},
		Status: status.New(codes.InvalidArgument, "Failed to extract digest for command: No digest provided").Proto(),
	}, executeResponse)
}

func TestLocalBuildExecutorOutputSymlinkReadingFailure(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	contentAddressableStorage := mock.NewMockBlobAccess(ctrl)
	contentAddressableStorage.EXPECT().Get(
		gomock.Any(),
		digest.MustNewDigest("nintendo64", "6666666666666666666666666666666666666666666666666666666666666666", 234),
	).Return(buffer.NewProtoBufferFromProto(&remoteexecution.Command{
		Arguments: []string{"touch", "foo"},
		EnvironmentVariables: []*remoteexecution.Command_EnvironmentVariable{
			{Name: "PATH", Value: "/bin:/usr/bin"},
		},
		OutputDirectories: []string{"foo"},
	}, buffer.UserProvided))
	buildDirectory := mock.NewMockBuildDirectory(ctrl)
	buildDirectory.EXPECT().UploadFile(ctx, path.MustNewComponent("stdout"), gomock.Any()).Return(
		digest.MustNewDigest("nintendo64", "0000000000000000000000000000000000000000000000000000000000000005", 567),
		nil)
	buildDirectory.EXPECT().UploadFile(ctx, path.MustNewComponent("stderr"), gomock.Any()).Return(
		digest.MustNewDigest("nintendo64", "0000000000000000000000000000000000000000000000000000000000000006", 678),
		nil)
	contentAddressableStorage.EXPECT().Put(
		ctx,
		digest.MustNewDigest("nintendo64", "102b51b9765a56a3e899f7cf0ee38e5251f9c503b357b330a49183eb7b155604", 2),
		gomock.Any()).
		DoAndReturn(func(ctx context.Context, digest digest.Digest, b buffer.Buffer) error {
			m, err := b.ToProto(&remoteexecution.Tree{}, 10000)
			require.NoError(t, err)
			testutil.RequireEqualProto(t, &remoteexecution.Tree{
				Root: &remoteexecution.Directory{},
			}, m)
			return nil
		})

	buildDirectoryCreator := mock.NewMockBuildDirectoryCreator(ctrl)
	buildDirectoryCreator.EXPECT().GetBuildDirectory(
		digest.MustNewDigest("nintendo64", "5555555555555555555555555555555555555555555555555555555555555555", 7),
		false,
	).Return(buildDirectory, nil, nil)
	filePool := mock.NewMockFilePool(ctrl)
	buildDirectory.EXPECT().InstallHooks(filePool, gomock.Any())
	buildDirectory.EXPECT().Mkdir(path.MustNewComponent("root"), os.FileMode(0o777))
	inputRootDirectory := mock.NewMockBuildDirectory(ctrl)
	buildDirectory.EXPECT().EnterBuildDirectory(path.MustNewComponent("root")).Return(inputRootDirectory, nil)
	inputRootDirectory.EXPECT().MergeDirectoryContents(
		ctx,
		gomock.Any(),
		digest.MustNewDigest("nintendo64", "7777777777777777777777777777777777777777777777777777777777777777", 42),
	).Return(nil)
	inputRootDirectory.EXPECT().Mkdir(path.MustNewComponent("foo"), os.FileMode(0o777)).Return(nil)
	buildDirectory.EXPECT().Mkdir(path.MustNewComponent("tmp"), os.FileMode(0o777))
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
	fooDirectory := mock.NewMockUploadableDirectory(ctrl)
	inputRootDirectory.EXPECT().Lstat(path.MustNewComponent("foo")).Return(filesystem.NewFileInfo(path.MustNewComponent("foo"), filesystem.FileTypeDirectory), nil)
	inputRootDirectory.EXPECT().EnterUploadableDirectory(path.MustNewComponent("foo")).Return(fooDirectory, nil)
	fooDirectory.EXPECT().ReadDir().Return([]filesystem.FileInfo{
		filesystem.NewFileInfo(path.MustNewComponent("bar"), filesystem.FileTypeSymlink),
	}, nil)
	fooDirectory.EXPECT().Readlink(path.MustNewComponent("bar")).Return("", status.Error(codes.Internal, "Cosmic rays caused interference"))
	fooDirectory.EXPECT().Close()
	inputRootDirectory.EXPECT().Close()
	buildDirectory.EXPECT().Close()
	clock := mock.NewMockClock(ctrl)
	clock.EXPECT().NewContextWithTimeout(gomock.Any(), time.Hour).DoAndReturn(func(parent context.Context, timeout time.Duration) (context.Context, context.CancelFunc) {
		return context.WithCancel(parent)
	})
	localBuildExecutor := builder.NewLocalBuildExecutor(contentAddressableStorage, buildDirectoryCreator, runner, clock, time.Hour, time.Hour, nil, 10000, map[string]string{})

	metadata := make(chan *remoteworker.CurrentState_Executing, 10)
	executeResponse := localBuildExecutor.Execute(
		ctx,
		filePool,
		digest.MustNewInstanceName("nintendo64"),
		&remoteworker.DesiredState_Executing{
			ActionDigest: &remoteexecution.Digest{
				Hash:      "5555555555555555555555555555555555555555555555555555555555555555",
				SizeBytes: 7,
			},
			Action: &remoteexecution.Action{
				CommandDigest: &remoteexecution.Digest{
					Hash:      "6666666666666666666666666666666666666666666666666666666666666666",
					SizeBytes: 234,
				},
				InputRootDigest: &remoteexecution.Digest{
					Hash:      "7777777777777777777777777777777777777777777777777777777777777777",
					SizeBytes: 42,
				},
			},
		},
		metadata)
	testutil.RequireEqualProto(t, &remoteexecution.ExecuteResponse{
		Result: &remoteexecution.ActionResult{
			OutputDirectories: []*remoteexecution.OutputDirectory{
				{
					Path: "foo",
					TreeDigest: &remoteexecution.Digest{
						Hash:      "102b51b9765a56a3e899f7cf0ee38e5251f9c503b357b330a49183eb7b155604",
						SizeBytes: 2,
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
			ExecutionMetadata: &remoteexecution.ExecutedActionMetadata{},
		},
		Status: status.New(codes.Internal, "Failed to read output symlink \"foo/bar\": Cosmic rays caused interference").Proto(),
	}, executeResponse)
}

// TestLocalBuildExecutorSuccess tests a full invocation of a simple
// build step, equivalent to compiling a simple C++ file.
func TestLocalBuildExecutorSuccess(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	// File system operations that should occur against the input
	// root directory. Creation of
	// bazel-out/k8-fastbuild/bin/_objs/hello.
	inputRootDirectory := mock.NewMockBuildDirectory(ctrl)
	inputRootDirectory.EXPECT().Mkdir(path.MustNewComponent("bazel-out"), os.FileMode(0o777)).Return(nil)
	bazelOutDirectory := mock.NewMockBuildDirectory(ctrl)
	inputRootDirectory.EXPECT().EnterBuildDirectory(path.MustNewComponent("bazel-out")).Return(bazelOutDirectory, nil)
	bazelOutDirectory.EXPECT().Close()
	bazelOutDirectory.EXPECT().Mkdir(path.MustNewComponent("k8-fastbuild"), os.FileMode(0o777)).Return(nil)
	k8FastbuildDirectory := mock.NewMockBuildDirectory(ctrl)
	bazelOutDirectory.EXPECT().EnterBuildDirectory(path.MustNewComponent("k8-fastbuild")).Return(k8FastbuildDirectory, nil)
	k8FastbuildDirectory.EXPECT().Close()
	k8FastbuildDirectory.EXPECT().Mkdir(path.MustNewComponent("bin"), os.FileMode(0o777)).Return(nil)
	binDirectory := mock.NewMockBuildDirectory(ctrl)
	k8FastbuildDirectory.EXPECT().EnterBuildDirectory(path.MustNewComponent("bin")).Return(binDirectory, nil)
	binDirectory.EXPECT().Close()
	binDirectory.EXPECT().Mkdir(path.MustNewComponent("_objs"), os.FileMode(0o777)).Return(nil)
	objsDirectory := mock.NewMockBuildDirectory(ctrl)
	binDirectory.EXPECT().EnterBuildDirectory(path.MustNewComponent("_objs")).Return(objsDirectory, nil)
	objsDirectory.EXPECT().Close()
	objsDirectory.EXPECT().Mkdir(path.MustNewComponent("hello"), os.FileMode(0o777)).Return(nil)

	// Uploading of files in bazel-out/k8-fastbuild/bin/_objs/hello.
	bazelOutUploadableDirectory := mock.NewMockUploadableDirectory(ctrl)
	inputRootDirectory.EXPECT().EnterUploadableDirectory(path.MustNewComponent("bazel-out")).Return(bazelOutUploadableDirectory, nil)
	bazelOutUploadableDirectory.EXPECT().Close()
	k8sFastbuildUploadableDirectory := mock.NewMockBuildDirectory(ctrl)
	bazelOutUploadableDirectory.EXPECT().EnterUploadableDirectory(path.MustNewComponent("k8-fastbuild")).Return(k8sFastbuildUploadableDirectory, nil)
	k8sFastbuildUploadableDirectory.EXPECT().Close()
	binUploadableDirectory := mock.NewMockUploadableDirectory(ctrl)
	k8sFastbuildUploadableDirectory.EXPECT().EnterUploadableDirectory(path.MustNewComponent("bin")).Return(binUploadableDirectory, nil)
	binUploadableDirectory.EXPECT().Close()
	objsUploadableDirectory := mock.NewMockUploadableDirectory(ctrl)
	binUploadableDirectory.EXPECT().EnterUploadableDirectory(path.MustNewComponent("_objs")).Return(objsUploadableDirectory, nil)
	objsUploadableDirectory.EXPECT().Close()
	helloUploadableDirectory := mock.NewMockUploadableDirectory(ctrl)
	objsUploadableDirectory.EXPECT().EnterUploadableDirectory(path.MustNewComponent("hello")).Return(helloUploadableDirectory, nil)
	helloUploadableDirectory.EXPECT().Lstat(path.MustNewComponent("hello.pic.d")).Return(filesystem.NewFileInfo(path.MustNewComponent("hello.pic.d"), filesystem.FileTypeRegularFile), nil)
	helloUploadableDirectory.EXPECT().Lstat(path.MustNewComponent("hello.pic.o")).Return(filesystem.NewFileInfo(path.MustNewComponent("hello.pic.o"), filesystem.FileTypeExecutableFile), nil)
	helloUploadableDirectory.EXPECT().Close()

	// Read operations against the Content Addressable Storage.
	contentAddressableStorage := mock.NewMockBlobAccess(ctrl)
	contentAddressableStorage.EXPECT().Get(
		gomock.Any(),
		digest.MustNewDigest("ubuntu1804", "0000000000000000000000000000000000000000000000000000000000000002", 234),
	).Return(buffer.NewProtoBufferFromProto(&remoteexecution.Command{
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
	}, buffer.UserProvided))

	// Write operations against the Content Addressable Storage.
	buildDirectory := mock.NewMockBuildDirectory(ctrl)
	buildDirectory.EXPECT().UploadFile(ctx, path.MustNewComponent("stdout"), gomock.Any()).Return(
		digest.MustNewDigest("ubuntu1804", "0000000000000000000000000000000000000000000000000000000000000005", 567),
		nil)
	buildDirectory.EXPECT().UploadFile(ctx, path.MustNewComponent("stderr"), gomock.Any()).Return(
		digest.MustNewDigest("ubuntu1804", "0000000000000000000000000000000000000000000000000000000000000006", 678),
		nil)
	helloUploadableDirectory.EXPECT().UploadFile(ctx, path.MustNewComponent("hello.pic.d"), gomock.Any()).Return(
		digest.MustNewDigest("ubuntu1804", "0000000000000000000000000000000000000000000000000000000000000007", 789),
		nil)
	helloUploadableDirectory.EXPECT().UploadFile(ctx, path.MustNewComponent("hello.pic.o"), gomock.Any()).Return(
		digest.MustNewDigest("ubuntu1804", "0000000000000000000000000000000000000000000000000000000000000008", 890),
		nil)

	// Command execution.
	buildDirectoryCreator := mock.NewMockBuildDirectoryCreator(ctrl)
	buildDirectoryCreator.EXPECT().GetBuildDirectory(
		digest.MustNewDigest("ubuntu1804", "0000000000000000000000000000000000000000000000000000000000000001", 123),
		false,
	).Return(buildDirectory, ((*path.Trace)(nil)).Append(path.MustNewComponent("0000000000000000")), nil)
	filePool := mock.NewMockFilePool(ctrl)
	buildDirectory.EXPECT().InstallHooks(filePool, gomock.Any())
	buildDirectory.EXPECT().Mkdir(path.MustNewComponent("root"), os.FileMode(0o777))
	buildDirectory.EXPECT().EnterBuildDirectory(path.MustNewComponent("root")).Return(inputRootDirectory, nil)
	inputRootDirectory.EXPECT().MergeDirectoryContents(
		ctx,
		gomock.Any(),
		digest.MustNewDigest("ubuntu1804", "0000000000000000000000000000000000000000000000000000000000000003", 345),
	).Return(nil)
	inputRootDirectory.EXPECT().Mkdir(path.MustNewComponent("dev"), os.FileMode(0o777))
	inputRootDevDirectory := mock.NewMockBuildDirectory(ctrl)
	inputRootDirectory.EXPECT().EnterBuildDirectory(path.MustNewComponent("dev")).Return(inputRootDevDirectory, nil)
	inputRootDevDirectory.EXPECT().Mknod(path.MustNewComponent("null"), os.FileMode(os.ModeDevice|os.ModeCharDevice|0o666), 259).Return(nil)
	inputRootDevDirectory.EXPECT().Close()
	buildDirectory.EXPECT().Mkdir(path.MustNewComponent("tmp"), os.FileMode(0o777))
	resourceUsage, err := anypb.New(&emptypb.Empty{})
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
			"TEST_VAR":                          "123",
		},
		WorkingDirectory:   "",
		StdoutPath:         "0000000000000000/stdout",
		StderrPath:         "0000000000000000/stderr",
		InputRootDirectory: "0000000000000000/root",
		TemporaryDirectory: "0000000000000000/tmp",
	}).Return(&runner_pb.RunResponse{
		ExitCode:      0,
		ResourceUsage: []*anypb.Any{resourceUsage},
	}, nil)
	inputRootDirectory.EXPECT().Close()
	buildDirectory.EXPECT().Close()
	clock := mock.NewMockClock(ctrl)
	clock.EXPECT().NewContextWithTimeout(gomock.Any(), time.Hour).DoAndReturn(func(parent context.Context, timeout time.Duration) (context.Context, context.CancelFunc) {
		return context.WithCancel(parent)
	})
	inputRootCharacterDevices := map[path.Component]int{
		path.MustNewComponent("null"): 259,
	}
	environmentVars := map[string]string{
		"TEST_VAR": "123",
		"PWD":      "dont-overwrite",
	}
	localBuildExecutor := builder.NewLocalBuildExecutor(contentAddressableStorage, buildDirectoryCreator, runner, clock, time.Hour, time.Hour, inputRootCharacterDevices, 10000, environmentVars)

	requestMetadata, err := anypb.New(&remoteexecution.RequestMetadata{
		ToolInvocationId: "666b72d8-c43e-4998-866c-9312a31fe86d",
	})
	require.NoError(t, err)
	metadata := make(chan *remoteworker.CurrentState_Executing, 10)
	executeResponse := localBuildExecutor.Execute(
		ctx,
		filePool,
		digest.MustNewInstanceName("ubuntu1804"),
		&remoteworker.DesiredState_Executing{
			ActionDigest: &remoteexecution.Digest{
				Hash:      "0000000000000000000000000000000000000000000000000000000000000001",
				SizeBytes: 123,
			},
			Action: &remoteexecution.Action{
				CommandDigest: &remoteexecution.Digest{
					Hash:      "0000000000000000000000000000000000000000000000000000000000000002",
					SizeBytes: 234,
				},
				InputRootDigest: &remoteexecution.Digest{
					Hash:      "0000000000000000000000000000000000000000000000000000000000000003",
					SizeBytes: 345,
				},
			},
			AuxiliaryMetadata: []*anypb.Any{requestMetadata},
		},
		metadata)
	testutil.RequireEqualProto(t, &remoteexecution.ExecuteResponse{
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
				AuxiliaryMetadata: []*anypb.Any{requestMetadata, resourceUsage},
			},
		},
	}, executeResponse)
}

func TestLocalBuildExecutorCachingInvalidTimeout(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	contentAddressableStorage := mock.NewMockBlobAccess(ctrl)
	buildDirectoryCreator := mock.NewMockBuildDirectoryCreator(ctrl)
	runner := mock.NewMockRunner(ctrl)
	clock := mock.NewMockClock(ctrl)
	localBuildExecutor := builder.NewLocalBuildExecutor(contentAddressableStorage, buildDirectoryCreator, runner, clock, time.Hour, time.Hour, nil, 10000, map[string]string{})

	// Execution should fail, as the number of nanoseconds in the
	// timeout is not within bounds.
	filePool := mock.NewMockFilePool(ctrl)
	metadata := make(chan *remoteworker.CurrentState_Executing, 10)
	executeResponse := localBuildExecutor.Execute(
		ctx,
		filePool,
		digest.MustNewInstanceName("ubuntu1804"),
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
				Timeout: &durationpb.Duration{
					Nanos: 1000000000,
				},
			},
		},
		metadata)
	testutil.RequirePrefixedStatus(t, status.Error(codes.InvalidArgument, "Invalid execution timeout: "), status.ErrorProto(executeResponse.Status))
}

func TestLocalBuildExecutorCachingTimeoutTooHigh(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	contentAddressableStorage := mock.NewMockBlobAccess(ctrl)
	buildDirectoryCreator := mock.NewMockBuildDirectoryCreator(ctrl)
	runner := mock.NewMockRunner(ctrl)
	clock := mock.NewMockClock(ctrl)
	localBuildExecutor := builder.NewLocalBuildExecutor(contentAddressableStorage, buildDirectoryCreator, runner, clock, time.Hour, time.Hour, nil, 10000, map[string]string{})

	// The protocol states that we must deny requests that have a
	// timeout that is longer than the server's maximum.
	filePool := mock.NewMockFilePool(ctrl)
	metadata := make(chan *remoteworker.CurrentState_Executing, 10)
	executeResponse := localBuildExecutor.Execute(
		ctx,
		filePool,
		digest.MustNewInstanceName("ubuntu1804"),
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
				Timeout: &durationpb.Duration{
					Seconds: 7200,
				},
			},
		},
		metadata)
	testutil.RequireEqualProto(t, &remoteexecution.ExecuteResponse{
		Result: &remoteexecution.ActionResult{
			ExecutionMetadata: &remoteexecution.ExecutedActionMetadata{},
		},
		Status: status.New(codes.InvalidArgument, "Execution timeout of 2h0m0s exceeds maximum permitted value of 1h0m0s").Proto(),
	}, executeResponse)
}

func TestLocalBuildExecutorInputRootIOFailureDuringExecution(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	// Build directory.
	buildDirectory := mock.NewMockBuildDirectory(ctrl)
	contentAddressableStorage := mock.NewMockBlobAccess(ctrl)
	contentAddressableStorage.EXPECT().Get(
		gomock.Any(),
		digest.MustNewDigest("ubuntu1804", "0000000000000000000000000000000000000000000000000000000000000002", 234),
	).Return(buffer.NewProtoBufferFromProto(&remoteexecution.Command{
		Arguments: []string{"clang"},
	}, buffer.UserProvided))
	buildDirectory.EXPECT().UploadFile(ctx, path.MustNewComponent("stdout"), gomock.Any()).Return(
		digest.MustNewDigest("ubuntu1804", "0000000000000000000000000000000000000000000000000000000000000005", 567),
		nil)
	buildDirectory.EXPECT().UploadFile(ctx, path.MustNewComponent("stderr"), gomock.Any()).Return(
		digest.MustNewDigest("ubuntu1804", "0000000000000000000000000000000000000000000000000000000000000006", 678),
		nil)

	// Build environment.
	buildDirectoryCreator := mock.NewMockBuildDirectoryCreator(ctrl)
	buildDirectoryCreator.EXPECT().GetBuildDirectory(
		digest.MustNewDigest("ubuntu1804", "0000000000000000000000000000000000000000000000000000000000000001", 123),
		false,
	).Return(buildDirectory, nil, nil)
	filePool := mock.NewMockFilePool(ctrl)
	buildDirectory.EXPECT().InstallHooks(filePool, gomock.Any())

	// Input root creation. Preserve the error logger that is
	// provided, so that an I/O error can be triggered during the
	// build.
	buildDirectory.EXPECT().Mkdir(path.MustNewComponent("root"), os.FileMode(0o777))
	inputRootDirectory := mock.NewMockBuildDirectory(ctrl)
	buildDirectory.EXPECT().EnterBuildDirectory(path.MustNewComponent("root")).Return(inputRootDirectory, nil)
	var errorLogger util.ErrorLogger
	inputRootDirectory.EXPECT().MergeDirectoryContents(
		ctx,
		gomock.Any(),
		digest.MustNewDigest("ubuntu1804", "0000000000000000000000000000000000000000000000000000000000000003", 345),
	).DoAndReturn(func(ctx context.Context, providedErrorLogger util.ErrorLogger, digest digest.Digest) error {
		errorLogger = providedErrorLogger
		return nil
	})
	buildDirectory.EXPECT().Mkdir(path.MustNewComponent("tmp"), os.FileMode(0o777))

	// Let an I/O error in the input root trigger during the build.
	// The build should be canceled immediately. The error should be
	// propagated to the response.
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
		errorLogger.Log(status.Error(codes.FailedPrecondition, "Blob not found"))
		<-ctx.Done()
		return nil, util.StatusFromContext(ctx)
	})
	inputRootDirectory.EXPECT().Close()
	buildDirectory.EXPECT().Close()
	clock := mock.NewMockClock(ctrl)
	clock.EXPECT().NewContextWithTimeout(gomock.Any(), 15*time.Minute).DoAndReturn(func(parent context.Context, timeout time.Duration) (context.Context, context.CancelFunc) {
		return context.WithCancel(parent)
	})
	localBuildExecutor := builder.NewLocalBuildExecutor(contentAddressableStorage, buildDirectoryCreator, runner, clock, time.Hour, time.Hour, nil, 10000, map[string]string{})

	metadata := make(chan *remoteworker.CurrentState_Executing, 10)
	executeResponse := localBuildExecutor.Execute(
		ctx,
		filePool,
		digest.MustNewInstanceName("ubuntu1804"),
		&remoteworker.DesiredState_Executing{
			ActionDigest: &remoteexecution.Digest{
				Hash:      "0000000000000000000000000000000000000000000000000000000000000001",
				SizeBytes: 123,
			},
			Action: &remoteexecution.Action{
				CommandDigest: &remoteexecution.Digest{
					Hash:      "0000000000000000000000000000000000000000000000000000000000000002",
					SizeBytes: 234,
				},
				InputRootDigest: &remoteexecution.Digest{
					Hash:      "0000000000000000000000000000000000000000000000000000000000000003",
					SizeBytes: 345,
				},
				Timeout: &durationpb.Duration{
					Seconds: 900,
				},
			},
		},
		metadata)
	testutil.RequireEqualProto(t, &remoteexecution.ExecuteResponse{
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
		Status: status.New(codes.FailedPrecondition, "I/O error while running command: Blob not found").Proto(),
	}, executeResponse)
}

func TestLocalBuildExecutorTimeoutDuringExecution(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	// Build directory.
	buildDirectory := mock.NewMockBuildDirectory(ctrl)
	contentAddressableStorage := mock.NewMockBlobAccess(ctrl)
	contentAddressableStorage.EXPECT().Get(
		gomock.Any(),
		digest.MustNewDigest("ubuntu1804", "0000000000000000000000000000000000000000000000000000000000000002", 234),
	).Return(buffer.NewProtoBufferFromProto(&remoteexecution.Command{
		Arguments: []string{"clang"},
	}, buffer.UserProvided))
	buildDirectory.EXPECT().UploadFile(ctx, path.MustNewComponent("stdout"), gomock.Any()).Return(
		digest.MustNewDigest("ubuntu1804", "0000000000000000000000000000000000000000000000000000000000000005", 567),
		nil)
	buildDirectory.EXPECT().UploadFile(ctx, path.MustNewComponent("stderr"), gomock.Any()).Return(
		digest.MustNewDigest("ubuntu1804", "0000000000000000000000000000000000000000000000000000000000000006", 678),
		nil)

	// Build environment.
	buildDirectoryCreator := mock.NewMockBuildDirectoryCreator(ctrl)
	buildDirectoryCreator.EXPECT().GetBuildDirectory(
		digest.MustNewDigest("ubuntu1804", "0000000000000000000000000000000000000000000000000000000000000001", 123),
		false,
	).Return(buildDirectory, nil, nil)
	filePool := mock.NewMockFilePool(ctrl)
	buildDirectory.EXPECT().InstallHooks(filePool, gomock.Any())

	// Input root creation.
	buildDirectory.EXPECT().Mkdir(path.MustNewComponent("root"), os.FileMode(0o777))
	inputRootDirectory := mock.NewMockBuildDirectory(ctrl)
	buildDirectory.EXPECT().EnterBuildDirectory(path.MustNewComponent("root")).Return(inputRootDirectory, nil)
	inputRootDirectory.EXPECT().MergeDirectoryContents(
		ctx,
		gomock.Any(),
		digest.MustNewDigest("ubuntu1804", "0000000000000000000000000000000000000000000000000000000000000003", 345),
	).Return(nil)
	buildDirectory.EXPECT().Mkdir(path.MustNewComponent("tmp"), os.FileMode(0o777))

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
	localBuildExecutor := builder.NewLocalBuildExecutor(contentAddressableStorage, buildDirectoryCreator, runner, clock, time.Hour, time.Hour, nil, 10000, map[string]string{})

	metadata := make(chan *remoteworker.CurrentState_Executing, 10)
	executeResponse := localBuildExecutor.Execute(
		ctx,
		filePool,
		digest.MustNewInstanceName("ubuntu1804"),
		&remoteworker.DesiredState_Executing{
			ActionDigest: &remoteexecution.Digest{
				Hash:      "0000000000000000000000000000000000000000000000000000000000000001",
				SizeBytes: 123,
			},
			Action: &remoteexecution.Action{
				CommandDigest: &remoteexecution.Digest{
					Hash:      "0000000000000000000000000000000000000000000000000000000000000002",
					SizeBytes: 234,
				},
				InputRootDigest: &remoteexecution.Digest{
					Hash:      "0000000000000000000000000000000000000000000000000000000000000003",
					SizeBytes: 345,
				},
			},
		},
		metadata)
	testutil.RequireEqualProto(t, &remoteexecution.ExecuteResponse{
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

	// Build directory.
	buildDirectory := mock.NewMockBuildDirectory(ctrl)
	contentAddressableStorage := mock.NewMockBlobAccess(ctrl)

	// Build environment.
	buildDirectoryCreator := mock.NewMockBuildDirectoryCreator(ctrl)
	buildDirectoryCreator.EXPECT().GetBuildDirectory(
		digest.MustNewDigest("ubuntu1804", "0000000000000000000000000000000000000000000000000000000000000001", 123),
		false,
	).Return(buildDirectory, nil, nil)
	filePool := mock.NewMockFilePool(ctrl)
	buildDirectory.EXPECT().InstallHooks(filePool, gomock.Any())

	// Input root creation.
	buildDirectory.EXPECT().Mkdir(path.MustNewComponent("root"), os.FileMode(0o777))
	inputRootDirectory := mock.NewMockBuildDirectory(ctrl)
	buildDirectory.EXPECT().EnterBuildDirectory(path.MustNewComponent("root")).Return(inputRootDirectory, nil)
	inputRootDirectory.EXPECT().MergeDirectoryContents(
		ctx,
		gomock.Any(),
		digest.MustNewDigest("ubuntu1804", "0000000000000000000000000000000000000000000000000000000000000003", 345),
	).Return(nil)
	inputRootDirectory.EXPECT().Mkdir(path.MustNewComponent("dev"), os.FileMode(0o777))
	inputRootDevDirectory := mock.NewMockBuildDirectory(ctrl)
	inputRootDirectory.EXPECT().EnterBuildDirectory(path.MustNewComponent("dev")).Return(inputRootDevDirectory, nil)
	inputRootDevDirectory.EXPECT().Mknod(path.MustNewComponent("null"), os.FileMode(os.ModeDevice|os.ModeCharDevice|0o666), 259).Return(status.Error(codes.Internal, "Device node creation failed"))
	inputRootDevDirectory.EXPECT().Close()
	inputRootDirectory.EXPECT().Close()
	buildDirectory.EXPECT().Close()
	runner := mock.NewMockRunner(ctrl)
	clock := mock.NewMockClock(ctrl)
	inputRootCharacterDevices := map[path.Component]int{
		path.MustNewComponent("null"): 259,
	}
	localBuildExecutor := builder.NewLocalBuildExecutor(contentAddressableStorage, buildDirectoryCreator, runner, clock, time.Hour, time.Hour, inputRootCharacterDevices, 10000, map[string]string{})

	metadata := make(chan *remoteworker.CurrentState_Executing, 10)
	executeResponse := localBuildExecutor.Execute(
		ctx,
		filePool,
		digest.MustNewInstanceName("ubuntu1804"),
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
		},
		metadata)
	testutil.RequireEqualProto(t, &remoteexecution.ExecuteResponse{
		Result: &remoteexecution.ActionResult{
			ExecutionMetadata: &remoteexecution.ExecutedActionMetadata{},
		},
		Status: status.New(codes.Internal, "Failed to create character device \"null\": Device node creation failed").Proto(),
	}, executeResponse)
}
