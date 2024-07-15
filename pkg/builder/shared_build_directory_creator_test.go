package builder_test

import (
	"context"
	"os"
	"sync/atomic"
	"testing"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-remote-execution/internal/mock"
	"github.com/buildbarn/bb-remote-execution/pkg/builder"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/filesystem/path"
	"github.com/buildbarn/bb-storage/pkg/testutil"
	"github.com/stretchr/testify/require"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"go.uber.org/mock/gomock"
)

func TestSharedBuildDirectoryCreatorGetBuildDirectoryFailure(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	// Failure to create environment should simply be forwarded.
	baseBuildDirectoryCreator := mock.NewMockBuildDirectoryCreator(ctrl)
	actionDigest := digest.MustNewDigest("debian8", remoteexecution.DigestFunction_SHA256, "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855", 0)
	baseBuildDirectoryCreator.EXPECT().GetBuildDirectory(ctx, &actionDigest).
		Return(nil, nil, status.Error(codes.Internal, "No space left on device"))

	var nextParallelActionID atomic.Uint64
	buildDirectoryCreator := builder.NewSharedBuildDirectoryCreator(baseBuildDirectoryCreator, &nextParallelActionID)
	_, _, err := buildDirectoryCreator.GetBuildDirectory(ctx, &actionDigest)
	testutil.RequireEqualStatus(t, status.Error(codes.Internal, "No space left on device"), err)
}

func TestSharedBuildDirectoryCreatorMkdirFailure(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	// Failure to create a build subdirectory is always an internal error.
	baseBuildDirectoryCreator := mock.NewMockBuildDirectoryCreator(ctrl)
	baseBuildDirectory := mock.NewMockBuildDirectory(ctrl)
	actionDigest := digest.MustNewDigest("debian8", remoteexecution.DigestFunction_SHA256, "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855", 0)
	baseBuildDirectoryCreator.EXPECT().GetBuildDirectory(ctx, &actionDigest).
		Return(baseBuildDirectory, ((*path.Trace)(nil)).Append(path.MustNewComponent("base-directory")), nil)
	baseBuildDirectory.EXPECT().Mkdir(path.MustNewComponent("e3b0c44298fc1c14"), os.FileMode(0o777)).Return(
		status.Error(codes.AlreadyExists, "Directory already exists"))
	baseBuildDirectory.EXPECT().Close()

	var nextParallelActionID atomic.Uint64
	buildDirectoryCreator := builder.NewSharedBuildDirectoryCreator(baseBuildDirectoryCreator, &nextParallelActionID)
	_, _, err := buildDirectoryCreator.GetBuildDirectory(ctx, &actionDigest)
	testutil.RequireEqualStatus(t, status.Error(codes.Internal, "Failed to create build directory \"base-directory/e3b0c44298fc1c14\": Directory already exists"), err)
}

func TestSharedBuildDirectoryCreatorEnterBuildDirectoryFailure(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	// Failure to enter a build subdirectory is always an internal error.
	baseBuildDirectoryCreator := mock.NewMockBuildDirectoryCreator(ctrl)
	baseBuildDirectory := mock.NewMockBuildDirectory(ctrl)
	actionDigest := digest.MustNewDigest("debian8", remoteexecution.DigestFunction_SHA256, "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855", 0)
	baseBuildDirectoryCreator.EXPECT().GetBuildDirectory(ctx, &actionDigest).
		Return(baseBuildDirectory, ((*path.Trace)(nil)).Append(path.MustNewComponent("base-directory")), nil)
	baseBuildDirectory.EXPECT().Mkdir(path.MustNewComponent("e3b0c44298fc1c14"), os.FileMode(0o777))
	baseBuildDirectory.EXPECT().EnterBuildDirectory(path.MustNewComponent("e3b0c44298fc1c14")).Return(nil, status.Error(codes.ResourceExhausted, "Out of file descriptors"))
	baseBuildDirectory.EXPECT().Remove(path.MustNewComponent("e3b0c44298fc1c14"))
	baseBuildDirectory.EXPECT().Close()

	var nextParallelActionID atomic.Uint64
	buildDirectoryCreator := builder.NewSharedBuildDirectoryCreator(baseBuildDirectoryCreator, &nextParallelActionID)
	_, _, err := buildDirectoryCreator.GetBuildDirectory(ctx, &actionDigest)
	testutil.RequireEqualStatus(t, status.Error(codes.Internal, "Failed to enter build directory \"base-directory/e3b0c44298fc1c14\": Out of file descriptors"), err)
}

func TestSharedBuildDirectoryCreatorCloseChildFailure(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	// Directory closure errors should be propagated.
	baseBuildDirectoryCreator := mock.NewMockBuildDirectoryCreator(ctrl)
	baseBuildDirectory := mock.NewMockBuildDirectory(ctrl)
	baseBuildDirectoryPath := ((*path.Trace)(nil)).Append(path.MustNewComponent("base-directory"))
	actionDigest := digest.MustNewDigest("debian8", remoteexecution.DigestFunction_SHA256, "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855", 0)
	baseBuildDirectoryCreator.EXPECT().GetBuildDirectory(ctx, &actionDigest).
		Return(baseBuildDirectory, baseBuildDirectoryPath, nil)
	baseBuildDirectory.EXPECT().Mkdir(path.MustNewComponent("e3b0c44298fc1c14"), os.FileMode(0o777))
	subDirectory := mock.NewMockBuildDirectory(ctrl)
	baseBuildDirectory.EXPECT().EnterBuildDirectory(path.MustNewComponent("e3b0c44298fc1c14")).Return(subDirectory, nil)
	subDirectory.EXPECT().Close().Return(status.Error(codes.Internal, "Bad file descriptor"))
	baseBuildDirectory.EXPECT().RemoveAll(path.MustNewComponent("e3b0c44298fc1c14"))
	baseBuildDirectory.EXPECT().Close()

	var nextParallelActionID atomic.Uint64
	buildDirectoryCreator := builder.NewSharedBuildDirectoryCreator(baseBuildDirectoryCreator, &nextParallelActionID)
	buildDirectory, buildDirectoryPath, err := buildDirectoryCreator.GetBuildDirectory(ctx, &actionDigest)
	require.NoError(t, err)
	require.Equal(t, baseBuildDirectoryPath.Append(path.MustNewComponent("e3b0c44298fc1c14")), buildDirectoryPath)
	testutil.RequireEqualStatus(t, status.Error(codes.Internal, "Failed to close build directory \"base-directory/e3b0c44298fc1c14\": Bad file descriptor"), buildDirectory.Close())
}

func TestSharedBuildDirectoryCreatorRemoveAllFailure(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	// Directory removal errors should be propagated. Permission
	// errors should be converted to internal errors, as they
	// indicate problems with the infrastructure.
	baseBuildDirectoryCreator := mock.NewMockBuildDirectoryCreator(ctrl)
	baseBuildDirectory := mock.NewMockBuildDirectory(ctrl)
	baseBuildDirectoryPath := ((*path.Trace)(nil)).Append(path.MustNewComponent("base-directory"))
	actionDigest := digest.MustNewDigest("debian8", remoteexecution.DigestFunction_SHA256, "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855", 0)
	baseBuildDirectoryCreator.EXPECT().GetBuildDirectory(ctx, &actionDigest).
		Return(baseBuildDirectory, baseBuildDirectoryPath, nil)
	baseBuildDirectory.EXPECT().Mkdir(path.MustNewComponent("e3b0c44298fc1c14"), os.FileMode(0o777))
	subDirectory := mock.NewMockBuildDirectory(ctrl)
	baseBuildDirectory.EXPECT().EnterBuildDirectory(path.MustNewComponent("e3b0c44298fc1c14")).Return(subDirectory, nil)
	subDirectory.EXPECT().Close()
	baseBuildDirectory.EXPECT().RemoveAll(path.MustNewComponent("e3b0c44298fc1c14")).Return(status.Error(codes.PermissionDenied, "Directory is owned by another user"))
	baseBuildDirectory.EXPECT().Close()

	var nextParallelActionID atomic.Uint64
	buildDirectoryCreator := builder.NewSharedBuildDirectoryCreator(baseBuildDirectoryCreator, &nextParallelActionID)
	buildDirectory, buildDirectoryPath, err := buildDirectoryCreator.GetBuildDirectory(ctx, &actionDigest)
	require.NoError(t, err)
	require.Equal(t, baseBuildDirectoryPath.Append(path.MustNewComponent("e3b0c44298fc1c14")), buildDirectoryPath)
	testutil.RequireEqualStatus(t, status.Error(codes.Internal, "Failed to remove build directory \"base-directory/e3b0c44298fc1c14\": Directory is owned by another user"), buildDirectory.Close())
}

func TestSharedBuildDirectoryCreatorCloseParentFailure(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	// Directory closure errors on the parent should also be
	// propagated, but there is no need to prefix any additional
	// info. The base BuildDirectoryCreator will already be
	// responsible for injecting more detailed errors.
	baseBuildDirectoryCreator := mock.NewMockBuildDirectoryCreator(ctrl)
	baseBuildDirectory := mock.NewMockBuildDirectory(ctrl)
	baseBuildDirectoryPath := ((*path.Trace)(nil)).Append(path.MustNewComponent("base-directory"))
	actionDigest := digest.MustNewDigest("debian8", remoteexecution.DigestFunction_SHA256, "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855", 0)
	baseBuildDirectoryCreator.EXPECT().GetBuildDirectory(ctx, &actionDigest).
		Return(baseBuildDirectory, baseBuildDirectoryPath, nil)
	baseBuildDirectory.EXPECT().Mkdir(path.MustNewComponent("e3b0c44298fc1c14"), os.FileMode(0o777))
	subDirectory := mock.NewMockBuildDirectory(ctrl)
	baseBuildDirectory.EXPECT().EnterBuildDirectory(path.MustNewComponent("e3b0c44298fc1c14")).Return(subDirectory, nil)
	subDirectory.EXPECT().Close()
	baseBuildDirectory.EXPECT().RemoveAll(path.MustNewComponent("e3b0c44298fc1c14"))
	baseBuildDirectory.EXPECT().Close().Return(status.Error(codes.Internal, "Bad file descriptor"))

	var nextParallelActionID atomic.Uint64
	buildDirectoryCreator := builder.NewSharedBuildDirectoryCreator(baseBuildDirectoryCreator, &nextParallelActionID)
	buildDirectory, buildDirectoryPath, err := buildDirectoryCreator.GetBuildDirectory(ctx, &actionDigest)
	require.NoError(t, err)
	require.Equal(t, baseBuildDirectoryPath.Append(path.MustNewComponent("e3b0c44298fc1c14")), buildDirectoryPath)
	testutil.RequireEqualStatus(t, status.Error(codes.Internal, "Bad file descriptor"), buildDirectory.Close())
}

func TestSharedBuildDirectoryCreatorSuccessNotParallel(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	// Successful build in a subdirectory for an action that does
	// not run in parallel. The subdirectory name is based on the
	// action digest.
	baseBuildDirectoryCreator := mock.NewMockBuildDirectoryCreator(ctrl)
	baseBuildDirectory := mock.NewMockBuildDirectory(ctrl)
	baseBuildDirectoryPath := ((*path.Trace)(nil)).Append(path.MustNewComponent("base-directory"))
	actionDigest := digest.MustNewDigest("debian8", remoteexecution.DigestFunction_SHA256, "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855", 0)
	baseBuildDirectoryCreator.EXPECT().GetBuildDirectory(ctx, &actionDigest).
		Return(baseBuildDirectory, baseBuildDirectoryPath, nil)
	baseBuildDirectory.EXPECT().Mkdir(path.MustNewComponent("e3b0c44298fc1c14"), os.FileMode(0o777))
	subDirectory := mock.NewMockBuildDirectory(ctrl)
	baseBuildDirectory.EXPECT().EnterBuildDirectory(path.MustNewComponent("e3b0c44298fc1c14")).Return(subDirectory, nil)
	subDirectory.EXPECT().Close()
	baseBuildDirectory.EXPECT().RemoveAll(path.MustNewComponent("e3b0c44298fc1c14"))
	baseBuildDirectory.EXPECT().Close()

	var nextParallelActionID atomic.Uint64
	buildDirectoryCreator := builder.NewSharedBuildDirectoryCreator(baseBuildDirectoryCreator, &nextParallelActionID)
	buildDirectory, buildDirectoryPath, err := buildDirectoryCreator.GetBuildDirectory(ctx, &actionDigest)
	require.NoError(t, err)
	require.Equal(t, baseBuildDirectoryPath.Append(path.MustNewComponent("e3b0c44298fc1c14")), buildDirectoryPath)
	require.NoError(t, buildDirectory.Close())
}

func TestSharedBuildDirectoryCreatorMkdirSuccessParallel(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	baseBuildDirectoryCreator := mock.NewMockBuildDirectoryCreator(ctrl)
	baseBuildDirectory := mock.NewMockBuildDirectory(ctrl)
	baseBuildDirectoryPath := ((*path.Trace)(nil)).Append(path.MustNewComponent("base-directory"))
	var nextParallelActionID atomic.Uint64
	buildDirectoryCreator := builder.NewSharedBuildDirectoryCreator(baseBuildDirectoryCreator, &nextParallelActionID)

	// Build directories for actions that run in parallel are simply
	// named incrementally to prevent collisions.
	baseBuildDirectoryCreator.EXPECT().GetBuildDirectory(ctx, nil).
		Return(baseBuildDirectory, baseBuildDirectoryPath, nil)
	baseBuildDirectory.EXPECT().Mkdir(path.MustNewComponent("1"), os.FileMode(0o777)).Return(
		status.Error(codes.Internal, "Foo"))
	baseBuildDirectory.EXPECT().Close()
	_, _, err := buildDirectoryCreator.GetBuildDirectory(ctx, nil)
	testutil.RequireEqualStatus(t, status.Error(codes.Internal, "Failed to create build directory \"base-directory/1\": Foo"), err)

	baseBuildDirectoryCreator.EXPECT().GetBuildDirectory(ctx, nil).
		Return(baseBuildDirectory, baseBuildDirectoryPath, nil)
	baseBuildDirectory.EXPECT().Mkdir(path.MustNewComponent("2"), os.FileMode(0o777)).Return(
		status.Error(codes.Internal, "Foo"))
	baseBuildDirectory.EXPECT().Close()
	_, _, err = buildDirectoryCreator.GetBuildDirectory(ctx, nil)
	testutil.RequireEqualStatus(t, status.Error(codes.Internal, "Failed to create build directory \"base-directory/2\": Foo"), err)
}
