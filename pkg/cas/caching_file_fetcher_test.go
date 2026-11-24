package cas_test

import (
	"context"
	"os"
	"syscall"
	"testing"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-remote-execution/internal/mock"
	"github.com/buildbarn/bb-remote-execution/pkg/cas"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/eviction"
	"github.com/buildbarn/bb-storage/pkg/filesystem"
	"github.com/buildbarn/bb-storage/pkg/filesystem/path"
	"github.com/buildbarn/bb-storage/pkg/testutil"
	"github.com/stretchr/testify/require"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"go.uber.org/mock/gomock"
)

func TestCachingFileFetcher(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	baseFileFetcher := mock.NewMockFileFetcher(ctrl)
	cacheDirectory := mock.NewMockDirectory(ctrl)
	fileFetcher := cas.NewCachingFileFetcher(baseFileFetcher, cacheDirectory, cas.LinkCachedFile, 1, 1024, eviction.NewLRUSet[string]())

	blobDigest1 := digest.MustNewDigest("example", remoteexecution.DigestFunction_MD5, "8b1a9953c4611296a827abf8c47804d7", 5)
	buildDirectory := mock.NewMockDirectory(ctrl)

	// Errors fetching files from the backend should be propagated.
	baseFileFetcher.EXPECT().GetFile(ctx, blobDigest1, buildDirectory, path.MustNewComponent("hello.txt"), false).
		Return(status.Error(codes.Internal, "Server not reachable"))
	require.Equal(
		t,
		status.Error(codes.Internal, "Server not reachable"),
		fileFetcher.GetFile(ctx, blobDigest1, buildDirectory, path.MustNewComponent("hello.txt"), false),
	)

	// Failing to link the file into the cache should not cause the
	// file to become cached immediately.
	baseFileFetcher.EXPECT().GetFile(ctx, blobDigest1, buildDirectory, path.MustNewComponent("hello.txt"), false)
	buildDirectory.EXPECT().Link(path.MustNewComponent("hello.txt"), cacheDirectory, path.MustNewComponent("3-8b1a9953c4611296a827abf8c47804d7-5-x")).
		Return(syscall.EIO)
	testutil.RequireEqualStatus(
		t,
		status.Error(codes.Internal, "Failed to add cached file \"3-8b1a9953c4611296a827abf8c47804d7-5-x\": input/output error"),
		fileFetcher.GetFile(ctx, blobDigest1, buildDirectory, path.MustNewComponent("hello.txt"), false),
	)

	// Successfully link the file into the cache.
	baseFileFetcher.EXPECT().GetFile(ctx, blobDigest1, buildDirectory, path.MustNewComponent("hello.txt"), false)
	buildDirectory.EXPECT().Link(path.MustNewComponent("hello.txt"), cacheDirectory, path.MustNewComponent("3-8b1a9953c4611296a827abf8c47804d7-5-x"))
	require.NoError(
		t,
		fileFetcher.GetFile(ctx, blobDigest1, buildDirectory, path.MustNewComponent("hello.txt"), false),
	)

	// Once the file is cached, hardlinks should be made in the
	// opposite direction.
	cacheDirectory.EXPECT().Link(path.MustNewComponent("3-8b1a9953c4611296a827abf8c47804d7-5-x"), buildDirectory, path.MustNewComponent("hello.txt"))
	require.NoError(
		t,
		fileFetcher.GetFile(ctx, blobDigest1, buildDirectory, path.MustNewComponent("hello.txt"), false),
	)

	// Failure when accessing a cached file.
	cacheDirectory.EXPECT().Link(path.MustNewComponent("3-8b1a9953c4611296a827abf8c47804d7-5-x"), buildDirectory, path.MustNewComponent("hello.txt")).
		Return(syscall.EIO)
	testutil.RequireEqualStatus(
		t,
		status.Error(codes.Internal, "Failed to materialize cached file \"3-8b1a9953c4611296a827abf8c47804d7-5-x\": input/output error"),
		fileFetcher.GetFile(ctx, blobDigest1, buildDirectory, path.MustNewComponent("hello.txt"), false),
	)

	// Recover from the case where the cache directory gets cleaned
	// up by another process. If hardlinking returns ENOENT, we
	// should fall back to downloading and reinserting the file.
	cacheDirectory.EXPECT().Link(path.MustNewComponent("3-8b1a9953c4611296a827abf8c47804d7-5-x"), buildDirectory, path.MustNewComponent("hello.txt")).
		Return(syscall.ENOENT)
	cacheDirectory.EXPECT().Link(path.MustNewComponent("3-8b1a9953c4611296a827abf8c47804d7-5-x"), buildDirectory, path.MustNewComponent("hello.txt")).
		Return(syscall.ENOENT)
	baseFileFetcher.EXPECT().GetFile(ctx, blobDigest1, buildDirectory, path.MustNewComponent("hello.txt"), false)
	buildDirectory.EXPECT().Link(path.MustNewComponent("hello.txt"), cacheDirectory, path.MustNewComponent("3-8b1a9953c4611296a827abf8c47804d7-5-x"))
	require.NoError(
		t,
		fileFetcher.GetFile(ctx, blobDigest1, buildDirectory, path.MustNewComponent("hello.txt"), false),
	)

	// The above may happen in multiple threads at the same time.
	// EEXIST errors should be ignored in that case.
	cacheDirectory.EXPECT().Link(path.MustNewComponent("3-8b1a9953c4611296a827abf8c47804d7-5-x"), buildDirectory, path.MustNewComponent("hello.txt")).
		Return(syscall.ENOENT)
	cacheDirectory.EXPECT().Link(path.MustNewComponent("3-8b1a9953c4611296a827abf8c47804d7-5-x"), buildDirectory, path.MustNewComponent("hello.txt")).
		Return(syscall.ENOENT)
	baseFileFetcher.EXPECT().GetFile(ctx, blobDigest1, buildDirectory, path.MustNewComponent("hello.txt"), false)
	buildDirectory.EXPECT().Link(path.MustNewComponent("hello.txt"), cacheDirectory, path.MustNewComponent("3-8b1a9953c4611296a827abf8c47804d7-5-x")).
		Return(os.ErrExist)
	require.NoError(
		t,
		fileFetcher.GetFile(ctx, blobDigest1, buildDirectory, path.MustNewComponent("hello.txt"), false),
	)

	// Errors other than EEXIST should be propagated as usual.
	cacheDirectory.EXPECT().Link(path.MustNewComponent("3-8b1a9953c4611296a827abf8c47804d7-5-x"), buildDirectory, path.MustNewComponent("hello.txt")).
		Return(syscall.ENOENT)
	cacheDirectory.EXPECT().Link(path.MustNewComponent("3-8b1a9953c4611296a827abf8c47804d7-5-x"), buildDirectory, path.MustNewComponent("hello.txt")).
		Return(syscall.ENOENT)
	baseFileFetcher.EXPECT().GetFile(ctx, blobDigest1, buildDirectory, path.MustNewComponent("hello.txt"), false)
	buildDirectory.EXPECT().Link(path.MustNewComponent("hello.txt"), cacheDirectory, path.MustNewComponent("3-8b1a9953c4611296a827abf8c47804d7-5-x")).
		Return(syscall.EIO)
	testutil.RequireEqualStatus(
		t,
		status.Error(codes.Internal, "Failed to repair cached file \"3-8b1a9953c4611296a827abf8c47804d7-5-x\": input/output error"),
		fileFetcher.GetFile(ctx, blobDigest1, buildDirectory, path.MustNewComponent("hello.txt"), false),
	)

	blobDigest2 := digest.MustNewDigest("example", remoteexecution.DigestFunction_MD5, "6fc422233a40a75a1f028e11c3cd1140", 7)

	// Errors other than ENOENT when removing a file should be
	// propagated, as we don't want to silently fill up the disk.
	baseFileFetcher.EXPECT().GetFile(ctx, blobDigest2, buildDirectory, path.MustNewComponent("goodbye.txt"), false)
	cacheDirectory.EXPECT().Remove(path.MustNewComponent("3-8b1a9953c4611296a827abf8c47804d7-5-x")).
		Return(syscall.EIO)
	testutil.RequireEqualStatus(
		t,
		status.Error(codes.Internal, "Failed to remove cached file \"3-8b1a9953c4611296a827abf8c47804d7-5-x\": input/output error"),
		fileFetcher.GetFile(ctx, blobDigest2, buildDirectory, path.MustNewComponent("goodbye.txt"), false),
	)

	// ENOENT errors when removing files should be tolerated. It
	// simply means that files in the cache directory were cleaned
	// up by another process.
	baseFileFetcher.EXPECT().GetFile(ctx, blobDigest2, buildDirectory, path.MustNewComponent("goodbye.txt"), false)
	cacheDirectory.EXPECT().Remove(path.MustNewComponent("3-8b1a9953c4611296a827abf8c47804d7-5-x")).
		Return(syscall.ENOENT)
	buildDirectory.EXPECT().Link(path.MustNewComponent("goodbye.txt"), cacheDirectory, path.MustNewComponent("3-6fc422233a40a75a1f028e11c3cd1140-7-x"))
	require.NoError(
		t,
		fileFetcher.GetFile(ctx, blobDigest2, buildDirectory, path.MustNewComponent("goodbye.txt"), false),
	)
}

// readinessProbe matches whichever name a readiness check picks for its
// probe file first, and only that name afterwards.
type readinessProbe struct {
	name *path.Component
}

func newReadinessProbe() readinessProbe {
	return readinessProbe{name: &path.Component{}}
}

func (m readinessProbe) Matches(x any) bool {
	name, ok := x.(path.Component)
	if !ok {
		return false
	}
	if *m.name == (path.Component{}) {
		*m.name = name
	}
	return name == *m.name
}

func (readinessProbe) String() string {
	return "is the readiness probe"
}

func TestCachingFileFetcherCheckReadiness(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	baseFileFetcher := mock.NewMockFileFetcher(ctrl)
	cacheDirectory := mock.NewMockDirectory(ctrl)
	buildDirectory := mock.NewMockDirectory(ctrl)
	fileFetcher := cas.NewCachingFileFetcher(baseFileFetcher, cacheDirectory, cas.LinkCachedFile, 1, 1024, eviction.NewLRUSet[string]())

	// Runners share the cache directory and may check readiness
	// concurrently, so no two checks may use the same probe name.
	probeNames := map[path.Component]struct{}{}
	expectCreateProbe := func(t *testing.T) readinessProbe {
		probe := newReadinessProbe()
		probeFile := mock.NewMockFileWriter(ctrl)
		buildDirectory.EXPECT().OpenWrite(probe, filesystem.CreateExcl(0o444)).Return(probeFile, nil)
		probeFile.EXPECT().Close()
		t.Cleanup(func() {
			require.NotContains(t, probeNames, *probe.name)
			probeNames[*probe.name] = struct{}{}
		})
		return probe
	}

	t.Run("Success", func(t *testing.T) {
		// The probe must travel in both directions GetFile() uses,
		// and be removed from both directories afterwards, so that
		// neither build actions nor the cache see it.
		probe := expectCreateProbe(t)
		gomock.InOrder(
			buildDirectory.EXPECT().Link(probe, cacheDirectory, probe),
			buildDirectory.EXPECT().Remove(probe),
			cacheDirectory.EXPECT().Link(probe, buildDirectory, probe),
			buildDirectory.EXPECT().Remove(probe),
			cacheDirectory.EXPECT().Remove(probe),
		)
		require.NoError(t, fileFetcher.CheckReadiness(ctx, buildDirectory))
	})

	t.Run("IntoCacheFails", func(t *testing.T) {
		// Adding a downloaded file to the cache fails if the build
		// and cache directories are on different file systems. A
		// worker that would fail every cold fetch must not be ready.
		probe := expectCreateProbe(t)
		buildDirectory.EXPECT().Link(probe, cacheDirectory, probe).Return(syscall.EXDEV)
		buildDirectory.EXPECT().Remove(probe)
		cacheDirectory.EXPECT().Remove(probe).Return(syscall.ENOENT)
		testutil.RequireEqualStatus(
			t,
			status.Error(codes.Internal, "Failed to materialize readiness probe file into cache directory: "+syscall.EXDEV.Error()),
			fileFetcher.CheckReadiness(ctx, buildDirectory),
		)
	})

	t.Run("FromCacheFails", func(t *testing.T) {
		// macOS checks permissions on the source and destination
		// separately, so materializing out of the cache can fail
		// even if materializing into it succeeded. A worker that
		// would fail every cache hit must not be ready either.
		probe := expectCreateProbe(t)
		buildDirectory.EXPECT().Link(probe, cacheDirectory, probe)
		buildDirectory.EXPECT().Remove(probe)
		cacheDirectory.EXPECT().Link(probe, buildDirectory, probe).Return(syscall.EPERM)
		buildDirectory.EXPECT().Remove(probe).Return(syscall.ENOENT)
		cacheDirectory.EXPECT().Remove(probe)
		testutil.RequireEqualStatus(
			t,
			status.Error(codes.Internal, "Failed to materialize readiness probe file from cache directory: "+syscall.EPERM.Error()),
			fileFetcher.CheckReadiness(ctx, buildDirectory),
		)
	})

	t.Run("RemovalFails", func(t *testing.T) {
		// A probe that cannot be removed would be visible to build
		// actions, so failing to remove it must be reported.
		probe := expectCreateProbe(t)
		buildDirectory.EXPECT().Link(probe, cacheDirectory, probe)
		buildDirectory.EXPECT().Remove(probe)
		cacheDirectory.EXPECT().Link(probe, buildDirectory, probe)
		buildDirectory.EXPECT().Remove(probe).Return(syscall.EIO)
		cacheDirectory.EXPECT().Remove(probe)
		testutil.RequireEqualStatus(
			t,
			status.Error(codes.Internal, "Failed to remove readiness probe file: "+syscall.EIO.Error()),
			fileFetcher.CheckReadiness(ctx, buildDirectory),
		)
	})
}
