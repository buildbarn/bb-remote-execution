//go:build darwin

package cas_test

import (
	"context"
	"os"
	"path/filepath"
	"syscall"
	"testing"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-remote-execution/internal/mock"
	"github.com/buildbarn/bb-remote-execution/pkg/cas"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/eviction"
	"github.com/buildbarn/bb-storage/pkg/filesystem"
	"github.com/buildbarn/bb-storage/pkg/filesystem/path"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
)

func openLocalDirectory(t *testing.T, location string) filesystem.DirectoryCloser {
	t.Helper()
	require.NoError(t, os.MkdirAll(location, 0o700))
	directory, err := filesystem.NewLocalDirectory(path.LocalFormat.NewParser(location))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, directory.Close()) })
	return directory
}

// expectDownload makes the base FileFetcher write contents to the
// destination once, as the CAS-backed one does, so that the real
// filesystem calls made around it can be observed.
func expectDownload(ctrl *gomock.Controller, blobDigest digest.Digest, contents string) cas.FileFetcher {
	base := mock.NewMockFileFetcher(ctrl)
	base.EXPECT().GetFile(gomock.Any(), blobDigest, gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
		func(ctx context.Context, blobDigest digest.Digest, directory filesystem.Directory, name path.Component, isExecutable bool) error {
			var mode os.FileMode = 0o444
			if isExecutable {
				mode = 0o555
			}
			w, err := directory.OpenAppend(name, filesystem.CreateExcl(mode))
			if err != nil {
				return err
			}
			defer w.Close()
			_, err = w.Write([]byte(contents))
			return err
		},
	)
	return base
}

func requireOwnInode(t *testing.T, location string) os.FileInfo {
	t.Helper()
	info, err := os.Lstat(location)
	require.NoError(t, err)
	require.Equal(t, uint16(1), info.Sys().(*syscall.Stat_t).Nlink)
	return info
}

func TestCachingFileFetcherCloneCheckReadiness(t *testing.T) {
	// The readiness check must pass on a volume that supports cloning,
	// and must not leave its probe file in the build directory, where
	// build actions would see it, or in the cache.
	root := t.TempDir()
	cache := openLocalDirectory(t, filepath.Join(root, "cache"))
	build := openLocalDirectory(t, filepath.Join(root, "build"))
	ctrl, ctx := gomock.WithContext(context.Background(), t)
	fetcher := cas.NewCachingFileFetcher(mock.NewMockFileFetcher(ctrl), cache, cas.CloneCachedFile, 10, 1024, eviction.NewLRUSet[string]())

	require.NoError(t, fetcher.CheckReadiness(ctx, build))
	for _, name := range []string{"cache", "build"} {
		entries, err := os.ReadDir(filepath.Join(root, name))
		require.NoError(t, err)
		require.Empty(t, entries)
	}
}

func TestCachingFileFetcherCloneDistinctInodes(t *testing.T) {
	// Concurrent build actions sharing one hardlinked inode can be
	// handed each other's paths by the kernel when resolving an
	// executable's location. Every materialized file must therefore have
	// an inode of its own, from a cold cache and then a warm one.
	root := t.TempDir()
	cache := openLocalDirectory(t, filepath.Join(root, "cache"))
	first := openLocalDirectory(t, filepath.Join(root, "first"))
	second := openLocalDirectory(t, filepath.Join(root, "second"))
	ctrl, ctx := gomock.WithContext(context.Background(), t)
	blobDigest := digest.MustNewDigest("example", remoteexecution.DigestFunction_MD5, "8b1a9953c4611296a827abf8c47804d7", 5)
	fetcher := cas.NewCachingFileFetcher(expectDownload(ctrl, blobDigest, "Hello"), cache, cas.CloneCachedFile, 10, 1024, eviction.NewLRUSet[string]())

	require.NoError(t, fetcher.GetFile(ctx, blobDigest, first, path.MustNewComponent("python3"), true))
	require.NoError(t, fetcher.GetFile(ctx, blobDigest, second, path.MustNewComponent("python3"), true))

	cached := requireOwnInode(t, filepath.Join(root, "cache", blobDigest.GetKey(digest.KeyWithoutInstance)+"+x"))
	for _, name := range []string{"first", "second"} {
		materialized := requireOwnInode(t, filepath.Join(root, name, "python3"))
		require.False(t, os.SameFile(cached, materialized))
		require.Equal(t, os.FileMode(0o555), materialized.Mode().Perm())
		contents, err := os.ReadFile(filepath.Join(root, name, "python3"))
		require.NoError(t, err)
		require.Equal(t, "Hello", string(contents))
	}
}

func TestCachingFileFetcherClonePkgInfo(t *testing.T) {
	// macOS refuses to create a hardlink to an application bundle's
	// PkgInfo file, so a cold fetch that downloads into the bundle must
	// not depend on linking out of it.
	root := t.TempDir()
	cache := openLocalDirectory(t, filepath.Join(root, "cache"))
	app := openLocalDirectory(t, filepath.Join(root, "Example.app"))
	ctrl, ctx := gomock.WithContext(context.Background(), t)
	blobDigest := digest.MustNewDigest("example", remoteexecution.DigestFunction_SHA256, "82502191c9484b04d685374f9879a0066069c49b8acae7a04b01d38d07e8eca0", 8)
	fetcher := cas.NewCachingFileFetcher(expectDownload(ctrl, blobDigest, "APPL????"), cache, cas.CloneCachedFile, 10, 1024, eviction.NewLRUSet[string]())

	require.NoError(t, fetcher.GetFile(ctx, blobDigest, app, path.MustNewComponent("PkgInfo"), false))
	contents, err := os.ReadFile(filepath.Join(root, "Example.app", "PkgInfo"))
	require.NoError(t, err)
	require.Equal(t, "APPL????", string(contents))
	requireOwnInode(t, filepath.Join(root, "cache", blobDigest.GetKey(digest.KeyWithoutInstance)+"-x"))

	// A warm fetch into a second bundle is served from the cache.
	other := openLocalDirectory(t, filepath.Join(root, "Other.app"))
	require.NoError(t, fetcher.GetFile(ctx, blobDigest, other, path.MustNewComponent("PkgInfo"), false))
	requireOwnInode(t, filepath.Join(root, "Other.app", "PkgInfo"))
}
