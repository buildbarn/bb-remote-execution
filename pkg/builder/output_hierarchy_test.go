package builder_test

import (
	"context"
	"os"
	"syscall"
	"testing"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-remote-execution/internal/mock"
	"github.com/buildbarn/bb-remote-execution/pkg/builder"
	"github.com/buildbarn/bb-storage/pkg/blobstore/buffer"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/filesystem"
	"github.com/buildbarn/bb-storage/pkg/filesystem/path"
	"github.com/buildbarn/bb-storage/pkg/testutil"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestOutputHierarchyCreation(t *testing.T) {
	t.Run("AbsoluteWorkingDirectory", func(t *testing.T) {
		_, err := builder.NewOutputHierarchy(&remoteexecution.Command{
			WorkingDirectory: "/tmp/hello/../..",
		})
		require.Equal(t, err, status.Error(codes.InvalidArgument, "Invalid working directory: Path is absolute, while a relative path was expected"))
	})

	t.Run("InvalidWorkingDirectory", func(t *testing.T) {
		_, err := builder.NewOutputHierarchy(&remoteexecution.Command{
			WorkingDirectory: "hello/../..",
		})
		require.Equal(t, err, status.Error(codes.InvalidArgument, "Invalid working directory: Path resolves to a location outside the input root directory"))
	})

	t.Run("AbsoluteOutputDirectory", func(t *testing.T) {
		_, err := builder.NewOutputHierarchy(&remoteexecution.Command{
			WorkingDirectory:  ".",
			OutputDirectories: []string{"/etc/passwd"},
		})
		require.Equal(t, err, status.Error(codes.InvalidArgument, "Invalid output directory \"/etc/passwd\": Path is absolute, while a relative path was expected"))
	})

	t.Run("InvalidOutputDirectory", func(t *testing.T) {
		_, err := builder.NewOutputHierarchy(&remoteexecution.Command{
			WorkingDirectory:  "hello",
			OutputDirectories: []string{"../.."},
		})
		require.Equal(t, err, status.Error(codes.InvalidArgument, "Invalid output directory \"../..\": Path resolves to a location outside the input root directory"))
	})

	t.Run("InvalidOutputFile", func(t *testing.T) {
		_, err := builder.NewOutputHierarchy(&remoteexecution.Command{
			WorkingDirectory: "hello",
			OutputFiles:      []string{".."},
		})
		require.Equal(t, err, status.Error(codes.InvalidArgument, "Output file \"..\" resolves to the input root directory"))
	})
}

func TestOutputHierarchyCreateParentDirectories(t *testing.T) {
	ctrl := gomock.NewController(t)

	root := mock.NewMockBuildDirectory(ctrl)

	t.Run("Noop", func(t *testing.T) {
		// No parent directories should be created.
		oh, err := builder.NewOutputHierarchy(&remoteexecution.Command{
			WorkingDirectory: ".",
		})
		require.NoError(t, err)
		require.NoError(t, oh.CreateParentDirectories(root))
	})

	t.Run("WorkingDirectory", func(t *testing.T) {
		// REv2 explicitly states that the working directory
		// must be a directory that exists in the input root.
		// Using a subdirectory as a working directory should
		// not cause any Mkdir() calls.
		oh, err := builder.NewOutputHierarchy(&remoteexecution.Command{
			WorkingDirectory: "foo/bar",
		})
		require.NoError(t, err)
		require.NoError(t, oh.CreateParentDirectories(root))
	})

	t.Run("StillNoop", func(t *testing.T) {
		// All of the provided paths expand to (locations under)
		// the root directory. There is thus no need to create
		// any output directories.
		oh, err := builder.NewOutputHierarchy(&remoteexecution.Command{
			WorkingDirectory:  "foo",
			OutputDirectories: []string{".."},
			OutputFiles:       []string{"../file"},
			OutputPaths:       []string{"../path"},
		})
		require.NoError(t, err)
		require.NoError(t, oh.CreateParentDirectories(root))
	})

	t.Run("Success", func(t *testing.T) {
		// Create /foo/bar/baz.
		root.EXPECT().Mkdir(path.MustNewComponent("foo"), os.FileMode(0777))
		foo := mock.NewMockBuildDirectory(ctrl)
		root.EXPECT().EnterBuildDirectory(path.MustNewComponent("foo")).Return(foo, nil)
		foo.EXPECT().Mkdir(path.MustNewComponent("bar"), os.FileMode(0777))
		bar := mock.NewMockBuildDirectory(ctrl)
		foo.EXPECT().EnterBuildDirectory(path.MustNewComponent("bar")).Return(bar, nil)
		bar.EXPECT().Mkdir(path.MustNewComponent("baz"), os.FileMode(0777))
		bar.EXPECT().Close()
		// Create /foo/qux.
		foo.EXPECT().Mkdir(path.MustNewComponent("qux"), os.FileMode(0777))
		foo.EXPECT().Close()
		// Create /alice.
		root.EXPECT().Mkdir(path.MustNewComponent("alice"), os.FileMode(0777))

		oh, err := builder.NewOutputHierarchy(&remoteexecution.Command{
			WorkingDirectory:  "foo",
			OutputDirectories: []string{"bar/baz"},
			OutputFiles:       []string{"../foo/qux/xyzzy"},
			OutputPaths:       []string{"../alice/bob"},
		})
		require.NoError(t, err)
		require.NoError(t, oh.CreateParentDirectories(root))
	})

	t.Run("MkdirFailureParent", func(t *testing.T) {
		// Failure to create the parent directory of a location
		// where an output file is expected.
		root.EXPECT().Mkdir(path.MustNewComponent("foo"), os.FileMode(0777))
		foo := mock.NewMockBuildDirectory(ctrl)
		root.EXPECT().EnterBuildDirectory(path.MustNewComponent("foo")).Return(foo, nil)
		foo.EXPECT().Mkdir(path.MustNewComponent("bar"), os.FileMode(0777)).Return(status.Error(codes.Internal, "I/O error"))
		foo.EXPECT().Close()

		oh, err := builder.NewOutputHierarchy(&remoteexecution.Command{
			WorkingDirectory: "foo",
			OutputFiles:      []string{"bar/baz"},
		})
		require.NoError(t, err)
		require.Equal(
			t,
			status.Error(codes.Internal, "Failed to create output parent directory \"foo/bar\": I/O error"),
			oh.CreateParentDirectories(root))
	})

	t.Run("MkdirFailureParentExists", func(t *testing.T) {
		// This test is identical to the previous, except that
		// the error is EEXIST. This should not cause a hard
		// failure.
		root.EXPECT().Mkdir(path.MustNewComponent("foo"), os.FileMode(0777))
		foo := mock.NewMockBuildDirectory(ctrl)
		root.EXPECT().EnterBuildDirectory(path.MustNewComponent("foo")).Return(foo, nil)
		foo.EXPECT().Mkdir(path.MustNewComponent("bar"), os.FileMode(0777)).Return(syscall.EEXIST)
		foo.EXPECT().Close()

		oh, err := builder.NewOutputHierarchy(&remoteexecution.Command{
			WorkingDirectory: "foo",
			OutputFiles:      []string{"bar/baz"},
		})
		require.NoError(t, err)
		require.NoError(t, oh.CreateParentDirectories(root))
	})

	t.Run("MkdirFailureOutput", func(t *testing.T) {
		// Failure to create a location where an output
		// directory is expected.
		root.EXPECT().Mkdir(path.MustNewComponent("foo"), os.FileMode(0777))
		foo := mock.NewMockBuildDirectory(ctrl)
		root.EXPECT().EnterBuildDirectory(path.MustNewComponent("foo")).Return(foo, nil)
		foo.EXPECT().Mkdir(path.MustNewComponent("bar"), os.FileMode(0777)).Return(status.Error(codes.Internal, "I/O error"))
		foo.EXPECT().Close()

		oh, err := builder.NewOutputHierarchy(&remoteexecution.Command{
			WorkingDirectory:  "foo",
			OutputDirectories: []string{"bar"},
		})
		require.NoError(t, err)
		require.Equal(
			t,
			status.Error(codes.Internal, "Failed to create output directory \"foo/bar\": I/O error"),
			oh.CreateParentDirectories(root))
	})

	t.Run("MkdirFailureOutputExists", func(t *testing.T) {
		// This test is identical to the previous, except that
		// the error is EEXIST. This should not cause a hard
		// failure.
		root.EXPECT().Mkdir(path.MustNewComponent("foo"), os.FileMode(0777))
		foo := mock.NewMockBuildDirectory(ctrl)
		root.EXPECT().EnterBuildDirectory(path.MustNewComponent("foo")).Return(foo, nil)
		foo.EXPECT().Mkdir(path.MustNewComponent("bar"), os.FileMode(0777)).Return(syscall.EEXIST)
		foo.EXPECT().Close()

		oh, err := builder.NewOutputHierarchy(&remoteexecution.Command{
			WorkingDirectory:  "foo",
			OutputDirectories: []string{"bar"},
		})
		require.NoError(t, err)
		require.NoError(t, oh.CreateParentDirectories(root))
	})

	t.Run("EnterFailure", func(t *testing.T) {
		root.EXPECT().Mkdir(path.MustNewComponent("foo"), os.FileMode(0777))
		foo := mock.NewMockBuildDirectory(ctrl)
		root.EXPECT().EnterBuildDirectory(path.MustNewComponent("foo")).Return(foo, nil)
		foo.EXPECT().Mkdir(path.MustNewComponent("bar"), os.FileMode(0777))
		foo.EXPECT().EnterBuildDirectory(path.MustNewComponent("bar")).Return(nil, status.Error(codes.Internal, "I/O error"))
		foo.EXPECT().Close()

		oh, err := builder.NewOutputHierarchy(&remoteexecution.Command{
			WorkingDirectory:  "foo",
			OutputDirectories: []string{"bar/baz"},
		})
		require.NoError(t, err)
		require.Equal(
			t,
			status.Error(codes.Internal, "Failed to enter output parent directory \"foo/bar\": I/O error"),
			oh.CreateParentDirectories(root))
	})
}

func TestOutputHierarchyUploadOutputs(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	root := mock.NewMockUploadableDirectory(ctrl)
	contentAddressableStorage := mock.NewMockBlobAccess(ctrl)
	digestFunction := digest.MustNewDigest("example", "8b1a9953c4611296a827abf8c47804d7", 5).GetDigestFunction()

	t.Run("Noop", func(t *testing.T) {
		// Uploading of a build action with no declared outputs
		// should not trigger any I/O.
		oh, err := builder.NewOutputHierarchy(&remoteexecution.Command{
			WorkingDirectory: ".",
		})
		require.NoError(t, err)
		var actionResult remoteexecution.ActionResult
		require.NoError(t, oh.UploadOutputs(ctx, root, contentAddressableStorage, digestFunction, &actionResult))
		require.Equal(t, remoteexecution.ActionResult{}, actionResult)
	})

	t.Run("Success", func(t *testing.T) {
		// Declare output directories, files and paths. For each
		// of these output types, let them match one of the
		// valid file types.
		foo := mock.NewMockUploadableDirectory(ctrl)
		root.EXPECT().EnterUploadableDirectory(path.MustNewComponent("foo")).Return(foo, nil)

		// Calls triggered to obtain the file type of the outputs.
		foo.EXPECT().Lstat(path.MustNewComponent("directory-directory")).Return(filesystem.NewFileInfo(path.MustNewComponent("directory-directory"), filesystem.FileTypeDirectory), nil)
		foo.EXPECT().Lstat(path.MustNewComponent("directory-symlink")).Return(filesystem.NewFileInfo(path.MustNewComponent("directory-symlink"), filesystem.FileTypeSymlink), nil)
		foo.EXPECT().Lstat(path.MustNewComponent("directory-enoent")).Return(filesystem.FileInfo{}, syscall.ENOENT)
		foo.EXPECT().Lstat(path.MustNewComponent("file-regular")).Return(filesystem.NewFileInfo(path.MustNewComponent("file-regular"), filesystem.FileTypeRegularFile), nil)
		foo.EXPECT().Lstat(path.MustNewComponent("file-executable")).Return(filesystem.NewFileInfo(path.MustNewComponent("file-executable"), filesystem.FileTypeExecutableFile), nil)
		foo.EXPECT().Lstat(path.MustNewComponent("file-symlink")).Return(filesystem.NewFileInfo(path.MustNewComponent("file-symlink"), filesystem.FileTypeSymlink), nil)
		foo.EXPECT().Lstat(path.MustNewComponent("file-enoent")).Return(filesystem.FileInfo{}, syscall.ENOENT)
		foo.EXPECT().Lstat(path.MustNewComponent("path-regular")).Return(filesystem.NewFileInfo(path.MustNewComponent("path-regular"), filesystem.FileTypeRegularFile), nil)
		foo.EXPECT().Lstat(path.MustNewComponent("path-executable")).Return(filesystem.NewFileInfo(path.MustNewComponent("path-executable"), filesystem.FileTypeExecutableFile), nil)
		foo.EXPECT().Lstat(path.MustNewComponent("path-directory")).Return(filesystem.NewFileInfo(path.MustNewComponent("path-directory"), filesystem.FileTypeDirectory), nil)
		foo.EXPECT().Lstat(path.MustNewComponent("path-symlink")).Return(filesystem.NewFileInfo(path.MustNewComponent("path-symlink"), filesystem.FileTypeSymlink), nil)
		foo.EXPECT().Lstat(path.MustNewComponent("path-enoent")).Return(filesystem.FileInfo{}, syscall.ENOENT)

		// Inspection/uploading of all non-directory outputs.
		foo.EXPECT().Readlink(path.MustNewComponent("directory-symlink")).Return("directory-symlink-target", nil)
		foo.EXPECT().UploadFile(ctx, path.MustNewComponent("file-regular"), gomock.Any()).
			Return(digest.MustNewDigest("example", "a58c2f2281011ca2e631b39baa1ab657", 12), nil)
		foo.EXPECT().UploadFile(ctx, path.MustNewComponent("file-executable"), gomock.Any()).
			Return(digest.MustNewDigest("example", "7590e1b46240ecb5ea65a80db7ee6fae", 15), nil)
		foo.EXPECT().Readlink(path.MustNewComponent("file-symlink")).Return("file-symlink-target", nil)
		foo.EXPECT().UploadFile(ctx, path.MustNewComponent("path-regular"), gomock.Any()).
			Return(digest.MustNewDigest("example", "44206648b7bb2f3b0d2ed0c52ad2e269", 12), nil)
		foo.EXPECT().UploadFile(ctx, path.MustNewComponent("path-executable"), gomock.Any()).
			Return(digest.MustNewDigest("example", "87729325cd08d300fb0e238a3a8da443", 15), nil)
		foo.EXPECT().Readlink(path.MustNewComponent("path-symlink")).Return("path-symlink-target", nil)

		// Uploading of /foo/directory-directory. Files with an
		// unknown type (UNIX sockets, FIFOs) should be ignored.
		// Returning a hard error makes debugging harder (e.g.,
		// in case the full input root is declared as an output).
		directoryDirectory := mock.NewMockUploadableDirectory(ctrl)
		foo.EXPECT().EnterUploadableDirectory(path.MustNewComponent("directory-directory")).Return(directoryDirectory, nil)
		directoryDirectory.EXPECT().ReadDir().Return([]filesystem.FileInfo{
			filesystem.NewFileInfo(path.MustNewComponent("directory"), filesystem.FileTypeDirectory),
			filesystem.NewFileInfo(path.MustNewComponent("executable"), filesystem.FileTypeExecutableFile),
			filesystem.NewFileInfo(path.MustNewComponent("other"), filesystem.FileTypeOther),
			filesystem.NewFileInfo(path.MustNewComponent("regular"), filesystem.FileTypeRegularFile),
			filesystem.NewFileInfo(path.MustNewComponent("symlink"), filesystem.FileTypeSymlink),
		}, nil)
		directoryDirectoryDirectory := mock.NewMockUploadableDirectory(ctrl)
		directoryDirectory.EXPECT().EnterUploadableDirectory(path.MustNewComponent("directory")).Return(directoryDirectoryDirectory, nil)
		directoryDirectoryDirectory.EXPECT().ReadDir().Return(nil, nil)
		directoryDirectoryDirectory.EXPECT().Close()
		directoryDirectory.EXPECT().UploadFile(ctx, path.MustNewComponent("executable"), gomock.Any()).
			Return(digest.MustNewDigest("example", "ee7004c7949d83f130592f15d98ca343", 10), nil)
		directoryDirectory.EXPECT().UploadFile(ctx, path.MustNewComponent("regular"), gomock.Any()).
			Return(digest.MustNewDigest("example", "af37d08ae228a87dc6b265fd1019c97d", 7), nil)
		directoryDirectory.EXPECT().Readlink(path.MustNewComponent("symlink")).Return("symlink-target", nil)
		directoryDirectory.EXPECT().Close()
		contentAddressableStorage.EXPECT().Put(
			ctx,
			digest.MustNewDigest("example", "55aed4acf40a28132fb2d2de2b5962f0", 184),
			gomock.Any()).
			DoAndReturn(func(ctx context.Context, digest digest.Digest, b buffer.Buffer) error {
				m, err := b.ToProto(&remoteexecution.Tree{}, 10000)
				require.NoError(t, err)
				testutil.RequireEqualProto(t, &remoteexecution.Tree{
					Root: &remoteexecution.Directory{
						Files: []*remoteexecution.FileNode{
							{
								Name: "executable",
								Digest: &remoteexecution.Digest{
									Hash:      "ee7004c7949d83f130592f15d98ca343",
									SizeBytes: 10,
								},
								IsExecutable: true,
							},
							{
								Name: "regular",
								Digest: &remoteexecution.Digest{
									Hash:      "af37d08ae228a87dc6b265fd1019c97d",
									SizeBytes: 7,
								},
							},
						},
						Directories: []*remoteexecution.DirectoryNode{
							{
								Name: "directory",
								Digest: &remoteexecution.Digest{
									Hash:      "d41d8cd98f00b204e9800998ecf8427e",
									SizeBytes: 0,
								},
							},
						},
						Symlinks: []*remoteexecution.SymlinkNode{
							{
								Name:   "symlink",
								Target: "symlink-target",
							},
						},
					},
					Children: []*remoteexecution.Directory{
						{},
					},
				}, m)
				return nil
			})

		// Uploading of /foo/path-directory.
		pathDirectory := mock.NewMockUploadableDirectory(ctrl)
		foo.EXPECT().EnterUploadableDirectory(path.MustNewComponent("path-directory")).Return(pathDirectory, nil)
		pathDirectory.EXPECT().ReadDir().Return(nil, nil)
		pathDirectory.EXPECT().Close()
		contentAddressableStorage.EXPECT().Put(
			ctx,
			digest.MustNewDigest("example", "9dd94c5a4b02914af42e8e6372e0b709", 2),
			gomock.Any()).
			DoAndReturn(func(ctx context.Context, digest digest.Digest, b buffer.Buffer) error {
				m, err := b.ToProto(&remoteexecution.Tree{}, 10000)
				require.NoError(t, err)
				testutil.RequireEqualProto(t, &remoteexecution.Tree{
					Root: &remoteexecution.Directory{},
				}, m)
				return nil
			})

		foo.EXPECT().Close()

		oh, err := builder.NewOutputHierarchy(&remoteexecution.Command{
			WorkingDirectory: "foo",
			OutputDirectories: []string{
				"directory-directory",
				"../foo/directory-directory",
				"directory-symlink",
				"../foo/directory-symlink",
				"directory-enoent",
				"../foo/directory-enoent",
			},
			OutputFiles: []string{
				"file-regular",
				"../foo/file-regular",
				"file-executable",
				"../foo/file-executable",
				"file-symlink",
				"../foo/file-symlink",
				"file-enoent",
				"../foo/file-enoent",
			},
			OutputPaths: []string{
				"path-regular",
				"../foo/path-regular",
				"path-executable",
				"../foo/path-executable",
				"path-directory",
				"../foo/path-directory",
				"path-symlink",
				"../foo/path-symlink",
				"path-enoent",
				"../foo/path-enoent",
			},
		})
		require.NoError(t, err)
		var actionResult remoteexecution.ActionResult
		require.NoError(t, oh.UploadOutputs(ctx, root, contentAddressableStorage, digestFunction, &actionResult))
		require.Equal(t, remoteexecution.ActionResult{
			OutputDirectories: []*remoteexecution.OutputDirectory{
				{
					Path: "directory-directory",
					TreeDigest: &remoteexecution.Digest{
						Hash:      "55aed4acf40a28132fb2d2de2b5962f0",
						SizeBytes: 184,
					},
				},
				{
					Path: "../foo/directory-directory",
					TreeDigest: &remoteexecution.Digest{
						Hash:      "55aed4acf40a28132fb2d2de2b5962f0",
						SizeBytes: 184,
					},
				},
				{
					Path: "path-directory",
					TreeDigest: &remoteexecution.Digest{
						Hash:      "9dd94c5a4b02914af42e8e6372e0b709",
						SizeBytes: 2,
					},
				},
				{
					Path: "../foo/path-directory",
					TreeDigest: &remoteexecution.Digest{
						Hash:      "9dd94c5a4b02914af42e8e6372e0b709",
						SizeBytes: 2,
					},
				},
			},
			OutputDirectorySymlinks: []*remoteexecution.OutputSymlink{
				{
					Path:   "directory-symlink",
					Target: "directory-symlink-target",
				},
				{
					Path:   "../foo/directory-symlink",
					Target: "directory-symlink-target",
				},
			},
			OutputFiles: []*remoteexecution.OutputFile{
				{
					Path: "file-executable",
					Digest: &remoteexecution.Digest{
						Hash:      "7590e1b46240ecb5ea65a80db7ee6fae",
						SizeBytes: 15,
					},
					IsExecutable: true,
				},
				{
					Path: "../foo/file-executable",
					Digest: &remoteexecution.Digest{
						Hash:      "7590e1b46240ecb5ea65a80db7ee6fae",
						SizeBytes: 15,
					},
					IsExecutable: true,
				},
				{
					Path: "file-regular",
					Digest: &remoteexecution.Digest{
						Hash:      "a58c2f2281011ca2e631b39baa1ab657",
						SizeBytes: 12,
					},
				},
				{
					Path: "../foo/file-regular",
					Digest: &remoteexecution.Digest{
						Hash:      "a58c2f2281011ca2e631b39baa1ab657",
						SizeBytes: 12,
					},
				},
				{
					Path: "path-executable",
					Digest: &remoteexecution.Digest{
						Hash:      "87729325cd08d300fb0e238a3a8da443",
						SizeBytes: 15,
					},
					IsExecutable: true,
				},
				{
					Path: "../foo/path-executable",
					Digest: &remoteexecution.Digest{
						Hash:      "87729325cd08d300fb0e238a3a8da443",
						SizeBytes: 15,
					},
					IsExecutable: true,
				},
				{
					Path: "path-regular",
					Digest: &remoteexecution.Digest{
						Hash:      "44206648b7bb2f3b0d2ed0c52ad2e269",
						SizeBytes: 12,
					},
				},
				{
					Path: "../foo/path-regular",
					Digest: &remoteexecution.Digest{
						Hash:      "44206648b7bb2f3b0d2ed0c52ad2e269",
						SizeBytes: 12,
					},
				},
			},
			OutputFileSymlinks: []*remoteexecution.OutputSymlink{
				{
					Path:   "file-symlink",
					Target: "file-symlink-target",
				},
				{
					Path:   "../foo/file-symlink",
					Target: "file-symlink-target",
				},
			},
			OutputSymlinks: []*remoteexecution.OutputSymlink{
				{
					Path:   "path-symlink",
					Target: "path-symlink-target",
				},
				{
					Path:   "../foo/path-symlink",
					Target: "path-symlink-target",
				},
			},
		}, actionResult)
	})

	t.Run("RootDirectory", func(t *testing.T) {
		// Special case: it is also permitted to add the root
		// directory as an REv2.0 output directory. This
		// shouldn't cause any Lstat() calls, as the root
		// directory always exists. It is also impossible to
		// call Lstat() on it, as that would require us to
		// traverse upwards.
		root.EXPECT().ReadDir().Return(nil, nil)
		contentAddressableStorage.EXPECT().Put(
			ctx,
			digest.MustNewDigest("example", "9dd94c5a4b02914af42e8e6372e0b709", 2),
			gomock.Any()).
			DoAndReturn(func(ctx context.Context, digest digest.Digest, b buffer.Buffer) error {
				m, err := b.ToProto(&remoteexecution.Tree{}, 10000)
				require.NoError(t, err)
				testutil.RequireEqualProto(t, &remoteexecution.Tree{
					Root: &remoteexecution.Directory{},
				}, m)
				return nil
			})

		oh, err := builder.NewOutputHierarchy(&remoteexecution.Command{
			WorkingDirectory:  "foo",
			OutputDirectories: []string{".."},
		})
		require.NoError(t, err)
		var actionResult remoteexecution.ActionResult
		require.NoError(t, oh.UploadOutputs(ctx, root, contentAddressableStorage, digestFunction, &actionResult))
		require.Equal(t, remoteexecution.ActionResult{
			OutputDirectories: []*remoteexecution.OutputDirectory{
				{
					Path: "..",
					TreeDigest: &remoteexecution.Digest{
						Hash:      "9dd94c5a4b02914af42e8e6372e0b709",
						SizeBytes: 2,
					},
				},
			},
		}, actionResult)
	})

	t.Run("RootPath", func(t *testing.T) {
		// Similar to the previous test, it is also permitted to
		// add the root directory as an REv2.1 output path.
		root.EXPECT().ReadDir().Return(nil, nil)
		contentAddressableStorage.EXPECT().Put(
			ctx,
			digest.MustNewDigest("example", "9dd94c5a4b02914af42e8e6372e0b709", 2),
			gomock.Any()).
			DoAndReturn(func(ctx context.Context, digest digest.Digest, b buffer.Buffer) error {
				m, err := b.ToProto(&remoteexecution.Tree{}, 10000)
				require.NoError(t, err)
				testutil.RequireEqualProto(t, &remoteexecution.Tree{
					Root: &remoteexecution.Directory{},
				}, m)
				return nil
			})

		oh, err := builder.NewOutputHierarchy(&remoteexecution.Command{
			WorkingDirectory: "foo",
			OutputPaths:      []string{".."},
		})
		require.NoError(t, err)
		var actionResult remoteexecution.ActionResult
		require.NoError(t, oh.UploadOutputs(ctx, root, contentAddressableStorage, digestFunction, &actionResult))
		require.Equal(t, remoteexecution.ActionResult{
			OutputDirectories: []*remoteexecution.OutputDirectory{
				{
					Path: "..",
					TreeDigest: &remoteexecution.Digest{
						Hash:      "9dd94c5a4b02914af42e8e6372e0b709",
						SizeBytes: 2,
					},
				},
			},
		}, actionResult)
	})

	t.Run("LstatFailureDirectory", func(t *testing.T) {
		// Failure to Lstat() an output directory should cause
		// it to be skipped.
		root.EXPECT().Lstat(path.MustNewComponent("foo")).Return(filesystem.FileInfo{}, status.Error(codes.Internal, "I/O error"))

		oh, err := builder.NewOutputHierarchy(&remoteexecution.Command{
			WorkingDirectory:  "",
			OutputDirectories: []string{"foo"},
		})
		require.NoError(t, err)
		var actionResult remoteexecution.ActionResult
		require.Equal(
			t,
			status.Error(codes.Internal, "Failed to read attributes of output directory \"foo\": I/O error"),
			oh.UploadOutputs(ctx, root, contentAddressableStorage, digestFunction, &actionResult))
		require.Equal(t, remoteexecution.ActionResult{}, actionResult)
	})

	t.Run("LstatFailureFile", func(t *testing.T) {
		// Failure to Lstat() an output file should cause it to
		// be skipped.
		root.EXPECT().Lstat(path.MustNewComponent("foo")).Return(filesystem.FileInfo{}, status.Error(codes.Internal, "I/O error"))

		oh, err := builder.NewOutputHierarchy(&remoteexecution.Command{
			WorkingDirectory: "",
			OutputFiles:      []string{"foo"},
		})
		require.NoError(t, err)
		var actionResult remoteexecution.ActionResult
		require.Equal(
			t,
			status.Error(codes.Internal, "Failed to read attributes of output file \"foo\": I/O error"),
			oh.UploadOutputs(ctx, root, contentAddressableStorage, digestFunction, &actionResult))
		require.Equal(t, remoteexecution.ActionResult{}, actionResult)
	})

	t.Run("LstatFailurePath", func(t *testing.T) {
		// Failure to Lstat() an output path should cause it to
		// be skipped.
		root.EXPECT().Lstat(path.MustNewComponent("foo")).Return(filesystem.FileInfo{}, status.Error(codes.Internal, "I/O error"))

		oh, err := builder.NewOutputHierarchy(&remoteexecution.Command{
			WorkingDirectory: "",
			OutputPaths:      []string{"foo"},
		})
		require.NoError(t, err)
		var actionResult remoteexecution.ActionResult
		require.Equal(
			t,
			status.Error(codes.Internal, "Failed to read attributes of output path \"foo\": I/O error"),
			oh.UploadOutputs(ctx, root, contentAddressableStorage, digestFunction, &actionResult))
		require.Equal(t, remoteexecution.ActionResult{}, actionResult)
	})

	// TODO: Are there other cases we'd like to unit test?
}
