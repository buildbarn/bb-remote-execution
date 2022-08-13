//go:build darwin || linux
// +build darwin linux

package fuse_test

import (
	"syscall"
	"testing"
	"time"

	"github.com/buildbarn/bb-remote-execution/internal/mock"
	"github.com/buildbarn/bb-remote-execution/pkg/filesystem/virtual"
	"github.com/buildbarn/bb-remote-execution/pkg/filesystem/virtual/fuse"
	"github.com/buildbarn/bb-storage/pkg/filesystem"
	"github.com/buildbarn/bb-storage/pkg/filesystem/path"
	"github.com/golang/mock/gomock"
	go_fuse "github.com/hanwen/go-fuse/v2/fuse"
	"github.com/stretchr/testify/require"
)

func TestSimpleRawFileSystemAccess(t *testing.T) {
	ctrl := gomock.NewController(t)

	rootDirectory := mock.NewMockVirtualDirectory(ctrl)
	removalNotifierRegistrar := mock.NewMockFUSERemovalNotifierRegistrar(ctrl)
	rfs := fuse.NewSimpleRawFileSystem(rootDirectory, removalNotifierRegistrar.Call)

	t.Run("Failure", func(t *testing.T) {
		rootDirectory.EXPECT().VirtualGetAttributes(virtual.AttributesMaskPermissions, gomock.Any()).DoAndReturn(
			func(requested virtual.AttributesMask, out *virtual.Attributes) {
				out.SetPermissions(virtual.PermissionsWrite | virtual.PermissionsExecute)
			})

		require.Equal(t, go_fuse.EACCES, rfs.Access(nil, &go_fuse.AccessIn{
			InHeader: go_fuse.InHeader{
				NodeId: go_fuse.FUSE_ROOT_ID,
			},
			Mask: go_fuse.R_OK,
		}))
	})

	t.Run("Success", func(t *testing.T) {
		rootDirectory.EXPECT().VirtualGetAttributes(virtual.AttributesMaskPermissions, gomock.Any()).DoAndReturn(
			func(requested virtual.AttributesMask, out *virtual.Attributes) {
				out.SetPermissions(virtual.PermissionsRead | virtual.PermissionsWrite | virtual.PermissionsExecute)
			})

		require.Equal(t, go_fuse.OK, rfs.Access(nil, &go_fuse.AccessIn{
			InHeader: go_fuse.InHeader{
				NodeId: go_fuse.FUSE_ROOT_ID,
			},
			Mask: go_fuse.R_OK | go_fuse.X_OK,
		}))
	})
}

func TestSimpleRawFileSystemLookup(t *testing.T) {
	ctrl := gomock.NewController(t)

	rootDirectory := mock.NewMockVirtualDirectory(ctrl)
	removalNotifierRegistrar := mock.NewMockFUSERemovalNotifierRegistrar(ctrl)
	rfs := fuse.NewSimpleRawFileSystem(rootDirectory, removalNotifierRegistrar.Call)

	t.Run("NotFound", func(t *testing.T) {
		// Lookup failure errors should be propagated.
		rootDirectory.EXPECT().VirtualLookup(path.MustNewComponent("nonexistent"), fuse.AttributesMaskForFUSEAttr, gomock.Any()).
			Return(nil, nil, virtual.StatusErrNoEnt)

		var entryOut go_fuse.EntryOut
		require.Equal(t, go_fuse.ENOENT, rfs.Lookup(nil, &go_fuse.InHeader{
			NodeId: go_fuse.FUSE_ROOT_ID,
		}, "nonexistent", &entryOut))
	})

	t.Run("Directory", func(t *testing.T) {
		// Looking up a directory should cause the attributes to
		// be returned. The inode number should be used as the
		// node ID, so that future operations can refer to it.
		childDirectory := mock.NewMockVirtualDirectory(ctrl)
		rootDirectory.EXPECT().VirtualLookup(path.MustNewComponent("directory"), fuse.AttributesMaskForFUSEAttr, gomock.Any()).DoAndReturn(
			func(name path.Component, requested virtual.AttributesMask, out *virtual.Attributes) (virtual.Directory, virtual.Leaf, virtual.Status) {
				out.SetFileType(filesystem.FileTypeDirectory)
				out.SetInodeNumber(123)
				out.SetLastDataModificationTime(time.Unix(1654790759, 405372932))
				out.SetLinkCount(5)
				out.SetPermissions(virtual.PermissionsExecute)
				out.SetSizeBytes(2048)
				return childDirectory, nil, virtual.StatusOK
			})

		var entryOut go_fuse.EntryOut
		require.Equal(t, go_fuse.OK, rfs.Lookup(nil, &go_fuse.InHeader{
			NodeId: go_fuse.FUSE_ROOT_ID,
		}, "directory", &entryOut))
		require.Equal(t, go_fuse.EntryOut{
			NodeId: 123,
			Attr: go_fuse.Attr{
				Mode:      go_fuse.S_IFDIR | 0o111,
				Ino:       123,
				Mtime:     1654790759,
				Mtimensec: 405372932,
				Nlink:     5,
				Size:      2048,
			},
		}, entryOut)

		// Performing a successive lookup against the node ID of
		// the directory should call into that directory; not
		// the root directory.
		childDirectory.EXPECT().VirtualLookup(path.MustNewComponent("nonexistent"), fuse.AttributesMaskForFUSEAttr, gomock.Any()).
			Return(nil, nil, virtual.StatusErrNoEnt)

		require.Equal(t, go_fuse.ENOENT, rfs.Lookup(nil, &go_fuse.InHeader{
			NodeId: 123,
		}, "nonexistent", &entryOut))
	})

	t.Run("File", func(t *testing.T) {
		// Looking up a file should work similarly to the above.
		childFile := mock.NewMockVirtualLeaf(ctrl)
		rootDirectory.EXPECT().VirtualLookup(path.MustNewComponent("file"), fuse.AttributesMaskForFUSEAttr, gomock.Any()).DoAndReturn(
			func(name path.Component, requested virtual.AttributesMask, out *virtual.Attributes) (virtual.Directory, virtual.Leaf, virtual.Status) {
				out.SetFileType(filesystem.FileTypeRegularFile)
				out.SetInodeNumber(456)
				out.SetLinkCount(1)
				out.SetPermissions(virtual.PermissionsRead)
				out.SetSizeBytes(1300)
				return nil, childFile, virtual.StatusOK
			})

		var entryOut go_fuse.EntryOut
		require.Equal(t, go_fuse.OK, rfs.Lookup(nil, &go_fuse.InHeader{
			NodeId: go_fuse.FUSE_ROOT_ID,
		}, "file", &entryOut))
		require.Equal(t, go_fuse.EntryOut{
			NodeId: 456,
			Attr: go_fuse.Attr{
				Mode:  go_fuse.S_IFREG | 0o444,
				Ino:   456,
				Nlink: 1,
				Size:  1300,
			},
		}, entryOut)
	})
}

func TestSimpleRawFileSystemForget(t *testing.T) {
	ctrl := gomock.NewController(t)

	rootDirectory := mock.NewMockVirtualDirectory(ctrl)
	removalNotifierRegistrar := mock.NewMockFUSERemovalNotifierRegistrar(ctrl)
	rfs := fuse.NewSimpleRawFileSystem(rootDirectory, removalNotifierRegistrar.Call)

	for i := 0; i < 10; i++ {
		// Perform ten lookups of the same directory.
		childDirectory := mock.NewMockVirtualDirectory(ctrl)
		for j := 0; j < 10; j++ {
			rootDirectory.EXPECT().VirtualLookup(path.MustNewComponent("directory"), fuse.AttributesMaskForFUSEAttr, gomock.Any()).DoAndReturn(
				func(name path.Component, requested virtual.AttributesMask, out *virtual.Attributes) (virtual.Directory, virtual.Leaf, virtual.Status) {
					out.SetFileType(filesystem.FileTypeDirectory)
					out.SetInodeNumber(123)
					out.SetLinkCount(4)
					out.SetPermissions(virtual.PermissionsExecute)
					out.SetSizeBytes(2048)
					return childDirectory, nil, virtual.StatusOK
				})

			var entryOut go_fuse.EntryOut
			require.Equal(t, go_fuse.OK, rfs.Lookup(nil, &go_fuse.InHeader{
				NodeId: go_fuse.FUSE_ROOT_ID,
			}, "directory", &entryOut))
			require.Equal(t, go_fuse.EntryOut{
				NodeId: 123,
				Attr: go_fuse.Attr{
					Mode:  go_fuse.S_IFDIR | 0o111,
					Ino:   123,
					Nlink: 4,
					Size:  2048,
				},
			}, entryOut)
		}

		// Operations against the node ID of the directory
		// should all be forwarded to the directory object.
		childDirectory.EXPECT().VirtualGetAttributes(fuse.AttributesMaskForFUSEAttr, gomock.Any()).DoAndReturn(
			func(requested virtual.AttributesMask, out *virtual.Attributes) {
				out.SetFileType(filesystem.FileTypeDirectory)
				out.SetInodeNumber(123)
				out.SetLinkCount(4)
				out.SetPermissions(virtual.PermissionsRead | virtual.PermissionsExecute)
				out.SetSizeBytes(2048)
			})

		var directoryAttrOut go_fuse.AttrOut
		require.Equal(t, go_fuse.OK, rfs.GetAttr(nil, &go_fuse.GetAttrIn{
			InHeader: go_fuse.InHeader{
				NodeId: 123,
			},
		}, &directoryAttrOut))
		require.Equal(t, go_fuse.AttrOut{
			Attr: go_fuse.Attr{
				Mode:  go_fuse.S_IFDIR | 0o555,
				Ino:   123,
				Nlink: 4,
				Size:  2048,
			},
		}, directoryAttrOut)

		// Forget the directory a total of ten times. This
		// should cause the node ID to be released, meaning that
		// it's safe to reuse the same ID for another purpose.
		rfs.Forget(123, 3)
		rfs.Forget(123, 2)
		rfs.Forget(123, 5)

		// Perform ten lookups of the same file. This file
		// reuses the node ID that was previously used by the
		// directory. This should be safe, as this node ID was
		// forgotten.
		childFile := mock.NewMockVirtualLeaf(ctrl)
		for j := 0; j < 10; j++ {
			rootDirectory.EXPECT().VirtualLookup(path.MustNewComponent("file"), fuse.AttributesMaskForFUSEAttr, gomock.Any()).DoAndReturn(
				func(name path.Component, requested virtual.AttributesMask, out *virtual.Attributes) (virtual.Directory, virtual.Leaf, virtual.Status) {
					out.SetFileType(filesystem.FileTypeRegularFile)
					out.SetInodeNumber(123)
					out.SetLinkCount(1)
					out.SetPermissions(virtual.PermissionsRead)
					out.SetSizeBytes(42)
					return nil, childFile, virtual.StatusOK
				})

			var entryOut go_fuse.EntryOut
			require.Equal(t, go_fuse.OK, rfs.Lookup(nil, &go_fuse.InHeader{
				NodeId: go_fuse.FUSE_ROOT_ID,
			}, "file", &entryOut))
			require.Equal(t, go_fuse.EntryOut{
				NodeId: 123,
				Attr: go_fuse.Attr{
					Mode:  go_fuse.S_IFREG | 0o444,
					Ino:   123,
					Nlink: 1,
					Size:  42,
				},
			}, entryOut)
		}

		// Operations against the node ID should now all go to
		// the file -- not the directory.
		childFile.EXPECT().VirtualGetAttributes(fuse.AttributesMaskForFUSEAttr, gomock.Any()).DoAndReturn(
			func(requested virtual.AttributesMask, out *virtual.Attributes) {
				out.SetFileType(filesystem.FileTypeRegularFile)
				out.SetInodeNumber(123)
				out.SetLinkCount(1)
				out.SetPermissions(virtual.PermissionsRead | virtual.PermissionsExecute)
				out.SetSizeBytes(5)
			})

		var fileAttrOut go_fuse.AttrOut
		require.Equal(t, go_fuse.OK, rfs.GetAttr(nil, &go_fuse.GetAttrIn{
			InHeader: go_fuse.InHeader{
				NodeId: 123,
			},
		}, &fileAttrOut))
		require.Equal(t, go_fuse.AttrOut{
			Attr: go_fuse.Attr{
				Mode:  go_fuse.S_IFREG | 0o555,
				Ino:   123,
				Nlink: 1,
				Size:  5,
			},
		}, fileAttrOut)

		// Also forget the file a total of ten times.
		rfs.Forget(123, 9)
		rfs.Forget(123, 1)
	}
}

func TestSimpleRawFileSystemGetAttr(t *testing.T) {
	ctrl := gomock.NewController(t)

	rootDirectory := mock.NewMockVirtualDirectory(ctrl)
	removalNotifierRegistrar := mock.NewMockFUSERemovalNotifierRegistrar(ctrl)
	rfs := fuse.NewSimpleRawFileSystem(rootDirectory, removalNotifierRegistrar.Call)

	t.Run("Success", func(t *testing.T) {
		rootDirectory.EXPECT().VirtualGetAttributes(fuse.AttributesMaskForFUSEAttr, gomock.Any()).DoAndReturn(
			func(requested virtual.AttributesMask, out *virtual.Attributes) {
				out.SetFileType(filesystem.FileTypeDirectory)
				out.SetInodeNumber(42)
				out.SetLinkCount(7)
				out.SetPermissions(virtual.PermissionsExecute)
				out.SetSizeBytes(12)
			})

		var attrOut go_fuse.AttrOut
		require.Equal(t, go_fuse.OK, rfs.GetAttr(nil, &go_fuse.GetAttrIn{
			InHeader: go_fuse.InHeader{
				NodeId: go_fuse.FUSE_ROOT_ID,
			},
		}, &attrOut))
		require.Equal(t, go_fuse.AttrOut{
			Attr: go_fuse.Attr{
				Mode:  go_fuse.S_IFDIR | 0o111,
				Ino:   42,
				Nlink: 7,
				Size:  12,
			},
		}, attrOut)
	})
}

func TestSimpleRawFileSystemSetAttr(t *testing.T) {
	ctrl := gomock.NewController(t)

	rootDirectory := mock.NewMockVirtualDirectory(ctrl)
	removalNotifierRegistrar := mock.NewMockFUSERemovalNotifierRegistrar(ctrl)
	rfs := fuse.NewSimpleRawFileSystem(rootDirectory, removalNotifierRegistrar.Call)

	t.Run("Chown", func(t *testing.T) {
		// chown() operations are not supported.
		var attrOut go_fuse.AttrOut
		require.Equal(t, go_fuse.EPERM, rfs.SetAttr(nil, &go_fuse.SetAttrIn{
			SetAttrInCommon: go_fuse.SetAttrInCommon{
				InHeader: go_fuse.InHeader{
					NodeId: go_fuse.FUSE_ROOT_ID,
				},
				Valid: go_fuse.FATTR_UID | go_fuse.FATTR_GID,
				Owner: go_fuse.Owner{
					Uid: 1000,
					Gid: 1000,
				},
			},
		}, &attrOut))
	})

	t.Run("Failure", func(t *testing.T) {
		// A truncate() call that is denied.
		rootDirectory.EXPECT().VirtualSetAttributes(
			(&virtual.Attributes{}).SetSizeBytes(400),
			fuse.AttributesMaskForFUSEAttr,
			gomock.Any(),
		).Return(virtual.StatusErrAccess)

		var attrOut go_fuse.AttrOut
		require.Equal(t, go_fuse.EACCES, rfs.SetAttr(nil, &go_fuse.SetAttrIn{
			SetAttrInCommon: go_fuse.SetAttrInCommon{
				InHeader: go_fuse.InHeader{
					NodeId: go_fuse.FUSE_ROOT_ID,
				},
				Valid: go_fuse.FATTR_SIZE,
				Size:  400,
			},
		}, &attrOut))
	})

	t.Run("Success", func(t *testing.T) {
		// A chmod() call that is permitted.
		rootDirectory.EXPECT().VirtualSetAttributes(
			(&virtual.Attributes{}).SetPermissions(virtual.PermissionsRead|virtual.PermissionsExecute),
			fuse.AttributesMaskForFUSEAttr,
			gomock.Any(),
		).DoAndReturn(func(in *virtual.Attributes, requested virtual.AttributesMask, out *virtual.Attributes) virtual.Status {
			out.SetFileType(filesystem.FileTypeDirectory)
			out.SetInodeNumber(9000)
			out.SetLinkCount(12)
			out.SetPermissions(virtual.PermissionsRead | virtual.PermissionsExecute)
			out.SetSizeBytes(42)
			return virtual.StatusOK
		})

		var attrOut go_fuse.AttrOut
		require.Equal(t, go_fuse.OK, rfs.SetAttr(nil, &go_fuse.SetAttrIn{
			SetAttrInCommon: go_fuse.SetAttrInCommon{
				InHeader: go_fuse.InHeader{
					NodeId: go_fuse.FUSE_ROOT_ID,
				},
				Valid: go_fuse.FATTR_MODE,
				Mode:  0o500,
			},
		}, &attrOut))
		require.Equal(t, go_fuse.AttrOut{
			Attr: go_fuse.Attr{
				Mode:  go_fuse.S_IFDIR | 0o555,
				Ino:   9000,
				Nlink: 12,
				Size:  42,
			},
		}, attrOut)
	})
}

func TestSimpleRawFileSystemMknod(t *testing.T) {
	ctrl := gomock.NewController(t)

	rootDirectory := mock.NewMockVirtualDirectory(ctrl)
	removalNotifierRegistrar := mock.NewMockFUSERemovalNotifierRegistrar(ctrl)
	rfs := fuse.NewSimpleRawFileSystem(rootDirectory, removalNotifierRegistrar.Call)

	t.Run("BlockDevice", func(t *testing.T) {
		// An mknod() call for a block device should be
		// rejected. Creating those would be a security issue.
		var entryOut go_fuse.EntryOut
		require.Equal(t, go_fuse.EPERM, rfs.Mknod(nil, &go_fuse.MknodIn{
			InHeader: go_fuse.InHeader{
				NodeId: go_fuse.FUSE_ROOT_ID,
			},
			Mode: syscall.S_IFBLK | 0o777,
			Rdev: 456,
		}, "hello", &entryOut))
	})

	t.Run("Failure", func(t *testing.T) {
		// An mknod() call for a socket that is denied.
		rootDirectory.EXPECT().VirtualMknod(path.MustNewComponent("hello"), filesystem.FileTypeSocket, fuse.AttributesMaskForFUSEAttr, gomock.Any()).
			Return(nil, virtual.ChangeInfo{}, virtual.StatusErrPerm)

		var entryOut go_fuse.EntryOut
		require.Equal(t, go_fuse.EPERM, rfs.Mknod(nil, &go_fuse.MknodIn{
			InHeader: go_fuse.InHeader{
				NodeId: go_fuse.FUSE_ROOT_ID,
			},
			Mode: syscall.S_IFSOCK | 0o777,
			Rdev: 456,
		}, "hello", &entryOut))
	})

	t.Run("Success", func(t *testing.T) {
		// An mknod() call for a FIFO that succeeds.
		childLeaf := mock.NewMockVirtualLeaf(ctrl)
		rootDirectory.EXPECT().VirtualMknod(path.MustNewComponent("hello"), filesystem.FileTypeFIFO, fuse.AttributesMaskForFUSEAttr, gomock.Any()).DoAndReturn(
			func(name path.Component, fileType filesystem.FileType, requested virtual.AttributesMask, out *virtual.Attributes) (virtual.Leaf, virtual.ChangeInfo, virtual.Status) {
				out.SetFileType(filesystem.FileTypeFIFO)
				out.SetInodeNumber(123)
				out.SetLinkCount(1)
				out.SetPermissions(virtual.PermissionsRead | virtual.PermissionsWrite)
				out.SetSizeBytes(100)
				return childLeaf, virtual.ChangeInfo{
					Before: 41,
					After:  42,
				}, virtual.StatusOK
			})

		var entryOut go_fuse.EntryOut
		require.Equal(t, go_fuse.OK, rfs.Mknod(nil, &go_fuse.MknodIn{
			InHeader: go_fuse.InHeader{
				NodeId: go_fuse.FUSE_ROOT_ID,
			},
			Mode: go_fuse.S_IFIFO | 0o700,
		}, "hello", &entryOut))
		require.Equal(t, go_fuse.EntryOut{
			NodeId: 123,
			Attr: go_fuse.Attr{
				Mode:  go_fuse.S_IFIFO | 0o666,
				Ino:   123,
				Nlink: 1,
				Size:  100,
			},
		}, entryOut)
	})
}

func TestSimpleRawFileSystemMkdir(t *testing.T) {
	ctrl := gomock.NewController(t)

	rootDirectory := mock.NewMockVirtualDirectory(ctrl)
	removalNotifierRegistrar := mock.NewMockFUSERemovalNotifierRegistrar(ctrl)
	rfs := fuse.NewSimpleRawFileSystem(rootDirectory, removalNotifierRegistrar.Call)

	t.Run("Failure", func(t *testing.T) {
		// An mkdir() call that fails due to an I/O error.
		rootDirectory.EXPECT().VirtualMkdir(path.MustNewComponent("hello"), fuse.AttributesMaskForFUSEAttr, gomock.Any()).
			Return(nil, virtual.ChangeInfo{}, virtual.StatusErrIO)

		var entryOut go_fuse.EntryOut
		require.Equal(t, go_fuse.EIO, rfs.Mkdir(nil, &go_fuse.MkdirIn{
			InHeader: go_fuse.InHeader{
				NodeId: go_fuse.FUSE_ROOT_ID,
			},
			Mode: 0o777,
		}, "hello", &entryOut))
	})

	t.Run("Success", func(t *testing.T) {
		// An mkdir() call that succeeds.
		childDirectory := mock.NewMockVirtualDirectory(ctrl)
		rootDirectory.EXPECT().VirtualMkdir(path.MustNewComponent("hello"), fuse.AttributesMaskForFUSEAttr, gomock.Any()).DoAndReturn(
			func(name path.Component, requested virtual.AttributesMask, out *virtual.Attributes) (virtual.Directory, virtual.ChangeInfo, virtual.Status) {
				out.SetFileType(filesystem.FileTypeDirectory)
				out.SetInodeNumber(123)
				out.SetLinkCount(12)
				out.SetPermissions(virtual.PermissionsRead | virtual.PermissionsWrite | virtual.PermissionsExecute)
				out.SetSizeBytes(500)
				return childDirectory, virtual.ChangeInfo{
					Before: 13,
					After:  14,
				}, virtual.StatusOK
			})

		var entryOut go_fuse.EntryOut
		require.Equal(t, go_fuse.OK, rfs.Mkdir(nil, &go_fuse.MkdirIn{
			InHeader: go_fuse.InHeader{
				NodeId: go_fuse.FUSE_ROOT_ID,
			},
			Mode: 0o777,
		}, "hello", &entryOut))
		require.Equal(t, go_fuse.EntryOut{
			NodeId: 123,
			Attr: go_fuse.Attr{
				Mode:  go_fuse.S_IFDIR | 0o777,
				Ino:   123,
				Nlink: 12,
				Size:  500,
			},
		}, entryOut)
	})
}

func TestSimpleRawFileSystemUnlink(t *testing.T) {
	ctrl := gomock.NewController(t)

	rootDirectory := mock.NewMockVirtualDirectory(ctrl)
	removalNotifierRegistrar := mock.NewMockFUSERemovalNotifierRegistrar(ctrl)
	rfs := fuse.NewSimpleRawFileSystem(rootDirectory, removalNotifierRegistrar.Call)

	t.Run("Failure", func(t *testing.T) {
		// An unlink() call that fails due to an I/O error.
		rootDirectory.EXPECT().VirtualRemove(path.MustNewComponent("hello"), false, true).
			Return(virtual.ChangeInfo{}, virtual.StatusErrIO)

		require.Equal(t, go_fuse.EIO, rfs.Unlink(nil, &go_fuse.InHeader{
			NodeId: go_fuse.FUSE_ROOT_ID,
		}, "hello"))
	})

	t.Run("Success", func(t *testing.T) {
		// An unlink() call that succeeds.
		rootDirectory.EXPECT().VirtualRemove(path.MustNewComponent("hello"), false, true).
			Return(virtual.ChangeInfo{
				Before: 5,
				After:  6,
			}, virtual.StatusOK)

		require.Equal(t, go_fuse.OK, rfs.Unlink(nil, &go_fuse.InHeader{
			NodeId: go_fuse.FUSE_ROOT_ID,
		}, "hello"))
	})
}

func TestSimpleRawFileSystemRmdir(t *testing.T) {
	ctrl := gomock.NewController(t)

	rootDirectory := mock.NewMockVirtualDirectory(ctrl)
	removalNotifierRegistrar := mock.NewMockFUSERemovalNotifierRegistrar(ctrl)
	rfs := fuse.NewSimpleRawFileSystem(rootDirectory, removalNotifierRegistrar.Call)

	t.Run("Failure", func(t *testing.T) {
		// An rmdir() call that fails due to an I/O error.
		rootDirectory.EXPECT().VirtualRemove(path.MustNewComponent("hello"), true, false).
			Return(virtual.ChangeInfo{}, virtual.StatusErrIO)

		require.Equal(t, go_fuse.EIO, rfs.Rmdir(nil, &go_fuse.InHeader{
			NodeId: go_fuse.FUSE_ROOT_ID,
		}, "hello"))
	})

	t.Run("Success", func(t *testing.T) {
		// An rmdir() call that succeeds.
		rootDirectory.EXPECT().VirtualRemove(path.MustNewComponent("hello"), true, false).
			Return(virtual.ChangeInfo{
				Before: 5,
				After:  6,
			}, virtual.StatusOK)

		require.Equal(t, go_fuse.OK, rfs.Rmdir(nil, &go_fuse.InHeader{
			NodeId: go_fuse.FUSE_ROOT_ID,
		}, "hello"))
	})
}

func TestSimpleRawFileSystemSymlink(t *testing.T) {
	ctrl := gomock.NewController(t)

	rootDirectory := mock.NewMockVirtualDirectory(ctrl)
	removalNotifierRegistrar := mock.NewMockFUSERemovalNotifierRegistrar(ctrl)
	rfs := fuse.NewSimpleRawFileSystem(rootDirectory, removalNotifierRegistrar.Call)

	t.Run("Failure", func(t *testing.T) {
		rootDirectory.EXPECT().VirtualSymlink(
			[]byte("target"),
			path.MustNewComponent("symlink"),
			fuse.AttributesMaskForFUSEAttr,
			gomock.Any(),
		).Return(nil, virtual.ChangeInfo{}, virtual.StatusErrExist)

		var entryOut go_fuse.EntryOut
		require.Equal(t, go_fuse.Status(syscall.EEXIST), rfs.Symlink(nil, &go_fuse.InHeader{
			NodeId: go_fuse.FUSE_ROOT_ID,
		}, "target", "symlink", &entryOut))
	})

	t.Run("Success", func(t *testing.T) {
		// Create a symbolic link.
		symlink := mock.NewMockVirtualLeaf(ctrl)
		rootDirectory.EXPECT().VirtualSymlink(
			[]byte("target"),
			path.MustNewComponent("symlink"),
			fuse.AttributesMaskForFUSEAttr,
			gomock.Any(),
		).DoAndReturn(func(pointedTo []byte, linkName path.Component, requested virtual.AttributesMask, out *virtual.Attributes) (virtual.Leaf, virtual.ChangeInfo, virtual.Status) {
			out.SetFileType(filesystem.FileTypeSymlink)
			out.SetInodeNumber(123)
			out.SetLinkCount(1)
			out.SetPermissions(virtual.PermissionsRead | virtual.PermissionsWrite | virtual.PermissionsExecute)
			out.SetSizeBytes(6)
			return symlink, virtual.ChangeInfo{
				Before: 12,
				After:  13,
			}, virtual.StatusOK
		})

		var entryOut go_fuse.EntryOut
		require.Equal(t, go_fuse.OK, rfs.Symlink(nil, &go_fuse.InHeader{
			NodeId: go_fuse.FUSE_ROOT_ID,
		}, "target", "symlink", &entryOut))
		require.Equal(t, go_fuse.EntryOut{
			NodeId: 123,
			Attr: go_fuse.Attr{
				Mode:  go_fuse.S_IFLNK | 0o777,
				Ino:   123,
				Nlink: 1,
				Size:  6,
			},
		}, entryOut)

		// Future calls of the node should be forwarded to the
		// right symlink instance.
		symlink.EXPECT().VirtualReadlink().Return([]byte("target"), virtual.StatusOK)

		target, s := rfs.Readlink(nil, &go_fuse.InHeader{NodeId: 123})
		require.Equal(t, go_fuse.OK, s)
		require.Equal(t, []byte("target"), target)
	})
}

func TestSimpleRawFileSystemCreate(t *testing.T) {
	ctrl := gomock.NewController(t)

	rootDirectory := mock.NewMockVirtualDirectory(ctrl)
	removalNotifierRegistrar := mock.NewMockFUSERemovalNotifierRegistrar(ctrl)
	rfs := fuse.NewSimpleRawFileSystem(rootDirectory, removalNotifierRegistrar.Call)

	t.Run("ReadWriteCreateExcl", func(t *testing.T) {
		rootDirectory.EXPECT().VirtualOpenChild(
			path.MustNewComponent("excl"),
			virtual.ShareMaskRead|virtual.ShareMaskWrite,
			(&virtual.Attributes{}).SetPermissions(virtual.PermissionsRead|virtual.PermissionsWrite|virtual.PermissionsExecute),
			nil,
			fuse.AttributesMaskForFUSEAttr,
			gomock.Any(),
		).Return(nil, virtual.AttributesMask(0), virtual.ChangeInfo{}, virtual.StatusErrExist)

		var createOut go_fuse.CreateOut
		require.Equal(t, go_fuse.Status(syscall.EEXIST), rfs.Create(nil, &go_fuse.CreateIn{
			InHeader: go_fuse.InHeader{
				NodeId: go_fuse.FUSE_ROOT_ID,
			},
			Flags: syscall.O_CREAT | syscall.O_EXCL | syscall.O_RDWR,
			Mode:  0o777,
		}, "excl", &createOut))
	})

	t.Run("WriteTruncate", func(t *testing.T) {
		rootDirectory.EXPECT().VirtualOpenChild(
			path.MustNewComponent("trunc"),
			virtual.ShareMaskWrite,
			(&virtual.Attributes{}).SetPermissions(virtual.PermissionsRead|virtual.PermissionsWrite),
			&virtual.OpenExistingOptions{Truncate: true},
			fuse.AttributesMaskForFUSEAttr,
			gomock.Any(),
		).Return(nil, virtual.AttributesMask(0), virtual.ChangeInfo{}, virtual.StatusErrNoEnt)

		var createOut go_fuse.CreateOut
		require.Equal(t, go_fuse.ENOENT, rfs.Create(nil, &go_fuse.CreateIn{
			InHeader: go_fuse.InHeader{
				NodeId: go_fuse.FUSE_ROOT_ID,
			},
			Flags: syscall.O_CREAT | syscall.O_TRUNC | syscall.O_WRONLY,
			Mode:  0o666,
		}, "trunc", &createOut))
	})
}

func TestSimpleRawFileSystemOpenDir(t *testing.T) {
	ctrl := gomock.NewController(t)

	rootDirectory := mock.NewMockVirtualDirectory(ctrl)
	removalNotifierRegistrar := mock.NewMockFUSERemovalNotifierRegistrar(ctrl)
	rfs := fuse.NewSimpleRawFileSystem(rootDirectory, removalNotifierRegistrar.Call)

	t.Run("PermissionDenied", func(t *testing.T) {
		// FUSE on Linux doesn't check permissions on the
		// directory prior to opening. Do that on the caller's
		// behalf.
		rootDirectory.EXPECT().VirtualGetAttributes(virtual.AttributesMaskPermissions, gomock.Any()).DoAndReturn(
			func(requested virtual.AttributesMask, out *virtual.Attributes) {
				out.SetPermissions(virtual.PermissionsExecute)
			})

		require.Equal(t, go_fuse.EACCES, rfs.OpenDir(nil, &go_fuse.OpenIn{
			InHeader: go_fuse.InHeader{
				NodeId: go_fuse.FUSE_ROOT_ID,
			},
		}, &go_fuse.OpenOut{}))
	})

	// Further testing coverage is provided as part of ReadDir() and
	// ReadDirPlus() tests.
}

func TestSimpleRawFileSystemReadDir(t *testing.T) {
	ctrl := gomock.NewController(t)

	rootDirectory := mock.NewMockVirtualDirectory(ctrl)
	removalNotifierRegistrar := mock.NewMockFUSERemovalNotifierRegistrar(ctrl)
	rfs := fuse.NewSimpleRawFileSystem(rootDirectory, removalNotifierRegistrar.Call)

	// Open the root directory.
	rootDirectory.EXPECT().VirtualGetAttributes(virtual.AttributesMaskPermissions, gomock.Any()).DoAndReturn(
		func(requested virtual.AttributesMask, out *virtual.Attributes) {
			out.SetPermissions(virtual.PermissionsRead)
		})

	var openOut go_fuse.OpenOut
	require.Equal(t, go_fuse.OK, rfs.OpenDir(nil, &go_fuse.OpenIn{
		InHeader: go_fuse.InHeader{
			NodeId: go_fuse.FUSE_ROOT_ID,
		},
	}, &openOut))
	require.Equal(t, go_fuse.OpenOut{}, openOut)

	t.Run("Failure", func(t *testing.T) {
		// Directory listing failures should be propagated.
		rootDirectory.EXPECT().VirtualReadDir(
			uint64(0),
			fuse.AttributesMaskForFUSEDirEntry,
			gomock.Any(),
		).Return(virtual.StatusErrIO)
		entryList := mock.NewMockReadDirEntryList(ctrl)
		gomock.InOrder(
			entryList.EXPECT().AddDirEntry(go_fuse.DirEntry{
				Name: ".",
				Mode: go_fuse.S_IFDIR,
			}, uint64(1)).Return(true),
			entryList.EXPECT().AddDirEntry(go_fuse.DirEntry{
				Name: "..",
				Mode: go_fuse.S_IFDIR,
			}, uint64(2)).Return(true),
		)

		require.Equal(t, go_fuse.EIO, rfs.ReadDir(nil, &go_fuse.ReadIn{
			InHeader: go_fuse.InHeader{
				NodeId: go_fuse.FUSE_ROOT_ID,
			},
		}, entryList))
	})

	t.Run("FromStart", func(t *testing.T) {
		// "." and ".." entries should be prepended. This should
		// cause all cookies of child entries to be incremented
		// by two to make space.
		rootDirectory.EXPECT().VirtualReadDir(
			uint64(0),
			fuse.AttributesMaskForFUSEDirEntry,
			gomock.Any(),
		).DoAndReturn(func(firstCookie uint64, requested virtual.AttributesMask, reporter virtual.DirectoryEntryReporter) virtual.Status {
			require.True(t, reporter.ReportDirectory(
				uint64(1),
				path.MustNewComponent("directory"),
				mock.NewMockVirtualDirectory(ctrl),
				(&virtual.Attributes{}).
					SetFileType(filesystem.FileTypeDirectory).
					SetInodeNumber(27)))
			require.True(t, reporter.ReportLeaf(
				uint64(2),
				path.MustNewComponent("file"),
				mock.NewMockVirtualLeaf(ctrl),
				(&virtual.Attributes{}).
					SetFileType(filesystem.FileTypeRegularFile).
					SetInodeNumber(42)))
			require.True(t, reporter.ReportLeaf(
				uint64(3),
				path.MustNewComponent("symlink"),
				mock.NewMockVirtualLeaf(ctrl),
				(&virtual.Attributes{}).
					SetFileType(filesystem.FileTypeSymlink).
					SetInodeNumber(83)))
			return virtual.StatusOK
		})
		entryList := mock.NewMockReadDirEntryList(ctrl)
		gomock.InOrder(
			entryList.EXPECT().AddDirEntry(go_fuse.DirEntry{
				Name: ".",
				Mode: go_fuse.S_IFDIR,
			}, uint64(1)).Return(true),
			entryList.EXPECT().AddDirEntry(go_fuse.DirEntry{
				Name: "..",
				Mode: go_fuse.S_IFDIR,
			}, uint64(2)).Return(true),
			entryList.EXPECT().AddDirEntry(go_fuse.DirEntry{
				Name: "directory",
				Mode: go_fuse.S_IFDIR,
				Ino:  27,
			}, uint64(3)).Return(true),
			entryList.EXPECT().AddDirEntry(go_fuse.DirEntry{
				Name: "file",
				Mode: go_fuse.S_IFREG,
				Ino:  42,
			}, uint64(4)).Return(true),
			entryList.EXPECT().AddDirEntry(go_fuse.DirEntry{
				Name: "symlink",
				Mode: go_fuse.S_IFLNK,
				Ino:  83,
			}, uint64(5)).Return(true),
		)

		require.Equal(t, go_fuse.OK, rfs.ReadDir(nil, &go_fuse.ReadIn{
			InHeader: go_fuse.InHeader{
				NodeId: go_fuse.FUSE_ROOT_ID,
			},
		}, entryList))
	})

	t.Run("BetweenDots", func(t *testing.T) {
		// Perform a partial read, starting between the "." and
		// ".." directory entries.
		rootDirectory.EXPECT().VirtualReadDir(
			uint64(0),
			fuse.AttributesMaskForFUSEDirEntry,
			gomock.Any(),
		).DoAndReturn(func(firstCookie uint64, requested virtual.AttributesMask, reporter virtual.DirectoryEntryReporter) virtual.Status {
			require.True(t, reporter.ReportDirectory(
				uint64(1),
				path.MustNewComponent("directory"),
				mock.NewMockVirtualDirectory(ctrl),
				(&virtual.Attributes{}).
					SetFileType(filesystem.FileTypeDirectory).
					SetInodeNumber(27)))
			return virtual.StatusOK
		})
		entryList := mock.NewMockReadDirEntryList(ctrl)
		gomock.InOrder(
			entryList.EXPECT().AddDirEntry(go_fuse.DirEntry{
				Name: "..",
				Mode: go_fuse.S_IFDIR,
			}, uint64(2)).Return(true),
			entryList.EXPECT().AddDirEntry(go_fuse.DirEntry{
				Name: "directory",
				Mode: go_fuse.S_IFDIR,
				Ino:  27,
			}, uint64(3)).Return(true),
		)

		require.Equal(t, go_fuse.OK, rfs.ReadDir(nil, &go_fuse.ReadIn{
			InHeader: go_fuse.InHeader{
				NodeId: go_fuse.FUSE_ROOT_ID,
			},
			Offset: 1,
		}, entryList))
	})

	t.Run("PastDots", func(t *testing.T) {
		// Perform a partial read, starting right after the "."
		// and ".." directory entries.
		rootDirectory.EXPECT().VirtualReadDir(
			uint64(0),
			fuse.AttributesMaskForFUSEDirEntry,
			gomock.Any(),
		).DoAndReturn(func(firstCookie uint64, requested virtual.AttributesMask, reporter virtual.DirectoryEntryReporter) virtual.Status {
			require.True(t, reporter.ReportDirectory(
				uint64(1),
				path.MustNewComponent("directory"),
				mock.NewMockVirtualDirectory(ctrl),
				(&virtual.Attributes{}).
					SetFileType(filesystem.FileTypeDirectory).
					SetInodeNumber(27)))
			return virtual.StatusOK
		})
		entryList := mock.NewMockReadDirEntryList(ctrl)
		gomock.InOrder(
			entryList.EXPECT().AddDirEntry(go_fuse.DirEntry{
				Name: "directory",
				Mode: go_fuse.S_IFDIR,
				Ino:  27,
			}, uint64(3)).Return(true),
		)

		require.Equal(t, go_fuse.OK, rfs.ReadDir(nil, &go_fuse.ReadIn{
			InHeader: go_fuse.InHeader{
				NodeId: go_fuse.FUSE_ROOT_ID,
			},
			Offset: 2,
		}, entryList))
	})

	t.Run("AtOffset", func(t *testing.T) {
		// Perform a partial read at an arbitrary offset.
		rootDirectory.EXPECT().VirtualReadDir(
			uint64(52),
			fuse.AttributesMaskForFUSEDirEntry,
			gomock.Any(),
		).DoAndReturn(func(firstCookie uint64, requested virtual.AttributesMask, reporter virtual.DirectoryEntryReporter) virtual.Status {
			require.False(t, reporter.ReportDirectory(
				uint64(55),
				path.MustNewComponent("directory"),
				mock.NewMockVirtualDirectory(ctrl),
				(&virtual.Attributes{}).
					SetFileType(filesystem.FileTypeDirectory).
					SetInodeNumber(27)))
			return virtual.StatusOK
		})
		entryList := mock.NewMockReadDirEntryList(ctrl)
		gomock.InOrder(
			entryList.EXPECT().AddDirEntry(go_fuse.DirEntry{
				Name: "directory",
				Mode: go_fuse.S_IFDIR,
				Ino:  27,
			}, uint64(57)),
		)

		require.Equal(t, go_fuse.OK, rfs.ReadDir(nil, &go_fuse.ReadIn{
			InHeader: go_fuse.InHeader{
				NodeId: go_fuse.FUSE_ROOT_ID,
			},
			Offset: 54,
		}, entryList))
	})

	// Close the root directory.
	rfs.ReleaseDir(&go_fuse.ReleaseIn{
		InHeader: go_fuse.InHeader{
			NodeId: go_fuse.FUSE_ROOT_ID,
		},
	})
}

func TestSimpleRawFileSystemReadDirPlus(t *testing.T) {
	ctrl := gomock.NewController(t)

	rootDirectory := mock.NewMockVirtualDirectory(ctrl)
	removalNotifierRegistrar := mock.NewMockFUSERemovalNotifierRegistrar(ctrl)
	rfs := fuse.NewSimpleRawFileSystem(rootDirectory, removalNotifierRegistrar.Call)

	// Open the root directory.
	rootDirectory.EXPECT().VirtualGetAttributes(virtual.AttributesMaskPermissions, gomock.Any()).DoAndReturn(
		func(requested virtual.AttributesMask, out *virtual.Attributes) {
			out.SetPermissions(virtual.PermissionsRead)
		})

	var openOut go_fuse.OpenOut
	require.Equal(t, go_fuse.OK, rfs.OpenDir(nil, &go_fuse.OpenIn{
		InHeader: go_fuse.InHeader{
			NodeId: go_fuse.FUSE_ROOT_ID,
		},
	}, &openOut))
	require.Equal(t, go_fuse.OpenOut{}, openOut)

	t.Run("Failure", func(t *testing.T) {
		// Directory listing failures should be propagated.
		rootDirectory.EXPECT().VirtualReadDir(
			uint64(0),
			fuse.AttributesMaskForFUSEAttr,
			gomock.Any(),
		).Return(virtual.StatusErrIO)
		entryList := mock.NewMockReadDirPlusEntryList(ctrl)
		var entryOutDot go_fuse.EntryOut
		gomock.InOrder(
			entryList.EXPECT().AddDirLookupEntry(go_fuse.DirEntry{
				Name: ".",
				Mode: go_fuse.S_IFDIR,
			}, uint64(1)).Return(&entryOutDot),
			entryList.EXPECT().AddDirLookupEntry(go_fuse.DirEntry{
				Name: "..",
				Mode: go_fuse.S_IFDIR,
			}, uint64(2)).Return(&entryOutDot),
		)

		require.Equal(t, go_fuse.EIO, rfs.ReadDirPlus(nil, &go_fuse.ReadIn{
			InHeader: go_fuse.InHeader{
				NodeId: go_fuse.FUSE_ROOT_ID,
			},
		}, entryList))
		require.Equal(t, go_fuse.EntryOut{}, entryOutDot)
	})

	t.Run("FromStart", func(t *testing.T) {
		// "." and ".." entries should be prepended. This should
		// cause all cookies of child entries to be incremented
		// by two to make space.
		childDirectory := mock.NewMockVirtualDirectory(ctrl)
		childLeaf := mock.NewMockVirtualLeaf(ctrl)
		rootDirectory.EXPECT().VirtualReadDir(
			uint64(0),
			fuse.AttributesMaskForFUSEAttr,
			gomock.Any(),
		).DoAndReturn(func(firstCookie uint64, requested virtual.AttributesMask, reporter virtual.DirectoryEntryReporter) virtual.Status {
			require.True(t, reporter.ReportDirectory(
				uint64(1),
				path.MustNewComponent("directory"),
				childDirectory,
				(&virtual.Attributes{}).
					SetFileType(filesystem.FileTypeDirectory).
					SetInodeNumber(2).
					SetLinkCount(12).
					SetPermissions(virtual.PermissionsExecute).
					SetSizeBytes(4096)))
			require.True(t, reporter.ReportLeaf(
				uint64(2),
				path.MustNewComponent("file"),
				childLeaf,
				(&virtual.Attributes{}).
					SetFileType(filesystem.FileTypeRegularFile).
					SetInodeNumber(3).
					SetLinkCount(4).
					SetPermissions(0).
					SetSizeBytes(8192)))
			return virtual.StatusOK
		})
		entryList := mock.NewMockReadDirPlusEntryList(ctrl)
		var entryOutDot, entryOutDirectory, entryOutFile go_fuse.EntryOut
		gomock.InOrder(
			entryList.EXPECT().AddDirLookupEntry(go_fuse.DirEntry{
				Name: ".",
				Mode: go_fuse.S_IFDIR,
			}, uint64(1)).Return(&entryOutDot),
			entryList.EXPECT().AddDirLookupEntry(go_fuse.DirEntry{
				Name: "..",
				Mode: go_fuse.S_IFDIR,
			}, uint64(2)).Return(&entryOutDot),
			entryList.EXPECT().AddDirLookupEntry(go_fuse.DirEntry{
				Name: "directory",
				Mode: go_fuse.S_IFDIR,
				Ino:  2,
			}, uint64(3)).Return(&entryOutDirectory),
			entryList.EXPECT().AddDirLookupEntry(go_fuse.DirEntry{
				Name: "file",
				Mode: go_fuse.S_IFREG,
				Ino:  3,
			}, uint64(4)).Return(&entryOutFile),
		)

		require.Equal(t, go_fuse.OK, rfs.ReadDirPlus(nil, &go_fuse.ReadIn{
			InHeader: go_fuse.InHeader{
				NodeId: go_fuse.FUSE_ROOT_ID,
			},
		}, entryList))
		require.Equal(t, go_fuse.EntryOut{}, entryOutDot)
		require.Equal(t, go_fuse.EntryOut{}, entryOutDot)
		require.Equal(t, go_fuse.EntryOut{
			NodeId: 2,
			Attr: go_fuse.Attr{
				Mode:  go_fuse.S_IFDIR | 0o111,
				Ino:   2,
				Nlink: 12,
				Size:  4096,
			},
		}, entryOutDirectory)
		require.Equal(t, go_fuse.EntryOut{
			NodeId: 3,
			Attr: go_fuse.Attr{
				Mode:  go_fuse.S_IFREG,
				Ino:   3,
				Nlink: 4,
				Size:  8192,
			},
		}, entryOutFile)

		// VirtualGetAttributes() should only be called on
		// objects contained within the resulting directory
		// listing.
		childDirectory.EXPECT().VirtualGetAttributes(fuse.AttributesMaskForFUSEAttr, gomock.Any()).DoAndReturn(
			func(requested virtual.AttributesMask, out *virtual.Attributes) {
				out.SetFileType(filesystem.FileTypeDirectory)
				out.SetInodeNumber(2)
				out.SetLinkCount(12)
				out.SetPermissions(virtual.PermissionsExecute)
				out.SetSizeBytes(4096)
			})
		childLeaf.EXPECT().VirtualGetAttributes(fuse.AttributesMaskForFUSEAttr, gomock.Any()).DoAndReturn(
			func(requested virtual.AttributesMask, out *virtual.Attributes) {
				out.SetFileType(filesystem.FileTypeRegularFile)
				out.SetInodeNumber(3)
				out.SetLinkCount(4)
				out.SetPermissions(0)
				out.SetSizeBytes(8192)
			})

		// The entries returned by ReadDirPlus() have been
		// registered automatically, meaning they can be
		// accessed without a separate Lookup().
		var directoryAttrOut go_fuse.AttrOut
		require.Equal(t, go_fuse.OK, rfs.GetAttr(nil, &go_fuse.GetAttrIn{
			InHeader: go_fuse.InHeader{
				NodeId: 2,
			},
		}, &directoryAttrOut))
		require.Equal(t, go_fuse.AttrOut{
			Attr: go_fuse.Attr{
				Mode:  go_fuse.S_IFDIR | 0o111,
				Ino:   2,
				Nlink: 12,
				Size:  4096,
			},
		}, directoryAttrOut)

		var leafAttrOut go_fuse.AttrOut
		require.Equal(t, go_fuse.OK, rfs.GetAttr(nil, &go_fuse.GetAttrIn{
			InHeader: go_fuse.InHeader{
				NodeId: 3,
			},
		}, &leafAttrOut))
		require.Equal(t, go_fuse.AttrOut{
			Attr: go_fuse.Attr{
				Mode:  go_fuse.S_IFREG,
				Ino:   3,
				Nlink: 4,
				Size:  8192,
			},
		}, leafAttrOut)

		// Release the children returned by ReadDirPlus().
		rfs.Forget(2, 1)
		rfs.Forget(3, 1)
	})

	// For partial reads, we defer to the test of ReadDir().

	// Close the root directory.
	rfs.ReleaseDir(&go_fuse.ReleaseIn{
		InHeader: go_fuse.InHeader{
			NodeId: go_fuse.FUSE_ROOT_ID,
		},
	})
}

func TestSimpleRawFileSystemReadlink(t *testing.T) {
	ctrl := gomock.NewController(t)

	rootDirectory := mock.NewMockVirtualDirectory(ctrl)
	removalNotifierRegistrar := mock.NewMockFUSERemovalNotifierRegistrar(ctrl)
	rfs := fuse.NewSimpleRawFileSystem(rootDirectory, removalNotifierRegistrar.Call)

	symlink := mock.NewMockVirtualLeaf(ctrl)
	rootDirectory.EXPECT().VirtualLookup(path.MustNewComponent("symlink"), fuse.AttributesMaskForFUSEAttr, gomock.Any()).DoAndReturn(
		func(name path.Component, requested virtual.AttributesMask, out *virtual.Attributes) (virtual.Directory, virtual.Leaf, virtual.Status) {
			out.SetFileType(filesystem.FileTypeSymlink)
			out.SetInodeNumber(2)
			out.SetLinkCount(1)
			out.SetPermissions(virtual.PermissionsRead | virtual.PermissionsWrite | virtual.PermissionsExecute)
			out.SetSizeBytes(6)
			return nil, symlink, virtual.StatusOK
		})

	var entryOut go_fuse.EntryOut
	require.Equal(t, go_fuse.OK, rfs.Lookup(nil, &go_fuse.InHeader{
		NodeId: go_fuse.FUSE_ROOT_ID,
	}, "symlink", &entryOut))
	require.Equal(t, go_fuse.EntryOut{
		NodeId: 2,
		Attr: go_fuse.Attr{
			Mode:  go_fuse.S_IFLNK | 0o777,
			Ino:   2,
			Nlink: 1,
			Size:  6,
		},
	}, entryOut)

	t.Run("IOError", func(t *testing.T) {
		symlink.EXPECT().VirtualReadlink().Return(nil, virtual.StatusErrIO)

		_, s := rfs.Readlink(nil, &go_fuse.InHeader{NodeId: 2})
		require.Equal(t, go_fuse.EIO, s)
	})

	t.Run("WrongFileType", func(t *testing.T) {
		symlink.EXPECT().VirtualReadlink().Return(nil, virtual.StatusErrInval)

		_, s := rfs.Readlink(nil, &go_fuse.InHeader{NodeId: 2})
		require.Equal(t, go_fuse.EINVAL, s)
	})

	t.Run("Success", func(t *testing.T) {
		symlink.EXPECT().VirtualReadlink().Return([]byte("target"), virtual.StatusOK)

		target, s := rfs.Readlink(nil, &go_fuse.InHeader{NodeId: 2})
		require.Equal(t, go_fuse.OK, s)
		require.Equal(t, []byte("target"), target)
	})
}

func TestSimpleRawFileSystemStatFs(t *testing.T) {
	ctrl := gomock.NewController(t)

	rootDirectory := mock.NewMockVirtualDirectory(ctrl)
	removalNotifierRegistrar := mock.NewMockFUSERemovalNotifierRegistrar(ctrl)
	rfs := fuse.NewSimpleRawFileSystem(rootDirectory, removalNotifierRegistrar.Call)

	t.Run("Success", func(t *testing.T) {
		// OSXFUSE lets the statvfs() system call succeed, even
		// if StatFs() returns ENOSYS. Linux is more strict, in
		// that this causes statvfs() to fail.
		//
		// Even though we don't provide any meaningful
		// statistics, return success to get consistent behavior
		// across platforms.
		var statfsOut go_fuse.StatfsOut
		require.Equal(t, go_fuse.OK, rfs.StatFs(nil, &go_fuse.InHeader{
			NodeId: go_fuse.FUSE_ROOT_ID,
		}, &statfsOut))
		require.Equal(t, go_fuse.StatfsOut{
			NameLen: 255,
		}, statfsOut)
	})
}

func TestSimpleRawFileSystemInit(t *testing.T) {
	ctrl := gomock.NewController(t)

	rootDirectory := mock.NewMockVirtualDirectory(ctrl)
	removalNotifierRegistrar := mock.NewMockFUSERemovalNotifierRegistrar(ctrl)
	rfs := fuse.NewSimpleRawFileSystem(rootDirectory, removalNotifierRegistrar.Call)

	// An Init() operation should cause SimpleRawFileSystem to
	// register a removal notifier that forwards calls to
	// EntryNotify() on the FUSE server.
	//
	// Because the FUSE server expects that we use
	// go_fuse.FUSE_ROOT_ID for the root directory, we should do a
	// one-time lookup of the inode number of the root directory, so
	// that we can distinguish it from the other directories going
	// forward.
	rootDirectory.EXPECT().VirtualGetAttributes(virtual.AttributesMaskInodeNumber, gomock.Any()).
		Do(func(requested virtual.AttributesMask, attributes *virtual.Attributes) {
			attributes.SetInodeNumber(123)
		})
	var removalNotifier virtual.FUSERemovalNotifier
	removalNotifierRegistrar.EXPECT().Call(gomock.Any()).Do(func(rn virtual.FUSERemovalNotifier) {
		removalNotifier = rn
	})
	mockServerCallbacks := mock.NewMockServerCallbacks(ctrl)
	rfs.Init(mockServerCallbacks)

	t.Run("RootDirectory", func(t *testing.T) {
		// Calls for the root directory should have their inode
		// number translated to FUSE_ROOT_ID, as that is the
		// node ID that the FUSE protocol uses for the root
		// directory object.
		mockServerCallbacks.EXPECT().EntryNotify(uint64(go_fuse.FUSE_ROOT_ID), "Hello")
		removalNotifier(123, path.MustNewComponent("Hello"))
	})

	t.Run("ChildDirectory", func(t *testing.T) {
		// Add a second directory to the map of directories
		// tracked by SimpleRawFileSystem.
		childDirectory := mock.NewMockVirtualDirectory(ctrl)
		rootDirectory.EXPECT().VirtualLookup(path.MustNewComponent("directory"), fuse.AttributesMaskForFUSEAttr, gomock.Any()).DoAndReturn(
			func(name path.Component, requested virtual.AttributesMask, out *virtual.Attributes) (virtual.Directory, virtual.Leaf, virtual.Status) {
				out.SetFileType(filesystem.FileTypeDirectory)
				out.SetInodeNumber(456)
				out.SetLinkCount(1)
				out.SetPermissions(virtual.PermissionsExecute)
				out.SetSizeBytes(1200)
				return childDirectory, nil, virtual.StatusOK
			})

		var entryOut go_fuse.EntryOut
		require.Equal(t, go_fuse.OK, rfs.Lookup(nil, &go_fuse.InHeader{
			NodeId: go_fuse.FUSE_ROOT_ID,
		}, "directory", &entryOut))
		require.Equal(t, go_fuse.EntryOut{
			NodeId: 456,
			Attr: go_fuse.Attr{
				Mode:  go_fuse.S_IFDIR | 0o111,
				Ino:   456,
				Nlink: 1,
				Size:  1200,
			},
		}, entryOut)

		// Calls to EntryNotify() should be forwarded to the
		// underlying server in unmodified form.
		mockServerCallbacks.EXPECT().EntryNotify(uint64(456), "hello")
		removalNotifier(456, path.MustNewComponent("hello"))

		// Once the kernel requests that the directory is
		// forgotten, there is no longer any need for calling
		// into the kernel to forget directory entries.
		rfs.Forget(456, 1)
		removalNotifier(456, path.MustNewComponent("hello"))
	})
}

// TODO: Add testing coverage for other calls as well.
