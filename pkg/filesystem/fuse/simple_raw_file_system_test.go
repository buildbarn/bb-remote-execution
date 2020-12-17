// +build darwin linux

package fuse_test

import (
	"sort"
	"syscall"
	"testing"

	"github.com/buildbarn/bb-remote-execution/internal/mock"
	"github.com/buildbarn/bb-remote-execution/pkg/filesystem/fuse"
	"github.com/golang/mock/gomock"
	go_fuse "github.com/hanwen/go-fuse/v2/fuse"
	"github.com/stretchr/testify/require"
)

func TestSimpleRawFileSystemLookup(t *testing.T) {
	ctrl := gomock.NewController(t)

	rootDirectory := mock.NewMockFUSEDirectory(ctrl)
	var serverCallbacks fuse.SimpleRawFileSystemServerCallbacks
	rfs := fuse.NewSimpleRawFileSystem(rootDirectory, 1, sort.Sort, &serverCallbacks)

	t.Run("NotFound", func(t *testing.T) {
		// Lookup failure errors should be propagated.
		rootDirectory.EXPECT().FUSELookup("nonexistent", gomock.Any()).
			Return(nil, nil, go_fuse.ENOENT)

		var entryOut go_fuse.EntryOut
		require.Equal(t, go_fuse.ENOENT, rfs.Lookup(nil, &go_fuse.InHeader{
			NodeId: go_fuse.FUSE_ROOT_ID,
		}, "nonexistent", &entryOut))
	})

	t.Run("Directory", func(t *testing.T) {
		// Looking up a directory should cause the attributes to
		// be returned. The inode number should be used as the
		// node ID, so that future operations can refer to it.
		childDirectory := mock.NewMockFUSEDirectory(ctrl)
		rootDirectory.EXPECT().FUSELookup("directory", gomock.Any()).DoAndReturn(
			func(name string, out *go_fuse.Attr) (fuse.Directory, fuse.Leaf, go_fuse.Status) {
				out.Mode = go_fuse.S_IFDIR | 0700
				out.Ino = 123
				return childDirectory, nil, go_fuse.OK
			})

		var entryOut go_fuse.EntryOut
		require.Equal(t, go_fuse.OK, rfs.Lookup(nil, &go_fuse.InHeader{
			NodeId: go_fuse.FUSE_ROOT_ID,
		}, "directory", &entryOut))
		require.Equal(t, go_fuse.EntryOut{
			NodeId: 123,
			Attr: go_fuse.Attr{
				Mode: go_fuse.S_IFDIR | 0700,
				Ino:  123,
			},
		}, entryOut)

		// Performing a successive lookup against the node ID of
		// the directory should call into that directory; not
		// the root directory.
		childDirectory.EXPECT().FUSELookup("nonexistent", gomock.Any()).
			Return(nil, nil, go_fuse.ENOENT)

		require.Equal(t, go_fuse.ENOENT, rfs.Lookup(nil, &go_fuse.InHeader{
			NodeId: 123,
		}, "nonexistent", &entryOut))
	})

	t.Run("File", func(t *testing.T) {
		// Looking up a file should work similarly to the above.
		childFile := mock.NewMockFUSELeaf(ctrl)
		rootDirectory.EXPECT().FUSELookup("file", gomock.Any()).DoAndReturn(
			func(name string, out *go_fuse.Attr) (fuse.Directory, fuse.Leaf, go_fuse.Status) {
				out.Mode = go_fuse.S_IFREG | 0600
				out.Ino = 456
				return nil, childFile, go_fuse.OK
			})

		var entryOut go_fuse.EntryOut
		require.Equal(t, go_fuse.OK, rfs.Lookup(nil, &go_fuse.InHeader{
			NodeId: go_fuse.FUSE_ROOT_ID,
		}, "file", &entryOut))
		require.Equal(t, go_fuse.EntryOut{
			NodeId: 456,
			Attr: go_fuse.Attr{
				Mode: go_fuse.S_IFREG | 0600,
				Ino:  456,
			},
		}, entryOut)
	})
}

func TestSimpleRawFileSystemForget(t *testing.T) {
	ctrl := gomock.NewController(t)

	rootDirectory := mock.NewMockFUSEDirectory(ctrl)
	var serverCallbacks fuse.SimpleRawFileSystemServerCallbacks
	rfs := fuse.NewSimpleRawFileSystem(rootDirectory, 1, sort.Sort, &serverCallbacks)

	for i := 0; i < 10; i++ {
		// Perform ten lookups of the same directory.
		childDirectory := mock.NewMockFUSEDirectory(ctrl)
		for j := 0; j < 10; j++ {
			rootDirectory.EXPECT().FUSELookup("directory", gomock.Any()).DoAndReturn(
				func(name string, out *go_fuse.Attr) (fuse.Directory, fuse.Leaf, go_fuse.Status) {
					out.Mode = go_fuse.S_IFDIR | 0700
					out.Ino = 123
					return childDirectory, nil, go_fuse.OK
				})

			var entryOut go_fuse.EntryOut
			require.Equal(t, go_fuse.OK, rfs.Lookup(nil, &go_fuse.InHeader{
				NodeId: go_fuse.FUSE_ROOT_ID,
			}, "directory", &entryOut))
			require.Equal(t, go_fuse.EntryOut{
				NodeId: 123,
				Attr: go_fuse.Attr{
					Mode: go_fuse.S_IFDIR | 0700,
					Ino:  123,
				},
			}, entryOut)
		}

		// Operations against the node ID of the directory
		// should all be forwarded to the directory object.
		childDirectory.EXPECT().FUSEGetAttr(gomock.Any()).DoAndReturn(
			func(out *go_fuse.Attr) {
				out.Mode = go_fuse.S_IFDIR | 0700
				out.Ino = 123
			})

		var directoryAttrOut go_fuse.AttrOut
		require.Equal(t, go_fuse.OK, rfs.GetAttr(nil, &go_fuse.GetAttrIn{
			InHeader: go_fuse.InHeader{
				NodeId: 123,
			},
		}, &directoryAttrOut))
		require.Equal(t, go_fuse.AttrOut{
			Attr: go_fuse.Attr{
				Mode: go_fuse.S_IFDIR | 0700,
				Ino:  123,
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
		childFile := mock.NewMockFUSELeaf(ctrl)
		for j := 0; j < 10; j++ {
			rootDirectory.EXPECT().FUSELookup("file", gomock.Any()).DoAndReturn(
				func(name string, out *go_fuse.Attr) (fuse.Directory, fuse.Leaf, go_fuse.Status) {
					out.Mode = go_fuse.S_IFREG | 0600
					out.Ino = 123
					return nil, childFile, go_fuse.OK
				})

			var entryOut go_fuse.EntryOut
			require.Equal(t, go_fuse.OK, rfs.Lookup(nil, &go_fuse.InHeader{
				NodeId: go_fuse.FUSE_ROOT_ID,
			}, "file", &entryOut))
			require.Equal(t, go_fuse.EntryOut{
				NodeId: 123,
				Attr: go_fuse.Attr{
					Mode: go_fuse.S_IFREG | 0600,
					Ino:  123,
				},
			}, entryOut)
		}

		// Operations against the node ID should now all go to
		// the file -- not the directory.
		childFile.EXPECT().FUSEGetAttr(gomock.Any()).DoAndReturn(
			func(out *go_fuse.Attr) {
				out.Mode = go_fuse.S_IFREG | 0600
				out.Ino = 123
			})

		var fileAttrOut go_fuse.AttrOut
		require.Equal(t, go_fuse.OK, rfs.GetAttr(nil, &go_fuse.GetAttrIn{
			InHeader: go_fuse.InHeader{
				NodeId: 123,
			},
		}, &fileAttrOut))
		require.Equal(t, go_fuse.AttrOut{
			Attr: go_fuse.Attr{
				Mode: go_fuse.S_IFREG | 0600,
				Ino:  123,
			},
		}, fileAttrOut)

		// Also forget the file a total of ten times.
		rfs.Forget(123, 9)
		rfs.Forget(123, 1)
	}
}

func TestSimpleRawFileSystemGetAttr(t *testing.T) {
	ctrl := gomock.NewController(t)

	rootDirectory := mock.NewMockFUSEDirectory(ctrl)
	var serverCallbacks fuse.SimpleRawFileSystemServerCallbacks
	rfs := fuse.NewSimpleRawFileSystem(rootDirectory, 1, sort.Sort, &serverCallbacks)

	t.Run("Success", func(t *testing.T) {
		rootDirectory.EXPECT().FUSEGetAttr(gomock.Any()).DoAndReturn(
			func(out *go_fuse.Attr) {
				out.Mode = go_fuse.S_IFDIR | 0700
				out.Ino = go_fuse.FUSE_ROOT_ID
			})

		var attrOut go_fuse.AttrOut
		require.Equal(t, go_fuse.OK, rfs.GetAttr(nil, &go_fuse.GetAttrIn{
			InHeader: go_fuse.InHeader{
				NodeId: go_fuse.FUSE_ROOT_ID,
			},
		}, &attrOut))
		require.Equal(t, go_fuse.AttrOut{
			Attr: go_fuse.Attr{
				Mode: go_fuse.S_IFDIR | 0700,
				Ino:  go_fuse.FUSE_ROOT_ID,
			},
		}, attrOut)
	})
}

func TestSimpleRawFileSystemSetAttr(t *testing.T) {
	ctrl := gomock.NewController(t)

	rootDirectory := mock.NewMockFUSEDirectory(ctrl)
	var serverCallbacks fuse.SimpleRawFileSystemServerCallbacks
	rfs := fuse.NewSimpleRawFileSystem(rootDirectory, 1, sort.Sort, &serverCallbacks)

	request := &go_fuse.SetAttrIn{
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
	}

	t.Run("Failure", func(t *testing.T) {
		// A chown() call that is denied.
		rootDirectory.EXPECT().FUSESetAttr(request, gomock.Any()).Return(go_fuse.EPERM)

		var attrOut go_fuse.AttrOut
		require.Equal(t, go_fuse.EPERM, rfs.SetAttr(nil, request, &attrOut))
	})

	t.Run("Success", func(t *testing.T) {
		// A chown() call that is permitted.
		rootDirectory.EXPECT().FUSESetAttr(request, gomock.Any()).DoAndReturn(
			func(in *go_fuse.SetAttrIn, out *go_fuse.Attr) go_fuse.Status {
				out.Mode = go_fuse.S_IFDIR | 0700
				out.Ino = go_fuse.FUSE_ROOT_ID
				out.Uid = 1000
				out.Gid = 1000
				return go_fuse.OK
			})

		var attrOut go_fuse.AttrOut
		require.Equal(t, go_fuse.OK, rfs.SetAttr(nil, request, &attrOut))
		require.Equal(t, go_fuse.AttrOut{
			Attr: go_fuse.Attr{
				Mode: go_fuse.S_IFDIR | 0700,
				Ino:  go_fuse.FUSE_ROOT_ID,
				Owner: go_fuse.Owner{
					Uid: 1000,
					Gid: 1000,
				},
			},
		}, attrOut)
	})
}

func TestSimpleRawFileSystemMknod(t *testing.T) {
	ctrl := gomock.NewController(t)

	rootDirectory := mock.NewMockFUSEDirectory(ctrl)
	var serverCallbacks fuse.SimpleRawFileSystemServerCallbacks
	rfs := fuse.NewSimpleRawFileSystem(rootDirectory, 1, sort.Sort, &serverCallbacks)

	t.Run("Failure", func(t *testing.T) {
		// An mknod() call for a block device that is denied.
		rootDirectory.EXPECT().FUSEMknod("hello", uint32(syscall.S_IFBLK|0777), uint32(456), gomock.Any()).
			Return(nil, go_fuse.EPERM)

		var entryOut go_fuse.EntryOut
		require.Equal(t, go_fuse.EPERM, rfs.Mknod(nil, &go_fuse.MknodIn{
			InHeader: go_fuse.InHeader{
				NodeId: go_fuse.FUSE_ROOT_ID,
			},
			Mode: syscall.S_IFBLK | 0777,
			Rdev: 456,
		}, "hello", &entryOut))
	})

	t.Run("Success", func(t *testing.T) {
		// An mknod() call for a FIFO that succeeds.
		childLeaf := mock.NewMockFUSELeaf(ctrl)
		rootDirectory.EXPECT().FUSEMknod("hello", uint32(go_fuse.S_IFIFO|0700), uint32(0), gomock.Any()).DoAndReturn(
			func(name string, mode, dev uint32, out *go_fuse.Attr) (fuse.Leaf, go_fuse.Status) {
				out.Mode = go_fuse.S_IFIFO | 0700
				out.Ino = 123
				return childLeaf, go_fuse.OK
			})

		var entryOut go_fuse.EntryOut
		require.Equal(t, go_fuse.OK, rfs.Mknod(nil, &go_fuse.MknodIn{
			InHeader: go_fuse.InHeader{
				NodeId: go_fuse.FUSE_ROOT_ID,
			},
			Mode: go_fuse.S_IFIFO | 0700,
		}, "hello", &entryOut))
		require.Equal(t, go_fuse.EntryOut{
			NodeId: 123,
			Attr: go_fuse.Attr{
				Mode: go_fuse.S_IFIFO | 0700,
				Ino:  123,
			},
		}, entryOut)
	})
}

func TestSimpleRawFileSystemMkdir(t *testing.T) {
	ctrl := gomock.NewController(t)

	rootDirectory := mock.NewMockFUSEDirectory(ctrl)
	var serverCallbacks fuse.SimpleRawFileSystemServerCallbacks
	rfs := fuse.NewSimpleRawFileSystem(rootDirectory, 1, sort.Sort, &serverCallbacks)

	t.Run("Failure", func(t *testing.T) {
		// An mkdir() call that fails due to an I/O error.
		rootDirectory.EXPECT().FUSEMkdir("hello", uint32(0777), gomock.Any()).
			Return(nil, go_fuse.EIO)

		var entryOut go_fuse.EntryOut
		require.Equal(t, go_fuse.EIO, rfs.Mkdir(nil, &go_fuse.MkdirIn{
			InHeader: go_fuse.InHeader{
				NodeId: go_fuse.FUSE_ROOT_ID,
			},
			Mode: 0777,
		}, "hello", &entryOut))
	})

	t.Run("Success", func(t *testing.T) {
		// An mkdir() call that succeeds.
		childDirectory := mock.NewMockFUSEDirectory(ctrl)
		rootDirectory.EXPECT().FUSEMkdir("hello", uint32(0777), gomock.Any()).DoAndReturn(
			func(name string, mode uint32, out *go_fuse.Attr) (fuse.Directory, go_fuse.Status) {
				out.Mode = go_fuse.S_IFDIR | 0777
				out.Ino = 123
				return childDirectory, go_fuse.OK
			})

		var entryOut go_fuse.EntryOut
		require.Equal(t, go_fuse.OK, rfs.Mkdir(nil, &go_fuse.MkdirIn{
			InHeader: go_fuse.InHeader{
				NodeId: go_fuse.FUSE_ROOT_ID,
			},
			Mode: 0777,
		}, "hello", &entryOut))
		require.Equal(t, go_fuse.EntryOut{
			NodeId: 123,
			Attr: go_fuse.Attr{
				Mode: go_fuse.S_IFDIR | 0777,
				Ino:  123,
			},
		}, entryOut)
	})
}

func TestSimpleRawFileSystemReadDir(t *testing.T) {
	ctrl := gomock.NewController(t)

	rootDirectory := mock.NewMockFUSEDirectory(ctrl)
	var serverCallbacks fuse.SimpleRawFileSystemServerCallbacks
	rfs := fuse.NewSimpleRawFileSystem(rootDirectory, 1, sort.Sort, &serverCallbacks)

	// Open the root directory.
	var openOut go_fuse.OpenOut
	require.Equal(t, go_fuse.OK, rfs.OpenDir(nil, &go_fuse.OpenIn{
		InHeader: go_fuse.InHeader{
			NodeId: go_fuse.FUSE_ROOT_ID,
		},
	}, &openOut))
	require.Equal(t, go_fuse.OpenOut{
		Fh: 1,
	}, openOut)

	t.Run("Failure", func(t *testing.T) {
		// Directory listing failures should be propagated.
		rootDirectory.EXPECT().FUSEReadDir().Return(nil, go_fuse.EIO)
		entryList := mock.NewMockReadDirEntryList(ctrl)

		require.Equal(t, go_fuse.EIO, rfs.ReadDir(nil, &go_fuse.ReadIn{
			InHeader: go_fuse.InHeader{
				NodeId: go_fuse.FUSE_ROOT_ID,
			},
			Fh: 1,
		}, entryList))
	})

	t.Run("SuccessOffsets", func(t *testing.T) {
		// Repeatedly request directory listings at different offsets.
		// FUSEReadDir() should only be invoked when ReadDir()
		// is called with offset zero.
		rootDirectory.EXPECT().FUSEReadDir().Return([]go_fuse.DirEntry{
			{Name: "directory", Mode: go_fuse.S_IFDIR, Ino: 2},
			{Name: "file", Mode: go_fuse.S_IFREG, Ino: 3},
		}, go_fuse.OK).Times(10)

		for round := 0; round < 10; round++ {
			entryList := mock.NewMockReadDirEntryList(ctrl)
			entryList.EXPECT().AddDirEntry(go_fuse.DirEntry{
				Name: ".",
				Mode: go_fuse.S_IFDIR,
			}).Return(true)
			entryList.EXPECT().AddDirEntry(go_fuse.DirEntry{
				Name: "..",
				Mode: go_fuse.S_IFDIR,
			}).Return(true).Times(2)
			entryList.EXPECT().AddDirEntry(go_fuse.DirEntry{
				Name: "directory",
				Mode: go_fuse.S_IFDIR,
				Ino:  2,
			}).Return(true).Times(3)
			entryList.EXPECT().AddDirEntry(go_fuse.DirEntry{
				Name: "file",
				Mode: go_fuse.S_IFREG,
				Ino:  3,
			}).Return(true).Times(4)

			for offset := uint64(0); offset < 10; offset++ {
				require.Equal(t, go_fuse.OK, rfs.ReadDir(nil, &go_fuse.ReadIn{
					InHeader: go_fuse.InHeader{
						NodeId: go_fuse.FUSE_ROOT_ID,
					},
					Fh:     1,
					Offset: offset,
				}, entryList))
			}
		}
	})

	t.Run("SuccessSorting", func(t *testing.T) {
		// SimpleRawFileSystem should respect the sorter that is
		// provided. In the case of FUSEReadDir(), dot and
		// dot-dot are returned first, followed by all other
		// entries in sorted order.
		rootDirectory.EXPECT().FUSEReadDir().Return([]go_fuse.DirEntry{
			{Name: "d", Mode: go_fuse.S_IFREG, Ino: 2},
			{Name: "a", Mode: go_fuse.S_IFREG, Ino: 3},
			{Name: "c", Mode: go_fuse.S_IFREG, Ino: 4},
			{Name: "e", Mode: go_fuse.S_IFREG, Ino: 5},
			{Name: "b", Mode: go_fuse.S_IFREG, Ino: 6},
		}, go_fuse.OK)
		entryList := mock.NewMockReadDirEntryList(ctrl)
		gomock.InOrder(
			entryList.EXPECT().AddDirEntry(go_fuse.DirEntry{
				Name: ".",
				Mode: go_fuse.S_IFDIR,
			}).Return(true),
			entryList.EXPECT().AddDirEntry(go_fuse.DirEntry{
				Name: "..",
				Mode: go_fuse.S_IFDIR,
			}).Return(true),
			entryList.EXPECT().AddDirEntry(go_fuse.DirEntry{
				Name: "a",
				Mode: go_fuse.S_IFREG,
				Ino:  3,
			}).Return(true),
			entryList.EXPECT().AddDirEntry(go_fuse.DirEntry{
				Name: "b",
				Mode: go_fuse.S_IFREG,
				Ino:  6,
			}).Return(true),
			entryList.EXPECT().AddDirEntry(go_fuse.DirEntry{
				Name: "c",
				Mode: go_fuse.S_IFREG,
				Ino:  4,
			}).Return(true),
			entryList.EXPECT().AddDirEntry(go_fuse.DirEntry{
				Name: "d",
				Mode: go_fuse.S_IFREG,
				Ino:  2,
			}).Return(true),
			entryList.EXPECT().AddDirEntry(go_fuse.DirEntry{
				Name: "e",
				Mode: go_fuse.S_IFREG,
				Ino:  5,
			}).Return(true),
		)

		require.Equal(t, go_fuse.OK, rfs.ReadDir(nil, &go_fuse.ReadIn{
			InHeader: go_fuse.InHeader{
				NodeId: go_fuse.FUSE_ROOT_ID,
			},
			Fh: 1,
		}, entryList))
	})

	// Close the root directory.
	rfs.ReleaseDir(&go_fuse.ReleaseIn{
		InHeader: go_fuse.InHeader{
			NodeId: go_fuse.FUSE_ROOT_ID,
		},
		Fh: 1,
	})
}

func TestSimpleRawFileSystemReadDirPlus(t *testing.T) {
	ctrl := gomock.NewController(t)

	rootDirectory := mock.NewMockFUSEDirectory(ctrl)
	var serverCallbacks fuse.SimpleRawFileSystemServerCallbacks
	rfs := fuse.NewSimpleRawFileSystem(rootDirectory, 1, sort.Sort, &serverCallbacks)

	// Open the root directory.
	var openOut go_fuse.OpenOut
	require.Equal(t, go_fuse.OK, rfs.OpenDir(nil, &go_fuse.OpenIn{
		InHeader: go_fuse.InHeader{
			NodeId: go_fuse.FUSE_ROOT_ID,
		},
	}, &openOut))
	require.Equal(t, go_fuse.OpenOut{
		Fh: 1,
	}, openOut)

	t.Run("Failure", func(t *testing.T) {
		// Directory listing failures should be propagated.
		rootDirectory.EXPECT().FUSEReadDirPlus().Return(nil, nil, go_fuse.EIO)
		entryList := mock.NewMockReadDirPlusEntryList(ctrl)

		require.Equal(t, go_fuse.EIO, rfs.ReadDirPlus(nil, &go_fuse.ReadIn{
			InHeader: go_fuse.InHeader{
				NodeId: go_fuse.FUSE_ROOT_ID,
			},
			Fh: 1,
		}, entryList))
	})

	t.Run("SuccessOffsets", func(t *testing.T) {
		// Repeatedly request directory listings at different offsets.
		childDirectory := mock.NewMockFUSEDirectory(ctrl)
		childLeaf := mock.NewMockFUSELeaf(ctrl)

		// FUSEReadDirPlus() should only be invoked when
		// ReadDirPlus() is called with offset zero.
		rootDirectory.EXPECT().FUSEReadDirPlus().Return(
			[]fuse.DirectoryDirEntry{
				{
					Child: childDirectory,
					DirEntry: go_fuse.DirEntry{
						Name: "directory",
						Mode: go_fuse.S_IFDIR,
						Ino:  2,
					},
				},
			},
			[]fuse.LeafDirEntry{
				{
					Child: childLeaf,
					DirEntry: go_fuse.DirEntry{
						Name: "file",
						Mode: go_fuse.S_IFREG,
						Ino:  3,
					},
				},
			},
			go_fuse.OK).Times(10)

		// FUSEGetAttr() should only be called on objects
		// contained within the resulting directory listing.
		childDirectory.EXPECT().FUSEGetAttr(gomock.Any()).DoAndReturn(
			func(out *go_fuse.Attr) {
				out.Mode = go_fuse.S_IFDIR | 0700
				out.Ino = 2
			}).Times(30 + 1)
		childLeaf.EXPECT().FUSEGetAttr(gomock.Any()).DoAndReturn(
			func(out *go_fuse.Attr) {
				out.Mode = go_fuse.S_IFREG | 0600
				out.Ino = 3
			}).Times(40 + 1)

		// Invoke FUSEReadDirPlus() at different offsets.
		for round := 0; round < 10; round++ {
			entryList := mock.NewMockReadDirPlusEntryList(ctrl)
			var entryOut1 go_fuse.EntryOut
			entryList.EXPECT().AddDirLookupEntry(go_fuse.DirEntry{
				Name: ".",
				Mode: go_fuse.S_IFDIR,
			}).Return(&entryOut1)
			var entryOut2 go_fuse.EntryOut
			entryList.EXPECT().AddDirLookupEntry(go_fuse.DirEntry{
				Name: "..",
				Mode: go_fuse.S_IFDIR,
			}).Return(&entryOut2).Times(2)
			var entryOut3 go_fuse.EntryOut
			entryList.EXPECT().AddDirLookupEntry(go_fuse.DirEntry{
				Name: "directory",
				Mode: go_fuse.S_IFDIR,
				Ino:  2,
			}).Return(&entryOut3).Times(3)
			var entryOut4 go_fuse.EntryOut
			entryList.EXPECT().AddDirLookupEntry(go_fuse.DirEntry{
				Name: "file",
				Mode: go_fuse.S_IFREG,
				Ino:  3,
			}).Return(&entryOut4).Times(4)

			for offset := uint64(0); offset < 10; offset++ {
				require.Equal(t, go_fuse.OK, rfs.ReadDirPlus(nil, &go_fuse.ReadIn{
					InHeader: go_fuse.InHeader{
						NodeId: go_fuse.FUSE_ROOT_ID,
					},
					Fh:     1,
					Offset: offset,
				}, entryList))
			}

			require.Equal(t, go_fuse.EntryOut{}, entryOut1)
			require.Equal(t, go_fuse.EntryOut{}, entryOut2)
			require.Equal(t, go_fuse.EntryOut{
				NodeId: 2,
				Attr: go_fuse.Attr{
					Mode: go_fuse.S_IFDIR | 0700,
					Ino:  2,
				},
			}, entryOut3)
			require.Equal(t, go_fuse.EntryOut{
				NodeId: 3,
				Attr: go_fuse.Attr{
					Mode: go_fuse.S_IFREG | 0600,
					Ino:  3,
				},
			}, entryOut4)
		}

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
				Mode: go_fuse.S_IFDIR | 0700,
				Ino:  2,
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
				Mode: go_fuse.S_IFREG | 0600,
				Ino:  3,
			},
		}, leafAttrOut)

		// Release the children returned by ReadDirPlus().
		rfs.Forget(2, 30)
		rfs.Forget(3, 40)
	})

	t.Run("SuccessSorting", func(t *testing.T) {
		// SimpleRawFileSystem should respect the sorter that is
		// provided. In the case of FUSEReadDirPlus(), dot and
		// dot-dot are returned first, followed by all
		// directories, followed by all leaves, both in sorted
		// order.
		childDirectory := mock.NewMockFUSEDirectory(ctrl)
		childLeaf := mock.NewMockFUSELeaf(ctrl)
		rootDirectory.EXPECT().FUSEReadDirPlus().Return(
			[]fuse.DirectoryDirEntry{
				{
					Child: childDirectory,
					DirEntry: go_fuse.DirEntry{
						Name: "f",
						Mode: go_fuse.S_IFDIR,
						Ino:  2,
					},
				},
				{
					Child: childDirectory,
					DirEntry: go_fuse.DirEntry{
						Name: "d",
						Mode: go_fuse.S_IFDIR,
						Ino:  2,
					},
				},
				{
					Child: childDirectory,
					DirEntry: go_fuse.DirEntry{
						Name: "a",
						Mode: go_fuse.S_IFDIR,
						Ino:  2,
					},
				},
			},
			[]fuse.LeafDirEntry{
				{
					Child: childLeaf,
					DirEntry: go_fuse.DirEntry{
						Name: "b",
						Mode: go_fuse.S_IFREG,
						Ino:  3,
					},
				},
				{
					Child: childLeaf,
					DirEntry: go_fuse.DirEntry{
						Name: "e",
						Mode: go_fuse.S_IFREG,
						Ino:  3,
					},
				},
				{
					Child: childLeaf,
					DirEntry: go_fuse.DirEntry{
						Name: "c",
						Mode: go_fuse.S_IFREG,
						Ino:  3,
					},
				},
			},
			go_fuse.OK)

		childDirectory.EXPECT().FUSEGetAttr(gomock.Any()).DoAndReturn(
			func(out *go_fuse.Attr) {
				out.Mode = go_fuse.S_IFDIR | 0700
				out.Ino = 2
			}).Times(3)
		childLeaf.EXPECT().FUSEGetAttr(gomock.Any()).DoAndReturn(
			func(out *go_fuse.Attr) {
				out.Mode = go_fuse.S_IFREG | 0600
				out.Ino = 3
			}).Times(3)

		entryList := mock.NewMockReadDirPlusEntryList(ctrl)
		var entryOutDot go_fuse.EntryOut
		var entryOutDirectory go_fuse.EntryOut
		var entryOutLeaf go_fuse.EntryOut
		gomock.InOrder(
			entryList.EXPECT().AddDirLookupEntry(go_fuse.DirEntry{
				Name: ".",
				Mode: go_fuse.S_IFDIR,
			}).Return(&entryOutDot),
			entryList.EXPECT().AddDirLookupEntry(go_fuse.DirEntry{
				Name: "..",
				Mode: go_fuse.S_IFDIR,
			}).Return(&entryOutDot),
			entryList.EXPECT().AddDirLookupEntry(go_fuse.DirEntry{
				Name: "a",
				Mode: go_fuse.S_IFDIR,
				Ino:  2,
			}).Return(&entryOutDirectory),
			entryList.EXPECT().AddDirLookupEntry(go_fuse.DirEntry{
				Name: "d",
				Mode: go_fuse.S_IFDIR,
				Ino:  2,
			}).Return(&entryOutDirectory),
			entryList.EXPECT().AddDirLookupEntry(go_fuse.DirEntry{
				Name: "f",
				Mode: go_fuse.S_IFDIR,
				Ino:  2,
			}).Return(&entryOutDirectory),
			entryList.EXPECT().AddDirLookupEntry(go_fuse.DirEntry{
				Name: "b",
				Mode: go_fuse.S_IFREG,
				Ino:  3,
			}).Return(&entryOutLeaf),
			entryList.EXPECT().AddDirLookupEntry(go_fuse.DirEntry{
				Name: "c",
				Mode: go_fuse.S_IFREG,
				Ino:  3,
			}).Return(&entryOutLeaf),
			entryList.EXPECT().AddDirLookupEntry(go_fuse.DirEntry{
				Name: "e",
				Mode: go_fuse.S_IFREG,
				Ino:  3,
			}).Return(&entryOutLeaf),
		)

		require.Equal(t, go_fuse.OK, rfs.ReadDirPlus(nil, &go_fuse.ReadIn{
			InHeader: go_fuse.InHeader{
				NodeId: go_fuse.FUSE_ROOT_ID,
			},
			Fh: 1,
		}, entryList))

		require.Equal(t, go_fuse.EntryOut{}, entryOutDot)
		require.Equal(t, go_fuse.EntryOut{
			NodeId: 2,
			Attr: go_fuse.Attr{
				Mode: go_fuse.S_IFDIR | 0700,
				Ino:  2,
			},
		}, entryOutDirectory)
		require.Equal(t, go_fuse.EntryOut{
			NodeId: 3,
			Attr: go_fuse.Attr{
				Mode: go_fuse.S_IFREG | 0600,
				Ino:  3,
			},
		}, entryOutLeaf)

		rfs.Forget(2, 3)
		rfs.Forget(3, 3)
	})

	// Close the root directory.
	rfs.ReleaseDir(&go_fuse.ReleaseIn{
		InHeader: go_fuse.InHeader{
			NodeId: go_fuse.FUSE_ROOT_ID,
		},
		Fh: 1,
	})
}

func TestSimpleRawFileSystemStatFs(t *testing.T) {
	ctrl := gomock.NewController(t)

	rootDirectory := mock.NewMockFUSEDirectory(ctrl)
	var serverCallbacks fuse.SimpleRawFileSystemServerCallbacks
	rfs := fuse.NewSimpleRawFileSystem(rootDirectory, 1, sort.Sort, &serverCallbacks)

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

	rootDirectory := mock.NewMockFUSEDirectory(ctrl)
	var serverCallbacks fuse.SimpleRawFileSystemServerCallbacks
	rfs := fuse.NewSimpleRawFileSystem(rootDirectory, 123, sort.Sort, &serverCallbacks)

	mockServerCallbacks := mock.NewMockServerCallbacks(ctrl)
	rfs.Init(mockServerCallbacks)

	t.Run("RootDirectory", func(t *testing.T) {
		// Calls for the root directory should have their inode
		// number translated to FUSE_ROOT_ID, as that is the
		// node ID that the FUSE protocol uses for the root
		// directory object.
		mockServerCallbacks.EXPECT().EntryNotify(uint64(go_fuse.FUSE_ROOT_ID), "Hello")
		serverCallbacks.EntryNotify(123, "Hello")
	})

	t.Run("ChildDirectory", func(t *testing.T) {
		// Add a second directory to the map of directories
		// tracked by SimpleRawFileSystem.
		childDirectory := mock.NewMockFUSEDirectory(ctrl)
		rootDirectory.EXPECT().FUSELookup("directory", gomock.Any()).DoAndReturn(
			func(name string, out *go_fuse.Attr) (fuse.Directory, fuse.Leaf, go_fuse.Status) {
				out.Mode = go_fuse.S_IFDIR | 0700
				out.Ino = 456
				return childDirectory, nil, go_fuse.OK
			})

		var entryOut go_fuse.EntryOut
		require.Equal(t, go_fuse.OK, rfs.Lookup(nil, &go_fuse.InHeader{
			NodeId: go_fuse.FUSE_ROOT_ID,
		}, "directory", &entryOut))
		require.Equal(t, go_fuse.EntryOut{
			NodeId: 456,
			Attr: go_fuse.Attr{
				Mode: go_fuse.S_IFDIR | 0700,
				Ino:  456,
			},
		}, entryOut)

		// Calls to EntryNotify() should be forwarded to the
		// underlying server in unmodified form.
		mockServerCallbacks.EXPECT().EntryNotify(uint64(456), "hello")
		serverCallbacks.EntryNotify(456, "hello")

		// Once the kernel requests that the directory is
		// forgotten, there is no longer any need for calling
		// into the kernel to forget directory entries.
		rfs.Forget(456, 1)
		serverCallbacks.EntryNotify(456, "hello")
	})
}

// TODO: Add testing coverage for other calls as well.
