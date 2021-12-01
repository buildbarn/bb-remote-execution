//go:build darwin || linux
// +build darwin linux

package fuse_test

import (
	"testing"
	"time"

	"github.com/buildbarn/bb-remote-execution/internal/mock"
	"github.com/buildbarn/bb-remote-execution/pkg/filesystem/fuse"
	"github.com/golang/mock/gomock"
	go_fuse "github.com/hanwen/go-fuse/v2/fuse"
	"github.com/stretchr/testify/require"
)

func TestDefaultAttributesInjectingRawFileSystem(t *testing.T) {
	ctrl := gomock.NewController(t)

	base := mock.NewMockRawFileSystem(ctrl)
	rfs := fuse.NewDefaultAttributesInjectingRawFileSystem(
		base,
		time.Minute+time.Second/2,
		time.Minute/2+time.Second/4,
		&go_fuse.Attr{
			Atime: 1596207097,
			Mtime: 1596207531,
		})

	t.Run("Lookup", func(t *testing.T) {
		// Lookup() is an example of an operation that returns
		// an EntryOut through an output parameter.
		base.EXPECT().Lookup(
			nil,
			&go_fuse.InHeader{NodeId: 5},
			"hello",
			gomock.Any(),
		).DoAndReturn(func(cancel <-chan struct{}, header *go_fuse.InHeader, name string, out *go_fuse.EntryOut) go_fuse.Status {
			out.NodeId = 6
			out.Ino = 6
			out.Size = 12
			out.Mode = go_fuse.S_IFLNK | 0o777
			out.Nlink = 1
			out.Mtime = 123
			return go_fuse.OK
		})

		var entryOut go_fuse.EntryOut
		require.Equal(
			t,
			go_fuse.OK,
			rfs.Lookup(nil, &go_fuse.InHeader{NodeId: 5}, "hello", &entryOut))
		require.Equal(
			t,
			go_fuse.EntryOut{
				NodeId:         6,
				EntryValid:     60,
				EntryValidNsec: 500000000,
				AttrValid:      30,
				AttrValidNsec:  250000000,
				Attr: go_fuse.Attr{
					Ino:   6,
					Size:  12,
					Mode:  go_fuse.S_IFLNK | 0o777,
					Nlink: 1,
					Atime: 1596207097,
					Mtime: 123,
				},
			},
			entryOut)
	})

	t.Run("GetAttr", func(t *testing.T) {
		// GetAttr() is an example of an operation that returns
		// an AttrOut through an output parameter.
		base.EXPECT().GetAttr(
			nil,
			&go_fuse.GetAttrIn{InHeader: go_fuse.InHeader{NodeId: 5}},
			gomock.Any(),
		).DoAndReturn(func(cancel <-chan struct{}, input *go_fuse.GetAttrIn, out *go_fuse.AttrOut) go_fuse.Status {
			out.Ino = 6
			out.Size = 12
			out.Mode = go_fuse.S_IFLNK | 0o777
			out.Nlink = 1
			out.Mtime = 123
			return go_fuse.OK
		})

		var attrOut go_fuse.AttrOut
		require.Equal(
			t,
			go_fuse.OK,
			rfs.GetAttr(nil, &go_fuse.GetAttrIn{InHeader: go_fuse.InHeader{NodeId: 5}}, &attrOut))
		require.Equal(
			t,
			go_fuse.AttrOut{
				AttrValid:     30,
				AttrValidNsec: 250000000,
				Attr: go_fuse.Attr{
					Ino:   6,
					Size:  12,
					Mode:  go_fuse.S_IFLNK | 0o777,
					Nlink: 1,
					Atime: 1596207097,
					Mtime: 123,
				},
			},
			attrOut)
	})

	t.Run("ReadDirPlus", func(t *testing.T) {
		// ReadDirPlus() returns EntryOut objects through a
		// separate DirEntryList handle.
		entryList := mock.NewMockReadDirPlusEntryList(ctrl)
		base.EXPECT().ReadDirPlus(
			nil,
			&go_fuse.ReadIn{InHeader: go_fuse.InHeader{NodeId: 5}},
			gomock.Any(),
		).DoAndReturn(func(cancel <-chan struct{}, input *go_fuse.ReadIn, out go_fuse.ReadDirPlusEntryList) go_fuse.Status {
			e := out.AddDirLookupEntry(go_fuse.DirEntry{
				Mode: go_fuse.S_IFLNK,
				Name: "symlink",
				Ino:  6,
			})
			e.NodeId = 6
			e.Ino = 6
			e.Size = 12
			e.Mode = go_fuse.S_IFLNK | 0o777
			e.Nlink = 1
			e.Mtime = 123

			e = out.AddDirLookupEntry(go_fuse.DirEntry{
				Mode: go_fuse.S_IFREG,
				Name: "file",
				Ino:  7,
			})
			e.NodeId = 7
			e.Ino = 7
			e.Size = 42
			e.Mode = go_fuse.S_IFREG | 0o644
			e.Nlink = 2
			e.Mtime = 123

			require.Nil(t, out.AddDirLookupEntry(go_fuse.DirEntry{
				Mode: go_fuse.S_IFDIR,
				Name: "directory",
				Ino:  8,
			}))
			return go_fuse.OK
		})
		var entry1 go_fuse.EntryOut
		entryList.EXPECT().AddDirLookupEntry(go_fuse.DirEntry{
			Mode: go_fuse.S_IFLNK,
			Name: "symlink",
			Ino:  6,
		}).Return(&entry1)
		var entry2 go_fuse.EntryOut
		entryList.EXPECT().AddDirLookupEntry(go_fuse.DirEntry{
			Mode: go_fuse.S_IFREG,
			Name: "file",
			Ino:  7,
		}).Return(&entry2)
		entryList.EXPECT().AddDirLookupEntry(go_fuse.DirEntry{
			Mode: go_fuse.S_IFDIR,
			Name: "directory",
			Ino:  8,
		})

		require.Equal(
			t,
			go_fuse.OK,
			rfs.ReadDirPlus(nil, &go_fuse.ReadIn{InHeader: go_fuse.InHeader{NodeId: 5}}, entryList))
		require.Equal(
			t,
			go_fuse.EntryOut{
				NodeId:         6,
				EntryValid:     60,
				EntryValidNsec: 500000000,
				AttrValid:      30,
				AttrValidNsec:  250000000,
				Attr: go_fuse.Attr{
					Ino:   6,
					Size:  12,
					Mode:  go_fuse.S_IFLNK | 0o777,
					Nlink: 1,
					Atime: 1596207097,
					Mtime: 123,
				},
			},
			entry1)
		require.Equal(
			t,
			go_fuse.EntryOut{
				NodeId:         7,
				EntryValid:     60,
				EntryValidNsec: 500000000,
				AttrValid:      30,
				AttrValidNsec:  250000000,
				Attr: go_fuse.Attr{
					Ino:   7,
					Size:  42,
					Mode:  go_fuse.S_IFREG | 0o644,
					Nlink: 2,
					Atime: 1596207097,
					Mtime: 123,
				},
			},
			entry2)
	})
}
