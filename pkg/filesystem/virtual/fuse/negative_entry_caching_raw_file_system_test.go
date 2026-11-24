//go:build freebsd || linux
// +build freebsd linux

package fuse_test

import (
	"testing"

	"github.com/buildbarn/bb-remote-execution/internal/mock"
	"github.com/buildbarn/bb-remote-execution/pkg/filesystem/virtual/fuse"
	go_fuse "github.com/hanwen/go-fuse/v2/fuse"
	"github.com/stretchr/testify/require"

	"go.uber.org/mock/gomock"
)

func TestNegativeEntryCachingRawFileSystemLookup(t *testing.T) {
	ctrl := gomock.NewController(t)

	base := mock.NewMockRawFileSystem(ctrl)
	rfs := fuse.NewNegativeEntryCachingRawFileSystem(base)

	t.Run("Missing", func(t *testing.T) {
		// A name that does not exist should yield a successful
		// reply with node ID zero, retaining the entry validity
		// that was filled in beforehand.
		base.EXPECT().Lookup(nil, &go_fuse.InHeader{NodeId: go_fuse.FUSE_ROOT_ID}, "missing", gomock.Any()).
			Return(go_fuse.ENOENT)

		out := go_fuse.EntryOut{EntryValid: 60}
		require.Equal(t, go_fuse.OK, rfs.Lookup(nil, &go_fuse.InHeader{NodeId: go_fuse.FUSE_ROOT_ID}, "missing", &out))
		require.Equal(t, go_fuse.EntryOut{EntryValid: 60}, out)
	})

	t.Run("Found", func(t *testing.T) {
		// Successful lookups should be left untouched.
		base.EXPECT().Lookup(nil, &go_fuse.InHeader{NodeId: go_fuse.FUSE_ROOT_ID}, "found", gomock.Any()).
			DoAndReturn(func(cancel <-chan struct{}, header *go_fuse.InHeader, name string, out *go_fuse.EntryOut) go_fuse.Status {
				out.NodeId = 123
				return go_fuse.OK
			})

		var out go_fuse.EntryOut
		require.Equal(t, go_fuse.OK, rfs.Lookup(nil, &go_fuse.InHeader{NodeId: go_fuse.FUSE_ROOT_ID}, "found", &out))
		require.Equal(t, uint64(123), out.NodeId)
	})

	t.Run("OtherError", func(t *testing.T) {
		// Errors other than ENOENT should be propagated.
		base.EXPECT().Lookup(nil, &go_fuse.InHeader{NodeId: go_fuse.FUSE_ROOT_ID}, "denied", gomock.Any()).
			Return(go_fuse.EACCES)

		var out go_fuse.EntryOut
		require.Equal(t, go_fuse.EACCES, rfs.Lookup(nil, &go_fuse.InHeader{NodeId: go_fuse.FUSE_ROOT_ID}, "denied", &out))
	})
}
