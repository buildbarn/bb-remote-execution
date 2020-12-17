// +build darwin linux

package fuse_test

import (
	"context"
	"crypto/md5"
	"crypto/sha256"
	"testing"

	"github.com/buildbarn/bb-remote-execution/internal/mock"
	"github.com/buildbarn/bb-remote-execution/pkg/filesystem/fuse"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/golang/mock/gomock"
	go_fuse "github.com/hanwen/go-fuse/v2/fuse"
	"github.com/stretchr/testify/require"
)

func TestContentAddressableStorageFileGetXAttr(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	contentAddressableStorage := mock.NewMockBlobAccess(ctrl)
	errorLogger := mock.NewMockErrorLogger(ctrl)
	f := fuse.NewContentAddressableStorageFile(
		ctx,
		contentAddressableStorage,
		errorLogger,
		123,
		digest.MustNewDigest("example", "8b1a9953c4611296a827abf8c47804d7", 123),
		false)

	t.Run("HashSuccess", func(t *testing.T) {
		// Buffer is large enough to fit the hash.
		var buf [md5.Size]byte
		n, s := f.FUSEGetXAttr("user.buildbarn.hash.md5", buf[:])
		require.Equal(t, uint32(md5.Size), n)
		require.Equal(t, go_fuse.OK, s)
		require.Equal(t, [...]byte{
			0x8b, 0x1a, 0x99, 0x53, 0xc4, 0x61, 0x12, 0x96,
			0xa8, 0x27, 0xab, 0xf8, 0xc4, 0x78, 0x04, 0xd7,
		}, buf)
	})

	t.Run("HashTooSmall", func(t *testing.T) {
		// Buffer is too small to fit the hash.
		var buf [md5.Size - 1]byte
		n, s := f.FUSEGetXAttr("user.buildbarn.hash.md5", buf[:])
		require.Equal(t, uint32(md5.Size), n)
		require.Equal(t, go_fuse.ERANGE, s)
	})

	t.Run("HashWrongAlgorithm", func(t *testing.T) {
		// Attempting to obtain the SHA-256 sum for a file that
		// uses MD5 should not return any hash. It isn't known.
		var buf [sha256.Size]byte
		_, s := f.FUSEGetXAttr("user.buildbarn.hash.sha256", buf[:])
		require.Equal(t, go_fuse.ENOATTR, s)
	})
}
