// +build darwin linux

package fuse_test

import (
	"context"
	"testing"

	"github.com/buildbarn/bb-remote-execution/internal/mock"
	"github.com/buildbarn/bb-remote-execution/pkg/filesystem/fuse"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/golang/mock/gomock"
	go_fuse "github.com/hanwen/go-fuse/v2/fuse"
	"github.com/stretchr/testify/require"
)

func TestBlobAccessCASFileFactoryGetContainingDigests(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	contentAddressableStorage := mock.NewMockBlobAccess(ctrl)
	errorLogger := mock.NewMockErrorLogger(ctrl)
	casFileFactory := fuse.NewBlobAccessCASFileFactory(
		ctx,
		contentAddressableStorage,
		errorLogger)

	digest := digest.MustNewDigest("example", "d7ac2672607ba20a44d01d03a6685b24", 400)
	var out go_fuse.Attr
	f := casFileFactory.LookupFile(digest, true, &out)
	require.Equal(t, go_fuse.Attr{
		Mode:  go_fuse.S_IFREG | 0555,
		Ino:   casFileFactory.GetFileInodeNumber(digest, true),
		Size:  400,
		Nlink: fuse.StatelessLeafLinkCount,
	}, out)

	require.Equal(t, digest.ToSingletonSet(), f.GetContainingDigests())
}
