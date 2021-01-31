// +build darwin linux

package fuse_test

import (
	"context"
	"testing"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-remote-execution/internal/mock"
	"github.com/buildbarn/bb-remote-execution/pkg/filesystem/fuse"
	"github.com/buildbarn/bb-remote-execution/pkg/proto/remoteoutputservice"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/testutil"
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

func TestBlobAccessCASFileFactoryGetOutputServiceFileStatus(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	contentAddressableStorage := mock.NewMockBlobAccess(ctrl)
	errorLogger := mock.NewMockErrorLogger(ctrl)
	casFileFactory := fuse.NewBlobAccessCASFileFactory(
		ctx,
		contentAddressableStorage,
		errorLogger)

	digest := digest.MustNewDigest("example", "8b1a9953c4611296a827abf8c47804d7", 123)
	var out go_fuse.Attr
	f := casFileFactory.LookupFile(digest, false, &out)
	require.Equal(t, go_fuse.Attr{
		Mode:  go_fuse.S_IFREG | 0444,
		Ino:   casFileFactory.GetFileInodeNumber(digest, false),
		Size:  123,
		Nlink: fuse.StatelessLeafLinkCount,
	}, out)

	// When the provided digest.Function is nil, we should only
	// report that this is a file.
	fileStatus, err := f.GetOutputServiceFileStatus(nil)
	require.NoError(t, err)
	testutil.RequireEqualProto(t, &remoteoutputservice.FileStatus{
		FileType: &remoteoutputservice.FileStatus_File_{
			File: &remoteoutputservice.FileStatus_File{},
		},
	}, fileStatus)

	// When the provided digest.Function is set, we should return
	// the digest of the file as well. There is no need to perform
	// any I/O, as the digest is already embedded in the file.
	digestFunction := digest.GetDigestFunction()
	fileStatus, err = f.GetOutputServiceFileStatus(&digestFunction)
	require.NoError(t, err)
	testutil.RequireEqualProto(t, &remoteoutputservice.FileStatus{
		FileType: &remoteoutputservice.FileStatus_File_{
			File: &remoteoutputservice.FileStatus_File{
				Digest: &remoteexecution.Digest{
					Hash:      "8b1a9953c4611296a827abf8c47804d7",
					SizeBytes: 123,
				},
			},
		},
	}, fileStatus)
}
