package virtual_test

import (
	"context"
	"testing"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-remote-execution/internal/mock"
	"github.com/buildbarn/bb-remote-execution/pkg/filesystem/virtual"
	"github.com/buildbarn/bb-remote-execution/pkg/proto/outputpathpersistency"
	"github.com/buildbarn/bb-remote-execution/pkg/proto/remoteoutputservice"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/filesystem"
	"github.com/buildbarn/bb-storage/pkg/filesystem/path"
	"github.com/buildbarn/bb-storage/pkg/testutil"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
)

const blobAccessCASFileFactoryAttributesMask = virtual.AttributesMaskChangeID |
	virtual.AttributesMaskFileType |
	virtual.AttributesMaskPermissions |
	virtual.AttributesMaskSizeBytes

func TestBlobAccessCASFileFactoryVirtualSeek(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	contentAddressableStorage := mock.NewMockBlobAccess(ctrl)
	errorLogger := mock.NewMockErrorLogger(ctrl)
	casFileFactory := virtual.NewBlobAccessCASFileFactory(
		ctx,
		contentAddressableStorage,
		errorLogger)

	digest := digest.MustNewDigest("example", remoteexecution.DigestFunction_MD5, "8b1a9953c4611296a827abf8c47804d7", 123)
	f := casFileFactory.LookupFile(digest, false)
	var out virtual.Attributes
	f.VirtualGetAttributes(ctx, blobAccessCASFileFactoryAttributesMask, &out)
	require.Equal(
		t,
		(&virtual.Attributes{}).
			SetChangeID(0).
			SetFileType(filesystem.FileTypeRegularFile).
			SetPermissions(virtual.PermissionsRead).
			SetSizeBytes(123),
		&out)

	t.Run("SEEK_DATA", func(t *testing.T) {
		offset, s := f.VirtualSeek(0, filesystem.Data)
		require.Equal(t, virtual.StatusOK, s)
		require.Equal(t, uint64(0), *offset)

		offset, s = f.VirtualSeek(122, filesystem.Data)
		require.Equal(t, virtual.StatusOK, s)
		require.Equal(t, uint64(122), *offset)

		_, s = f.VirtualSeek(123, filesystem.Data)
		require.Equal(t, virtual.StatusErrNXIO, s)
	})

	t.Run("SEEK_HOLE", func(t *testing.T) {
		offset, s := f.VirtualSeek(0, filesystem.Hole)
		require.Equal(t, virtual.StatusOK, s)
		require.Equal(t, uint64(123), *offset)

		offset, s = f.VirtualSeek(122, filesystem.Hole)
		require.Equal(t, virtual.StatusOK, s)
		require.Equal(t, uint64(123), *offset)

		_, s = f.VirtualSeek(123, filesystem.Hole)
		require.Equal(t, virtual.StatusErrNXIO, s)
	})
}

func TestBlobAccessCASFileFactoryGetContainingDigests(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	contentAddressableStorage := mock.NewMockBlobAccess(ctrl)
	errorLogger := mock.NewMockErrorLogger(ctrl)
	casFileFactory := virtual.NewBlobAccessCASFileFactory(
		ctx,
		contentAddressableStorage,
		errorLogger)

	digest := digest.MustNewDigest("example", remoteexecution.DigestFunction_MD5, "d7ac2672607ba20a44d01d03a6685b24", 400)
	f := casFileFactory.LookupFile(digest, true)
	var out virtual.Attributes
	f.VirtualGetAttributes(ctx, blobAccessCASFileFactoryAttributesMask, &out)
	require.Equal(
		t,
		(&virtual.Attributes{}).
			SetChangeID(0).
			SetFileType(filesystem.FileTypeRegularFile).
			SetPermissions(virtual.PermissionsRead|virtual.PermissionsExecute).
			SetSizeBytes(400),
		&out)

	require.Equal(t, digest.ToSingletonSet(), f.GetContainingDigests())
}

func TestBlobAccessCASFileFactoryGetOutputServiceFileStatus(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	contentAddressableStorage := mock.NewMockBlobAccess(ctrl)
	errorLogger := mock.NewMockErrorLogger(ctrl)
	casFileFactory := virtual.NewBlobAccessCASFileFactory(
		ctx,
		contentAddressableStorage,
		errorLogger)

	digest := digest.MustNewDigest("example", remoteexecution.DigestFunction_MD5, "8b1a9953c4611296a827abf8c47804d7", 123)
	f := casFileFactory.LookupFile(digest, false)
	var out virtual.Attributes
	f.VirtualGetAttributes(ctx, blobAccessCASFileFactoryAttributesMask, &out)
	require.Equal(
		t,
		(&virtual.Attributes{}).
			SetChangeID(0).
			SetFileType(filesystem.FileTypeRegularFile).
			SetPermissions(virtual.PermissionsRead).
			SetSizeBytes(123),
		&out)

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

func TestBlobAccessCASFileFactoryAppendOutputPathPersistencyDirectoryNode(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	contentAddressableStorage := mock.NewMockBlobAccess(ctrl)
	errorLogger := mock.NewMockErrorLogger(ctrl)
	casFileFactory := virtual.NewBlobAccessCASFileFactory(
		ctx,
		contentAddressableStorage,
		errorLogger)

	digest1 := digest.MustNewDigest("example", remoteexecution.DigestFunction_MD5, "8b1a9953c4611296a827abf8c47804d7", 123)
	f1 := casFileFactory.LookupFile(digest1, false)
	var out1 virtual.Attributes
	f1.VirtualGetAttributes(ctx, blobAccessCASFileFactoryAttributesMask, &out1)
	require.Equal(
		t,
		(&virtual.Attributes{}).
			SetChangeID(0).
			SetFileType(filesystem.FileTypeRegularFile).
			SetPermissions(virtual.PermissionsRead).
			SetSizeBytes(123),
		&out1)

	digest2 := digest.MustNewDigest("example", remoteexecution.DigestFunction_MD5, "0282d25bf4aefdb9cb50ccc78d974f0a", 456)
	f2 := casFileFactory.LookupFile(digest2, true)
	var out2 virtual.Attributes
	f2.VirtualGetAttributes(ctx, blobAccessCASFileFactoryAttributesMask, &out2)
	require.Equal(
		t,
		(&virtual.Attributes{}).
			SetChangeID(0).
			SetFileType(filesystem.FileTypeRegularFile).
			SetPermissions(virtual.PermissionsRead|virtual.PermissionsExecute).
			SetSizeBytes(456),
		&out2)

	var directory outputpathpersistency.Directory
	f1.AppendOutputPathPersistencyDirectoryNode(&directory, path.MustNewComponent("hello"))
	f2.AppendOutputPathPersistencyDirectoryNode(&directory, path.MustNewComponent("world"))
	testutil.RequireEqualProto(t, &outputpathpersistency.Directory{
		Files: []*remoteexecution.FileNode{
			{
				Name: "hello",
				Digest: &remoteexecution.Digest{
					Hash:      "8b1a9953c4611296a827abf8c47804d7",
					SizeBytes: 123,
				},
				IsExecutable: false,
			},
			{
				Name: "world",
				Digest: &remoteexecution.Digest{
					Hash:      "0282d25bf4aefdb9cb50ccc78d974f0a",
					SizeBytes: 456,
				},
				IsExecutable: true,
			},
		},
	}, &directory)
}
