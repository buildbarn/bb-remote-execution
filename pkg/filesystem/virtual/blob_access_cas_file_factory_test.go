package virtual_test

import (
	"context"
	"testing"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-remote-execution/internal/mock"
	"github.com/buildbarn/bb-remote-execution/pkg/filesystem/virtual"
	"github.com/buildbarn/bb-remote-execution/pkg/proto/bazeloutputservice"
	bazeloutputservicerev2 "github.com/buildbarn/bb-remote-execution/pkg/proto/bazeloutputservice/rev2"
	"github.com/buildbarn/bb-remote-execution/pkg/proto/outputpathpersistency"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/filesystem"
	"github.com/buildbarn/bb-storage/pkg/filesystem/path"
	"github.com/buildbarn/bb-storage/pkg/testutil"
	"github.com/stretchr/testify/require"

	"google.golang.org/protobuf/types/known/anypb"

	"go.uber.org/mock/gomock"
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
	f := casFileFactory.LookupFile(digest, false, nil)
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
	f := casFileFactory.LookupFile(digest, true, nil)
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

	p := virtual.ApplyGetContainingDigests{}
	require.True(t, f.VirtualApply(&p))
	require.Equal(t, digest.ToSingletonSet(), p.ContainingDigests)
}

func TestBlobAccessCASFileFactoryGetBazelOutputServiceStat(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	contentAddressableStorage := mock.NewMockBlobAccess(ctrl)
	errorLogger := mock.NewMockErrorLogger(ctrl)
	casFileFactory := virtual.NewBlobAccessCASFileFactory(
		ctx,
		contentAddressableStorage,
		errorLogger)

	digest := digest.MustNewDigest("example", remoteexecution.DigestFunction_MD5, "8b1a9953c4611296a827abf8c47804d7", 123)
	f := casFileFactory.LookupFile(digest, false, nil)
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

	// We should return the digest of the file as well. There is no
	// need to perform any I/O, as the digest is already embedded in
	// the file.
	digestFunction := digest.GetDigestFunction()
	p := virtual.ApplyGetBazelOutputServiceStat{
		DigestFunction: &digestFunction,
	}
	require.True(t, f.VirtualApply(&p))
	require.NoError(t, p.Err)
	locator, err := anypb.New(&bazeloutputservicerev2.FileArtifactLocator{
		Digest: &remoteexecution.Digest{
			Hash:      "8b1a9953c4611296a827abf8c47804d7",
			SizeBytes: 123,
		},
	})
	require.NoError(t, err)
	testutil.RequireEqualProto(t, &bazeloutputservice.BatchStatResponse_Stat{
		Type: &bazeloutputservice.BatchStatResponse_Stat_File_{
			File: &bazeloutputservice.BatchStatResponse_Stat_File{
				Locator: locator,
			},
		},
	}, p.Stat)
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
	f1 := casFileFactory.LookupFile(digest1, false, nil)
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
	f2 := casFileFactory.LookupFile(digest2, true, nil)
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
	require.True(t, f1.VirtualApply(&virtual.ApplyAppendOutputPathPersistencyDirectoryNode{
		Directory: &directory,
		Name:      path.MustNewComponent("hello"),
	}))
	require.True(t, f2.VirtualApply(&virtual.ApplyAppendOutputPathPersistencyDirectoryNode{
		Directory: &directory,
		Name:      path.MustNewComponent("world"),
	}))
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
