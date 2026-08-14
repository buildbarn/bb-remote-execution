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
	"github.com/buildbarn/bb-storage/pkg/blobstore/chunk"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/filesystem"
	"github.com/buildbarn/bb-storage/pkg/filesystem/path"
	"github.com/buildbarn/bb-storage/pkg/testutil"
	"github.com/stretchr/testify/require"

	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/anypb"

	"go.uber.org/mock/gomock"
)

const blobAccessCASFileFactoryAttributesMask = virtual.AttributesMaskChangeID |
	virtual.AttributesMaskFileType |
	virtual.AttributesMaskHasNamedAttributes |
	virtual.AttributesMaskIsInNamedAttributeDirectory |
	virtual.AttributesMaskPermissions |
	virtual.AttributesMaskSizeBytes

func TestBlobAccessCASFileFactoryVirtualRead(t *testing.T) {
	// CAS-backed files report storage read failures by logging the
	// error and returning EIO. If the failure is due to the blob being
	// missing from storage, the logged error is converted to
	// FAILED_PRECONDITION, so that the client can be told to re-upload
	// the blob.
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	chunkBytesReader := mock.NewMockReader[[]byte](ctrl)
	chunkMappingFetcher := mock.NewMockMappingFetcher(ctrl)
	cdcParametersFetcher := mock.NewMockCDCParametersFetcher(ctrl)
	errorLogger := mock.NewMockErrorLogger(ctrl)
	casFileFactory := virtual.NewBlobAccessCASFileFactory(
		ctx,
		chunkBytesReader,
		chunkMappingFetcher,
		cdcParametersFetcher,
		errorLogger,
	)

	helloWorldDigest := digest.MustNewDigest("example", remoteexecution.DigestFunction_MD5, "5eb63bbbe01eeed093cb22bb8f5acdc3", 11)
	chunkDigestHello := digest.MustNewDigest("example", remoteexecution.DigestFunction_MD5, "5d41402abc4b2a76b9719d911017c592", 5)
	chunkDigestWorld := digest.MustNewDigest("example", remoteexecution.DigestFunction_MD5, "b7913aa15c43be7d534b4eec6e99e8a0", 6)
	mapping, err := chunk.NewMappingFromDigests([]digest.Digest{chunkDigestHello, chunkDigestWorld}, 11, true)
	require.NoError(t, err)
	params := &remoteexecution.RepMaxCdcParams{MinChunkSizeBytes: 1, HorizonSizeBytes: 2}
	f := casFileFactory.LookupFile(helloWorldDigest, false, nil)

	t.Run("Success", func(t *testing.T) {
		cdcParametersFetcher.EXPECT().FetchCDCParameters(gomock.Any(), helloWorldDigest.GetInstanceName()).Return(params, nil)
		chunkMappingFetcher.EXPECT().FetchChunkMapping(gomock.Any(), helloWorldDigest).Return(mapping, nil)
		gomock.InOrder(
			chunkBytesReader.EXPECT().Read(gomock.Any(), chunkDigestHello).Return([]byte("hello"), nil),
			chunkBytesReader.EXPECT().Read(gomock.Any(), chunkDigestWorld).Return([]byte(" world"), nil),
		)

		buf := make([]byte, 11)
		n, eof, s := f.VirtualRead(ctx, buf, 0)
		require.Equal(t, virtual.StatusOK, s)
		require.Equal(t, 11, n)
		require.True(t, eof)
		require.Equal(t, "hello world", string(buf))
	})

	t.Run("PartialReadFailure", func(t *testing.T) {
		cdcParametersFetcher.EXPECT().FetchCDCParameters(gomock.Any(), helloWorldDigest.GetInstanceName()).Return(params, nil)
		chunkMappingFetcher.EXPECT().FetchChunkMapping(gomock.Any(), helloWorldDigest).Return(mapping, nil)
		chunkBytesReader.EXPECT().Read(gomock.Any(), chunkDigestHello).Return([]byte("hello"), nil)
		chunkBytesReader.EXPECT().Read(gomock.Any(), chunkDigestWorld).Return(nil, status.Error(codes.Unavailable, "Storage backends offline"))
		errorLogger.EXPECT().Log(testutil.EqStatus(t, status.Error(
			codes.Unavailable,
			"Failed to read from 3-5eb63bbbe01eeed093cb22bb8f5acdc3-11-example at offset 0: Read 5 bytes instead of 11 from 3-5eb63bbbe01eeed093cb22bb8f5acdc3-11-example at offset 0: Could not fetch chunk: Storage backends offline",
		)))

		buf := make([]byte, 64)
		n, eof, s := f.VirtualRead(ctx, buf, 0)
		require.Equal(t, virtual.StatusErrIO, s)
		require.Equal(t, 0, n)
		require.False(t, eof)
	})

	t.Run("PartialReadWithoutError", func(t *testing.T) {
		cdcParametersFetcher.EXPECT().FetchCDCParameters(gomock.Any(), helloWorldDigest.GetInstanceName()).Return(params, nil)
		chunkMappingFetcher.EXPECT().FetchChunkMapping(gomock.Any(), helloWorldDigest).Return(mapping, nil)
		chunkBytesReader.EXPECT().Read(gomock.Any(), chunkDigestHello).Return([]byte("hello"), nil)
		chunkBytesReader.EXPECT().Read(gomock.Any(), chunkDigestWorld).Return(nil, nil)
		errorLogger.EXPECT().Log(testutil.EqStatus(t, status.Error(
			codes.Unknown,
			"Failed to read from 3-5eb63bbbe01eeed093cb22bb8f5acdc3-11-example at offset 0: Read 5 bytes instead of 11 from 3-5eb63bbbe01eeed093cb22bb8f5acdc3-11-example at offset 0: EOF",
		)))

		buf := make([]byte, 64)
		n, eof, s := f.VirtualRead(ctx, buf, 0)
		require.Equal(t, virtual.StatusErrIO, s)
		require.Equal(t, 0, n)
		require.False(t, eof)
	})

	t.Run("MissingBlob", func(t *testing.T) {
		cdcParametersFetcher.EXPECT().FetchCDCParameters(gomock.Any(), helloWorldDigest.GetInstanceName()).Return(params, nil)
		chunkMappingFetcher.EXPECT().FetchChunkMapping(gomock.Any(), helloWorldDigest).DoAndReturn(
			func(ctx context.Context, d digest.Digest) (chunk.Mapping, error) {
				return chunk.Mapping{}, status.Error(codes.NotFound, "Blob not found")
			},
		)
		errorLogger.EXPECT().Log(gomock.Any()).DoAndReturn(func(err error) {
			// NotFound errors are converted to FAILED_PRECONDITION,
			// with a PreconditionFailure that names the missing
			// blob, so that the client knows to re-upload it.
			s := status.Convert(err)
			require.Equal(t, codes.FailedPrecondition, s.Code())
			require.Equal(
				t,
				"Failed to read from 3-5eb63bbbe01eeed093cb22bb8f5acdc3-11-example at offset 0: Read 0 bytes instead of 11 from 3-5eb63bbbe01eeed093cb22bb8f5acdc3-11-example at offset 0: Could not fetch chunk mapping: Blob not found",
				s.Message(),
			)
			require.Len(t, s.Details(), 1)
			preconditionFailure, ok := s.Details()[0].(*errdetails.PreconditionFailure)
			require.True(t, ok)
			require.Len(t, preconditionFailure.Violations, 1)
			require.Equal(t, "MISSING", preconditionFailure.Violations[0].Type)
			require.Equal(
				t,
				digest.NewInstanceNamePatcher(helloWorldDigest.GetInstanceName(), digest.EmptyInstanceName).
					PatchDigest(helloWorldDigest).
					GetByteStreamReadPath(remoteexecution.Compressor_IDENTITY),
				preconditionFailure.Violations[0].Subject,
			)
		})

		buf := make([]byte, 64)
		n, eof, s := f.VirtualRead(ctx, buf, 0)
		require.Equal(t, virtual.StatusErrIO, s)
		require.Equal(t, 0, n)
		require.False(t, eof)
	})
}

func TestBlobAccessCASFileFactoryVirtualSeek(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	chunkBytesReader := mock.NewMockReader[[]byte](ctrl)
	chunkMappingFetcher := mock.NewMockMappingFetcher(ctrl)
	cdcParametersFetcher := mock.NewMockCDCParametersFetcher(ctrl)
	errorLogger := mock.NewMockErrorLogger(ctrl)
	casFileFactory := virtual.NewBlobAccessCASFileFactory(
		ctx,
		chunkBytesReader,
		chunkMappingFetcher,
		cdcParametersFetcher,
		errorLogger,
	)

	digest := digest.MustNewDigest("example", remoteexecution.DigestFunction_MD5, "8b1a9953c4611296a827abf8c47804d7", 123)
	f := casFileFactory.LookupFile(digest, false, nil)
	var out virtual.Attributes
	f.VirtualGetAttributes(ctx, blobAccessCASFileFactoryAttributesMask, &out)
	require.Equal(
		t,
		(&virtual.Attributes{}).
			SetChangeID(0).
			SetFileType(filesystem.FileTypeRegularFile).
			SetHasNamedAttributes(false).
			SetIsInNamedAttributeDirectory(false).
			SetPermissions(virtual.PermissionsRead).
			SetSizeBytes(123),
		&out,
	)

	t.Run("SEEK_DATA", func(t *testing.T) {
		offset, s := f.VirtualSeek(ctx, 0, filesystem.Data)
		require.Equal(t, virtual.StatusOK, s)
		require.Equal(t, uint64(0), *offset)

		offset, s = f.VirtualSeek(ctx, 122, filesystem.Data)
		require.Equal(t, virtual.StatusOK, s)
		require.Equal(t, uint64(122), *offset)

		_, s = f.VirtualSeek(ctx, 123, filesystem.Data)
		require.Equal(t, virtual.StatusErrNXIO, s)
	})

	t.Run("SEEK_HOLE", func(t *testing.T) {
		offset, s := f.VirtualSeek(ctx, 0, filesystem.Hole)
		require.Equal(t, virtual.StatusOK, s)
		require.Equal(t, uint64(123), *offset)

		offset, s = f.VirtualSeek(ctx, 122, filesystem.Hole)
		require.Equal(t, virtual.StatusOK, s)
		require.Equal(t, uint64(123), *offset)

		_, s = f.VirtualSeek(ctx, 123, filesystem.Hole)
		require.Equal(t, virtual.StatusErrNXIO, s)
	})
}

func TestBlobAccessCASFileFactoryGetContainingDigests(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	chunkBytesReader := mock.NewMockReader[[]byte](ctrl)
	chunkMappingFetcher := mock.NewMockMappingFetcher(ctrl)
	cdcParametersFetcher := mock.NewMockCDCParametersFetcher(ctrl)
	errorLogger := mock.NewMockErrorLogger(ctrl)
	casFileFactory := virtual.NewBlobAccessCASFileFactory(
		ctx,
		chunkBytesReader,
		chunkMappingFetcher,
		cdcParametersFetcher,
		errorLogger,
	)

	digest := digest.MustNewDigest("example", remoteexecution.DigestFunction_MD5, "d7ac2672607ba20a44d01d03a6685b24", 400)
	f := casFileFactory.LookupFile(digest, true, nil)
	var out virtual.Attributes
	f.VirtualGetAttributes(ctx, blobAccessCASFileFactoryAttributesMask, &out)
	require.Equal(
		t,
		(&virtual.Attributes{}).
			SetChangeID(0).
			SetFileType(filesystem.FileTypeRegularFile).
			SetHasNamedAttributes(false).
			SetIsInNamedAttributeDirectory(false).
			SetPermissions(virtual.PermissionsRead|virtual.PermissionsExecute).
			SetSizeBytes(400),
		&out,
	)

	p := virtual.ApplyGetContainingDigests{}
	require.True(t, f.VirtualApply(&p))
	require.Equal(t, digest.ToSingletonSet(), p.ContainingDigests)
}

func TestBlobAccessCASFileFactoryGetBazelOutputServiceStat(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	chunkBytesReader := mock.NewMockReader[[]byte](ctrl)
	chunkMappingFetcher := mock.NewMockMappingFetcher(ctrl)
	cdcParametersFetcher := mock.NewMockCDCParametersFetcher(ctrl)
	errorLogger := mock.NewMockErrorLogger(ctrl)
	casFileFactory := virtual.NewBlobAccessCASFileFactory(
		ctx,
		chunkBytesReader,
		chunkMappingFetcher,
		cdcParametersFetcher,
		errorLogger,
	)

	digest := digest.MustNewDigest("example", remoteexecution.DigestFunction_MD5, "8b1a9953c4611296a827abf8c47804d7", 123)
	f := casFileFactory.LookupFile(digest, false, nil)
	var out virtual.Attributes
	f.VirtualGetAttributes(ctx, blobAccessCASFileFactoryAttributesMask, &out)
	require.Equal(
		t,
		(&virtual.Attributes{}).
			SetChangeID(0).
			SetFileType(filesystem.FileTypeRegularFile).
			SetHasNamedAttributes(false).
			SetIsInNamedAttributeDirectory(false).
			SetPermissions(virtual.PermissionsRead).
			SetSizeBytes(123),
		&out,
	)

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

	chunkBytesReader := mock.NewMockReader[[]byte](ctrl)
	chunkMappingFetcher := mock.NewMockMappingFetcher(ctrl)
	cdcParametersFetcher := mock.NewMockCDCParametersFetcher(ctrl)
	errorLogger := mock.NewMockErrorLogger(ctrl)
	casFileFactory := virtual.NewBlobAccessCASFileFactory(
		ctx,
		chunkBytesReader,
		chunkMappingFetcher,
		cdcParametersFetcher,
		errorLogger,
	)

	digest1 := digest.MustNewDigest("example", remoteexecution.DigestFunction_MD5, "8b1a9953c4611296a827abf8c47804d7", 123)
	f1 := casFileFactory.LookupFile(digest1, false, nil)
	var out1 virtual.Attributes
	f1.VirtualGetAttributes(ctx, blobAccessCASFileFactoryAttributesMask, &out1)
	require.Equal(
		t,
		(&virtual.Attributes{}).
			SetChangeID(0).
			SetFileType(filesystem.FileTypeRegularFile).
			SetHasNamedAttributes(false).
			SetIsInNamedAttributeDirectory(false).
			SetPermissions(virtual.PermissionsRead).
			SetSizeBytes(123),
		&out1,
	)

	digest2 := digest.MustNewDigest("example", remoteexecution.DigestFunction_MD5, "0282d25bf4aefdb9cb50ccc78d974f0a", 456)
	f2 := casFileFactory.LookupFile(digest2, true, nil)
	var out2 virtual.Attributes
	f2.VirtualGetAttributes(ctx, blobAccessCASFileFactoryAttributesMask, &out2)
	require.Equal(
		t,
		(&virtual.Attributes{}).
			SetChangeID(0).
			SetFileType(filesystem.FileTypeRegularFile).
			SetHasNamedAttributes(false).
			SetIsInNamedAttributeDirectory(false).
			SetPermissions(virtual.PermissionsRead|virtual.PermissionsExecute).
			SetSizeBytes(456),
		&out2,
	)

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
