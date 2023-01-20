package blobstore_test

import (
	"context"
	"io"
	"testing"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-remote-execution/internal/mock"
	"github.com/buildbarn/bb-remote-execution/pkg/blobstore"
	"github.com/buildbarn/bb-storage/pkg/blobstore/buffer"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/testutil"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
)

func TestSuspendingBlobAccess(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	baseBlobAccess := mock.NewMockBlobAccess(ctrl)
	suspendable := mock.NewMockSuspendable(ctrl)
	blobAccess := blobstore.NewSuspendingBlobAccess(baseBlobAccess, suspendable)

	exampleDigest := digest.MustNewDigest("hello", remoteexecution.DigestFunction_MD5, "8b1a9953c4611296a827abf8c47804d7", 5)
	exampleInstanceName := digest.MustNewInstanceName("hello")

	t.Run("Get", func(t *testing.T) {
		r := mock.NewMockReadCloser(ctrl)
		gomock.InOrder(
			suspendable.EXPECT().Suspend(),
			baseBlobAccess.EXPECT().Get(ctx, exampleDigest).
				Return(buffer.NewCASBufferFromReader(exampleDigest, r, buffer.UserProvided)))

		b := blobAccess.Get(ctx, exampleDigest)

		gomock.InOrder(
			r.EXPECT().Read(gomock.Any()).DoAndReturn(func(p []byte) (int, error) {
				return copy(p, "Hello"), io.EOF
			}),
			r.EXPECT().Close(),
			suspendable.EXPECT().Resume())

		data, err := b.ToByteSlice(1000)
		require.NoError(t, err)
		require.Equal(t, []byte("Hello"), data)
	})

	t.Run("GetFromComposite", func(t *testing.T) {
		llDigest := digest.MustNewDigest("hello", remoteexecution.DigestFunction_MD5, "5b54c0a045f179bcbbbc9abcb8b5cd4c", 2)
		blobSlicer := mock.NewMockBlobSlicer(ctrl)
		r := mock.NewMockReadCloser(ctrl)
		gomock.InOrder(
			suspendable.EXPECT().Suspend(),
			baseBlobAccess.EXPECT().GetFromComposite(ctx, exampleDigest, llDigest, blobSlicer).
				Return(buffer.NewCASBufferFromReader(llDigest, r, buffer.UserProvided)))

		b := blobAccess.GetFromComposite(ctx, exampleDigest, llDigest, blobSlicer)

		gomock.InOrder(
			r.EXPECT().Read(gomock.Any()).DoAndReturn(func(p []byte) (int, error) {
				return copy(p, "ll"), io.EOF
			}),
			r.EXPECT().Close(),
			suspendable.EXPECT().Resume())

		data, err := b.ToByteSlice(1000)
		require.NoError(t, err)
		require.Equal(t, []byte("ll"), data)
	})

	t.Run("Put", func(t *testing.T) {
		gomock.InOrder(
			suspendable.EXPECT().Suspend(),
			baseBlobAccess.EXPECT().Put(ctx, exampleDigest, gomock.Any()).DoAndReturn(
				func(ctx context.Context, digest digest.Digest, b buffer.Buffer) error {
					data, err := b.ToByteSlice(1000)
					require.NoError(t, err)
					require.Equal(t, []byte("Hello"), data)
					return nil
				}),
			suspendable.EXPECT().Resume())

		require.NoError(t, blobAccess.Put(ctx, exampleDigest, buffer.NewValidatedBufferFromByteSlice([]byte("Hello"))))
	})

	t.Run("FindMissing", func(t *testing.T) {
		gomock.InOrder(
			suspendable.EXPECT().Suspend(),
			baseBlobAccess.EXPECT().FindMissing(ctx, digest.EmptySet).Return(digest.EmptySet, nil),
			suspendable.EXPECT().Resume())

		missing, err := blobAccess.FindMissing(ctx, digest.EmptySet)
		require.NoError(t, err)
		require.Equal(t, digest.EmptySet, missing)
	})

	t.Run("GetCapabilities", func(t *testing.T) {
		gomock.InOrder(
			suspendable.EXPECT().Suspend(),
			baseBlobAccess.EXPECT().GetCapabilities(ctx, exampleInstanceName).Return(&remoteexecution.ServerCapabilities{
				CacheCapabilities: &remoteexecution.CacheCapabilities{},
			}, nil),
			suspendable.EXPECT().Resume())

		serverCapabilities, err := blobAccess.GetCapabilities(ctx, exampleInstanceName)
		require.NoError(t, err)
		testutil.RequireEqualProto(t, &remoteexecution.ServerCapabilities{
			CacheCapabilities: &remoteexecution.CacheCapabilities{},
		}, serverCapabilities)
	})
}
