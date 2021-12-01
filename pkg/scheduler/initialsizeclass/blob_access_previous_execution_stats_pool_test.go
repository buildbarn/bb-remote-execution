package initialsizeclass_test

import (
	"context"
	"testing"

	"github.com/buildbarn/bb-remote-execution/internal/mock"
	"github.com/buildbarn/bb-remote-execution/pkg/scheduler/initialsizeclass"
	"github.com/buildbarn/bb-storage/pkg/blobstore/buffer"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/proto/iscc"
	"github.com/buildbarn/bb-storage/pkg/testutil"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestBlobAccessPreviousExecutionStatsPool(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	blobAccess := mock.NewMockBlobAccess(ctrl)
	store := initialsizeclass.NewBlobAccessPreviousExecutionStatsStore(blobAccess, 10000)

	t.Run("InitialStorageGetFailure", func(t *testing.T) {
		// Errors should be propagated from the backend.
		blobAccess.EXPECT().Get(gomock.Any(), digest.MustNewDigest("hello", "a8ade48a0fb410f9c315723ef0aca3e3", 123)).
			Return(buffer.NewBufferFromError(status.Error(codes.Internal, "Storage failure")))

		_, err := store.Get(ctx, digest.MustNewDigest("hello", "a8ade48a0fb410f9c315723ef0aca3e3", 123))
		testutil.RequireEqualStatus(t, status.Error(codes.Internal, "Failed to read previous execution stats with digest \"a8ade48a0fb410f9c315723ef0aca3e3-123-hello\": Storage failure"), err)
	})

	t.Run("EmptyMessages", func(t *testing.T) {
		// Reading a number of nonexistent messages should
		// succeed and not trigger any writes against storage.
		blobAccess.EXPECT().Get(gomock.Any(), digest.MustNewDigest("hello", "a8ade48a0fb410f9c315723ef0aca3e3", 123)).
			Return(buffer.NewBufferFromError(status.Error(codes.NotFound, "Blob does not exist")))

		handle1, err := store.Get(ctx, digest.MustNewDigest("hello", "a8ade48a0fb410f9c315723ef0aca3e3", 123))
		require.NoError(t, err)
		testutil.RequireEqualProto(t, &iscc.PreviousExecutionStats{}, handle1.GetPreviousExecutionStats())
		handle1.Release(false)

		blobAccess.EXPECT().Get(gomock.Any(), digest.MustNewDigest("hello", "ad328f7d3be9f12b93ce14e8937a083e", 456)).
			Return(buffer.NewBufferFromError(status.Error(codes.NotFound, "Blob does not exist")))

		handle2, err := store.Get(ctx, digest.MustNewDigest("hello", "ad328f7d3be9f12b93ce14e8937a083e", 456))
		require.NoError(t, err)
		testutil.RequireEqualProto(t, &iscc.PreviousExecutionStats{}, handle2.GetPreviousExecutionStats())
		handle2.Release(false)

		blobAccess.EXPECT().Get(gomock.Any(), digest.MustNewDigest("hello", "4c754f07001495a591b25e486d45b347", 789)).
			Return(buffer.NewBufferFromError(status.Error(codes.NotFound, "Blob does not exist")))

		handle3, err := store.Get(ctx, digest.MustNewDigest("hello", "4c754f07001495a591b25e486d45b347", 789))
		require.NoError(t, err)
		testutil.RequireEqualProto(t, &iscc.PreviousExecutionStats{}, handle3.GetPreviousExecutionStats())
		handle3.Release(false)
	})

	t.Run("ReusingHandle", func(t *testing.T) {
		// Create a handle that is backed by an existing stats
		// message stored in the Initial Size Class Cache.
		blobAccess.EXPECT().Get(gomock.Any(), digest.MustNewDigest("hello", "6467817c5aab2f887b2d88679cc2fd76", 123)).
			Return(buffer.NewProtoBufferFromProto(&iscc.PreviousExecutionStats{
				SizeClasses:     map[uint32]*iscc.PerSizeClassStats{},
				LastSeenFailure: &timestamppb.Timestamp{Seconds: 1620818827},
			}, buffer.UserProvided))

		handle1, err := store.Get(ctx, digest.MustNewDigest("hello", "6467817c5aab2f887b2d88679cc2fd76", 123))
		require.NoError(t, err)
		testutil.RequireEqualProto(t, &iscc.PreviousExecutionStats{
			SizeClasses:     map[uint32]*iscc.PerSizeClassStats{},
			LastSeenFailure: &timestamppb.Timestamp{Seconds: 1620818827},
		}, handle1.GetPreviousExecutionStats())

		// Because the first handle hasn't been released, we can
		// create other handles without causing the first handle
		// to be flushed.
		blobAccess.EXPECT().Get(gomock.Any(), digest.MustNewDigest("hello", "57f48d9268744c949c1103bf0e665e28", 456)).
			Return(buffer.NewBufferFromError(status.Error(codes.NotFound, "Blob does not exist")))

		handle2, err := store.Get(ctx, digest.MustNewDigest("hello", "57f48d9268744c949c1103bf0e665e28", 456))
		require.NoError(t, err)
		testutil.RequireEqualProto(t, &iscc.PreviousExecutionStats{}, handle2.GetPreviousExecutionStats())
		handle2.Release(false)

		// Modify and release the original handle. This should
		// normally cause the next call to Get() to write it...
		handle1.GetPreviousExecutionStats().LastSeenFailure = &timestamppb.Timestamp{Seconds: 1620819007}
		handle1.Release(true)

		// ... except if the next call to Get() requests the
		// same handle. We should simply reuse the original one.
		handle3, err := store.Get(ctx, digest.MustNewDigest("hello", "6467817c5aab2f887b2d88679cc2fd76", 123))
		require.NoError(t, err)
		testutil.RequireEqualProto(t, &iscc.PreviousExecutionStats{
			SizeClasses:     map[uint32]*iscc.PerSizeClassStats{},
			LastSeenFailure: &timestamppb.Timestamp{Seconds: 1620819007},
		}, handle3.GetPreviousExecutionStats())

		// Release it once again. Even though we did not make
		// any changes to it, it's still dirty. This means the
		// next call to Get() should still try to write it.
		handle1.Release(false)

		// Let the next call to Get() write the old handle.
		// Unfortunately, at the same time we see a failure
		// reading the new handle, meaning the write is
		// interrupted.
		blobAccess.EXPECT().Get(gomock.Any(), digest.MustNewDigest("hello", "ee2d29afd9b3e8715e68a709c15a6784", 789)).
			Return(buffer.NewBufferFromError(status.Error(codes.Internal, "Storage failure")))
		blobAccess.EXPECT().Put(gomock.Any(), digest.MustNewDigest("hello", "6467817c5aab2f887b2d88679cc2fd76", 123), gomock.Any()).
			DoAndReturn(func(ctx context.Context, digest digest.Digest, b buffer.Buffer) error {
				<-ctx.Done()
				require.Equal(t, context.Canceled, ctx.Err())
				b.Discard()
				return status.Error(codes.Canceled, "Request canceled")
			})

		_, err = store.Get(ctx, digest.MustNewDigest("hello", "ee2d29afd9b3e8715e68a709c15a6784", 789))
		testutil.RequireEqualStatus(t, status.Error(codes.Internal, "Failed to read previous execution stats with digest \"ee2d29afd9b3e8715e68a709c15a6784-789-hello\": Storage failure"), err)

		// Let's try this again. Except that now the write fails.
		blobAccess.EXPECT().Get(gomock.Any(), digest.MustNewDigest("hello", "e1e6496be3124289bfb7374bbab057bf", 234)).
			DoAndReturn(func(ctx context.Context, digest digest.Digest) buffer.Buffer {
				<-ctx.Done()
				require.Equal(t, context.Canceled, ctx.Err())
				return buffer.NewBufferFromError(status.Error(codes.Canceled, "Request canceled"))
			})
		blobAccess.EXPECT().Put(gomock.Any(), digest.MustNewDigest("hello", "6467817c5aab2f887b2d88679cc2fd76", 123), gomock.Any()).
			DoAndReturn(func(ctx context.Context, digest digest.Digest, b buffer.Buffer) error {
				b.Discard()
				return status.Error(codes.Internal, "Storage failure")
			})

		_, err = store.Get(ctx, digest.MustNewDigest("hello", "e1e6496be3124289bfb7374bbab057bf", 234))
		testutil.RequireEqualStatus(t, status.Error(codes.Internal, "Failed to write previous execution stats with digest \"6467817c5aab2f887b2d88679cc2fd76-123-hello\": Storage failure"), err)

		// Now we let both the read and write succeed.
		blobAccess.EXPECT().Get(gomock.Any(), digest.MustNewDigest("hello", "e1e6496be3124289bfb7374bbab057bf", 345)).
			Return(buffer.NewBufferFromError(status.Error(codes.NotFound, "Blob does not exist")))
		blobAccess.EXPECT().Put(gomock.Any(), digest.MustNewDigest("hello", "6467817c5aab2f887b2d88679cc2fd76", 123), gomock.Any()).
			DoAndReturn(func(ctx context.Context, digest digest.Digest, b buffer.Buffer) error {
				m, err := b.ToProto(&iscc.PreviousExecutionStats{}, 10000)
				require.NoError(t, err)
				testutil.RequireEqualProto(t, &iscc.PreviousExecutionStats{
					SizeClasses:     map[uint32]*iscc.PerSizeClassStats{},
					LastSeenFailure: &timestamppb.Timestamp{Seconds: 1620819007},
				}, m)
				return nil
			})

		handle4, err := store.Get(ctx, digest.MustNewDigest("hello", "e1e6496be3124289bfb7374bbab057bf", 345))
		require.NoError(t, err)

		handle4.Release(false)

		// With the write having completed successfully, future
		// attempts to access the store should no longer try to
		// write the released handle.
		blobAccess.EXPECT().Get(gomock.Any(), digest.MustNewDigest("hello", "b3edf9adbbd9cbfc2673c84cd03e5598", 567)).
			Return(buffer.NewBufferFromError(status.Error(codes.NotFound, "Blob does not exist")))

		handle5, err := store.Get(ctx, digest.MustNewDigest("hello", "b3edf9adbbd9cbfc2673c84cd03e5598", 567))
		require.NoError(t, err)

		handle5.Release(false)
	})
}
