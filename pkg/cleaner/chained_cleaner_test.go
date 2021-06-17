package cleaner_test

import (
	"context"
	"testing"

	"github.com/buildbarn/bb-remote-execution/internal/mock"
	"github.com/buildbarn/bb-remote-execution/pkg/cleaner"
	"github.com/buildbarn/bb-storage/pkg/testutil"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestChainedCleaner(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	t.Run("Empty", func(t *testing.T) {
		require.NoError(t, cleaner.NewChainedCleaner(nil)(ctx))
	})

	t.Run("SingleFailure", func(t *testing.T) {
		cleaner1 := mock.NewMockCleaner(ctrl)
		cleaner1.EXPECT().Call(ctx).Return(status.Error(codes.Internal, "Failed to clean process table"))

		testutil.RequireEqualStatus(
			t,
			status.Error(codes.Internal, "Failed to clean process table"),
			cleaner.NewChainedCleaner([]cleaner.Cleaner{
				cleaner1.Call,
			})(ctx))
	})

	t.Run("SingleSuccess", func(t *testing.T) {
		cleaner1 := mock.NewMockCleaner(ctrl)
		cleaner1.EXPECT().Call(ctx)

		require.NoError(t, cleaner.NewChainedCleaner([]cleaner.Cleaner{
			cleaner1.Call,
		})(ctx))
	})

	t.Run("MultipleFailure", func(t *testing.T) {
		// When multiple cleaners are provided, they must be
		// invoked in order. Even in case of failures, the
		// remaining cleaners need to be run. The error of the
		// first cleaner is returned.
		cleaner1 := mock.NewMockCleaner(ctrl)
		cleaner2 := mock.NewMockCleaner(ctrl)
		cleaner3 := mock.NewMockCleaner(ctrl)
		gomock.InOrder(
			cleaner1.EXPECT().Call(ctx),
			cleaner2.EXPECT().Call(ctx).Return(status.Error(codes.Internal, "Failed to clean process table")),
			cleaner3.EXPECT().Call(ctx).Return(status.Error(codes.Internal, "Failed to clean directory")))

		testutil.RequireEqualStatus(
			t,
			status.Error(codes.Internal, "Failed to clean process table"),
			cleaner.NewChainedCleaner([]cleaner.Cleaner{
				cleaner1.Call,
				cleaner2.Call,
				cleaner3.Call,
			})(ctx))
	})

	t.Run("MultipleSuccess", func(t *testing.T) {
		cleaner1 := mock.NewMockCleaner(ctrl)
		cleaner2 := mock.NewMockCleaner(ctrl)
		cleaner3 := mock.NewMockCleaner(ctrl)
		gomock.InOrder(
			cleaner1.EXPECT().Call(ctx),
			cleaner2.EXPECT().Call(ctx),
			cleaner3.EXPECT().Call(ctx))

		require.NoError(t, cleaner.NewChainedCleaner([]cleaner.Cleaner{
			cleaner1.Call,
			cleaner2.Call,
			cleaner3.Call,
		})(ctx))
	})
}
