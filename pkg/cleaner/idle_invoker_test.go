package cleaner_test

import (
	"context"
	"sync"
	"testing"

	"github.com/buildbarn/bb-remote-execution/internal/mock"
	"github.com/buildbarn/bb-remote-execution/pkg/cleaner"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
)

func TestIdleInvoker(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	mockCleaner := mock.NewMockCleaner(ctrl)
	idleInvoker := cleaner.NewIdleInvoker(mockCleaner.Call)

	t.Run("Once", func(t *testing.T) {
		// The cleaner function should be called upon
		// acquisition and release.
		mockCleaner.EXPECT().Call(ctx)
		require.NoError(t, idleInvoker.Acquire(ctx))

		mockCleaner.EXPECT().Call(ctx)
		require.NoError(t, idleInvoker.Release(ctx))
	})

	t.Run("Twice", func(t *testing.T) {
		// The cleaner function should only be called once when
		// acquired initially, and once during the final
		// release.
		mockCleaner.EXPECT().Call(ctx)
		require.NoError(t, idleInvoker.Acquire(ctx))
		require.NoError(t, idleInvoker.Acquire(ctx))

		require.NoError(t, idleInvoker.Release(ctx))
		mockCleaner.EXPECT().Call(ctx)
		require.NoError(t, idleInvoker.Release(ctx))
	})

	t.Run("AcquireFailure", func(t *testing.T) {
		// Cleaning failures upon acquisition should cause us to
		// remain in the initial uninitialized state. This
		// causes cleaning to be retried.
		mockCleaner.EXPECT().Call(ctx).Return(context.DeadlineExceeded)
		require.Equal(t, context.DeadlineExceeded, idleInvoker.Acquire(ctx))

		mockCleaner.EXPECT().Call(ctx)
		require.NoError(t, idleInvoker.Acquire(ctx))

		mockCleaner.EXPECT().Call(ctx)
		require.NoError(t, idleInvoker.Release(ctx))
	})

	t.Run("ReleaseFailure", func(t *testing.T) {
		// Cleaning failures upon release should cause us to
		// still release. The error has to be propagated.
		mockCleaner.EXPECT().Call(ctx)
		require.NoError(t, idleInvoker.Acquire(ctx))

		mockCleaner.EXPECT().Call(ctx).Return(context.DeadlineExceeded)
		require.Equal(t, context.DeadlineExceeded, idleInvoker.Release(ctx))

		mockCleaner.EXPECT().Call(ctx)
		require.NoError(t, idleInvoker.Acquire(ctx))

		mockCleaner.EXPECT().Call(ctx)
		require.NoError(t, idleInvoker.Release(ctx))
	})

	t.Run("Parallel", func(t *testing.T) {
		// The cleaning function should be called exactly once,
		// even when used concurrently.
		var wg sync.WaitGroup
		mockCleaner.EXPECT().Call(ctx)
		wg.Add(100)
		for i := 0; i < 100; i++ {
			go func() {
				require.NoError(t, idleInvoker.Acquire(ctx))
				wg.Done()
			}()
		}
		wg.Wait()

		mockCleaner.EXPECT().Call(ctx)
		wg.Add(100)
		for i := 0; i < 100; i++ {
			go func() {
				require.NoError(t, idleInvoker.Release(ctx))
				wg.Done()
			}()
		}
		wg.Wait()
	})
}
