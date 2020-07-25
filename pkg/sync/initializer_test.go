package sync_test

import (
	"context"
	"sync"
	"testing"

	"github.com/buildbarn/bb-remote-execution/internal/mock"
	re_sync "github.com/buildbarn/bb-remote-execution/pkg/sync"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
)

func TestInitializer(t *testing.T) {
	ctrl := gomock.NewController(t)

	var initializer re_sync.Initializer
	initializationFunc := mock.NewMockInitializationFunc(ctrl)

	t.Run("Once", func(t *testing.T) {
		// The initialization function should be called upon
		// acquisition.
		initializationFunc.EXPECT().Call()
		require.NoError(t, initializer.Acquire(initializationFunc.Call))
		initializer.Release()
	})

	t.Run("Twice", func(t *testing.T) {
		// The initialization function  should only be called
		// once when acquired multiple times.
		initializationFunc.EXPECT().Call()
		require.NoError(t, initializer.Acquire(initializationFunc.Call))
		require.NoError(t, initializer.Acquire(initializationFunc.Call))
		initializer.Release()
		initializer.Release()
	})

	t.Run("Error", func(t *testing.T) {
		// Initialization failures should cause us to remain in
		// the initial uninitialized state. This causes
		// initialization to be retried.
		initializationFunc.EXPECT().Call().Return(context.DeadlineExceeded)
		require.Equal(t, context.DeadlineExceeded, initializer.Acquire(initializationFunc.Call))
		initializationFunc.EXPECT().Call()
		require.NoError(t, initializer.Acquire(initializationFunc.Call))
		initializer.Release()
	})

	t.Run("Parallel", func(t *testing.T) {
		// The initialization function should be called exactly
		// once, even when used concurrently.
		initializationFunc.EXPECT().Call()
		var wg sync.WaitGroup
		wg.Add(100)
		for i := 0; i < 100; i++ {
			go func() {
				require.NoError(t, initializer.Acquire(initializationFunc.Call))
				wg.Done()
			}()
		}
		wg.Wait()
		for i := 0; i < 100; i++ {
			initializer.Release()
		}
	})
}
