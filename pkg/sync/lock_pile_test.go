package sync_test

import (
	"testing"

	"github.com/buildbarn/bb-remote-execution/internal/mock"
	"github.com/buildbarn/bb-remote-execution/pkg/sync"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
)

func TestLockPile(t *testing.T) {
	ctrl := gomock.NewController(t)

	// Attempt to acquire an initial lock. Because it's the initial
	// lock, it's fine to block.
	l1 := mock.NewMockTryLocker(ctrl)
	l1.EXPECT().Lock()
	lockPile := sync.LockPile{}
	require.True(t, lockPile.Lock(l1))

	// Subsequent lock acquisition attempts should not be blocking,
	// as that could cause deadlocks.
	l2 := mock.NewMockTryLocker(ctrl)
	l3 := mock.NewMockTryLocker(ctrl)
	l2.EXPECT().TryLock().Return(true)
	l3.EXPECT().TryLock().Return(true)
	require.True(t, lockPile.Lock(l2, l3))

	// If a subsequent lock acquisition attempt fails, we should
	// temporarily release all locks and retry acquiring it through
	// blocking.
	l4 := mock.NewMockTryLocker(ctrl)
	l4.EXPECT().TryLock().Return(false)
	l1.EXPECT().Unlock()
	l2.EXPECT().Unlock()
	l3.EXPECT().Unlock()
	l4.EXPECT().Lock()
	l1.EXPECT().TryLock().Return(true)
	l2.EXPECT().TryLock().Return(true)
	l3.EXPECT().TryLock().Return(true)
	require.False(t, lockPile.Lock(l4))

	// Recursively acquiring a lock should have no effect.
	require.True(t, lockPile.Lock(l1))
	lockPile.Unlock(l1)

	// Drop all locks.
	l1.EXPECT().Unlock()
	l2.EXPECT().Unlock()
	l3.EXPECT().Unlock()
	l4.EXPECT().Unlock()
	lockPile.UnlockAll()
}
