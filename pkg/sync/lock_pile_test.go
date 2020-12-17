package sync_test

import (
	"testing"

	"github.com/buildbarn/bb-remote-execution/pkg/sync"
	"github.com/stretchr/testify/require"
)

type simpleLock struct {
	t              *testing.T
	operationCount int
}

func (s *simpleLock) Lock() {
	require.Equal(s.t, 0, s.operationCount%2)
	s.operationCount++
}

func (s *simpleLock) Unlock() {
	require.Equal(s.t, 1, s.operationCount%2)
	s.operationCount++
}

func TestLockPile(t *testing.T) {
	locks := [...]simpleLock{
		{t: t},
		{t: t},
		{t: t},
		{t: t},
	}
	lockPile := sync.LockPile{}

	// Initial set of locks should always be acquired without any
	// deadlock avoidance.
	require.True(t, lockPile.Lock(&locks[0], &locks[1]))
	require.Equal(t, 1, locks[0].operationCount)
	require.Equal(t, 1, locks[1].operationCount)
	require.Equal(t, 0, locks[2].operationCount)
	require.Equal(t, 0, locks[3].operationCount)

	// Additional locks have a higher memory address, meaning they
	// are further on the total order. They can also be acquired
	// without deadlock avoidance.
	require.True(t, lockPile.Lock(&locks[2], &locks[3]))
	require.Equal(t, 1, locks[0].operationCount)
	require.Equal(t, 1, locks[1].operationCount)
	require.Equal(t, 1, locks[2].operationCount)
	require.Equal(t, 1, locks[3].operationCount)

	// Unlocking a single lock should only affect one lock.
	lockPile.Unlock(&locks[1])
	require.Equal(t, 1, locks[0].operationCount)
	require.Equal(t, 2, locks[1].operationCount)
	require.Equal(t, 1, locks[2].operationCount)
	require.Equal(t, 1, locks[3].operationCount)

	// Recursively picking up an existing lock should have no
	// effect. The underlying lock shouldn't be reacquired.
	require.True(t, lockPile.Lock(&locks[2]))
	require.Equal(t, 1, locks[0].operationCount)
	require.Equal(t, 2, locks[1].operationCount)
	require.Equal(t, 1, locks[2].operationCount)
	require.Equal(t, 1, locks[3].operationCount)

	// Similarly, unlocking a recursively held lock should also have
	// no effect. The underlying lock should not be released yet.
	lockPile.Unlock(&locks[2])
	require.Equal(t, 1, locks[0].operationCount)
	require.Equal(t, 2, locks[1].operationCount)
	require.Equal(t, 1, locks[2].operationCount)
	require.Equal(t, 1, locks[3].operationCount)

	// Picking up a lock with a lower memory address  would cause a
	// deadlock, which is why Lock() temporarily drops some of the
	// other locks to be able to progress. The function should
	// return false to indicate this.
	require.False(t, lockPile.Lock(&locks[1]))
	require.Equal(t, 1, locks[0].operationCount)
	require.Equal(t, 3, locks[1].operationCount)
	require.Equal(t, 3, locks[2].operationCount)
	require.Equal(t, 3, locks[3].operationCount)

	// Termination.
	lockPile.UnlockAll()
	require.Equal(t, 2, locks[0].operationCount)
	require.Equal(t, 4, locks[1].operationCount)
	require.Equal(t, 4, locks[2].operationCount)
	require.Equal(t, 4, locks[3].operationCount)
}
