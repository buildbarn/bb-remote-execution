package virtual_test

import (
	"math"
	"testing"

	"github.com/buildbarn/bb-remote-execution/pkg/filesystem/virtual"
	"github.com/stretchr/testify/require"
)

func TestByteRangeLockSet(t *testing.T) {
	var ls virtual.ByteRangeLockSet[rune]
	ls.Initialize()

	// Create two locks that are disjoint.
	require.Equal(t, 1, ls.Set(&virtual.ByteRangeLock[rune]{
		Start: 0,
		End:   10,
		Owner: 'A',
		Type:  virtual.ByteRangeLockTypeLockedExclusive,
	}))

	require.Equal(t, 1, ls.Set(&virtual.ByteRangeLock[rune]{
		Start: 20,
		End:   30,
		Owner: 'A',
		Type:  virtual.ByteRangeLockTypeLockedExclusive,
	}))

	// Placing a third lock in between should cause the locks to be
	// merged into one.
	require.Equal(t, -1, ls.Set(&virtual.ByteRangeLock[rune]{
		Start: 10,
		End:   20,
		Owner: 'A',
		Type:  virtual.ByteRangeLockTypeLockedExclusive,
	}))

	// Downgrading a range in the middle to a shared lock should
	// cause the exclusive lock to be split and a new shared lock to
	// be inserted in between.
	require.Equal(t, 2, ls.Set(&virtual.ByteRangeLock[rune]{
		Start: 10,
		End:   20,
		Owner: 'A',
		Type:  virtual.ByteRangeLockTypeLockedShared,
	}))

	// Now that locks have been set up, check whether querying locks
	// through Test() works as expected.
	require.Equal(
		t,
		&virtual.ByteRangeLock[rune]{
			Start: 10,
			End:   20,
			Owner: 'A',
			Type:  virtual.ByteRangeLockTypeLockedShared,
		},
		ls.Test(&virtual.ByteRangeLock[rune]{
			Start: 14,
			End:   16,
			Owner: 'B',
			Type:  virtual.ByteRangeLockTypeLockedExclusive,
		}))
	require.Equal(
		t,
		&virtual.ByteRangeLock[rune]{
			Start: 0,
			End:   10,
			Owner: 'A',
			Type:  virtual.ByteRangeLockTypeLockedExclusive,
		},
		ls.Test(&virtual.ByteRangeLock[rune]{
			Start: 8,
			End:   12,
			Owner: 'B',
			Type:  virtual.ByteRangeLockTypeLockedShared,
		}))
	require.Nil(t, ls.Test(&virtual.ByteRangeLock[rune]{
		Start: 14,
		End:   16,
		Owner: 'B',
		Type:  virtual.ByteRangeLockTypeLockedShared,
	}))
	require.Nil(t, ls.Test(&virtual.ByteRangeLock[rune]{
		Start: 14,
		End:   16,
		Owner: 'A',
		Type:  virtual.ByteRangeLockTypeLockedExclusive,
	}))

	// Fully unlocking the file should cause all locks to be
	// dropped.
	require.Equal(t, -3, ls.Set(&virtual.ByteRangeLock[rune]{
		Start: 0,
		End:   math.MaxUint64,
		Owner: 'A',
		Type:  virtual.ByteRangeLockTypeUnlocked,
	}))

	require.Equal(t, 0, ls.Set(&virtual.ByteRangeLock[rune]{
		Start: 0,
		End:   math.MaxUint64,
		Owner: 'A',
		Type:  virtual.ByteRangeLockTypeUnlocked,
	}))
}
