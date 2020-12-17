package sync

import (
	"reflect"
	"sync"
)

type lockHandle struct {
	lock      sync.Locker
	recursion int
}

// LockPile is a list to keep track of locks held by a thread. For every
// lock, it keeps track of a recursion count, allowing locks that don't
// support recursion to be acquired multiple times. The underlying lock
// will only be unlocked if the recursion count reaches zero.
//
// LockPile implements a deadlock avoidance algorithm, ensuring that
// locks are always acquired along a total order. It's not important
// what the actual order is, as long as it is guaranteed to be total.
// This implementation orders locks by numerical memory address.
//
// There are better algorithms for achieving deadlock avoidance, but
// those generally depend on the availability of a TryLock() function,
// which sync.Locker does not support:
//
// https://github.com/golang/go/issues/6123
//
// What LockPile implements is equivalent to the "Ordered" algorithm
// described on Howard Hinnant's page titled "Dining Philosophers
// Rebooted":
//
// https://howardhinnant.github.io/dining_philosophers.html
//
// As the set of locks held can be extended over time, there may be a
// possibility LockPile has to backtrack and temporarily unlock one or
// more locks it held prior to acquiring more. The caller is signalled
// when this happens, so that it may revalidate its state. Depending on
// the state's validity, the caller may either continue or retry.
type LockPile []lockHandle

func (lp *LockPile) insert(newLock sync.Locker, lockedUpTo *int) {
	// Find spot at which to store the lock in the pile. Store locks
	// by memory address in increasing order in the list.
	i := len(*lp)
	for i > 0 {
		lh := &(*lp)[i-1]
		newLockAddress := reflect.ValueOf(newLock).Pointer()
		existingLockAddress := reflect.ValueOf(lh.lock).Pointer()
		if newLockAddress == existingLockAddress {
			// Lock has already been acquired. No need to
			// lock it; just increase the recursion count.
			lh.recursion++
			return
		} else if newLockAddress > existingLockAddress {
			break
		}
		i--
	}

	// Before inserting, unlock all locks that are stored further
	// within the list. This way Lock() will pick up the locks in
	// sorted order once again.
	for *lockedUpTo > i {
		*lockedUpTo--
		(*lp)[*lockedUpTo].lock.Unlock()
	}

	// Insert the lock into the list.
	*lp = append(*lp, lockHandle{})
	copy((*lp)[i+1:], (*lp)[i:])
	(*lp)[i] = lockHandle{lock: newLock}
}

// Lock one or more Locker objects, adding them to the LockPile. This
// function returns true iff it was capable of acquiring all locks
// without temporarily unlocking one of the existingly owned locks.
// Regardless of whether this function returns true or false, the same
// set of locks is held by the calling threads afterwards.
//
// Example usage, of a function that computes the sum of two value nodes
// in a tree atomically:
//
//     func (node *Node) GetSumOfParentAndChild(name string) (int, bool) {
//         lockPile := util.LockPile{}
//         defer lockPile.UnlockAll()
//         lockPile.Lock(&node.lock)  // Always returns 'true'
//         for {
//             if child, ok := node.children[name]; !ok {
//                 return 0, false
//             } else if lockPile.Lock(&child.lock) {
//                 // Successfully acquired child lock without unlocking
//                 // the parent.
//                 break
//             } else if node.children[name] == child {
//                 // Even though the parent was temporarily unlocked,
//                 // the parent-child relationship did not change.
//                 break
//             }
//             // Race condition during unlock. Retry.
//             lockPile.Unlock(&child.Lock)
//         }
//         return node.value + child.value, true
//     }
func (lp *LockPile) Lock(newLocks ...sync.Locker) bool {
	// Insert the locks into the pile one by one.
	originallyLockedUpTo := len(*lp)
	lockedUpTo := len(*lp)
	for _, newLock := range newLocks {
		lp.insert(newLock, &lockedUpTo)
	}

	// Acquire any new locks or reacquire any we had to drop.
	for i := lockedUpTo; i < len(*lp); i++ {
		(*lp)[i].lock.Lock()
	}
	return originallyLockedUpTo == lockedUpTo
}

// Unlock a Locker object, removing it from the LockPile.
func (lp *LockPile) Unlock(oldLock sync.Locker) {
	// Find lock to unlock.
	i := 0
	for (*lp)[i].lock != oldLock {
		i++
	}

	// When locked recursively, just decrement the recursion count.
	if (*lp)[i].recursion > 0 {
		(*lp)[i].recursion--
		return
	}

	// Unlock and remove entry from lock pile.
	(*lp)[i].lock.Unlock()
	copy((*lp)[i:], (*lp)[i+1:])
	*lp = (*lp)[:len(*lp)-1]
}

// UnlockAll unlocks all locks associated with a LockPile. Calling this
// function using 'defer' ensures that no locks remain acquired after
// the calling function returns.
func (lp *LockPile) UnlockAll() {
	// Release all locks contained in the pile exactly once.
	for _, lockHandle := range *lp {
		lockHandle.lock.Unlock()
	}
	*lp = nil
}
