package sync

import (
	"sync"
)

// TryLocker represents a lock type that can both be acquired in a
// blocking and non-blocking fashion.
type TryLocker interface {
	sync.Locker
	TryLock() bool
}

var (
	_ TryLocker = &sync.Mutex{}
	_ TryLocker = &sync.RWMutex{}
)

type lockHandle struct {
	lock      TryLocker
	recursion int
}

// LockPile is a list to keep track of locks held by a thread. For every
// lock, it keeps track of a recursion count, allowing locks that don't
// support recursion to be acquired multiple times. The underlying lock
// will only be unlocked if the recursion count reaches zero.
//
// LockPile implements a deadlock avoidance algorithm, ensuring that
// blocking on a lock is only performed when no other locks are held.
// What LockPile implements is equivalent to the "Smart" algorithm
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

func (lp *LockPile) insert(newLock TryLocker) {
	for i := 0; i < len(*lp); i++ {
		lh := &(*lp)[i]
		if lh.lock == newLock {
			// Lock already required. Increase recursion count.
			lh.recursion++
			return
		}
	}
	*lp = append(*lp, lockHandle{lock: newLock})
}

// Lock one or more TryLocker objects, adding them to the LockPile. This
// function returns true iff it was capable of acquiring all locks
// without temporarily unlocking one of the existingly owned locks.
// Regardless of whether this function returns true or false, the same
// set of locks is held by the calling threads afterwards.
//
// Example usage, of a function that computes the sum of two value nodes
// in a tree atomically:
//
//	func (node *Node) GetSumOfParentAndChild(name string) (int, bool) {
//	    lockPile := util.LockPile{}
//	    defer lockPile.UnlockAll()
//	    lockPile.Lock(&node.lock)  // Always returns 'true'
//	    for {
//	        if child, ok := node.children[name]; !ok {
//	            return 0, false
//	        } else if lockPile.Lock(&child.lock) {
//	            // Successfully acquired child lock without unlocking
//	            // the parent.
//	            break
//	        } else if node.children[name] == child {
//	            // Even though the parent was temporarily unlocked,
//	            // the parent-child relationship did not change.
//	            break
//	        }
//	        // Race condition during unlock. Retry.
//	        lockPile.Unlock(&child.Lock)
//	    }
//	    return node.value + child.value, true
//	}
func (lp *LockPile) Lock(newLocks ...TryLocker) bool {
	currentlyAcquired := len(*lp)
	for _, newLock := range newLocks {
		lp.insert(newLock)
	}

	completedWithoutUnlocking := true
	lhFirst := &(*lp)[0]
	for currentlyAcquired < len(*lp) {
		if currentlyAcquired > 0 {
			lhTry := &(*lp)[currentlyAcquired]
			if lhTry.lock.TryLock() {
				// Successfully acquired a subsequent lock.
				currentlyAcquired++
				continue
			}

			// Cannot acquire a subsequent lock. Temporarily
			// release all other locks, so that we can
			// attempt a blocking acquisition of the
			// subsequent lock.
			completedWithoutUnlocking = false
			for i := 0; i < currentlyAcquired; i++ {
				lhUnlock := &(*lp)[i]
				lhUnlock.lock.Unlock()
			}
			*lhFirst, *lhTry = *lhTry, *lhFirst
		}

		// First lock to acquire. Perform blocking acquisition.
		lhFirst.lock.Lock()
		currentlyAcquired = 1
	}
	return completedWithoutUnlocking
}

// Unlock a TryLocker object, removing it from the LockPile.
func (lp *LockPile) Unlock(oldLock TryLocker) {
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
	(*lp)[i] = (*lp)[len(*lp)-1]
	(*lp)[len(*lp)-1].lock = nil
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
