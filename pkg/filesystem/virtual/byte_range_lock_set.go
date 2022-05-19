package virtual

// ByteRangeLockType is an enumeration that controls what kind of lock
// needs to be acquired.
type ByteRangeLockType int

const (
	// ByteRangeLockTypeUnlocked indicates that a byte range should
	// be unlocked. This value can only be provided to Set(); not
	// Test(). It is equivalent to POSIX's F_UNLCK.
	ByteRangeLockTypeUnlocked ByteRangeLockType = iota
	// ByteRangeLockTypeLockedExclusive indicates that a byte range
	// should be locked exclusively for writing. It is equivalent to
	// POSIX's F_WRLCK.
	ByteRangeLockTypeLockedExclusive
	// ByteRangeLockTypeLockedShared indicates that a byte range
	// should be locked shared for reading. It is equivalent to
	// F_RDLCK.
	ByteRangeLockTypeLockedShared
)

// ByteRangeLock holds information on a lock held on a non-empty range
// of bytes. Each lock has an owner associated with it. This field is
// only checked for equality, allowing it to merge adjacent or
// overlapping locks held by the same owner.
type ByteRangeLock[Owner comparable] struct {
	Start uint64
	End   uint64
	Owner Owner
	Type  ByteRangeLockType
}

type byteRangeLockEntry[Owner comparable] struct {
	previous *byteRangeLockEntry[Owner]
	next     *byteRangeLockEntry[Owner]
	lock     ByteRangeLock[Owner]
}

func (le *byteRangeLockEntry[Owner]) insertBefore(leNew *byteRangeLockEntry[Owner]) {
	leNew.previous = le.previous
	leNew.next = le
	leNew.previous.next = leNew
	le.previous = leNew
}

func (le *byteRangeLockEntry[Owner]) remove() {
	le.previous.next = le.next
	le.next.previous = le.previous
	le.previous = nil
	le.next = nil
}

// ByteRangeLockSet is a set for ByteRangeLocks applied against the same
// file. The set is modeled as a linked list, where entries are sorted
// by starting address. All entries describe disjoint non-empty ranges
// of bytes, except for shared locks with distinct owners.
type ByteRangeLockSet[Owner comparable] struct {
	list byteRangeLockEntry[Owner]
}

// Initialize the ByteRangeLockSet, so that it does not contain any
// locks.
func (ls *ByteRangeLockSet[Owner]) Initialize() {
	ls.list.previous = &ls.list
	ls.list.next = &ls.list
}

// Set a byte range to a given lock. Calls to this method must generally
// be preceded by calls to Test(), as it is assumed the lock to be
// inserted does not conflict with the locks that are currently held.
//
// This method returns the increase (positive) or decrease (negative) of
// the number of entries in the set. This can be used by the caller to
// determine when the owner of the lock can be released.
func (ls *ByteRangeLockSet[Owner]) Set(lProvided *ByteRangeLock[Owner]) int {
	leNew := &byteRangeLockEntry[Owner]{
		lock: *lProvided,
	}
	lNew := &leNew.lock

	// Find the spot where to insert the new entry.
	var leTrailing *byteRangeLockEntry[Owner]
	leSearch := ls.list.next
	for {
		lSearch := &leSearch.lock
		if leSearch == &ls.list || lNew.Start <= lSearch.Start {
			// Found the spot where the new entry should be
			// inserted.
			break
		}

		// The new entry starts after the existing entry.
		if lSearch.Owner == lNew.Owner {
			if lNew.Type == lSearch.Type {
				if lNew.Start <= lSearch.End {
					// Beginning of the new entry touches
					// or overlaps with an entry of the
					// same type. Grow the entry and
					// insert it, so it gets combined.
					lNew.Start = lSearch.Start
					break
				}
			} else {
				if lNew.Start < lSearch.End {
					// Beginning of the new entry overlaps
					// with an entry of a different type.
					// Truncate the existing entry.
					if lNew.End < lSearch.End {
						// New entry punches a hole in
						// the existing entry. Split it.
						if leTrailing != nil {
							panic("New entry has multiple trailing overlapping entries, which is impossible")
						}
						leTrailing = &byteRangeLockEntry[Owner]{
							lock: ByteRangeLock[Owner]{
								Start: lNew.End,
								End:   lSearch.End,
								Owner: lSearch.Owner,
								Type:  lSearch.Type,
							},
						}
						lSearch.End = lNew.Start
					}
					lSearch.End = lNew.Start
				}
			}
		}
		leSearch = leSearch.next
	}

	// Insert the new entry, except if we're unlocking.
	delta := 0
	if lNew.Type != ByteRangeLockTypeUnlocked {
		leSearch.insertBefore(leNew)
		delta++
	}

	// Merge successive entries into the new entry.
	for {
		lSearch := &leSearch.lock
		if leSearch == &ls.list || lNew.End < lSearch.Start {
			// New entry does not affect any entries beyond
			// this point.
			break
		}

		leSearchNext := leSearch.next
		if lSearch.Owner == lNew.Owner {
			if lNew.End >= lSearch.End {
				// New entry completely overlaps with an
				// existing entry. Remove the existing
				// entry.
				leSearch.remove()
				delta--
			} else {
				if lNew.Type == lSearch.Type {
					// End of new entry touches or
					// overlaps with an entry of the
					// same type. Combine them.
					lNew.End = lSearch.End
					leSearch.remove()
					delta--
				} else {
					// End of new entry touches or
					// overlaps with an entry of a
					// different type. Remove the
					// leading part of the entry and
					// move it to a location further
					// down the list.
					if leTrailing != nil {
						panic("New entry has multiple trailing overlapping entries, which is impossible")
					}
					leTrailing = leSearch
					lSearch.Start = lNew.End
					leSearch.remove()
					delta--
				}
			}
		}
		leSearch = leSearchNext
	}

	// Insert the trailing part of the entry whose leading part got
	// overwritten, or in which a hole got punched.
	if leTrailing != nil {
		leSearch.insertBefore(leTrailing)
		delta++
	}
	return delta
}

// Test whether a new lock to be inserted does not conflict with any of
// the other locks registered in the set.
func (ls *ByteRangeLockSet[Owner]) Test(lTest *ByteRangeLock[Owner]) *ByteRangeLock[Owner] {
	leSearch := ls.list.next
	for {
		lSearch := &leSearch.lock
		if leSearch == &ls.list || lSearch.Start >= lTest.End {
			// At the end of the list, or there are no
			// longer any entries that follow that have any
			// overlap.
			return nil
		}
		if lSearch.Owner != lTest.Owner && lSearch.End > lTest.Start && (lSearch.Type == ByteRangeLockTypeLockedExclusive || lTest.Type == ByteRangeLockTypeLockedExclusive) {
			// Found a conflicting entry.
			return lSearch
		}
		leSearch = leSearch.next
	}
}
