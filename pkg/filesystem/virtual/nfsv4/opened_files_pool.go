package nfsv4

import (
	"bytes"
	"math"
	"sync"

	"github.com/buildbarn/bb-remote-execution/pkg/filesystem/virtual"
	"github.com/buildbarn/go-xdr/pkg/protocols/nfsv4"
)

// OpenedFilesPool holds the state of files that are opened by one or
// more clients through an NFSv4 server. The purpose of this type is
// twofold:
//
//   - To ensure that files that are opened are always resolvable by
//     handle. This is needed to ensure that PUTFH for such files
//     succeeds, even if they are no longer present in the file system. If
//     this were not the case, it would be impossible to call CLOSE on
//     them.
//
//   - To keep track of any byte-range locks that are currently applied
//     against opened files.
type OpenedFilesPool struct {
	handleResolver virtual.HandleResolver

	lock          sync.RWMutex
	filesByHandle map[string]*OpenedFile
}

// NewOpenedFilesPool creates an OpenedFilesPool that is empty.
func NewOpenedFilesPool(handleResolver virtual.HandleResolver) *OpenedFilesPool {
	return &OpenedFilesPool{
		handleResolver: handleResolver,
		filesByHandle:  map[string]*OpenedFile{},
	}
}

// Open is invoked as part of the OPEN operation to either create new
// opened file state, or return a reference to existing opened file
// state.
func (ofp *OpenedFilesPool) Open(handle nfsv4.NfsFh4, leaf virtual.Leaf) *OpenedFile {
	ofp.lock.Lock()
	defer ofp.lock.Unlock()

	handleKey := string(handle)
	of, ok := ofp.filesByHandle[handleKey]
	if ok {
		of.useCount.increase()
	} else {
		of = &OpenedFile{
			pool:     ofp,
			handle:   handle,
			leaf:     leaf,
			useCount: 1,
		}
		of.locks.Initialize()
		ofp.filesByHandle[handleKey] = of
	}
	return of
}

// TestLock returns whether a new byte-range lock can be acquired
// without conflicting with any of the locks that are already present.
// It can be used to implement the LOCKT operation.
func (ofp *OpenedFilesPool) TestLock(handle nfsv4.NfsFh4, lockOwner *nfsv4.LockOwner4, offset nfsv4.Offset4, length nfsv4.Length4, lockType nfsv4.NfsLockType4) nfsv4.Lockt4res {
	start, end, st := offsetLengthToStartEnd(offset, length)
	if st != nfsv4.NFS4_OK {
		return &nfsv4.Lockt4res_default{Status: st}
	}
	byteRangeLockType, st := nfsLockType4ToByteRangeLockType(lockType)
	if st != nfsv4.NFS4_OK {
		return &nfsv4.Lockt4res_default{Status: st}
	}

	ofp.lock.RLock()
	defer ofp.lock.RUnlock()

	of, ok := ofp.filesByHandle[string(handle)]
	if !ok {
		// File isn't opened by anyone, meaning no locks may
		// cause a conflict.
		return &nfsv4.Lockt4res_NFS4_OK{}
	}

	lock := virtual.ByteRangeLock[*nfsv4.LockOwner4]{
		Owner: lockOwner,
		Start: start,
		End:   end,
		Type:  byteRangeLockType,
	}

	of.locksLock.Lock()
	defer of.locksLock.Unlock()

	// Test whether a lock exists that conflicts.
	if conflictingLock := of.locks.Test(&lock); conflictingLock != nil {
		return &nfsv4.Lockt4res_NFS4ERR_DENIED{
			Denied: byteRangeLockToLock4Denied(conflictingLock),
		}
	}
	return &nfsv4.Lockt4res_NFS4_OK{}
}

// Resolve the virtual file system node corresponding to a given file
// handle.
func (ofp *OpenedFilesPool) Resolve(handle nfsv4.NfsFh4) (virtual.DirectoryChild, nfsv4.Nfsstat4) {
	ofp.lock.RLock()
	of, ok := ofp.filesByHandle[string(handle)]
	ofp.lock.RUnlock()
	if ok {
		// File is opened at least once. Return this copy, so
		// that access is guaranteed even if the file has been
		// removed from the file system.
		return virtual.DirectoryChild{}.FromLeaf(of.leaf), nfsv4.NFS4_OK
	}

	// File is currently not open. Call into the handle resolver to
	// do a lookup.
	child, vs := ofp.handleResolver(bytes.NewBuffer(handle))
	if vs != virtual.StatusOK {
		return virtual.DirectoryChild{}, toNFSv4Status(vs)
	}
	return child, nfsv4.NFS4_OK
}

// OpenedFile contains the state that needs to be tracked for files that
// are opened at least once.
type OpenedFile struct {
	// Constant fields.
	pool   *OpenedFilesPool
	handle nfsv4.NfsFh4
	leaf   virtual.Leaf

	// Fields protected by OpenedFilePool.lock.
	useCount referenceCount

	// Fields protected by locksLock.
	locksLock sync.RWMutex
	locks     virtual.ByteRangeLockSet[*nfsv4.LockOwner4]
}

// GetHandle returns the file handle that was originally provided when
// OpenedFilesPool.Open() was called.
func (of *OpenedFile) GetHandle() nfsv4.NfsFh4 {
	return of.handle
}

// GetLeaf returns the virtual file system node that was originally
// provided when OpenedFilesPool.Open() was called.
func (of *OpenedFile) GetLeaf() virtual.Leaf {
	return of.leaf
}

// Lock a byte range in the file. This method can be used to implement
// the LOCK operation.
//
// Note that the provided lock-owner is stored by reference. Exactly the
// same memory address must to be provided to TestLock(), Unlock() and
// UnlockAll() to make these functions work as expected.
func (of *OpenedFile) Lock(lockOwner *nfsv4.LockOwner4, offset nfsv4.Offset4, length nfsv4.Length4, lockType nfsv4.NfsLockType4) (int, nfsv4.Lock4res) {
	start, end, st := offsetLengthToStartEnd(offset, length)
	if st != nfsv4.NFS4_OK {
		return 0, &nfsv4.Lock4res_default{Status: st}
	}
	byteRangeLockType, st := nfsLockType4ToByteRangeLockType(lockType)
	if st != nfsv4.NFS4_OK {
		return 0, &nfsv4.Lock4res_default{Status: st}
	}

	lock := virtual.ByteRangeLock[*nfsv4.LockOwner4]{
		Owner: lockOwner,
		Start: start,
		End:   end,
		Type:  byteRangeLockType,
	}

	of.locksLock.Lock()
	defer of.locksLock.Unlock()

	// Test whether the new lock conflicts with an existing one.
	if conflictingLock := of.locks.Test(&lock); conflictingLock != nil {
		return 0, &nfsv4.Lock4res_NFS4ERR_DENIED{
			Denied: byteRangeLockToLock4Denied(conflictingLock),
		}
	}
	return of.locks.Set(&lock), nil
}

// Unlock a byte range in the file. This method can be used to implement
// the LOCKU operation.
func (of *OpenedFile) Unlock(lockOwner *nfsv4.LockOwner4, offset nfsv4.Offset4, length nfsv4.Length4) (int, nfsv4.Nfsstat4) {
	start, end, st := offsetLengthToStartEnd(offset, length)
	if st != nfsv4.NFS4_OK {
		return 0, st
	}

	lock := virtual.ByteRangeLock[*nfsv4.LockOwner4]{
		Owner: lockOwner,
		Start: start,
		End:   end,
		Type:  virtual.ByteRangeLockTypeUnlocked,
	}

	of.locksLock.Lock()
	defer of.locksLock.Unlock()

	return of.locks.Set(&lock), nfsv4.NFS4_OK
}

// UnlockAll releases all byte-range locks in the file belonging to a
// given lock-owner. This method can be used in circumstances where
// locks need to be forcefully removed, such as the CLOSE operation, or
// when state of inactive clients is purged.
func (of *OpenedFile) UnlockAll(lockOwner *nfsv4.LockOwner4) int {
	lock := virtual.ByteRangeLock[*nfsv4.LockOwner4]{
		Owner: lockOwner,
		Start: 0,
		End:   math.MaxUint64,
		Type:  virtual.ByteRangeLockTypeUnlocked,
	}

	of.locksLock.Lock()
	defer of.locksLock.Unlock()

	return of.locks.Set(&lock)
}

// Close a file. If the reference count of the file drops to zero, all
// associated state is removed.
func (of *OpenedFile) Close() {
	ofp := of.pool
	ofp.lock.Lock()
	defer ofp.lock.Unlock()

	if of.useCount.decrease() {
		delete(ofp.filesByHandle, string(of.handle))
	}
}

// offsetLengthToStartEnd converts an (offset, length) pair to a
// (start, end) pair. The former is used by NFSv4, while the latter is
// used by ByteRangeLock.
//
// More details: RFC 7530, section 16.10.4, paragraph 2.
func offsetLengthToStartEnd(offset, length uint64) (uint64, uint64, nfsv4.Nfsstat4) {
	switch length {
	case 0:
		return 0, 0, nfsv4.NFS4ERR_INVAL
	case math.MaxUint64:
		// A length of all ones indicates end-of-file.
		return offset, math.MaxUint64, nfsv4.NFS4_OK
	default:
		if length > math.MaxUint64-offset {
			// The end exceeds the maximum 64-bit unsigned
			// integer value.
			return 0, 0, nfsv4.NFS4ERR_INVAL
		}
		return offset, offset + length, nfsv4.NFS4_OK
	}
}

// nfsLockType4ToByteRangeLockType converts an NFSv4 lock type to a
// virtual file system byte range lock type. As this implementation does
// not attempt to provide any fairness, no distinction is made between
// waiting and non-waiting lock type variants.
func nfsLockType4ToByteRangeLockType(in nfsv4.NfsLockType4) (virtual.ByteRangeLockType, nfsv4.Nfsstat4) {
	switch in {
	case nfsv4.READ_LT, nfsv4.READW_LT:
		return virtual.ByteRangeLockTypeLockedShared, nfsv4.NFS4_OK
	case nfsv4.WRITE_LT, nfsv4.WRITEW_LT:
		return virtual.ByteRangeLockTypeLockedExclusive, nfsv4.NFS4_OK
	default:
		return 0, nfsv4.NFS4ERR_INVAL
	}
}

// byteRangeLockToLock4Denied converts information on a conflicting byte
// range lock into a LOCK4denied response.
func byteRangeLockToLock4Denied(lock *virtual.ByteRangeLock[*nfsv4.LockOwner4]) nfsv4.Lock4denied {
	length := uint64(math.MaxUint64)
	if lock.End != math.MaxUint64 {
		length = lock.End - lock.Start
	}
	var lockType nfsv4.NfsLockType4
	switch lock.Type {
	case virtual.ByteRangeLockTypeLockedShared:
		lockType = nfsv4.READ_LT
	case virtual.ByteRangeLockTypeLockedExclusive:
		lockType = nfsv4.WRITE_LT
	default:
		panic("Unexpected lock type")
	}
	return nfsv4.Lock4denied{
		Offset:   lock.Start,
		Length:   length,
		Locktype: lockType,
		Owner:    *lock.Owner,
	}
}
