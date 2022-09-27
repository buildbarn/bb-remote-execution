package virtual

import (
	"bytes"
	"context"
	"encoding/binary"
	"io"
	"sync"

	"github.com/buildbarn/bb-storage/pkg/filesystem/path"
	"github.com/buildbarn/bb-storage/pkg/random"
)

// fileHandleToInodeNumber converts a file handle to an inode number.
func fileHandleToInodeNumber(fileHandle []byte) uint64 {
	hasher := fnv1aHasher{
		hash: binary.LittleEndian.Uint64(fileHandle),
	}
	if _, err := bytes.NewBuffer(fileHandle[8:]).WriteTo(&hasher); err != nil {
		panic(err)
	}
	return hasher.hash
}

// inodeNumberToBaseFileHandle converts the inode number of a
// non-resolvable file to a file handle. File handles for such files are
// simply identical to the inode number.
func inodeNumberToBaseFileHandle(inodeNumber uint64) [8]byte {
	var fileHandle [8]byte
	binary.LittleEndian.PutUint64(fileHandle[:], inodeNumber)
	return fileHandle
}

// setAttributesForFileHandle sets the file handle and inode number
// attributes for a given file.
func setAttributesForFileHandle(fileHandle []byte, requested AttributesMask, attributes *Attributes) {
	attributes.SetFileHandle(fileHandle)
	if requested&AttributesMaskInodeNumber != 0 {
		attributes.SetInodeNumber(fileHandleToInodeNumber(fileHandle))
	}
}

type nfsHandlePool struct {
	lock                  sync.RWMutex
	randomNumberGenerator random.SingleThreadedGenerator
	directories           map[uint64]Directory
	statefulLeaves        map[uint64]*nfsStatefulNativeLeaf
	statelessLeaves       map[uint64]*nfsStatelessNativeLeaf
	resolvers             map[uint64]HandleResolver
}

func (hp *nfsHandlePool) createStatelessDirectoryLocked(inodeNumber uint64, underlyingDirectory Directory) Directory {
	// Reuse an existing directory if one exists.
	if directory, ok := hp.directories[inodeNumber]; ok {
		return directory
	}

	fileHandle := inodeNumberToBaseFileHandle(inodeNumber)
	directory := &nfsStatelessDirectory{
		Directory:  underlyingDirectory,
		fileHandle: fileHandle[:],
	}
	hp.directories[inodeNumber] = directory
	return directory
}

func (hp *nfsHandlePool) createResolvableAllocatorLocked(inodeNumber uint64, resolver HandleResolver) ResolvableHandleAllocator {
	if _, ok := hp.resolvers[inodeNumber]; !ok {
		hp.resolvers[inodeNumber] = resolver
	}

	fileHandlePrefix := inodeNumberToBaseFileHandle(inodeNumber)
	return &nfsResolvableHandleAllocator{
		fileHandlePrefix: fileHandlePrefix[:],
	}
}

// NFSStatefulHandleAllocator creates a handle allocator for the purpose
// of exposing the virtual file system through NFS. It is responsible
// for decorating all files in the file system, so that they have file
// handles, inode numbers and link counts. File handles and inode
// numbers are unique for stateful (mutable) files, while they are
// identical for stateless files that share the same identifiers,
// meaning they can be deduplicated by the kernel.
//
// The NFS protocol is stateless, in the sense that the client and
// server share no state on which nodes in the file system have been
// resolved. The client does not inform the server that it has released
// files from its cache. This means that the server needs to be able to
// resolve all file handles that are either still present in the file
// system or are still opened by the client. This handle allocator is
// capable of only doing the former. The latter can be supported at a
// higher level.
//
// To work well with infinitely big directory structures (e.g.,
// bb_clientd's "cas" directory), this implementation makes use of the
// handle resolver function provided to AsResolvableAllocator(). Instead
// of tracking these nodes explicitly, it generates longer file handles
// that have the value provided to ResolvableHandleAllocator.New() as
// a suffix, making it possible to regenerate these nodes on the fly.
type NFSStatefulHandleAllocator struct {
	pool *nfsHandlePool
}

var _ StatefulHandleAllocator = (*NFSStatefulHandleAllocator)(nil)

// NewNFSHandleAllocator creates a new NFSStatefulHandleAllocator that
// does not have any resolvable objects.
func NewNFSHandleAllocator(randomNumberGenerator random.SingleThreadedGenerator) *NFSStatefulHandleAllocator {
	return &NFSStatefulHandleAllocator{
		pool: &nfsHandlePool{
			randomNumberGenerator: randomNumberGenerator,
			directories:           map[uint64]Directory{},
			statefulLeaves:        map[uint64]*nfsStatefulNativeLeaf{},
			statelessLeaves:       map[uint64]*nfsStatelessNativeLeaf{},
			resolvers:             map[uint64]HandleResolver{},
		},
	}
}

// ResolveHandle resolves a directory or leaf object that corresponds
// with a file handle previously returned by Attributes.GetFileHandle().
//
// Only files that are linked into the file system are guaranteed to be
// resolvable. Files that have been unlinked, but are still opened have
// to be tracked at a higher level.
func (hr *NFSStatefulHandleAllocator) ResolveHandle(r io.ByteReader) (DirectoryChild, Status) {
	// The first eight bytes of the handle always correspond to a
	// base inode number.
	var inodeNumberBytes [8]byte
	for i := 0; i < len(inodeNumberBytes); i++ {
		c, err := r.ReadByte()
		if err != nil {
			return DirectoryChild{}, StatusErrBadHandle
		}
		inodeNumberBytes[i] = c
	}
	inodeNumber := binary.LittleEndian.Uint64(inodeNumberBytes[:])

	p := hr.pool
	p.lock.RLock()
	if directory, ok := p.directories[inodeNumber]; ok {
		p.lock.RUnlock()
		return DirectoryChild{}.FromDirectory(directory), StatusOK
	}
	if leaf, ok := p.statefulLeaves[inodeNumber]; ok {
		p.lock.RUnlock()
		return DirectoryChild{}.FromLeaf(leaf), StatusOK
	}
	if leaf, ok := p.statelessLeaves[inodeNumber]; ok {
		p.lock.RUnlock()
		return DirectoryChild{}.FromLeaf(leaf), StatusOK
	}
	if resolver, ok := p.resolvers[inodeNumber]; ok {
		p.lock.RUnlock()
		return resolver(r)
	}
	p.lock.RUnlock()
	return DirectoryChild{}, StatusErrStale
}

// New creates a new stateful handle allocation.
func (hr *NFSStatefulHandleAllocator) New() StatefulHandleAllocation {
	return &nfsStatefulHandleAllocation{
		pool: hr.pool,
	}
}

type nfsStatefulHandleAllocation struct {
	pool *nfsHandlePool
}

func (hn *nfsStatefulHandleAllocation) AsStatelessAllocator() StatelessHandleAllocator {
	hp := hn.pool
	hp.lock.Lock()
	inodeNumberSeed := hp.randomNumberGenerator.Uint64()
	hp.lock.Unlock()
	*hn = nfsStatefulHandleAllocation{}
	return &nfsStatelessHandleAllocator{
		pool:            hp,
		inodeNumberSeed: inodeNumberSeed,
	}
}

func (hn *nfsStatefulHandleAllocation) AsResolvableAllocator(resolver HandleResolver) ResolvableHandleAllocator {
	hp := hn.pool
	hp.lock.Lock()
	hr := hp.createResolvableAllocatorLocked(hp.randomNumberGenerator.Uint64(), resolver)
	hp.lock.Unlock()
	*hn = nfsStatefulHandleAllocation{}
	return hr
}

func (hn *nfsStatefulHandleAllocation) AsStatefulDirectory(directory Directory) StatefulDirectoryHandle {
	hp := hn.pool
	hp.lock.Lock()
	inodeNumber := hp.randomNumberGenerator.Uint64()
	hp.directories[inodeNumber] = directory
	hp.lock.Unlock()

	*hn = nfsStatefulHandleAllocation{}
	return &nfsStatefulDirectoryHandle{
		pool:        hp,
		inodeNumber: inodeNumber,
	}
}

func (hn *nfsStatefulHandleAllocation) AsStatelessDirectory(underlyingDirectory Directory) Directory {
	hp := hn.pool
	hp.lock.Lock()
	directory := hp.createStatelessDirectoryLocked(hp.randomNumberGenerator.Uint64(), underlyingDirectory)
	hp.lock.Unlock()
	*hn = nfsStatefulHandleAllocation{}
	return directory
}

func (hn *nfsStatefulHandleAllocation) AsNativeLeaf(underlyingLeaf NativeLeaf) NativeLeaf {
	hp := hn.pool
	hp.lock.Lock()
	inodeNumber := hp.randomNumberGenerator.Uint64()
	fileHandle := inodeNumberToBaseFileHandle(inodeNumber)
	leaf := &nfsStatefulNativeLeaf{
		NativeLeaf: underlyingLeaf,
		pool:       hp,
		fileHandle: fileHandle[:],
		linkCount:  1,
	}
	hp.statefulLeaves[inodeNumber] = leaf
	hp.lock.Unlock()

	*hn = nfsStatefulHandleAllocation{}
	return leaf
}

func (hn *nfsStatefulHandleAllocation) AsLeaf(underlyingLeaf Leaf) Leaf {
	panic("Regular leaf objects cannot be used in stateful contexts, as they cannot be linked/unlinked")
}

type nfsStatelessHandleAllocator struct {
	pool            *nfsHandlePool
	inodeNumberSeed uint64
}

func (hr *nfsStatelessHandleAllocator) New(w io.WriterTo) StatelessHandleAllocation {
	hasher := fnv1aHasher{
		hash: hr.inodeNumberSeed,
	}
	if _, err := w.WriteTo(&hasher); err != nil {
		panic(err)
	}
	return &nfsStatelessHandleAllocation{
		pool:               hr.pool,
		currentInodeNumber: hasher.hash,
	}
}

type nfsStatelessHandleAllocation struct {
	pool               *nfsHandlePool
	currentInodeNumber uint64
}

func (hn *nfsStatelessHandleAllocation) AsStatelessAllocator() StatelessHandleAllocator {
	hr := &nfsStatelessHandleAllocator{
		pool:            hn.pool,
		inodeNumberSeed: hn.currentInodeNumber,
	}
	*hn = nfsStatelessHandleAllocation{}
	return hr
}

func (hn *nfsStatelessHandleAllocation) AsResolvableAllocator(resolver HandleResolver) ResolvableHandleAllocator {
	hp := hn.pool
	hp.lock.Lock()
	hr := hp.createResolvableAllocatorLocked(hn.currentInodeNumber, resolver)
	hp.lock.Unlock()
	*hn = nfsStatelessHandleAllocation{}
	return hr
}

func (hn *nfsStatelessHandleAllocation) AsStatelessDirectory(underlyingDirectory Directory) Directory {
	hp := hn.pool
	hp.lock.Lock()
	directory := hp.createStatelessDirectoryLocked(hn.currentInodeNumber, underlyingDirectory)
	hp.lock.Unlock()
	*hn = nfsStatelessHandleAllocation{}
	return directory
}

func (hn *nfsStatelessHandleAllocation) AsNativeLeaf(underlyingLeaf NativeLeaf) NativeLeaf {
	hp := hn.pool
	hp.lock.Lock()

	// Reuse an existing leaf if one exists.
	if leaf, ok := hp.statelessLeaves[hn.currentInodeNumber]; ok {
		leaf.linkCount++
		hp.lock.Unlock()
		underlyingLeaf.Unlink()
		return leaf
	}

	// None exists. Create a new one.
	fileHandle := inodeNumberToBaseFileHandle(hn.currentInodeNumber)
	leaf := &nfsStatelessNativeLeaf{
		NativeLeaf: underlyingLeaf,
		pool:       hp,
		fileHandle: fileHandle[:],
		linkCount:  1,
	}
	hp.statelessLeaves[hn.currentInodeNumber] = leaf
	hp.lock.Unlock()

	*hn = nfsStatelessHandleAllocation{}
	return leaf
}

func (hn *nfsStatelessHandleAllocation) AsLeaf(underlyingLeaf Leaf) Leaf {
	panic("Regular leaf objects cannot be used in stateless contexts, as they cannot be linked/unlinked")
}

type nfsResolvableHandleAllocator struct {
	fileHandlePrefix []byte
}

func (hr *nfsResolvableHandleAllocator) New(w io.WriterTo) ResolvableHandleAllocation {
	fileHandle := bytes.NewBuffer(hr.fileHandlePrefix[:len(hr.fileHandlePrefix):len(hr.fileHandlePrefix)])
	if _, err := w.WriteTo(fileHandle); err != nil {
		panic(err)
	}
	return &nfsResolvableHandleAllocation{
		currentFileHandle: fileHandle.Bytes(),
	}
}

type nfsResolvableHandleAllocation struct {
	currentFileHandle []byte
}

func (hn *nfsResolvableHandleAllocation) AsResolvableAllocator(resolver HandleResolver) ResolvableHandleAllocator {
	hr := &nfsResolvableHandleAllocator{
		fileHandlePrefix: hn.currentFileHandle,
	}
	*hn = nfsResolvableHandleAllocation{}
	return hr
}

func (hn *nfsResolvableHandleAllocation) AsStatelessDirectory(underlyingDirectory Directory) Directory {
	directory := &nfsStatelessDirectory{
		Directory:  underlyingDirectory,
		fileHandle: hn.currentFileHandle,
	}
	*hn = nfsResolvableHandleAllocation{}
	return directory
}

func (hn *nfsResolvableHandleAllocation) AsNativeLeaf(underlyingLeaf NativeLeaf) NativeLeaf {
	leaf := &nfsResolvableNativeLeaf{
		NativeLeaf: underlyingLeaf,
		fileHandle: hn.currentFileHandle,
	}
	*hn = nfsResolvableHandleAllocation{}
	return leaf
}

func (hn *nfsResolvableHandleAllocation) AsLeaf(underlyingLeaf Leaf) Leaf {
	leaf := &nfsResolvableLeaf{
		Leaf:       underlyingLeaf,
		fileHandle: hn.currentFileHandle,
	}
	*hn = nfsResolvableHandleAllocation{}
	return leaf
}

// nfsStatefulDirectoryHandle is a handle for stateful directories that
// augments the results of VirtualGetAttributes() to contain a file
// handle and inode number.
type nfsStatefulDirectoryHandle struct {
	pool        *nfsHandlePool
	inodeNumber uint64
}

func (dh *nfsStatefulDirectoryHandle) GetAttributes(requested AttributesMask, attributes *Attributes) {
	fileHandle := inodeNumberToBaseFileHandle(dh.inodeNumber)
	attributes.SetFileHandle(fileHandle[:])
	attributes.SetInodeNumber(dh.inodeNumber)
}

func (dh *nfsStatefulDirectoryHandle) NotifyRemoval(name path.Component) {
	// Removal notification could be supported using NFSv4.1's
	// CB_NOTIFY operation. Unfortunately, none of the major client
	// implementations seem to support it.
	// https://github.com/torvalds/linux/blob/b05bf5c63b326ce1da84ef42498d8e0e292e694c/fs/nfs/callback_xdr.c#L779-L783
}

func (dh *nfsStatefulDirectoryHandle) Release() {
	hp := dh.pool
	hp.lock.Lock()
	delete(hp.directories, dh.inodeNumber)
	hp.lock.Unlock()
}

// nfsStatelessDirectory is a decorator for stateless Directory objects
// that augments the results of VirtualGetAttributes() to contain a file
// handle and inode number.
type nfsStatelessDirectory struct {
	Directory
	fileHandle []byte
}

func (d *nfsStatelessDirectory) VirtualGetAttributes(ctx context.Context, requested AttributesMask, attributes *Attributes) {
	if remaining := requested &^ (AttributesMaskFileHandle | AttributesMaskInodeNumber); remaining != 0 {
		d.Directory.VirtualGetAttributes(ctx, remaining, attributes)
	}
	setAttributesForFileHandle(d.fileHandle, requested, attributes)
}

func (d *nfsStatelessDirectory) VirtualSetAttributes(ctx context.Context, in *Attributes, requested AttributesMask, attributes *Attributes) Status {
	if s := d.Directory.VirtualSetAttributes(ctx, in, requested, attributes); s != StatusOK {
		return s
	}
	setAttributesForFileHandle(d.fileHandle, requested, attributes)
	return StatusOK
}

// nfsStatefulNativeLeaf is a decorator for NativeLeaf that augments
// the results of VirtualGetAttributes() to contain a file handle, inode
// number and link count. Link() and Unlink() calls are intercepted, and
// are only forwarded if the link count drops to zero.
type nfsStatefulNativeLeaf struct {
	NativeLeaf
	pool       *nfsHandlePool
	fileHandle []byte

	// Protected by pool.lock.
	linkCount uint32
	changeID  uint64
}

func (l *nfsStatefulNativeLeaf) Link() Status {
	hp := l.pool
	hp.lock.Lock()
	defer hp.lock.Unlock()

	if l.linkCount == 0 {
		return StatusErrStale
	}
	l.linkCount++
	l.changeID++
	return StatusOK
}

func (l *nfsStatefulNativeLeaf) Unlink() {
	inodeNumber := fileHandleToInodeNumber(l.fileHandle)

	hp := l.pool
	hp.lock.Lock()
	if l.linkCount == 0 {
		panic("Attempted to unlink file with link count zero")
	}
	l.linkCount--
	l.changeID++
	if l.linkCount == 0 {
		delete(hp.statefulLeaves, inodeNumber)
		hp.lock.Unlock()
		l.NativeLeaf.Unlink()
	} else {
		hp.lock.Unlock()
	}
}

func (l *nfsStatefulNativeLeaf) injectAttributes(requested AttributesMask, attributes *Attributes) {
	setAttributesForFileHandle(l.fileHandle, requested, attributes)
	if requested&(AttributesMaskChangeID|AttributesMaskLinkCount) != 0 {
		hp := l.pool
		hp.lock.RLock()
		if requested&AttributesMaskChangeID != 0 {
			attributes.SetChangeID(attributes.GetChangeID() + l.changeID)
		}
		attributes.SetLinkCount(l.linkCount)
		hp.lock.RUnlock()
	}
}

func (l *nfsStatefulNativeLeaf) VirtualGetAttributes(ctx context.Context, requested AttributesMask, attributes *Attributes) {
	if remaining := requested &^ (AttributesMaskFileHandle | AttributesMaskInodeNumber | AttributesMaskLinkCount); remaining != 0 {
		l.NativeLeaf.VirtualGetAttributes(ctx, remaining, attributes)
	}
	l.injectAttributes(requested, attributes)
}

func (l *nfsStatefulNativeLeaf) VirtualSetAttributes(ctx context.Context, in *Attributes, requested AttributesMask, attributes *Attributes) Status {
	if s := l.NativeLeaf.VirtualSetAttributes(ctx, in, requested, attributes); s != StatusOK {
		return s
	}
	l.injectAttributes(requested, attributes)
	return StatusOK
}

func (l *nfsStatefulNativeLeaf) VirtualOpenSelf(ctx context.Context, shareAccess ShareMask, options *OpenExistingOptions, requested AttributesMask, attributes *Attributes) Status {
	if s := l.NativeLeaf.VirtualOpenSelf(ctx, shareAccess, options, requested, attributes); s != StatusOK {
		return s
	}
	l.injectAttributes(requested, attributes)
	return StatusOK
}

// nfsStatelessNativeLeaf is a decorator for NativeLeaf that augments
// the results of VirtualGetAttributes() to contain a file handle, inode
// number and link count.
//
// Even though these files are stateless, we need to track an actual
// link count to determine when it's safe to release the file handle
// from nfsHandlePool. We do report a constant link count back to the
// user, both to prevent invalidation of the attributes and for
// consistency with FUSE.
type nfsStatelessNativeLeaf struct {
	NativeLeaf
	pool       *nfsHandlePool
	fileHandle []byte

	// Protected by pool.lock.
	linkCount uint32
}

func (l *nfsStatelessNativeLeaf) Link() Status {
	hp := l.pool
	hp.lock.Lock()
	defer hp.lock.Unlock()

	if l.linkCount == 0 {
		return StatusErrStale
	}
	l.linkCount++
	return StatusOK
}

func (l *nfsStatelessNativeLeaf) Unlink() {
	inodeNumber := fileHandleToInodeNumber(l.fileHandle)

	hp := l.pool
	hp.lock.Lock()
	if l.linkCount == 0 {
		panic("Attempted to unlink file with link count zero")
	}
	l.linkCount--
	if l.linkCount == 0 {
		delete(hp.statelessLeaves, inodeNumber)
		hp.lock.Unlock()
		l.NativeLeaf.Unlink()
	} else {
		hp.lock.Unlock()
	}
}

func (l *nfsStatelessNativeLeaf) injectAttributes(requested AttributesMask, attributes *Attributes) {
	setAttributesForFileHandle(l.fileHandle, requested, attributes)
	attributes.SetLinkCount(StatelessLeafLinkCount)
}

func (l *nfsStatelessNativeLeaf) VirtualGetAttributes(ctx context.Context, requested AttributesMask, attributes *Attributes) {
	if remaining := requested &^ (AttributesMaskFileHandle | AttributesMaskInodeNumber | AttributesMaskLinkCount); remaining != 0 {
		l.NativeLeaf.VirtualGetAttributes(ctx, remaining, attributes)
	}
	l.injectAttributes(requested, attributes)
}

func (l *nfsStatelessNativeLeaf) VirtualSetAttributes(ctx context.Context, in *Attributes, requested AttributesMask, attributes *Attributes) Status {
	if s := l.NativeLeaf.VirtualSetAttributes(ctx, in, requested, attributes); s != StatusOK {
		return s
	}
	l.injectAttributes(requested, attributes)
	return StatusOK
}

func (l *nfsStatelessNativeLeaf) VirtualOpenSelf(ctx context.Context, shareAccess ShareMask, options *OpenExistingOptions, requested AttributesMask, attributes *Attributes) Status {
	if s := l.NativeLeaf.VirtualOpenSelf(ctx, shareAccess, options, requested, attributes); s != StatusOK {
		return s
	}
	l.injectAttributes(requested, attributes)
	return StatusOK
}

// nfsResolvableNativeLeaf is a decorator for NativeLeaf that augments
// the results of VirtualGetAttributes() to contain a file handle, inode
// number and link count. For these kinds of files, the link count is
// just a constant.
type nfsResolvableNativeLeaf struct {
	NativeLeaf
	fileHandle []byte
}

func (l *nfsResolvableNativeLeaf) Link() Status {
	return StatusOK
}

func (l *nfsResolvableNativeLeaf) Unlink() {}

func (l *nfsResolvableNativeLeaf) injectAttributes(requested AttributesMask, attributes *Attributes) {
	setAttributesForFileHandle(l.fileHandle, requested, attributes)
	attributes.SetLinkCount(StatelessLeafLinkCount)
}

func (l *nfsResolvableNativeLeaf) VirtualGetAttributes(ctx context.Context, requested AttributesMask, attributes *Attributes) {
	if remaining := requested &^ (AttributesMaskFileHandle | AttributesMaskInodeNumber | AttributesMaskLinkCount); remaining != 0 {
		l.NativeLeaf.VirtualGetAttributes(ctx, remaining, attributes)
	}
	l.injectAttributes(requested, attributes)
}

func (l *nfsResolvableNativeLeaf) VirtualSetAttributes(ctx context.Context, in *Attributes, requested AttributesMask, attributes *Attributes) Status {
	if s := l.NativeLeaf.VirtualSetAttributes(ctx, in, requested, attributes); s != StatusOK {
		return s
	}
	l.injectAttributes(requested, attributes)
	return StatusOK
}

func (l *nfsResolvableNativeLeaf) VirtualOpenSelf(ctx context.Context, shareAccess ShareMask, options *OpenExistingOptions, requested AttributesMask, attributes *Attributes) Status {
	if s := l.NativeLeaf.VirtualOpenSelf(ctx, shareAccess, options, requested, attributes); s != StatusOK {
		return s
	}
	l.injectAttributes(requested, attributes)
	return StatusOK
}

// nfsResolvableLeaf is a decorator for Leaf that augments the results
// of VirtualGetAttributes() to contain a file handle, inode number and
// link count. For these kinds of files, the link count is just a
// constant.
type nfsResolvableLeaf struct {
	Leaf
	fileHandle []byte
}

func (l *nfsResolvableLeaf) injectAttributes(requested AttributesMask, attributes *Attributes) {
	setAttributesForFileHandle(l.fileHandle, requested, attributes)
	attributes.SetLinkCount(StatelessLeafLinkCount)
}

func (l *nfsResolvableLeaf) VirtualGetAttributes(ctx context.Context, requested AttributesMask, attributes *Attributes) {
	if remaining := requested &^ (AttributesMaskFileHandle | AttributesMaskInodeNumber | AttributesMaskLinkCount); remaining != 0 {
		l.Leaf.VirtualGetAttributes(ctx, remaining, attributes)
	}
	l.injectAttributes(requested, attributes)
}

func (l *nfsResolvableLeaf) VirtualSetAttributes(ctx context.Context, in *Attributes, requested AttributesMask, attributes *Attributes) Status {
	if s := l.Leaf.VirtualSetAttributes(ctx, in, requested, attributes); s != StatusOK {
		return s
	}
	l.injectAttributes(requested, attributes)
	return StatusOK
}

func (l *nfsResolvableLeaf) VirtualOpenSelf(ctx context.Context, shareAccess ShareMask, options *OpenExistingOptions, requested AttributesMask, attributes *Attributes) Status {
	if s := l.Leaf.VirtualOpenSelf(ctx, shareAccess, options, requested, attributes); s != StatusOK {
		return s
	}
	l.injectAttributes(requested, attributes)
	return StatusOK
}
