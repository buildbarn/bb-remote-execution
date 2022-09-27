package virtual

import (
	"context"
	"io"
	"sync"
	"sync/atomic"

	"github.com/buildbarn/bb-storage/pkg/filesystem/path"
	"github.com/buildbarn/bb-storage/pkg/random"
)

// fnv1aHasher is a helper type for computing FNV-1a hashes.
type fnv1aHasher struct {
	hash uint64
}

func (w *fnv1aHasher) Write(p []byte) (int, error) {
	for _, c := range p {
		w.hash ^= uint64(c)
		w.hash *= 1099511628211
	}
	return len(p), nil
}

// FUSERemovalNotifier is a callback method that can be registered to
// report the removal of files from stateful directories.
type FUSERemovalNotifier func(parent uint64, name path.Component)

// FUSERemovalNotifierRegistrar has the same signature as
// FUSEStatefulHandleAllocator.RegisterRemovalNotifier(). It has been
// added to aid testing.
type FUSERemovalNotifierRegistrar func(removalNotifier FUSERemovalNotifier)

type fuseHandleOptions struct {
	randomNumberGenerator random.ThreadSafeGenerator

	removalNotifiersLock sync.RWMutex
	removalNotifiers     []FUSERemovalNotifier
}

// FUSEStatefulHandleAllocator creates a handle allocator for the
// purpose of exposing the virtual file system through FUSE. It is
// responsible for decorating all files in the file system, so that they
// have inode numbers and link counts. Inode numbers are unique for
// stateful (mutable) files, while they are identical for stateless
// files that share the same identifiers, meaning they can be
// deduplicated by the kernel.
//
// The FUSE protocol is stateful, in the sense that the kernel and the
// userspace service share state on which nodes in the file system have
// been resolved. The kernel will never issue requests against objects
// that have not been resolved yet, or for which the kernel has issued a
// FORGET operation. This means that this handle allocator does not need
// to provide any mechanism for resolving arbitrary files present in the
// file system, making its implementation simple. There is thus no
// distinction between stateless and resolvable files.
type FUSEStatefulHandleAllocator struct {
	options *fuseHandleOptions
}

var _ StatefulHandleAllocator = (*FUSEStatefulHandleAllocator)(nil)

// NewFUSEHandleAllocator creates a new FUSEStatefulHandleAllocator.
func NewFUSEHandleAllocator(randomNumberGenerator random.ThreadSafeGenerator) *FUSEStatefulHandleAllocator {
	return &FUSEStatefulHandleAllocator{
		options: &fuseHandleOptions{
			randomNumberGenerator: randomNumberGenerator,
		},
	}
}

// RegisterRemovalNotifier adds a new file removal notifier to the
// handle allocator. Any future calls to Release() against
// DirectoryHandles returned by this handle allocator will call into the
// FUSERemovalNotifier, providing it the inode number of the parent
// directory.
//
// This method is used by the FUSE server to register a callback that
// sends "entry notify" events to the kernel, causing the directory
// entry to be removed from the kernel's cache.
func (hr *FUSEStatefulHandleAllocator) RegisterRemovalNotifier(removalNotifier FUSERemovalNotifier) {
	hr.options.removalNotifiersLock.Lock()
	hr.options.removalNotifiers = append(hr.options.removalNotifiers, removalNotifier)
	hr.options.removalNotifiersLock.Unlock()
}

// New creates a new stateful handle allocation.
func (hr *FUSEStatefulHandleAllocator) New() StatefulHandleAllocation {
	return &fuseStatefulHandleAllocation{
		options: hr.options,
	}
}

type fuseStatefulHandleAllocation struct {
	options *fuseHandleOptions
}

func (hn *fuseStatefulHandleAllocation) AsStatelessAllocator() StatelessHandleAllocator {
	hr := &fuseStatelessHandleAllocator{
		inodeNumberSeed: hn.options.randomNumberGenerator.Uint64(),
	}
	*hn = fuseStatefulHandleAllocation{}
	return hr
}

func (hn *fuseStatefulHandleAllocation) AsResolvableAllocator(resolver HandleResolver) ResolvableHandleAllocator {
	hr := &fuseResolvableHandleAllocator{
		inodeNumberSeed: hn.options.randomNumberGenerator.Uint64(),
	}
	*hn = fuseStatefulHandleAllocation{}
	return hr
}

func (hn *fuseStatefulHandleAllocation) AsStatefulDirectory(directory Directory) StatefulDirectoryHandle {
	dh := &fuseStatefulDirectoryHandle{
		options:     hn.options,
		inodeNumber: hn.options.randomNumberGenerator.Uint64(),
	}
	*hn = fuseStatefulHandleAllocation{}
	return dh
}

func (hn *fuseStatefulHandleAllocation) AsStatelessDirectory(directory Directory) Directory {
	d := &fuseStatelessDirectory{
		Directory:   directory,
		inodeNumber: hn.options.randomNumberGenerator.Uint64(),
	}
	*hn = fuseStatefulHandleAllocation{}
	return d
}

func (hn *fuseStatefulHandleAllocation) AsNativeLeaf(leaf NativeLeaf) NativeLeaf {
	l := &fuseStatefulNativeLeaf{
		NativeLeaf:  leaf,
		inodeNumber: hn.options.randomNumberGenerator.Uint64(),
	}
	l.linkCount.Store(1)
	*hn = fuseStatefulHandleAllocation{}
	return l
}

func (hn *fuseStatefulHandleAllocation) AsLeaf(leaf Leaf) Leaf {
	panic("Regular leaf objects cannot be used in stateful contexts, as they cannot be linked/unlinked")
}

type fuseStatelessHandleAllocator struct {
	inodeNumberSeed uint64
}

func (hr *fuseStatelessHandleAllocator) New(w io.WriterTo) StatelessHandleAllocation {
	hasher := fnv1aHasher{
		hash: hr.inodeNumberSeed,
	}
	if _, err := w.WriteTo(&hasher); err != nil {
		panic(err)
	}
	return &fuseStatelessHandleAllocation{
		currentInodeNumber: hasher.hash,
	}
}

type fuseStatelessHandleAllocation struct {
	currentInodeNumber uint64
}

func (hn *fuseStatelessHandleAllocation) AsStatelessAllocator() StatelessHandleAllocator {
	hr := &fuseStatelessHandleAllocator{
		inodeNumberSeed: hn.currentInodeNumber,
	}
	*hn = fuseStatelessHandleAllocation{}
	return hr
}

func (hn *fuseStatelessHandleAllocation) AsResolvableAllocator(resolver HandleResolver) ResolvableHandleAllocator {
	hr := &fuseResolvableHandleAllocator{
		inodeNumberSeed: hn.currentInodeNumber,
	}
	*hn = fuseStatelessHandleAllocation{}
	return hr
}

func (hn *fuseStatelessHandleAllocation) AsStatelessDirectory(directory Directory) Directory {
	d := &fuseStatelessDirectory{
		Directory:   directory,
		inodeNumber: hn.currentInodeNumber,
	}
	*hn = fuseStatelessHandleAllocation{}
	return d
}

func (hn *fuseStatelessHandleAllocation) AsNativeLeaf(leaf NativeLeaf) NativeLeaf {
	return &fuseStatelessNativeLeaf{
		NativeLeaf:  leaf,
		inodeNumber: hn.currentInodeNumber,
	}
}

func (hn *fuseStatelessHandleAllocation) AsLeaf(leaf Leaf) Leaf {
	return &fuseStatelessLeaf{
		Leaf:        leaf,
		inodeNumber: hn.currentInodeNumber,
	}
}

type fuseResolvableHandleAllocator struct {
	inodeNumberSeed uint64
}

func (hr *fuseResolvableHandleAllocator) New(w io.WriterTo) ResolvableHandleAllocation {
	hasher := fnv1aHasher{
		hash: hr.inodeNumberSeed,
	}
	if _, err := w.WriteTo(&hasher); err != nil {
		panic(err)
	}
	// Because we don't care about actually resolving files, we
	// treat stateless and resolvable files equally.
	return &fuseStatelessHandleAllocation{
		currentInodeNumber: hasher.hash,
	}
}

// fuseStatefulDirectoryHandle is a handle for stateful directories that
// augments the results of VirtualGetAttributes() to contain an inode
// number. It also provides a removal notifier that can call into the
// FUSE server.
type fuseStatefulDirectoryHandle struct {
	options     *fuseHandleOptions
	inodeNumber uint64
}

func (dh *fuseStatefulDirectoryHandle) GetAttributes(requested AttributesMask, attributes *Attributes) {
	attributes.SetInodeNumber(dh.inodeNumber)
}

func (dh *fuseStatefulDirectoryHandle) NotifyRemoval(name path.Component) {
	dh.options.removalNotifiersLock.RLock()
	removalNotifiers := dh.options.removalNotifiers
	dh.options.removalNotifiersLock.RUnlock()

	for _, removalNotifier := range removalNotifiers {
		removalNotifier(dh.inodeNumber, name)
	}
}

func (dh *fuseStatefulDirectoryHandle) Release() {}

// fuseStatelessDirectory is a decorator for stateless Directory objects
// that augments the results of VirtualGetAttributes() to contain an
// inode number.
type fuseStatelessDirectory struct {
	Directory
	inodeNumber uint64
}

func (d *fuseStatelessDirectory) VirtualGetAttributes(ctx context.Context, requested AttributesMask, attributes *Attributes) {
	if remaining := requested &^ AttributesMaskInodeNumber; remaining != 0 {
		d.Directory.VirtualGetAttributes(ctx, remaining, attributes)
	}
	attributes.SetInodeNumber(d.inodeNumber)
}

func (d *fuseStatelessDirectory) VirtualSetAttributes(ctx context.Context, in *Attributes, requested AttributesMask, attributes *Attributes) Status {
	if s := d.Directory.VirtualSetAttributes(ctx, in, requested, attributes); s != StatusOK {
		return s
	}
	attributes.SetInodeNumber(d.inodeNumber)
	return StatusOK
}

// fuseStatefulNativeLeaf is a decorator for NativeLeaf that augments
// the results of VirtualGetAttributes() to contain an inode number and
// link count. Link() and Unlink() calls are intercepted, and are only
// forwarded if the link count drops to zero.
type fuseStatefulNativeLeaf struct {
	NativeLeaf
	inodeNumber uint64
	linkCount   atomic.Uint32
}

func (l *fuseStatefulNativeLeaf) Link() Status {
	for {
		current := l.linkCount.Load()
		if current == 0 {
			// Attempted to link a file that was already unlinked.
			return StatusErrStale
		}
		if l.linkCount.CompareAndSwap(current, current+1) {
			return StatusOK
		}
	}
}

func (l *fuseStatefulNativeLeaf) Unlink() {
	if l.linkCount.Add(^uint32(0)) == 0 {
		l.NativeLeaf.Unlink()
	}
}

func (l *fuseStatefulNativeLeaf) injectAttributes(attributes *Attributes) {
	attributes.SetInodeNumber(l.inodeNumber)
	attributes.SetLinkCount(l.linkCount.Load())
	// The change ID should normally also be affected by the link
	// count. We don't bother overriding the change ID here, as FUSE
	// does not depend on it.
}

func (l *fuseStatefulNativeLeaf) VirtualGetAttributes(ctx context.Context, requested AttributesMask, attributes *Attributes) {
	if remaining := requested &^ (AttributesMaskInodeNumber | AttributesMaskLinkCount); remaining != 0 {
		l.NativeLeaf.VirtualGetAttributes(ctx, remaining, attributes)
	}
	l.injectAttributes(attributes)
}

func (l *fuseStatefulNativeLeaf) VirtualSetAttributes(ctx context.Context, in *Attributes, requested AttributesMask, attributes *Attributes) Status {
	if s := l.NativeLeaf.VirtualSetAttributes(ctx, in, requested, attributes); s != StatusOK {
		return s
	}
	l.injectAttributes(attributes)
	return StatusOK
}

func (l *fuseStatefulNativeLeaf) VirtualOpenSelf(ctx context.Context, shareAccess ShareMask, options *OpenExistingOptions, requested AttributesMask, attributes *Attributes) Status {
	if s := l.NativeLeaf.VirtualOpenSelf(ctx, shareAccess, options, requested, attributes); s != StatusOK {
		return s
	}
	l.injectAttributes(attributes)
	return StatusOK
}

// fuseStatelessNativeLeaf is a decorator for NativeLeaf that augments
// the results of VirtualGetAttributes() to contain an inode number and
// link count. For these kinds of files, the link count is just a
// constant.
type fuseStatelessNativeLeaf struct {
	NativeLeaf
	inodeNumber uint64
}

func (l *fuseStatelessNativeLeaf) Link() Status {
	return StatusOK
}

func (l *fuseStatelessNativeLeaf) Unlink() {}

func (l *fuseStatelessNativeLeaf) injectAttributes(attributes *Attributes) {
	attributes.SetInodeNumber(l.inodeNumber)
	attributes.SetLinkCount(StatelessLeafLinkCount)
}

func (l *fuseStatelessNativeLeaf) VirtualGetAttributes(ctx context.Context, requested AttributesMask, attributes *Attributes) {
	if remaining := requested &^ (AttributesMaskInodeNumber | AttributesMaskLinkCount); remaining != 0 {
		l.NativeLeaf.VirtualGetAttributes(ctx, remaining, attributes)
	}
	l.injectAttributes(attributes)
}

func (l *fuseStatelessNativeLeaf) VirtualSetAttributes(ctx context.Context, in *Attributes, requested AttributesMask, attributes *Attributes) Status {
	if s := l.NativeLeaf.VirtualSetAttributes(ctx, in, requested, attributes); s != StatusOK {
		return s
	}
	l.injectAttributes(attributes)
	return StatusOK
}

func (l *fuseStatelessNativeLeaf) VirtualOpenSelf(ctx context.Context, shareAccess ShareMask, options *OpenExistingOptions, requested AttributesMask, attributes *Attributes) Status {
	if s := l.NativeLeaf.VirtualOpenSelf(ctx, shareAccess, options, requested, attributes); s != StatusOK {
		return s
	}
	l.injectAttributes(attributes)
	return StatusOK
}

// fuseStatelessLeaf is a decorator for Leaf that augments the results
// of VirtualGetAttributes() to contain an inode number and link count.
// For these kinds of files, the link count is just a constant.
type fuseStatelessLeaf struct {
	Leaf
	inodeNumber uint64
}

func (l *fuseStatelessLeaf) injectAttributes(attributes *Attributes) {
	attributes.SetInodeNumber(l.inodeNumber)
	attributes.SetLinkCount(StatelessLeafLinkCount)
}

func (l *fuseStatelessLeaf) VirtualGetAttributes(ctx context.Context, requested AttributesMask, attributes *Attributes) {
	if remaining := requested &^ (AttributesMaskInodeNumber | AttributesMaskLinkCount); remaining != 0 {
		l.Leaf.VirtualGetAttributes(ctx, remaining, attributes)
	}
	l.injectAttributes(attributes)
}

func (l *fuseStatelessLeaf) VirtualSetAttributes(ctx context.Context, in *Attributes, requested AttributesMask, attributes *Attributes) Status {
	if s := l.Leaf.VirtualSetAttributes(ctx, in, requested, attributes); s != StatusOK {
		return s
	}
	l.injectAttributes(attributes)
	return StatusOK
}

func (l *fuseStatelessLeaf) VirtualOpenSelf(ctx context.Context, shareAccess ShareMask, options *OpenExistingOptions, requested AttributesMask, attributes *Attributes) Status {
	if s := l.Leaf.VirtualOpenSelf(ctx, shareAccess, options, requested, attributes); s != StatusOK {
		return s
	}
	l.injectAttributes(attributes)
	return StatusOK
}
