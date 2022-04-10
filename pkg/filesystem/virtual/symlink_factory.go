package virtual

// SymlinkFactory is a factory type for symbolic links. Symbolic links are
// immutable files; the target to which they point can only be altered by
// replacing the node entirely (e.g., by first unlinking it from the
// directory).
type SymlinkFactory interface {
	LookupSymlink(target []byte) NativeLeaf
}
