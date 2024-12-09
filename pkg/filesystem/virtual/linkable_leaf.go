package virtual

// LinkableLeaf objects are non-directory nodes that can be placed in a
// PrepopulatedDirectory.
type LinkableLeaf interface {
	Leaf
	InitialNode

	// Operations called into by implementations of
	// PrepopulatedDirectory. The Link() operation may fail, for the
	// reason that Directory.VirtualLink() may be called on leaf
	// nodes that have been removed concurrently.
	Link() Status
	Unlink()
}
