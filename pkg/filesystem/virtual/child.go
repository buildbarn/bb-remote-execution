package virtual

// Child is a variant type that either contains a directory or leaf
// object.
//
// TODO: In principle it should be possible to eliminate the 'kind'
// field, if it weren't for the fact that we can't compare TDirectory
// and TLeaf against nil. There is no nullable constraint.
// https://github.com/golang/go/issues/53656
type Child[TDirectory any, TLeaf any, TNode any] struct {
	kind      int
	directory TDirectory
	leaf      TLeaf
}

// FromDirectory creates a Child that contains a directory.
func (Child[TDirectory, TLeaf, TNode]) FromDirectory(directory TDirectory) Child[TDirectory, TLeaf, TNode] {
	return Child[TDirectory, TLeaf, TNode]{kind: 1, directory: directory}
}

// FromLeaf creates a Child that contains a leaf.
func (Child[TDirectory, TLeaf, TNode]) FromLeaf(leaf TLeaf) Child[TDirectory, TLeaf, TNode] {
	return Child[TDirectory, TLeaf, TNode]{kind: 2, leaf: leaf}
}

// IsSet returns true if the Child contains either a directory or leaf.
func (c Child[TDirectory, TLeaf, TNode]) IsSet() bool {
	return c.kind != 0
}

// GetNode returns the value of the child as a single object, making it
// possible to call into methods that are both provided by the directory
// and leaf types.
func (c Child[TDirectory, TLeaf, TNode]) GetNode() TNode {
	switch c.kind {
	case 1:
		// These casts are unnecessary, and are only needed
		// because type parameters can not constrain each other.
		// https://groups.google.com/g/golang-nuts/c/VrbEngj8Itg/m/54_l5f73BQAJ
		return any(c.directory).(TNode)
	case 2:
		return any(c.leaf).(TNode)
	default:
		panic("Child is not set")
	}
}

// GetPair returns the value of the child as a directory or leaf object,
// making it possible to call into directory/leaf specific methods.
func (c Child[TDirectory, TLeaf, TNode]) GetPair() (TDirectory, TLeaf) {
	return c.directory, c.leaf
}
