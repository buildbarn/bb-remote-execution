package virtual

import (
	"github.com/buildbarn/bb-storage/pkg/filesystem/path"
)

type errorSymlinkFactory struct {
	err error
}

// NewErrorSymlinkFactory creates a SymlinkFactory that returns a fixed error
// response. Such an implementation is useful for explicitly disabling symlink
// creation.
func NewErrorSymlinkFactory(err error) SymlinkFactory {
	if err == nil {
		panic("Attempted to create error symlink factory with nil error")
	}
	return &errorSymlinkFactory{
		err: err,
	}
}

func (sf *errorSymlinkFactory) LookupSymlink(target path.Parser) (LinkableLeaf, error) {
	return nil, sf.err
}
