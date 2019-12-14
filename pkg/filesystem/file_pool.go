package filesystem

import (
	"github.com/buildbarn/bb-storage/pkg/filesystem"
)

// FilePool is an allocator for temporary files. Files are created by
// calling NewFile(). They are automatically removed by calling Close().
//
// File handles returned by NewFile() are not thread-safe. Additional
// locking needs to be done at higher levels to permit safe concurrent
// access.
type FilePool interface {
	NewFile() (filesystem.FileReadWriter, error)
}
