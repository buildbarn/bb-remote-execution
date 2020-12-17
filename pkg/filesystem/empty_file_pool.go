package filesystem

import (
	"github.com/buildbarn/bb-storage/pkg/filesystem"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type emptyFilePool struct{}

func (fp emptyFilePool) NewFile() (filesystem.FileReadWriter, error) {
	return nil, status.Error(codes.ResourceExhausted, "Cannot create file in empty file pool")
}

// EmptyFilePool is a FilePool that does not permit the creation of new
// files. It is used as the default FilePool for the root of the
// worker's FUSE file system to disallow the creation of files not bound
// to a specific build action.
var EmptyFilePool FilePool = emptyFilePool{}
