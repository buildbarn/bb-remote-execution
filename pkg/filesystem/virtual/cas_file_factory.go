package virtual

import (
	"github.com/buildbarn/bb-storage/pkg/digest"
)

// CASFileFactory is a factory type for files whose contents correspond
// with an object stored in the Content Addressable Storage (CAS).
type CASFileFactory interface {
	LookupFile(digest digest.Digest, isExecutable bool) NativeLeaf
}
