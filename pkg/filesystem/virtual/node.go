package virtual

import (
	"context"

	"github.com/buildbarn/bb-remote-execution/pkg/proto/bazeloutputservice"
	"github.com/buildbarn/bb-remote-execution/pkg/proto/outputpathpersistency"
	"github.com/buildbarn/bb-storage/pkg/blobstore"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/filesystem"
	"github.com/buildbarn/bb-storage/pkg/filesystem/path"
)

// Node is the intersection between Directory and Leaf. These are the
// operations that can be applied to both kinds of objects.
type Node interface {
	VirtualGetAttributes(ctx context.Context, requested AttributesMask, attributes *Attributes)
	VirtualSetAttributes(ctx context.Context, in *Attributes, requested AttributesMask, attributes *Attributes) Status
	// VirtualApply can be used to send data between backing nodes. Returns true
	// if the request was intercepted.
	VirtualApply(data any) bool
}

// GetFileInfo extracts the attributes of a node and returns it in the
// form of a FileInfo object.
func GetFileInfo(name path.Component, node Node) filesystem.FileInfo {
	var attributes Attributes
	node.VirtualGetAttributes(context.TODO(), AttributesMaskFileType|AttributesMaskPermissions, &attributes)
	permissions, ok := attributes.GetPermissions()
	if !ok {
		panic("Node did not return permissions attribute, even though it was requested")
	}
	return filesystem.NewFileInfo(
		name,
		attributes.GetFileType(),
		permissions&PermissionsExecute != 0)
}

// ApplyReadlink is an operation for VirtualApply that returns the
// target of a symbolic link in parsed form.
type ApplyReadlink struct {
	// Outputs.
	Target path.Parser
	Err    error
}

// ApplyUploadFile is an operation for VirtualApply that uploads the
// contents of a file into the Content Addressable Storage and returns
// the resulting object's digest.
type ApplyUploadFile struct {
	// Inputs.
	Context                   context.Context
	ContentAddressableStorage blobstore.BlobAccess
	DigestFunction            digest.Function
	WritableFileUploadDelay   <-chan struct{}

	// Outputs.
	Digest digest.Digest
	Err    error
}

// ApplyGetContainingDigests is an operation for VirtualApply that
// returns a set of digests of objects in the Content Addressable
// Storage that back the contents of this file.
//
// The set returned by this function may be passed to
// ContentAddressableStorage.FindMissingBlobs() to check whether
// the file still exists in its entirety, and to prevent that
// the file is removed in the nearby future.
type ApplyGetContainingDigests struct {
	// Inputs.
	Context context.Context

	// Outputs.
	ContainingDigests digest.Set
	Err               error
}

// ApplyGetBazelOutputServiceStat is an operation for VirtualApply that
// returns the status of the leaf node in the form of a Status message
// that is used by the Bazel Output Service protocol.
type ApplyGetBazelOutputServiceStat struct {
	// Inputs.
	DigestFunction *digest.Function

	// Outputs.
	Stat *bazeloutputservice.BatchStatResponse_Stat
	Err  error
}

// ApplyAppendOutputPathPersistencyDirectoryNode is an operation for
// VirtualApply that appends a FileNode or SymlinkNode entry to a
// Directory message that is used to persist the state of a Bazel Output
// Service output path to disk.
type ApplyAppendOutputPathPersistencyDirectoryNode struct {
	// Inputs.
	Directory *outputpathpersistency.Directory
	Name      path.Component
}

// ApplyOpenReadFrozen is an operation for VirtualApply that opens a
// regular file for reading. The file's contents are guaranteed to be
// immutable as long as the file is kept open.
//
// If the file is still opened for writing through the virtual file
// system, implementations should wait for the file to be closed to
// ensure that all data is flushed from the page cache. The
// WritableFileDelay channel can be used to place a bound on the maximum
// amount of time to wait.
type ApplyOpenReadFrozen struct {
	// Inputs.
	WritableFileDelay <-chan struct{}

	// Outputs.
	Reader filesystem.FileReader
	Err    error
}
