// +build freebsd windows

package builder

import (
	"github.com/buildbarn/bb-remote-execution/pkg/cas"
	"github.com/buildbarn/bb-remote-execution/pkg/filesystem/fuse"
	"github.com/buildbarn/bb-storage/pkg/blobstore"
)

func NewFUSEBuildDirectory(directory fuse.PrepopulatedDirectory, directoryFetcher cas.DirectoryFetcher, contentAddressableStorage blobstore.BlobAccess, inodeNumberTree fuse.InodeNumberTree) BuildDirectory {
	panic("FUSE is not supported on this platform")
}
