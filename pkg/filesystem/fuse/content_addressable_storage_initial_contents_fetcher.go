// +build darwin linux

package fuse

import (
	"context"

	"github.com/buildbarn/bb-remote-execution/pkg/cas"
	"github.com/buildbarn/bb-storage/pkg/blobstore"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/util"
)

type contentAddressableStorageInitialContentsFetcherOptions struct {
	context                   context.Context
	directoryFetcher          cas.DirectoryFetcher
	contentAddressableStorage blobstore.BlobAccess
	errorLogger               util.ErrorLogger
	fileInodeNumberTree       InodeNumberTree
	executableInodeNumberTree InodeNumberTree
}

type contentAddressableStorageInitialContentsFetcher struct {
	options *contentAddressableStorageInitialContentsFetcherOptions
	digest  digest.Digest
}

// NewContentAddressableStorageInitialContentsFetcher creates an
// InitialContentsFetcher that lazily instantiates a full directory
// hierarchy based on directory objects stored in the Content
// Addressable Storage (CAS).
//
// Upon request, it loads the root directory of the tree and converts
// all of the children to either additional InitialContentFetchers
// (directories), FileBackedFiles (regular files) or Symlinks (symbolic
// links).
func NewContentAddressableStorageInitialContentsFetcher(context context.Context, directoryFetcher cas.DirectoryFetcher, contentAddressableStorage blobstore.BlobAccess, errorLogger util.ErrorLogger, digest digest.Digest) InitialContentsFetcher {
	f := &contentAddressableStorageInitialContentsFetcher{
		options: &contentAddressableStorageInitialContentsFetcherOptions{
			context:                   context,
			directoryFetcher:          directoryFetcher,
			contentAddressableStorage: contentAddressableStorage,
			errorLogger:               errorLogger,
			fileInodeNumberTree:       NewRandomInodeNumberTree(),
			executableInodeNumberTree: NewRandomInodeNumberTree(),
		},
		digest: digest,
	}
	return f
}

func (f *contentAddressableStorageInitialContentsFetcher) FetchContents() (map[string]InitialContentsFetcher, map[string]NativeLeaf, error) {
	directory, err := f.options.directoryFetcher.GetDirectory(f.options.context, f.digest)
	if err != nil {
		return nil, nil, err
	}

	// Create InitialContentsFetchers for all child directories.
	// These can yield even more InitialContentsFetchers for
	// grandchildren.
	instanceName := f.digest.GetInstanceName()
	directories := map[string]InitialContentsFetcher{}
	for _, entry := range directory.Directories {
		childDigest, err := instanceName.NewDigestFromProto(entry.Digest)
		if err != nil {
			return nil, nil, util.StatusWrapf(err, "Failed to obtain digest for directory %#v", entry.Name)
		}
		directories[entry.Name] = &contentAddressableStorageInitialContentsFetcher{
			options: f.options,
			digest:  childDigest,
		}
	}

	// Create Content Addressable Storage backed read-only files.
	leaves := map[string]NativeLeaf{}
	for _, entry := range directory.Files {
		childDigest, err := instanceName.NewDigestFromProto(entry.Digest)
		if err != nil {
			return nil, nil, util.StatusWrapf(err, "Failed to obtain digest for file %#v", entry.Name)
		}
		// Generate an inode number based on the digest of the
		// underlying object. That way two identical files share
		// the same inode number, meaning that the kernel only
		// caches its contents once.
		//
		// Do make sure that two identical files with different
		// +x bits have a different inode number to prevent them
		// from being merged. Let every build action use its own
		// inode number trees, so that inode numbers are not
		// shared across build actions. Each build action has
		// its own timeout. We wouldn't want build actions to
		// receive timeouts prematurely.
		inodeNumberTree := &f.options.fileInodeNumberTree
		if entry.IsExecutable {
			inodeNumberTree = &f.options.executableInodeNumberTree
		}
		leaves[entry.Name] = NewContentAddressableStorageFile(
			f.options.context,
			f.options.contentAddressableStorage,
			f.options.errorLogger,
			inodeNumberTree.AddString(childDigest.String()).Get(),
			childDigest,
			entry.IsExecutable)
	}
	for _, entry := range directory.Symlinks {
		leaves[entry.Name] = NewSymlink(entry.Target)
	}

	return directories, leaves, nil
}
