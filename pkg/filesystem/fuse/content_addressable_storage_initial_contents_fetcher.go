// +build darwin linux

package fuse

import (
	"context"

	"github.com/buildbarn/bb-remote-execution/pkg/cas"
	"github.com/buildbarn/bb-storage/pkg/blobstore"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/filesystem/path"
	"github.com/buildbarn/bb-storage/pkg/util"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
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

func (f *contentAddressableStorageInitialContentsFetcher) FetchContents() (map[path.Component]InitialContentsFetcher, map[path.Component]NativeLeaf, error) {
	directory, err := f.options.directoryFetcher.GetDirectory(f.options.context, f.digest)
	if err != nil {
		return nil, nil, err
	}

	// Create InitialContentsFetchers for all child directories.
	// These can yield even more InitialContentsFetchers for
	// grandchildren.
	instanceName := f.digest.GetInstanceName()
	directories := map[path.Component]InitialContentsFetcher{}
	for _, entry := range directory.Directories {
		component, ok := path.NewComponent(entry.Name)
		if !ok {
			return nil, nil, status.Errorf(codes.InvalidArgument, "Directory %#v has an invalid name", entry.Name)
		}
		childDigest, err := instanceName.NewDigestFromProto(entry.Digest)
		if err != nil {
			return nil, nil, util.StatusWrapf(err, "Failed to obtain digest for directory %#v", entry.Name)
		}
		directories[component] = &contentAddressableStorageInitialContentsFetcher{
			options: f.options,
			digest:  childDigest,
		}
	}

	// Create Content Addressable Storage backed read-only files.
	leaves := map[path.Component]NativeLeaf{}
	for _, entry := range directory.Files {
		component, ok := path.NewComponent(entry.Name)
		if !ok {
			return nil, nil, status.Errorf(codes.InvalidArgument, "File %#v has an invalid name", entry.Name)
		}
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
		leaves[component] = NewContentAddressableStorageFile(
			f.options.context,
			f.options.contentAddressableStorage,
			f.options.errorLogger,
			inodeNumberTree.AddString(childDigest.String()).Get(),
			childDigest,
			entry.IsExecutable)
	}
	for _, entry := range directory.Symlinks {
		component, ok := path.NewComponent(entry.Name)
		if !ok {
			return nil, nil, status.Errorf(codes.InvalidArgument, "Symlink %#v has an invalid name", entry.Name)
		}
		leaves[component] = NewSymlink(entry.Target)
	}

	return directories, leaves, nil
}
