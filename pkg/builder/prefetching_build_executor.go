package builder

import (
	"context"
	"io"
	"log"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-remote-execution/pkg/cas"
	re_filesystem "github.com/buildbarn/bb-remote-execution/pkg/filesystem"
	"github.com/buildbarn/bb-remote-execution/pkg/filesystem/access"
	"github.com/buildbarn/bb-remote-execution/pkg/proto/remoteworker"
	"github.com/buildbarn/bb-storage/pkg/blobstore"
	"github.com/buildbarn/bb-storage/pkg/blobstore/buffer"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/filesystem/path"
	"github.com/buildbarn/bb-storage/pkg/proto/fsac"
	"github.com/buildbarn/bb-storage/pkg/util"

	"golang.org/x/sync/errgroup"
	"golang.org/x/sync/semaphore"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

type prefetchingBuildExecutor struct {
	BuildExecutor
	contentAddressableStorage   blobstore.BlobAccess
	directoryFetcher            cas.DirectoryFetcher
	fileReadSemaphore           *semaphore.Weighted
	fileSystemAccessCache       blobstore.BlobAccess
	maximumMessageSizeBytes     int
	bloomFilterBitsPerElement   int
	bloomFilterMaximumSizeBytes int
	emptyProfile                *fsac.FileSystemAccessProfile
}

// NewPrefetchingBuildExecutor creates a decorator for BuildExecutor
// that as the action gets executed, prefetches files that are part of
// the action's input root from the Content Addressable Storage (CAS).
// It determines which files to download by making use of Bloom filters
// stored in the File System Access Cache (FSAC).
//
// It also monitors the file system access of the action to be executed.
// If this yields a Bloom filter that differs from the one retried from
// the FSAC, it stores an updated version. This ensures that the next
// time a similar action is ran, only the files that are expected to be
// used are downloaded.
//
// This decorator is only of use on workers that use a virtual build
// directory (FUSE, NFSv4). On workers that use native build
// directories, the monitor is ignored, leading to empty Bloom filters
// being stored.
func NewPrefetchingBuildExecutor(buildExecutor BuildExecutor, contentAddressableStorage blobstore.BlobAccess, directoryFetcher cas.DirectoryFetcher, fileReadSemaphore *semaphore.Weighted, fileSystemAccessCache blobstore.BlobAccess, maximumMessageSizeBytes, bloomFilterBitsPerElement, bloomFilterMaximumSizeBytes int) BuildExecutor {
	be := &prefetchingBuildExecutor{
		BuildExecutor:               buildExecutor,
		contentAddressableStorage:   contentAddressableStorage,
		directoryFetcher:            directoryFetcher,
		fileReadSemaphore:           fileReadSemaphore,
		fileSystemAccessCache:       fileSystemAccessCache,
		maximumMessageSizeBytes:     maximumMessageSizeBytes,
		bloomFilterBitsPerElement:   bloomFilterBitsPerElement,
		bloomFilterMaximumSizeBytes: bloomFilterMaximumSizeBytes,
	}
	be.emptyProfile = be.computeProfile(access.NewBloomFilterComputingUnreadDirectoryMonitor())
	return be
}

func (be *prefetchingBuildExecutor) computeProfile(monitor *access.BloomFilterComputingUnreadDirectoryMonitor) *fsac.FileSystemAccessProfile {
	bloomFilter, bloomFilterHashFunctions := monitor.GetBloomFilter(be.bloomFilterBitsPerElement, be.bloomFilterMaximumSizeBytes)
	return &fsac.FileSystemAccessProfile{
		BloomFilter:              bloomFilter,
		BloomFilterHashFunctions: bloomFilterHashFunctions,
	}
}

func (be *prefetchingBuildExecutor) Execute(ctx context.Context, filePool re_filesystem.FilePool, monitor access.UnreadDirectoryMonitor, digestFunction digest.Function, request *remoteworker.DesiredState_Executing, executionStateUpdates chan<- *remoteworker.CurrentState_Executing) *remoteexecution.ExecuteResponse {
	// Obtain the reduced Action digest, which is needed to read
	// from, and write to the File System Access Cache (FSAC).
	action := request.Action
	if action == nil {
		response := NewDefaultExecuteResponse(request)
		attachErrorToExecuteResponse(response, status.Error(codes.InvalidArgument, "Request does not contain an action"))
		return response
	}
	reducedActionDigest, err := blobstore.GetReducedActionDigest(digestFunction, action)
	if err != nil {
		response := NewDefaultExecuteResponse(request)
		attachErrorToExecuteResponse(response, util.StatusWrap(err, "Cannot compute reduced action digest"))
		return response
	}

	group, groupCtx := errgroup.WithContext(ctx)
	prefetchCtx, cancelPrefetch := context.WithCancel(groupCtx)

	// Fetch the profile from the File System Access Cache. If one
	// exists, traverse the input root and prefetch any files and
	// directories matched by the Bloom filter.
	var existingProfile *fsac.FileSystemAccessProfile
	group.Go(func() error {
		profileMessage, err := be.fileSystemAccessCache.Get(groupCtx, reducedActionDigest).ToProto(&fsac.FileSystemAccessProfile{}, be.maximumMessageSizeBytes)
		if err != nil {
			if status.Code(err) == codes.NotFound {
				existingProfile = be.emptyProfile
				return nil
			}
			return util.StatusWrap(err, "Failed to fetch file system access profile")
		}
		existingProfile = profileMessage.(*fsac.FileSystemAccessProfile)

		bloomFilter, err := access.NewBloomFilterReader(existingProfile.BloomFilter, existingProfile.BloomFilterHashFunctions)
		if err != nil {
			// Don't fail if the profile is malformed, as
			// this prevents forward progress. Simply ignore
			// its contents, so that it is replaced when the
			// action completes.
			log.Printf("Cannot read Bloom filter for %s: %s", reducedActionDigest.String(), err)
			return nil
		}

		directoryPrefetcher := directoryPrefetcher{
			context:                   prefetchCtx,
			group:                     group,
			bloomFilter:               bloomFilter,
			digestFunction:            digestFunction,
			contentAddressableStorage: be.contentAddressableStorage,
			directoryFetcher:          be.directoryFetcher,
			fileReadSemaphore:         be.fileReadSemaphore,
		}
		// Prefetching may be interrupted if the action
		// completes quickly. These cancelation errors should
		// not propagate to the caller.
		if err := directoryPrefetcher.prefetchRecursively(nil, access.RootPathHashes, action.InputRootDigest); status.Code(err) != codes.Canceled {
			return err
		}
		return nil
	})

	// While prefetching is happening, already launch the build
	// action. It may initially run slower due to local cache
	// misses, but should speed up as prefetching nears completion.
	bloomFilterMonitor := access.NewBloomFilterComputingUnreadDirectoryMonitor()
	var response *remoteexecution.ExecuteResponse
	group.Go(func() error {
		response = be.BuildExecutor.Execute(groupCtx, filePool, bloomFilterMonitor, digestFunction, request, executionStateUpdates)
		cancelPrefetch()
		if !executeResponseIsSuccessful(response) {
			return dontReadFromFSACError{}
		}
		return nil
	})

	if err := group.Wait(); err == nil {
		// Action completed successfully. Store an updated
		// profile in the File System Access Cache if it is
		// different from the one fetched previously.
		if newProfile := be.computeProfile(bloomFilterMonitor); !proto.Equal(existingProfile, newProfile) {
			if err := be.fileSystemAccessCache.Put(ctx, reducedActionDigest, buffer.NewProtoBufferFromProto(newProfile, buffer.UserProvided)); err != nil {
				attachErrorToExecuteResponse(response, util.StatusWrap(err, "Failed to store file system access profile"))
			}
		}
	} else if err != (dontReadFromFSACError{}) {
		response.Status = status.Convert(err).Proto()
	}
	return response
}

// dontReadFromFSACError is a placeholder error type. If an action
// completes unsuccessfully before the read against the File System
// Access Cache (FSAC) completes, there is no point in continuing that.
// This type is used to force the error group to cancel its context,
// causing the read to be interrupted.
type dontReadFromFSACError struct{}

func (dontReadFromFSACError) Error() string {
	panic("This error is merely a placeholder, and should not be returned")
}

// directoryPrefetcher is used by prefetchingBuildExecutor to
// recursively traverse the input root, only downloading parts of the
// input root that are matched by a Bloom filter.
type directoryPrefetcher struct {
	context                   context.Context
	group                     *errgroup.Group
	bloomFilter               *access.BloomFilterReader
	digestFunction            digest.Function
	contentAddressableStorage blobstore.BlobAccess
	directoryFetcher          cas.DirectoryFetcher
	fileReadSemaphore         *semaphore.Weighted
}

func (dp *directoryPrefetcher) shouldPrefetch(pathHashes access.PathHashes) bool {
	return dp.bloomFilter.Contains(pathHashes)
}

func (dp *directoryPrefetcher) prefetchRecursively(pathTrace *path.Trace, directoryPathHashes access.PathHashes, rawDirectoryDigest *remoteexecution.Digest) error {
	if !dp.shouldPrefetch(directoryPathHashes) {
		// This directory nor any of its children are expected
		// to be accessed.
		return nil
	}

	directoryDigest, err := dp.digestFunction.NewDigestFromProto(rawDirectoryDigest)
	if err != nil {
		return util.StatusWrapf(err, "Failed to parse digest for directory %#v", pathTrace.String())
	}
	directory, err := dp.directoryFetcher.GetDirectory(dp.context, directoryDigest)
	if err != nil {
		return util.StatusWrapf(err, "Failed to prefetch directory %#v", pathTrace.String())
	}

	// Directories are traversed sequentially to prevent unbounded
	// concurrency. Therefore, schedule prefetching of files before
	// directories, so that concurrency is introduced as soon as
	// possible.
	for _, file := range directory.Files {
		component, ok := path.NewComponent(file.Name)
		if !ok {
			return status.Errorf(codes.InvalidArgument, "File %#v in directory %#v has an invalid name", file.Name, pathTrace.String())
		}
		if dp.shouldPrefetch(directoryPathHashes.AppendComponent(component)) {
			childPathTrace := pathTrace.Append(component)
			fileDigest, err := dp.digestFunction.NewDigestFromProto(file.Digest)
			if err != nil {
				return util.StatusWrapf(err, "Failed to parse digest for file %#v", childPathTrace.String())
			}

			// Download files at a globally bounded concurrency.
			//
			// TODO: We currently do a 1 byte read against
			// the file, as a BlobAccess.Prefetch() doesn't
			// carry its weight just yet. We should revisit
			// this once we support chunking/decomposition,
			// as in that case it is insufficient.
			if dp.context.Err() != nil || dp.fileReadSemaphore.Acquire(dp.context, 1) != nil {
				return util.StatusFromContext(dp.context)
			}
			dp.group.Go(func() error {
				var b [1]byte
				_, err := dp.contentAddressableStorage.Get(dp.context, fileDigest).ReadAt(b[:], 0)
				dp.fileReadSemaphore.Release(1)
				if err != nil && err != io.EOF && status.Code(err) != codes.Canceled {
					return util.StatusWrapf(err, "Failed to prefetch file %#v", childPathTrace.String())
				}
				return nil
			})
		}
	}
	for _, directory := range directory.Directories {
		component, ok := path.NewComponent(directory.Name)
		if !ok {
			return status.Errorf(codes.InvalidArgument, "Directory %#v in directory %#v has an invalid name", directory.Name, pathTrace.String())
		}
		if err := dp.prefetchRecursively(pathTrace.Append(component), directoryPathHashes.AppendComponent(component), directory.Digest); err != nil {
			return err
		}
	}
	return nil
}
