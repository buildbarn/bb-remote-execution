package builder

import (
	"context"
	"log"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	re_cas "github.com/buildbarn/bb-remote-execution/pkg/cas"
	"github.com/buildbarn/bb-remote-execution/pkg/filesystem/access"
	"github.com/buildbarn/bb-remote-execution/pkg/filesystem/pool"
	"github.com/buildbarn/bb-remote-execution/pkg/proto/remoteworker"
	"github.com/buildbarn/bb-storage/pkg/blobstore"
	"github.com/buildbarn/bb-storage/pkg/blobstore/chunk"
	"github.com/buildbarn/bb-storage/pkg/cas"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/filesystem/path"
	"github.com/buildbarn/bb-storage/pkg/proto/fsac"
	"github.com/buildbarn/bb-storage/pkg/util"

	"golang.org/x/sync/errgroup"
	"golang.org/x/sync/semaphore"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
)

type prefetchingBuildExecutor struct {
	BuildExecutor
	readInfo                    cas.ReadInfo
	chunkStorage                blobstore.BlobAccess[*chunk.Chunk]
	directoryFetcher            re_cas.DirectoryFetcher
	fileReadSemaphore           *semaphore.Weighted
	fileSystemAccessCache       blobstore.BlobAccess[*fsac.FileSystemAccessProfile]
	bloomFilterBitsPerElement   int
	bloomFilterMaximumSizeBytes int
	logFileSystemAccessProfile  bool
	emptyProfile                *fsac.FileSystemAccessProfile
	protoPutter                 re_cas.ProtoPutter
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
func NewPrefetchingBuildExecutor(buildExecutor BuildExecutor, readInfo cas.ReadInfo, chunkStorage blobstore.BlobAccess[*chunk.Chunk], directoryFetcher re_cas.DirectoryFetcher, fileReadSemaphore *semaphore.Weighted, fileSystemAccessCache blobstore.BlobAccess[*fsac.FileSystemAccessProfile], bloomFilterBitsPerElement, bloomFilterMaximumSizeBytes int, logFileSystemAccessProfile bool, protoPutter re_cas.ProtoPutter) BuildExecutor {
	be := &prefetchingBuildExecutor{
		BuildExecutor:               buildExecutor,
		readInfo:                    readInfo,
		chunkStorage:                chunkStorage,
		directoryFetcher:            directoryFetcher,
		fileReadSemaphore:           fileReadSemaphore,
		fileSystemAccessCache:       fileSystemAccessCache,
		bloomFilterBitsPerElement:   bloomFilterBitsPerElement,
		bloomFilterMaximumSizeBytes: bloomFilterMaximumSizeBytes,
		logFileSystemAccessProfile:  logFileSystemAccessProfile,
		protoPutter:                 protoPutter,
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

func (be *prefetchingBuildExecutor) Execute(ctx context.Context, filePool pool.FilePool, monitor access.UnreadDirectoryMonitor, digestFunction digest.Function, request *remoteworker.DesiredState_Executing, executionStateUpdates chan<- *remoteworker.CurrentState_Executing) *remoteexecution.ExecuteResponse {
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
		profileMessage, err := be.fileSystemAccessCache.Get(groupCtx, reducedActionDigest)
		if err != nil {
			if status.Code(err) == codes.NotFound {
				existingProfile = be.emptyProfile
				return nil
			}
			return util.StatusWrap(err, "Failed to fetch file system access profile")
		}
		existingProfile = profileMessage

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
			context:           prefetchCtx,
			group:             group,
			bloomFilter:       bloomFilter,
			digestFunction:    digestFunction,
			readInfo:          be.readInfo,
			chunkStorage:      be.chunkStorage,
			directoryFetcher:  be.directoryFetcher,
			fileReadSemaphore: be.fileReadSemaphore,
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

	err = group.Wait()
	newProfile := be.computeProfile(bloomFilterMonitor)
	if err == nil {
		// Action completed successfully. Store an updated
		// profile in the File System Access Cache if it is
		// different from the one fetched previously.
		if !proto.Equal(existingProfile, newProfile) {
			if err := be.fileSystemAccessCache.Put(ctx, reducedActionDigest, newProfile); err != nil {
				attachErrorToExecuteResponse(response, util.StatusWrap(err, "Failed to store file system access profile to FSAC"))
			}
		}
	} else if err != (dontReadFromFSACError{}) {
		response.Status = status.Convert(err).Proto()
	}

	if be.logFileSystemAccessProfile {
		// Store a copy of the profile in content addressable storage
		// and attach it to the response server logs if profile
		// logging is turned on.
		if profileDigest, err := be.protoPutter.PutProto(ctx, newProfile, digestFunction); err == nil {
			if response.ServerLogs == nil {
				response.ServerLogs = make(map[string]*remoteexecution.LogFile)
			}
			response.ServerLogs["buildbarn.fsac.FileSystemAccessProfile"] = &remoteexecution.LogFile{
				Digest:        profileDigest.GetProto(),
				HumanReadable: false,
			}
		} else {
			attachErrorToExecuteResponse(response, util.StatusWrap(err, "Failed to store file system access profile to CAS"))
		}
	}

	if resourceUsage, err := anypb.New(bloomFilterMonitor.GetInputRootResourceUsage()); err == nil {
		response.Result.ExecutionMetadata.AuxiliaryMetadata = append(response.Result.ExecutionMetadata.AuxiliaryMetadata, resourceUsage)
	} else {
		attachErrorToExecuteResponse(response, util.StatusWrap(err, "Failed to marshal input root resource usage"))
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
	context           context.Context
	group             *errgroup.Group
	bloomFilter       *access.BloomFilterReader
	digestFunction    digest.Function
	readInfo          cas.ReadInfo
	chunkStorage      blobstore.BlobAccess[*chunk.Chunk]
	directoryFetcher  re_cas.DirectoryFetcher
	fileReadSemaphore *semaphore.Weighted
}

func (dp *directoryPrefetcher) shouldPrefetch(pathHashes access.PathHashes) bool {
	return dp.bloomFilter.Contains(pathHashes)
}

// prefetchFile ensures that the data making up a file is present in the
// Chunk Storage (CS). Files that are stored as a single chunk are
// prefetched directly. Larger files are decomposed into their
// constituent chunks by fetching the chunk mapping, which also warms the
// Chunk Mapping Storage (CMS).
//
// Because a file may be made up of multiple chunks, a NOT_FOUND error
// observed while prefetching is translated into a FAILED_PRECONDITION
// error that refers to the file as a whole. This ensures that the client
// is asked to upload the file, as opposed to one of its chunks.
func (dp *directoryPrefetcher) prefetchFile(ctx context.Context, d digest.Digest) error {
	if d.GetSizeBytes() == 0 {
		// By definition in the Remote Execution API, a zero-sized
		// blob is always present.
		return nil
	}
	params, err := dp.readInfo.CDCParametersFetcher.FetchCDCParameters(ctx, d.GetInstanceName())
	if err != nil {
		return util.StatusWrap(err, "Failed to fetch CDC parameters")
	}
	if cas.IsSingleChunk(params, d) {
		return re_cas.FailedPreconditionOnMissingBlob(d, dp.chunkStorage.Prefetch(ctx, d.ToSingletonSet()))
	}
	mapping, err := dp.readInfo.ChunkMappingFetcher.FetchChunkMapping(ctx, d)
	if err != nil {
		return re_cas.FailedPreconditionOnMissingBlob(d, util.StatusWrapf(err, "Failed to fetch chunk mapping for file %s", d))
	}
	chunks := digest.NewSetBuilder(mapping.Length())
	for i := range mapping.Length() {
		chunks.Add(mapping.GetDigestAtIndex(d.GetInstanceName(), i))
	}
	return re_cas.FailedPreconditionOnMissingBlob(d, dp.chunkStorage.Prefetch(ctx, chunks.Build()))
}

func (dp *directoryPrefetcher) prefetchRecursively(pathTrace *path.Trace, directoryPathHashes access.PathHashes, rawDirectoryDigest *remoteexecution.Digest) error {
	if !dp.shouldPrefetch(directoryPathHashes) {
		// This directory nor any of its children are expected
		// to be accessed.
		return nil
	}

	directoryDigest, err := dp.digestFunction.NewDigestFromProto(rawDirectoryDigest)
	if err != nil {
		return util.StatusWrapf(err, "Failed to parse digest for directory %#v", pathTrace.GetUNIXString())
	}
	directory, err := dp.directoryFetcher.GetDirectory(dp.context, directoryDigest)
	if err != nil {
		return util.StatusWrapf(err, "Failed to prefetch directory %#v", pathTrace.GetUNIXString())
	}

	// Directories are traversed sequentially to prevent unbounded
	// concurrency. Therefore, schedule prefetching of files before
	// directories, so that concurrency is introduced as soon as
	// possible.
	for _, file := range directory.Files {
		component, ok := path.NewComponent(file.Name)
		if !ok {
			return status.Errorf(codes.InvalidArgument, "File %#v in directory %#v has an invalid name", file.Name, pathTrace.GetUNIXString())
		}
		if dp.shouldPrefetch(directoryPathHashes.AppendComponent(component)) {
			childPathTrace := pathTrace.Append(component)
			fileDigest, err := dp.digestFunction.NewDigestFromProto(file.Digest)
			if err != nil {
				return util.StatusWrapf(err, "Failed to parse digest for file %#v", childPathTrace.GetUNIXString())
			}

			// Download files at a globally bounded concurrency.
			if err := util.AcquireSemaphore(dp.context, dp.fileReadSemaphore, 1); err != nil {
				return err
			}
			dp.group.Go(func() error {
				err := dp.prefetchFile(dp.context, fileDigest)
				dp.fileReadSemaphore.Release(1)
				if err != nil && status.Code(err) != codes.Canceled {
					return util.StatusWrapf(err, "Failed to prefetch file %#v", childPathTrace.GetUNIXString())
				}
				return nil
			})
		}
	}
	for _, directory := range directory.Directories {
		component, ok := path.NewComponent(directory.Name)
		if !ok {
			return status.Errorf(codes.InvalidArgument, "Directory %#v in directory %#v has an invalid name", directory.Name, pathTrace.GetUNIXString())
		}
		if err := dp.prefetchRecursively(pathTrace.Append(component), directoryPathHashes.AppendComponent(component), directory.Digest); err != nil {
			return err
		}
	}
	return nil
}
