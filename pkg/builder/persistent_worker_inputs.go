package builder

import (
	"context"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-remote-execution/pkg/cas"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/filesystem/path"
	"github.com/buildbarn/bb-storage/pkg/util"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func readPersistentWorkerInputs(ctx context.Context, fetcher cas.DirectoryFetcher, rootDigest digest.Digest) (map[string]*remoteexecution.FileNode, error) {
	inputs := map[string]*remoteexecution.FileNode{}
	activeDirectories := map[digest.Digest]struct{}{}
	var visit func(digest.Digest, *path.Trace) error
	visit = func(directoryDigest digest.Digest, directoryPath *path.Trace) error {
		if err := ctx.Err(); err != nil {
			return status.FromContextError(err).Err()
		}
		if _, ok := activeDirectories[directoryDigest]; ok {
			return status.Error(codes.InvalidArgument, "Input directory tree contains a cycle")
		}
		activeDirectories[directoryDigest] = struct{}{}
		defer delete(activeDirectories, directoryDigest)
		directory, err := fetcher.GetDirectory(ctx, directoryDigest)
		if err != nil {
			return util.StatusWrapf(err, "Failed to obtain input directory %q", directoryPath.GetUNIXString())
		}
		if directory == nil {
			return status.Error(codes.DataLoss, "Missing input directory message")
		}
		names := map[path.Component]struct{}{}
		childPath := func(name string) (*path.Trace, error) {
			component, ok := path.NewComponent(name)
			if !ok {
				return nil, status.Errorf(codes.InvalidArgument, "Invalid input name %q", name)
			}
			if _, ok := names[component]; ok {
				return nil, status.Errorf(codes.InvalidArgument, "Duplicate input name %q", name)
			}
			names[component] = struct{}{}
			return directoryPath.Append(component), nil
		}
		for _, file := range directory.Files {
			filePath, err := childPath(file.GetName())
			if err != nil {
				return err
			}
			if _, err := rootDigest.GetDigestFunction().NewDigestFromProto(file.GetDigest()); err != nil {
				return util.StatusWrapf(err, "Invalid digest for input %q", filePath.GetUNIXString())
			}
			inputs[filePath.GetUNIXString()] = file
		}
		for _, symlink := range directory.Symlinks {
			if _, err := childPath(symlink.GetName()); err != nil {
				return err
			}
		}
		for _, child := range directory.Directories {
			childTrace, err := childPath(child.GetName())
			if err != nil {
				return err
			}
			childDigest, err := rootDigest.GetDigestFunction().NewDigestFromProto(child.GetDigest())
			if err != nil {
				return util.StatusWrapf(err, "Invalid digest for input directory %q", childTrace.GetUNIXString())
			}
			if err := visit(childDigest, childTrace); err != nil {
				return err
			}
		}
		return nil
	}
	err := visit(rootDigest, nil)
	return inputs, err
}
