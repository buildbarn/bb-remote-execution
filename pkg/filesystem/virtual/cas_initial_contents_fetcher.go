package virtual

import (
	"context"

	"github.com/buildbarn/bb-remote-execution/pkg/cas"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/filesystem/path"
	"github.com/buildbarn/bb-storage/pkg/util"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type casInitialContentsFetcherOptions struct {
	context        context.Context
	casFileFactory CASFileFactory
	symlinkFactory SymlinkFactory
	digestFunction digest.Function
}

type casInitialContentsFetcher struct {
	options         *casInitialContentsFetcherOptions
	directoryWalker cas.DirectoryWalker
}

// NewCASInitialContentsFetcher creates an InitialContentsFetcher that
// lazily instantiates a full directory hierarchy based on directory
// objects stored in the Content Addressable Storage (CAS).
//
// Upon request, it loads the root directory of the tree and converts
// all of the children to either additional InitialContentFetchers
// (directories), FileBackedFiles (regular files) or Symlinks (symbolic
// links).
func NewCASInitialContentsFetcher(ctx context.Context, directoryWalker cas.DirectoryWalker, casFileFactory CASFileFactory, symlinkFactory SymlinkFactory, digestFunction digest.Function) InitialContentsFetcher {
	return &casInitialContentsFetcher{
		options: &casInitialContentsFetcherOptions{
			context:        ctx,
			casFileFactory: casFileFactory,
			symlinkFactory: symlinkFactory,
			digestFunction: digestFunction,
		},
		directoryWalker: directoryWalker,
	}
}

func (icf *casInitialContentsFetcher) fetchContentsUnwrapped(fileReadMonitorFactory FileReadMonitorFactory) (map[path.Component]InitialChild, error) {
	directory, err := icf.directoryWalker.GetDirectory(icf.options.context)
	if err != nil {
		return nil, err
	}

	// Create InitialContentsFetchers for all child directories.
	// These can yield even more InitialContentsFetchers for
	// grandchildren.
	children := make(map[path.Component]InitialChild, len(directory.Directories)+len(directory.Files)+len(directory.Symlinks))
	for _, entry := range directory.Directories {
		component, ok := path.NewComponent(entry.Name)
		if !ok {
			return nil, status.Errorf(codes.InvalidArgument, "Directory %#v has an invalid name", entry.Name)
		}
		if _, ok := children[component]; ok {
			return nil, status.Errorf(codes.InvalidArgument, "Directory contains multiple children named %#v", entry.Name)
		}

		childDigest, err := icf.options.digestFunction.NewDigestFromProto(entry.Digest)
		if err != nil {
			return nil, util.StatusWrapf(err, "Failed to obtain digest for directory %#v", entry.Name)
		}
		children[component] = InitialChild{}.FromDirectory(&casInitialContentsFetcher{
			options:         icf.options,
			directoryWalker: icf.directoryWalker.GetChild(childDigest),
		})
	}

	// Ensure that leaves are properly unlinked if this method fails.
	leavesToUnlink := make([]LinkableLeaf, 0, len(directory.Files)+len(directory.Symlinks))
	defer func() {
		for _, leaf := range leavesToUnlink {
			leaf.Unlink()
		}
	}()

	// Create Content Addressable Storage backed read-only files.
	for _, entry := range directory.Files {
		component, ok := path.NewComponent(entry.Name)
		if !ok {
			return nil, status.Errorf(codes.InvalidArgument, "File %#v has an invalid name", entry.Name)
		}
		if _, ok := children[component]; ok {
			return nil, status.Errorf(codes.InvalidArgument, "Directory contains multiple children named %#v", entry.Name)
		}

		childDigest, err := icf.options.digestFunction.NewDigestFromProto(entry.Digest)
		if err != nil {
			return nil, util.StatusWrapf(err, "Failed to obtain digest for file %#v", entry.Name)
		}
		leaf := icf.options.casFileFactory.LookupFile(childDigest, entry.IsExecutable, fileReadMonitorFactory(component))
		children[component] = InitialChild{}.FromLeaf(leaf)
		leavesToUnlink = append(leavesToUnlink, leaf)
	}

	// Create symbolic links.
	for _, entry := range directory.Symlinks {
		component, ok := path.NewComponent(entry.Name)
		if !ok {
			return nil, status.Errorf(codes.InvalidArgument, "Symlink %#v has an invalid name", entry.Name)
		}
		if _, ok := children[component]; ok {
			return nil, status.Errorf(codes.InvalidArgument, "Directory contains multiple children named %#v", entry.Name)
		}

		leaf := icf.options.symlinkFactory.LookupSymlink([]byte(entry.Target))
		children[component] = InitialChild{}.FromLeaf(leaf)
		leavesToUnlink = append(leavesToUnlink, leaf)
	}

	leavesToUnlink = nil
	return children, nil
}

func (icf *casInitialContentsFetcher) FetchContents(fileReadMonitorFactory FileReadMonitorFactory) (map[path.Component]InitialChild, error) {
	children, err := icf.fetchContentsUnwrapped(fileReadMonitorFactory)
	if err != nil {
		return nil, util.StatusWrap(err, icf.directoryWalker.GetDescription())
	}
	return children, nil
}

func (icf *casInitialContentsFetcher) GetContainingDigests(ctx context.Context) (digest.Set, error) {
	gatherer := casContainingDigestsGatherer{
		context:             ctx,
		digestFunction:      icf.options.digestFunction,
		digests:             digest.NewSetBuilder(),
		directoriesGathered: map[digest.Digest]struct{}{},
	}
	err := gatherer.traverse(icf.directoryWalker)
	if err != nil {
		return digest.EmptySet, err
	}
	return gatherer.digests.Build(), nil
}

// casContainingDigestsGatherer is used by casInitialContentsFetcher's
// GetContainingDigests() to compute the transitive closure of digests
// referenced by a hierarchy of Directory objects.
type casContainingDigestsGatherer struct {
	context             context.Context
	digestFunction      digest.Function
	digests             digest.SetBuilder
	directoriesGathered map[digest.Digest]struct{}
}

func (g *casContainingDigestsGatherer) traverse(directoryWalker cas.DirectoryWalker) error {
	// Add the directory itself.
	g.digests.Add(directoryWalker.GetContainingDigest())

	directory, err := directoryWalker.GetDirectory(g.context)
	if err != nil {
		return util.StatusWrap(err, directoryWalker.GetDescription())
	}

	// Recursively traverse all child directories. Ignore
	// directories that were processed before, as we don't want to
	// be tricked into performing an exponential number of
	// traversals against malicious Tree objects.
	for _, entry := range directory.Directories {
		childDigest, err := g.digestFunction.NewDigestFromProto(entry.Digest)
		if err != nil {
			return util.StatusWrapf(err, "%s: Failed to obtain digest for directory %#v", directoryWalker.GetDescription(), entry.Name)
		}
		if _, ok := g.directoriesGathered[childDigest]; !ok {
			g.directoriesGathered[childDigest] = struct{}{}
			if err := g.traverse(directoryWalker.GetChild(childDigest)); err != nil {
				return err
			}
		}
	}

	for _, entry := range directory.Files {
		childDigest, err := g.digestFunction.NewDigestFromProto(entry.Digest)
		if err != nil {
			return util.StatusWrapf(err, "%s: Failed to obtain digest for file %#v", directoryWalker.GetDescription(), entry.Name)
		}
		g.digests.Add(childDigest)
	}

	return nil
}
