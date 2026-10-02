package runner

import (
	"io"
	"os"
	"sort"

	"github.com/buildbarn/bb-storage/pkg/filesystem"
	"github.com/buildbarn/bb-storage/pkg/filesystem/path"
	"github.com/buildbarn/bb-storage/pkg/util"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// toolInputTree is a prefix tree of the paths of the input files that
// belong to the tool that a persistent worker process runs, as opposed
// to the data that a single build action processes.
//
// A worker process outlives the build action that caused it to be
// launched, while the input root of that build action is removed as
// soon as it completes. Exposing the tool through symbolic links that
// point into the input root, as is done for all other input files,
// would therefore leave the process with an executable that no longer
// resolves. Every JVM derives 'java.home' from the location of its own
// executable at startup, so for tools such as Bazel's JavaBuilder this
// is fatal the moment the process is reused.
//
// The tool is instead given a home inside the execution root of the
// worker process itself, where it remains valid for as long as the
// process lives.
type toolInputTree struct {
	// Directories that merely need to be traversed to reach tool
	// inputs. These are created as real directories, whose
	// remaining contents are refreshed for every build action.
	directories map[path.Component]*toolInputTree
	// Files and directories that consist of nothing but tool
	// inputs. These are materialized once, before the worker
	// process is started, and left alone afterwards.
	entries map[path.Component]struct{}
}

func newToolInputTree() *toolInputTree {
	return &toolInputTree{
		directories: map[path.Component]*toolInputTree{},
		entries:     map[path.Component]struct{}{},
	}
}

// parseToolInputPaths converts the list of tool input paths provided by
// the client into a prefix tree.
//
// The working directory of the build action is rejected as a tool
// input, as its contents need to be refreshed for every build action.
// Clients have no reason to mark it, as it holds the outputs that the
// tool produces.
func parseToolInputPaths(toolInputPaths []string, workingDirectory []path.Component) (*toolInputTree, error) {
	t := newToolInputTree()
	for _, toolInputPath := range toolInputPaths {
		var componentWalker componentCollectingComponentWalker
		if err := path.Resolve(path.UNIXFormat.NewParser(toolInputPath), path.NewRelativeScopeWalker(&componentWalker)); err != nil {
			return nil, util.StatusWrapf(err, "Failed to resolve tool input path %#v", toolInputPath)
		}
		components := componentWalker.components
		if len(components) == 0 {
			return nil, status.Errorf(codes.InvalidArgument, "Tool input path %#v refers to the input root itself", toolInputPath)
		}
		if isPrefixOf(components, workingDirectory) {
			return nil, status.Errorf(codes.InvalidArgument, "Tool input path %#v contains the working directory of the build action", toolInputPath)
		}
		t.insert(components)
	}
	return t, nil
}

// isPrefixOf returns whether one sequence of pathname components refers
// to the same location as another, or to one of its ancestors.
func isPrefixOf(prefix, components []path.Component) bool {
	if len(prefix) > len(components) {
		return false
	}
	for i, component := range prefix {
		if component != components[i] {
			return false
		}
	}
	return true
}

func (t *toolInputTree) insert(components []path.Component) {
	n := t
	for _, name := range components[:len(components)-1] {
		if _, ok := n.entries[name]; ok {
			// An ancestor is already materialized in its
			// entirety, so there is nothing left to record.
			return
		}
		child, ok := n.directories[name]
		if !ok {
			child = newToolInputTree()
			n.directories[name] = child
		}
		n = child
	}
	name := components[len(components)-1]
	if _, ok := n.directories[name]; ok {
		// Already traversed to reach a tool input stored
		// underneath it. Keep descending into it, which is the
		// more conservative of the two options.
		return
	}
	n.entries[name] = struct{}{}
}

// contains returns whether a single pathname component inside a
// directory is part of the tool, meaning the execution root provides it
// instead of the input root.
func (t *toolInputTree) contains(name path.Component) bool {
	if t == nil {
		return false
	}
	if _, ok := t.entries[name]; ok {
		return true
	}
	_, ok := t.directories[name]
	return ok
}

// child returns the part of the tree that applies to a subdirectory.
func (t *toolInputTree) child(name path.Component) *toolInputTree {
	if t == nil {
		return nil
	}
	return t.directories[name]
}

func (t *toolInputTree) isEmpty() bool {
	return t == nil || (len(t.directories) == 0 && len(t.entries) == 0)
}

// sortedComponents returns the keys of a map of pathname components in
// sorted order, so that the file system is always accessed in a
// deterministic order.
func sortedComponents[V any](m map[path.Component]V) []path.Component {
	names := make([]path.Component, 0, len(m))
	for name := range m {
		names = append(names, name)
	}
	sort.Slice(names, func(i, j int) bool { return names[i].String() < names[j].String() })
	return names
}

// materializeToolInputs copies the input files belonging to the tool
// from the input root of the build action into the execution root of a
// persistent worker process. This only needs to happen once, as every
// build action that is executed by the same worker process is
// guaranteed to provide the same tool.
func materializeToolInputs(target, source filesystem.Directory, tree *toolInputTree) error {
	for _, name := range sortedComponents(tree.entries) {
		if err := copyEntry(target, source, name, true); err != nil {
			return util.StatusWrapfWithCode(err, codes.Internal, "Failed to materialize tool input %#v", name.String())
		}
	}
	for _, name := range sortedComponents(tree.directories) {
		if err := target.Mkdir(name, 0o777); err != nil && !os.IsExist(err) {
			return util.StatusWrapfWithCode(err, codes.Internal, "Failed to create directory %#v in the execution root of the persistent worker", name.String())
		}
		if err := enterBoth(target, source, name, func(targetChild, sourceChild filesystem.Directory) error {
			return materializeToolInputs(targetChild, sourceChild, tree.directories[name])
		}); err != nil {
			return err
		}
	}
	return nil
}

// enterBoth obtains handles to a subdirectory of both the execution
// root of a persistent worker process and the input root of a build
// action, and invokes a callback against them.
func enterBoth(target, source filesystem.Directory, name path.Component, f func(target, source filesystem.Directory) error) error {
	targetChild, err := target.EnterDirectory(name)
	if err != nil {
		return util.StatusWrapfWithCode(err, codes.Internal, "Failed to enter directory %#v in the execution root of the persistent worker", name.String())
	}
	defer targetChild.Close()
	sourceChild, err := source.EnterDirectory(name)
	if err != nil {
		return util.StatusWrapfWithCode(err, codes.Internal, "Failed to enter directory %#v of the input root", name.String())
	}
	defer sourceChild.Close()
	return f(targetChild, sourceChild)
}

// copyEntry recursively copies a file, directory or symbolic link from
// one directory to another.
//
// If tryLink is set, regular files are hard linked, so that their
// contents do not need to be stored twice. This only works if both
// directories are backed by the same file system, which is not the case
// for deployments that place build directories on a virtual file
// system. Copying is used as a fallback.
func copyEntry(target, source filesystem.Directory, name path.Component, tryLink bool) error {
	fileInfo, err := source.Lstat(name)
	if err != nil {
		return util.StatusWrapf(err, "Failed to determine the type of %#v", name.String())
	}
	switch fileType := fileInfo.Type(); fileType {
	case filesystem.FileTypeRegularFile:
		if tryLink {
			if err := source.Link(name, target, name); err == nil {
				return nil
			}
		}
		return copyRegularFile(target, source, name, fileInfo.IsExecutable())
	case filesystem.FileTypeSymlink:
		oldName, err := source.Readlink(name)
		if err != nil {
			return util.StatusWrapf(err, "Failed to read symbolic link %#v", name.String())
		}
		if err := target.Symlink(oldName, name); err != nil {
			return util.StatusWrapf(err, "Failed to create symbolic link %#v", name.String())
		}
		return nil
	case filesystem.FileTypeDirectory:
		if err := target.Mkdir(name, 0o777); err != nil && !os.IsExist(err) {
			return util.StatusWrapf(err, "Failed to create directory %#v", name.String())
		}
		return enterBoth(target, source, name, func(targetChild, sourceChild filesystem.Directory) error {
			entries, err := sourceChild.ReadDir()
			if err != nil {
				return util.StatusWrapf(err, "Failed to read contents of directory %#v", name.String())
			}
			for _, entry := range entries {
				childName := entry.Name()
				if err := copyEntry(targetChild, sourceChild, childName, tryLink); err != nil {
					return util.StatusWrapf(err, "Failed to copy %#v", childName.String())
				}
			}
			return nil
		})
	default:
		return status.Errorf(codes.InvalidArgument, "Cannot copy %#v, as it has file type %d", name.String(), fileType)
	}
}

func copyRegularFile(target, source filesystem.Directory, name path.Component, isExecutable bool) error {
	r, err := source.OpenRead(name)
	if err != nil {
		return util.StatusWrapf(err, "Failed to open %#v for reading", name.String())
	}
	defer r.Close()
	sizeBytes, err := r.Len()
	if err != nil {
		return util.StatusWrapf(err, "Failed to determine the size of %#v", name.String())
	}
	perm := os.FileMode(0o666)
	if isExecutable {
		perm = 0o777
	}
	w, err := target.OpenWrite(name, filesystem.CreateExcl(perm))
	if err != nil {
		return util.StatusWrapf(err, "Failed to open %#v for writing", name.String())
	}
	defer w.Close()
	if _, err := io.Copy(io.NewOffsetWriter(w, 0), io.NewSectionReader(r, 0, sizeBytes)); err != nil {
		return util.StatusWrapf(err, "Failed to copy the contents of %#v", name.String())
	}
	return nil
}
