package builder

import (
	"context"
	"os"
	"path"
	"sort"
	"strings"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-storage/pkg/cas"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/filesystem"
	"github.com/buildbarn/bb-storage/pkg/util"
	"github.com/golang/protobuf/proto"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// PathBuffer is a helper type for generating pathname strings. It is
// used to by OutputHierarchy to generate error messages that contain
// pathnames.
type pathBuffer struct {
	components []string
}

func newPathBuffer() pathBuffer {
	return pathBuffer{
		components: []string{"."},
	}
}

// Enter a directory. Create one more slot for storing a filename.
func (pb *pathBuffer) enter() {
	pb.components = append(pb.components, "")
}

// SetLastComponent updates the last component of the pathname.
func (pb *pathBuffer) setLastComponent(component string) {
	pb.components[len(pb.components)-1] = component
}

// Leave a directory. Pop the last filename off the pathname.
func (pb *pathBuffer) leave() {
	pb.components = pb.components[:len(pb.components)-1]
}

// Join all components of the pathname together and return its string
// representation.
func (pb *pathBuffer) join() string {
	return path.Join(pb.components...)
}

// OutputNode is a node in a directory hierarchy that contains one or
// more locations where output directories and files are expected.
type outputNode struct {
	directoriesToUpload map[string][]string
	filesToUpload       map[string][]string
	pathsToUpload       map[string][]string
	subdirectories      map[string]*outputNode
}

func (on *outputNode) getSubdirectoryNames() []string {
	l := make([]string, 0, len(on.subdirectories))
	for k := range on.subdirectories {
		l = append(l, k)
	}
	sort.Strings(l)
	return l
}

func sortToUpload(m map[string][]string) []string {
	l := make([]string, 0, len(m))
	for k := range m {
		l = append(l, k)
	}
	sort.Strings(l)
	return l
}

// NewOutputDirectory creates a new outputNode that is in the initial
// state. It contains no locations where output directories or files are
// expected.
func newOutputDirectory() *outputNode {
	return &outputNode{
		directoriesToUpload: map[string][]string{},
		filesToUpload:       map[string][]string{},
		pathsToUpload:       map[string][]string{},
		subdirectories:      map[string]*outputNode{},
	}
}

// CreateParentDirectories is recursive invoked by
// OutputHierarchy.CreateParentDirectories() to create parent
// directories of locations where output directories and files are
// expected.
func (on *outputNode) createParentDirectories(d filesystem.Directory, dPath *pathBuffer) error {
	dPath.enter()
	defer dPath.leave()

	for _, name := range on.getSubdirectoryNames() {
		dPath.setLastComponent(name)
		if err := d.Mkdir(name, 0777); err != nil && !os.IsExist(err) {
			return util.StatusWrapf(err, "Failed to create output parent directory %#v", dPath.join())
		}

		// Recurse if we need to create one or more directories within.
		if child := on.subdirectories[name]; len(child.subdirectories) > 0 || len(child.directoriesToUpload) > 0 {
			childDirectory, err := d.EnterDirectory(name)
			if err != nil {
				return util.StatusWrapf(err, "Failed to enter output parent directory %#v", dPath.join())
			}
			err = child.createParentDirectories(childDirectory, dPath)
			childDirectory.Close()
			if err != nil {
				return err
			}
		}
	}

	// Although REv2 explicitly documents that only parents of
	// output directories are created (i.e., not the output
	// directory itself), Bazel changed its behaviour and now
	// creates output directories when using local execution. See
	// these issues for details:
	//
	// https://github.com/bazelbuild/bazel/issues/6262
	// https://github.com/bazelbuild/bazel/issues/6393
	//
	// Considering that the 'output_directories' field is deprecated
	// in REv2.1 anyway, be consistent with Bazel's local execution.
	// Once Bazel switches to REv2.1, it will be forced to solve
	// this matter in a protocol conforming way.
	for _, name := range sortToUpload(on.directoriesToUpload) {
		if _, ok := on.subdirectories[name]; !ok {
			dPath.setLastComponent(name)
			if err := d.Mkdir(name, 0777); err != nil && !os.IsExist(err) {
				return util.StatusWrapf(err, "Failed to create output directory %#v", dPath.join())
			}
		}
	}
	return nil
}

// UploadOutputs is recursively invoked by
// OutputHierarchy.UploadOutputs() to upload output directories and
// files from the locations where they are expected.
func (on *outputNode) uploadOutputs(s *uploadOutputsState, d filesystem.Directory) {
	s.dPath.enter()
	defer s.dPath.leave()

	// Upload REv2.0 output directories that are expected to be
	// present in this directory.
	for _, component := range sortToUpload(on.directoriesToUpload) {
		s.dPath.setLastComponent(component)
		paths := on.directoriesToUpload[component]
		if fileInfo, err := d.Lstat(component); err == nil {
			switch fileType := fileInfo.Type(); fileType {
			case filesystem.FileTypeDirectory:
				s.uploadOutputDirectory(d, component, paths)
			case filesystem.FileTypeSymlink:
				s.uploadOutputSymlink(d, component, &s.actionResult.OutputDirectorySymlinks, paths)
			default:
				s.saveError(status.Errorf(codes.InvalidArgument, "Output directory %#v is not a directory or symlink", s.dPath.join()))
			}
		} else if !os.IsNotExist(err) {
			s.saveError(util.StatusWrapf(err, "Failed to read attributes of output directory %#v", s.dPath.join()))
		}
	}

	// Upload REv2.0 output files that are expected to be present in
	// this directory.
	for _, component := range sortToUpload(on.filesToUpload) {
		s.dPath.setLastComponent(component)
		paths := on.filesToUpload[component]
		if fileInfo, err := d.Lstat(component); err == nil {
			switch fileType := fileInfo.Type(); fileType {
			case filesystem.FileTypeRegularFile, filesystem.FileTypeExecutableFile:
				s.uploadOutputFile(d, component, fileType, paths)
			case filesystem.FileTypeSymlink:
				s.uploadOutputSymlink(d, component, &s.actionResult.OutputFileSymlinks, paths)
			default:
				s.saveError(status.Errorf(codes.InvalidArgument, "Output file %#v is not a regular file or symlink", s.dPath.join()))
			}
		} else if !os.IsNotExist(err) {
			s.saveError(util.StatusWrapf(err, "Failed to read attributes of output file %#v", s.dPath.join()))
		}
	}

	// Upload REv2.1 output paths that are expected to be present in
	// this directory.
	for _, component := range sortToUpload(on.pathsToUpload) {
		s.dPath.setLastComponent(component)
		paths := on.pathsToUpload[component]
		if fileInfo, err := d.Lstat(component); err == nil {
			switch fileType := fileInfo.Type(); fileType {
			case filesystem.FileTypeDirectory:
				s.uploadOutputDirectory(d, component, paths)
			case filesystem.FileTypeRegularFile, filesystem.FileTypeExecutableFile:
				s.uploadOutputFile(d, component, fileType, paths)
			case filesystem.FileTypeSymlink:
				s.uploadOutputSymlink(d, component, &s.actionResult.OutputSymlinks, paths)
			default:
				s.saveError(status.Errorf(codes.InvalidArgument, "Output path %#v is not a directory, regular file or symlink", s.dPath.join()))
			}
		} else if !os.IsNotExist(err) {
			s.saveError(util.StatusWrapf(err, "Failed to read attributes of output path %#v", s.dPath.join()))
		}
	}

	// Traverse into subdirectories.
	for _, component := range on.getSubdirectoryNames() {
		s.dPath.setLastComponent(component)
		childNode := on.subdirectories[component]
		if childDirectory, err := d.EnterDirectory(component); err == nil {
			childNode.uploadOutputs(s, childDirectory)
			childDirectory.Close()
		} else if !os.IsNotExist(err) {
			s.saveError(util.StatusWrapf(err, "Failed to enter output parent directory %#v", s.dPath.join()))
		}
	}
}

// UploadOutputsState is used by OutputHierarchy.UploadOutputs() to
// track common parameters during recursion.
type uploadOutputsState struct {
	context                   context.Context
	contentAddressableStorage cas.ContentAddressableStorage
	parentDigest              digest.Digest
	actionResult              *remoteexecution.ActionResult

	dPath      pathBuffer
	firstError error
}

// SaveError preserves errors that occur during uploading. Even when
// errors occur, the remainder of the output files is still uploaded.
// This makes debugging easier.
func (s *uploadOutputsState) saveError(err error) {
	if s.firstError == nil {
		s.firstError = err
	}
}

// UploadDirectory is called to upload a single directory. Elements in
// the directory are stored in a remoteexecution.Directory, so that they
// can be placed in a remoteexecution.Tree.
func (s *uploadOutputsState) uploadDirectory(d filesystem.Directory, children map[digest.Digest]*remoteexecution.Directory) *remoteexecution.Directory {
	files, err := d.ReadDir()
	if err != nil {
		s.saveError(util.StatusWrapf(err, "Failed to read output directory %#v", s.dPath.join()))
		return &remoteexecution.Directory{}
	}

	s.dPath.enter()
	defer s.dPath.leave()

	var directory remoteexecution.Directory
	for _, file := range files {
		name := file.Name()
		s.dPath.setLastComponent(name)
		switch fileType := file.Type(); fileType {
		case filesystem.FileTypeRegularFile, filesystem.FileTypeExecutableFile:
			if childDigest, err := s.contentAddressableStorage.PutFile(s.context, d, name, s.parentDigest); err == nil {
				directory.Files = append(directory.Files, &remoteexecution.FileNode{
					Name:         name,
					Digest:       childDigest.GetProto(),
					IsExecutable: fileType == filesystem.FileTypeExecutableFile,
				})
			} else {
				s.saveError(util.StatusWrapf(err, "Failed to store output file %#v", s.dPath.join()))
			}
		case filesystem.FileTypeDirectory:
			if childDirectory, err := d.EnterDirectory(name); err == nil {
				child := s.uploadDirectory(childDirectory, children)
				childDirectory.Close()

				// Compute digest of the child directory. This requires serializing it.
				if data, err := proto.Marshal(child); err == nil {
					digestGenerator := s.parentDigest.NewGenerator()
					if _, err := digestGenerator.Write(data); err == nil {
						childDigest := digestGenerator.Sum()
						children[childDigest] = child
						directory.Directories = append(directory.Directories, &remoteexecution.DirectoryNode{
							Name:   name,
							Digest: childDigest.GetProto(),
						})
					} else {
						s.saveError(util.StatusWrapf(err, "Failed to compute digest of output directory %#v", s.dPath.join()))
					}
				} else {
					s.saveError(util.StatusWrapf(err, "Failed to marshal output directory %#v", s.dPath.join()))
				}
			} else {
				s.saveError(util.StatusWrapf(err, "Failed to enter output directory %#v", s.dPath.join()))
			}
		case filesystem.FileTypeSymlink:
			if target, err := d.Readlink(name); err == nil {
				directory.Symlinks = append(directory.Symlinks, &remoteexecution.SymlinkNode{
					Name:   name,
					Target: target,
				})
			} else {
				s.saveError(util.StatusWrapf(err, "Failed to read output symlink %#v", s.dPath.join()))
			}
		}
	}
	return &directory
}

// UploadOutputDirectoryEntered is called to upload a single output
// directory as a remoteexecution.Tree. The root directory is assumed to
// already be opened.
func (s *uploadOutputsState) uploadOutputDirectoryEntered(d filesystem.Directory, paths []string) {
	children := map[digest.Digest]*remoteexecution.Directory{}
	tree := &remoteexecution.Tree{
		Root: s.uploadDirectory(d, children),
	}

	childDigests := digest.NewSetBuilder()
	for childDigest := range children {
		childDigests.Add(childDigest)
	}
	for _, childDigest := range childDigests.Build().Items() {
		tree.Children = append(tree.Children, children[childDigest])
	}

	if treeDigest, err := s.contentAddressableStorage.PutTree(s.context, tree, s.parentDigest); err == nil {
		for _, path := range paths {
			s.actionResult.OutputDirectories = append(
				s.actionResult.OutputDirectories,
				&remoteexecution.OutputDirectory{
					Path:       path,
					TreeDigest: treeDigest.GetProto(),
				})
		}
	} else {
		s.saveError(util.StatusWrapf(err, "Failed to store output directory %#v", s.dPath.join()))
	}
}

// UploadOutputDirectory is called to upload a single output directory
// as a remoteexecution.Tree. The root directory is opened opened by
// this function.
func (s *uploadOutputsState) uploadOutputDirectory(d filesystem.Directory, name string, paths []string) {
	if childDirectory, err := d.EnterDirectory(name); err == nil {
		s.uploadOutputDirectoryEntered(childDirectory, paths)
		childDirectory.Close()
	} else {
		s.saveError(util.StatusWrapf(err, "Failed to enter output directory %#v", s.dPath.join()))
	}
}

// UploadOutputDirectory is called to upload a single output file.
func (s *uploadOutputsState) uploadOutputFile(d filesystem.Directory, name string, fileType filesystem.FileType, paths []string) {
	if digest, err := s.contentAddressableStorage.PutFile(s.context, d, name, s.parentDigest); err == nil {
		for _, path := range paths {
			s.actionResult.OutputFiles = append(
				s.actionResult.OutputFiles,
				&remoteexecution.OutputFile{
					Path:         path,
					Digest:       digest.GetProto(),
					IsExecutable: fileType == filesystem.FileTypeExecutableFile,
				})
		}
	} else {
		s.saveError(util.StatusWrapf(err, "Failed to store output file %#v", s.dPath.join()))
	}
}

// UploadOutputDirectory is called to read the attributes of a single
// output symlink.
func (s *uploadOutputsState) uploadOutputSymlink(d filesystem.Directory, name string, outputSymlinks *[]*remoteexecution.OutputSymlink, paths []string) {
	if target, err := d.Readlink(name); err == nil {
		for _, path := range paths {
			*outputSymlinks = append(
				*outputSymlinks,
				&remoteexecution.OutputSymlink{
					Path:   path,
					Target: target,
				})
		}
	} else {
		s.saveError(util.StatusWrapf(err, "Failed to read output symlink %#v", s.dPath.join()))
	}
}

// OutputHierarchy is used by LocalBuildExecutor to track output
// directories and files that are expected to be generated by the build
// action. OutputHierarchy can be used to create parent directories of
// outputs prior to execution, and to upload outputs into the CAS after
// execution.
type OutputHierarchy struct {
	workingDirectory string
	root             outputNode
	rootsToUpload    []string
}

// NewOutputHierarchy creates a new OutputHierarchy that is in the
// initial state. It contains no output directories or files.
func NewOutputHierarchy(workingDirectory string) *OutputHierarchy {
	return &OutputHierarchy{
		workingDirectory: workingDirectory,
		root:             *newOutputDirectory(),
	}
}

func (oh *OutputHierarchy) pathToComponents(p string) ([]string, string, bool) {
	// Join path with working directory and obtain pathname components.
	rawComponents := strings.FieldsFunc(
		path.Join(oh.workingDirectory, p),
		func(r rune) bool { return r == '/' })
	components := make([]string, 0, len(rawComponents))
	for _, component := range rawComponents {
		if component != "." {
			components = append(components, component)
		}
	}
	if len(components) == 0 {
		// Pathname expands to the root directory.
		return nil, "", false
	}
	// Pathname expands to a location underneath the root directory.
	return components[:len(components)-1], components[len(components)-1], true
}

func (oh *OutputHierarchy) lookup(components []string) *outputNode {
	on := &oh.root
	for _, component := range components {
		child, ok := on.subdirectories[component]
		if !ok {
			child = newOutputDirectory()
			on.subdirectories[component] = child
		}
		on = child
	}
	return on
}

// AddDirectory is called to indicate that the build action is expected
// to create an REv2.0 output directory.
func (oh *OutputHierarchy) AddDirectory(path string) {
	if dirName, fileName, notRoot := oh.pathToComponents(path); notRoot {
		on := oh.lookup(dirName)
		on.directoriesToUpload[fileName] = append(on.directoriesToUpload[fileName], path)
	} else {
		oh.rootsToUpload = append(oh.rootsToUpload, path)
	}
}

// AddFile is called to indicate that the build action is expected to
// create an REv2.0 output file.
func (oh *OutputHierarchy) AddFile(path string) {
	if dirName, fileName, notRoot := oh.pathToComponents(path); notRoot {
		on := oh.lookup(dirName)
		on.filesToUpload[fileName] = append(on.filesToUpload[fileName], path)
	}
}

// AddPath is called to indicate that the build action is expected to
// create an REv2.1 output path.
func (oh *OutputHierarchy) AddPath(path string) {
	if dirName, fileName, notRoot := oh.pathToComponents(path); notRoot {
		on := oh.lookup(dirName)
		on.pathsToUpload[fileName] = append(on.pathsToUpload[fileName], path)
	} else {
		oh.rootsToUpload = append(oh.rootsToUpload, path)
	}
}

// CreateParentDirectories creates parent directories of outputs. This
// function is called prior to executing the build action.
func (oh *OutputHierarchy) CreateParentDirectories(d filesystem.Directory) error {
	dPath := newPathBuffer()
	return oh.root.createParentDirectories(d, &dPath)
}

// UploadOutputs uploads outputs of the build action into the CAS. This
// function is called after executing the build action.
func (oh *OutputHierarchy) UploadOutputs(ctx context.Context, d filesystem.Directory, contentAddressableStorage cas.ContentAddressableStorage, parentDigest digest.Digest, actionResult *remoteexecution.ActionResult) error {
	s := uploadOutputsState{
		context:                   ctx,
		contentAddressableStorage: contentAddressableStorage,
		parentDigest:              parentDigest,
		actionResult:              actionResult,

		dPath: newPathBuffer(),
	}

	if len(oh.rootsToUpload) > 0 {
		s.uploadOutputDirectoryEntered(d, oh.rootsToUpload)
	}
	oh.root.uploadOutputs(&s, d)
	return s.firstError
}
