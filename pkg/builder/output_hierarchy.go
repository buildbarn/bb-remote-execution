package builder

import (
	"context"
	"os"
	"path"
	"sort"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-storage/pkg/blobstore"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/filesystem"
	bb_path "github.com/buildbarn/bb-storage/pkg/filesystem/path"
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
func (pb *pathBuffer) setLastComponent(component bb_path.Component) {
	pb.components[len(pb.components)-1] = component.String()
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

// componentsList is a sortable list of filenames in a directory.
type componentsList []bb_path.Component

func (l componentsList) Len() int {
	return len(l)
}

func (l componentsList) Less(i, j int) bool {
	return l[i].String() < l[j].String()
}

func (l componentsList) Swap(i, j int) {
	l[i], l[j] = l[j], l[i]
}

// OutputNode is a node in a directory hierarchy that contains one or
// more locations where output directories and files are expected.
type outputNode struct {
	directoriesToUpload map[bb_path.Component][]string
	filesToUpload       map[bb_path.Component][]string
	pathsToUpload       map[bb_path.Component][]string
	subdirectories      map[bb_path.Component]*outputNode
}

func (on *outputNode) getSubdirectoryNames() []bb_path.Component {
	l := make(componentsList, 0, len(on.subdirectories))
	for k := range on.subdirectories {
		l = append(l, k)
	}
	sort.Sort(l)
	return l
}

func sortToUpload(m map[bb_path.Component][]string) []bb_path.Component {
	l := make(componentsList, 0, len(m))
	for k := range m {
		l = append(l, k)
	}
	sort.Sort(l)
	return l
}

// NewOutputDirectory creates a new outputNode that is in the initial
// state. It contains no locations where output directories or files are
// expected.
func newOutputDirectory() *outputNode {
	return &outputNode{
		directoriesToUpload: map[bb_path.Component][]string{},
		filesToUpload:       map[bb_path.Component][]string{},
		pathsToUpload:       map[bb_path.Component][]string{},
		subdirectories:      map[bb_path.Component]*outputNode{},
	}
}

// CreateParentDirectories is recursive invoked by
// OutputHierarchy.CreateParentDirectories() to create parent
// directories of locations where output directories and files are
// expected.
func (on *outputNode) createParentDirectories(d BuildDirectory, dPath *pathBuffer) error {
	dPath.enter()
	defer dPath.leave()

	for _, name := range on.getSubdirectoryNames() {
		dPath.setLastComponent(name)
		if err := d.Mkdir(name, 0777); err != nil && !os.IsExist(err) {
			return util.StatusWrapf(err, "Failed to create output parent directory %#v", dPath.join())
		}

		// Recurse if we need to create one or more directories within.
		if child := on.subdirectories[name]; len(child.subdirectories) > 0 || len(child.directoriesToUpload) > 0 {
			childDirectory, err := d.EnterBuildDirectory(name)
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
func (on *outputNode) uploadOutputs(s *uploadOutputsState, d UploadableDirectory) {
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
		if childDirectory, err := d.EnterUploadableDirectory(component); err == nil {
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
	contentAddressableStorage blobstore.BlobAccess
	digestFunction            digest.Function
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
func (s *uploadOutputsState) uploadDirectory(d UploadableDirectory, children map[digest.Digest]*remoteexecution.Directory) *remoteexecution.Directory {
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
			if childDigest, err := d.UploadFile(s.context, name, s.digestFunction); err == nil {
				directory.Files = append(directory.Files, &remoteexecution.FileNode{
					Name:         name.String(),
					Digest:       childDigest.GetProto(),
					IsExecutable: fileType == filesystem.FileTypeExecutableFile,
				})
			} else {
				s.saveError(util.StatusWrapf(err, "Failed to store output file %#v", s.dPath.join()))
			}
		case filesystem.FileTypeDirectory:
			if childDirectory, err := d.EnterUploadableDirectory(name); err == nil {
				child := s.uploadDirectory(childDirectory, children)
				childDirectory.Close()

				// Compute digest of the child directory. This requires serializing it.
				if data, err := proto.Marshal(child); err == nil {
					digestGenerator := s.digestFunction.NewGenerator()
					if _, err := digestGenerator.Write(data); err == nil {
						childDigest := digestGenerator.Sum()
						children[childDigest] = child
						directory.Directories = append(directory.Directories, &remoteexecution.DirectoryNode{
							Name:   name.String(),
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
					Name:   name.String(),
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
func (s *uploadOutputsState) uploadOutputDirectoryEntered(d UploadableDirectory, paths []string) {
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

	if treeDigest, err := blobstore.CASPutProto(s.context, s.contentAddressableStorage, tree, s.digestFunction); err == nil {
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
func (s *uploadOutputsState) uploadOutputDirectory(d UploadableDirectory, name bb_path.Component, paths []string) {
	if childDirectory, err := d.EnterUploadableDirectory(name); err == nil {
		s.uploadOutputDirectoryEntered(childDirectory, paths)
		childDirectory.Close()
	} else {
		s.saveError(util.StatusWrapf(err, "Failed to enter output directory %#v", s.dPath.join()))
	}
}

// UploadOutputDirectory is called to upload a single output file.
func (s *uploadOutputsState) uploadOutputFile(d UploadableDirectory, name bb_path.Component, fileType filesystem.FileType, paths []string) {
	if digest, err := d.UploadFile(s.context, name, s.digestFunction); err == nil {
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
func (s *uploadOutputsState) uploadOutputSymlink(d UploadableDirectory, name bb_path.Component, outputSymlinks *[]*remoteexecution.OutputSymlink, paths []string) {
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

// outputNodePath is an implementation of path.ComponentWalker that is
// used by NewOutputHierarchy() to compute normalized paths of outputs
// of a build action.
//
// It might have been cleaner if path.Resolve() was performed directly
// against the tree of outputNode objects. Unfortunately, the Remote
// Execution protocol requires us to create the parent directories of
// outputs, while the working directory needs to be part of the input
// root explicitly. Operating directly against outputNode objects would
// make it harder to achieve that.
type outputNodePath struct {
	components []bb_path.Component
}

func (onp *outputNodePath) OnDirectory(name bb_path.Component) (bb_path.GotDirectoryOrSymlink, error) {
	onp.components = append(onp.components, name)
	return bb_path.GotDirectory{
		Child:        onp,
		IsReversible: true,
	}, nil
}

func (onp *outputNodePath) OnTerminal(name bb_path.Component) (*bb_path.GotSymlink, error) {
	onp.components = append(onp.components, name)
	return nil, nil
}

func (onp *outputNodePath) OnUp() (bb_path.ComponentWalker, error) {
	if len(onp.components) == 0 {
		return nil, status.Error(codes.InvalidArgument, "Path resolves to a location outside the input root directory")
	}
	onp.components = onp.components[:len(onp.components)-1]
	return onp, nil
}

// OutputHierarchy is used by LocalBuildExecutor to track output
// directories and files that are expected to be generated by the build
// action. OutputHierarchy can be used to create parent directories of
// outputs prior to execution, and to upload outputs into the CAS after
// execution.
type OutputHierarchy struct {
	root          outputNode
	rootsToUpload []string
}

// NewOutputHierarchy creates a new OutputHierarchy that uses the
// working directory and the output paths specified in an REv2 Command
// message.
func NewOutputHierarchy(command *remoteexecution.Command) (*OutputHierarchy, error) {
	var workingDirectory outputNodePath
	if err := bb_path.Resolve(command.WorkingDirectory, bb_path.NewRelativeScopeWalker(&workingDirectory)); err != nil {
		return nil, util.StatusWrap(err, "Invalid working directory")
	}

	oh := &OutputHierarchy{
		root: *newOutputDirectory(),
	}

	// Register REv2.0 output directories.
	for _, outputDirectory := range command.OutputDirectories {
		if on, name, err := oh.lookup(workingDirectory, outputDirectory); err != nil {
			return nil, util.StatusWrapf(err, "Invalid output directory %#v", outputDirectory)
		} else if on == nil {
			oh.rootsToUpload = append(oh.rootsToUpload, outputDirectory)
		} else {
			on.directoriesToUpload[*name] = append(on.directoriesToUpload[*name], outputDirectory)
		}
	}

	// Register REv2.0 output files.
	for _, outputFile := range command.OutputFiles {
		if on, name, err := oh.lookup(workingDirectory, outputFile); err != nil {
			return nil, util.StatusWrapf(err, "Invalid output file %#v", outputFile)
		} else if on == nil {
			return nil, status.Errorf(codes.InvalidArgument, "Output file %#v resolves to the input root directory", outputFile)
		} else {
			on.filesToUpload[*name] = append(on.filesToUpload[*name], outputFile)
		}
	}

	// Register REv2.1 output paths.
	for _, outputPath := range command.OutputPaths {
		if on, name, err := oh.lookup(workingDirectory, outputPath); err != nil {
			return nil, util.StatusWrapf(err, "Invalid output path %#v", outputPath)
		} else if on == nil {
			oh.rootsToUpload = append(oh.rootsToUpload, outputPath)
		} else {
			on.pathsToUpload[*name] = append(on.pathsToUpload[*name], outputPath)
		}
	}
	return oh, nil
}

func (oh *OutputHierarchy) lookup(workingDirectory outputNodePath, path string) (*outputNode, *bb_path.Component, error) {
	// Resolve the path of the output relative to the working directory.
	outputPath := outputNodePath{
		components: append([]bb_path.Component(nil), workingDirectory.components...),
	}
	if err := bb_path.Resolve(path, bb_path.NewRelativeScopeWalker(&outputPath)); err != nil {
		return nil, nil, err
	}

	components := outputPath.components
	if len(components) == 0 {
		// Path resolves to the root directory.
		return nil, nil, nil
	}

	// Path resolves to a location inside the root directory,
	// meaning it is named. Create all parent directories.
	on := &oh.root
	for _, component := range components[:len(components)-1] {
		child, ok := on.subdirectories[component]
		if !ok {
			child = newOutputDirectory()
			on.subdirectories[component] = child
		}
		on = child
	}
	return on, &components[len(components)-1], nil
}

// CreateParentDirectories creates parent directories of outputs. This
// function is called prior to executing the build action.
func (oh *OutputHierarchy) CreateParentDirectories(d BuildDirectory) error {
	dPath := newPathBuffer()
	return oh.root.createParentDirectories(d, &dPath)
}

// UploadOutputs uploads outputs of the build action into the CAS. This
// function is called after executing the build action.
func (oh *OutputHierarchy) UploadOutputs(ctx context.Context, d UploadableDirectory, contentAddressableStorage blobstore.BlobAccess, digestFunction digest.Function, actionResult *remoteexecution.ActionResult) error {
	s := uploadOutputsState{
		context:                   ctx,
		contentAddressableStorage: contentAddressableStorage,
		digestFunction:            digestFunction,
		actionResult:              actionResult,

		dPath: newPathBuffer(),
	}

	if len(oh.rootsToUpload) > 0 {
		s.uploadOutputDirectoryEntered(d, oh.rootsToUpload)
	}
	oh.root.uploadOutputs(&s, d)
	return s.firstError
}
