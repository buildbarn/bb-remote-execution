package builder

import (
	"context"
	"os"
	"sort"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-storage/pkg/blobstore"
	"github.com/buildbarn/bb-storage/pkg/blobstore/buffer"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/filesystem"
	"github.com/buildbarn/bb-storage/pkg/filesystem/path"
	"github.com/buildbarn/bb-storage/pkg/util"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
)

// OutputNode is a node in a directory hierarchy that contains one or
// more locations where output directories and files are expected.
type outputNode struct {
	directoriesToUpload map[path.Component][]string
	filesToUpload       map[path.Component][]string
	pathsToUpload       map[path.Component][]string
	subdirectories      map[path.Component]*outputNode
}

func (on *outputNode) getSubdirectoryNames() []path.Component {
	l := make(path.ComponentsList, 0, len(on.subdirectories))
	for k := range on.subdirectories {
		l = append(l, k)
	}
	sort.Sort(l)
	return l
}

func sortToUpload(m map[path.Component][]string) []path.Component {
	l := make(path.ComponentsList, 0, len(m))
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
		directoriesToUpload: map[path.Component][]string{},
		filesToUpload:       map[path.Component][]string{},
		pathsToUpload:       map[path.Component][]string{},
		subdirectories:      map[path.Component]*outputNode{},
	}
}

// CreateParentDirectories is recursive invoked by
// OutputHierarchy.CreateParentDirectories() to create parent
// directories of locations where output directories and files are
// expected.
func (on *outputNode) createParentDirectories(d ParentPopulatableDirectory, dPath *path.Trace) error {
	for _, name := range on.getSubdirectoryNames() {
		childPath := dPath.Append(name)
		if err := d.Mkdir(name, 0o777); err != nil && !os.IsExist(err) {
			return util.StatusWrapf(err, "Failed to create output parent directory %#v", childPath.GetUNIXString())
		}

		// Recurse if we need to create one or more directories within.
		if child := on.subdirectories[name]; len(child.subdirectories) > 0 || len(child.directoriesToUpload) > 0 {
			childDirectory, err := d.EnterParentPopulatableDirectory(name)
			if err != nil {
				return util.StatusWrapf(err, "Failed to enter output parent directory %#v", childPath.GetUNIXString())
			}
			err = child.createParentDirectories(childDirectory, childPath)
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
			childPath := dPath.Append(name)
			if err := d.Mkdir(name, 0o777); err != nil && !os.IsExist(err) {
				return util.StatusWrapf(err, "Failed to create output directory %#v", childPath.GetUNIXString())
			}
		}
	}
	return nil
}

// UploadOutputs is recursively invoked by
// OutputHierarchy.UploadOutputs() to upload output directories and
// files from the locations where they are expected.
func (on *outputNode) uploadOutputs(s *uploadOutputsState, d UploadableDirectory, dPath *path.Trace) {
	// Upload REv2.0 output directories that are expected to be
	// present in this directory.
	for _, component := range sortToUpload(on.directoriesToUpload) {
		childPath := dPath.Append(component)
		paths := on.directoriesToUpload[component]
		if fileInfo, err := d.Lstat(component); err == nil {
			switch fileType := fileInfo.Type(); fileType {
			case filesystem.FileTypeDirectory:
				s.uploadOutputDirectory(d, component, childPath, paths)
			case filesystem.FileTypeSymlink:
				s.uploadOutputSymlink(d, component, childPath, &s.actionResult.OutputDirectorySymlinks, paths)
			default:
				s.saveError(status.Errorf(codes.InvalidArgument, "Output directory %#v is not a directory or symlink", childPath.GetUNIXString()))
			}
		} else if !os.IsNotExist(err) {
			s.saveError(util.StatusWrapf(err, "Failed to read attributes of output directory %#v", childPath.GetUNIXString()))
		}
	}

	// Upload REv2.0 output files that are expected to be present in
	// this directory.
	for _, component := range sortToUpload(on.filesToUpload) {
		childPath := dPath.Append(component)
		paths := on.filesToUpload[component]
		if fileInfo, err := d.Lstat(component); err == nil {
			switch fileType := fileInfo.Type(); fileType {
			case filesystem.FileTypeRegularFile:
				s.uploadOutputFile(d, component, childPath, fileInfo.IsExecutable(), paths)
			case filesystem.FileTypeSymlink:
				s.uploadOutputSymlink(d, component, childPath, &s.actionResult.OutputFileSymlinks, paths)
			default:
				s.saveError(status.Errorf(codes.InvalidArgument, "Output file %#v is not a regular file or symlink", childPath.GetUNIXString()))
			}
		} else if !os.IsNotExist(err) {
			s.saveError(util.StatusWrapf(err, "Failed to read attributes of output file %#v", childPath.GetUNIXString()))
		}
	}

	// Upload REv2.1 output paths that are expected to be present in
	// this directory.
	for _, component := range sortToUpload(on.pathsToUpload) {
		childPath := dPath.Append(component)
		paths := on.pathsToUpload[component]
		if fileInfo, err := d.Lstat(component); err == nil {
			switch fileType := fileInfo.Type(); fileType {
			case filesystem.FileTypeDirectory:
				s.uploadOutputDirectory(d, component, childPath, paths)
			case filesystem.FileTypeRegularFile:
				s.uploadOutputFile(d, component, childPath, fileInfo.IsExecutable(), paths)
			case filesystem.FileTypeSymlink:
				s.uploadOutputSymlink(d, component, childPath, &s.actionResult.OutputSymlinks, paths)
			default:
				s.saveError(status.Errorf(codes.InvalidArgument, "Output path %#v is not a directory, regular file or symlink", childPath.GetUNIXString()))
			}
		} else if !os.IsNotExist(err) {
			s.saveError(util.StatusWrapf(err, "Failed to read attributes of output path %#v", childPath.GetUNIXString()))
		}
	}

	// Traverse into subdirectories.
	for _, component := range on.getSubdirectoryNames() {
		childPath := dPath.Append(component)
		childNode := on.subdirectories[component]
		if childDirectory, err := d.EnterUploadableDirectory(component); err == nil {
			childNode.uploadOutputs(s, childDirectory, childPath)
			childDirectory.Close()
		} else if !os.IsNotExist(err) {
			s.saveError(util.StatusWrapf(err, "Failed to enter output parent directory %#v", childPath.GetUNIXString()))
		}
	}
}

// UploadOutputsState is used by OutputHierarchy.UploadOutputs() to
// track common parameters during recursion.
type uploadOutputsState struct {
	context                   context.Context
	contentAddressableStorage blobstore.BlobAccess
	digestFunction            digest.Function
	writableFileUploadDelay   <-chan struct{}
	actionResult              *remoteexecution.ActionResult
	uploadTreesAndDirectories bool

	firstError error
}

// computeDigest computes the digest of a byte slice, using the digest
// function that's also used by the client.
func (s *uploadOutputsState) computeDigest(data []byte) digest.Digest {
	digestGenerator := s.digestFunction.NewGenerator(int64(len(data)))
	if _, err := digestGenerator.Write(data); err != nil {
		panic(err)
	}
	return digestGenerator.Sum()
}

// SaveError preserves errors that occur during uploading. Even when
// errors occur, the remainder of the output files is still uploaded.
// This makes debugging easier.
func (s *uploadOutputsState) saveError(err error) {
	if s.firstError == nil {
		s.firstError = err
	}
}

// UploadOutputDirectoryEntered is called to upload a single output
// directory as a remoteexecution.Tree. The root directory is assumed to
// already be opened.
func (s *uploadOutputsState) uploadOutputDirectoryEntered(d UploadableDirectory, dPath *path.Trace, paths []string) {
	dState := uploadOutputDirectoryState{
		uploadOutputsState: s,
		directoriesSeen:    map[digest.Digest][]byte{},
	}
	if rootDirectoryDigest, err := dState.uploadDirectory(d, dPath); err == nil {
		// Approximate the size of the resulting Tree object, so
		// that we may allocate all space at once.
		directories := dState.directories
		maximumTreeSizeBytes := 0
		for _, directory := range directories {
			maximumTreeSizeBytes += len(directory)
		}
		maximumTreeSizeBytes += len(directories) * (1 + protowire.SizeVarint(uint64(maximumTreeSizeBytes)))

		// Construct the Tree object. We don't want to use
		// proto.Marshal() for this, as it would require us to
		// marshal all of the directories a second time.
		treeData := make([]byte, 0, maximumTreeSizeBytes)
		tag := byte(blobstore.TreeRootFieldNumber<<3) | byte(protowire.BytesType)
		for i := len(directories); i > 0; i-- {
			directory := directories[i-1]
			treeData = append(treeData, tag)
			treeData = protowire.AppendVarint(treeData, uint64(len(directory)))
			treeData = append(treeData, directory...)
			tag = byte(blobstore.TreeChildrenFieldNumber<<3) | byte(protowire.BytesType)
		}

		// Always upload the directory in Tree form, even if the
		// client did not request it. CompletenessCheckingBlobAccess
		// depends on it to work efficiently.
		successfullyUploaded := true
		treeDigest := s.computeDigest(treeData)
		if err := s.contentAddressableStorage.Put(s.context, treeDigest, buffer.NewValidatedBufferFromByteSlice(treeData)); err != nil {
			s.saveError(util.StatusWrapf(err, "Failed to store output directory %#v", dPath.GetUNIXString()))
			successfullyUploaded = false
		}

		// Upload Directory messages if requested by the client.
		// Only in this case may we set OutputDirectory's
		// root_directory_digest.
		var rootDirectoryDigestProto *remoteexecution.Digest
		if s.uploadTreesAndDirectories {
			rootDirectoryDigestProto = rootDirectoryDigest.GetProto()
			for directoryDigest, directory := range dState.directoriesSeen {
				if err := s.contentAddressableStorage.Put(s.context, directoryDigest, buffer.NewValidatedBufferFromByteSlice(directory)); err != nil {
					s.saveError(util.StatusWrapf(err, "Failed to store output directory %#v", dPath.GetUNIXString()))
					successfullyUploaded = false
				}
			}
		}

		if successfullyUploaded {
			for _, path := range paths {
				s.actionResult.OutputDirectories = append(
					s.actionResult.OutputDirectories,
					&remoteexecution.OutputDirectory{
						Path:                  path,
						TreeDigest:            treeDigest.GetProto(),
						IsTopologicallySorted: true,
						RootDirectoryDigest:   rootDirectoryDigestProto,
					})
			}
		}
	} else {
		s.saveError(err)
	}
}

// UploadOutputDirectory is called to upload a single output directory
// as a remoteexecution.Tree. The root directory is opened opened by
// this function.
func (s *uploadOutputsState) uploadOutputDirectory(d UploadableDirectory, name path.Component, childPath *path.Trace, paths []string) {
	if childDirectory, err := d.EnterUploadableDirectory(name); err == nil {
		s.uploadOutputDirectoryEntered(childDirectory, childPath, paths)
		childDirectory.Close()
	} else {
		s.saveError(util.StatusWrapf(err, "Failed to enter output directory %#v", childPath.GetUNIXString()))
	}
}

// UploadOutputDirectory is called to upload a single output file.
func (s *uploadOutputsState) uploadOutputFile(d UploadableDirectory, name path.Component, childPath *path.Trace, isExecutable bool, paths []string) {
	if digest, err := d.UploadFile(s.context, name, s.digestFunction, s.writableFileUploadDelay); err == nil {
		for _, path := range paths {
			s.actionResult.OutputFiles = append(
				s.actionResult.OutputFiles,
				&remoteexecution.OutputFile{
					Path:         path,
					Digest:       digest.GetProto(),
					IsExecutable: isExecutable,
				})
		}
	} else {
		s.saveError(util.StatusWrapf(err, "Failed to store output file %#v", childPath.GetUNIXString()))
	}
}

// UploadOutputDirectory is called to read the attributes of a single
// output symlink.
func (s *uploadOutputsState) uploadOutputSymlink(d UploadableDirectory, name path.Component, childPath *path.Trace, outputSymlinks *[]*remoteexecution.OutputSymlink, paths []string) {
	if targetParser, err := d.Readlink(name); err == nil {
		targetPath, scopeWalker := path.EmptyBuilder.Join(path.VoidScopeWalker)
		if path.Resolve(targetParser, scopeWalker); err == nil {
			target := targetPath.GetUNIXString()
			for _, path := range paths {
				*outputSymlinks = append(
					*outputSymlinks,
					&remoteexecution.OutputSymlink{
						Path:   path,
						Target: target,
					})
			}
		} else {
			s.saveError(util.StatusWrapf(err, "Failed to resolve target of output symlink %#v", childPath.GetUNIXString()))
		}
	} else {
		s.saveError(util.StatusWrapf(err, "Failed to read output symlink %#v", childPath.GetUNIXString()))
	}
}

// UploadOutputDirectoryState is used by OutputHierarchy.UploadOutputs()
// to track state specific to uploading a single output directory.
type uploadOutputDirectoryState struct {
	*uploadOutputsState

	directories     [][]byte
	directoriesSeen map[digest.Digest][]byte
}

// UploadDirectory is called to upload a single directory. Elements in
// the directory are stored in a remoteexecution.Directory, so that they
// can be placed in a remoteexecution.Tree.
func (s *uploadOutputDirectoryState) uploadDirectory(d UploadableDirectory, dPath *path.Trace) (digest.Digest, error) {
	files, err := d.ReadDir()
	if err != nil {
		return digest.BadDigest, util.StatusWrapf(err, "Failed to read output directory %#v", dPath.GetUNIXString())
	}

	var directory remoteexecution.Directory
	for _, file := range files {
		name := file.Name()
		childPath := dPath.Append(name)
		switch fileType := file.Type(); fileType {
		case filesystem.FileTypeRegularFile:
			if childDigest, err := d.UploadFile(s.context, name, s.digestFunction, s.writableFileUploadDelay); err == nil {
				directory.Files = append(directory.Files, &remoteexecution.FileNode{
					Name:         name.String(),
					Digest:       childDigest.GetProto(),
					IsExecutable: file.IsExecutable(),
				})
			} else {
				s.saveError(util.StatusWrapf(err, "Failed to store output file %#v", childPath.GetUNIXString()))
			}
		case filesystem.FileTypeDirectory:
			if childDirectory, err := d.EnterUploadableDirectory(name); err == nil {
				childDigest, err := s.uploadDirectory(childDirectory, dPath)
				childDirectory.Close()
				if err == nil {
					directory.Directories = append(directory.Directories, &remoteexecution.DirectoryNode{
						Name:   name.String(),
						Digest: childDigest.GetProto(),
					})
				} else {
					s.saveError(err)
				}
			} else {
				s.saveError(util.StatusWrapf(err, "Failed to enter output directory %#v", childPath.GetUNIXString()))
			}
		case filesystem.FileTypeSymlink:
			if targetParser, err := d.Readlink(name); err == nil {
				targetPath, scopeWalker := path.EmptyBuilder.Join(path.VoidScopeWalker)
				if path.Resolve(targetParser, scopeWalker); err == nil {
					directory.Symlinks = append(directory.Symlinks, &remoteexecution.SymlinkNode{
						Name:   name.String(),
						Target: targetPath.GetUNIXString(),
					})
				} else {
					s.saveError(util.StatusWrapf(err, "Failed to resolve target of output symlink %#v", childPath.GetUNIXString()))
				}
			} else {
				s.saveError(util.StatusWrapf(err, "Failed to read output symlink %#v", childPath.GetUNIXString()))
			}
		}
	}

	data, err := proto.Marshal(&directory)
	if err != nil {
		return digest.BadDigest, util.StatusWrapf(err, "Failed to marshal output directory %#v", dPath.GetUNIXString())
	}

	// There is no need to make the directory part of the Tree if we
	// have seen an identical directory previously.
	digest := s.computeDigest(data)
	if _, ok := s.directoriesSeen[digest]; !ok {
		s.directories = append(s.directories, data)
		s.directoriesSeen[digest] = data
	}
	return digest, nil
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
	components []path.Component
}

func (onp *outputNodePath) OnDirectory(name path.Component) (path.GotDirectoryOrSymlink, error) {
	onp.components = append(onp.components, name)
	return path.GotDirectory{
		Child:        onp,
		IsReversible: true,
	}, nil
}

func (onp *outputNodePath) OnTerminal(name path.Component) (*path.GotSymlink, error) {
	onp.components = append(onp.components, name)
	return nil, nil
}

func (onp *outputNodePath) OnUp() (path.ComponentWalker, error) {
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
	root                      outputNode
	rootsToUpload             []string
	uploadTreesAndDirectories bool
}

// NewOutputHierarchy creates a new OutputHierarchy that uses the
// working directory and the output paths specified in an REv2 Command
// message.
func NewOutputHierarchy(command *remoteexecution.Command) (*OutputHierarchy, error) {
	var workingDirectory outputNodePath
	if err := path.Resolve(path.NewUNIXParser(command.WorkingDirectory), path.NewRelativeScopeWalker(&workingDirectory)); err != nil {
		return nil, util.StatusWrap(err, "Invalid working directory")
	}

	oh := &OutputHierarchy{
		root: *newOutputDirectory(),
		uploadTreesAndDirectories: command.OutputDirectoryFormat == remoteexecution.Command_DIRECTORY_ONLY ||
			command.OutputDirectoryFormat == remoteexecution.Command_TREE_AND_DIRECTORY,
	}

	if len(command.OutputPaths) == 0 {
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
	} else {
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
	}
	return oh, nil
}

func (oh *OutputHierarchy) lookup(workingDirectory outputNodePath, targetPath string) (*outputNode, *path.Component, error) {
	// Resolve the path of the output relative to the working directory.
	outputPath := outputNodePath{
		components: append([]path.Component(nil), workingDirectory.components...),
	}
	if err := path.Resolve(path.NewUNIXParser(targetPath), path.NewRelativeScopeWalker(&outputPath)); err != nil {
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

// ParentPopulatableDirectory contains a subset of the methods of
// filesystem.Directory that are required for creating the parent
// directories of output files of a build action.
type ParentPopulatableDirectory interface {
	Close() error
	EnterParentPopulatableDirectory(name path.Component) (ParentPopulatableDirectory, error)
	Mkdir(name path.Component, perm os.FileMode) error
}

// CreateParentDirectories creates parent directories of outputs. This
// function is called prior to executing the build action.
func (oh *OutputHierarchy) CreateParentDirectories(d ParentPopulatableDirectory) error {
	return oh.root.createParentDirectories(d, nil)
}

// UploadOutputs uploads outputs of the build action into the CAS. This
// function is called after executing the build action.
func (oh *OutputHierarchy) UploadOutputs(ctx context.Context, d UploadableDirectory, contentAddressableStorage blobstore.BlobAccess, digestFunction digest.Function, writableFileUploadDelay <-chan struct{}, actionResult *remoteexecution.ActionResult, forceUploadTreesAndDirectories bool) error {
	s := uploadOutputsState{
		context:                   ctx,
		contentAddressableStorage: contentAddressableStorage,
		digestFunction:            digestFunction,
		writableFileUploadDelay:   writableFileUploadDelay,
		actionResult:              actionResult,
		uploadTreesAndDirectories: oh.uploadTreesAndDirectories || forceUploadTreesAndDirectories,
	}

	if len(oh.rootsToUpload) > 0 {
		s.uploadOutputDirectoryEntered(d, nil, oh.rootsToUpload)
	}
	oh.root.uploadOutputs(&s, d, nil)
	return s.firstError
}
