package builder

import (
	"context"
	"os"
	"path"
	"strings"
	"time"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	re_filesystem "github.com/buildbarn/bb-remote-execution/pkg/filesystem"
	"github.com/buildbarn/bb-remote-execution/pkg/proto/remoteworker"
	runner_pb "github.com/buildbarn/bb-remote-execution/pkg/proto/runner"
	"github.com/buildbarn/bb-remote-execution/pkg/runner"
	"github.com/buildbarn/bb-storage/pkg/cas"
	"github.com/buildbarn/bb-storage/pkg/clock"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/filesystem"
	"github.com/buildbarn/bb-storage/pkg/util"
	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes"
	"github.com/golang/protobuf/ptypes/empty"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type localBuildExecutor struct {
	contentAddressableStorage cas.ContentAddressableStorage
	buildDirectoryCreator     BuildDirectoryCreator
	runner                    runner.Runner
	clock                     clock.Clock
	defaultExecutionTimeout   time.Duration
	maximumExecutionTimeout   time.Duration
	inputRootCharacterDevices map[string]int
}

// NewLocalBuildExecutor returns a BuildExecutor that executes build
// steps on the local system.
func NewLocalBuildExecutor(contentAddressableStorage cas.ContentAddressableStorage, buildDirectoryCreator BuildDirectoryCreator, runner runner.Runner, clock clock.Clock, defaultExecutionTimeout time.Duration, maximumExecutionTimeout time.Duration, inputRootCharacterDevices map[string]int) BuildExecutor {
	return &localBuildExecutor{
		contentAddressableStorage: contentAddressableStorage,
		buildDirectoryCreator:     buildDirectoryCreator,
		runner:                    runner,
		clock:                     clock,
		defaultExecutionTimeout:   defaultExecutionTimeout,
		maximumExecutionTimeout:   maximumExecutionTimeout,
		inputRootCharacterDevices: inputRootCharacterDevices,
	}
}

func (be *localBuildExecutor) uploadDirectory(ctx context.Context, outputDirectory filesystem.Directory, parentDigest digest.Digest, children map[string]*remoteexecution.Directory, components []string) (*remoteexecution.Directory, error) {
	files, err := outputDirectory.ReadDir()
	if err != nil {
		return nil, util.StatusWrapf(err, "Failed to read output directory %#v", path.Join(components...))
	}

	var directory remoteexecution.Directory
	for _, file := range files {
		name := file.Name()
		childComponents := append(components, name)
		// The elided default case of the below switch statement would
		// represent a UNIX socket, FIFO or device node. These files cannot
		// be represented in a Directory message. They are simply skipped.
		// Returning an error would make the overall user experience worse.
		switch fileType := file.Type(); fileType {
		case filesystem.FileTypeRegularFile, filesystem.FileTypeExecutableFile:
			childDigest, err := be.contentAddressableStorage.PutFile(ctx, outputDirectory, name, parentDigest)
			if err != nil {
				return nil, util.StatusWrapf(err, "Failed to store output file %#v", path.Join(childComponents...))
			}
			directory.Files = append(directory.Files, &remoteexecution.FileNode{
				Name:         name,
				Digest:       childDigest.GetPartialDigest(),
				IsExecutable: fileType == filesystem.FileTypeExecutableFile,
			})
		case filesystem.FileTypeDirectory:
			childDirectory, err := outputDirectory.EnterDirectory(name)
			if err != nil {
				return nil, util.StatusWrapf(err, "Failed to enter output directory %#v", path.Join(childComponents...))
			}
			child, err := be.uploadDirectory(ctx, childDirectory, parentDigest, children, childComponents)
			childDirectory.Close()
			if err != nil {
				return nil, err
			}

			// Compute digest of the child directory. This requires serializing it.
			data, err := proto.Marshal(child)
			if err != nil {
				return nil, util.StatusWrapf(err, "Failed to marshal output directory %#v", path.Join(childComponents...))
			}
			digestGenerator := parentDigest.NewGenerator()
			if _, err := digestGenerator.Write(data); err != nil {
				return nil, util.StatusWrapf(err, "Failed to compute digest of output directory %#v", path.Join(childComponents...))
			}
			childDigest := digestGenerator.Sum()

			children[childDigest.GetKey(digest.KeyWithoutInstance)] = child
			directory.Directories = append(directory.Directories, &remoteexecution.DirectoryNode{
				Name:   name,
				Digest: childDigest.GetPartialDigest(),
			})
		case filesystem.FileTypeSymlink:
			target, err := outputDirectory.Readlink(name)
			if err != nil {
				return nil, util.StatusWrapf(err, "Failed to read output symlink %#v", path.Join(childComponents...))
			}
			directory.Symlinks = append(directory.Symlinks, &remoteexecution.SymlinkNode{
				Name:   name,
				Target: target,
			})
		}
	}
	return &directory, nil
}

func (be *localBuildExecutor) uploadTree(ctx context.Context, outputDirectory filesystem.Directory, parentDigest digest.Digest, components []string) (digest.Digest, error) {
	// Gather all individual directory objects and turn them into a tree.
	children := map[string]*remoteexecution.Directory{}
	root, err := be.uploadDirectory(ctx, outputDirectory, parentDigest, children, components)
	if err != nil {
		return digest.BadDigest, err
	}
	tree := &remoteexecution.Tree{
		Root: root,
	}
	for _, child := range children {
		tree.Children = append(tree.Children, child)
	}
	treeDigest, err := be.contentAddressableStorage.PutTree(ctx, tree, parentDigest)
	if err != nil {
		return digest.BadDigest, util.StatusWrapf(err, "Failed to store output directory %#v", path.Join(components...))
	}
	return treeDigest, err
}

func (be *localBuildExecutor) createOutputParentDirectory(inputRootDirectory filesystem.Directory, outputParentPath string) (filesystem.DirectoryCloser, error) {
	// Create and enter successive components, closing the former.
	components := strings.FieldsFunc(outputParentPath, func(r rune) bool { return r == '/' })
	d := filesystem.NopDirectoryCloser(inputRootDirectory)
	for n, component := range components {
		if component != "." {
			if err := d.Mkdir(component, 0777); err != nil && !os.IsExist(err) {
				d.Close()
				return nil, util.StatusWrapf(err, "Failed to create output directory %#v", path.Join(components[:n+1]...))
			}
			d2, err := d.EnterDirectory(component)
			d.Close()
			if err != nil {
				return nil, util.StatusWrapf(err, "Failed to enter output directory %#v", path.Join(components[:n+1]...))
			}
			d = d2
		}
	}
	return d, nil
}

func (be *localBuildExecutor) createCharacterDevices(inputRootDirectory BuildDirectory) error {
	if err := inputRootDirectory.Mkdir("dev", 0777); err != nil && !os.IsExist(err) {
		return util.StatusWrap(err, "Unable to create /dev directory in input root")
	}
	devDir, err := inputRootDirectory.EnterDirectory("dev")
	defer devDir.Close()
	if err != nil {
		return util.StatusWrap(err, "Unable to enter /dev directory in input root")
	}
	for name, number := range be.inputRootCharacterDevices {
		if err := devDir.Mknod(name, os.ModeDevice|os.ModeCharDevice|0666, number); err != nil {
			return util.StatusWrapf(err, "Failed to create character device %#v", name)
		}
	}
	return nil
}

func (be *localBuildExecutor) Execute(ctx context.Context, filePool re_filesystem.FilePool, instanceName string, request *remoteworker.DesiredState_Executing, executionStateUpdates chan<- *remoteworker.CurrentState_Executing) *remoteexecution.ExecuteResponse {
	response := &remoteexecution.ExecuteResponse{
		Result: &remoteexecution.ActionResult{
			ExecutionMetadata: &remoteexecution.ExecutedActionMetadata{},
		},
	}

	// Timeout handling.
	action := request.Action
	if action == nil {
		attachErrorToExecuteResponse(response, status.Error(codes.InvalidArgument, "Request does not contain an action"))
		return response
	}
	var executionTimeout time.Duration
	if action.Timeout == nil {
		executionTimeout = be.defaultExecutionTimeout
	} else {
		var err error
		executionTimeout, err = ptypes.Duration(action.Timeout)
		if err != nil {
			attachErrorToExecuteResponse(
				response,
				util.StatusWrapWithCode(err, codes.InvalidArgument, "Invalid execution timeout"))
			return response
		}
		if executionTimeout > be.maximumExecutionTimeout {
			attachErrorToExecuteResponse(
				response,
				status.Errorf(
					codes.InvalidArgument,
					"Execution timeout of %s exceeds maximum permitted value of %s",
					executionTimeout,
					be.maximumExecutionTimeout))
			return response
		}
	}

	// Obtain build directory.
	actionDigest, err := digest.NewDigestFromPartialDigest(instanceName, request.ActionDigest)
	if err != nil {
		attachErrorToExecuteResponse(response, util.StatusWrap(err, "Failed to extract digest for action"))
		return response
	}
	command := request.Command
	if command == nil {
		attachErrorToExecuteResponse(response, status.Error(codes.InvalidArgument, "Request does not contain a command"))
		return response
	}
	buildDirectory, buildDirectoryPath, err := be.buildDirectoryCreator.GetBuildDirectory(actionDigest, action.DoNotCache)
	if err != nil {
		attachErrorToExecuteResponse(
			response,
			util.StatusWrap(err, "Failed to acquire build environment"))
		return response
	}
	defer buildDirectory.Close()

	// Install hooks on build directory to capture file creation
	// events.
	buildDirectory.InstallHooks(filePool)

	executionStateUpdates <- &remoteworker.CurrentState_Executing{
		ActionDigest: request.ActionDigest,
		ExecutionState: &remoteworker.CurrentState_Executing_FetchingInputs{
			FetchingInputs: &empty.Empty{},
		},
	}

	// Create input root directory inside of build directory.
	if err := buildDirectory.Mkdir("root", 0777); err != nil {
		attachErrorToExecuteResponse(
			response,
			util.StatusWrap(err, "Failed to create input root directory"))
		return response
	}
	inputRootDirectory, err := buildDirectory.EnterBuildDirectory("root")
	if err != nil {
		attachErrorToExecuteResponse(
			response,
			util.StatusWrap(err, "Failed to enter input root directory"))
		return response
	}
	defer inputRootDirectory.Close()

	inputRootDigest, err := actionDigest.NewDerivedDigest(action.InputRootDigest)
	if err != nil {
		attachErrorToExecuteResponse(
			response,
			util.StatusWrap(err, "Failed to extract digest for input root"))
		return response
	}
	if err := inputRootDirectory.MergeDirectoryContents(ctx, inputRootDigest); err != nil {
		attachErrorToExecuteResponse(response, err)
		return response
	}

	if len(be.inputRootCharacterDevices) > 0 {
		if err := be.createCharacterDevices(inputRootDirectory); err != nil {
			attachErrorToExecuteResponse(response, err)
			return response
		}
	}

	// Create and open parent directories of where we expect to see output.
	// Build rules generally expect the parent directories to already be
	// there. We later use the directory handles to extract output files.
	outputParentDirectories := map[string]filesystem.Directory{}
	for _, outputDirectory := range command.OutputDirectories {
		// Although REv2 explicitly document that only parents
		// of output directories are created (i.e., not the
		// output directory itself), Bazel recently changed its
		// behaviour to do so after all for local execution. See
		// these issues for details:
		//
		// https://github.com/bazelbuild/bazel/issues/6262
		// https://github.com/bazelbuild/bazel/issues/6393
		//
		// For now, be consistent with Bazel. What the intended
		// protocol behaviour is should be clarified at some
		// point. What is especially confusing is that creating
		// these directories up front somewhat rules out the
		// possibility of omitting output directories and using
		// OutputDirectorySymlinks.
		if _, ok := outputParentDirectories[outputDirectory]; !ok {
			dir, err := be.createOutputParentDirectory(inputRootDirectory, path.Join(command.WorkingDirectory, outputDirectory))
			if err != nil {
				attachErrorToExecuteResponse(response, err)
				return response
			}
			outputParentDirectories[outputDirectory] = dir
			defer dir.Close()
		}
	}
	for _, outputFile := range command.OutputFiles {
		dirPath := path.Dir(outputFile)
		if _, ok := outputParentDirectories[dirPath]; !ok {
			dir, err := be.createOutputParentDirectory(inputRootDirectory, path.Join(command.WorkingDirectory, dirPath))
			if err != nil {
				attachErrorToExecuteResponse(response, err)
				return response
			}
			outputParentDirectories[dirPath] = dir
			defer dir.Close()
		}
	}

	// Create a directory inside the build directory that build
	// actions may use to store temporary files. This ensures that
	// temporary files are automatically removed when the build
	// action completes. When using FUSE, it also causes quotas to
	// be applied to them.
	if err := buildDirectory.Mkdir("tmp", 0777); err != nil {
		attachErrorToExecuteResponse(
			response,
			util.StatusWrap(err, "Failed to create temporary directory inside build directory"))
		return response
	}

	executionStateUpdates <- &remoteworker.CurrentState_Executing{
		ActionDigest: request.ActionDigest,
		ExecutionState: &remoteworker.CurrentState_Executing_Running{
			Running: &empty.Empty{},
		},
	}

	// Invoke the command.
	ctxWithTimeout, cancelTimeout := be.clock.NewContextWithTimeout(ctx, executionTimeout)
	defer cancelTimeout()
	environmentVariables := map[string]string{}
	for _, environmentVariable := range command.EnvironmentVariables {
		environmentVariables[environmentVariable.Name] = environmentVariable.Value
	}
	runResponse, runErr := be.runner.Run(ctxWithTimeout, &runner_pb.RunRequest{
		Arguments:            command.Arguments,
		EnvironmentVariables: environmentVariables,
		WorkingDirectory:     command.WorkingDirectory,
		StdoutPath:           path.Join(buildDirectoryPath, "stdout"),
		StderrPath:           path.Join(buildDirectoryPath, "stderr"),
		InputRootDirectory:   path.Join(buildDirectoryPath, "root"),
		TemporaryDirectory:   path.Join(buildDirectoryPath, "tmp"),
	})

	// Attach the exit code or execution error.
	if runErr == nil {
		response.Result.ExitCode = runResponse.ExitCode
		response.Result.ExecutionMetadata.AuxiliaryMetadata = runResponse.ResourceUsage
	} else {
		attachErrorToExecuteResponse(response, util.StatusWrap(runErr, "Failed to run command"))
	}

	executionStateUpdates <- &remoteworker.CurrentState_Executing{
		ActionDigest: request.ActionDigest,
		ExecutionState: &remoteworker.CurrentState_Executing_UploadingOutputs{
			UploadingOutputs: &empty.Empty{},
		},
	}

	// Upload command output. In the common case, the files are
	// empty. If that's the case, don't bother setting the digest to
	// keep the ActionResult small.
	if stdoutDigest, err := be.contentAddressableStorage.PutFile(ctx, buildDirectory, "stdout", actionDigest); err != nil {
		attachErrorToExecuteResponse(response, util.StatusWrap(err, "Failed to store stdout"))
	} else if stdoutDigest.GetSizeBytes() > 0 {
		response.Result.StdoutDigest = stdoutDigest.GetPartialDigest()
	}
	if stderrDigest, err := be.contentAddressableStorage.PutFile(ctx, buildDirectory, "stderr", actionDigest); err != nil {
		attachErrorToExecuteResponse(response, util.StatusWrap(err, "Failed to store stderr"))
	} else if stderrDigest.GetSizeBytes() > 0 {
		response.Result.StderrDigest = stderrDigest.GetPartialDigest()
	}

	// Upload output files.
	for _, outputFile := range command.OutputFiles {
		outputParentDirectory := outputParentDirectories[path.Dir(outputFile)]
		outputBaseName := path.Base(outputFile)
		if fileInfo, err := outputParentDirectory.Lstat(outputBaseName); err == nil {
			switch fileType := fileInfo.Type(); fileType {
			case filesystem.FileTypeRegularFile, filesystem.FileTypeExecutableFile:
				if digest, err := be.contentAddressableStorage.PutFile(ctx, outputParentDirectory, outputBaseName, actionDigest); err == nil {
					response.Result.OutputFiles = append(response.Result.OutputFiles, &remoteexecution.OutputFile{
						Path:         outputFile,
						Digest:       digest.GetPartialDigest(),
						IsExecutable: fileType == filesystem.FileTypeExecutableFile,
					})
				} else {
					attachErrorToExecuteResponse(
						response,
						util.StatusWrapf(err, "Failed to store output file %#v", outputFile))
				}
			case filesystem.FileTypeSymlink:
				if target, err := outputParentDirectory.Readlink(outputBaseName); err == nil {
					response.Result.OutputFileSymlinks = append(response.Result.OutputFileSymlinks, &remoteexecution.OutputSymlink{
						Path:   outputFile,
						Target: target,
					})
				} else {
					attachErrorToExecuteResponse(
						response,
						util.StatusWrapf(err, "Failed to read output symlink %#v", outputFile))
				}
			default:
				attachErrorToExecuteResponse(
					response,
					status.Errorf(codes.Internal, "Output file %#v is not a regular file or symlink", outputFile))
			}
		} else if !os.IsNotExist(err) {
			attachErrorToExecuteResponse(
				response,
				util.StatusWrapf(err, "Failed to read attributes of output file %#v", outputFile))
		}
	}

	// Upload output directories.
	for _, outputDirectory := range command.OutputDirectories {
		if digest, err := be.uploadTree(ctx, outputParentDirectories[outputDirectory], actionDigest, []string{outputDirectory}); err == nil {
			response.Result.OutputDirectories = append(response.Result.OutputDirectories, &remoteexecution.OutputDirectory{
				Path:       outputDirectory,
				TreeDigest: digest.GetPartialDigest(),
			})
		} else {
			attachErrorToExecuteResponse(response, err)
		}
	}

	return response
}
