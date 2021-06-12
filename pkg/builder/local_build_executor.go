package builder

import (
	"context"
	"os"
	"sync"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	re_filesystem "github.com/buildbarn/bb-remote-execution/pkg/filesystem"
	"github.com/buildbarn/bb-remote-execution/pkg/proto/remoteworker"
	runner_pb "github.com/buildbarn/bb-remote-execution/pkg/proto/runner"
	"github.com/buildbarn/bb-remote-execution/pkg/runner"
	"github.com/buildbarn/bb-storage/pkg/blobstore"
	"github.com/buildbarn/bb-storage/pkg/clock"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/filesystem/path"
	"github.com/buildbarn/bb-storage/pkg/util"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
)

// Filenames of objects to be created inside the build directory.
var (
	stdoutComponent             = path.MustNewComponent("stdout")
	stderrComponent             = path.MustNewComponent("stderr")
	deviceDirectoryComponent    = path.MustNewComponent("dev")
	inputRootDirectoryComponent = path.MustNewComponent("root")
	temporaryDirectoryComponent = path.MustNewComponent("tmp")
)

// capturingErrorLogger is an error logger that stores up to a single
// error. When the error is stored, a context cancelation function is
// invoked. This is used by localBuildExecutor to kill a build action in
// case an I/O error occurs on the FUSE file system.
type capturingErrorLogger struct {
	lock   sync.Mutex
	cancel context.CancelFunc
	error  error
}

func (el *capturingErrorLogger) Log(err error) {
	el.lock.Lock()
	defer el.lock.Unlock()

	if el.cancel != nil {
		el.error = err
		el.cancel()
		el.cancel = nil
	}
}

func (el *capturingErrorLogger) GetError() error {
	el.lock.Lock()
	defer el.lock.Unlock()

	return el.error
}

type localBuildExecutor struct {
	contentAddressableStorage blobstore.BlobAccess
	buildDirectoryCreator     BuildDirectoryCreator
	runner                    runner.Runner
	clock                     clock.Clock
	inputRootCharacterDevices map[path.Component]int
	maximumMessageSizeBytes   int
	environmentVariables      map[string]string
}

// NewLocalBuildExecutor returns a BuildExecutor that executes build
// steps on the local system.
func NewLocalBuildExecutor(contentAddressableStorage blobstore.BlobAccess, buildDirectoryCreator BuildDirectoryCreator, runner runner.Runner, clock clock.Clock, inputRootCharacterDevices map[path.Component]int, maximumMessageSizeBytes int, environmentVariables map[string]string) BuildExecutor {
	return &localBuildExecutor{
		contentAddressableStorage: contentAddressableStorage,
		buildDirectoryCreator:     buildDirectoryCreator,
		runner:                    runner,
		clock:                     clock,
		inputRootCharacterDevices: inputRootCharacterDevices,
		maximumMessageSizeBytes:   maximumMessageSizeBytes,
		environmentVariables:      environmentVariables,
	}
}

func (be *localBuildExecutor) createCharacterDevices(inputRootDirectory BuildDirectory) error {
	if err := inputRootDirectory.Mkdir(deviceDirectoryComponent, 0o777); err != nil && !os.IsExist(err) {
		return util.StatusWrap(err, "Unable to create /dev directory in input root")
	}
	deviceDirectory, err := inputRootDirectory.EnterBuildDirectory(deviceDirectoryComponent)
	if err != nil {
		return util.StatusWrap(err, "Unable to enter /dev directory in input root")
	}
	defer deviceDirectory.Close()
	for name, number := range be.inputRootCharacterDevices {
		if err := deviceDirectory.Mknod(name, os.ModeDevice|os.ModeCharDevice|0o666, number); err != nil {
			return util.StatusWrapf(err, "Failed to create character device %#v", name.String())
		}
	}
	return nil
}

func (be *localBuildExecutor) Execute(ctx context.Context, filePool re_filesystem.FilePool, instanceName digest.InstanceName, request *remoteworker.DesiredState_Executing, executionStateUpdates chan<- *remoteworker.CurrentState_Executing) *remoteexecution.ExecuteResponse {
	// Timeout handling.
	response := NewDefaultExecuteResponse(request)
	action := request.Action
	if action == nil {
		attachErrorToExecuteResponse(response, status.Error(codes.InvalidArgument, "Request does not contain an action"))
		return response
	}
	if err := action.Timeout.CheckValid(); err != nil {
		attachErrorToExecuteResponse(
			response,
			util.StatusWrapWithCode(err, codes.InvalidArgument, "Invalid execution timeout"))
		return response
	}
	executionTimeout := action.Timeout.AsDuration()

	// Obtain build directory.
	actionDigest, err := instanceName.NewDigestFromProto(request.ActionDigest)
	if err != nil {
		attachErrorToExecuteResponse(response, util.StatusWrap(err, "Failed to extract digest for action"))
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

	// Install hooks on build directory to capture file creation and
	// I/O error events.
	ctxWithIOError, cancelIOError := context.WithCancel(ctx)
	defer cancelIOError()
	ioErrorCapturer := capturingErrorLogger{cancel: cancelIOError}
	buildDirectory.InstallHooks(filePool, &ioErrorCapturer)

	executionStateUpdates <- &remoteworker.CurrentState_Executing{
		ActionDigest: request.ActionDigest,
		ExecutionState: &remoteworker.CurrentState_Executing_FetchingInputs{
			FetchingInputs: &emptypb.Empty{},
		},
	}

	// Create input root directory inside of build directory.
	if err := buildDirectory.Mkdir(inputRootDirectoryComponent, 0o777); err != nil {
		attachErrorToExecuteResponse(
			response,
			util.StatusWrap(err, "Failed to create input root directory"))
		return response
	}
	inputRootDirectory, err := buildDirectory.EnterBuildDirectory(inputRootDirectoryComponent)
	if err != nil {
		attachErrorToExecuteResponse(
			response,
			util.StatusWrap(err, "Failed to enter input root directory"))
		return response
	}
	defer inputRootDirectory.Close()

	inputRootDigest, err := instanceName.NewDigestFromProto(action.InputRootDigest)
	if err != nil {
		attachErrorToExecuteResponse(
			response,
			util.StatusWrap(err, "Failed to extract digest for input root"))
		return response
	}
	if err := inputRootDirectory.MergeDirectoryContents(ctx, &ioErrorCapturer, inputRootDigest); err != nil {
		attachErrorToExecuteResponse(response, err)
		return response
	}

	if len(be.inputRootCharacterDevices) > 0 {
		if err := be.createCharacterDevices(inputRootDirectory); err != nil {
			attachErrorToExecuteResponse(response, err)
			return response
		}
	}

	// Create parent directories of output files and directories.
	// These are not declared in the input root explicitly.
	commandDigest, err := instanceName.NewDigestFromProto(action.CommandDigest)
	if err != nil {
		attachErrorToExecuteResponse(response, util.StatusWrap(err, "Failed to extract digest for command"))
		return response
	}
	commandMessage, err := be.contentAddressableStorage.Get(ctx, commandDigest).ToProto(&remoteexecution.Command{}, be.maximumMessageSizeBytes)
	if err != nil {
		attachErrorToExecuteResponse(response, util.StatusWrap(err, "Failed to obtain command"))
		return response
	}
	command := commandMessage.(*remoteexecution.Command)
	outputHierarchy, err := NewOutputHierarchy(command)
	if err != nil {
		attachErrorToExecuteResponse(response, err)
		return response
	}
	if err := outputHierarchy.CreateParentDirectories(inputRootDirectory); err != nil {
		attachErrorToExecuteResponse(response, err)
		return response
	}

	// Create a directory inside the build directory that build
	// actions may use to store temporary files. This ensures that
	// temporary files are automatically removed when the build
	// action completes. When using FUSE, it also causes quotas to
	// be applied to them.
	if err := buildDirectory.Mkdir(temporaryDirectoryComponent, 0o777); err != nil {
		attachErrorToExecuteResponse(
			response,
			util.StatusWrap(err, "Failed to create temporary directory inside build directory"))
		return response
	}

	executionStateUpdates <- &remoteworker.CurrentState_Executing{
		ActionDigest: request.ActionDigest,
		ExecutionState: &remoteworker.CurrentState_Executing_Running{
			Running: &emptypb.Empty{},
		},
	}

	// Invoke the command.
	ctxWithTimeout, cancelTimeout := be.clock.NewContextWithTimeout(ctxWithIOError, executionTimeout)
	defer cancelTimeout()
	environmentVariables := map[string]string{}
	for name, value := range be.environmentVariables {
		environmentVariables[name] = value
	}
	for _, environmentVariable := range command.EnvironmentVariables {
		environmentVariables[environmentVariable.Name] = environmentVariable.Value
	}
	runResponse, runErr := be.runner.Run(ctxWithTimeout, &runner_pb.RunRequest{
		Arguments:            command.Arguments,
		EnvironmentVariables: environmentVariables,
		WorkingDirectory:     command.WorkingDirectory,
		StdoutPath:           buildDirectoryPath.Append(stdoutComponent).String(),
		StderrPath:           buildDirectoryPath.Append(stderrComponent).String(),
		InputRootDirectory:   buildDirectoryPath.Append(inputRootDirectoryComponent).String(),
		TemporaryDirectory:   buildDirectoryPath.Append(temporaryDirectoryComponent).String(),
	})

	// If an I/O error occurred during execution, attach any errors
	// related to it to the response first. These errors should be
	// preferred over the cancelation errors that are a result of it.
	if err := ioErrorCapturer.GetError(); err != nil {
		attachErrorToExecuteResponse(response, util.StatusWrap(err, "I/O error while running command"))
	}

	// Attach the exit code or execution error.
	if runErr == nil {
		response.Result.ExitCode = runResponse.ExitCode
		response.Result.ExecutionMetadata.AuxiliaryMetadata = append(response.Result.ExecutionMetadata.AuxiliaryMetadata, runResponse.ResourceUsage...)
	} else {
		attachErrorToExecuteResponse(response, util.StatusWrap(runErr, "Failed to run command"))
	}

	executionStateUpdates <- &remoteworker.CurrentState_Executing{
		ActionDigest: request.ActionDigest,
		ExecutionState: &remoteworker.CurrentState_Executing_UploadingOutputs{
			UploadingOutputs: &emptypb.Empty{},
		},
	}

	// Upload command output. In the common case, the stdout and
	// stderr files are empty. If that's the case, don't bother
	// setting the digest to keep the ActionResult small.
	digestFunction := actionDigest.GetDigestFunction()
	if stdoutDigest, err := buildDirectory.UploadFile(ctx, stdoutComponent, digestFunction); err != nil {
		attachErrorToExecuteResponse(response, util.StatusWrap(err, "Failed to store stdout"))
	} else if stdoutDigest.GetSizeBytes() > 0 {
		response.Result.StdoutDigest = stdoutDigest.GetProto()
	}
	if stderrDigest, err := buildDirectory.UploadFile(ctx, stderrComponent, digestFunction); err != nil {
		attachErrorToExecuteResponse(response, util.StatusWrap(err, "Failed to store stderr"))
	} else if stderrDigest.GetSizeBytes() > 0 {
		response.Result.StderrDigest = stderrDigest.GetProto()
	}
	if err := outputHierarchy.UploadOutputs(ctx, inputRootDirectory, be.contentAddressableStorage, digestFunction, response.Result); err != nil {
		attachErrorToExecuteResponse(response, err)
	}

	return response
}
