package builder

import (
	"context"
	"time"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-remote-execution/pkg/filesystem/access"
	"github.com/buildbarn/bb-remote-execution/pkg/filesystem/pool"
	"github.com/buildbarn/bb-remote-execution/pkg/proto/remoteworker"
	runner_pb "github.com/buildbarn/bb-remote-execution/pkg/proto/runner"
	"github.com/buildbarn/bb-storage/pkg/blobstore"
	"github.com/buildbarn/bb-storage/pkg/cas"
	"github.com/buildbarn/bb-storage/pkg/clock"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/filesystem"
	"github.com/buildbarn/bb-storage/pkg/filesystem/path"
	"github.com/buildbarn/bb-storage/pkg/util"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
)

type localBuildExecutor struct {
	contentAddressableStorage      blobstore.BlobAccess
	commandReader                  cas.MessageReader[*remoteexecution.Command]
	buildDirectoryCreator          BuildDirectoryCreator
	runner                         runner_pb.RunnerClient
	clock                          clock.Clock
	maximumWritableFileUploadDelay time.Duration
	inputRootCharacterDevices      map[path.Component]filesystem.DeviceNumber
	environmentVariables           map[string]string
	forceUploadTreesAndDirectories bool
}

// NewLocalBuildExecutor returns a BuildExecutor that executes build
// steps on the local system.
func NewLocalBuildExecutor(contentAddressableStorage blobstore.BlobAccess, commandReader cas.MessageReader[*remoteexecution.Command], buildDirectoryCreator BuildDirectoryCreator, runner runner_pb.RunnerClient, clock clock.Clock, maximumWritableFileUploadDelay time.Duration, inputRootCharacterDevices map[path.Component]filesystem.DeviceNumber, environmentVariables map[string]string, forceUploadTreesAndDirectories bool) BuildExecutor {
	return &localBuildExecutor{
		contentAddressableStorage:      contentAddressableStorage,
		commandReader:                  commandReader,
		buildDirectoryCreator:          buildDirectoryCreator,
		runner:                         runner,
		clock:                          clock,
		maximumWritableFileUploadDelay: maximumWritableFileUploadDelay,
		inputRootCharacterDevices:      inputRootCharacterDevices,
		environmentVariables:           environmentVariables,
		forceUploadTreesAndDirectories: forceUploadTreesAndDirectories,
	}
}

func (be *localBuildExecutor) CheckReadiness(ctx context.Context) error {
	buildDirectory, buildDirectoryPath, err := be.buildDirectoryCreator.GetBuildDirectory(ctx, nil)
	if err != nil {
		return util.StatusWrap(err, "Failed to get build directory")
	}
	defer buildDirectory.Close()

	// Create a useless directory inside the build directory. The
	// runner will validate that it exists.
	if err := buildDirectory.Mkdir(checkReadinessComponent, 0o777); err != nil {
		return util.StatusWrap(err, "Failed to create readiness checking directory")
	}
	_, err = be.runner.CheckReadiness(ctx, &runner_pb.CheckReadinessRequest{
		Path: buildDirectoryPath.Append(checkReadinessComponent).GetUNIXString(),
	})
	return err
}

func (be *localBuildExecutor) Execute(ctx context.Context, filePool pool.FilePool, monitor access.UnreadDirectoryMonitor, digestFunction digest.Function, request *remoteworker.DesiredState_Executing, executionStateUpdates chan<- *remoteworker.CurrentState_Executing) *remoteexecution.ExecuteResponse {
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
			util.StatusWrapWithCode(err, codes.InvalidArgument, "Invalid execution timeout"),
		)
		return response
	}
	executionTimeout := action.Timeout.AsDuration()

	// Obtain build directory.
	actionDigest, err := digestFunction.NewDigestFromProto(request.ActionDigest)
	if err != nil {
		attachErrorToExecuteResponse(response, util.StatusWrap(err, "Failed to extract digest for action"))
		return response
	}
	var actionDigestIfNotRunInParallel *digest.Digest
	if !action.DoNotCache {
		actionDigestIfNotRunInParallel = &actionDigest
	}
	buildDirectory, buildDirectoryPath, err := be.buildDirectoryCreator.GetBuildDirectory(ctx, actionDigestIfNotRunInParallel)
	if err != nil {
		attachErrorToExecuteResponse(
			response,
			util.StatusWrap(err, "Failed to acquire build environment"),
		)
		return response
	}
	defer func() {
		err := buildDirectory.Close()
		if err != nil {
			attachErrorToExecuteResponse(
				response,
				util.StatusWrap(err, "Failed to close build directory"),
			)
		}
	}()

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
			util.StatusWrap(err, "Failed to create input root directory"),
		)
		return response
	}
	inputRootDirectory, err := buildDirectory.EnterBuildDirectory(inputRootDirectoryComponent)
	if err != nil {
		attachErrorToExecuteResponse(
			response,
			util.StatusWrap(err, "Failed to enter input root directory"),
		)
		return response
	}
	defer inputRootDirectory.Close()

	if err := populateInputRoot(ctx, inputRootDirectory, &ioErrorCapturer, digestFunction, action.InputRootDigest, monitor, be.inputRootCharacterDevices); err != nil {
		attachErrorToExecuteResponse(response, err)
		return response
	}

	// Create parent directories of output files and directories.
	// These are not declared in the input root explicitly.
	command, err := readCommand(ctx, be.commandReader, digestFunction, action.CommandDigest)
	if err != nil {
		attachErrorToExecuteResponse(response, err)
		return response
	}
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
			util.StatusWrap(err, "Failed to create temporary directory inside build directory"),
		)
		return response
	}

	if err := buildDirectory.Mkdir(serverLogsDirectoryComponent, 0o777); err != nil {
		attachErrorToExecuteResponse(
			response,
			util.StatusWrap(err, "Failed to create server logs directory inside build directory"),
		)
		return response
	}

	executionStateUpdates <- &remoteworker.CurrentState_Executing{
		ActionDigest: request.ActionDigest,
		ExecutionState: &remoteworker.CurrentState_Executing_Running{
			Running: &emptypb.Empty{},
		},
	}

	environmentVariables := getCommandEnvironmentVariables(command, be.environmentVariables)

	// Invoke the command.
	ctxWithTimeout, cancelTimeout := be.clock.NewContextWithTimeout(ctxWithIOError, executionTimeout)
	runResponse, runErr := be.runner.Run(ctxWithTimeout, &runner_pb.RunRequest{
		Arguments:            command.Arguments,
		EnvironmentVariables: environmentVariables,
		WorkingDirectory:     command.WorkingDirectory,
		StdoutPath:           buildDirectoryPath.Append(stdoutComponent).GetUNIXString(),
		StderrPath:           buildDirectoryPath.Append(stderrComponent).GetUNIXString(),
		InputRootDirectory:   buildDirectoryPath.Append(inputRootDirectoryComponent).GetUNIXString(),
		TemporaryDirectory:   buildDirectoryPath.Append(temporaryDirectoryComponent).GetUNIXString(),
		ServerLogsDirectory:  buildDirectoryPath.Append(serverLogsDirectoryComponent).GetUNIXString(),
	})
	cancelTimeout()
	<-ctxWithTimeout.Done()

	attachExecutionResult(ctxWithTimeout, response, int32(runResponse.GetExitCode()), runResponse.GetResourceUsage(), runErr, ioErrorCapturer.GetError())

	executionStateUpdates <- &remoteworker.CurrentState_Executing{
		ActionDigest: request.ActionDigest,
		ExecutionState: &remoteworker.CurrentState_Executing_UploadingOutputs{
			UploadingOutputs: &emptypb.Empty{},
		},
	}

	writableFileUploadDelayCtx, writableFileUploadDelayCancel := be.clock.NewContextWithTimeout(ctx, be.maximumWritableFileUploadDelay)
	defer writableFileUploadDelayCancel()
	uploadBuildOutputs(ctx, buildDirectory, inputRootDirectory, outputHierarchy, be.contentAddressableStorage, digestFunction, writableFileUploadDelayCtx.Done(), response, be.forceUploadTreesAndDirectories)

	return response
}
