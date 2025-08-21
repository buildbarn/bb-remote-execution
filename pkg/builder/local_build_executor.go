package builder

import (
	"context"
	"os"
	"sync"
	"time"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	re_clock "github.com/buildbarn/bb-remote-execution/pkg/clock"
	"github.com/buildbarn/bb-remote-execution/pkg/filesystem/access"
	"github.com/buildbarn/bb-remote-execution/pkg/filesystem/pool"
	"github.com/buildbarn/bb-remote-execution/pkg/proto/remoteworker"
	runner_pb "github.com/buildbarn/bb-remote-execution/pkg/proto/runner"
	"github.com/buildbarn/bb-storage/pkg/blobstore"
	"github.com/buildbarn/bb-storage/pkg/clock"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/filesystem"
	"github.com/buildbarn/bb-storage/pkg/filesystem/path"
	"github.com/buildbarn/bb-storage/pkg/util"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/emptypb"
)

// Filenames of objects to be created inside the build directory.
var (
	stdoutComponent              = path.MustNewComponent("stdout")
	stderrComponent              = path.MustNewComponent("stderr")
	deviceDirectoryComponent     = path.MustNewComponent("dev")
	serverLogsDirectoryComponent = path.MustNewComponent("server_logs")
	temporaryDirectoryComponent  = path.MustNewComponent("tmp")
	checkReadinessComponent      = path.MustNewComponent("check_readiness")
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
	contentAddressableStorage              blobstore.BlobAccess
	buildDirectoryCreator                  BuildDirectoryCreator
	runner                                 runner_pb.RunnerClient
	clock                                  clock.Clock
	maximumWritableFileUploadDelay         time.Duration
	inputRootCharacterDevices              map[path.Component]filesystem.DeviceNumber
	maximumMessageSizeBytes                int
	environmentVariables                   map[string]string
	forceUploadTreesAndDirectories         bool
	supportLegacyOutputFilesAndDirectories bool
	inputRootComponents                    []path.Component
}

// NewLocalBuildExecutor returns a BuildExecutor that executes build
// steps on the local system.
func NewLocalBuildExecutor(contentAddressableStorage blobstore.BlobAccess, buildDirectoryCreator BuildDirectoryCreator, runner runner_pb.RunnerClient, clock clock.Clock, maximumWritableFileUploadDelay time.Duration, inputRootCharacterDevices map[path.Component]filesystem.DeviceNumber, maximumMessageSizeBytes int, environmentVariables map[string]string, forceUploadTreesAndDirectories, supportLegacyOutputFilesAndDirectories bool, inputRootComponents []path.Component) BuildExecutor {
	return &localBuildExecutor{
		contentAddressableStorage:              contentAddressableStorage,
		buildDirectoryCreator:                  buildDirectoryCreator,
		runner:                                 runner,
		clock:                                  clock,
		maximumWritableFileUploadDelay:         maximumWritableFileUploadDelay,
		inputRootCharacterDevices:              inputRootCharacterDevices,
		maximumMessageSizeBytes:                maximumMessageSizeBytes,
		environmentVariables:                   environmentVariables,
		forceUploadTreesAndDirectories:         forceUploadTreesAndDirectories,
		supportLegacyOutputFilesAndDirectories: supportLegacyOutputFilesAndDirectories,
		inputRootComponents:                    inputRootComponents,
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
			util.StatusWrapWithCode(err, codes.InvalidArgument, "Invalid execution timeout"))
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
			util.StatusWrap(err, "Failed to acquire build environment"))
		return response
	}
	defer func() {
		err := buildDirectory.Close()
		if err != nil {
			attachErrorToExecuteResponse(
				response,
				util.StatusWrap(err, "Failed to close build directory"))
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
	inputRootDirectory := buildDirectory
	inputRootDirectoryPath := buildDirectoryPath
	for _, component := range be.inputRootComponents {
		if err := inputRootDirectory.Mkdir(component, 0o777); err != nil {
			attachErrorToExecuteResponse(
				response,
				util.StatusWrap(err, "Failed to create input root directory"))
			return response
		}
		inputRootDirectory, err = inputRootDirectory.EnterBuildDirectory(component)
		if err != nil {
			attachErrorToExecuteResponse(
				response,
				util.StatusWrap(err, "Failed to enter main root directory"))
			return response
		}
		defer inputRootDirectory.Close()
		inputRootDirectoryPath = inputRootDirectoryPath.Append(component)
	}

	inputRootDigest, err := digestFunction.NewDigestFromProto(action.InputRootDigest)
	if err != nil {
		attachErrorToExecuteResponse(
			response,
			util.StatusWrap(err, "Failed to extract digest for input root"))
		return response
	}
	if err := inputRootDirectory.MergeDirectoryContents(ctx, &ioErrorCapturer, inputRootDigest, monitor); err != nil {
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
	commandDigest, err := digestFunction.NewDigestFromProto(action.CommandDigest)
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
	outputHierarchy, err := NewOutputHierarchy(command, be.supportLegacyOutputFilesAndDirectories)
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

	if err := buildDirectory.Mkdir(serverLogsDirectoryComponent, 0o777); err != nil {
		attachErrorToExecuteResponse(
			response,
			util.StatusWrap(err, "Failed to create server logs directory inside build directory"))
		return response
	}

	executionStateUpdates <- &remoteworker.CurrentState_Executing{
		ActionDigest: request.ActionDigest,
		ExecutionState: &remoteworker.CurrentState_Executing_Running{
			Running: &emptypb.Empty{},
		},
	}

	environmentVariables := map[string]string{}
	for name, value := range be.environmentVariables {
		environmentVariables[name] = value
	}
	for _, environmentVariable := range command.EnvironmentVariables {
		environmentVariables[environmentVariable.Name] = environmentVariable.Value
	}

	// Invoke the command.
	ctxWithTimeout, cancelTimeout := be.clock.NewContextWithTimeout(ctxWithIOError, executionTimeout)
	runResponse, runErr := be.runner.Run(ctxWithTimeout, &runner_pb.RunRequest{
		Arguments:            command.Arguments,
		EnvironmentVariables: environmentVariables,
		WorkingDirectory:     command.WorkingDirectory,
		StdoutPath:           buildDirectoryPath.Append(stdoutComponent).GetUNIXString(),
		StderrPath:           buildDirectoryPath.Append(stderrComponent).GetUNIXString(),
		InputRootDirectory:   inputRootDirectoryPath.GetUNIXString(),
		TemporaryDirectory:   buildDirectoryPath.Append(temporaryDirectoryComponent).GetUNIXString(),
		ServerLogsDirectory:  buildDirectoryPath.Append(serverLogsDirectoryComponent).GetUNIXString(),
	})
	cancelTimeout()
	<-ctxWithTimeout.Done()

	// If an I/O error occurred during execution, attach any errors
	// related to it to the response first. These errors should be
	// preferred over the cancelation errors that are a result of it.
	if err := ioErrorCapturer.GetError(); err != nil {
		attachErrorToExecuteResponse(response, util.StatusWrap(err, "I/O error while running command"))
	}

	// Attach the exit code or execution error.
	if runErr == nil {
		response.Result.ExitCode = int32(runResponse.ExitCode)
		response.Result.ExecutionMetadata.AuxiliaryMetadata = append(response.Result.ExecutionMetadata.AuxiliaryMetadata, runResponse.ResourceUsage...)
	} else {
		attachErrorToExecuteResponse(response, util.StatusWrap(runErr, "Failed to run command"))
	}

	// For FUSE-based workers: Attach the amount of time the action
	// ran, minus the time it was delayed reading data from storage.
	if unsuspendedDuration, ok := ctxWithTimeout.Value(re_clock.UnsuspendedDurationKey{}).(time.Duration); ok {
		response.Result.ExecutionMetadata.VirtualExecutionDuration = durationpb.New(unsuspendedDuration)
	}

	executionStateUpdates <- &remoteworker.CurrentState_Executing{
		ActionDigest: request.ActionDigest,
		ExecutionState: &remoteworker.CurrentState_Executing_UploadingOutputs{
			UploadingOutputs: &emptypb.Empty{},
		},
	}

	writableFileUploadDelayCtx, writableFileUploadDelayCancel := be.clock.NewContextWithTimeout(ctx, be.maximumWritableFileUploadDelay)
	defer writableFileUploadDelayCancel()
	writableFileUploadDelayChan := writableFileUploadDelayCtx.Done()

	// Upload command output. In the common case, the stdout and
	// stderr files are empty. If that's the case, don't bother
	// setting the digest to keep the ActionResult small.
	if stdoutDigest, err := buildDirectory.UploadFile(ctx, stdoutComponent, digestFunction, writableFileUploadDelayChan); err != nil {
		attachErrorToExecuteResponse(response, util.StatusWrap(err, "Failed to store stdout"))
	} else if stdoutDigest.GetSizeBytes() > 0 {
		response.Result.StdoutDigest = stdoutDigest.GetProto()
	}
	if stderrDigest, err := buildDirectory.UploadFile(ctx, stderrComponent, digestFunction, writableFileUploadDelayChan); err != nil {
		attachErrorToExecuteResponse(response, util.StatusWrap(err, "Failed to store stderr"))
	} else if stderrDigest.GetSizeBytes() > 0 {
		response.Result.StderrDigest = stderrDigest.GetProto()
	}
	if err := outputHierarchy.UploadOutputs(ctx, inputRootDirectory, be.contentAddressableStorage, digestFunction, writableFileUploadDelayChan, response.Result, be.forceUploadTreesAndDirectories); err != nil {
		attachErrorToExecuteResponse(response, err)
	}

	// Recursively traverse the server logs directory and attach any
	// file stored within to the ExecuteResponse.
	serverLogsDirectoryUploader := serverLogsDirectoryUploader{
		context:                 ctx,
		executeResponse:         response,
		digestFunction:          digestFunction,
		writableFileUploadDelay: writableFileUploadDelayChan,
	}
	serverLogsDirectoryUploader.uploadDirectory(buildDirectory, serverLogsDirectoryComponent, nil)

	return response
}

type serverLogsDirectoryUploader struct {
	context                 context.Context
	executeResponse         *remoteexecution.ExecuteResponse
	digestFunction          digest.Function
	writableFileUploadDelay <-chan struct{}
}

func (u *serverLogsDirectoryUploader) uploadDirectory(parentDirectory UploadableDirectory, dName path.Component, dPath *path.Trace) {
	d, err := parentDirectory.EnterUploadableDirectory(dName)
	if err != nil {
		attachErrorToExecuteResponse(u.executeResponse, util.StatusWrapf(err, "Failed to enter server logs directory %#v", dPath.GetUNIXString()))
		return
	}
	defer d.Close()

	files, err := d.ReadDir()
	if err != nil {
		attachErrorToExecuteResponse(u.executeResponse, util.StatusWrapf(err, "Failed to read server logs directory %#v", dPath.GetUNIXString()))
		return
	}

	for _, file := range files {
		childName := file.Name()
		childPath := dPath.Append(childName)
		switch fileType := file.Type(); fileType {
		case filesystem.FileTypeRegularFile:
			if childDigest, err := d.UploadFile(u.context, childName, u.digestFunction, u.writableFileUploadDelay); err == nil {
				u.executeResponse.ServerLogs[childPath.GetUNIXString()] = &remoteexecution.LogFile{
					Digest: childDigest.GetProto(),
				}
			} else {
				attachErrorToExecuteResponse(u.executeResponse, util.StatusWrapf(err, "Failed to store server log %#v", childPath.GetUNIXString()))
			}
		case filesystem.FileTypeDirectory:
			u.uploadDirectory(d, childName, childPath)
		}
	}
}
