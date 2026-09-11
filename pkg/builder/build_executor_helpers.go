package builder

import (
	"context"
	"os"
	"sync"
	"time"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	re_clock "github.com/buildbarn/bb-remote-execution/pkg/clock"
	"github.com/buildbarn/bb-remote-execution/pkg/filesystem/access"
	"github.com/buildbarn/bb-storage/pkg/blobstore"
	"github.com/buildbarn/bb-storage/pkg/cas"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/filesystem"
	"github.com/buildbarn/bb-storage/pkg/filesystem/path"
	"github.com/buildbarn/bb-storage/pkg/util"

	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/durationpb"
)

// Filenames of objects to be created inside the build directory.
var (
	stdoutComponent              = path.MustNewComponent("stdout")
	stderrComponent              = path.MustNewComponent("stderr")
	deviceDirectoryComponent     = path.MustNewComponent("dev")
	inputRootDirectoryComponent  = path.MustNewComponent("root")
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

func (logger *capturingErrorLogger) Log(err error) {
	logger.lock.Lock()
	defer logger.lock.Unlock()

	if logger.cancel != nil {
		logger.error = err
		logger.cancel()
		logger.cancel = nil
	}
}

func (logger *capturingErrorLogger) GetError() error {
	logger.lock.Lock()
	defer logger.lock.Unlock()

	return logger.error
}

func populateInputRoot(ctx context.Context, inputRootDirectory BuildDirectory, errorLogger util.ErrorLogger, digestFunction digest.Function, inputRootDigestProto *remoteexecution.Digest, monitor access.UnreadDirectoryMonitor, characterDevices map[path.Component]filesystem.DeviceNumber) error {
	inputRootDigest, err := digestFunction.NewDigestFromProto(inputRootDigestProto)
	if err != nil {
		return util.StatusWrap(err, "Failed to extract digest for input root")
	}
	if err := inputRootDirectory.MergeDirectoryContents(ctx, errorLogger, inputRootDigest, monitor); err != nil {
		return err
	}
	if len(characterDevices) == 0 {
		return nil
	}
	if err := inputRootDirectory.Mkdir(deviceDirectoryComponent, 0o777); err != nil && !os.IsExist(err) {
		return util.StatusWrap(err, "Unable to create /dev directory in input root")
	}
	deviceDirectory, err := inputRootDirectory.EnterBuildDirectory(deviceDirectoryComponent)
	if err != nil {
		return util.StatusWrap(err, "Unable to enter /dev directory in input root")
	}
	defer deviceDirectory.Close()
	for name, number := range characterDevices {
		if err := deviceDirectory.Mknod(name, os.ModeDevice|os.ModeCharDevice|0o666, number); err != nil {
			return util.StatusWrapf(err, "Failed to create character device %#v", name.String())
		}
	}
	return nil
}

func readCommand(ctx context.Context, commandReader cas.MessageReader[*remoteexecution.Command], digestFunction digest.Function, commandDigestProto *remoteexecution.Digest) (*remoteexecution.Command, error) {
	commandDigest, err := digestFunction.NewDigestFromProto(commandDigestProto)
	if err != nil {
		return nil, util.StatusWrap(err, "Failed to extract digest for command")
	}
	command, err := commandReader.ReadMessage(ctx, commandDigest)
	if err != nil {
		return nil, util.StatusWrap(err, "Failed to obtain command")
	}
	return command, nil
}

func getCommandEnvironmentVariables(command *remoteexecution.Command, defaults map[string]string) map[string]string {
	environmentVariables := map[string]string{}
	for name, value := range defaults {
		environmentVariables[name] = value
	}
	for _, environmentVariable := range command.EnvironmentVariables {
		environmentVariables[environmentVariable.Name] = environmentVariable.Value
	}
	return environmentVariables
}

func attachExecutionResult(ctx context.Context, response *remoteexecution.ExecuteResponse, exitCode int32, resourceUsage []*anypb.Any, executionError, ioError error) {
	// If an I/O error occurred during execution, attach any errors
	// related to it to the response first. These errors should be
	// preferred over the cancelation errors that are a result of it.
	if ioError != nil {
		attachErrorToExecuteResponse(response, util.StatusWrap(ioError, "I/O error while running command"))
	}

	// Attach the exit code or execution error.
	if executionError == nil {
		response.Result.ExitCode = exitCode
		response.Result.ExecutionMetadata.AuxiliaryMetadata = append(response.Result.ExecutionMetadata.AuxiliaryMetadata, resourceUsage...)
	} else {
		attachErrorToExecuteResponse(response, util.StatusWrap(executionError, "Failed to run command"))
	}

	// For FUSE-based workers: Attach the amount of time the action
	// ran, minus the time it was delayed reading data from storage.
	if unsuspendedDuration, ok := ctx.Value(re_clock.UnsuspendedDurationKey{}).(time.Duration); ok {
		response.Result.ExecutionMetadata.VirtualExecutionDuration = durationpb.New(unsuspendedDuration)
	}
}

func uploadBuildOutputs(ctx context.Context, buildDirectory, inputRootDirectory UploadableDirectory, outputHierarchy *OutputHierarchy, contentAddressableStorage blobstore.BlobAccess, digestFunction digest.Function, writableFileUploadDelay <-chan struct{}, response *remoteexecution.ExecuteResponse, forceUploadTreesAndDirectories bool) {
	// Upload command output. In the common case, the stdout and
	// stderr files are empty. If that's the case, don't bother
	// setting the digest to keep the ActionResult small.
	if stdoutDigest, err := buildDirectory.UploadFile(ctx, stdoutComponent, digestFunction, writableFileUploadDelay); err != nil {
		attachErrorToExecuteResponse(response, util.StatusWrap(err, "Failed to store stdout"))
	} else if stdoutDigest.GetSizeBytes() > 0 {
		response.Result.StdoutDigest = stdoutDigest.GetProto()
	}
	if stderrDigest, err := buildDirectory.UploadFile(ctx, stderrComponent, digestFunction, writableFileUploadDelay); err != nil {
		attachErrorToExecuteResponse(response, util.StatusWrap(err, "Failed to store stderr"))
	} else if stderrDigest.GetSizeBytes() > 0 {
		response.Result.StderrDigest = stderrDigest.GetProto()
	}
	if err := outputHierarchy.UploadOutputs(ctx, inputRootDirectory, contentAddressableStorage, digestFunction, writableFileUploadDelay, response.Result, forceUploadTreesAndDirectories); err != nil {
		attachErrorToExecuteResponse(response, err)
	}

	// Recursively traverse the server logs directory and attach any
	// file stored within to the ExecuteResponse.
	uploader := serverLogsDirectoryUploader{
		context:                 ctx,
		executeResponse:         response,
		digestFunction:          digestFunction,
		writableFileUploadDelay: writableFileUploadDelay,
	}
	uploader.uploadDirectory(buildDirectory, serverLogsDirectoryComponent, nil)
}

type serverLogsDirectoryUploader struct {
	context                 context.Context
	executeResponse         *remoteexecution.ExecuteResponse
	digestFunction          digest.Function
	writableFileUploadDelay <-chan struct{}
}

func (uploader *serverLogsDirectoryUploader) uploadDirectory(parentDirectory UploadableDirectory, directoryName path.Component, directoryPath *path.Trace) {
	directory, err := parentDirectory.EnterUploadableDirectory(directoryName)
	if err != nil {
		attachErrorToExecuteResponse(uploader.executeResponse, util.StatusWrapf(err, "Failed to enter server logs directory %#v", directoryPath.GetUNIXString()))
		return
	}
	defer directory.Close()

	files, err := directory.ReadDir()
	if err != nil {
		attachErrorToExecuteResponse(uploader.executeResponse, util.StatusWrapf(err, "Failed to read server logs directory %#v", directoryPath.GetUNIXString()))
		return
	}

	for _, file := range files {
		childName := file.Name()
		childPath := directoryPath.Append(childName)
		switch fileType := file.Type(); fileType {
		case filesystem.FileTypeRegularFile:
			if childDigest, err := directory.UploadFile(uploader.context, childName, uploader.digestFunction, uploader.writableFileUploadDelay); err == nil {
				uploader.executeResponse.ServerLogs[childPath.GetUNIXString()] = &remoteexecution.LogFile{
					Digest: childDigest.GetProto(),
				}
			} else {
				attachErrorToExecuteResponse(uploader.executeResponse, util.StatusWrapf(err, "Failed to store server log %#v", childPath.GetUNIXString()))
			}
		case filesystem.FileTypeDirectory:
			uploader.uploadDirectory(directory, childName, childPath)
		}
	}
}
