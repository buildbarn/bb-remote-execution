package builder

import (
	"context"
	"crypto/sha256"
	"math"
	"sync"
	"time"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	re_cas "github.com/buildbarn/bb-remote-execution/pkg/cas"
	"github.com/buildbarn/bb-remote-execution/pkg/filesystem/access"
	"github.com/buildbarn/bb-remote-execution/pkg/filesystem/pool"
	"github.com/buildbarn/bb-remote-execution/pkg/proto/remoteworker"
	runner_pb "github.com/buildbarn/bb-remote-execution/pkg/proto/runner"
	worker_pb "github.com/buildbarn/bb-remote-execution/pkg/proto/worker"
	"github.com/buildbarn/bb-storage/pkg/blobstore"
	"github.com/buildbarn/bb-storage/pkg/blobstore/buffer"
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

// PersistentBuildExecutor executes eligible actions in persistent worker
// session, delegating other actions to an ordinary BuildExecutor. Each
// instance belongs to one execution slot and retains at most one session.
type PersistentBuildExecutor struct {
	lifetimeContext                context.Context
	ordinary                       BuildExecutor
	contentAddressableStorage      blobstore.BlobAccess
	commandReader                  cas.MessageReader[*remoteexecution.Command]
	directoryFetcher               re_cas.DirectoryFetcher
	buildDirectoryCreator          BuildDirectoryCreator
	runner                         runner_pb.PersistentRunnerClient
	filePool                       pool.FilePool
	clock                          clock.Clock
	maximumWritableFileUploadDelay time.Duration
	sessionCleanupTimeout          time.Duration
	inputRootCharacterDevices      map[path.Component]filesystem.DeviceNumber
	environmentVariables           map[string]string
	forceUploadTreesAndDirectories bool

	lock                     sync.Mutex
	closed                   bool
	workspace                *PersistentWorkerWorkspace
	compatibilityKey         [sha256.Size]byte
	sessionID                string
	sessionCreationUncertain bool
	retiring                 bool
}

// NewPersistentBuildExecutor creates an executor for one slot and a fixed
// runner endpoint. The context and quota-enforcing file pool must outlive
// individual actions. The directory creator must provide isolated session
// directories. SessionCleanupTimeout bounds cleanup independently of action
// cancellation. The caller must cancel and join active execution before Close.
func NewPersistentBuildExecutor(ctx context.Context, ordinary BuildExecutor, contentAddressableStorage blobstore.BlobAccess, commandReader cas.MessageReader[*remoteexecution.Command], directoryFetcher re_cas.DirectoryFetcher, buildDirectoryCreator BuildDirectoryCreator, runner runner_pb.PersistentRunnerClient, filePool pool.FilePool, clock clock.Clock, maximumWritableFileUploadDelay, sessionCleanupTimeout time.Duration, inputRootCharacterDevices map[path.Component]filesystem.DeviceNumber, environmentVariables map[string]string, forceUploadTreesAndDirectories bool) *PersistentBuildExecutor {
	return &PersistentBuildExecutor{
		lifetimeContext:                ctx,
		ordinary:                       ordinary,
		contentAddressableStorage:      contentAddressableStorage,
		commandReader:                  commandReader,
		directoryFetcher:               directoryFetcher,
		buildDirectoryCreator:          buildDirectoryCreator,
		runner:                         runner,
		filePool:                       filePool,
		clock:                          clock,
		maximumWritableFileUploadDelay: maximumWritableFileUploadDelay,
		sessionCleanupTimeout:          sessionCleanupTimeout,
		inputRootCharacterDevices:      inputRootCharacterDevices,
		environmentVariables:           environmentVariables,
		forceUploadTreesAndDirectories: forceUploadTreesAndDirectories,
	}
}

// CheckReadiness verifies both execution paths and shared-directory access.
// An unusable retained session must be retired before the slot is ready.
func (executor *PersistentBuildExecutor) CheckReadiness(ctx context.Context) error {
	if !executor.lock.TryLock() {
		return status.Error(codes.FailedPrecondition, "Persistent build executor is busy")
	}
	defer executor.lock.Unlock()
	if err := executor.checkAvailability(ctx); err != nil {
		return err
	}
	if err := executor.ordinary.CheckReadiness(ctx); err != nil {
		return err
	}
	directory, directoryPath, err := executor.buildDirectoryCreator.GetBuildDirectory(ctx, nil)
	if err != nil {
		return err
	}
	if err := directory.Mkdir(checkReadinessComponent, 0o777); err != nil {
		directory.Close()
		return err
	}
	_, readinessError := executor.runner.CheckReadiness(ctx, &runner_pb.CheckReadinessRequest{
		Path: directoryPath.Append(checkReadinessComponent).GetUNIXString(),
	})
	closeError := directory.Close()
	if readinessError != nil {
		return readinessError
	}
	return closeError
}

func (executor *PersistentBuildExecutor) checkAvailability(ctx context.Context) error {
	if executor.closed || executor.lifetimeContext.Err() != nil {
		return status.Error(codes.Unavailable, "Persistent build executor is shutting down")
	}
	if err := ctx.Err(); err != nil {
		return status.FromContextError(err).Err()
	}
	if executor.workspace != nil && (executor.retiring || executor.workspace.context.Err() != nil) {
		return executor.retireSessionWithTimeout(ctx)
	}
	return nil
}

// Execute prepares and runs one action, reusing a compatible session when
// possible. Fallback is permitted only before persistent dispatch; failed
// requests are never retried. The per-action pool and monitor are forwarded
// only to ordinary execution, not retained by persistent filesystem nodes.
func (executor *PersistentBuildExecutor) Execute(ctx context.Context, filePool pool.FilePool, monitor access.UnreadDirectoryMonitor, digestFunction digest.Function, request *remoteworker.DesiredState_Executing, executionStateUpdates chan<- *remoteworker.CurrentState_Executing) *remoteexecution.ExecuteResponse {
	response := NewDefaultExecuteResponse(request)
	if !executor.lock.TryLock() {
		attachErrorToExecuteResponse(response, status.Error(codes.FailedPrecondition, "Persistent build executor is busy"))
		return response
	}
	defer executor.lock.Unlock()
	if err := executor.checkAvailability(ctx); err != nil {
		attachErrorToExecuteResponse(response, err)
		return response
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	stopLifetimeWatch := context.AfterFunc(executor.lifetimeContext, cancel)
	defer stopLifetimeWatch()
	action := request.Action
	if action == nil {
		attachErrorToExecuteResponse(response, status.Error(codes.InvalidArgument, "Request does not contain an action"))
		return response
	}
	command, err := readCommand(ctx, executor.commandReader, digestFunction, action.CommandDigest)
	if err != nil {
		attachErrorToExecuteResponse(response, err)
		return response
	}
	platform, err := getPersistentWorkerPlatform(action, command)
	if err != nil {
		attachErrorToExecuteResponse(response, err)
		return response
	}
	if platform == nil {
		return executor.ordinary.Execute(ctx, filePool, monitor, digestFunction, request, executionStateUpdates)
	}
	if err := action.Timeout.CheckValid(); err != nil {
		attachErrorToExecuteResponse(response, util.StatusWrapWithCode(err, codes.InvalidArgument, "Invalid execution timeout"))
		return response
	}
	if _, err := digestFunction.NewDigestFromProto(request.ActionDigest); err != nil {
		attachErrorToExecuteResponse(response, util.StatusWrap(err, "Failed to extract digest for action"))
		return response
	}
	inputRootDigest, err := digestFunction.NewDigestFromProto(action.InputRootDigest)
	if err != nil {
		attachErrorToExecuteResponse(response, util.StatusWrap(err, "Failed to extract digest for input root"))
		return response
	}
	inputs, hasSymlinks, err := readPersistentWorkerInputs(ctx, executor.directoryFetcher, inputRootDigest)
	if err != nil {
		attachErrorToExecuteResponse(response, err)
		return response
	}
	if hasSymlinks {
		return executor.ordinary.Execute(ctx, filePool, monitor, digestFunction, request, executionStateUpdates)
	}
	prepared, err := NewPersistentWorkerCommand(platform, command, digestFunction, inputs, getCommandEnvironmentVariables(command, executor.environmentVariables))
	if err != nil {
		attachErrorToExecuteResponse(response, err)
		return response
	}
	if prepared == nil {
		return executor.ordinary.Execute(ctx, filePool, monitor, digestFunction, request, executionStateUpdates)
	}
	outputHierarchy, err := NewOutputHierarchy(command)
	if err != nil {
		attachErrorToExecuteResponse(response, err)
		return response
	}
	if executor.workspace != nil && executor.compatibilityKey != prepared.CompatibilityKey {
		if err := executor.retireSessionWithTimeout(ctx); err != nil {
			attachErrorToExecuteResponse(response, err)
			return response
		}
	}
	if executor.workspace == nil {
		executor.workspace, err = NewPersistentWorkerWorkspace(executor.lifetimeContext, executor.buildDirectoryCreator, executor.filePool, prepared.WorkingDirectory, executor.inputRootCharacterDevices)
		if err != nil {
			attachErrorToExecuteResponse(response, err)
			return response
		}
		executor.compatibilityKey = prepared.CompatibilityKey
	}

	var lease *PersistentWorkerWorkspaceLease
	retire := true
	cleanupFailed := false
	defer func() {
		if ioError := executor.workspace.GetIOError(); ioError != nil {
			attachErrorToExecuteResponse(response, util.StatusWrap(ioError, "I/O error while running command"))
		}
		if lease != nil {
			if err := lease.Context.Err(); err != nil {
				attachErrorToExecuteResponse(response, status.FromContextError(err).Err())
			}
			lease.Release()
		}
		if retire || status.ErrorProto(response.Status) != nil || executor.workspace.context.Err() != nil {
			executor.retiring = true
			if !cleanupFailed {
				if err := executor.retireSessionWithTimeout(ctx); err != nil {
					attachErrorToExecuteResponse(response, err)
				}
			}
		}
	}()
	executionStateUpdates <- &remoteworker.CurrentState_Executing{
		ActionDigest: request.ActionDigest,
		ExecutionState: &remoteworker.CurrentState_Executing_FetchingInputs{
			FetchingInputs: &emptypb.Empty{},
		},
	}
	lease, err = executor.workspace.Prepare(ctx, inputRootDigest, outputHierarchy)
	if err != nil {
		attachErrorToExecuteResponse(response, err)
		return response
	}
	executionStateUpdates <- &remoteworker.CurrentState_Executing{
		ActionDigest: request.ActionDigest,
		ExecutionState: &remoteworker.CurrentState_Executing_Running{
			Running: &emptypb.Empty{},
		},
	}
	executionContext, cancelExecution := executor.clock.NewContextWithTimeout(lease.Context, action.Timeout.AsDuration())
	workResponse, runResponse, executionError := executor.executeInSession(executionContext, prepared, inputs, digestFunction)
	if executionError == nil && executionContext.Err() != nil {
		executionError = status.FromContextError(executionContext.Err()).Err()
	}
	cancelExecution()
	<-executionContext.Done()
	attachExecutionResult(executionContext, response, workResponse.GetExitCode(), runResponse.GetResourceUsage(), executionError, executor.workspace.GetIOError())
	if executionError != nil || executor.workspace.context.Err() != nil {
		cleanupContext, cancelCleanup := context.WithTimeout(context.WithoutCancel(ctx), executor.sessionCleanupTimeout)
		err := executor.stopSession(cleanupContext)
		cancelCleanup()
		if err != nil {
			cleanupFailed = true
			attachErrorToExecuteResponse(response, err)
			return response
		}
	} else {
		retire = false
	}
	executionStateUpdates <- &remoteworker.CurrentState_Executing{
		ActionDigest: request.ActionDigest,
		ExecutionState: &remoteworker.CurrentState_Executing_UploadingOutputs{
			UploadingOutputs: &emptypb.Empty{},
		},
	}
	if output := workResponse.GetOutput(); output != "" {
		generator := digestFunction.NewGenerator(int64(len(output)))
		generator.Write([]byte(output))
		outputDigest := generator.Sum()
		if err := executor.contentAddressableStorage.Put(ctx, outputDigest, buffer.NewValidatedBufferFromByteSlice([]byte(output))); err != nil {
			attachErrorToExecuteResponse(response, util.StatusWrap(err, "Failed to store persistent worker diagnostics"))
		} else {
			response.Result.StderrDigest = outputDigest.GetProto()
		}
	}
	uploadContext, cancelUpload := executor.clock.NewContextWithTimeout(ctx, executor.maximumWritableFileUploadDelay)
	defer cancelUpload()
	uploadOutputPathsAndLogs(ctx, lease.BuildDirectory, lease.InputRootDirectory, outputHierarchy, executor.contentAddressableStorage, digestFunction, uploadContext.Done(), response, executor.forceUploadTreesAndDirectories)
	return response
}

func getPersistentWorkerPlatform(action *remoteexecution.Action, command *remoteexecution.Command) (*remoteexecution.Platform, error) {
	platform := action.GetPlatform()
	if platform == nil {
		platform = command.GetPlatform()
	}
	properties := map[string]string{}
	for _, property := range platform.GetProperties() {
		if property.GetName() == "persistentWorkerKey" || property.GetName() == "persistentWorkerProtocol" {
			if _, ok := properties[property.Name]; ok {
				return nil, status.Errorf(codes.InvalidArgument, "Duplicate platform property %q", property.Name)
			}
			properties[property.Name] = property.Value
		}
	}
	if properties["persistentWorkerKey"] == "" {
		return nil, nil
	}

	// TODO: support JSON as well.
	if protocol := properties["persistentWorkerProtocol"]; protocol != "" && protocol != "proto" {
		return nil, nil
	}
	return platform, nil
}

func (executor *PersistentBuildExecutor) executeInSession(ctx context.Context, prepared *PersistentWorkerCommand, inputs map[string]*remoteexecution.FileNode, digestFunction digest.Function) (*worker_pb.WorkResponse, *runner_pb.ExecuteInPersistentWorkerResponse, error) {
	if err := ctx.Err(); err != nil {
		return nil, nil, status.FromContextError(err).Err()
	}
	if executor.sessionID == "" {
		directoryPath := executor.workspace.GetBuildDirectoryPath()
		executor.sessionCreationUncertain = true
		created, err := executor.runner.CreateSession(ctx, &runner_pb.CreateSessionRequest{
			Arguments:            prepared.Arguments,
			EnvironmentVariables: prepared.EnvironmentVariables,
			WorkingDirectory:     prepared.WorkingDirectory,
			InputRootDirectory:   directoryPath.Append(inputRootDirectoryComponent).GetUNIXString(),
			TemporaryDirectory:   directoryPath.Append(temporaryDirectoryComponent).GetUNIXString(),
			ProcessStderrPath:    directoryPath.Append(persistentWorkerSessionLogsComponent).Append(stderrComponent).GetUNIXString(),
			ServerLogsDirectory:  directoryPath.Append(persistentWorkerSessionLogsComponent).GetUNIXString(),
		})
		if created.GetSessionId() != "" {
			executor.sessionID = created.SessionId
			executor.sessionCreationUncertain = false
		}
		if err != nil {
			for _, detail := range status.Convert(err).Details() {
				if _, ok := detail.(*runner_pb.CreateSessionFailure); ok {
					executor.sessionCreationUncertain = false
				}
			}
			return nil, nil, err
		}
		if executor.sessionID == "" {
			return nil, nil, status.Error(codes.DataLoss, "Persistent runner returned no session ID")
		}
	}
	request, err := prepared.NewExecuteInPersistentWorkerRequest(ctx, executor.sessionID, func(ctx context.Context, name string) ([]byte, error) {
		fileDigest, err := digestFunction.NewDigestFromProto(inputs[name].GetDigest())
		if err != nil {
			return nil, err
		}
		return executor.contentAddressableStorage.Get(ctx, fileDigest).ToByteSlice(math.MaxInt)
	})
	if err != nil {
		return nil, nil, err
	}
	response, err := executor.runner.ExecuteInPersistentWorker(ctx, request)
	if err != nil {
		return nil, response, err
	}
	workResponse, err := DecodePersistentWorkerResponse(response)
	return workResponse, response, err
}

func (executor *PersistentBuildExecutor) stopSession(ctx context.Context) error {
	if executor.sessionCreationUncertain {
		return status.Error(codes.FailedPrecondition, "Cannot retire a session whose creation outcome is unknown")
	}
	if executor.sessionID != "" {
		if _, err := executor.runner.CloseSession(ctx, &runner_pb.SessionRequest{SessionId: executor.sessionID}); err != nil {
			return util.StatusWrap(err, "Failed to stop persistent worker session")
		}
		executor.sessionID = ""
	}
	return nil
}

func (executor *PersistentBuildExecutor) retireSession(ctx context.Context) error {
	if executor.workspace == nil {
		return nil
	}
	executor.retiring = true
	if err := executor.workspace.Close(ctx, executor.stopSession); err != nil {
		return util.StatusWrap(err, "Failed to retire persistent worker workspace")
	}
	executor.workspace = nil
	executor.retiring = false
	return nil
}

func (executor *PersistentBuildExecutor) retireSessionWithTimeout(ctx context.Context) error {
	cleanupContext, cancel := context.WithTimeout(context.WithoutCancel(ctx), executor.sessionCleanupTimeout)
	defer cancel()
	return executor.retireSession(cleanupContext)
}

// Close prevents further execution and retires the retained session before
// releasing its workspace. It rejects active operations and may be retried
// after cleanup failure. The caller must provide an independent cleanup
// context; the configured cleanup timeout also bounds this operation.
func (executor *PersistentBuildExecutor) Close(ctx context.Context) error {
	if !executor.lock.TryLock() {
		return status.Error(codes.FailedPrecondition, "Persistent build executor is busy")
	}
	defer executor.lock.Unlock()
	executor.closed = true
	cleanupContext, cancel := context.WithTimeout(ctx, executor.sessionCleanupTimeout)
	defer cancel()
	return executor.retireSession(cleanupContext)
}
