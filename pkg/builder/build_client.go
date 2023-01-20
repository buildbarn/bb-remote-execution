package builder

import (
	"context"
	"time"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-remote-execution/pkg/filesystem"
	"github.com/buildbarn/bb-remote-execution/pkg/proto/remoteworker"
	"github.com/buildbarn/bb-storage/pkg/clock"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/otel"
	"github.com/buildbarn/bb-storage/pkg/util"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
)

// BuildClient is a client for the Remote Worker protocol. It can send
// synchronization requests to a scheduler, informing it of the current
// state of the worker, while also obtaining requests for executing
// build actions.
type BuildClient struct {
	// Constant fields.
	scheduler           remoteworker.OperationQueueClient
	buildExecutor       BuildExecutor
	filePool            filesystem.FilePool
	clock               clock.Clock
	instanceNamePrefix  digest.InstanceName
	instanceNamePatcher digest.InstanceNamePatcher

	// Mutable fields that are always set.
	request               remoteworker.SynchronizeRequest
	inExecutingState      bool
	nextSynchronizationAt time.Time

	// Mutable fields that are only set when executing an action.
	executionCancellation func()
	executionUpdates      <-chan *remoteworker.CurrentState_Executing
}

// NewBuildClient creates a new BuildClient instance that is set to the
// initial state (i.e., being idle).
func NewBuildClient(scheduler remoteworker.OperationQueueClient, buildExecutor BuildExecutor, filePool filesystem.FilePool, clock clock.Clock, workerID map[string]string, instanceNamePrefix digest.InstanceName, platform *remoteexecution.Platform, sizeClass uint32) *BuildClient {
	return &BuildClient{
		scheduler:           scheduler,
		buildExecutor:       buildExecutor,
		filePool:            filePool,
		clock:               clock,
		instanceNamePrefix:  instanceNamePrefix,
		instanceNamePatcher: digest.NewInstanceNamePatcher(digest.EmptyInstanceName, instanceNamePrefix),

		request: remoteworker.SynchronizeRequest{
			WorkerId:           workerID,
			InstanceNamePrefix: instanceNamePrefix.String(),
			Platform:           platform,
			SizeClass:          sizeClass,
			CurrentState: &remoteworker.CurrentState{
				WorkerState: &remoteworker.CurrentState_Idle{
					Idle: &emptypb.Empty{},
				},
			},
		},
		nextSynchronizationAt: clock.Now(),
	}
}

func (bc *BuildClient) startExecution(executionRequest *remoteworker.DesiredState_Executing) error {
	instanceNameSuffix, err := digest.NewInstanceName(executionRequest.InstanceNameSuffix)
	if err != nil {
		return util.StatusWrapf(err, "Invalid instance name suffix %#v", executionRequest.InstanceNameSuffix)
	}
	digestFunction, err := bc.instanceNamePatcher.PatchInstanceName(instanceNameSuffix).
		GetDigestFunction(executionRequest.DigestFunction, len(executionRequest.ActionDigest.GetHash()))
	if err != nil {
		return err
	}

	bc.stopExecution()

	// Spawn the execution of the build action.
	var ctx context.Context
	ctx, bc.executionCancellation = context.WithCancel(
		otel.NewContextWithW3CTraceContext(
			context.Background(),
			executionRequest.W3CTraceContext))
	updates := make(chan *remoteworker.CurrentState_Executing, 10)
	bc.executionUpdates = updates
	go func() {
		executeResponse := bc.buildExecutor.Execute(
			ctx,
			bc.filePool,
			digestFunction,
			executionRequest,
			updates)
		updates <- &remoteworker.CurrentState_Executing{
			ActionDigest: executionRequest.ActionDigest,
			ExecutionState: &remoteworker.CurrentState_Executing_Completed{
				Completed: executeResponse,
			},
			// In case execution failed with a serious
			// error, request that the worker gets a brief
			// amount of idle time, so that we can do some
			// health checks prior to picking up more work.
			PreferBeingIdle: status.ErrorProto(executeResponse.Status) != nil,
		}
		close(updates)
	}()

	// Change state to indicate the build has started.
	bc.request.CurrentState.WorkerState = &remoteworker.CurrentState_Executing_{
		Executing: &remoteworker.CurrentState_Executing{
			ActionDigest: executionRequest.ActionDigest,
			ExecutionState: &remoteworker.CurrentState_Executing_Started{
				Started: &emptypb.Empty{},
			},
		},
	}
	bc.inExecutingState = true
	return nil
}

func (bc *BuildClient) stopExecution() {
	// Trigger cancellation of the existing build action and wait
	// for it to complete. Discard the results.
	if bc.executionCancellation != nil {
		bc.executionCancellation()
		for {
			if _, hasUpdate := <-bc.executionUpdates; !hasUpdate {
				break
			}
		}
		bc.executionCancellation = nil
		bc.executionUpdates = nil
	}

	bc.request.CurrentState.WorkerState = &remoteworker.CurrentState_Idle{
		Idle: &emptypb.Empty{},
	}
	bc.inExecutingState = false
}

func (bc *BuildClient) applyExecutionUpdate(update *remoteworker.CurrentState_Executing) {
	if update != nil {
		// New update received.
		bc.request.CurrentState.WorkerState = &remoteworker.CurrentState_Executing_{
			Executing: update,
		}
	} else {
		// Execution has finished. Clean up resources.
		bc.executionCancellation()
		bc.executionCancellation = nil
		bc.executionUpdates = nil
	}
}

func (bc *BuildClient) consumeExecutionUpdatesNonBlocking() {
	for {
		select {
		case update := <-bc.executionUpdates:
			bc.applyExecutionUpdate(update)
		default:
			// No more updates left.
			return
		}
	}
}

// Run a iteration of the Remote Worker client, by performing a single
// synchronization against the scheduler.
func (bc *BuildClient) Run() error {
	// If the scheduler isn't assuming we're executing any action
	// right now, perform some readiness checks. This ensures we
	// don't dequeue actions from the scheduler while unhealthy.
	if !bc.inExecutingState {
		if err := bc.buildExecutor.CheckReadiness(context.Background()); err != nil {
			return util.StatusWrap(err, "Worker failed readiness check")
		}
	}

	// When executing an action, see if there are any updates on the
	// execution state.
	if bc.executionCancellation != nil {
		timer, timerChannel := bc.clock.NewTimer(bc.nextSynchronizationAt.Sub(bc.clock.Now()))
		select {
		case <-timerChannel:
			// No meaningful updates. Send the last update
			// once again.
		case update := <-bc.executionUpdates:
			// One or more execution updates available. Send
			// a new update with the latest state.
			timer.Stop()
			bc.applyExecutionUpdate(update)
			bc.consumeExecutionUpdatesNonBlocking()
			bc.nextSynchronizationAt = bc.clock.Now()
		}
	}

	// Inform scheduler of current worker state, potentially
	// requesting new work.
	response, err := bc.scheduler.Synchronize(context.Background(), &bc.request)
	if err != nil {
		return util.StatusWrap(err, "Failed to synchronize with scheduler")
	}

	// Determine when we should contact the scheduler again in case
	// of no activity.
	nextSynchronizationAt := response.NextSynchronizationAt
	if err := nextSynchronizationAt.CheckValid(); err != nil {
		return util.StatusWrap(err, "Scheduler response contained invalid synchronization timestamp")
	}
	bc.nextSynchronizationAt = nextSynchronizationAt.AsTime()

	// Apply desired state changes provided by the scheduler.
	if desiredState := response.DesiredState; desiredState != nil {
		switch workerState := desiredState.WorkerState.(type) {
		case *remoteworker.DesiredState_Executing_:
			// Scheduler is requesting us to execute the
			// next action, maybe forcing us to to stop
			// execution of the current build action.
			if err := bc.startExecution(workerState.Executing); err != nil {
				return err
			}
		case *remoteworker.DesiredState_Idle:
			// Scheduler is forcing us to go back to idle.
			bc.stopExecution()
		default:
			return status.Error(codes.Internal, "Scheduler provided an unknown desired state")
		}
	}
	return nil
}
