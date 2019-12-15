package builder

import (
	"context"
	"net/url"
	"time"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-remote-execution/pkg/filesystem"
	"github.com/buildbarn/bb-remote-execution/pkg/proto/remoteworker"
	"github.com/buildbarn/bb-storage/pkg/clock"
	"github.com/buildbarn/bb-storage/pkg/util"
	"github.com/golang/protobuf/ptypes"
	"github.com/golang/protobuf/ptypes/empty"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// BuildClient is a client for the Remote Worker protocol. It can send
// synchronization requests to a scheduler, informing it of the current
// state of the worker, while also obtaining requests for executing
// build actions.
type BuildClient struct {
	// Constant fields.
	scheduler     remoteworker.OperationQueueClient
	buildExecutor BuildExecutor
	filePool      filesystem.FilePool
	clock         clock.Clock

	// Mutable fields that are always set.
	request               remoteworker.SynchronizeRequest
	nextSynchronizationAt time.Time

	// Mutable fields that are only set when executing an action.
	executionCancellation func()
	executionUpdates      <-chan *remoteworker.CurrentState_Executing
}

// NewBuildClient creates a new BuildClient instance that is set to the
// initial state (i.e., being idle).
func NewBuildClient(scheduler remoteworker.OperationQueueClient, buildExecutor BuildExecutor, filePool filesystem.FilePool, clock clock.Clock, browserURL *url.URL, workerID map[string]string, instanceName string, platform *remoteexecution.Platform) *BuildClient {
	return &BuildClient{
		scheduler:     scheduler,
		buildExecutor: buildExecutor,
		filePool:      filePool,
		clock:         clock,

		request: remoteworker.SynchronizeRequest{
			WorkerId:     workerID,
			InstanceName: instanceName,
			Platform:     platform,
			CurrentState: &remoteworker.CurrentState{
				WorkerState: &remoteworker.CurrentState_Idle{
					Idle: &empty.Empty{},
				},
			},
		},
		nextSynchronizationAt: clock.Now(),
	}
}

func (bc *BuildClient) startExecution(executionRequest *remoteworker.DesiredState_Executing) {
	bc.stopExecution()

	// Spawn the execution of the build action.
	var ctx context.Context
	ctx, bc.executionCancellation = context.WithCancel(context.Background())
	updates := make(chan *remoteworker.CurrentState_Executing, 10)
	bc.executionUpdates = updates
	go func() {
		updates <- &remoteworker.CurrentState_Executing{
			ActionDigest: executionRequest.ActionDigest,
			ExecutionState: &remoteworker.CurrentState_Executing_Completed{
				Completed: bc.buildExecutor.Execute(
					ctx,
					bc.filePool,
					bc.request.InstanceName,
					executionRequest,
					updates),
			},
		}
		close(updates)
	}()

	// Change state to indicate the build has started.
	bc.request.CurrentState.WorkerState = &remoteworker.CurrentState_Executing_{
		Executing: &remoteworker.CurrentState_Executing{
			ActionDigest: executionRequest.ActionDigest,
			ExecutionState: &remoteworker.CurrentState_Executing_Started{
				Started: &empty.Empty{},
			},
		},
	}
}

func (bc *BuildClient) stopExecution() {
	// Triger cancellation of the existing build action and wait for
	// it to complete. Discard the results.
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
		Idle: &empty.Empty{},
	}
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
	nextSynchronizationAt, err := ptypes.Timestamp(response.NextSynchronizationAt)
	if err != nil {
		return util.StatusWrap(err, "Scheduler response contained invalid synchronization timestamp")
	}
	bc.nextSynchronizationAt = nextSynchronizationAt

	// Apply desired state changes provided by the scheduler.
	if desiredState := response.DesiredState; desiredState != nil {
		switch workerState := desiredState.WorkerState.(type) {
		case *remoteworker.DesiredState_Executing_:
			// Scheduler is requesting us to execute the
			// next action, maybe forcing us to to stop
			// execution of the current build action.
			bc.startExecution(workerState.Executing)
		case *remoteworker.DesiredState_Idle:
			// Scheduler is forcing us to go back to idle.
			bc.stopExecution()
		default:
			return status.Error(codes.Internal, "Scheduler provided an unknown desired state")
		}
	}
	return nil
}
