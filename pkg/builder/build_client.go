package builder

import (
	"context"
	"log"
	"time"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-remote-execution/pkg/filesystem"
	"github.com/buildbarn/bb-remote-execution/pkg/proto/remoteworker"
	"github.com/buildbarn/bb-storage/pkg/clock"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/otel"
	"github.com/buildbarn/bb-storage/pkg/program"
	"github.com/buildbarn/bb-storage/pkg/random"
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
	request                         remoteworker.SynchronizeRequest
	schedulerMayThinkExecutingUntil *time.Time
	nextSynchronizationAt           time.Time

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
		GetDigestFunction(executionRequest.DigestFunction, 0)
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
			nil,
			digestFunction,
			executionRequest,
			updates)
		updates <- &remoteworker.CurrentState_Executing{
			ActionDigest: executionRequest.ActionDigest,
			ExecutionState: &remoteworker.CurrentState_Executing_Completed{
				Completed: executeResponse,
			},
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

// touchSchedulerMayThinkExecuting updates state on whether the
// scheduler may think the worker is currently executing an action. This
// is used to determine whether it is safe to terminate the worker
// gracefully.
func (bc *BuildClient) touchSchedulerMayThinkExecuting() {
	// Assume that if we've missed the desired synchronization time
	// provided by the scheduler by more than a minute, the
	// scheduler has purged our state.
	until := bc.nextSynchronizationAt.Add(time.Minute)
	bc.schedulerMayThinkExecutingUntil = &until
}

// Run a iteration of the Remote Worker client, by performing a single
// synchronization against the scheduler.
func (bc *BuildClient) Run(ctx context.Context) (bool, error) {
	// Allow the worker to terminate if the scheduler doesn't think
	// we're executing any action, or if we haven't been able to
	// successfully synchronize for a prolonged amount of time.
	if ctx.Err() != nil && (bc.schedulerMayThinkExecutingUntil == nil || bc.clock.Now().After(*bc.schedulerMayThinkExecutingUntil)) {
		return true, nil
	}

	// If the scheduler isn't assuming we're executing any action
	// right now, perform some readiness checks. This ensures we
	// don't dequeue actions from the scheduler while unhealthy.
	if bc.schedulerMayThinkExecutingUntil == nil {
		if err := bc.buildExecutor.CheckReadiness(ctx); err != nil {
			return true, util.StatusWrap(err, "Worker failed readiness check")
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
			// a new update with the latest state,
			// regardless of the next synchronization time
			// returned by the scheduler during the previous
			// synchronize call.
			timer.Stop()
			bc.applyExecutionUpdate(update)
			bc.consumeExecutionUpdatesNonBlocking()
			if now := bc.clock.Now(); bc.nextSynchronizationAt.After(now) {
				bc.nextSynchronizationAt = now
			}
		}
	}

	// Determine whether we should perform call to Synchronize with
	// prefer_being_able set to false (potentially blocking) or true
	// (non-blocking).
	currentStateIsExecuting := false
	switch workerState := bc.request.CurrentState.WorkerState.(type) {
	case *remoteworker.CurrentState_Idle:
		// Even though we are idle, the scheduler may think we
		// are executing. This means we were not able to perform
		// readiness checks. Forcefully switch to idle, so that
		// we can still do this before picking up more work.
		bc.request.PreferBeingIdle = bc.schedulerMayThinkExecutingUntil != nil
	case *remoteworker.CurrentState_Executing_:
		if updateCompleted, ok := workerState.Executing.ExecutionState.(*remoteworker.CurrentState_Executing_Completed); ok {
			// In case execution failed with a serious
			// error, request that the worker gets a brief
			// amount of idle time, so that we can do some
			// health checks prior to picking up more work.
			bc.request.PreferBeingIdle = status.ErrorProto(updateCompleted.Completed.Status) != nil
		} else {
			currentStateIsExecuting = true
			bc.request.PreferBeingIdle = false
		}
	default:
		panic("Unknown worker state")
	}

	// If we need to shut down, we should never be performing
	// blocking Synchronize() calls. We should still let the calls
	// go through, so that we can either finish the current action,
	// or properly ensure the scheduler thinks we're in the idle
	// state.
	if ctx.Err() != nil {
		bc.request.PreferBeingIdle = true
		ctx = context.Background()
	}

	// Inform scheduler of current worker state, potentially
	// requesting new work. If this fails, we might have lost an
	// execute request sent by the scheduler, so assume the
	// scheduler thinks we may be executing.
	response, err := bc.scheduler.Synchronize(ctx, &bc.request)
	if bc.schedulerMayThinkExecutingUntil == nil {
		bc.touchSchedulerMayThinkExecuting()
	}
	if err != nil {
		return false, util.StatusWrap(err, "Failed to synchronize with scheduler")
	}

	// Determine when we should contact the scheduler again in case
	// of no activity.
	nextSynchronizationAt := response.NextSynchronizationAt
	if err := nextSynchronizationAt.CheckValid(); err != nil {
		return false, util.StatusWrap(err, "Scheduler response contained invalid synchronization timestamp")
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
				return false, err
			}
			bc.touchSchedulerMayThinkExecuting()
			return false, nil
		case *remoteworker.DesiredState_Idle:
			// Scheduler is forcing us to go back to idle.
			bc.stopExecution()
			bc.schedulerMayThinkExecutingUntil = nil
			return true, nil
		default:
			return false, status.Error(codes.Internal, "Scheduler provided an unknown desired state")
		}
	}

	// Scheduler has instructed to continue as is.
	if currentStateIsExecuting {
		bc.touchSchedulerMayThinkExecuting()
		return false, nil
	}
	bc.schedulerMayThinkExecutingUntil = nil
	return true, nil
}

// LaunchWorkerThread launches a single routine that uses a build client
// to repeatedly synchronizes against the scheduler, requesting a task
// to execute.
func LaunchWorkerThread(group program.Group, buildClient *BuildClient, workerName string) {
	group.Go(func(ctx context.Context, siblingsGroup, dependenciesGroup program.Group) error {
		generator := random.NewFastSingleThreadedGenerator()
		for {
			terminationStartedBeforeRun := ctx.Err() != nil
			if mayTerminate, err := buildClient.Run(ctx); mayTerminate && ctx.Err() != nil {
				log.Printf("Worker %s: terminating", workerName)
				return nil
			} else if err != nil {
				log.Printf("Worker %s: %s", workerName, err)

				// In case of errors, sleep a random amount of
				// time. Allow the sleep to be skipped once when
				// termination is initiated, so that it happens
				// quickly.
				if d := random.Duration(generator, 5*time.Second); terminationStartedBeforeRun {
					time.Sleep(d)
				} else {
					t := time.NewTimer(d)
					select {
					case <-t.C:
					case <-ctx.Done():
						t.Stop()
					}
				}
			}
		}
	})
}
