package builder

import (
	"container/heap"
	"context"
	"encoding/json"
	"fmt"
	"math"
	"sort"
	"strconv"
	"sync"
	"time"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/bazelbuild/remote-apis/build/bazel/semver"
	"github.com/buildbarn/bb-remote-execution/pkg/builder/initialsizeclass"
	"github.com/buildbarn/bb-remote-execution/pkg/builder/platform"
	"github.com/buildbarn/bb-remote-execution/pkg/proto/buildqueuestate"
	"github.com/buildbarn/bb-remote-execution/pkg/proto/remoteworker"
	"github.com/buildbarn/bb-storage/pkg/blobstore"
	"github.com/buildbarn/bb-storage/pkg/builder"
	"github.com/buildbarn/bb-storage/pkg/clock"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/util"
	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"

	"google.golang.org/genproto/googleapis/longrunning"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

var (
	inMemoryBuildQueuePrometheusMetrics sync.Once

	inMemoryBuildQueueTasksScheduledTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "buildbarn",
			Subsystem: "builder",
			Name:      "in_memory_build_queue_tasks_scheduled_total",
			Help:      "Number of times tasks were scheduled, either by calling Execute() or through initial size class selection retries.",
		},
		[]string{"instance_name_prefix", "platform", "size_class", "assignment", "do_not_cache"})
	inMemoryBuildQueueTasksQueuedDurationSeconds = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "buildbarn",
			Subsystem: "builder",
			Name:      "in_memory_build_queue_tasks_queued_duration_seconds",
			Help:      "Time in seconds that tasks were queued before executing.",
			Buckets:   util.DecimalExponentialBuckets(-3, 6, 2),
		},
		[]string{"instance_name_prefix", "platform", "size_class"})
	inMemoryBuildQueueTasksExecutingDurationSeconds = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "buildbarn",
			Subsystem: "builder",
			Name:      "in_memory_build_queue_tasks_executing_duration_seconds",
			Help:      "Time in seconds that tasks were executing before completing.",
			Buckets:   util.DecimalExponentialBuckets(-3, 6, 2),
		},
		[]string{"instance_name_prefix", "platform", "size_class", "result", "grpc_code"})
	inMemoryBuildQueueTasksExecutingRetries = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "buildbarn",
			Subsystem: "builder",
			Name:      "in_memory_build_queue_tasks_executing_retries",
			Help:      "Number of times that tasks were retried before completing.",
			Buckets:   prometheus.LinearBuckets(0, 1, 11),
		},
		[]string{"instance_name_prefix", "platform", "size_class", "result", "grpc_code"})
	inMemoryBuildQueueTasksCompletedDurationSeconds = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "buildbarn",
			Subsystem: "builder",
			Name:      "in_memory_build_queue_tasks_completed_duration_seconds",
			Help:      "Time in seconds that tasks were completed before being removed.",
			Buckets:   util.DecimalExponentialBuckets(-3, 6, 2),
		},
		[]string{"instance_name_prefix", "platform", "size_class"})

	inMemoryBuildQueueWorkersCreatedTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "buildbarn",
			Subsystem: "builder",
			Name:      "in_memory_build_queue_workers_created_total",
			Help:      "Number of workers created by Synchronize().",
		},
		[]string{"instance_name_prefix", "platform", "size_class"})
	inMemoryBuildQueueWorkersRemovedTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "buildbarn",
			Subsystem: "builder",
			Name:      "in_memory_build_queue_workers_removed_total",
			Help:      "Number of workers removed due to expiration.",
		},
		[]string{"instance_name_prefix", "platform", "size_class", "state"})

	inMemoryBuildQueueWorkerInvocationStickinessOutcomesTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "buildbarn",
			Subsystem: "builder",
			Name:      "worker_invocation_stickiness_outcomes_total",
			Help:      "Outcomes of worker invocation stickiness, as configured through worker_invocation_stickiness_limit.",
		},
		[]string{"instance_name_prefix", "platform", "size_class", "outcome"})
)

// InMemoryBuildQueueConfiguration contains all the tunable settings of
// the InMemoryBuildQueue.
type InMemoryBuildQueueConfiguration struct {
	// ExecutionUpdateInterval specifies how frequently Execute()
	// and WaitExecution() should stream updates for a task to the
	// client.
	ExecutionUpdateInterval time.Duration

	// OperationWithNoWaitersTimeout specifies how long an operation
	// may remain registered without having a single client calling
	// Execute() or WaitExecution() on it.
	OperationWithNoWaitersTimeout time.Duration

	// PlatformQueueWithNoWorkersTimeout specifies how long a
	// platform may remain registered by InMemoryBuildQueue when no
	// Synchronize() calls are received for any workers.
	PlatformQueueWithNoWorkersTimeout time.Duration

	// BusyWorkerSynchronizationInterval specifies how frequently
	// busy workers should be instructed to report their state, even
	// if no changes to their running state occurred.
	BusyWorkerSynchronizationInterval time.Duration

	// GetIdleWorkerSynchronizationInterval returns the maximum
	// amount of time a synchronization performed by a worker
	// against the scheduler may block. Once this amount of time is
	// passed, the worker is instructed to resynchronize, as a form
	// of health checking.
	//
	// Implementations may add jitter to this value to ensure
	// synchronization requests get smeared out over time.
	GetIdleWorkerSynchronizationInterval func() time.Duration

	// WorkerTaskRetryCount specifies how many times a worker may
	// redundantly request that a single task is started. By
	// limiting this, we can prevent a single task from
	// crash-looping a worker indefinitely.
	WorkerTaskRetryCount int

	// WorkerWithNoSynchronizationsTimeout specifies how long a
	// worker may remain registered by InMemoryBuildQueue when no
	// Synchronize() calls are received.
	WorkerWithNoSynchronizationsTimeout time.Duration
}

// PlatformHooks contains a set of methods that can be provided for any
// platform managed by InMemoryBuildQueue to analyze incoming execution
// requests. For each of these requests, it can compute what the
// invocation ID is and on which size class the action needs to be
// executed.
type PlatformHooks interface {
	InvocationIDExtractor
	initialsizeclass.Analyzer
}

// InMemoryBuildQueue implements a BuildQueue that can distribute
// requests through the Remote Worker protocol to worker processes. All
// of the state of the build queue (i.e., list of queued execution
// requests and list of workers) is kept in memory.
type InMemoryBuildQueue struct {
	contentAddressableStorage           blobstore.BlobAccess
	clock                               clock.Clock
	uuidGenerator                       util.UUIDGenerator
	configuration                       *InMemoryBuildQueueConfiguration
	platformQueueAbsenceHardFailureTime time.Time
	maximumMessageSizeBytes             int
	platformHooksTrie                   *platform.Trie
	platformHooks                       []PlatformHooks

	lock               sync.Mutex
	platformQueuesTrie *platform.Trie
	platformQueues     []*platformQueue
	sizeClassQueues    map[sizeClassKey]*sizeClassQueue

	// Bookkeeping for WaitExecution(). This call permits us to
	// re-attach to operations by name. It also allows us to obtain
	// results for historical actions, up to a certain degree.
	operationsNameMap map[string]*operation

	// Map of each task that does not have DoNotCache set by digest.
	// This map is used to deduplicate concurrent requests for the
	// same action.
	inFlightDeduplicationMap map[digest.Digest]*task

	// Time value that is updated during every mutation of build
	// queue state. This reduces the number of clock accesses, while
	// also making it easier to test this code.
	now time.Time

	// Binary heap containing closures that purge stale workers,
	// platform queues and operations.
	cleanupQueue cleanupQueue
}

// NewInMemoryBuildQueue creates a new InMemoryBuildQueue that is in the
// initial state. It does not have any queues, workers or queued
// execution requests. All of these are created by sending it RPCs.
func NewInMemoryBuildQueue(contentAddressableStorage blobstore.BlobAccess, clock clock.Clock, uuidGenerator util.UUIDGenerator, configuration *InMemoryBuildQueueConfiguration, maximumMessageSizeBytes int, defaultPlatformHooks PlatformHooks) *InMemoryBuildQueue {
	inMemoryBuildQueuePrometheusMetrics.Do(func() {
		prometheus.MustRegister(inMemoryBuildQueueTasksScheduledTotal)
		prometheus.MustRegister(inMemoryBuildQueueTasksQueuedDurationSeconds)
		prometheus.MustRegister(inMemoryBuildQueueTasksExecutingDurationSeconds)
		prometheus.MustRegister(inMemoryBuildQueueTasksExecutingRetries)
		prometheus.MustRegister(inMemoryBuildQueueTasksCompletedDurationSeconds)

		prometheus.MustRegister(inMemoryBuildQueueWorkersCreatedTotal)
		prometheus.MustRegister(inMemoryBuildQueueWorkersRemovedTotal)

		prometheus.MustRegister(inMemoryBuildQueueWorkerInvocationStickinessOutcomesTotal)
	})

	return &InMemoryBuildQueue{
		contentAddressableStorage:           contentAddressableStorage,
		clock:                               clock,
		uuidGenerator:                       uuidGenerator,
		configuration:                       configuration,
		platformQueueAbsenceHardFailureTime: clock.Now().Add(configuration.PlatformQueueWithNoWorkersTimeout),
		maximumMessageSizeBytes:             maximumMessageSizeBytes,
		platformHooksTrie:                   platform.NewTrie(),
		platformHooks:                       []PlatformHooks{defaultPlatformHooks},
		platformQueuesTrie:                  platform.NewTrie(),
		sizeClassQueues:                     map[sizeClassKey]*sizeClassQueue{},
		operationsNameMap:                   map[string]*operation{},
		inFlightDeduplicationMap:            map[digest.Digest]*task{},
	}
}

var (
	_ builder.BuildQueue                    = (*InMemoryBuildQueue)(nil)
	_ remoteworker.OperationQueueServer     = (*InMemoryBuildQueue)(nil)
	_ buildqueuestate.BuildQueueStateServer = (*InMemoryBuildQueue)(nil)
)

// RegisterPredeclaredPlatformQueue adds a platform queue to
// InMemoryBuildQueue that remains present, regardless of whether
// workers appear.
//
// The main purpose of this method is to create platform queues that are
// capable of using multiple size classes, as a maximum size class and
// initialsizeclass.Analyzer can be provided for specifying how
// operations are assigned to size classes.
func (bq *InMemoryBuildQueue) RegisterPredeclaredPlatformQueue(instanceNamePrefix digest.InstanceName, platformMessage *remoteexecution.Platform, workerInvocationStickinessLimit time.Duration, maximumQueuedBackgroundLearningOperations int, backgroundLearningOperationPriority int32, maximumSizeClass uint32, platformHooks PlatformHooks) error {
	platformKey, err := platform.NewKey(instanceNamePrefix, platformMessage)
	if err != nil {
		return err
	}

	bq.enter(bq.clock.Now())
	defer bq.leave()

	if bq.platformQueuesTrie.ContainsExact(platformKey) {
		return status.Error(codes.AlreadyExists, "A queue with the same instance name prefix or platform already exists")
	}

	pq := bq.addPlatformQueue(platformKey, workerInvocationStickinessLimit, maximumQueuedBackgroundLearningOperations, backgroundLearningOperationPriority)
	pq.addSizeClassQueue(bq, maximumSizeClass, false)
	bq.platformHooksTrie.Set(platformKey, len(bq.platformHooks)-1)
	bq.platformHooks = append(bq.platformHooks, platformHooks)
	return nil
}

// GetCapabilities returns the Remote Execution protocol capabilities
// that this service supports.
func (bq *InMemoryBuildQueue) GetCapabilities(ctx context.Context, in *remoteexecution.GetCapabilitiesRequest) (*remoteexecution.ServerCapabilities, error) {
	return &remoteexecution.ServerCapabilities{
		CacheCapabilities: &remoteexecution.CacheCapabilities{
			DigestFunction: digest.SupportedDigestFunctions,
			ActionCacheUpdateCapabilities: &remoteexecution.ActionCacheUpdateCapabilities{
				UpdateEnabled: false,
			},
			// CachePriorityCapabilities: Priorities not supported.
			// MaxBatchTotalSize: Not used by Bazel yet.
			SymlinkAbsolutePathStrategy: remoteexecution.SymlinkAbsolutePathStrategy_ALLOWED,
		},
		ExecutionCapabilities: &remoteexecution.ExecutionCapabilities{
			DigestFunction: remoteexecution.DigestFunction_SHA256,
			ExecEnabled:    true,
			ExecutionPriorityCapabilities: &remoteexecution.PriorityCapabilities{
				Priorities: []*remoteexecution.PriorityCapabilities_PriorityRange{
					{MinPriority: math.MinInt32, MaxPriority: math.MaxInt32},
				},
			},
		},
		// TODO: DeprecatedApiVersion.
		LowApiVersion:  &semver.SemVer{Major: 2},
		HighApiVersion: &semver.SemVer{Major: 2},
	}, nil
}

// getRequestMetadata extracts the RequestMetadata message stored in the
// gRPC request headers. This message contains the invocation ID that is
// used to group incoming requests by client, so that tasks can be
// scheduled across workers fairly.
func getRequestMetadata(ctx context.Context) *remoteexecution.RequestMetadata {
	if md, ok := metadata.FromIncomingContext(ctx); ok {
		for _, requestMetadataBin := range md.Get("build.bazel.remote.execution.v2.requestmetadata-bin") {
			var requestMetadata remoteexecution.RequestMetadata
			if err := proto.Unmarshal([]byte(requestMetadataBin), &requestMetadata); err == nil {
				return &requestMetadata
			}
		}
	}
	return nil
}

// Execute an action by scheduling it in the build queue. This call
// blocks until the action is completed.
func (bq *InMemoryBuildQueue) Execute(in *remoteexecution.ExecuteRequest, out remoteexecution.Execution_ExecuteServer) error {
	// Fetch the action corresponding to the execute request.
	// Ideally, a scheduler is oblivious of what this message looks
	// like, if it weren't for the fact that DoNotCache and Platform
	// are used for scheduling decisions.
	//
	// To prevent loading this messages from the Content Addressable
	// Storage (CAS) multiple times, the scheduler holds on to it
	// and passes it on to the workers.
	ctx := out.Context()
	instanceName, err := digest.NewInstanceName(in.InstanceName)
	if err != nil {
		return util.StatusWrapf(err, "Invalid instance name %#v", in.InstanceName)
	}
	actionDigest, err := instanceName.NewDigestFromProto(in.ActionDigest)
	if err != nil {
		return util.StatusWrap(err, "Failed to extract digest for action")
	}
	actionMessage, err := bq.contentAddressableStorage.Get(ctx, actionDigest).ToProto(&remoteexecution.Action{}, bq.maximumMessageSizeBytes)
	if err != nil {
		return util.StatusWrap(err, "Failed to obtain action")
	}
	action := actionMessage.(*remoteexecution.Action)
	platformKey, err := platform.NewKey(instanceName, action.Platform)
	if err != nil {
		return err
	}

	// Forward the client-provided request metadata, so that the
	// worker logs it.
	requestMetadata := getRequestMetadata(ctx)
	targetID := requestMetadata.GetTargetId()
	var auxiliaryMetadata []*anypb.Any
	if requestMetadata != nil {
		requestMetadataAny, err := anypb.New(requestMetadata)
		if err != nil {
			return util.StatusWrapWithCode(err, codes.InvalidArgument, "Failed to marshal request metadata")
		}
		auxiliaryMetadata = []*anypb.Any{requestMetadataAny}
	}
	w3cTraceContext := util.W3CTraceContextFromContext(ctx)

	// TODO: Remove this code once all clients support REv2.2.
	if action.Platform == nil || targetID == "" {
		commandDigest, err := instanceName.NewDigestFromProto(action.CommandDigest)
		if err != nil {
			return util.StatusWrap(err, "Failed to extract digest for command")
		}
		commandMessage, err := bq.contentAddressableStorage.Get(ctx, commandDigest).ToProto(&remoteexecution.Command{}, bq.maximumMessageSizeBytes)
		if err != nil {
			return util.StatusWrap(err, "Failed to obtain command")
		}
		command := commandMessage.(*remoteexecution.Command)

		// REv2.1 and older don't provide platform properties as
		// part of the Action message.
		if action.Platform == nil {
			platformKey, err = platform.NewKey(instanceName, command.Platform)
			if err != nil {
				return err
			}
		}

		// REv2.1 RequestMetadata doesn't include the target_id
		// field. Provide the argv[0] instead, so that we gain
		// some insight in what this action does.
		if targetID == "" && len(command.Arguments) > 0 {
			targetID = command.Arguments[0]
		}
	}

	// Create an invocation key. Operations are scheduled by
	// grouping them by invocation, so that scheduling is fair.
	platformHooks := bq.platformHooks[bq.platformHooksTrie.GetLongestPrefix(platformKey)+1]
	invocationID, err := platformHooks.ExtractInvocationID(ctx, instanceName, action, requestMetadata)
	if err != nil {
		return util.StatusWrap(err, "Failed to extract invocation ID from request")
	}
	invocationKey, err := newInvocationKey(invocationID)
	if err != nil {
		return err
	}

	// Analyze the action, so that we can later determine on which
	// size class this action needs to run.
	initialSizeClassSelector, err := platformHooks.Analyze(ctx, actionDigest.GetDigestFunction(), action)
	if err != nil {
		return util.StatusWrap(err, "Failed to analyze initial size class of action")
	}

	bq.enter(bq.clock.Now())
	defer bq.leave()

	if t, ok := bq.inFlightDeduplicationMap[actionDigest]; ok {
		// A task for the same action digest already exists
		// against which we may deduplicate. No need to create a
		// task.
		initialSizeClassSelector.Abandoned()
		scq := t.getSizeClassQueue()
		i := scq.getOrCreateInvocation(invocationKey)
		if o, ok := t.operations[i]; ok {
			// Task is already associated with the current
			// invocation. Simply wait on the operation that
			// already exists.
			return o.waitExecution(bq, out)
		}

		// Create an additional operation for this task.
		o := t.newOperation(bq, in.ExecutionPolicy.GetPriority(), i, false)
		switch t.getStage() {
		case remoteexecution.ExecutionStage_QUEUED:
			// The request has been deduplicated against a
			// task that is still queued.
			o.enqueue()
		case remoteexecution.ExecutionStage_EXECUTING:
			// The request has been deduplicated against a
			// task that is already in the executing stage.
			i.incrementExecutingWorkersCount()
		default:
			panic("Task in unexpected stage")
		}
		return o.waitExecution(bq, out)
	}

	// We need to create a new task. For that we first need to
	// obtain the size class queue in which we're going to place it.
	platformQueueIndex := bq.platformQueuesTrie.GetLongestPrefix(platformKey)
	if platformQueueIndex < 0 {
		code := codes.FailedPrecondition
		if bq.now.Before(bq.platformQueueAbsenceHardFailureTime) {
			// The scheduler process started not too long
			// ago. It may be the case that clients ended up
			// connecting to the scheduler before workers
			// got a chance to synchronize.
			//
			// Prevent builds from failing unnecessarily by
			// providing a brief window of time where
			// soft errors are returned to the client,
			// giving workers time to reconnect.
			code = codes.Unavailable
		}
		initialSizeClassSelector.Abandoned()
		return status.Errorf(code, "No workers exist for instance name prefix %#v platform %s", platformKey.GetInstanceNamePrefix().String(), platformKey.GetPlatformString())
	}
	pq := bq.platformQueues[platformQueueIndex]
	sizeClassIndex, timeout, initialSizeClassLearner := initialSizeClassSelector.Select(pq.sizeClasses)
	scq := pq.sizeClassQueues[sizeClassIndex]

	// Create the task.
	actionWithCustomTimeout := *action
	actionWithCustomTimeout.Timeout = durationpb.New(timeout)
	t := &task{
		operations:   map[*invocation]*operation{},
		actionDigest: actionDigest,
		desiredState: remoteworker.DesiredState_Executing{
			ActionDigest:       in.ActionDigest,
			Action:             &actionWithCustomTimeout,
			QueuedTimestamp:    bq.getCurrentTime(),
			AuxiliaryMetadata:  auxiliaryMetadata,
			InstanceNameSuffix: pq.instanceNamePatcher.PatchInstanceName(instanceName).String(),
			W3CTraceContext:    w3cTraceContext,
		},
		targetID:                targetID,
		initialSizeClassLearner: initialSizeClassLearner,
		stageChangeWakeup:       make(chan struct{}),
	}
	if !action.DoNotCache {
		bq.inFlightDeduplicationMap[actionDigest] = t
	}
	i := scq.getOrCreateInvocation(invocationKey)
	o := t.newOperation(bq, in.ExecutionPolicy.GetPriority(), i, false)
	t.schedule(bq)
	return o.waitExecution(bq, out)
}

// WaitExecution attaches to an existing operation that was created by
// Execute(). This call can be used by the client to reattach to an
// operation in case of network failure.
func (bq *InMemoryBuildQueue) WaitExecution(in *remoteexecution.WaitExecutionRequest, out remoteexecution.Execution_WaitExecutionServer) error {
	bq.enter(bq.clock.Now())
	defer bq.leave()

	o, ok := bq.operationsNameMap[in.Name]
	if !ok {
		return status.Errorf(codes.NotFound, "Operation with name %#v not found", in.Name)
	}
	return o.waitExecution(bq, out)
}

// Synchronize the state of a worker with the scheduler. This call is
// used by a worker to report the completion of an operation and to
// request more work.
func (bq *InMemoryBuildQueue) Synchronize(ctx context.Context, request *remoteworker.SynchronizeRequest) (*remoteworker.SynchronizeResponse, error) {
	instanceNamePrefix, err := digest.NewInstanceName(request.InstanceNamePrefix)
	if err != nil {
		return nil, util.StatusWrapf(err, "Invalid instance name %#v", request.InstanceNamePrefix)
	}
	platformKey, err := platform.NewKey(instanceNamePrefix, request.Platform)
	if err != nil {
		return nil, err
	}
	workerKey := newWorkerKey(request.WorkerId)

	bq.enter(bq.clock.Now())
	defer bq.leave()

	sizeClassKey := sizeClassKey{
		platformKey: platformKey,
		sizeClass:   request.SizeClass,
	}
	var pq *platformQueue
	scq, ok := bq.sizeClassQueues[sizeClassKey]
	if ok {
		// Found an existing size class queue. Prevent the
		// platform queue from being garbage collected, as it
		// will now have an active worker.
		pq = scq.platformQueue
		if scq.cleanupKey.isActive() {
			bq.cleanupQueue.remove(scq.cleanupKey)
		}
	} else {
		if platformQueueIndex := bq.platformQueuesTrie.GetExact(platformKey); platformQueueIndex >= 0 {
			// Worker for this type of instance/platform pair has
			// been observed before, but not for this size class.
			// Create a new size class queue.
			//
			// Only allow this to take place if the platform
			// queue is predeclared, as the build results
			// are non-deterministic otherwise.
			pq = bq.platformQueues[platformQueueIndex]
			if maximumSizeClassQueue := pq.sizeClassQueues[len(pq.sizeClassQueues)-1]; maximumSizeClassQueue.mayBeRemoved {
				return nil, status.Error(codes.InvalidArgument, "Cannot add multiple size classes to a platform queue that is not predeclared")
			} else if maximumSizeClass := pq.sizeClasses[len(pq.sizeClasses)-1]; request.SizeClass > maximumSizeClass {
				return nil, status.Errorf(codes.InvalidArgument, "Worker provided size class %d, which exceeds the predeclared maximum of %d", request.SizeClass, maximumSizeClass)
			} else if maximumSizeClass > 0 && request.SizeClass < 1 {
				return nil, status.Error(codes.InvalidArgument, "Worker did not provide a size class, even though this platform queue uses them")
			}
		} else {
			// Worker for this type of instance/platform
			// pair has not been observed before. Create a
			// new platform queue containing a single size
			// class queue.
			pq = bq.addPlatformQueue(platformKey, 0, 0, 0)
		}
		scq = pq.addSizeClassQueue(bq, request.SizeClass, true)
	}

	w, ok := scq.workers[workerKey]
	if ok {
		// Prevent the worker from being garbage collected while
		// synchronization is happening.
		if !w.cleanupKey.isActive() {
			return nil, status.Error(codes.ResourceExhausted, "Worker is already synchronizing with the scheduler")
		}
		bq.cleanupQueue.remove(w.cleanupKey)
	} else {
		// First time we're seeing this worker.
		w = &worker{}
		scq.workers[workerKey] = w
		scq.workersCreatedTotal.Inc()
	}

	// Install cleanup handlers to ensure stale workers and queues
	// are purged after sufficient amount of time.
	defer func() {
		removalTime := bq.now.Add(bq.configuration.WorkerWithNoSynchronizationsTimeout)
		bq.cleanupQueue.add(&w.cleanupKey, removalTime, func() {
			scq.removeStaleWorker(bq, workerKey, removalTime)
		})
	}()

	// Process the current state of the worker to determine what it
	// should be doing next.
	currentState := request.CurrentState
	if currentState == nil {
		return nil, status.Error(codes.InvalidArgument, "Worker did not provide its current state")
	}
	switch workerState := currentState.WorkerState.(type) {
	case *remoteworker.CurrentState_Idle:
		return w.getCurrentOrNextTask(ctx, bq, scq, request.WorkerId, false)
	case *remoteworker.CurrentState_Executing_:
		executing := workerState.Executing
		if executing.ActionDigest == nil {
			return nil, status.Error(codes.InvalidArgument, "Worker is executing, but provided no action digest")
		}
		switch executionState := executing.ExecutionState.(type) {
		case *remoteworker.CurrentState_Executing_Completed:
			return w.completeTask(ctx, bq, scq, request.WorkerId, executing.ActionDigest, executionState.Completed, executing.PreferBeingIdle)
		default:
			return w.updateTask(bq, scq, request.WorkerId, executing.ActionDigest, executing.PreferBeingIdle)
		}
	default:
		return nil, status.Error(codes.InvalidArgument, "Worker provided an unknown current state")
	}
}

// ListPlatformQueues returns a list of all platform queues currently
// managed by the scheduler.
func (bq *InMemoryBuildQueue) ListPlatformQueues(ctx context.Context, request *emptypb.Empty) (*buildqueuestate.ListPlatformQueuesResponse, error) {
	bq.enter(bq.clock.Now())
	defer bq.leave()

	// Obtain platform queue IDs in sorted order.
	platformQueueList := append(platformQueueList(nil), bq.platformQueues...)
	sort.Sort(platformQueueList)

	// Extract status.
	platformQueues := make([]*buildqueuestate.PlatformQueueState, 0, len(bq.platformQueues))
	for _, pq := range platformQueueList {
		sizeClassQueues := make([]*buildqueuestate.SizeClassQueueState, 0, len(pq.sizeClassQueues))
		for i, scq := range pq.sizeClassQueues {
			executingWorkersCount := uint32(0)
			for _, w := range scq.workers {
				if w.currentTask != nil {
					executingWorkersCount++
				}
			}
			activeInvocationsCount := uint32(0)
			for _, i := range scq.invocations {
				if i.isActive() {
					activeInvocationsCount++
				}
			}
			sizeClassQueues = append(sizeClassQueues, &buildqueuestate.SizeClassQueueState{
				SizeClass:              pq.sizeClasses[i],
				Timeout:                bq.cleanupQueue.getTimestamp(scq.cleanupKey),
				InvocationsCount:       uint32(len(scq.invocations)),
				QueuedInvocationsCount: uint32(scq.queuedInvocations.Len()),
				ActiveInvocationsCount: uint32(activeInvocationsCount),
				WorkersCount:           uint32(len(scq.workers)),
				ExecutingWorkersCount:  executingWorkersCount,
				DrainsCount:            uint32(len(scq.drains)),
			})
		}
		platformQueues = append(platformQueues, &buildqueuestate.PlatformQueueState{
			Name:            pq.platformKey.GetPlatformQueueName(),
			SizeClassQueues: sizeClassQueues,
		})
	}
	return &buildqueuestate.ListPlatformQueuesResponse{
		PlatformQueues: platformQueues,
	}, nil
}

// GetOperation returns detailed information about a single operation
// identified by name.
func (bq *InMemoryBuildQueue) GetOperation(ctx context.Context, request *buildqueuestate.GetOperationRequest) (*buildqueuestate.GetOperationResponse, error) {
	bq.enter(bq.clock.Now())
	defer bq.leave()

	o, ok := bq.operationsNameMap[request.OperationName]
	if !ok {
		return nil, status.Errorf(codes.NotFound, "Operation %#v not found", request.OperationName)
	}
	s := o.getOperationState(bq)
	s.Name = ""
	return &buildqueuestate.GetOperationResponse{
		Operation: s,
	}, nil
}

// getPaginationInfo uses binary searching to determine which
// information should be returned by InMemoryBuildQueue's List*()
// operations.
func getPaginationInfo(n int, pageSize uint32, f func(int) bool) (*buildqueuestate.PaginationInfo, int) {
	startIndex := uint32(sort.Search(n, f))
	endIndex := uint32(n)
	if endIndex-startIndex > pageSize {
		endIndex = startIndex + pageSize
	}
	return &buildqueuestate.PaginationInfo{
		StartIndex:   startIndex,
		TotalEntries: uint32(n),
	}, int(endIndex)
}

// KillOperation requests that an operation that is currently QUEUED or
// EXECUTING is moved the COMPLETED stage immediately. The next time any
// worker associated with the operation contacts the scheduler, it is
// requested to stop executing the operation.
func (bq *InMemoryBuildQueue) KillOperation(ctx context.Context, request *buildqueuestate.KillOperationRequest) (*emptypb.Empty, error) {
	bq.enter(bq.clock.Now())
	defer bq.leave()

	o, ok := bq.operationsNameMap[request.OperationName]
	if !ok {
		return nil, status.Errorf(codes.NotFound, "Operation %#v not found", request.OperationName)
	}
	o.task.complete(bq, &remoteexecution.ExecuteResponse{
		Status: status.New(codes.Unavailable, "Operation was killed administratively").Proto(),
	}, false)
	return &emptypb.Empty{}, nil
}

// ListOperations returns detailed information about all of the
// operations tracked by the InMemoryBuildQueue.
func (bq *InMemoryBuildQueue) ListOperations(ctx context.Context, request *buildqueuestate.ListOperationsRequest) (*buildqueuestate.ListOperationsResponse, error) {
	bq.enter(bq.clock.Now())
	defer bq.leave()

	// Obtain operation names in sorted order.
	nameList := make([]string, 0, len(bq.operationsNameMap))
	for name := range bq.operationsNameMap {
		nameList = append(nameList, name)
	}
	sort.Strings(nameList)
	paginationInfo, endIndex := getPaginationInfo(len(nameList), request.PageSize, func(i int) bool {
		return request.StartAfter == nil || nameList[i] > request.StartAfter.OperationName
	})

	// Extract status.
	nameListRegion := nameList[paginationInfo.StartIndex:endIndex]
	operations := make([]*buildqueuestate.OperationState, 0, len(nameListRegion))
	for _, name := range nameListRegion {
		o := bq.operationsNameMap[name]
		operations = append(operations, o.getOperationState(bq))
	}
	return &buildqueuestate.ListOperationsResponse{
		Operations:     operations,
		PaginationInfo: paginationInfo,
	}, nil
}

// ListInvocations returns properties of all client invocations for
// which one or more operations are either queued or executing within a
// given platform queue.
//
// When justQueuedInvocations is false, entries for invocations are
// returned even if they have no queued operations; only ones that are
// being executed right now. Entries will be sorted by invocation ID.
//
// When justQueuedInvocations is true, entries for invocations are
// returned only if they have queued operations. Entries will be sorted
// by priority at which operations are scheduled.
func (bq *InMemoryBuildQueue) ListInvocations(ctx context.Context, request *buildqueuestate.ListInvocationsRequest) (*buildqueuestate.ListInvocationsResponse, error) {
	sizeClassKey, err := newSizeClassKeyFromName(request.SizeClassQueueName)
	if err != nil {
		return nil, err
	}

	bq.enter(bq.clock.Now())
	defer bq.leave()

	scq, ok := bq.sizeClassQueues[sizeClassKey]
	if !ok {
		return nil, status.Error(codes.NotFound, "No workers for this instance name, platform and size class exist")
	}

	switch request.Filter {
	case buildqueuestate.ListInvocationsRequest_ALL, buildqueuestate.ListInvocationsRequest_ACTIVE:
		// Return all or active invocations in alphabetic order.
		keyList := make([]string, 0, len(scq.invocations))
		for invocationKey, i := range scq.invocations {
			if request.Filter == buildqueuestate.ListInvocationsRequest_ALL || i.isActive() {
				keyList = append(keyList, string(invocationKey))
			}
		}
		sort.Strings(keyList)

		invocations := make([]*buildqueuestate.InvocationState, 0, len(scq.invocations))
		for _, key := range keyList {
			invocationKey := invocationKey(key)
			i := scq.invocations[invocationKey]
			invocations = append(invocations, i.getInvocationState(bq))
		}
		return &buildqueuestate.ListInvocationsResponse{
			Invocations: invocations,
		}, nil
	case buildqueuestate.ListInvocationsRequest_QUEUED:
		// Return invocations with one or more queued
		// operations, sorted by scheduling order.
		invocations := make([]*buildqueuestate.InvocationState, 0, scq.queuedInvocations.Len())
		sort.Sort(&scq.queuedInvocations)
		for _, i := range scq.queuedInvocations {
			invocations = append(invocations, i.getInvocationState(bq))
		}
		return &buildqueuestate.ListInvocationsResponse{
			Invocations: invocations,
		}, nil
	default:
		return nil, status.Error(codes.InvalidArgument, "Unknown filter provided")
	}
}

// ListQueuedOperations returns properties of all queued operations
// contained for a given invocation within a platform queue.
func (bq *InMemoryBuildQueue) ListQueuedOperations(ctx context.Context, request *buildqueuestate.ListQueuedOperationsRequest) (*buildqueuestate.ListQueuedOperationsResponse, error) {
	sizeClassKey, err := newSizeClassKeyFromName(request.SizeClassQueueName)
	if err != nil {
		return nil, err
	}
	invocationKey, err := newInvocationKey(request.InvocationId)
	if err != nil {
		return nil, err
	}

	bq.enter(bq.clock.Now())
	defer bq.leave()

	scq, ok := bq.sizeClassQueues[sizeClassKey]
	if !ok {
		return nil, status.Error(codes.NotFound, "No workers for this instance name, platform and size class exist")
	}
	i, ok := scq.invocations[invocationKey]
	if !ok {
		return nil, status.Error(codes.NotFound, "No operations for this invocation ID exist")
	}

	startAfter := request.StartAfter
	var startAfterQueuedTimestamp time.Time
	if startAfter != nil {
		if err := startAfter.QueuedTimestamp.CheckValid(); err != nil {
			return nil, util.StatusWrapWithCode(err, codes.InvalidArgument, "Invalid queued timestamp")
		}
		startAfterQueuedTimestamp = startAfter.QueuedTimestamp.AsTime()
	}

	// As every sorted list is also a valid binary heap, simply sort
	// the queued operations list prior to emitting it.
	sort.Sort(i.queuedOperations)
	paginationInfo, endIndex := getPaginationInfo(i.queuedOperations.Len(), request.PageSize, func(idx int) bool {
		o := i.queuedOperations[idx]
		if startAfter == nil || o.priority > startAfter.Priority {
			return true
		}
		if o.priority < startAfter.Priority {
			return false
		}
		return o.task.desiredState.QueuedTimestamp.AsTime().After(startAfterQueuedTimestamp)
	})

	queuedOperationsRegion := i.queuedOperations[paginationInfo.StartIndex:endIndex]
	queuedOperations := make([]*buildqueuestate.OperationState, 0, queuedOperationsRegion.Len())
	for _, o := range queuedOperationsRegion {
		s := o.getOperationState(bq)
		s.SizeClassQueueName = nil
		s.InvocationId = nil
		queuedOperations = append(queuedOperations, s)
	}
	return &buildqueuestate.ListQueuedOperationsResponse{
		QueuedOperations: queuedOperations,
		PaginationInfo:   paginationInfo,
	}, nil
}

// ListWorkers returns basic properties of all workers for a given
// platform queue.
func (bq *InMemoryBuildQueue) ListWorkers(ctx context.Context, request *buildqueuestate.ListWorkersRequest) (*buildqueuestate.ListWorkersResponse, error) {
	sizeClassKey, err := newSizeClassKeyFromName(request.SizeClassQueueName)
	if err != nil {
		return nil, err
	}
	var startAfterWorkerKey *string
	if startAfter := request.StartAfter; startAfter != nil {
		workerKey := string(newWorkerKey(startAfter.WorkerId))
		startAfterWorkerKey = &workerKey
	}

	bq.enter(bq.clock.Now())
	defer bq.leave()

	scq, ok := bq.sizeClassQueues[sizeClassKey]
	if !ok {
		return nil, status.Error(codes.NotFound, "No workers for this instance name, platform and size class exist")
	}

	// Obtain IDs of all workers in sorted order.
	var keyList []string
	for workerKey, w := range scq.workers {
		if !request.JustExecutingWorkers || w.currentTask != nil {
			keyList = append(keyList, string(workerKey))
		}
	}
	sort.Strings(keyList)
	paginationInfo, endIndex := getPaginationInfo(len(keyList), request.PageSize, func(i int) bool {
		return startAfterWorkerKey == nil || keyList[i] > *startAfterWorkerKey
	})

	// Extract status.
	keyListRegion := keyList[paginationInfo.StartIndex:endIndex]
	workers := make([]*buildqueuestate.WorkerState, 0, len(keyListRegion))
	for _, key := range keyListRegion {
		workerKey := workerKey(key)
		w := scq.workers[workerKey]
		var currentOperation *buildqueuestate.OperationState
		if t := w.currentTask; t != nil {
			// A task may have more than one operation
			// associated with it, in case deduplication of
			// in-flight requests occurred. For the time
			// being, let's not expose the concept of tasks
			// through the web UI yet. Just show one of the
			// operations.
			//
			// Do make this deterministic by picking the
			// operation with the lowest name,
			// alphabetically.
			var o *operation
			for _, oCheck := range t.operations {
				if o == nil || o.name > oCheck.name {
					o = oCheck
				}
			}
			currentOperation = o.getOperationState(bq)
			currentOperation.SizeClassQueueName = nil
			currentOperation.Stage = nil
		}
		workerID := workerKey.getWorkerID()
		workers = append(workers, &buildqueuestate.WorkerState{
			Id:               workerID,
			Timeout:          bq.cleanupQueue.getTimestamp(w.cleanupKey),
			CurrentOperation: currentOperation,
			Drained:          w.isDrained(scq, workerID),
		})
	}
	return &buildqueuestate.ListWorkersResponse{
		Workers:        workers,
		PaginationInfo: paginationInfo,
	}, nil
}

// ListDrains returns a list of all the drains that are present within a
// given platform queue.
func (bq *InMemoryBuildQueue) ListDrains(ctx context.Context, request *buildqueuestate.ListDrainsRequest) (*buildqueuestate.ListDrainsResponse, error) {
	sizeClassKey, err := newSizeClassKeyFromName(request.SizeClassQueueName)
	if err != nil {
		return nil, err
	}

	bq.enter(bq.clock.Now())
	defer bq.leave()

	scq, ok := bq.sizeClassQueues[sizeClassKey]
	if !ok {
		return nil, status.Error(codes.NotFound, "No workers for this instance name, platform and size class exist")
	}

	// Obtain IDs of all drains in sorted order.
	keyList := make([]string, 0, len(scq.drains))
	for drainKey := range scq.drains {
		keyList = append(keyList, drainKey)
	}
	sort.Strings(keyList)

	// Extract drains.
	drains := make([]*buildqueuestate.DrainState, 0, len(keyList))
	for _, key := range keyList {
		drains = append(drains, scq.drains[key])
	}
	return &buildqueuestate.ListDrainsResponse{
		Drains: drains,
	}, nil
}

func (bq *InMemoryBuildQueue) modifyDrain(request *buildqueuestate.AddOrRemoveDrainRequest, modifyFunc func(scq *sizeClassQueue, drainKey string)) (*emptypb.Empty, error) {
	sizeClassKey, err := newSizeClassKeyFromName(request.SizeClassQueueName)
	if err != nil {
		return nil, err
	}
	drainKey, err := json.Marshal(request.WorkerIdPattern)
	if err != nil {
		return nil, util.StatusWrapWithCode(err, codes.InvalidArgument, "Failed to marshal worker ID pattern")
	}

	bq.enter(bq.clock.Now())
	defer bq.leave()

	scq, ok := bq.sizeClassQueues[sizeClassKey]
	if !ok {
		return nil, status.Error(codes.NotFound, "No workers for this instance name, platform and size class exist")
	}
	modifyFunc(scq, string(drainKey))
	return &emptypb.Empty{}, nil
}

// AddDrain inserts a new drain into the list of drains currently
// tracked by the platform queue.
func (bq *InMemoryBuildQueue) AddDrain(ctx context.Context, request *buildqueuestate.AddOrRemoveDrainRequest) (*emptypb.Empty, error) {
	return bq.modifyDrain(request, func(scq *sizeClassQueue, drainKey string) {
		scq.drains[drainKey] = &buildqueuestate.DrainState{
			WorkerIdPattern:  request.WorkerIdPattern,
			CreatedTimestamp: bq.getCurrentTime(),
		}

		// Wake up all synchronizing workers that are queued,
		// but are supposed to be drained. This ensures that
		// they will stop picking up work immediately.
		for workerKey, w := range scq.workers {
			if w.wakeup != nil && workerMatchesPattern(workerKey.getWorkerID(), request.WorkerIdPattern) {
				w.wakeUp(scq)
			}
		}
	})
}

// RemoveDrain removes a drain from the list of drains currently tracked
// by the platform queue.
func (bq *InMemoryBuildQueue) RemoveDrain(ctx context.Context, request *buildqueuestate.AddOrRemoveDrainRequest) (*emptypb.Empty, error) {
	return bq.modifyDrain(request, func(scq *sizeClassQueue, drainKey string) {
		delete(scq.drains, drainKey)

		// Wake up all synchronizing workers that are drained.
		// This ensures that they pick up work immediately.
		close(scq.undrainWakeup)
		scq.undrainWakeup = make(chan struct{})
	})
}

// TerminateWorkers can be used to indicate that workers are going to be
// terminated in the nearby future. This function will block until any
// operations running on the workers complete, thereby allowing the
// workers to be terminated without interrupting operations.
func (bq *InMemoryBuildQueue) TerminateWorkers(ctx context.Context, request *buildqueuestate.TerminateWorkersRequest) (*emptypb.Empty, error) {
	var completionWakeups []chan struct{}
	bq.enter(bq.clock.Now())
	for _, scq := range bq.sizeClassQueues {
		for workerKey, w := range scq.workers {
			if workerMatchesPattern(workerKey.getWorkerID(), request.WorkerIdPattern) {
				w.terminating = true
				if t := w.currentTask; t != nil {
					// The task will be at the
					// EXECUTING stage, so it can
					// only transition to COMPLETED.
					completionWakeups = append(completionWakeups, t.stageChangeWakeup)
				} else if w.wakeup != nil {
					// Wake up the worker, so that
					// it's dequeued. This prevents
					// additional tasks to be
					// assigned to it.
					w.wakeUp(scq)
				}
			}
		}
	}
	bq.leave()

	for _, completionWakeup := range completionWakeups {
		select {
		case <-completionWakeup:
			// Worker has become idle.
		case <-ctx.Done():
			// Client has canceled the request.
			return nil, util.StatusFromContext(ctx)
		}
	}
	return &emptypb.Empty{}, nil
}

// getNextSynchronizationAtDelay generates a timestamp that is attached
// to SynchronizeResponses, indicating that the worker is permitted to
// hold off sending updates for a limited amount of time.
func (bq *InMemoryBuildQueue) getNextSynchronizationAtDelay() *timestamppb.Timestamp {
	return timestamppb.New(bq.now.Add(bq.configuration.BusyWorkerSynchronizationInterval))
}

// getCurrentTime generates a timestamp that corresponds to the current
// time. It is attached to SynchronizeResponses, indicating that the
// worker should resynchronize again as soon as possible. It is also
// used to compute QueuedTimestamps.
func (bq *InMemoryBuildQueue) getCurrentTime() *timestamppb.Timestamp {
	return timestamppb.New(bq.now)
}

// enter acquires the lock on the InMemoryBuildQueue and runs any
// cleanup tasks that should be executed prior mutating its state.
func (bq *InMemoryBuildQueue) enter(t time.Time) {
	bq.lock.Lock()
	if t.After(bq.now) {
		bq.now = t
		bq.cleanupQueue.run(bq.now)
	}
}

// leave releases the lock on the InMemoryBuildQueue.
func (bq *InMemoryBuildQueue) leave() {
	bq.lock.Unlock()
}

// getIdleSynchronizeResponse returns a synchronization response that
// explicitly instructs a worker to return to the idle state.
func (bq *InMemoryBuildQueue) getIdleSynchronizeResponse() *remoteworker.SynchronizeResponse {
	return &remoteworker.SynchronizeResponse{
		NextSynchronizationAt: bq.getCurrentTime(),
		DesiredState: &remoteworker.DesiredState{
			WorkerState: &remoteworker.DesiredState_Idle{
				Idle: &emptypb.Empty{},
			},
		},
	}
}

// addPlatformQueue creates a new platform queue for a given platform.
func (bq *InMemoryBuildQueue) addPlatformQueue(platformKey platform.Key, workerInvocationStickinessLimit time.Duration, maximumQueuedBackgroundLearningOperations int, backgroundLearningOperationPriority int32) *platformQueue {
	pq := &platformQueue{
		platformKey:                               platformKey,
		instanceNamePatcher:                       digest.NewInstanceNamePatcher(platformKey.GetInstanceNamePrefix(), digest.EmptyInstanceName),
		workerInvocationStickinessLimit:           workerInvocationStickinessLimit,
		maximumQueuedBackgroundLearningOperations: maximumQueuedBackgroundLearningOperations,
		backgroundLearningOperationPriority:       backgroundLearningOperationPriority,
	}
	bq.platformQueuesTrie.Set(platformKey, len(bq.platformQueues))
	bq.platformQueues = append(bq.platformQueues, pq)
	return pq
}

// platformQueueList is a list of *platformQueue objects that is
// sortable. It is used by InMemoryBuildQueue.GetBuildQueueState() to
// emit all platform queues in sorted order.
type platformQueueList []*platformQueue

func (h platformQueueList) Len() int {
	return len(h)
}

func (h platformQueueList) Less(i, j int) bool {
	pi, pj := h[i].platformKey, h[j].platformKey
	ii, ij := pi.GetInstanceNamePrefix().String(), pj.GetInstanceNamePrefix().String()
	return ii < ij || (ii == ij && pi.GetPlatformString() < pj.GetPlatformString())
}

func (h platformQueueList) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
}

func newPlatformKeyFromName(name *buildqueuestate.PlatformQueueName) (platform.Key, error) {
	var badPlatformKey platform.Key
	if name == nil {
		return badPlatformKey, status.Error(codes.InvalidArgument, "No platform queue name provided")
	}
	instanceName, err := digest.NewInstanceName(name.InstanceNamePrefix)
	if err != nil {
		return badPlatformKey, util.StatusWrapf(err, "Invalid instance name prefix %#v", name.InstanceNamePrefix)
	}
	return platform.NewKey(instanceName, name.Platform)
}

// sizeClassKey can be used as a key for maps to uniquely identify a set
// of workers that are all for the same platform and have the same size.
type sizeClassKey struct {
	platformKey platform.Key
	sizeClass   uint32
}

func newSizeClassKeyFromName(name *buildqueuestate.SizeClassQueueName) (sizeClassKey, error) {
	if name == nil {
		return sizeClassKey{}, status.Error(codes.InvalidArgument, "No size class queue name provided")
	}
	platformKey, err := newPlatformKeyFromName(name.PlatformQueueName)
	if err != nil {
		return sizeClassKey{}, err
	}
	return sizeClassKey{
		platformKey: platformKey,
		sizeClass:   name.SizeClass,
	}, nil
}

func (k *sizeClassKey) getSizeClassQueueName() *buildqueuestate.SizeClassQueueName {
	return &buildqueuestate.SizeClassQueueName{
		PlatformQueueName: k.platformKey.GetPlatformQueueName(),
		SizeClass:         k.sizeClass,
	}
}

// platformQueue is an actual build operations queue that contains a
// list of associated workers and operations that are queued to be
// executed. An InMemoryBuildQueue contains a platformQueue for every
// instance/platform for which one or more workers exist.
type platformQueue struct {
	platformKey                               platform.Key
	instanceNamePatcher                       digest.InstanceNamePatcher
	workerInvocationStickinessLimit           time.Duration
	maximumQueuedBackgroundLearningOperations int
	backgroundLearningOperationPriority       int32

	sizeClasses     []uint32
	sizeClassQueues []*sizeClassQueue
}

func (pq *platformQueue) addSizeClassQueue(bq *InMemoryBuildQueue, sizeClass uint32, mayBeRemoved bool) *sizeClassQueue {
	instanceNamePrefix := pq.platformKey.GetInstanceNamePrefix().String()
	platformStr := pq.platformKey.GetPlatformString()
	sizeClassStr := strconv.FormatUint(uint64(sizeClass), 10)
	platformLabels := map[string]string{
		"instance_name_prefix": instanceNamePrefix,
		"platform":             platformStr,
		"size_class":           sizeClassStr,
	}
	tasksScheduledTotal := inMemoryBuildQueueTasksScheduledTotal.MustCurryWith(platformLabels)
	scq := &sizeClassQueue{
		platformQueue: pq,
		sizeClass:     sizeClass,
		mayBeRemoved:  mayBeRemoved,

		invocations: map[invocationKey]*invocation{},
		workers:     map[workerKey]*worker{},

		drains:        map[string]*buildqueuestate.DrainState{},
		undrainWakeup: make(chan struct{}),

		tasksScheduledWorkerFromSameInvocation:  newTasksScheduledCounterVec(tasksScheduledTotal, "WorkerFromSameInvocation"),
		tasksScheduledWorkerWithoutInvocation:   newTasksScheduledCounterVec(tasksScheduledTotal, "WorkerWithoutInvocation"),
		tasksScheduledWorkerFromOtherInvocation: newTasksScheduledCounterVec(tasksScheduledTotal, "WorkerFromOtherInvocation"),
		tasksScheduledQueue:                     newTasksScheduledCounterVec(tasksScheduledTotal, "Queue"),
		tasksQueuedDurationSeconds:              inMemoryBuildQueueTasksQueuedDurationSeconds.WithLabelValues(instanceNamePrefix, platformStr, sizeClassStr),
		tasksExecutingDurationSeconds:           inMemoryBuildQueueTasksExecutingDurationSeconds.MustCurryWith(platformLabels),
		tasksExecutingRetries:                   inMemoryBuildQueueTasksExecutingRetries.MustCurryWith(platformLabels),
		tasksCompletedDurationSeconds:           inMemoryBuildQueueTasksCompletedDurationSeconds.WithLabelValues(instanceNamePrefix, platformStr, sizeClassStr),

		workersCreatedTotal:          inMemoryBuildQueueWorkersCreatedTotal.WithLabelValues(instanceNamePrefix, platformStr, sizeClassStr),
		workersRemovedIdleTotal:      inMemoryBuildQueueWorkersRemovedTotal.WithLabelValues(instanceNamePrefix, platformStr, sizeClassStr, "Idle"),
		workersRemovedExecutingTotal: inMemoryBuildQueueWorkersRemovedTotal.WithLabelValues(instanceNamePrefix, platformStr, sizeClassStr, "Executing"),
	}
	if pq.workerInvocationStickinessLimit > 0 {
		outcomes := inMemoryBuildQueueWorkerInvocationStickinessOutcomesTotal.MustCurryWith(platformLabels)
		scq.workerInvocationStickinessOutcomesNoLastInvocations = outcomes.WithLabelValues("NoLastInvocations")
		scq.workerInvocationStickinessOutcomesNoQueuedLastInvocations = outcomes.WithLabelValues("NoQueuedLastInvocations")
		scq.workerInvocationStickinessOutcomesNoStickyLastInvocations = outcomes.WithLabelValues("NoStickyLastInvocations")
		scq.workerInvocationStickinessOutcomesSelectedBestTask = outcomes.WithLabelValues("SelectedBestTask")
		scq.workerInvocationStickinessOutcomesSelectedStickyTask = outcomes.WithLabelValues("SelectedStickyTask")
	}

	// Force creation of all metrics associated with this platform
	// queue to make recording rules work.
	scq.tasksExecutingDurationSeconds.WithLabelValues("Success", "")
	scq.tasksExecutingRetries.WithLabelValues("Success", "")

	// Insert the new size class queue into the platform queue.
	// Keep the size class queues sorted, so that they are provided
	// to initialsizeclass.Selector deterministically.
	i := 0
	for i < len(pq.sizeClasses) && pq.sizeClasses[i] < sizeClass {
		i++
	}

	pq.sizeClasses = append(pq.sizeClasses, 0)
	copy(pq.sizeClasses[i+1:], pq.sizeClasses[i:])
	pq.sizeClasses[i] = sizeClass

	pq.sizeClassQueues = append(pq.sizeClassQueues, nil)
	copy(pq.sizeClassQueues[i+1:], pq.sizeClassQueues[i:])
	pq.sizeClassQueues[i] = scq

	bq.sizeClassQueues[scq.getKey()] = scq

	return scq
}

// tasksScheduledCounterVec is a pair of counters for both boolean
// values of "do_not_cache". It is used by the "tasks_scheduled_total"
// Prometheus metric.
type tasksScheduledCounterVec struct {
	doNotCacheTrue  prometheus.Counter
	doNotCacheFalse prometheus.Counter
}

func newTasksScheduledCounterVec(counterVec *prometheus.CounterVec, assignment string) tasksScheduledCounterVec {
	return tasksScheduledCounterVec{
		doNotCacheTrue:  counterVec.WithLabelValues(assignment, "true"),
		doNotCacheFalse: counterVec.WithLabelValues(assignment, "false"),
	}
}

type sizeClassQueue struct {
	platformQueue *platformQueue
	sizeClass     uint32
	mayBeRemoved  bool

	// Map of all invocations for which we track state.
	invocations map[invocationKey]*invocation
	// Binary heap of all invocations that have one or more queued
	// operations. Used by workers to pick an operation.
	queuedInvocations queuedInvocationsHeap
	// Binary heap of all invocations for which one or more idle
	// workers exist that previously ran a task belonging to an
	// invocation. This heap is sorted by time at which the last
	// operation completed.
	idleSynchronizingWorkersInvocations idleSynchronizingWorkersInvocationsHeap
	idleSynchronizingWorkers            idleSynchronizingWorkersList
	workers                             map[workerKey]*worker
	cleanupKey                          cleanupKey

	drains        map[string]*buildqueuestate.DrainState
	undrainWakeup chan struct{}

	// Prometheus metrics.
	tasksScheduledWorkerFromSameInvocation  tasksScheduledCounterVec
	tasksScheduledWorkerWithoutInvocation   tasksScheduledCounterVec
	tasksScheduledWorkerFromOtherInvocation tasksScheduledCounterVec
	tasksScheduledQueue                     tasksScheduledCounterVec
	tasksQueuedDurationSeconds              prometheus.Observer
	tasksExecutingDurationSeconds           prometheus.ObserverVec
	tasksExecutingRetries                   prometheus.ObserverVec
	tasksCompletedDurationSeconds           prometheus.Observer

	workersCreatedTotal          prometheus.Counter
	workersRemovedIdleTotal      prometheus.Counter
	workersRemovedExecutingTotal prometheus.Counter

	workerInvocationStickinessOutcomesNoLastInvocations       prometheus.Counter
	workerInvocationStickinessOutcomesNoQueuedLastInvocations prometheus.Counter
	workerInvocationStickinessOutcomesNoStickyLastInvocations prometheus.Counter
	workerInvocationStickinessOutcomesSelectedBestTask        prometheus.Counter
	workerInvocationStickinessOutcomesSelectedStickyTask      prometheus.Counter
}

func (scq *sizeClassQueue) getKey() sizeClassKey {
	return sizeClassKey{
		platformKey: scq.platformQueue.platformKey,
		sizeClass:   scq.sizeClass,
	}
}

// remove is invoked when Synchronize() isn't being invoked by any
// worker for a given platform quickly enough. It causes the platform
// queue and all associated queued operations to be removed from the
// InMemoryBuildQueue.
func (scq *sizeClassQueue) remove(bq *InMemoryBuildQueue) {
	// Cancel all queued operations.
	for scq.queuedInvocations.Len() > 0 {
		i := scq.queuedInvocations[scq.queuedInvocations.Len()-1]
		i.queuedOperations[i.queuedOperations.Len()-1].task.complete(bq, &remoteexecution.ExecuteResponse{
			Status: status.New(codes.Unavailable, "Workers for this instance name, platform and size class disappeared while task was queued").Proto(),
		}, false)
	}

	delete(bq.sizeClassQueues, scq.getKey())
	pq := scq.platformQueue
	i := 0
	for pq.sizeClassQueues[i] != scq {
		i++
	}
	pq.sizeClasses = append(pq.sizeClasses[:i], pq.sizeClasses[i+1:]...)
	pq.sizeClassQueues = append(pq.sizeClassQueues[:i], pq.sizeClassQueues[i+1:]...)

	if len(pq.sizeClasses) == 0 {
		// No size classes remain for this platform queue,
		// meaning that we can remove it. We must make sure the
		// list of platform queues remains contiguous.
		index := bq.platformQueuesTrie.GetExact(pq.platformKey)
		newLength := len(bq.platformQueues) - 1
		lastPQ := bq.platformQueues[newLength]
		bq.platformQueues[index] = lastPQ
		bq.platformQueues = bq.platformQueues[:newLength]
		bq.platformQueuesTrie.Set(lastPQ.platformKey, index)
		bq.platformQueuesTrie.Remove(pq.platformKey)
	}
}

// removeStaleWorker is invoked when Synchronize() isn't being invoked
// by a worker quickly enough. It causes the worker to be removed from
// the InMemoryBuildQueue.
func (scq *sizeClassQueue) removeStaleWorker(bq *InMemoryBuildQueue, workerKey workerKey, removalTime time.Time) {
	w := scq.workers[workerKey]
	if t := w.currentTask; t == nil {
		scq.workersRemovedIdleTotal.Inc()
	} else {
		scq.workersRemovedExecutingTotal.Inc()
		t.complete(bq, &remoteexecution.ExecuteResponse{
			Status: status.Newf(codes.Unavailable, "Worker %s disappeared while task was executing", workerKey).Proto(),
		}, false)
	}
	w.clearLastInvocations()
	delete(scq.workers, workerKey)

	// Trigger platform queue removal if necessary.
	if len(scq.workers) == 0 && scq.mayBeRemoved {
		bq.cleanupQueue.add(&scq.cleanupKey, removalTime.Add(bq.configuration.PlatformQueueWithNoWorkersTimeout), func() {
			scq.remove(bq)
		})
	}
}

// GetOrCreateInvocation looks up the invocation key in the size class
// queue, returning the corresponding invocation. If no invocation under
// this key exists, a new invocation is created.
//
// As the invocation may be just created, the caller must either queue
// an operation, increase the executing operations count, place a worker
// in the invocation or call invocation.removeIfEmpty().
func (scq *sizeClassQueue) getOrCreateInvocation(invocationKey invocationKey) *invocation {
	if i, ok := scq.invocations[invocationKey]; ok {
		return i
	}
	i := &invocation{
		invocationKey:                            invocationKey,
		sizeClassQueue:                           scq,
		queuedInvocationsIndex:                   -1,
		idleSynchronizingWorkersInvocationsIndex: -1,
	}
	scq.invocations[invocationKey] = i
	return i
}

// workerKey can be used as a key for maps to uniquely identify a worker
// within the domain of a certain platform. This key is used for looking
// up the state of a worker when synchronizing.
type workerKey string

func newWorkerKey(workerID map[string]string) workerKey {
	key, err := json.Marshal(workerID)
	if err != nil {
		panic(fmt.Sprintf("Failed to marshal worker ID: %s", err))
	}
	return workerKey(key)
}

// getWorkerID reobtains the worker ID map that was used to construct
// the platformKey. As this is only used infrequently, we don't bother
// keeping the unmarshalled map around to preserve memory usage.
func (k workerKey) getWorkerID() map[string]string {
	var workerID map[string]string
	if err := json.Unmarshal([]byte(k), &workerID); err != nil {
		panic(fmt.Sprintf("Failed to unmarshal previously marshalled worker ID: %s", err))
	}
	return workerID
}

// queuedInvocationsHeap is a binary heap that contains a list of all
// invocations in a platform queue that have one or more queued
// operations. It is used to determine which operation should be started
// in case a worker requests a new task.
type queuedInvocationsHeap []*invocation

func (h queuedInvocationsHeap) Len() int {
	return len(h)
}

func (h queuedInvocationsHeap) Less(i, j int) bool {
	return h[i].isPreferred(h[j], 0)
}

func (h queuedInvocationsHeap) Swap(i, j int) {
	if h[i].queuedInvocationsIndex != i || h[j].queuedInvocationsIndex != j {
		panic("Invalid queue indices")
	}
	h[i], h[j] = h[j], h[i]
	h[i].queuedInvocationsIndex = i
	h[j].queuedInvocationsIndex = j
}

func (h *queuedInvocationsHeap) Push(x interface{}) {
	e := x.(*invocation)
	if e.queuedInvocationsIndex != -1 {
		panic("Invalid queue index")
	}
	e.queuedInvocationsIndex = len(*h)
	*h = append(*h, e)
}

func (h *queuedInvocationsHeap) Pop() interface{} {
	old := *h
	n := len(old)
	e := old[n-1]
	old[n-1] = nil
	*h = old[:n-1]
	if e.queuedInvocationsIndex != n-1 {
		panic("Invalid queue index")
	}
	e.queuedInvocationsIndex = -1
	return e
}

// IdleSynchronizingWorkersInvocationsHeap is a binary heap that
// contains all invocations for which one or more workers exist that
// most recently ran a task associated with that invocation, and are
// currently synchronizing against the scheduler.
//
// This heap is used to determine which invocations should have their
// workers rebalanced to other invocations.
type idleSynchronizingWorkersInvocationsHeap []*invocation

func (h idleSynchronizingWorkersInvocationsHeap) Len() int {
	return len(h)
}

func (h idleSynchronizingWorkersInvocationsHeap) Less(i, j int) bool {
	// Sort invocations by worker utilization rate, so that
	// invocations with an excessive number of workers have their
	// workers taken first. Invocations can only be part of this
	// heap if they have no queued operations, so we only need to
	// consider executing and idle workers:
	//
	//     utilization = executing / (executing + idle)
	//
	// Simplify the comparison by cross-multiplying and removing the
	// common part.
	ui := uint64(h[i].executingWorkersCount) * uint64(len(h[j].idleSynchronizingWorkers))
	uj := uint64(h[j].executingWorkersCount) * uint64(len(h[i].idleSynchronizingWorkers))
	if ui < uj {
		return true
	} else if ui > uj {
		return false
	}
	// Tie breaker, most likely because both invocations are no
	// longer executing anything. Remove workers from the oldest
	// invocation first, as those are the least likely to be needed.
	return h[i].lastOperationCompletion.Before(h[j].lastOperationCompletion)
}

func (h idleSynchronizingWorkersInvocationsHeap) Swap(i, j int) {
	if h[i].idleSynchronizingWorkersInvocationsIndex != i || h[j].idleSynchronizingWorkersInvocationsIndex != j {
		panic("Invalid queue indices")
	}
	h[i], h[j] = h[j], h[i]
	h[i].idleSynchronizingWorkersInvocationsIndex = i
	h[j].idleSynchronizingWorkersInvocationsIndex = j
}

func (h *idleSynchronizingWorkersInvocationsHeap) Push(x interface{}) {
	e := x.(*invocation)
	if e.idleSynchronizingWorkersInvocationsIndex != -1 {
		panic("Invalid queue index")
	}
	e.idleSynchronizingWorkersInvocationsIndex = len(*h)
	*h = append(*h, e)
}

func (h *idleSynchronizingWorkersInvocationsHeap) Pop() interface{} {
	old := *h
	n := len(old)
	e := old[n-1]
	old[n-1] = nil
	*h = old[:n-1]
	if e.idleSynchronizingWorkersInvocationsIndex != n-1 {
		panic("Invalid queue index")
	}
	e.idleSynchronizingWorkersInvocationsIndex = -1
	return e
}

func mustNewAny(src proto.Message) *anypb.Any {
	any, err := anypb.New(src)
	if err != nil {
		panic(err)
	}
	return any
}

// invocationKey is a key type for maps for identifying client
// invocations.
type invocationKey string

// backgroundLearningInvocationKey is a predefined invocationKey that is
// used for all operations that are created to perform background
// learning (see initialsizeclass.FeedbackAnalyzer).
var backgroundLearningInvocationKey = mustNewInvocationKey(mustNewAny(&buildqueuestate.BackgroundLearning{}))

func newInvocationKey(invocationID *anypb.Any) (invocationKey, error) {
	data, err := protojson.Marshal(invocationID)
	if err != nil {
		return "", util.StatusWrap(err, "Failed to marshal invocation ID")
	}
	return invocationKey(data), nil
}

func mustNewInvocationKey(invocationID *anypb.Any) invocationKey {
	invocationKey, err := newInvocationKey(invocationID)
	if err != nil {
		panic(err)
	}
	return invocationKey
}

func (k invocationKey) getInvocationID() *anypb.Any {
	var invocationID anypb.Any
	if err := protojson.Unmarshal([]byte(k), &invocationID); err != nil {
		panic(fmt.Sprintf("Failed to unmarshal previously marshalled invocation ID: %s", err))
	}
	return &invocationID
}

// Invocation keeps track of operations that all need to be scheduled on
// a single size class queue, all having the same invocation ID. These
// operations will be scheduled fairly with respect to other
// invocations.
type invocation struct {
	invocationKey  invocationKey
	sizeClassQueue *sizeClassQueue

	// The index of this invocation inside the
	// sizeClassQueue.queuedInvocations heap.
	queuedInvocationsIndex int
	// The index of this invocation inside the
	// sizeClassQueue.idleSynchronizingWorkersInvocations heap.
	idleSynchronizingWorkersInvocationsIndex int

	// Heap of operations that are part of this invocation that are
	// currently in the QUEUED stage.
	queuedOperations queuedOperationsHeap

	// Number of workers that are executing operations that belong
	// to this invocation. This equals the number of operations that
	// are part of this invocation that are currently in the
	// EXECUTING stage.
	executingWorkersCount uint32
	// The time at which the last executing operation was completed.
	// This value is used to determine which invocations are the
	// best candidates for rebalancing idle synchronizing workers.
	lastOperationCompletion time.Time
	// Number of workers that are idle and most recently completed
	// an operation belonging to this invocation.
	idleWorkersCount uint32
	// List of workers that are idle and most recently executed an
	// operation belonging to this invocation and are currently
	// synchronizing against the scheduler.
	idleSynchronizingWorkers idleSynchronizingWorkersList
}

// IsActive returns whether an invocation has one or more queued or
// executing operations. These are generally the ones that users of the
// BuildQueueState service want to view.
func (i *invocation) isActive() bool {
	return i.queuedOperations.Len() > 0 || i.executingWorkersCount > 0
}

// RemoveIfEmpty checks whether the invocation is empty (i.e., not
// containing any operations or workers). If so, it removes the
// invocation from the size class queue in which it is contained.
func (i *invocation) removeIfEmpty() {
	if i.queuedOperations.Len() == 0 && i.executingWorkersCount == 0 && i.idleWorkersCount == 0 {
		scq := i.sizeClassQueue
		if scq.invocations[i.invocationKey] != i {
			panic("Attempted to remove an invocation that was already removed")
		}
		delete(scq.invocations, i.invocationKey)
	}
}

func (i *invocation) getInvocationState(bq *InMemoryBuildQueue) *buildqueuestate.InvocationState {
	is := &buildqueuestate.InvocationState{
		Id:                            i.invocationKey.getInvocationID(),
		QueuedOperationsCount:         uint32(i.queuedOperations.Len()),
		ExecutingWorkersCount:         i.executingWorkersCount,
		IdleWorkersCount:              i.idleWorkersCount,
		IdleSynchronizingWorkersCount: uint32(len(i.idleSynchronizingWorkers)),
	}
	if is.QueuedOperationsCount > 0 {
		is.FirstQueuedOperation = i.queuedOperations[0].getOperationState(bq)
		is.FirstQueuedOperation.SizeClassQueueName = nil
		is.FirstQueuedOperation.InvocationId = nil
	}
	return is
}

// DecrementExecutingWorkersCount decrements the number of operations in
// the EXECUTING stage that are part of this invocation.
//
// Because the number of operations in the EXECUTING stage is used to
// prioritize tasks, this function may need to adjust the position of
// this invocation in the queued invocations heap. It may also need to
// remove the invocation entirely in case it no longer contains any
// operations.
func (i *invocation) decrementExecutingWorkersCount(bq *InMemoryBuildQueue) {
	if i.executingWorkersCount == 0 {
		panic("Executing operations count invalid")
	}
	i.executingWorkersCount--
	i.lastOperationCompletion = bq.now
	scq := i.sizeClassQueue
	heapMaybeFix(&scq.queuedInvocations, i.queuedInvocationsIndex)
	heapMaybeFix(&scq.idleSynchronizingWorkersInvocations, i.idleSynchronizingWorkersInvocationsIndex)
	i.removeIfEmpty()
}

// IncrementExecutingWorkersCount increments the number of operations in
// the EXECUTING stage that are part of this invocation.
func (i *invocation) incrementExecutingWorkersCount() {
	i.executingWorkersCount++
	scq := i.sizeClassQueue
	heapMaybeFix(&scq.queuedInvocations, i.queuedInvocationsIndex)
	heapMaybeFix(&scq.idleSynchronizingWorkersInvocations, i.idleSynchronizingWorkersInvocationsIndex)
}

var priorityExponentiationBase = math.Pow(2.0, 0.01)

// IsPreferred returns whether the first queued operation of invocation
// i should be preferred over the first queued operation of invocation j.
func (i *invocation) isPreferred(j *invocation, stickinessLimit time.Duration) bool {
	// To introduce fairness, we want to prefer scheduling
	// operations belonging to invocations that have the fewest
	// running operations. In addition to that, we still want to
	// respect priorities at the global level.
	//
	// Combine these two properties into a single score value
	// according to the following expression, where the invocation
	// with the lowest score is most favourable.
	//
	// S = (executingWorkersCount + 1) * b^priority
	//
	// Note that REv2 priorities are inverted; the lower the integer
	// value, the higher the priority. The '+ 1' part has been added
	// to this expression to ensure that the priority is still taken
	// into account when the number of executing operations is zero.
	//
	// The base value for the expontentiation is chosen to be
	// 2^0.01 =~ 1.007. This means that if the difference in
	// priority between two builds is 100, one build will be allowed
	// to run twice as many operations as the other.
	oi, oj := i.queuedOperations[0], j.queuedOperations[0]
	ei, ej := float64(i.executingWorkersCount+1), float64(j.executingWorkersCount+1)
	var si, sj float64
	if pi, pj := float64(oi.priority), float64(oj.priority); pi < pj {
		// Invocation i has a higher priority. Give invocation j
		// a penalty based on the difference in priority.
		si, sj = ei, ej*math.Pow(priorityExponentiationBase, pj-pi)
	} else if pi > pj {
		// Invocation j has a higher priority. Give invocation i
		// a penalty based on the difference in priority.
		si, sj = ei*math.Pow(priorityExponentiationBase, pi-pj), ej
	} else {
		// Both invocations have the same priority.
		si, sj = ei, ej
	}
	return si < sj || (si == sj && oi.task.queuedBefore(oj.task, stickinessLimit))
}

// queuedOperationsHeap is a binary heap that stores queued operations,
// sorted by order in which they need to be assigned to workers.
type queuedOperationsHeap []*operation

func (h queuedOperationsHeap) Len() int {
	return len(h)
}

func (h queuedOperationsHeap) Less(i, j int) bool {
	// Lexicographic order on priority and queued timestamp.
	return h[i].priority < h[j].priority || (h[i].priority == h[j].priority &&
		h[i].task.queuedBefore(h[j].task, 0))
}

func (h queuedOperationsHeap) Swap(i, j int) {
	if h[i].queueIndex != i || h[j].queueIndex != j {
		panic("Invalid queue indices")
	}
	h[i], h[j] = h[j], h[i]
	h[i].queueIndex = i
	h[j].queueIndex = j
}

func (h *queuedOperationsHeap) Push(x interface{}) {
	o := x.(*operation)
	if o.queueIndex != -1 {
		panic("Invalid queue index")
	}
	o.queueIndex = len(*h)
	*h = append(*h, o)
}

func (h *queuedOperationsHeap) Pop() interface{} {
	old := *h
	n := len(old)
	o := old[n-1]
	old[n-1] = nil
	*h = old[:n-1]
	if o.queueIndex != n-1 {
		panic("Invalid queue index")
	}
	o.queueIndex = -1
	return o
}

// Operation that a client can use to reference a task.
//
// The difference between operations and tasks is that tasks manage the
// lifecycle of a piece of work in general, while operations manage it
// in the context of a client invocation. This means that if in-flight
// deduplication of requests occurs, a task may be associated with two
// or more operations.
//
// If a single client were to abandon an operation (e.g., by closing the
// gRPC channel), the task and other operations that task will remain
// unaffected.
type operation struct {
	name     string
	task     *task
	priority int32

	// The invocation of which this operation is a part. queueIndex
	// contains the index at which the operation is stored in the
	// invocation's queuedOperations heap. When negative, it means
	// that the operation is no longer in the queued stage (and thus
	// either in the executing or completed stage).
	//
	// Because invocations are managed per size class, an operation
	// may move from one invocation to another one it is retried as
	// part of a different size class. All invocations of which this
	// operation is a part during its lifetime will have the same
	// invocation ID.
	invocation *invocation
	queueIndex int

	// Number of clients that are calling Execute() or
	// WaitExecution() on this operation.
	waiters                uint
	mayExistWithoutWaiters bool
	cleanupKey             cleanupKey
}

// waitExecution periodically streams a series of longrunning.Operation
// messages back to the client, containing the state of the current
// operation. Streaming is stopped after execution of the operation is
// completed.
func (o *operation) waitExecution(bq *InMemoryBuildQueue, out remoteexecution.Execution_ExecuteServer) error {
	ctx := out.Context()

	// Bookkeeping for determining whether operations are abandoned
	// by clients. Operations should be removed if there are no
	// clients calling Execute() or WaitExecution() for a certain
	// amount of time.
	if o.cleanupKey.isActive() {
		bq.cleanupQueue.remove(o.cleanupKey)
	}
	o.waiters++
	defer func() {
		if o.waiters == 0 {
			panic("Invalid waiters count on operation")
		}
		o.waiters--
		o.maybeStartCleanup(bq)
	}()

	t := o.task
	for {
		// Construct the longrunning.Operation that needs to be
		// sent back to the client.
		metadata, err := anypb.New(&remoteexecution.ExecuteOperationMetadata{
			Stage:        t.getStage(),
			ActionDigest: t.desiredState.ActionDigest,
		})
		if err != nil {
			return util.StatusWrap(err, "Failed to marshal execute operation metadata")
		}
		operation := &longrunning.Operation{
			Name:     o.name,
			Metadata: metadata,
		}
		if t.executeResponse != nil {
			operation.Done = true
			response, err := anypb.New(t.executeResponse)
			if err != nil {
				return util.StatusWrap(err, "Failed to marshal execute response")
			}
			operation.Result = &longrunning.Operation_Response{Response: response}
		}
		stageChangeWakeup := t.stageChangeWakeup
		bq.leave()

		// Send the longrunning.Operation back to the client.
		if err := out.Send(operation); operation.Done || err != nil {
			bq.enter(bq.clock.Now())
			return err
		}

		// Suspend until the client closes the connection, the
		// action completes or a certain amount of time has
		// passed without any updates.
		timer, timerChannel := bq.clock.NewTimer(bq.configuration.ExecutionUpdateInterval)
		select {
		case <-ctx.Done():
			timer.Stop()
			bq.enter(bq.clock.Now())
			return util.StatusFromContext(ctx)
		case <-stageChangeWakeup:
			timer.Stop()
			bq.enter(bq.clock.Now())
		case t := <-timerChannel:
			bq.enter(t)
		}
	}
}

// removeQueuedFromInvocation removes an operation that is in the queued
// state from the invocation. If the invocation no longer has any queued
// operations, it will be removed from the queued invocations heap in
// the containing platform queue.
func (o *operation) removeQueuedFromInvocation() {
	i := o.invocation
	heap.Remove(&i.queuedOperations, o.queueIndex)
	scq := i.sizeClassQueue
	heapRemoveOrFix(&scq.queuedInvocations, i.queuedInvocationsIndex, i.queuedOperations.Len())
}

// Enqueue a newly created operation in the heap of queued operations of
// an invocation. This method is called whenever an operation can't be
// assigned to a worker immediately, due to no idle synchronizing
// workers for this size class queue being available.
func (o *operation) enqueue() {
	i := o.invocation
	heap.Push(&i.queuedOperations, o)
	scq := i.sizeClassQueue
	heapPushOrFix(&scq.queuedInvocations, i.queuedInvocationsIndex, i)
}

func (o *operation) remove(bq *InMemoryBuildQueue) {
	delete(bq.operationsNameMap, o.name)

	t := o.task
	if len(t.operations) == 1 {
		// Forcefully terminate the associated task if it won't
		// have any other operations associated with it.
		t.complete(bq, &remoteexecution.ExecuteResponse{
			Status: status.New(codes.Canceled, "Task no longer has any waiting clients").Proto(),
		}, false)
		t.registerCompletedStageFinished(bq)
	} else {
		// The underlying task is shared with other operations.
		// Remove the operation, while leaving the task intact.
		i := o.invocation
		switch t.getStage() {
		case remoteexecution.ExecutionStage_QUEUED:
			o.removeQueuedFromInvocation()
			i.removeIfEmpty()
		case remoteexecution.ExecutionStage_EXECUTING:
			i.decrementExecutingWorkersCount(bq)
		}
	}
	delete(t.operations, o.invocation)
}

func (o *operation) getOperationState(bq *InMemoryBuildQueue) *buildqueuestate.OperationState {
	i := o.invocation
	t := o.task
	sizeClassKey := i.sizeClassQueue.getKey()
	s := &buildqueuestate.OperationState{
		Name:               o.name,
		SizeClassQueueName: sizeClassKey.getSizeClassQueueName(),
		InvocationId:       i.invocationKey.getInvocationID(),
		QueuedTimestamp:    t.desiredState.QueuedTimestamp,
		ActionDigest:       t.desiredState.ActionDigest,
		TargetId:           t.targetID,
		Timeout:            bq.cleanupQueue.getTimestamp(o.cleanupKey),
		Priority:           o.priority,
		InstanceNameSuffix: t.desiredState.InstanceNameSuffix,
	}
	switch t.getStage() {
	case remoteexecution.ExecutionStage_QUEUED:
		s.Stage = &buildqueuestate.OperationState_Queued{
			Queued: &emptypb.Empty{},
		}
	case remoteexecution.ExecutionStage_EXECUTING:
		s.Stage = &buildqueuestate.OperationState_Executing{
			Executing: &emptypb.Empty{},
		}
	case remoteexecution.ExecutionStage_COMPLETED:
		s.Stage = &buildqueuestate.OperationState_Completed{
			Completed: t.executeResponse,
		}
	}
	return s
}

func (o *operation) maybeStartCleanup(bq *InMemoryBuildQueue) {
	if o.waiters == 0 && !o.mayExistWithoutWaiters {
		bq.cleanupQueue.add(&o.cleanupKey, bq.now.Add(bq.configuration.OperationWithNoWaitersTimeout), func() {
			o.remove(bq)
		})
	}
}

// Task state that is created for every piece of work that needs to be
// executed by a worker. Tasks are associated with one or more
// operations. In the general case a task has one operation, but there
// may be multiple in case multiple clients request that the same action
// is built and deduplication is performed.
type task struct {
	operations   map[*invocation]*operation
	actionDigest digest.Digest
	desiredState remoteworker.DesiredState_Executing

	// The name of the target that triggered this operation. This
	// field is not strictly necessary to implement the BuildQueue
	// and OperationQueueServer interfaces. It needs to be present
	// to implement BuildQueueState.
	targetID string

	// currentStageStartTime is used by register*StageFinished() to
	// obtain Prometheus metrics.
	currentStageStartTime time.Time

	// The worker that is currently executing the task. The
	// retryCount specifies how many additional times the operation
	// was provided to the worker. This counter may be non-zero in
	// case of network flakiness or worker crashes.
	currentWorker *worker
	retryCount    int

	initialSizeClassLearner initialsizeclass.Learner
	mayExistWithoutWaiters  bool

	executeResponse   *remoteexecution.ExecuteResponse
	stageChangeWakeup chan struct{}
}

// NewOperation attaches a new operation to a task. This function must
// be called at least once after creating a task. It is most frequently
// called right after creating a task, but may also be used to attach
// additional operations to an existing task in case of in-flight
// deduplication.
func (t *task) newOperation(bq *InMemoryBuildQueue, priority int32, i *invocation, mayExistWithoutWaiters bool) *operation {
	o := &operation{
		name:                   uuid.Must(bq.uuidGenerator()).String(),
		task:                   t,
		priority:               priority,
		invocation:             i,
		mayExistWithoutWaiters: mayExistWithoutWaiters,
		queueIndex:             -1,
	}
	if _, ok := t.operations[i]; ok {
		panic("Task is already associated with this invocation")
	}
	t.operations[i] = o
	bq.operationsNameMap[o.name] = o
	return o
}

// ReportNonFinalStageChange can be used to wake up clients that are
// calling Execute() or WaitExecution(), causing them to receive another
// non-final stage change update.
func (t *task) reportNonFinalStageChange() {
	close(t.stageChangeWakeup)
	t.stageChangeWakeup = make(chan struct{})
}

// Schedule a task. This function will first attempt to directly assign
// a task to an idle worker that is synchronizing against the scheduler.
// When no such worker exists, it will queue the operation, so that a
// worker may pick it up later.
func (t *task) schedule(bq *InMemoryBuildQueue) {
	// Check whether there are idle workers that are synchronizing
	// against the scheduler on which we can schedule the operation
	// directly.
	scq := t.getSizeClassQueue()
	for i := range t.operations {
		if l := i.idleSynchronizingWorkers; len(l) > 0 {
			// Pick a worker that last ran another task that
			// belonged to the same invocation. These
			// workers will most likely to give a high input
			// file cache hit rate.
			t.registerQueuedStageStarted(bq, &scq.tasksScheduledWorkerFromSameInvocation)
			l[0].worker.assignUnqueuedTaskAndWakeUp(bq, t)
			return
		}
	}
	if l := scq.idleSynchronizingWorkers; len(l) > 0 {
		// Pick a recently created worker that hasn't run any
		// tasks yet. This ensures that other workers remain
		// available for their last invocations.
		t.registerQueuedStageStarted(bq, &scq.tasksScheduledWorkerWithoutInvocation)
		l[0].worker.assignUnqueuedTaskAndWakeUp(bq, t)
	} else if h := scq.idleSynchronizingWorkersInvocations; len(h) > 0 {
		// Pick a worker that most recently ran a task that is
		// associated with the invocation with the lowest worker
		// utilization rate.
		t.registerQueuedStageStarted(bq, &scq.tasksScheduledWorkerFromOtherInvocation)
		h[0].idleSynchronizingWorkers[0].worker.assignUnqueuedTaskAndWakeUp(bq, t)
	} else {
		// No idle workers are available that are synchronizing
		// against the scheduler. Queue the operation, so that
		// workers can pick it up when they become available.
		t.registerQueuedStageStarted(bq, &scq.tasksScheduledQueue)
		for _, o := range t.operations {
			o.enqueue()
		}
	}
}

// getStage returns whether the task is in the queued, executing or
// completed stage.
func (t *task) getStage() remoteexecution.ExecutionStage_Value {
	if t.executeResponse != nil {
		return remoteexecution.ExecutionStage_COMPLETED
	}
	if t.currentWorker != nil {
		return remoteexecution.ExecutionStage_EXECUTING
	}
	return remoteexecution.ExecutionStage_QUEUED
}

func (t *task) queuedBefore(other *task, stickinessLimit time.Duration) bool {
	return t.desiredState.QueuedTimestamp.AsTime().Before(other.desiredState.QueuedTimestamp.AsTime().Add(stickinessLimit))
}

// Complete execution of the task by registering the execution response.
// This function wakes up any clients waiting on the task to complete.
func (t *task) complete(bq *InMemoryBuildQueue, executeResponse *remoteexecution.ExecuteResponse, completedByWorker bool) {
	switch t.getStage() {
	case remoteexecution.ExecutionStage_QUEUED:
		// The task isn't executing. Create a temporary worker
		// on which we start the task, so that we can go through
		// the regular completion code below.
		var w worker
		w.assignQueuedTask(bq, t)
	case remoteexecution.ExecutionStage_EXECUTING:
		// Task is executing on a worker. Make sure to preserve
		// worker.lastInvocations.
		if completedByWorker {
			w := t.currentWorker
			if len(w.lastInvocations) != 0 {
				panic("Executing worker cannot have invocations associated with it")
			}
			if len(t.operations) == 0 {
				panic("Task no longer has an invocation associated with it")
			}
			for i := range t.operations {
				w.lastInvocations = append(w.lastInvocations, i)
				i.idleWorkersCount++
			}
		}
	case remoteexecution.ExecutionStage_COMPLETED:
		// Task is already completed. Nothing to do.
		return
	}

	currentSCQ := t.getSizeClassQueue()
	for i := range t.operations {
		i.decrementExecutingWorkersCount(bq)
	}
	t.currentWorker.currentTask = nil
	t.currentWorker = nil
	result, grpcCode := getResultAndGRPCCodeFromExecuteResponse(executeResponse)
	t.registerExecutingStageFinished(bq, result, grpcCode)

	// Communicate the results to the initial size class learner,
	// which may request that the task is re-executed.
	pq := currentSCQ.platformQueue
	var timeout time.Duration
	if code, actionResult := status.FromProto(executeResponse.Status).Code(), executeResponse.Result; code == codes.OK && actionResult.GetExitCode() == 0 {
		// The task succeeded, but we're still getting
		// instructed to run the task again for training
		// purposes. If that happens, create a new task that
		// runs in the background. The user does not need to be
		// blocked on this.
		executionMetadata := actionResult.GetExecutionMetadata()
		backgroundSizeClassIndex, backgroundTimeout, backgroundInitialSizeClassLearner := t.initialSizeClassLearner.Succeeded(
			executionMetadata.GetExecutionCompletedTimestamp().AsTime().Sub(
				executionMetadata.GetExecutionStartTimestamp().AsTime()),
			pq.sizeClasses)
		t.initialSizeClassLearner = nil
		if backgroundInitialSizeClassLearner != nil {
			if pq.maximumQueuedBackgroundLearningOperations == 0 {
				// No background learning permitted.
				backgroundInitialSizeClassLearner.Abandoned()
			} else {
				backgroundSCQ := pq.sizeClassQueues[backgroundSizeClassIndex]
				backgroundInvocation := backgroundSCQ.getOrCreateInvocation(backgroundLearningInvocationKey)
				if backgroundInvocation.queuedOperations.Len() >= pq.maximumQueuedBackgroundLearningOperations {
					// Already running too many background tasks.
					backgroundInitialSizeClassLearner.Abandoned()
				} else {
					backgroundAction := *t.desiredState.Action
					backgroundAction.DoNotCache = true
					backgroundAction.Timeout = durationpb.New(backgroundTimeout)
					backgroundTask := &task{
						operations:              map[*invocation]*operation{},
						actionDigest:            t.actionDigest,
						desiredState:            t.desiredState,
						targetID:                t.targetID,
						initialSizeClassLearner: backgroundInitialSizeClassLearner,
						stageChangeWakeup:       make(chan struct{}),
					}
					backgroundTask.desiredState.Action = &backgroundAction
					backgroundTask.newOperation(bq, pq.backgroundLearningOperationPriority, backgroundInvocation, true)
					backgroundTask.schedule(bq)
				}
			}
		}
	} else if completedByWorker {
		// The worker communicated that the task failed. Attempt
		// to run it on another size class.
		timeout, t.initialSizeClassLearner = t.initialSizeClassLearner.Failed(code == codes.DeadlineExceeded)
	} else {
		// The task was completed, but this was not done by the
		// worker. Treat is as a regular failure.
		t.initialSizeClassLearner.Abandoned()
		t.initialSizeClassLearner = nil
	}

	if t.initialSizeClassLearner != nil {
		// Re-execution against the largest size class is
		// requested, using the original timeout value.
		// Transplant all operations to the other size class
		// queue and reschedule.
		t.desiredState.Action.Timeout = durationpb.New(timeout)
		t.registerCompletedStageFinished(bq)
		largestSCQ := pq.sizeClassQueues[len(pq.sizeClassQueues)-1]
		operations := t.operations
		t.operations = make(map[*invocation]*operation, len(operations))
		for oldI, o := range operations {
			i := largestSCQ.getOrCreateInvocation(oldI.invocationKey)
			t.operations[i] = o
			o.invocation = i
		}
		t.schedule(bq)
		t.reportNonFinalStageChange()
	} else {
		// The task succeeded or it failed on the largest size
		// class. Let's just complete it.
		//
		// Scrub data from the task that are no longer needed
		// after completion. This reduces memory usage
		// significantly. Keep the Action digest, so that
		// there's still a way to figure out what the task was.
		delete(bq.inFlightDeduplicationMap, t.actionDigest)
		t.executeResponse = executeResponse
		t.desiredState.Action = nil
		close(t.stageChangeWakeup)
		t.stageChangeWakeup = nil

		// Background learning tasks may continue to exist, even
		// if no clients wait for the results. Now that this
		// task is completed, it must go through the regular
		// cleanup process.
		for _, o := range t.operations {
			if o.mayExistWithoutWaiters {
				o.mayExistWithoutWaiters = false
				o.maybeStartCleanup(bq)
			}
		}
	}
}

func (t *task) getSizeClassQueue() *sizeClassQueue {
	// Tasks don't store an explicit reference to the size class
	// queue. This isn't necessary, as a task is guaranteed to have
	// one or more operations. We ensure that all operations belong
	// to invocations inside the same size class queue.
	for i := range t.operations {
		return i.sizeClassQueue
	}
	panic("Task does not have any operation associated with it")
}

// RegisterQueuedStageStarted updates Prometheus metrics related to the
// task entering the QUEUED stage.
func (t *task) registerQueuedStageStarted(bq *InMemoryBuildQueue, tasksScheduledCounterVec *tasksScheduledCounterVec) {
	if t.desiredState.Action.DoNotCache {
		tasksScheduledCounterVec.doNotCacheTrue.Inc()
	} else {
		tasksScheduledCounterVec.doNotCacheFalse.Inc()
	}
	t.currentStageStartTime = bq.now
}

// RegisterQueuedStageFinished updates Prometheus metrics related to the
// task finishing the QUEUED stage.
func (t *task) registerQueuedStageFinished(bq *InMemoryBuildQueue) {
	scq := t.getSizeClassQueue()
	scq.tasksQueuedDurationSeconds.Observe(bq.now.Sub(t.currentStageStartTime).Seconds())
	t.currentStageStartTime = bq.now
}

// RegisterExecutingStageFinished updates Prometheus metrics related to
// the task finishing the EXECUTING stage.
func (t *task) registerExecutingStageFinished(bq *InMemoryBuildQueue, result, grpcCode string) {
	scq := t.getSizeClassQueue()
	scq.tasksExecutingDurationSeconds.WithLabelValues(result, grpcCode).Observe(bq.now.Sub(t.currentStageStartTime).Seconds())
	scq.tasksExecutingRetries.WithLabelValues(result, grpcCode).Observe(float64(t.retryCount))
	t.currentStageStartTime = bq.now
}

// RegisterCompletedStageFinished updates Prometheus metrics related to
// the task finishing the COMPLETED stage, meaning the task got removed.
func (t *task) registerCompletedStageFinished(bq *InMemoryBuildQueue) {
	scq := t.getSizeClassQueue()
	scq.tasksCompletedDurationSeconds.Observe(bq.now.Sub(t.currentStageStartTime).Seconds())
	t.currentStageStartTime = bq.now
}

// worker state for every node capable of executing operations.
type worker struct {
	// The task that this worker is currently executing. This field
	// must be kept in sync with task.currentWorker.
	currentTask *task
	// Used to garbage collect workers that have disappeared.
	cleanupKey cleanupKey
	// When true, this worker is going to terminate in the nearby
	// future. This is effectively a drain that cannot be cleared
	// through the BuildQueueState interface.
	terminating bool
	// The set of invocations that were associated with the task
	// that this worker completed most recently. This set is used to
	// make sure successive tasks belonging to one of these
	// invocations is more likely to end up on this worker. This
	// improves locality.
	lastInvocations []*invocation
	// When set, the worker is idle and currently issuing a blocking
	// Synchronize() call against the scheduler. This channel can be
	// closed to wake up the worker, either after assigning a task
	// to it or to force it becoming drained.
	wakeup chan<- struct{}
	// When 'wakeup' is set, this list contains the indices at which
	// this worker is placed in idleSynchronizingWorkersLists. These
	// indices are needed to efficiently dequeue workers in case of
	// wakeups or Synchronize() interruptions.
	//
	// When 'lastInvocations' is empty, this list contains a single
	// element, storing at which index this worker is queued in
	// sizeClassQueue.idleSynchronizingWorkers.
	//
	// When 'lastInvocations' is non-empty, this list contains
	// indices at which this worker is queued in
	// invocation.idleSynchronizingWorkers, where the order of
	// invocations corresponds to that of 'lastInvocations'.
	listIndices []int
}

func workerMatchesPattern(workerID, workerIDPattern map[string]string) bool {
	for key, value := range workerIDPattern {
		if workerID[key] != value {
			return false
		}
	}
	return true
}

func (w *worker) isDrained(scq *sizeClassQueue, workerID map[string]string) bool {
	// Implicitly treat workers that are terminating as being
	// drained. This prevents tasks from getting interrupted.
	if w.terminating {
		return true
	}
	for _, drain := range scq.drains {
		if workerMatchesPattern(workerID, drain.WorkerIdPattern) {
			return true
		}
	}
	return false
}

// Dequeue a worker. This method is either called by the worker itself
// at the end of Synchronize(), or when a worker needs to be woken up.
func (w *worker) dequeue(scq *sizeClassQueue) {
	if len(w.lastInvocations) == 0 {
		// Worker has never run an operation before. The worker
		// was only queued as part of the size class queue
		// itself.
		scq.idleSynchronizingWorkers.dequeue(w.listIndices[0])
	} else {
		// Worker has run an operation before. The worker was
		// queued as part of one or more invocations.
		for idx, i := range w.lastInvocations {
			i.idleSynchronizingWorkers.dequeue(w.listIndices[idx])
			heapRemoveOrFix(
				&scq.idleSynchronizingWorkersInvocations,
				i.idleSynchronizingWorkersInvocationsIndex,
				len(i.idleSynchronizingWorkers))
		}
	}
	w.wakeup = nil
	w.listIndices = w.listIndices[:0]
}

// MaybeDequeue is the same as dequeue(), except that it may also be
// called if the worker is not actually queued. This is used to dequeue
// a worker as part of interrupted Synchronize() calls.
func (w *worker) maybeDequeue(scq *sizeClassQueue) {
	if w.wakeup != nil {
		w.dequeue(scq)
	}
}

// WakeUp wakes up an idle worker that is currently synchronizing
// against the scheduler.
func (w *worker) wakeUp(scq *sizeClassQueue) {
	close(w.wakeup)
	w.dequeue(scq)
}

// AssignUnqueuedTask assigns a task that is not queued to a worker.
func (w *worker) assignUnqueuedTask(bq *InMemoryBuildQueue, t *task) {
	if w.currentTask != nil {
		panic("Worker is already associated with a task")
	}
	if t.currentWorker != nil {
		panic("Task is already associated with a worker")
	}

	t.registerQueuedStageFinished(bq)
	w.currentTask = t
	t.currentWorker = w
	t.retryCount = 0
	for i := range t.operations {
		i.incrementExecutingWorkersCount()
	}
	w.clearLastInvocations()
}

// AssignQueuedTask assigns a task that is queued to a worker. The task
// is unqueued in the process.
func (w *worker) assignQueuedTask(bq *InMemoryBuildQueue, t *task) {
	w.assignUnqueuedTask(bq, t)

	for _, o := range t.operations {
		o.removeQueuedFromInvocation()
	}
	t.reportNonFinalStageChange()
}

// AssignNextQueuedTask determines which queued task is the best
// candidate for execution and assigns it to the current task.
func (w *worker) assignNextQueuedTask(bq *InMemoryBuildQueue, scq *sizeClassQueue, workerID map[string]string) bool {
	if scq.queuedInvocations.Len() == 0 {
		return false
	}

	// Determine from which invocation we need to extract an
	// operation. We always want to pick the one that has the fewest
	// executing operations (corrected for the priority). In case of
	// ties, we want to schedule the oldest operation.
	//
	// The exception to this rule is when worker invocation
	// stickiness is enabled. In that case tie breaking gives a
	// slight advantage to any of the invocations associated with
	// the last executed task. This reduces the startup overhead of
	// actions that leave state behind on the worker.
	iBest := scq.queuedInvocations[0]
	tBest := iBest.queuedOperations[0].task
	tSelected := tBest
	if workerInvocationStickinessLimit := scq.platformQueue.workerInvocationStickinessLimit; workerInvocationStickinessLimit > 0 {
		var iSticky *invocation
		hasQueuedLastInvocations := false
		iSelected := iBest
		for _, iLast := range w.lastInvocations {
			if iLast.queuedOperations.Len() > 0 {
				hasQueuedLastInvocations = true
				if iLast.isPreferred(iBest, workerInvocationStickinessLimit) && (iSticky == nil || iLast.isPreferred(iSticky, 0)) {
					iSticky = iLast
					iSelected = iLast
				}
			}
		}
		tSelected = iSelected.queuedOperations[0].task

		if len(w.lastInvocations) == 0 {
			scq.workerInvocationStickinessOutcomesNoLastInvocations.Inc()
		} else if !hasQueuedLastInvocations {
			scq.workerInvocationStickinessOutcomesNoQueuedLastInvocations.Inc()
		} else if iSticky == nil {
			scq.workerInvocationStickinessOutcomesNoStickyLastInvocations.Inc()
		} else if tSelected == tBest {
			scq.workerInvocationStickinessOutcomesSelectedBestTask.Inc()
		} else {
			scq.workerInvocationStickinessOutcomesSelectedStickyTask.Inc()
		}
	}

	w.assignQueuedTask(bq, tSelected)
	return true
}

// ClearLastInvocations clears the list of invocations of the last task
// to run on this worker. This method needs to be called when workers
// are removed to make sure that invocations don't leak.
func (w *worker) clearLastInvocations() {
	if w.wakeup != nil {
		panic("Clearing last invocations would make it impossible to dequeue the worker")
	}
	for idx, i := range w.lastInvocations {
		if i.idleWorkersCount == 0 {
			panic("Invalid workers count")
		}
		i.idleWorkersCount--
		i.removeIfEmpty()
		w.lastInvocations[idx] = nil
	}
	w.lastInvocations = w.lastInvocations[:0]
}

// AssignUnqueuedTaskAndWakeUp is used to assign a task to an idle
// worker that is synchronizing against the scheduler and to wake it up.
// This method is used when clients directly assign a task to a worker,
// as opposed to letting the worker select a task itself.
func (w *worker) assignUnqueuedTaskAndWakeUp(bq *InMemoryBuildQueue, t *task) {
	// Wake up the worker prior to assigning the task. Assigning
	// clears w.lastInvocations, which cannot be done while the
	// worker is queued.
	w.wakeUp(t.getSizeClassQueue())
	w.assignUnqueuedTask(bq, t)
}

// GetExecutingSynchronizeResponse returns a synchronization response
// that instructs a worker to start executing a task.
func (w *worker) getExecutingSynchronizeResponse(bq *InMemoryBuildQueue) *remoteworker.SynchronizeResponse {
	t := w.currentTask
	return &remoteworker.SynchronizeResponse{
		NextSynchronizationAt: bq.getNextSynchronizationAtDelay(),
		DesiredState: &remoteworker.DesiredState{
			WorkerState: &remoteworker.DesiredState_Executing_{
				Executing: &t.desiredState,
			},
		},
	}
}

// getNextTask extracts the next queued task from the queue and assigns
// it to the current worker. Depending on whether a context object is
// provided, this function either blocks until work is available or
// returns immediately. When returning immediately, it instructs the
// worker to go idle.
func (w *worker) getNextTask(ctx context.Context, bq *InMemoryBuildQueue, scq *sizeClassQueue, workerID map[string]string, preferBeingIdle bool) (*remoteworker.SynchronizeResponse, error) {
	if preferBeingIdle {
		// The worker wants to terminate or is experiencing some
		// issues. Explicitly instruct the worker to go idle, so
		// that it knows it can hold off synchronizing.
		return bq.getIdleSynchronizeResponse(), nil
	}

	isDrained := w.isDrained(scq, workerID)
	if !isDrained && w.assignNextQueuedTask(bq, scq, workerID) {
		return w.getExecutingSynchronizeResponse(bq), nil
	}

	if ctx == nil {
		// We shouldn't block, as the worker is currently doing
		// some work that it shouldn't be doing. Request that
		// the worker goes idle immediately. It will
		// resynchronize as soon as it's done terminating its
		// current build action.
		return bq.getIdleSynchronizeResponse(), nil
	}

	timeoutTimer, timeoutChannel := bq.clock.NewTimer(bq.configuration.GetIdleWorkerSynchronizationInterval())
	defer timeoutTimer.Stop()

	for {
		if isDrained {
			// The worker is drained. Simply wait until
			// undrain operations occur.
			undrainWakeup := scq.undrainWakeup
			bq.leave()

			select {
			case t := <-timeoutChannel:
				// Timeout has been reached.
				bq.enter(t)
				return bq.getIdleSynchronizeResponse(), nil
			case <-ctx.Done():
				// Worker has canceled the request.
				bq.enter(bq.clock.Now())
				return nil, util.StatusFromContext(ctx)
			case <-undrainWakeup:
				// Worker might have been undrained.
				bq.enter(bq.clock.Now())
			}
		} else if w.assignNextQueuedTask(bq, scq, workerID) {
			// One or more tasks were queued. We're able to
			// synchronize without blocking by starting one
			// of those tasks.
			return w.getExecutingSynchronizeResponse(bq), nil
		} else {
			// No queued tasks available. Queue the worker,
			// so that clients may assign a task to us.
			if w.wakeup != nil || len(w.listIndices) != 0 {
				panic("Worker is already queued")
			}
			wakeup := make(chan struct{})
			w.wakeup = wakeup
			lastInvocations := w.lastInvocations
			if len(lastInvocations) == 0 {
				// Worker has not completed any tasks
				// before. Place ourselves in the size
				// class queue.
				w.listIndices = append(w.listIndices[:0], 0)
				scq.idleSynchronizingWorkers.enqueue(&idleSynchronizingWorker{
					worker:    w,
					listIndex: &w.listIndices[0],
				})
			} else {
				// Worker has completed a task before.
				// Place ourselves in each of the
				// invocations of the task, so that
				// we're more likely to receive tasks
				// belonging to the same invocations.
				w.listIndices = append(w.listIndices[:0], make([]int, len(w.lastInvocations))...)
				for idx, i := range w.lastInvocations {
					i.idleSynchronizingWorkers.enqueue(&idleSynchronizingWorker{
						worker:    w,
						listIndex: &w.listIndices[idx],
					})
					heapPushOrFix(&scq.idleSynchronizingWorkersInvocations, i.idleSynchronizingWorkersInvocationsIndex, i)
				}
			}
			bq.leave()

			select {
			case t := <-timeoutChannel:
				// Timeout has been reached.
				bq.enter(t)
				w.maybeDequeue(scq)
				if w.currentTask != nil {
					return w.getExecutingSynchronizeResponse(bq), nil
				}
				return bq.getIdleSynchronizeResponse(), nil
			case <-ctx.Done():
				// Worker has canceled the request.
				bq.enter(bq.clock.Now())
				w.maybeDequeue(scq)
				return nil, util.StatusFromContext(ctx)
			case <-wakeup:
				// Worker got woken up, meaning a task
				// got assigned to it, or it got drained.
				bq.enter(bq.clock.Now())
				if w.currentTask != nil {
					return w.getExecutingSynchronizeResponse(bq), nil
				}
			}
		}
		isDrained = w.isDrained(scq, workerID)
	}
}

// getCurrentOrNextTask either returns a synchronization response that
// instructs the worker to run the task it should be running. When the
// worker has no task assigned to it, it attempts to request a task from
// the queue.
func (w *worker) getCurrentOrNextTask(ctx context.Context, bq *InMemoryBuildQueue, scq *sizeClassQueue, workerID map[string]string, preferBeingIdle bool) (*remoteworker.SynchronizeResponse, error) {
	if t := w.currentTask; t != nil {
		if t.retryCount < bq.configuration.WorkerTaskRetryCount {
			t.retryCount++
			return &remoteworker.SynchronizeResponse{
				NextSynchronizationAt: bq.getNextSynchronizationAtDelay(),
				DesiredState: &remoteworker.DesiredState{
					WorkerState: &remoteworker.DesiredState_Executing_{
						Executing: &t.desiredState,
					},
				},
			}, nil
		}
		t.complete(bq, &remoteexecution.ExecuteResponse{
			Status: status.Newf(
				codes.Internal,
				"Attempted to execute task %d times, but it never completed. This task may cause worker %s to crash.",
				t.retryCount+1,
				newWorkerKey(workerID)).Proto(),
		}, false)
	}
	return w.getNextTask(ctx, bq, scq, workerID, preferBeingIdle)
}

// isRunningCorrectTask determines whether the worker is actually
// running the task the scheduler instructed it to run previously.
func (w *worker) isRunningCorrectTask(actionDigest *remoteexecution.Digest) bool {
	t := w.currentTask
	if t == nil {
		return false
	}
	desiredDigest := t.desiredState.ActionDigest
	return proto.Equal(actionDigest, desiredDigest)
}

// updateTask processes execution status updates from the worker that do
// not equal the 'completed' state.
func (w *worker) updateTask(bq *InMemoryBuildQueue, scq *sizeClassQueue, workerID map[string]string, actionDigest *remoteexecution.Digest, preferBeingIdle bool) (*remoteworker.SynchronizeResponse, error) {
	if !w.isRunningCorrectTask(actionDigest) {
		return w.getCurrentOrNextTask(nil, bq, scq, workerID, preferBeingIdle)
	}
	// The worker is doing fine. Allow it to continue with what it's
	// doing right now.
	return &remoteworker.SynchronizeResponse{
		NextSynchronizationAt: bq.getNextSynchronizationAtDelay(),
	}, nil
}

// completeTask processes execution status updates from the worker that
// equal the 'completed' state. It causes the execute response to be
// preserved and communicated to clients that are waiting on the
// completion of the task.
func (w *worker) completeTask(ctx context.Context, bq *InMemoryBuildQueue, scq *sizeClassQueue, workerID map[string]string, actionDigest *remoteexecution.Digest, executeResponse *remoteexecution.ExecuteResponse, preferBeingIdle bool) (*remoteworker.SynchronizeResponse, error) {
	if !w.isRunningCorrectTask(actionDigest) {
		return w.getCurrentOrNextTask(ctx, bq, scq, workerID, preferBeingIdle)
	}
	w.currentTask.complete(bq, executeResponse, true)
	return w.getNextTask(ctx, bq, scq, workerID, preferBeingIdle)
}

type idleSynchronizingWorker struct {
	worker    *worker
	listIndex *int
}

type idleSynchronizingWorkersList []idleSynchronizingWorker

func (l *idleSynchronizingWorkersList) enqueue(entry *idleSynchronizingWorker) {
	*entry.listIndex = len(*l)
	*l = append(*l, *entry)
}

func (l *idleSynchronizingWorkersList) dequeue(listIndex int) {
	(*l)[listIndex] = (*l)[len(*l)-1]
	*(*l)[listIndex].listIndex = listIndex
	(*l)[len(*l)-1] = idleSynchronizingWorker{}
	(*l) = (*l)[:len(*l)-1]
}

// HeapRemoveOrFix either calls heap.Remove() or heap.Fix(), depending
// on a provided counter value. This function may be used to correct the
// position of an object that contains a counter or list inside a heap,
// after the counter or list is mutated.
func heapRemoveOrFix(h heap.Interface, i, count int) {
	if count > 0 {
		heap.Fix(h, i)
	} else {
		heap.Remove(h, i)
	}
}

// HeapPushOrFix either calls heap.Push() or heap.Fix(), depending on an
// existing heap index. This function can be used to push an object into
// a heap, only if it's not part of the heap already.
func heapPushOrFix(h heap.Interface, i int, v interface{}) {
	if i < 0 {
		heap.Push(h, v)
	} else {
		heap.Fix(h, i)
	}
}

// HeapMaybeFix calls heap.Fix() only when an object is actually stored
// in the heap.
func heapMaybeFix(h heap.Interface, i int) {
	if i >= 0 {
		heap.Fix(h, i)
	}
}

// cleanupKey is a handle that is used by cleanupQueue to refer to
// scheduled cleanups. It can be used to cancel a cleanup task. The key
// refers to the index of the corresponding entry in the cleanupHeap.
// Keys are offset by one, so that a zero value indicates the key is not
// associated with any cleanup entry.
type cleanupKey int

func (k cleanupKey) isActive() bool {
	return k != 0
}

// cleanupHeap is an implementation of container.Heap for cleanupEntry
// objects. It ensures that the cleanupKeys remain in sync with the
// indices of the cleanupEntries.
type cleanupHeap []cleanupEntry

func (h cleanupHeap) Len() int {
	return len(h)
}

func (h cleanupHeap) Less(i, j int) bool {
	return h[i].timestamp.Before(h[j].timestamp)
}

func (h cleanupHeap) Swap(i, j int) {
	if *h[i].key != cleanupKey(i+1) || *h[j].key != cleanupKey(j+1) {
		panic("Invalid cleanup keys")
	}
	h[i], h[j] = h[j], h[i]
	*h[i].key = cleanupKey(i + 1)
	*h[j].key = cleanupKey(j + 1)
}

func (h *cleanupHeap) Push(x interface{}) {
	e := x.(cleanupEntry)
	if *e.key != 0 {
		panic("Cleanup key already in use")
	}
	*h = append(*h, e)
	*e.key = cleanupKey(len(*h))
}

func (h *cleanupHeap) Pop() interface{} {
	old := *h
	n := len(old)
	e := old[n-1]
	old[n-1] = cleanupEntry{}
	*h = old[:n-1]
	if *e.key != cleanupKey(n) {
		panic("Invalid cleanup key")
	}
	*e.key = 0
	return e
}

// cleanupEntry stores at what point in time a certain cleanup function
// needs to be executed.
type cleanupEntry struct {
	key       *cleanupKey
	timestamp time.Time
	callback  func()
}

// cleanupQueue is an event queue that keeps track of closures that need
// to be executed at some point in the future. This data structure is
// used by InMemoryBuildQueue to keep track of workers, platform queues,
// operations, etc. that need to be garbage collected.
//
// Every entry in cleanupQueue is associated with one cleanupKey. The
// cleanupKey can be used to cancel the execution of a cleanup function.
type cleanupQueue struct {
	heap cleanupHeap
}

func (q *cleanupQueue) add(key *cleanupKey, timestamp time.Time, callback func()) {
	if *key != 0 {
		panic("Cleanup key is already in use")
	}
	heap.Push(&q.heap, cleanupEntry{
		key:       key,
		timestamp: timestamp,
		callback:  callback,
	})
}

func (q *cleanupQueue) remove(key cleanupKey) {
	heap.Remove(&q.heap, int(key)-1)
}

func (q *cleanupQueue) run(now time.Time) {
	for len(q.heap) > 0 && !q.heap[0].timestamp.After(now) {
		heap.Pop(&q.heap).(cleanupEntry).callback()
	}
}

func (q *cleanupQueue) getTimestamp(key cleanupKey) *timestamppb.Timestamp {
	if key == 0 {
		return nil
	}
	return timestamppb.New(q.heap[key-1].timestamp)
}
