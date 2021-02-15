package builder

import (
	"container/heap"
	"context"
	"encoding/json"
	"fmt"
	"math"
	"sort"
	"sync"
	"time"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/bazelbuild/remote-apis/build/bazel/semver"
	"github.com/buildbarn/bb-remote-execution/pkg/proto/buildqueuestate"
	"github.com/buildbarn/bb-remote-execution/pkg/proto/remoteworker"
	"github.com/buildbarn/bb-storage/pkg/blobstore"
	"github.com/buildbarn/bb-storage/pkg/builder"
	"github.com/buildbarn/bb-storage/pkg/clock"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/util"
	"github.com/golang/protobuf/jsonpb"
	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes"
	"github.com/golang/protobuf/ptypes/any"
	"github.com/golang/protobuf/ptypes/empty"
	"github.com/golang/protobuf/ptypes/timestamp"
	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"

	"google.golang.org/genproto/googleapis/longrunning"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"go.opencensus.io/trace"
	"go.opencensus.io/trace/propagation"
)

var (
	inMemoryBuildQueuePrometheusMetrics sync.Once

	inMemoryBuildQueueTasksQueuedTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "buildbarn",
			Subsystem: "builder",
			Name:      "in_memory_build_queue_tasks_queued_total",
			Help:      "Number of tasks created through Execute().",
		},
		[]string{"instance_name", "platform"})
	inMemoryBuildQueueTasksQueuedDurationSeconds = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "buildbarn",
			Subsystem: "builder",
			Name:      "in_memory_build_queue_tasks_queued_duration_seconds",
			Help:      "Time in seconds that tasks were queued before executing.",
			Buckets:   util.DecimalExponentialBuckets(-3, 6, 2),
		},
		[]string{"instance_name", "platform"})
	inMemoryBuildQueueTasksExecutingDurationSeconds = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "buildbarn",
			Subsystem: "builder",
			Name:      "in_memory_build_queue_tasks_executing_duration_seconds",
			Help:      "Time in seconds that tasks were executing before completing.",
			Buckets:   util.DecimalExponentialBuckets(-3, 6, 2),
		},
		[]string{"instance_name", "platform", "result", "grpc_code"})
	inMemoryBuildQueueTasksExecutingRetries = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "buildbarn",
			Subsystem: "builder",
			Name:      "in_memory_build_queue_tasks_executing_retries",
			Help:      "Number of times that tasks were retried before completing.",
			Buckets:   prometheus.LinearBuckets(0, 1, 11),
		},
		[]string{"instance_name", "platform", "result", "grpc_code"})
	inMemoryBuildQueueTasksCompletedDurationSeconds = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "buildbarn",
			Subsystem: "builder",
			Name:      "in_memory_build_queue_tasks_completed_duration_seconds",
			Help:      "Time in seconds that tasks were completed before being removed.",
			Buckets:   util.DecimalExponentialBuckets(-3, 6, 2),
		},
		[]string{"instance_name", "platform"})

	inMemoryBuildQueueWorkersCreatedTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "buildbarn",
			Subsystem: "builder",
			Name:      "in_memory_build_queue_workers_created_total",
			Help:      "Number of workers created by Synchronize().",
		},
		[]string{"instance_name", "platform"})
	inMemoryBuildQueueWorkersRemovedTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "buildbarn",
			Subsystem: "builder",
			Name:      "in_memory_build_queue_workers_removed_total",
			Help:      "Number of workers removed due to expiration.",
		},
		[]string{"instance_name", "platform", "state"})
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

	lock           sync.Mutex
	platformQueues map[platformKey]*platformQueue

	// Bookkeeping for WaitExecution(). This call permits us to
	// re-attach to operations by name. It also allows us to obtain
	// results for historical actions, up to a certain degree.
	operationsNameMap map[string]*operation

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
func NewInMemoryBuildQueue(contentAddressableStorage blobstore.BlobAccess, clock clock.Clock, uuidGenerator util.UUIDGenerator, configuration *InMemoryBuildQueueConfiguration, maximumMessageSizeBytes int) *InMemoryBuildQueue {
	inMemoryBuildQueuePrometheusMetrics.Do(func() {
		prometheus.MustRegister(inMemoryBuildQueueTasksQueuedTotal)
		prometheus.MustRegister(inMemoryBuildQueueTasksQueuedDurationSeconds)
		prometheus.MustRegister(inMemoryBuildQueueTasksExecutingDurationSeconds)
		prometheus.MustRegister(inMemoryBuildQueueTasksExecutingRetries)
		prometheus.MustRegister(inMemoryBuildQueueTasksCompletedDurationSeconds)

		prometheus.MustRegister(inMemoryBuildQueueWorkersCreatedTotal)
		prometheus.MustRegister(inMemoryBuildQueueWorkersRemovedTotal)
	})

	return &InMemoryBuildQueue{
		contentAddressableStorage:           contentAddressableStorage,
		clock:                               clock,
		uuidGenerator:                       uuidGenerator,
		configuration:                       configuration,
		platformQueueAbsenceHardFailureTime: clock.Now().Add(configuration.PlatformQueueWithNoWorkersTimeout),
		maximumMessageSizeBytes:             maximumMessageSizeBytes,
		platformQueues:                      map[platformKey]*platformQueue{},
		operationsNameMap:                   map[string]*operation{},
	}
}

var (
	_ builder.BuildQueue                    = (*InMemoryBuildQueue)(nil)
	_ remoteworker.OperationQueueServer     = (*InMemoryBuildQueue)(nil)
	_ buildqueuestate.BuildQueueStateServer = (*InMemoryBuildQueue)(nil)
)

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
	// Fetch the action and command corresponding to the execute
	// request. Ideally, a scheduler is oblivious of what these look
	// like, if it weren't for the fact that Action.DoNotCache and
	// Command.Platform are used for scheduling decisions.
	//
	// To prevent loading these messages from the Content
	// Addressable Storage (CAS) multiple times, the scheduler holds
	// on to them and passes them on to the workers.
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
	commandDigest, err := instanceName.NewDigestFromProto(action.CommandDigest)
	if err != nil {
		return util.StatusWrap(err, "Failed to extract digest for command")
	}
	commandMessage, err := bq.contentAddressableStorage.Get(ctx, commandDigest).ToProto(&remoteexecution.Command{}, bq.maximumMessageSizeBytes)
	if err != nil {
		return util.StatusWrap(err, "Failed to obtain command")
	}
	command := commandMessage.(*remoteexecution.Command)
	platformKey, err := newPlatformKey(instanceName, command.Platform)
	if err != nil {
		return err
	}

	bq.enter(bq.clock.Now())
	defer bq.leave()

	pq, ok := bq.platformQueues[platformKey]
	if !ok {
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
		return status.Errorf(code, "No workers exist for instance %#v platform %s", platformKey.instanceName.String(), platformKey.platform)
	}

	// Create a new task in case none exist against which this
	// request may be deduplicated.
	inFlightDeduplicationKey := newInFlightDeduplicationKey(in.ActionDigest)
	t, ok := pq.inFlightDeduplicationMap[inFlightDeduplicationKey]
	tStage := remoteexecution.ExecutionStage_QUEUED
	requestMetadata := getRequestMetadata(ctx)
	if ok {
		tStage = t.getStage()
	} else {
		argv0 := ""
		if argv := command.Arguments; len(argv) > 0 {
			argv0 = argv[0]
		}

		desiredState := remoteworker.DesiredState_Executing{
			ActionDigest:    in.ActionDigest,
			Action:          action,
			Command:         command,
			QueuedTimestamp: bq.getCurrentTime(),
		}

		span := trace.FromContext(ctx)
		if span != nil {
			desiredState.TraceContext = propagation.Binary(span.SpanContext())
		}

		// Forward the client-provided request metadata, so that
		// the worker logs it.
		if requestMetadata != nil {
			requestMetadataAny, err := ptypes.MarshalAny(requestMetadata)
			if err != nil {
				return util.StatusWrapWithCode(err, codes.InvalidArgument, "Failed to marshal request metadata")
			}
			desiredState.AuxiliaryMetadata = []*any.Any{requestMetadataAny}
		}

		t = &task{
			operations:    map[*invocation]*operation{},
			platformQueue: pq,
			desiredState:  desiredState,

			argv0: argv0,

			currentStageStartTime: bq.now,

			completionWakeup: make(chan struct{}),
		}
		if !action.DoNotCache {
			pq.inFlightDeduplicationMap[inFlightDeduplicationKey] = t
		}
		pq.tasksQueuedTotal.Inc()
	}

	// See if there are any other queued or executing tasks for this
	// invocation of the build client. Tasks are scheduled by
	// grouping them by invocation, so that scheduling is fair.
	invocationID := requestMetadata.GetToolInvocationId()
	i, ok := pq.invocations[invocationID]
	if !ok {
		i = &invocation{
			invocationID:  invocationID,
			platformQueue: pq,
		}
		pq.invocations[invocationID] = i
	}

	// Create an operation for this task if it's not yet part of
	// this invocation.
	o, ok := t.operations[i]
	if !ok {
		o = &operation{
			invocation: i,
			name:       uuid.Must(bq.uuidGenerator()).String(),
			task:       t,
		}
		t.operations[i] = o
		bq.operationsNameMap[o.name] = o

		switch tStage {
		case remoteexecution.ExecutionStage_QUEUED:
			// The task is either new, or the request has
			// been deduplicated against a task that is
			// still queued.
			heap.Push(&i.queuedOperations, queuedOperationsEntry{
				priority:  in.ExecutionPolicy.GetPriority(),
				operation: o,
			})
			if i.queuedOperations.Len() == 1 {
				heap.Push(&pq.queuedInvocations, i)
				pq.wakeupNextWorker()
			} else {
				heap.Fix(&pq.queuedInvocations, i.queueIndex)
			}
		case remoteexecution.ExecutionStage_EXECUTING:
			// The request has been deduplicated against a
			// task that is already in the executing stage.
			o.queueIndex = -1
			i.executingOperationsCount++
			heap.Fix(&pq.queuedInvocations, i.queueIndex)
		}
	}
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
	instanceName, err := digest.NewInstanceName(request.InstanceName)
	if err != nil {
		return nil, util.StatusWrapf(err, "Invalid instance name %#v", request.InstanceName)
	}
	platformKey, err := newPlatformKey(instanceName, request.Platform)
	if err != nil {
		return nil, err
	}
	workerKey := newWorkerKey(request.WorkerId)

	bq.enter(bq.clock.Now())
	defer bq.leave()

	pq, ok := bq.platformQueues[platformKey]
	if ok {
		// Prevent the platform queue from being garbage
		// collected, as it will now have an active worker.
		if pq.cleanupKey.isActive() {
			bq.cleanupQueue.remove(pq.cleanupKey)
		}
	} else {
		// Worker for this type of instance/platform pair has
		// never been observed before. Create a new queue to be
		// able to accept work.
		pq = newPlatformQueue(platformKey)
		bq.platformQueues[platformKey] = pq
	}

	w, ok := pq.workers[workerKey]
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
		pq.workers[workerKey] = w
		pq.workersCreatedTotal.Inc()
	}

	// Install cleanup handlers to ensure stale workers and queues
	// are purged after sufficient amount of time.
	defer func() {
		removalTime := bq.now.Add(bq.configuration.WorkerWithNoSynchronizationsTimeout)
		bq.cleanupQueue.add(&w.cleanupKey, removalTime, func() {
			pq.removeStaleWorker(bq, workerKey, removalTime)
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
		return w.getCurrentOrNextTask(ctx, bq, pq, request.WorkerId, false)
	case *remoteworker.CurrentState_Executing_:
		executing := workerState.Executing
		if executing.ActionDigest == nil {
			return nil, status.Error(codes.InvalidArgument, "Worker is executing, but provided no action digest")
		}
		switch executionState := executing.ExecutionState.(type) {
		case *remoteworker.CurrentState_Executing_Completed:
			return w.completeTask(ctx, bq, pq, request.WorkerId, executing.ActionDigest, executionState.Completed, executing.PreferBeingIdle)
		default:
			return w.updateTask(bq, pq, request.WorkerId, executing.ActionDigest, executing.PreferBeingIdle)
		}
	default:
		return nil, status.Error(codes.InvalidArgument, "Worker provided an unknown current state")
	}
}

// ListPlatformQueues returns a list of all platform queues currently
// managed by the scheduler.
func (bq *InMemoryBuildQueue) ListPlatformQueues(ctx context.Context, request *empty.Empty) (*buildqueuestate.ListPlatformQueuesResponse, error) {
	bq.enter(bq.clock.Now())
	defer bq.leave()

	// Obtain platform queue IDs in sorted order.
	var platformKeyList platformKeyList
	for platformKey := range bq.platformQueues {
		platformKeyList = append(platformKeyList, platformKey)
	}
	sort.Sort(platformKeyList)

	// Extract status.
	platformQueues := make([]*buildqueuestate.PlatformQueueState, 0, len(bq.platformQueues))
	for _, platformKey := range platformKeyList {
		pq := bq.platformQueues[platformKey]
		executingWorkersCount := uint32(0)
		for _, w := range pq.workers {
			if w.getCurrentTask() != nil {
				executingWorkersCount++
			}
		}
		platformQueues = append(platformQueues, &buildqueuestate.PlatformQueueState{
			Name:                   platformKey.getPlatformQueueName(),
			Timeout:                bq.cleanupQueue.getTimestamp(pq.cleanupKey),
			InvocationsCount:       uint32(len(pq.invocations)),
			QueuedInvocationsCount: uint32(pq.queuedInvocations.Len()),
			WorkersCount:           uint32(len(pq.workers)),
			ExecutingWorkersCount:  executingWorkersCount,
			DrainsCount:            uint32(len(pq.drains)),
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
func (bq *InMemoryBuildQueue) KillOperation(ctx context.Context, request *buildqueuestate.KillOperationRequest) (*empty.Empty, error) {
	bq.enter(bq.clock.Now())
	defer bq.leave()

	o, ok := bq.operationsNameMap[request.OperationName]
	if !ok {
		return nil, status.Errorf(codes.NotFound, "Operation %#v not found", request.OperationName)
	}
	o.task.complete(bq, &remoteexecution.ExecuteResponse{
		Status: status.New(codes.Unavailable, "Operation was killed administratively").Proto(),
	})
	return &empty.Empty{}, nil
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
	platformKey, err := newPlatformKeyFromName(request.PlatformQueueName)
	if err != nil {
		return nil, err
	}

	bq.enter(bq.clock.Now())
	defer bq.leave()

	pq, ok := bq.platformQueues[platformKey]
	if !ok {
		return nil, status.Error(codes.NotFound, "No workers for this instance name and platform exist")
	}

	if request.JustQueuedInvocations {
		// Return invocations with one or more queued
		// operations, sorted by scheduling order.
		invocations := make([]*buildqueuestate.InvocationState, 0, pq.queuedInvocations.Len())
		sort.Sort(&pq.queuedInvocations)
		for _, i := range pq.queuedInvocations {
			invocations = append(invocations, i.getInvocationState(bq))
		}
		return &buildqueuestate.ListInvocationsResponse{
			Invocations: invocations,
		}, nil
	}

	// Return all invocations in alphabetic order.
	keyList := make([]string, 0, len(pq.invocations))
	for invocationID := range pq.invocations {
		keyList = append(keyList, invocationID)
	}
	sort.Strings(keyList)

	invocations := make([]*buildqueuestate.InvocationState, 0, len(pq.invocations))
	for _, invocationID := range keyList {
		i := pq.invocations[invocationID]
		invocations = append(invocations, i.getInvocationState(bq))
	}
	return &buildqueuestate.ListInvocationsResponse{
		Invocations: invocations,
	}, nil
}

// ListQueuedOperations returns properties of all queued operations
// contained for a given invocation within a platform queue.
func (bq *InMemoryBuildQueue) ListQueuedOperations(ctx context.Context, request *buildqueuestate.ListQueuedOperationsRequest) (*buildqueuestate.ListQueuedOperationsResponse, error) {
	platformKey, err := newPlatformKeyFromName(request.PlatformQueueName)
	if err != nil {
		return nil, err
	}

	bq.enter(bq.clock.Now())
	defer bq.leave()

	pq, ok := bq.platformQueues[platformKey]
	if !ok {
		return nil, status.Error(codes.NotFound, "No workers for this instance name and platform exist")
	}
	i, ok := pq.invocations[request.InvocationId]
	if !ok {
		return nil, status.Error(codes.NotFound, "No operations for this invocation ID exist")
	}

	startAfter := request.StartAfter
	var startAfterQueuedTimestamp time.Time
	if startAfter != nil {
		startAfterQueuedTimestamp, err = ptypes.Timestamp(startAfter.QueuedTimestamp)
		if err != nil {
			return nil, util.StatusWrapWithCode(err, codes.InvalidArgument, "Invalid queued timestamp")
		}
	}

	// As every sorted list is also a valid binary heap, simply sort
	// the queued operations list prior to emitting it.
	sort.Sort(i.queuedOperations)
	paginationInfo, endIndex := getPaginationInfo(i.queuedOperations.Len(), request.PageSize, func(idx int) bool {
		e := i.queuedOperations[idx]
		if startAfter == nil || e.priority > startAfter.Priority {
			return true
		}
		if e.priority < startAfter.Priority {
			return false
		}
		queuedTimestamp, err := ptypes.Timestamp(e.operation.task.desiredState.QueuedTimestamp)
		if err != nil {
			panic(fmt.Sprintf("Failed to parse previously generated timestamp: %s", err))
		}
		return queuedTimestamp.After(startAfterQueuedTimestamp)
	})

	queuedOperationsRegion := i.queuedOperations[paginationInfo.StartIndex:endIndex]
	queuedOperations := make([]*buildqueuestate.OperationState, 0, queuedOperationsRegion.Len())
	for _, entry := range queuedOperationsRegion {
		s := entry.operation.getOperationState(bq)
		s.PlatformQueueName = nil
		s.InvocationId = ""
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
	platformKey, err := newPlatformKeyFromName(request.PlatformQueueName)
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

	pq, ok := bq.platformQueues[platformKey]
	if !ok {
		return nil, status.Error(codes.NotFound, "No workers for this instance name and platform exist")
	}

	// Obtain IDs of all workers in sorted order.
	var keyList []string
	for workerKey, w := range pq.workers {
		if !request.JustExecutingWorkers || w.getCurrentTask() != nil {
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
		w := pq.workers[workerKey]
		var currentOperation *buildqueuestate.OperationState
		if t := w.getCurrentTask(); t != nil {
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
			currentOperation.PlatformQueueName = nil
		}
		workerID := workerKey.getWorkerID()
		workers = append(workers, &buildqueuestate.WorkerState{
			Id:               workerID,
			Timeout:          bq.cleanupQueue.getTimestamp(w.cleanupKey),
			CurrentOperation: currentOperation,
			Drained:          w.isDrained(pq, workerID),
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
	platformKey, err := newPlatformKeyFromName(request.PlatformQueueName)
	if err != nil {
		return nil, err
	}

	bq.enter(bq.clock.Now())
	defer bq.leave()

	pq, ok := bq.platformQueues[platformKey]
	if !ok {
		return nil, status.Error(codes.NotFound, "No workers for this instance name and platform exist")
	}

	// Obtain IDs of all drains in sorted order.
	keyList := make([]string, 0, len(pq.drains))
	for drainKey := range pq.drains {
		keyList = append(keyList, drainKey)
	}
	sort.Strings(keyList)

	// Extract drains.
	drains := make([]*buildqueuestate.DrainState, 0, len(keyList))
	for _, key := range keyList {
		drains = append(drains, pq.drains[key])
	}
	return &buildqueuestate.ListDrainsResponse{
		Drains: drains,
	}, nil
}

func (bq *InMemoryBuildQueue) modifyDrain(request *buildqueuestate.AddOrRemoveDrainRequest, modifyFunc func(pq *platformQueue, drainKey string)) (*empty.Empty, error) {
	platformKey, err := newPlatformKeyFromName(request.PlatformQueueName)
	if err != nil {
		return nil, err
	}
	drainKey, err := json.Marshal(request.WorkerIdPattern)
	if err != nil {
		return nil, util.StatusWrapWithCode(err, codes.InvalidArgument, "Failed to marshal worker ID pattern")
	}

	bq.enter(bq.clock.Now())
	defer bq.leave()

	pq, ok := bq.platformQueues[platformKey]
	if !ok {
		return nil, status.Error(codes.NotFound, "No workers for this instance name and platform exist")
	}
	modifyFunc(pq, string(drainKey))
	close(pq.drainsWakeup)
	pq.drainsWakeup = make(chan struct{})
	return &empty.Empty{}, nil
}

// AddDrain inserts a new drain into the list of drains currently
// tracked by the platform queue.
func (bq *InMemoryBuildQueue) AddDrain(ctx context.Context, request *buildqueuestate.AddOrRemoveDrainRequest) (*empty.Empty, error) {
	return bq.modifyDrain(request, func(pq *platformQueue, drainKey string) {
		pq.drains[drainKey] = &buildqueuestate.DrainState{
			WorkerIdPattern:  request.WorkerIdPattern,
			CreatedTimestamp: bq.getCurrentTime(),
		}
	})
}

// RemoveDrain removes a drain from the list of drains currently tracked
// by the platform queue.
func (bq *InMemoryBuildQueue) RemoveDrain(ctx context.Context, request *buildqueuestate.AddOrRemoveDrainRequest) (*empty.Empty, error) {
	return bq.modifyDrain(request, func(pq *platformQueue, drainKey string) {
		delete(pq.drains, drainKey)
	})
}

// TerminateWorkers can be used to indicate that workers are going to be
// terminated in the nearby future. This function will block until any
// operations running on the workers complete, thereby allowing the
// workers to be terminated without interrupting operations.
func (bq *InMemoryBuildQueue) TerminateWorkers(ctx context.Context, request *buildqueuestate.TerminateWorkersRequest) (*empty.Empty, error) {
	var completionWakeups []chan struct{}
	bq.enter(bq.clock.Now())
	for _, pq := range bq.platformQueues {
		for workerKey, w := range pq.workers {
			if workerMatchesPattern(workerKey.getWorkerID(), request.WorkerIdPattern) {
				w.terminating = true
				if t := w.getCurrentTask(); t != nil {
					completionWakeups = append(completionWakeups, t.completionWakeup)
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
	return &empty.Empty{}, nil
}

// getNextSynchronizationAtDelay generates a timestamp that is attached
// to SynchronizeResponses, indicating that the worker is permitted to
// hold off sending updates for a limited amount of time.
func (bq *InMemoryBuildQueue) getNextSynchronizationAtDelay() *timestamp.Timestamp {
	t, err := ptypes.TimestampProto(bq.now.Add(bq.configuration.BusyWorkerSynchronizationInterval))
	if err != nil {
		panic(fmt.Sprintf("Failed to compute next synchronization timestamp: %s", err))
	}
	return t
}

// getCurrentTime generates a timestamp that corresponds to the current
// time. It is attached to SynchronizeResponses, indicating that the
// worker should resynchronize again as soon as possible. It is also
// used to compute QueuedTimestamps.
func (bq *InMemoryBuildQueue) getCurrentTime() *timestamp.Timestamp {
	t, err := ptypes.TimestampProto(bq.now)
	if err != nil {
		panic(fmt.Sprintf("Failed to compute next synchronization timestamp: %s", err))
	}
	return t
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
				Idle: &empty.Empty{},
			},
		},
	}
}

// platformKey can be used as a key for maps to uniquely identify a
// certain platform that should have its own operation queue.
type platformKey struct {
	instanceName digest.InstanceName
	platform     string
}

// platformKeyList is a list of platformKey objects that is sortable. It
// is used by InMemoryBuildQueue.GetBuildQueueState() to emit all
// platform queues in sorted order.
type platformKeyList []platformKey

func (h platformKeyList) Len() int {
	return len(h)
}

func (h platformKeyList) Less(i, j int) bool {
	ii := h[i].instanceName.String()
	ij := h[j].instanceName.String()
	return ii < ij || (ii == ij && h[i].platform < h[j].platform)
}

func (h platformKeyList) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
}

// getPlatformQueueName reobtains the instance name Platform message
// that was used to construct the platformKey. As this is only used
// infrequently, we don't bother keeping the unmarshalled Platform
// message around to preserve memory usage.
func (k *platformKey) getPlatformQueueName() *buildqueuestate.PlatformQueueName {
	var platform remoteexecution.Platform
	if err := jsonpb.UnmarshalString(k.platform, &platform); err != nil {
		panic(fmt.Sprintf("Failed to unmarshal previously marshalled platform: %s", err))
	}
	return &buildqueuestate.PlatformQueueName{
		InstanceName: k.instanceName.String(),
		Platform:     &platform,
	}
}

func newPlatformKey(instanceName digest.InstanceName, platform *remoteexecution.Platform) (platformKey, error) {
	// Ensure that the platform properties are in normal form.
	if platform == nil {
		platform = &remoteexecution.Platform{}
	}
	properties := platform.Properties
	for i := 1; i < len(properties); i++ {
		if properties[i-1].Name > properties[i].Name ||
			(properties[i-1].Name == properties[i].Name &&
				properties[i-1].Value >= properties[i].Value) {
			return platformKey{}, status.Error(codes.InvalidArgument, "Platform properties are not sorted")
		}
	}
	marshaler := jsonpb.Marshaler{}
	platformString, err := marshaler.MarshalToString(platform)
	if err != nil {
		return platformKey{}, util.StatusWrapWithCode(err, codes.InvalidArgument, "Failed to marshal platform message")
	}
	return platformKey{
		instanceName: instanceName,
		platform:     platformString,
	}, nil
}

func newPlatformKeyFromName(name *buildqueuestate.PlatformQueueName) (platformKey, error) {
	if name == nil {
		return platformKey{}, status.Error(codes.InvalidArgument, "No platform queue name provided")
	}
	instanceName, err := digest.NewInstanceName(name.InstanceName)
	if err != nil {
		return platformKey{}, util.StatusWrapf(err, "Invalid instance name %#v", name.InstanceName)
	}
	return newPlatformKey(instanceName, name.Platform)
}

// inFlightDeduplicationKey can be used as a key for maps to uniquely
// identify an action. This key is used for deduplicating requests for
// executing the same action.
type inFlightDeduplicationKey string

func newInFlightDeduplicationKey(digest *remoteexecution.Digest) inFlightDeduplicationKey {
	return inFlightDeduplicationKey(proto.MarshalTextString(digest))
}

// platformQueue is an actual build operations queue that contains a
// list of associated workers and operations that are queued to be
// executed. An InMemoryBuildQueue contains a platformQueue for every
// instance/platform for which one or more workers exist.
type platformQueue struct {
	platformKey platformKey

	inFlightDeduplicationMap map[inFlightDeduplicationKey]*task
	invocations              map[string]*invocation
	queuedInvocations        queuedInvocationsHeap
	queuedInvocationsWakeup  chan struct{}
	workers                  map[workerKey]*worker
	cleanupKey               cleanupKey

	drains       map[string]*buildqueuestate.DrainState
	drainsWakeup chan struct{}

	// Prometheus metrics.
	tasksQueuedTotal              prometheus.Counter
	tasksQueuedDurationSeconds    prometheus.Observer
	tasksExecutingDurationSeconds prometheus.ObserverVec
	tasksExecutingRetries         prometheus.ObserverVec
	tasksCompletedDurationSeconds prometheus.Observer

	workersCreatedTotal          prometheus.Counter
	workersRemovedIdleTotal      prometheus.Counter
	workersRemovedExecutingTotal prometheus.Counter
}

func newPlatformQueue(platformKey platformKey) *platformQueue {
	// Force creation of all metrics associated with
	// this platform queue to make recording rules work.
	instanceName := platformKey.instanceName.String()
	inMemoryBuildQueueTasksExecutingDurationSeconds.WithLabelValues(instanceName, platformKey.platform, "Success", "")
	inMemoryBuildQueueTasksExecutingRetries.WithLabelValues(instanceName, platformKey.platform, "Success", "")

	platformLabels := map[string]string{
		"instance_name": instanceName,
		"platform":      platformKey.platform,
	}
	return &platformQueue{
		platformKey: platformKey,

		inFlightDeduplicationMap: map[inFlightDeduplicationKey]*task{},
		invocations:              map[string]*invocation{},
		queuedInvocationsWakeup:  make(chan struct{}, 1),
		workers:                  map[workerKey]*worker{},

		drains:       map[string]*buildqueuestate.DrainState{},
		drainsWakeup: make(chan struct{}),

		tasksQueuedTotal:              inMemoryBuildQueueTasksQueuedTotal.WithLabelValues(instanceName, platformKey.platform),
		tasksQueuedDurationSeconds:    inMemoryBuildQueueTasksQueuedDurationSeconds.WithLabelValues(instanceName, platformKey.platform),
		tasksExecutingDurationSeconds: inMemoryBuildQueueTasksExecutingDurationSeconds.MustCurryWith(platformLabels),
		tasksExecutingRetries:         inMemoryBuildQueueTasksExecutingRetries.MustCurryWith(platformLabels),
		tasksCompletedDurationSeconds: inMemoryBuildQueueTasksCompletedDurationSeconds.WithLabelValues(instanceName, platformKey.platform),

		workersCreatedTotal:          inMemoryBuildQueueWorkersCreatedTotal.WithLabelValues(instanceName, platformKey.platform),
		workersRemovedIdleTotal:      inMemoryBuildQueueWorkersRemovedTotal.WithLabelValues(instanceName, platformKey.platform, "Idle"),
		workersRemovedExecutingTotal: inMemoryBuildQueueWorkersRemovedTotal.WithLabelValues(instanceName, platformKey.platform, "Executing"),
	}
}

func workerMatchesPattern(workerID, workerIDPattern map[string]string) bool {
	for key, value := range workerIDPattern {
		if workerID[key] != value {
			return false
		}
	}
	return true
}

func (w *worker) isDrained(pq *platformQueue, workerID map[string]string) bool {
	// Implicitly treat workers that are terminating as being
	// drained. This prevents tasks from getting interrupted.
	if w.terminating {
		return true
	}
	for _, drain := range pq.drains {
		if workerMatchesPattern(workerID, drain.WorkerIdPattern) {
			return true
		}
	}
	return false
}

// getNextTaskNonBlocking extracts the next task that should be assigned
// to a worker. Even when the worker is drained or no tasks are
// available, this function returns immediately.
func (w *worker) getNextTaskNonBlocking(bq *InMemoryBuildQueue, pq *platformQueue, workerID map[string]string) (*task, bool) {
	if !w.isDrained(pq, workerID) && pq.queuedInvocations.Len() > 0 {
		// Obtain the task that was associated with the highest
		// priority operation of the invocation that is most
		// favourable to run.
		t := pq.queuedInvocations[0].queuedOperations[0].operation.task
		t.startExecuting(bq)
		return t, true
	}
	return nil, false
}

// getNextTaskBlocking extracts the next task that should be assigned to
// a worker. This function blocks until either the worker is undrained
// and a task is available, or a configurable timeout has been reached.
// The timeout ensures that workers resynchronize periodically, ensuring
// that no stale workers are left behind indefinitely.
func (w *worker) getNextTaskBlocking(ctx context.Context, bq *InMemoryBuildQueue, pq *platformQueue, workerID map[string]string) (*task, error) {
	timeoutTimer, timeoutChannel := bq.clock.NewTimer(bq.configuration.GetIdleWorkerSynchronizationInterval())
	for {
		drained := w.isDrained(pq, workerID)
		drainsWakeup := pq.drainsWakeup
		bq.leave()

		if drained {
			select {
			case t := <-timeoutChannel:
				// Timeout has been reached.
				bq.enter(t)
				return nil, nil
			case <-ctx.Done():
				// Worker has canceled the request.
				timeoutTimer.Stop()
				bq.enter(bq.clock.Now())
				return nil, util.StatusFromContext(ctx)
			case <-drainsWakeup:
				// Worker might have been undrained.
				bq.enter(bq.clock.Now())
			}
		} else {
			select {
			case t := <-timeoutChannel:
				// Timeout has been reached.
				bq.enter(t)
				return nil, nil
			case <-ctx.Done():
				// Worker has canceled the request.
				timeoutTimer.Stop()
				bq.enter(bq.clock.Now())
				return nil, util.StatusFromContext(ctx)
			case <-pq.queuedInvocationsWakeup:
				// Work has appeared, but it may also have been
				// taken by another worker in the meantime.
				bq.enter(bq.clock.Now())
				o, ok := w.getNextTaskNonBlocking(bq, pq, workerID)
				if pq.queuedInvocations.Len() > 0 {
					pq.wakeupNextWorker()
				}
				if ok {
					timeoutTimer.Stop()
					return o, nil
				}
			}
		}
	}
}

// wakeupNextWorker is called after mutating the queued operations
// queue in such a way that at least one operation is present. This
// function ensures that a worker is woken up to take the operation.
func (pq *platformQueue) wakeupNextWorker() {
	select {
	case pq.queuedInvocationsWakeup <- struct{}{}:
	default:
	}
}

// removeStaleWorker is invoked when Synchronize() isn't being invoked
// by a worker quickly enough. It causes the worker to be removed from
// the InMemoryBuildQueue.
func (pq *platformQueue) removeStaleWorker(bq *InMemoryBuildQueue, workerKey workerKey, removalTime time.Time) {
	w := pq.workers[workerKey]
	if t := w.getCurrentTask(); t == nil {
		pq.workersRemovedIdleTotal.Inc()
	} else {
		pq.workersRemovedExecutingTotal.Inc()
		t.complete(bq, &remoteexecution.ExecuteResponse{
			Status: status.Newf(codes.Unavailable, "Worker %s disappeared while task was executing", workerKey).Proto(),
		})
	}
	delete(pq.workers, workerKey)

	// Trigger platform queue removal if necessary.
	if len(pq.workers) == 0 {
		bq.cleanupQueue.add(&pq.cleanupKey, removalTime.Add(bq.configuration.PlatformQueueWithNoWorkersTimeout), func() {
			pq.remove(bq)
		})
	}
}

// remove is invoked when Synchronize() isn't being invoked by any
// worker for a given platform quickly enough. It causes the platform
// queue and all associated queued operations to be removed from the
// InMemoryBuildQueue.
func (pq *platformQueue) remove(bq *InMemoryBuildQueue) {
	for pq.queuedInvocations.Len() > 0 {
		i := pq.queuedInvocations[pq.queuedInvocations.Len()-1]
		i.queuedOperations[i.queuedOperations.Len()-1].operation.task.complete(bq, &remoteexecution.ExecuteResponse{
			Status: status.New(codes.Unavailable, "Workers for this instance name and platform disappeared while task was queued").Proto(),
		})
	}
	delete(bq.platformQueues, pq.platformKey)
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

var priorityExponentiationBase = math.Pow(2.0, 0.01)

func (h queuedInvocationsHeap) Less(i, j int) bool {
	// To introduce fairness, we want to prefer scheduling
	// operations belonging to invocations that have the fewest
	// running operations. In addition to that, we still want to
	// respect priorities at the global level.
	//
	// Combine these two properties into a single score value
	// according to the following expression, where the invocation
	// with the lowest score is most favourable.
	//
	// S = (executingOperationsCount + 1) * b^priority
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
	oi, oj := h[i].queuedOperations[0], h[j].queuedOperations[0]
	ei, ej := float64(h[i].executingOperationsCount+1), float64(h[j].executingOperationsCount+1)
	var si, sj float64
	if pi, pj := oi.priority, oj.priority; pi < pj {
		// Invocation i has a higher priority. Give invocation j
		// a penalty based on the difference in priority.
		si, sj = ei, ej*math.Pow(priorityExponentiationBase, float64(pj-pi))
	} else if pi > pj {
		// Invocation j has a higher priority. Give invocation i
		// a penalty based on the difference in priority.
		si, sj = ei*math.Pow(priorityExponentiationBase, float64(pi-pj)), ej
	} else {
		// Both invocations have the same priority.
		si, sj = ei, ej
	}
	return si < sj || (si == sj && oi.operation.task.queuedBefore(oj.operation.task))
}

func (h queuedInvocationsHeap) Swap(i, j int) {
	if h[i].queueIndex != i || h[j].queueIndex != j {
		panic("Invalid queue indices")
	}
	h[i], h[j] = h[j], h[i]
	h[i].queueIndex = i
	h[j].queueIndex = j
}

func (h *queuedInvocationsHeap) Push(x interface{}) {
	e := x.(*invocation)
	e.queueIndex = len(*h)
	*h = append(*h, e)
}

func (h *queuedInvocationsHeap) Pop() interface{} {
	old := *h
	n := len(old)
	e := old[n-1]
	*h = old[0 : n-1]
	if e.queueIndex != n-1 {
		panic("Invalid queue index")
	}
	e.queueIndex = -1
	return e
}

type invocation struct {
	invocationID  string
	platformQueue *platformQueue
	queueIndex    int

	queuedOperations         queuedOperationsHeap
	executingOperationsCount uint32
}

func (i *invocation) getInvocationState(bq *InMemoryBuildQueue) *buildqueuestate.InvocationState {
	is := &buildqueuestate.InvocationState{
		Id:                       i.invocationID,
		QueuedOperationsCount:    uint32(i.queuedOperations.Len()),
		ExecutingOperationsCount: i.executingOperationsCount,
	}
	if is.QueuedOperationsCount > 0 {
		is.FirstQueuedOperation = i.queuedOperations[0].operation.getOperationState(bq)
		is.FirstQueuedOperation.PlatformQueueName = nil
		is.FirstQueuedOperation.InvocationId = ""
	}
	return is
}

type queuedOperationsEntry struct {
	priority  int32
	operation *operation
}

// decrementExecutingOperationsCount decrements the number of operations
// in the EXECUTING stage that are part of this invocation.
//
// Because the number of operations in the EXECUTING stage is used to
// prioritize tasks, this function may need to adjust the position of
// this invocation in the queued invocations heap. It may also need to
// remove the invocation entirely in case it no longer contains any
// operations.
func (i *invocation) decrementExecutingOperationsCount() {
	i.executingOperationsCount--
	pq := i.platformQueue
	if i.queuedOperations.Len() > 0 {
		heap.Fix(&pq.queuedInvocations, i.queueIndex)
	} else if i.executingOperationsCount == 0 {
		delete(pq.invocations, i.invocationID)
	}
}

// queuedOperationsHeap is a binary heap that stores queued operations,
// sorted by order in which they need to be assigned to workers.
type queuedOperationsHeap []queuedOperationsEntry

func (h queuedOperationsHeap) Len() int {
	return len(h)
}

func (h queuedOperationsHeap) Less(i, j int) bool {
	// Lexicographic order on priority and queued timestamp.
	return h[i].priority < h[j].priority || (h[i].priority == h[j].priority &&
		h[i].operation.task.queuedBefore(h[j].operation.task))
}

func (h queuedOperationsHeap) Swap(i, j int) {
	if h[i].operation.queueIndex != i || h[j].operation.queueIndex != j {
		panic("Invalid queue indices")
	}
	h[i], h[j] = h[j], h[i]
	h[i].operation.queueIndex = i
	h[j].operation.queueIndex = j
}

func (h *queuedOperationsHeap) Push(x interface{}) {
	e := x.(queuedOperationsEntry)
	e.operation.queueIndex = len(*h)
	*h = append(*h, e)
}

func (h *queuedOperationsHeap) Pop() interface{} {
	old := *h
	n := len(old)
	e := old[n-1]
	*h = old[0 : n-1]
	if e.operation.queueIndex != n-1 {
		panic("Invalid queue index")
	}
	e.operation.queueIndex = -1
	return e
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
	invocation *invocation
	name       string
	task       *task

	waiters uint

	// queueIndex contains the index at which the operation is
	// stored in the platformQueue's queuedOperations heap. When
	// negative, it means that the operation is no longer in the
	// queued stage (and thus either in the executing or completed
	// stage).
	queueIndex int

	cleanupKey cleanupKey
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
		if o.waiters == 0 {
			bq.cleanupQueue.add(&o.cleanupKey, bq.now.Add(bq.configuration.OperationWithNoWaitersTimeout), func() {
				o.remove(bq)
			})
		}
	}()

	t := o.task
	for {
		// Construct the longrunning.Operation that needs to be
		// sent back to the client.
		metadata, err := ptypes.MarshalAny(&remoteexecution.ExecuteOperationMetadata{
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
			response, err := ptypes.MarshalAny(t.executeResponse)
			if err != nil {
				return util.StatusWrap(err, "Failed to marshal execute response")
			}
			operation.Result = &longrunning.Operation_Response{Response: response}
		}
		completionWakeup := t.completionWakeup
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
		case <-completionWakeup:
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
//
// This function returns true if this was the last queued operation to
// be removed from the invocation.
func (o *operation) removeQueuedFromInvocation() bool {
	i := o.invocation
	heap.Remove(&i.queuedOperations, o.queueIndex)

	pq := i.platformQueue
	if i.queuedOperations.Len() == 0 {
		// Associated invocation no longer has any queued
		// operations. Remove the invocation from the queue.
		heap.Remove(&pq.queuedInvocations, i.queueIndex)
		return true
	}

	// Associated invocation still has one or more queued
	// operations, though we may need to schedule operations from
	// other invocations first. Move the invocation down the heap.
	heap.Fix(&pq.queuedInvocations, i.queueIndex)
	return false
}

func (o *operation) remove(bq *InMemoryBuildQueue) {
	delete(bq.operationsNameMap, o.name)

	t := o.task
	if len(t.operations) == 1 {
		// Forcefully terminate the associated task if it won't
		// have any other operations associated with it.
		t.complete(bq, &remoteexecution.ExecuteResponse{
			Status: status.New(codes.Canceled, "Task no longer has any waiting clients").Proto(),
		})
		t.registerCompletedStageFinished(bq)
	} else {
		// The underlying task is shared with other operations.
		// Remove the operation, while leaving the task intact.
		i := o.invocation
		switch t.getStage() {
		case remoteexecution.ExecutionStage_QUEUED:
			if o.removeQueuedFromInvocation() && i.executingOperationsCount == 0 {
				pq := i.platformQueue
				delete(pq.invocations, i.invocationID)
			}
		case remoteexecution.ExecutionStage_EXECUTING:
			i.decrementExecutingOperationsCount()
		}
	}
	delete(t.operations, o.invocation)
}

func (o *operation) getOperationState(bq *InMemoryBuildQueue) *buildqueuestate.OperationState {
	i := o.invocation
	t := o.task
	s := &buildqueuestate.OperationState{
		Name:              o.name,
		PlatformQueueName: i.platformQueue.platformKey.getPlatformQueueName(),
		InvocationId:      i.invocationID,
		QueuedTimestamp:   t.desiredState.QueuedTimestamp,
		ActionDigest:      t.desiredState.ActionDigest,
		Argv0:             t.argv0,
		Timeout:           bq.cleanupQueue.getTimestamp(o.cleanupKey),
	}
	switch t.getStage() {
	case remoteexecution.ExecutionStage_QUEUED:
		s.Stage = &buildqueuestate.OperationState_Queued_{
			Queued: &buildqueuestate.OperationState_Queued{
				Priority: i.queuedOperations[o.queueIndex].priority,
			},
		}
	case remoteexecution.ExecutionStage_EXECUTING:
		s.Stage = &buildqueuestate.OperationState_Executing{
			Executing: &empty.Empty{},
		}
	case remoteexecution.ExecutionStage_COMPLETED:
		s.Stage = &buildqueuestate.OperationState_Completed{
			Completed: t.executeResponse,
		}
	}
	return s
}

// Task state that is created for every piece of work that needs to be
// executed by a worker. Tasks are associated with one or more
// operations. In the general case a task has one operation, but there
// may be multiple in case multiple clients request that the same action
// is built and deduplication is performed.
type task struct {
	operations    map[*invocation]*operation
	platformQueue *platformQueue
	desiredState  remoteworker.DesiredState_Executing

	// This field is not strictly necessary to implement the
	// BuildQueue and OperationQueueServer interfaces. It needs to
	// be present to implement BuildQueueState.
	argv0 string

	// currentStageStartTime is used by register*StageFinished() to
	// obtain Prometheus metrics.
	currentStageStartTime time.Time

	// retryCount specifies how many additional times the operation
	// was provided to the worker to which it was allocated. This
	// counter may be non-zero in case of network flakiness or
	// worker crashes.
	retryCount int

	executeResponse  *remoteexecution.ExecuteResponse
	completionWakeup chan struct{}
}

// getStage returns whether the task is in the queued, executing or
// completed stage.
func (t *task) getStage() remoteexecution.ExecutionStage_Value {
	if t.executeResponse != nil {
		return remoteexecution.ExecutionStage_COMPLETED
	}
	for _, o := range t.operations {
		if o.queueIndex < 0 {
			return remoteexecution.ExecutionStage_EXECUTING
		}
		return remoteexecution.ExecutionStage_QUEUED
	}
	panic("Task doesn't have any operations associated with it. It should have been in the completed stage.")
}

func (t *task) queuedBefore(other *task) bool {
	ti := t.desiredState.QueuedTimestamp
	tj := other.desiredState.QueuedTimestamp
	return ti.Seconds < tj.Seconds || (ti.Seconds == tj.Seconds && ti.Nanos < tj.Nanos)
}

// startExecuting transitions a task and all of its associated
// operations from the queued to the executing stage.
func (t *task) startExecuting(bq *InMemoryBuildQueue) {
	for i, o := range t.operations {
		i.executingOperationsCount++
		o.removeQueuedFromInvocation()
	}
	t.registerQueuedStageFinished(bq)
}

// Complete execution of the task by registering the execution response.
// This function wakes up any clients waiting on the task to complete.
func (t *task) complete(bq *InMemoryBuildQueue, executeResponse *remoteexecution.ExecuteResponse) {
	switch t.getStage() {
	case remoteexecution.ExecutionStage_QUEUED:
		// The task isn't even executing. First transition it to
		// the executing stage, so that it can be completed
		// safely.
		t.startExecuting(bq)
		fallthrough
	case remoteexecution.ExecutionStage_EXECUTING:
		// Mark the task as completed.
		pq := t.platformQueue
		delete(pq.inFlightDeduplicationMap, newInFlightDeduplicationKey(t.desiredState.ActionDigest))
		t.executeResponse = executeResponse
		close(t.completionWakeup)

		for i := range t.operations {
			i.decrementExecutingOperationsCount()
		}

		// Scrub data from the task that are no longer needed
		// after completion. This reduces memory usage
		// significantly. Keep the Action digest, so that
		// there's still a way to figure out what the task was.
		t.desiredState.Action = nil
		t.desiredState.Command = nil
		t.completionWakeup = nil

		result, grpcCode := getResultAndGRPCCodeFromExecuteResponse(executeResponse)
		t.registerExecutingStageFinished(bq, result, grpcCode)
	}
}

// registerQueuedStageFinished updates Prometheus metrics related to
// task finishing the QUEUED stage.
func (t *task) registerQueuedStageFinished(bq *InMemoryBuildQueue) {
	t.platformQueue.tasksQueuedDurationSeconds.Observe(bq.now.Sub(t.currentStageStartTime).Seconds())
	t.currentStageStartTime = bq.now
}

// registerQueuedStageFinished updates Prometheus metrics related to
// task finishing the EXECUTING stage.
func (t *task) registerExecutingStageFinished(bq *InMemoryBuildQueue, result, grpcCode string) {
	t.platformQueue.tasksExecutingDurationSeconds.WithLabelValues(result, grpcCode).Observe(bq.now.Sub(t.currentStageStartTime).Seconds())
	t.platformQueue.tasksExecutingRetries.WithLabelValues(result, grpcCode).Observe(float64(t.retryCount))
	t.currentStageStartTime = bq.now
}

// registerQueuedStageFinished updates Prometheus metrics related to
// task finishing the COMPLETED stage, meaning the task got removed.
func (t *task) registerCompletedStageFinished(bq *InMemoryBuildQueue) {
	t.platformQueue.tasksCompletedDurationSeconds.Observe(bq.now.Sub(t.currentStageStartTime).Seconds())
	t.currentStageStartTime = bq.now
}

// worker state for every node capable of executing operations.
type worker struct {
	currentTask *task
	cleanupKey  cleanupKey
	terminating bool
}

func (w *worker) getCurrentTask() *task {
	if t := w.currentTask; t != nil && t.executeResponse == nil {
		return t
	}
	return nil
}

// startTask assigns a task to the worker, returning a synchronization
// response that instructs the worker to start executing it.
func (w *worker) startTask(bq *InMemoryBuildQueue, pq *platformQueue, t *task) *remoteworker.SynchronizeResponse {
	w.currentTask = t
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
func (w *worker) getNextTask(ctx context.Context, bq *InMemoryBuildQueue, pq *platformQueue, workerID map[string]string, preferBeingIdle bool) (*remoteworker.SynchronizeResponse, error) {
	if preferBeingIdle {
		// The worker wants to terminate or is experiencing some
		// issues. Explicitly instruct the worker to go idle, so
		// that it knows it can hold off synchronizing.
		return bq.getIdleSynchronizeResponse(), nil
	}

	if t, ok := w.getNextTaskNonBlocking(bq, pq, workerID); ok {
		return w.startTask(bq, pq, t), nil
	}

	if ctx == nil {
		// We shouldn't block, as the worker is currently doing
		// some work that it shouldn't be doing. Request that
		// the worker goes idle immediately. It will
		// resynchronize as soon as it's done terminating its
		// current build action.
		return bq.getIdleSynchronizeResponse(), nil
	}

	t, err := w.getNextTaskBlocking(ctx, bq, pq, workerID)
	if t == nil {
		// There is no work available, even after waiting for
		// some time. Allow the worker to switch back to the
		// idle state, so that it knows it doesn't need to hold
		// on to any previous execute response and may terminate
		// safely.
		return bq.getIdleSynchronizeResponse(), err
	}
	return w.startTask(bq, pq, t), nil
}

// getCurrentOrNextTask either returns a synchronization response that
// instructs the worker to run the task it should be running. When the
// worker has no task assigned to it, it attempts to request a task from
// the queue.
func (w *worker) getCurrentOrNextTask(ctx context.Context, bq *InMemoryBuildQueue, pq *platformQueue, workerID map[string]string, preferBeingIdle bool) (*remoteworker.SynchronizeResponse, error) {
	if t := w.getCurrentTask(); t != nil {
		if t.retryCount >= bq.configuration.WorkerTaskRetryCount {
			t.complete(bq, &remoteexecution.ExecuteResponse{
				Status: status.Newf(
					codes.Internal,
					"Attempted to execute task %d times, but it never completed. This task may cause worker %s to crash.",
					t.retryCount+1,
					newWorkerKey(workerID)).Proto(),
			})
		} else {
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
	}
	return w.getNextTask(ctx, bq, pq, workerID, preferBeingIdle)
}

// isRunningCorrectTask determines whether the worker is actually
// running the task the scheduler instructed it to run previously.
func (w *worker) isRunningCorrectTask(actionDigest *remoteexecution.Digest) bool {
	t := w.getCurrentTask()
	if t == nil {
		return false
	}
	desiredDigest := t.desiredState.ActionDigest
	return proto.Equal(actionDigest, desiredDigest)
}

// updateTask processes execution status updates from the worker that do
// not equal the 'completed' state.
func (w *worker) updateTask(bq *InMemoryBuildQueue, pq *platformQueue, workerID map[string]string, actionDigest *remoteexecution.Digest, preferBeingIdle bool) (*remoteworker.SynchronizeResponse, error) {
	if !w.isRunningCorrectTask(actionDigest) {
		return w.getCurrentOrNextTask(nil, bq, pq, workerID, preferBeingIdle)
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
func (w *worker) completeTask(ctx context.Context, bq *InMemoryBuildQueue, pq *platformQueue, workerID map[string]string, actionDigest *remoteexecution.Digest, executeResponse *remoteexecution.ExecuteResponse, preferBeingIdle bool) (*remoteworker.SynchronizeResponse, error) {
	if !w.isRunningCorrectTask(actionDigest) {
		return w.getCurrentOrNextTask(ctx, bq, pq, workerID, preferBeingIdle)
	}
	w.getCurrentTask().complete(bq, executeResponse)
	return w.getNextTask(ctx, bq, pq, workerID, preferBeingIdle)
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
	*h = old[0 : n-1]
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

func (q *cleanupQueue) getTimestamp(key cleanupKey) *timestamp.Timestamp {
	if key == 0 {
		return nil
	}
	t, err := ptypes.TimestampProto(q.heap[key-1].timestamp)
	if err != nil {
		panic(fmt.Sprintf("Failed to compute cleanup key timestamp: %s", err))
	}
	return t
}
