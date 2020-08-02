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
	"github.com/buildbarn/bb-remote-execution/pkg/proto/remoteworker"
	"github.com/buildbarn/bb-storage/pkg/blobstore"
	"github.com/buildbarn/bb-storage/pkg/builder"
	"github.com/buildbarn/bb-storage/pkg/clock"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/util"
	"github.com/golang/protobuf/jsonpb"
	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes"
	"github.com/golang/protobuf/ptypes/empty"
	"github.com/golang/protobuf/ptypes/timestamp"
	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"

	"google.golang.org/genproto/googleapis/longrunning"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"go.opencensus.io/trace"
	"go.opencensus.io/trace/propagation"
)

var (
	inMemoryBuildQueuePrometheusMetrics sync.Once

	inMemoryBuildQueueOperationsQueuedTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "buildbarn",
			Subsystem: "builder",
			Name:      "in_memory_build_queue_operations_queued_total",
			Help:      "Number of operations created through Execute().",
		},
		[]string{"instance_name", "platform"})
	inMemoryBuildQueueOperationsQueuedDurationSeconds = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "buildbarn",
			Subsystem: "builder",
			Name:      "in_memory_build_queue_operations_queued_duration_seconds",
			Help:      "Time in seconds that operations were queued before executing.",
			Buckets:   util.DecimalExponentialBuckets(-3, 6, 2),
		},
		[]string{"instance_name", "platform"})
	inMemoryBuildQueueOperationsExecutingDurationSeconds = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "buildbarn",
			Subsystem: "builder",
			Name:      "in_memory_build_queue_operations_executing_duration_seconds",
			Help:      "Time in seconds that operations were executing before completing.",
			Buckets:   util.DecimalExponentialBuckets(-3, 6, 2),
		},
		[]string{"instance_name", "platform", "result", "grpc_code"})
	inMemoryBuildQueueOperationsExecutingRetries = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "buildbarn",
			Subsystem: "builder",
			Name:      "in_memory_build_queue_operations_executing_retries",
			Help:      "Number of times that operations were retried before completing.",
			Buckets:   prometheus.LinearBuckets(0, 1, 11),
		},
		[]string{"instance_name", "platform", "result", "grpc_code"})
	inMemoryBuildQueueOperationsCompletedDurationSeconds = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "buildbarn",
			Subsystem: "builder",
			Name:      "in_memory_build_queue_operations_completed_duration_seconds",
			Help:      "Time in seconds that operations were completed before being removed.",
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
	// and WaitExecution() should stream updates for an operation to
	// the client.
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

	// WorkerOperationRetryCount specifies how many times a worker
	// may redundantly request that a single operation is started.
	// By limiting this, we can prevent a single operation from
	// crash-looping a worker indefinitely.
	WorkerOperationRetryCount int

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
		prometheus.MustRegister(inMemoryBuildQueueOperationsQueuedTotal)
		prometheus.MustRegister(inMemoryBuildQueueOperationsQueuedDurationSeconds)
		prometheus.MustRegister(inMemoryBuildQueueOperationsExecutingDurationSeconds)
		prometheus.MustRegister(inMemoryBuildQueueOperationsExecutingRetries)
		prometheus.MustRegister(inMemoryBuildQueueOperationsCompletedDurationSeconds)

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

var _ builder.BuildQueue = (*InMemoryBuildQueue)(nil)
var _ remoteworker.OperationQueueServer = (*InMemoryBuildQueue)(nil)
var _ BuildQueueStateProvider = (*InMemoryBuildQueue)(nil)

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

// Execute an action by scheduling it in the build queue. This call
// blocks until the action is completed.
func (bq *InMemoryBuildQueue) Execute(in *remoteexecution.ExecuteRequest, out remoteexecution.Execution_ExecuteServer) error {
	// Fetch the action and command corresponding to the execute
	// request. Ideally, a scheduler is be oblivious of what these
	// look like, if it weren't for the fact that Action.DoNotCache
	// and Command.Platform are used for scheduling decisions.
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

	// Create a new operation in case none exist against which this
	// request may be deduplicated.
	inFlightDeduplicationKey := newInFlightDeduplicationKey(in.ActionDigest)
	o, ok := pq.inFlightDeduplicationMap[inFlightDeduplicationKey]
	if !ok {
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

		o = &operation{
			platformQueue: pq,
			desiredState:  desiredState,

			name:         uuid.Must(bq.uuidGenerator()).String(),
			instanceName: instanceName,
			argv0:        argv0,

			currentStageStartTime: bq.now,

			completionWakeup: make(chan struct{}),
		}
		bq.operationsNameMap[o.name] = o
		if !action.DoNotCache {
			pq.inFlightDeduplicationMap[inFlightDeduplicationKey] = o
		}
		priority := int32(0)
		if in.ExecutionPolicy != nil {
			priority = in.ExecutionPolicy.Priority
		}
		heap.Push(&pq.queuedOperations, queuedOperationsEntry{
			priority:  priority,
			operation: o,
		})
		pq.wakeupNextWorker()
		pq.operationsQueuedTotal.Inc()
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
		return w.getCurrentOrNextOperation(ctx, bq, pq, request.WorkerId)
	case *remoteworker.CurrentState_Executing_:
		executing := workerState.Executing
		if executing.ActionDigest == nil {
			return nil, status.Error(codes.InvalidArgument, "Worker is executing, but provided no action digest")
		}
		switch executionState := executing.ExecutionState.(type) {
		case *remoteworker.CurrentState_Executing_Completed:
			return w.completeOperation(ctx, bq, pq, request.WorkerId, executing.ActionDigest, executionState.Completed)
		default:
			return w.updateOperation(bq, pq, request.WorkerId, executing.ActionDigest)
		}
	default:
		return nil, status.Error(codes.InvalidArgument, "Worker provided an unknown current state")
	}
}

// GetBuildQueueState returns global state of the InMemoryBuildQueue
// that may be displayed by the main bb_scheduler information page.
func (bq *InMemoryBuildQueue) GetBuildQueueState() *BuildQueueState {
	bq.enter(bq.clock.Now())
	defer bq.leave()

	// Obtain platform queue IDs in sorted order.
	var platformKeyList platformKeyList
	for platformKey := range bq.platformQueues {
		platformKeyList = append(platformKeyList, platformKey)
	}
	sort.Sort(platformKeyList)

	// Extract status.
	state := &BuildQueueState{
		OperationsCount: len(bq.operationsNameMap),
	}
	for _, platformKey := range platformKeyList {
		pq := bq.platformQueues[platformKey]
		executingWorkersCount := 0
		for _, w := range pq.workers {
			if w.getCurrentOperation() != nil {
				executingWorkersCount++
			}
		}
		state.PlatformQueues = append(state.PlatformQueues, PlatformQueueState{
			InstanceName:          platformKey.instanceName,
			Platform:              *platformKey.getPlatform(),
			Timeout:               bq.cleanupQueue.getTimestamp(pq.cleanupKey),
			QueuedOperationsCount: len(pq.queuedOperations),
			WorkersCount:          len(pq.workers),
			ExecutingWorkersCount: executingWorkersCount,
			DrainsCount:           len(pq.drains),
		})
	}
	return state
}

// GetDetailedOperationState returns detailed information about a single
// operation identified by name.
func (bq *InMemoryBuildQueue) GetDetailedOperationState(name string) (*DetailedOperationState, bool) {
	bq.enter(bq.clock.Now())
	defer bq.leave()

	o, ok := bq.operationsNameMap[name]
	if !ok {
		return nil, false
	}
	return o.getDetailedOperationState(bq), true
}

// getPaginationInfo uses binary searching to determine which
// information should be returned by InMemoryBuildQueue's List*()
// operations.
func getPaginationInfo(n int, pageSize int, f func(int) bool) PaginationInfo {
	startIndex := sort.Search(n, f)
	endIndex := startIndex + pageSize
	if endIndex > n {
		endIndex = n
	}
	return PaginationInfo{
		StartIndex:   startIndex,
		EndIndex:     endIndex,
		TotalEntries: n,
	}
}

// KillOperation requests that an operation that is currently QUEUED or
// EXECUTING is moved the COMPLETED stage immediately. The next time any
// worker associated with the operation contacts the scheduler, it is
// requested to stop executing the operation.
func (bq *InMemoryBuildQueue) KillOperation(name string) bool {
	bq.enter(bq.clock.Now())
	defer bq.leave()

	o, ok := bq.operationsNameMap[name]
	if !ok {
		return false
	}
	o.complete(bq, &remoteexecution.ExecuteResponse{
		Status: status.New(codes.Unavailable, "Operation was killed administratively").Proto(),
	})
	return true
}

// ListDetailedOperationState returns detailed information about all of
// the operations tracked by the InMemoryBuildQueue.
func (bq *InMemoryBuildQueue) ListDetailedOperationState(pageSize int, startAfterOperation *string) ([]DetailedOperationState, PaginationInfo) {
	bq.enter(bq.clock.Now())
	defer bq.leave()

	// Obtain operation names in sorted order.
	nameList := make([]string, 0, len(bq.operationsNameMap))
	for name := range bq.operationsNameMap {
		nameList = append(nameList, name)
	}
	sort.Strings(nameList)
	paginationInfo := getPaginationInfo(len(nameList), pageSize, func(i int) bool {
		return startAfterOperation == nil || nameList[i] > *startAfterOperation
	})

	// Extract status.
	nameListRegion := nameList[paginationInfo.StartIndex:paginationInfo.EndIndex]
	results := make([]DetailedOperationState, 0, len(nameListRegion))
	for _, name := range nameListRegion {
		o := bq.operationsNameMap[name]
		results = append(results, *o.getDetailedOperationState(bq))
	}
	return results, paginationInfo
}

// ListQueuedOperationState returns properties of all queued
// operations contained within a given platform queue.
func (bq *InMemoryBuildQueue) ListQueuedOperationState(instanceName digest.InstanceName, platform *remoteexecution.Platform, pageSize int, startAfterPriority *int32, startAfterQueuedTimestamp *time.Time) ([]QueuedOperationState, PaginationInfo, error) {
	platformKey, err := newPlatformKey(instanceName, platform)
	if err != nil {
		return nil, PaginationInfo{}, err
	}

	bq.enter(bq.clock.Now())
	defer bq.leave()

	pq, ok := bq.platformQueues[platformKey]
	if !ok {
		return nil, PaginationInfo{}, status.Error(codes.NotFound, "No workers for this instance and platform exist")
	}

	// As every sorted list is also a valid binary heap, simply sort
	// the queued operations list prior to emitting it.
	sort.Sort(pq.queuedOperations)
	paginationInfo := getPaginationInfo(len(pq.queuedOperations), pageSize, func(i int) bool {
		e := pq.queuedOperations[i]
		if startAfterPriority == nil || startAfterQueuedTimestamp == nil || e.priority > *startAfterPriority {
			return true
		}
		if e.priority < *startAfterPriority {
			return false
		}
		queuedTimestamp, err := ptypes.Timestamp(e.operation.desiredState.QueuedTimestamp)
		if err != nil {
			panic(fmt.Sprintf("Failed to parse previously generated timestamp: %s", err))
		}
		return queuedTimestamp.After(*startAfterQueuedTimestamp)
	})

	queuedOperationsRegion := pq.queuedOperations[paginationInfo.StartIndex:paginationInfo.EndIndex]
	results := make([]QueuedOperationState, 0, len(queuedOperationsRegion))
	for _, entry := range queuedOperationsRegion {
		results = append(results, QueuedOperationState{
			BasicOperationState: *entry.operation.getBasicOperationState(bq),
			Priority:            entry.priority,
		})
	}
	return results, paginationInfo, nil
}

// ListWorkerState returns basic properties of all workers for a given
// platform queue.
func (bq *InMemoryBuildQueue) ListWorkerState(instanceName digest.InstanceName, platform *remoteexecution.Platform, justExecutingWorkers bool, pageSize int, startAfterWorkerID map[string]string) ([]WorkerState, PaginationInfo, error) {
	platformKey, err := newPlatformKey(instanceName, platform)
	if err != nil {
		return nil, PaginationInfo{}, err
	}
	var startAfterWorkerKey *string
	if startAfterWorkerID != nil {
		workerKey := string(newWorkerKey(startAfterWorkerID))
		startAfterWorkerKey = &workerKey
	}

	bq.enter(bq.clock.Now())
	defer bq.leave()

	pq, ok := bq.platformQueues[platformKey]
	if !ok {
		return nil, PaginationInfo{}, status.Error(codes.NotFound, "No workers for this instance and platform exist")
	}

	// Obtain IDs of all workers in sorted order.
	var keyList []string
	for workerKey, w := range pq.workers {
		if !justExecutingWorkers || w.getCurrentOperation() != nil {
			keyList = append(keyList, string(workerKey))
		}
	}
	sort.Strings(keyList)
	paginationInfo := getPaginationInfo(len(keyList), pageSize, func(i int) bool {
		return startAfterWorkerKey == nil || keyList[i] > *startAfterWorkerKey
	})

	// Extract status.
	keyListRegion := keyList[paginationInfo.StartIndex:paginationInfo.EndIndex]
	results := make([]WorkerState, 0, len(keyListRegion))
	for _, key := range keyListRegion {
		workerKey := workerKey(key)
		w := pq.workers[workerKey]
		var currentOperation *BasicOperationState
		if o := w.getCurrentOperation(); o != nil {
			currentOperation = o.getBasicOperationState(bq)
		}
		workerID := workerKey.getWorkerID()
		results = append(results, WorkerState{
			WorkerID:         workerID,
			Timeout:          bq.cleanupQueue.getTimestamp(w.cleanupKey),
			CurrentOperation: currentOperation,
			Drained:          w.isDrained(pq, workerID),
		})
	}
	return results, paginationInfo, nil
}

// ListDrainState returns a list of all the drains that are present
// within a given platform queue.
func (bq *InMemoryBuildQueue) ListDrainState(instanceName digest.InstanceName, platform *remoteexecution.Platform) ([]DrainState, error) {
	platformKey, err := newPlatformKey(instanceName, platform)
	if err != nil {
		return nil, err
	}

	bq.enter(bq.clock.Now())
	defer bq.leave()

	pq, ok := bq.platformQueues[platformKey]
	if !ok {
		return nil, status.Error(codes.NotFound, "No workers for this instance and platform exist")
	}

	// Obtain IDs of all drains in sorted order.
	keyList := make([]string, 0, len(pq.drains))
	for drainKey := range pq.drains {
		keyList = append(keyList, drainKey)
	}
	sort.Strings(keyList)

	// Extract drains.
	results := make([]DrainState, 0, len(keyList))
	for _, key := range keyList {
		results = append(results, pq.drains[key])
	}
	return results, nil
}

func (bq *InMemoryBuildQueue) modifyDrain(instanceName digest.InstanceName, platform *remoteexecution.Platform, workerIDPattern map[string]string, modifyFunc func(pq *platformQueue, drainKey string)) error {
	platformKey, err := newPlatformKey(instanceName, platform)
	if err != nil {
		return err
	}
	drainKey, err := json.Marshal(workerIDPattern)
	if err != nil {
		return util.StatusWrapWithCode(err, codes.InvalidArgument, "Failed to marshal worker ID pattern")
	}

	bq.enter(bq.clock.Now())
	defer bq.leave()

	pq, ok := bq.platformQueues[platformKey]
	if !ok {
		return status.Error(codes.NotFound, "No workers for this instance and platform exist")
	}
	modifyFunc(pq, string(drainKey))
	close(pq.drainsWakeup)
	pq.drainsWakeup = make(chan struct{})
	return nil
}

// AddDrain inserts a new drain into the list of drains currently
// tracked by the platform queue.
func (bq *InMemoryBuildQueue) AddDrain(instanceName digest.InstanceName, platform *remoteexecution.Platform, workerIDPattern map[string]string) error {
	return bq.modifyDrain(instanceName, platform, workerIDPattern, func(pq *platformQueue, drainKey string) {
		pq.drains[drainKey] = DrainState{
			WorkerIDPattern:   workerIDPattern,
			CreationTimestamp: bq.now,
		}
	})
}

// RemoveDrain removes a drain from the list of drains currently tracked
// by the platform queue.
func (bq *InMemoryBuildQueue) RemoveDrain(instanceName digest.InstanceName, platform *remoteexecution.Platform, workerIDPattern map[string]string) error {
	return bq.modifyDrain(instanceName, platform, workerIDPattern, func(pq *platformQueue, drainKey string) {
		delete(pq.drains, drainKey)
	})
}

// MarkTerminatingAndWait can be used to indicate that workers are going
// to be terminated in the nearby future. This function will block until
// any operations running on the workers complete, thereby allowing the
// workers to be terminated without interrupting operations.
func (bq *InMemoryBuildQueue) MarkTerminatingAndWait(workerIDPattern map[string]string) {
	var completionWakeups []chan struct{}
	bq.enter(bq.clock.Now())
	for _, pq := range bq.platformQueues {
		for workerKey, w := range pq.workers {
			if workerMatchesPattern(workerKey.getWorkerID(), workerIDPattern) {
				w.terminating = true
				if o := w.getCurrentOperation(); o != nil {
					completionWakeups = append(completionWakeups, o.completionWakeup)
				}
			}
		}
	}
	bq.leave()

	for _, completionWakeup := range completionWakeups {
		<-completionWakeup
	}
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

// getPlatform reobtains the Platform message that was used to construct
// the platformKey. As this is only used infrequently, we don't bother
// keeping the unmarshalled Platform message around to preserve memory
// usage.
func (k *platformKey) getPlatform() *remoteexecution.Platform {
	var platform remoteexecution.Platform
	if err := jsonpb.UnmarshalString(k.platform, &platform); err != nil {
		panic(fmt.Sprintf("Failed to unmarshal previously marshalled platform: %s", err))
	}
	return &platform
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
		util.StatusWrapWithCode(err, codes.InvalidArgument, "Failed to marshal platform message")
	}
	return platformKey{
		instanceName: instanceName,
		platform:     platformString,
	}, nil
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

	inFlightDeduplicationMap map[inFlightDeduplicationKey]*operation
	queuedOperations         queuedOperationsHeap
	queuedOperationsWakeup   chan struct{}
	workers                  map[workerKey]*worker
	cleanupKey               cleanupKey

	drains       map[string]DrainState
	drainsWakeup chan struct{}

	// Prometheus metrics.
	operationsQueuedTotal              prometheus.Counter
	operationsQueuedDurationSeconds    prometheus.Observer
	operationsExecutingDurationSeconds prometheus.ObserverVec
	operationsExecutingRetries         prometheus.ObserverVec
	operationsCompletedDurationSeconds prometheus.Observer

	workersCreatedTotal          prometheus.Counter
	workersRemovedIdleTotal      prometheus.Counter
	workersRemovedExecutingTotal prometheus.Counter
}

func newPlatformQueue(platformKey platformKey) *platformQueue {
	// Force creation of all metrics associated with
	// this platform queue to make recording rules work.
	instanceName := platformKey.instanceName.String()
	inMemoryBuildQueueOperationsExecutingDurationSeconds.WithLabelValues(instanceName, platformKey.platform, "Success", "")
	inMemoryBuildQueueOperationsExecutingRetries.WithLabelValues(instanceName, platformKey.platform, "Success", "")

	platformLabels := map[string]string{
		"instance_name": instanceName,
		"platform":      platformKey.platform,
	}
	return &platformQueue{
		platformKey: platformKey,

		inFlightDeduplicationMap: map[inFlightDeduplicationKey]*operation{},
		workers:                  map[workerKey]*worker{},
		queuedOperationsWakeup:   make(chan struct{}, 1),

		drains:       map[string]DrainState{},
		drainsWakeup: make(chan struct{}),

		operationsQueuedTotal:              inMemoryBuildQueueOperationsQueuedTotal.WithLabelValues(instanceName, platformKey.platform),
		operationsQueuedDurationSeconds:    inMemoryBuildQueueOperationsQueuedDurationSeconds.WithLabelValues(instanceName, platformKey.platform),
		operationsExecutingDurationSeconds: inMemoryBuildQueueOperationsExecutingDurationSeconds.MustCurryWith(platformLabels),
		operationsExecutingRetries:         inMemoryBuildQueueOperationsExecutingRetries.MustCurryWith(platformLabels),
		operationsCompletedDurationSeconds: inMemoryBuildQueueOperationsCompletedDurationSeconds.WithLabelValues(instanceName, platformKey.platform),

		workersCreatedTotal:          inMemoryBuildQueueWorkersCreatedTotal.WithLabelValues(instanceName, platformKey.platform),
		workersRemovedIdleTotal:      inMemoryBuildQueueWorkersRemovedTotal.WithLabelValues(instanceName, platformKey.platform, "Idle"),
		workersRemovedExecutingTotal: inMemoryBuildQueueWorkersRemovedTotal.WithLabelValues(instanceName, platformKey.platform, "Executing"),
	}
}

func workerMatchesPattern(workerID map[string]string, workerIDPattern map[string]string) bool {
	for key, value := range workerIDPattern {
		if workerID[key] != value {
			return false
		}
	}
	return true
}

func (w *worker) isDrained(pq *platformQueue, workerID map[string]string) bool {
	// Implicitly treat workers that are terminating as being
	// drained. This prevents operations from getting interrupted.
	if w.terminating {
		return true
	}
	for _, drain := range pq.drains {
		if workerMatchesPattern(workerID, drain.WorkerIDPattern) {
			return true
		}
	}
	return false
}

// getNextOperationNonBlocking extracts the next operation that should
// be assigned to a worker. Even when the worker is drained or no
// operations are available, this function returns immediately.
func (w *worker) getNextOperationNonBlocking(bq *InMemoryBuildQueue, pq *platformQueue, workerID map[string]string) (*operation, bool) {
	if !w.isDrained(pq, workerID) && pq.queuedOperations.Len() > 0 {
		entry := heap.Pop(&pq.queuedOperations).(queuedOperationsEntry)
		return entry.operation, true
	}
	return nil, false
}

// getNextOperationBlocking extracts the next operation that should be
// assigned to a worker. This function blocks until either the worker is
// undrained and an operation is available, or a configurable timeout
// has been reached. The timeout ensures that workers resynchronize
// periodically, ensuring that no stale workers are left behind
// indefinitely.
func (w *worker) getNextOperationBlocking(ctx context.Context, bq *InMemoryBuildQueue, pq *platformQueue, workerID map[string]string) (*operation, error) {
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
			case <-pq.queuedOperationsWakeup:
				// Work has appeared, but it may also have been
				// taken by another worker in the meantime.
				bq.enter(bq.clock.Now())
				o, ok := w.getNextOperationNonBlocking(bq, pq, workerID)
				if pq.queuedOperations.Len() > 0 {
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
	case pq.queuedOperationsWakeup <- struct{}{}:
	default:
	}
}

// removeStaleWorker is invoked when Synchronize() isn't being invoked
// by a worker quickly enough. It causes the worker to be removed from
// the InMemoryBuildQueue.
func (pq *platformQueue) removeStaleWorker(bq *InMemoryBuildQueue, workerKey workerKey, removalTime time.Time) {
	w := pq.workers[workerKey]
	if o := w.getCurrentOperation(); o == nil {
		pq.workersRemovedIdleTotal.Inc()
	} else {
		pq.workersRemovedExecutingTotal.Inc()
		o.complete(bq, &remoteexecution.ExecuteResponse{
			Status: status.Newf(codes.Unavailable, "Worker %s disappeared while operation was executing", workerKey).Proto(),
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
	for pq.queuedOperations.Len() > 0 {
		pq.queuedOperations[pq.queuedOperations.Len()-1].operation.complete(bq, &remoteexecution.ExecuteResponse{
			Status: status.New(codes.Unavailable, "Workers for this instance and platform disappeared while operation was queued").Proto(),
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

type queuedOperationsEntry struct {
	priority  int32
	operation *operation
}

// queuedOperationsHeap is a binary heap that stores queued operations,
// sorted by order in which they need to be assigned to workers.
type queuedOperationsHeap []queuedOperationsEntry

func (h queuedOperationsHeap) Len() int {
	return len(h)
}

func (h queuedOperationsHeap) Less(i, j int) bool {
	// Lexicographic order on priority and queued timestamp.
	ti := h[i].operation.desiredState.QueuedTimestamp
	tj := h[j].operation.desiredState.QueuedTimestamp
	return h[i].priority < h[j].priority || (h[i].priority == h[j].priority &&
		(ti.Seconds < tj.Seconds || (ti.Seconds == tj.Seconds &&
			ti.Nanos < tj.Nanos)))
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

// operation state that is created for every execution request.
type operation struct {
	platformQueue *platformQueue
	desiredState  remoteworker.DesiredState_Executing

	// These fields are not strictly necessary to implement the
	// BuildQueue and OperationQueueServer interfaces. They need to
	// be present to implement BuildQueueStateProvider.
	name         string
	instanceName digest.InstanceName
	argv0        string

	waiters uint

	// queueIndex contains the index at which the operation is
	// stored in the platformQueue's queuedOperations heap. When
	// negative, it means that the operation is no longer in the
	// queued stage (and thus either in the executing or completed
	// stage).
	queueIndex int

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
	cleanupKey       cleanupKey
}

func (o *operation) getStage() remoteexecution.ExecutionStage_Value {
	if o.executeResponse != nil {
		return remoteexecution.ExecutionStage_COMPLETED
	}
	if o.queueIndex < 0 {
		return remoteexecution.ExecutionStage_EXECUTING
	}
	return remoteexecution.ExecutionStage_QUEUED
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

	for {
		// Construct the longrunning.Operation that needs to be
		// sent back to the client.
		metadata, err := ptypes.MarshalAny(&remoteexecution.ExecuteOperationMetadata{
			Stage:        o.getStage(),
			ActionDigest: o.desiredState.ActionDigest,
		})
		if err != nil {
			return util.StatusWrap(err, "Failed to marshal execute operation metadata")
		}
		operation := &longrunning.Operation{
			Name:     o.name,
			Metadata: metadata,
		}
		if o.executeResponse != nil {
			operation.Done = true
			response, err := ptypes.MarshalAny(o.executeResponse)
			if err != nil {
				return util.StatusWrap(err, "Failed to marshal execute response")
			}
			operation.Result = &longrunning.Operation_Response{Response: response}
		}
		completionWakeup := o.completionWakeup
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

// complete execution of the operation by registering the execution
// response. This function wakes up any clients waiting on the operation
// to complete.
func (o *operation) complete(bq *InMemoryBuildQueue, executeResponse *remoteexecution.ExecuteResponse) {
	// Move the operation to the executing stage if it's still in
	// the queued stage.
	pq := o.platformQueue
	if o.queueIndex >= 0 {
		heap.Remove(&pq.queuedOperations, o.queueIndex)
		o.registerQueuedStageFinished(bq)
	}

	// Move the operation to the completed stage if it's still in
	// the executing stage.
	if o.executeResponse == nil {
		// Mark the operation as completed.
		delete(pq.inFlightDeduplicationMap, newInFlightDeduplicationKey(o.desiredState.ActionDigest))
		o.executeResponse = executeResponse
		close(o.completionWakeup)

		// Scrub data from the operation that are no longer
		// needed after completion. This reduces memory usage
		// significantly. Keep the Action digest, so that
		// there's still a way to figure out what the operation
		// was.
		o.desiredState.Action = nil
		o.desiredState.Command = nil
		o.completionWakeup = nil

		var result, grpcCode string
		if s := status.FromProto(executeResponse.Status); s.Err() != nil {
			result = "Failure"
			grpcCode = s.Code().String()
		} else if actionResult := executeResponse.Result; actionResult == nil {
			result = "ActionResultMissing"
		} else if actionResult.ExitCode == 0 {
			result = "Success"
		} else {
			result = "NonZeroExitCode"
		}
		o.registerExecutingStageFinished(bq, result, grpcCode)
	}
}

func (o *operation) remove(bq *InMemoryBuildQueue) {
	o.complete(bq, &remoteexecution.ExecuteResponse{
		Status: status.New(codes.Canceled, "Operation no longer has any waiting clients").Proto(),
	})

	// Remove the operation.
	delete(bq.operationsNameMap, o.name)
	o.registerCompletedStageFinished(bq)
}

func (o *operation) getBasicOperationState(bq *InMemoryBuildQueue) *BasicOperationState {
	queuedTimestamp, err := ptypes.Timestamp(o.desiredState.QueuedTimestamp)
	if err != nil {
		panic(fmt.Sprintf("Failed to parse previously generated timestamp: %s", err))
	}
	return &BasicOperationState{
		Name:            o.name,
		QueuedTimestamp: queuedTimestamp,
		ActionDigest:    o.desiredState.ActionDigest,
		Argv0:           o.argv0,
		Timeout:         bq.cleanupQueue.getTimestamp(o.cleanupKey),
	}
}

func (o *operation) getDetailedOperationState(bq *InMemoryBuildQueue) *DetailedOperationState {
	return &DetailedOperationState{
		BasicOperationState: *o.getBasicOperationState(bq),
		InstanceName:        o.instanceName,
		Stage:               o.getStage(),
		ExecuteResponse:     o.executeResponse,
	}
}

// registerQueuedStageFinished updates Prometheus metrics related to
// operations finishing the QUEUED stage.
func (o *operation) registerQueuedStageFinished(bq *InMemoryBuildQueue) {
	o.platformQueue.operationsQueuedDurationSeconds.Observe(bq.now.Sub(o.currentStageStartTime).Seconds())
	o.currentStageStartTime = bq.now
}

// registerQueuedStageFinished updates Prometheus metrics related to
// operations finishing the EXECUTING stage.
func (o *operation) registerExecutingStageFinished(bq *InMemoryBuildQueue, result string, grpcCode string) {
	o.platformQueue.operationsExecutingDurationSeconds.WithLabelValues(result, grpcCode).Observe(bq.now.Sub(o.currentStageStartTime).Seconds())
	o.platformQueue.operationsExecutingRetries.WithLabelValues(result, grpcCode).Observe(float64(o.retryCount))
	o.currentStageStartTime = bq.now
}

// registerQueuedStageFinished updates Prometheus metrics related to
// operations finishing the COMPLETED stage, meaning the operation got
// removed.
func (o *operation) registerCompletedStageFinished(bq *InMemoryBuildQueue) {
	o.platformQueue.operationsCompletedDurationSeconds.Observe(bq.now.Sub(o.currentStageStartTime).Seconds())
	o.currentStageStartTime = bq.now
}

// worker state for every node capable of executing operations.
type worker struct {
	currentOperation *operation
	cleanupKey       cleanupKey
	terminating      bool
}

func (w *worker) getCurrentOperation() *operation {
	if o := w.currentOperation; o != nil && o.executeResponse == nil {
		return o
	}
	return nil
}

// startOperation assigns an operation to the worker, returning a
// synchronization response that instructs the worker to start executing
// it.
func (w *worker) startOperation(bq *InMemoryBuildQueue, pq *platformQueue, o *operation) *remoteworker.SynchronizeResponse {
	w.currentOperation = o
	o.registerQueuedStageFinished(bq)
	return &remoteworker.SynchronizeResponse{
		NextSynchronizationAt: bq.getNextSynchronizationAtDelay(),
		DesiredState: &remoteworker.DesiredState{
			WorkerState: &remoteworker.DesiredState_Executing_{
				Executing: &o.desiredState,
			},
		},
	}
}

// getNextOperation extracts the next queued operation from the queue
// and assigns it to the current worker. Depending on whether a context
// object is provided, this function either blocks until work is
// available or returns immediately. When returning immediately, it
// instructs the worker to go idle.
func (w *worker) getNextOperation(ctx context.Context, bq *InMemoryBuildQueue, pq *platformQueue, workerID map[string]string) (*remoteworker.SynchronizeResponse, error) {
	if o, ok := w.getNextOperationNonBlocking(bq, pq, workerID); ok {
		return w.startOperation(bq, pq, o), nil
	}

	if ctx == nil {
		// We shouldn't block, as the worker is currently doing
		// some work that it shouldn't be doing. Request that
		// the worker goes idle immediately. It will
		// resynchronize as soon as it's done terminating its
		// current build action.
		return &remoteworker.SynchronizeResponse{
			NextSynchronizationAt: bq.getCurrentTime(),
			DesiredState: &remoteworker.DesiredState{
				WorkerState: &remoteworker.DesiredState_Idle{
					Idle: &empty.Empty{},
				},
			},
		}, nil
	}

	o, err := w.getNextOperationBlocking(ctx, bq, pq, workerID)
	if o == nil {
		return &remoteworker.SynchronizeResponse{
			NextSynchronizationAt: bq.getCurrentTime(),
		}, err
	}
	return w.startOperation(bq, pq, o), nil
}

// getCurrentOrNextOperation either returns a synchronization response
// that instructs the worker to run the operation it should be running.
// When the worker has no operation assigned to it, it attempts to
// request an operation from the queue.
func (w *worker) getCurrentOrNextOperation(ctx context.Context, bq *InMemoryBuildQueue, pq *platformQueue, workerID map[string]string) (*remoteworker.SynchronizeResponse, error) {
	if o := w.getCurrentOperation(); o != nil {
		if o.retryCount >= bq.configuration.WorkerOperationRetryCount {
			o.complete(bq, &remoteexecution.ExecuteResponse{
				Status: status.Newf(
					codes.Internal,
					"Attempted to execute operation %d times, but it never completed. This operation may cause worker %s to crash.",
					o.retryCount+1,
					newWorkerKey(workerID)).Proto(),
			})
		} else {
			o.retryCount++
			return &remoteworker.SynchronizeResponse{
				NextSynchronizationAt: bq.getNextSynchronizationAtDelay(),
				DesiredState: &remoteworker.DesiredState{
					WorkerState: &remoteworker.DesiredState_Executing_{
						Executing: &o.desiredState,
					},
				},
			}, nil
		}
	}
	return w.getNextOperation(ctx, bq, pq, workerID)
}

// isRunningCorrectOperation determines whether the worker is actually
// running the action the scheduler instructed it to run previously.
func (w *worker) isRunningCorrectOperation(actionDigest *remoteexecution.Digest) bool {
	o := w.getCurrentOperation()
	if o == nil {
		return false
	}
	desiredDigest := o.desiredState.ActionDigest
	return proto.Equal(actionDigest, desiredDigest)
}

// updateOperation processes execution status updates from the worker
// that do not equal the 'completed' state.
func (w *worker) updateOperation(bq *InMemoryBuildQueue, pq *platformQueue, workerID map[string]string, actionDigest *remoteexecution.Digest) (*remoteworker.SynchronizeResponse, error) {
	if !w.isRunningCorrectOperation(actionDigest) {
		return w.getCurrentOrNextOperation(nil, bq, pq, workerID)
	}
	// The worker is doing fine. Allow it to continue with what it's
	// doing right now.
	return &remoteworker.SynchronizeResponse{
		NextSynchronizationAt: bq.getNextSynchronizationAtDelay(),
	}, nil
}

// completeOperation processes execution status updates from the worker
// that equal the 'completed' state. It causes the execute response to
// be preserved and communicated to clients that are waiting on the
// completion of the operation.
func (w *worker) completeOperation(ctx context.Context, bq *InMemoryBuildQueue, pq *platformQueue, workerID map[string]string, actionDigest *remoteexecution.Digest, executeResponse *remoteexecution.ExecuteResponse) (*remoteworker.SynchronizeResponse, error) {
	if !w.isRunningCorrectOperation(actionDigest) {
		return w.getCurrentOrNextOperation(ctx, bq, pq, workerID)
	}
	w.getCurrentOperation().complete(bq, executeResponse)
	return w.getNextOperation(ctx, bq, pq, workerID)
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

func (q *cleanupQueue) getTimestamp(key cleanupKey) *time.Time {
	if key == 0 {
		return nil
	}
	t := q.heap[key-1].timestamp
	return &t
}
