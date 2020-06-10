package builder

import (
	"time"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-storage/pkg/digest"
)

// BuildQueueStateProvider is implemented by InMemoryBuildQueue to
// expose the current state of the scheduler in such a way that
// bb_scheduler can serve it through a web interface.
type BuildQueueStateProvider interface {
	GetBuildQueueState() *BuildQueueState
	GetDetailedOperationState(name string) (*DetailedOperationState, bool)
	KillOperation(name string) bool
	KillInvocation(invocationID string) bool
	ListDetailedOperationState(invocationID *string, selection *string, pageSize int, startAfterOperation *string) ([]DetailedOperationState, PaginationInfo, error)
	ListInvocations(instanceName digest.InstanceName, pageSize int, active bool) ([]InvocationEntry, PaginationInfo, error)
	ListQueuedOperationState(instanceName digest.InstanceName, platform *remoteexecution.Platform, pageSize int, startAfterPriority *int32, startAfterQueuedTimestamp *time.Time) ([]QueuedOperationState, PaginationInfo, error)
	ListWorkerState(instanceName digest.InstanceName, platform *remoteexecution.Platform, justExecutingWorkers bool, pageSize int, startAfterWorkerID map[string]string) ([]WorkerState, PaginationInfo, error)

	// Support for installing drains on workers.
	ListDrainState(instanceName digest.InstanceName, platform *remoteexecution.Platform) ([]DrainState, error)
	AddDrain(instanceName digest.InstanceName, platform *remoteexecution.Platform, workerIDPattern map[string]string) error
	RemoveDrain(instanceName digest.InstanceName, platform *remoteexecution.Platform, workerIDPattern map[string]string) error

	// Support for gracefully terminating workers.
	MarkTerminatingAndWait(workerIDPattern map[string]string)
}

// PaginationInfo returns offsets of the data returned by
// BuildQueueStateProvider's List*() calls.
type PaginationInfo struct {
	StartIndex   int
	EndIndex     int
	TotalEntries int
}

// BuildQueueState contains the overall state of the build queue.
type BuildQueueState struct {
	PlatformQueues  []PlatformQueueState
	OperationsCount int
}

// PlatformQueueState contains the state of a single per-platform queue.
type PlatformQueueState struct {
	InstanceName digest.InstanceName
	Platform     remoteexecution.Platform

	Timeout               *time.Time
	InvocationCount       int
	QueuedOperationsCount int
	WorkersCount          int
	ExecutingWorkersCount int
	DrainsCount           int
}

// BasicOperationState contains basic properties of an operation that
// exists within a build queue. It provides just those properties that
// are interesting enough to be part of listings.
type BasicOperationState struct {
	Name            string
	QueuedTimestamp time.Time
	ActionDigest    *remoteexecution.Digest
	Argv0           string
	Timeout         *time.Time
}

// QueuedOperationState contains properties of an operation that is in
// the QUEUED state.
type QueuedOperationState struct {
	BasicOperationState

	Priority int32
}

// InvocationEntry contains properties of recent invocation.
type InvocationEntry struct {
	Priority                 int32
	InvocationID             string
	InvocationTimestamp      time.Time
	InvocationTimeout        time.Time
	QueuedOperationsCount    int
	ExecutingOperationsCount int
	FinishedOperationsCount  int
	Canceled                 bool
	Keywords                 map[string]string
}

// DetailedOperationState contains properties of an operation that is
// looked up by name.
type DetailedOperationState struct {
	BasicOperationState

	InstanceName    digest.InstanceName
	Stage           remoteexecution.ExecutionStage_Value
	ExecuteResponse *remoteexecution.ExecuteResponse
}

// WorkerState contains properties of a worker that reports to the build
// queue.
type WorkerState struct {
	WorkerID         map[string]string
	Timeout          *time.Time
	CurrentOperation *BasicOperationState
	Drained          bool
}

// DrainState contains properties of a drain that is present within the
// scheduler.
type DrainState struct {
	WorkerIDPattern   map[string]string
	CreationTimestamp time.Time
}
