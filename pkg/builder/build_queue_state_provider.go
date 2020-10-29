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
	// Global operations.
	GetBuildQueueState() *BuildQueueState
	GetDetailedOperationState(name string) (*DetailedOperationState, bool)
	ListDetailedOperationState(pageSize int, startAfterOperation *string) ([]DetailedOperationState, PaginationInfo)
	KillOperation(name string) bool

	// Operations at the platform queue level.
	ListInvocationState(instanceName digest.InstanceName, platform *remoteexecution.Platform, justQueuedInvocations bool) ([]InvocationState, error)
	ListWorkerState(instanceName digest.InstanceName, platform *remoteexecution.Platform, justExecutingWorkers bool, pageSize int, startAfterWorkerID map[string]string) ([]WorkerState, PaginationInfo, error)

	// Operations at the invocation level.
	ListQueuedOperationState(instanceName digest.InstanceName, platform *remoteexecution.Platform, invocationID string, pageSize int, startAfterPriority *int32, startAfterQueuedTimestamp *time.Time) ([]QueuedOperationState, PaginationInfo, error)

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

	Timeout                *time.Time
	InvocationsCount       int
	QueuedInvocationsCount int
	WorkersCount           int
	ExecutingWorkersCount  int
	DrainsCount            int
}

// InvocationState contains the state of a single client invocation
// inside a per-platform queue.
type InvocationState struct {
	InvocationID             string
	QueuedOperationsCount    int
	FirstQueuedOperation     *QueuedOperationState
	ExecutingOperationsCount int
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
