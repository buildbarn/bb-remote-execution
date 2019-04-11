package builder

import (
	"container/heap"
	"context"
	"fmt"
	"log"
	"math"
	"sync"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/bazelbuild/remote-apis/build/bazel/semver"
	remoteworker "github.com/buildbarn/bb-remote-execution/pkg/proto/worker"
	"github.com/buildbarn/bb-storage/pkg/builder"
	"github.com/buildbarn/bb-storage/pkg/util"
	"github.com/golang/protobuf/ptypes"
	"google.golang.org/genproto/googleapis/longrunning"
	stat "google.golang.org/genproto/googleapis/rpc/status"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// workerBuildJob holds the information we need to track for a single
// build action that is enqueued.
type workerBuildJob struct {
	name             string
	actionDigest     *remoteexecution.Digest
	deduplicationKey string
	executeRequest   remoteexecution.ExecuteRequest
	insertionOrder   uint64

	stage                   remoteexecution.ExecuteOperationMetadata_Stage
	workerStage             remoteworker.WorkerOperationMetadata_Stage
	executeResponse         *remoteexecution.ExecuteResponse
	executeTransitionWakeup *sync.Cond
}

// workerBuildJobHeap is a heap of workerBuildJob entries, sorted by
// priority in which they should be execution.
type workerBuildJobHeap []*workerBuildJob

func (h workerBuildJobHeap) Len() int {
	return len(h)
}

func (h workerBuildJobHeap) Less(i, j int) bool {
	// Lexicographic order on priority and insertion order.
	var iPriority int32
	if policy := h[i].executeRequest.ExecutionPolicy; policy != nil {
		iPriority = policy.Priority
	}
	var jPriority int32
	if policy := h[j].executeRequest.ExecutionPolicy; policy != nil {
		jPriority = policy.Priority
	}
	return iPriority < jPriority || (iPriority == jPriority && h[i].insertionOrder < h[j].insertionOrder)
}

func (h workerBuildJobHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
}

func (h *workerBuildJobHeap) Push(x interface{}) {
	*h = append(*h, x.(*workerBuildJob))
}

func (h *workerBuildJobHeap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}

func (job *workerBuildJob) waitExecution(out remoteexecution.Execution_ExecuteServer) error {
	for {
		// Send current state.
		metadata, err := ptypes.MarshalAny(&remoteexecution.ExecuteOperationMetadata{
			Stage:        job.stage,
			ActionDigest: job.actionDigest,
		})
		if err != nil {
			log.Fatal("Failed to marshal execute operation metadata: ", err)
		}
		operation := &longrunning.Operation{
			Name:     job.name,
			Metadata: metadata,
		}
		if job.executeResponse != nil {
			operation.Done = true
			response, err := ptypes.MarshalAny(job.executeResponse)
			if err != nil {
				log.Fatal("Failed to marshal execute response: ", err)
			}
			operation.Result = &longrunning.Operation_Response{Response: response}
		}
		if err := out.Send(operation); err != nil {
			return err
		}

		// Wait for state transition.
		// TODO(edsch): Should take a context.
		// TODO(edsch): Should wake up periodically.
		if job.executeResponse != nil {
			return nil
		}
		job.executeTransitionWakeup.Wait()
	}
}

type workerBuildQueue struct {
	deduplicationKeyFormat util.DigestKeyFormat
	jobsPendingMax         uint64
	nextInsertionOrder     uint64

	jobsLock                   sync.Mutex
	jobsNameMap                map[string]*workerBuildJob
	jobsDeduplicationMap       map[string]*workerBuildJob
	jobsPending                workerBuildJobHeap
	jobsPendingInsertionWakeup *sync.Cond
}

// NewWorkerBuildQueue creates an execution server that places execution
// requests in a queue. These execution requests may be extracted by
// workers.
func NewWorkerBuildQueue(deduplicationKeyFormat util.DigestKeyFormat, jobsPendingMax uint64) (builder.BuildQueue, remoteworker.OperationQueueServer) {
	bq := &workerBuildQueue{
		deduplicationKeyFormat: deduplicationKeyFormat,
		jobsPendingMax:         jobsPendingMax,

		jobsNameMap:          map[string]*workerBuildJob{},
		jobsDeduplicationMap: map[string]*workerBuildJob{},
	}
	bq.jobsPendingInsertionWakeup = sync.NewCond(&bq.jobsLock)

	return bq, bq
}

func (bq *workerBuildQueue) Take(ctx context.Context, in *remoteworker.TakeOperationRequest) (*remoteworker.TakeOperationResponse, error) {
	bq.jobsLock.Lock()
	defer bq.jobsLock.Unlock()

	// Wait for jobs to appear.
	// TODO(edsch): sync.Cond.WaitWithContext() would be helpful here.
	for bq.jobsPending.Len() == 0 {
		bq.jobsPendingInsertionWakeup.Wait()
	}
	if err := ctx.Err(); err != nil {
		bq.jobsPendingInsertionWakeup.Signal()
		return nil, err
	}

	// Extract job from queue.
	job := heap.Pop(&bq.jobsPending).(*workerBuildJob)
	// Could make the job.name the hash of the worker_id
	// job.name = in.WorkerId
	// ok := bq.jobsmap[job.name] = job
	job.stage = remoteexecution.ExecuteOperationMetadata_EXECUTING
	// Should transition when setting status?
	// job.executeTransitionWakeup.Broadcast()

	// Expect a poll interval every 60 seconds
	config := map[string]string{
		"poll_interval_seconds": "1",
	}

	takeResponse := remoteworker.TakeOperationResponse{
		Config:         config,
		ExecuteRequest: &job.executeRequest,
	}

	return &takeResponse, nil
}

func (bq *workerBuildQueue) Poll(ctx context.Context, in *longrunning.Operation) (*stat.Status, error) {
	bq.jobsLock.Lock()
	defer bq.jobsLock.Unlock()

	job, ok := bq.jobsNameMap[in.Name]
	if !ok {
		return &stat.Status{}, fmt.Errorf("Cannot find job: %s", in.Name)
	}

	workerMetadata := &remoteworker.WorkerOperationMetadata{}

	if err := ptypes.UnmarshalAny(in.Metadata, workerMetadata); err != nil {
		return &stat.Status{}, fmt.Errorf("Cannot unmarshal metadata: %s", in.Name)
	}

	if in.Done {
		delete(bq.jobsDeduplicationMap, job.deduplicationKey)
		job.stage = remoteexecution.ExecuteOperationMetadata_COMPLETED

		switch in.Result.(type) {
		case *longrunning.Operation_Error:
			job.executeResponse = &remoteexecution.ExecuteResponse{
				Status: in.GetError(),
			}
			job.executeTransitionWakeup.Broadcast()
			return in.GetError(), nil
		case *longrunning.Operation_Response:
			if in == nil {
				return &stat.Status{}, nil
			}
			executeResponse := &remoteexecution.ExecuteResponse{}
			if err := ptypes.UnmarshalAny(in.GetResponse(), executeResponse); err != nil {
				return &stat.Status{}, err
			}
			job.executeResponse = executeResponse
			job.executeTransitionWakeup.Broadcast()
		}
	} else {
		job.workerStage = workerMetadata.Stage
	}

	return &stat.Status{}, nil
}

func (bq *workerBuildQueue) GetCapabilities(ctx context.Context, in *remoteexecution.GetCapabilitiesRequest) (*remoteexecution.ServerCapabilities, error) {
	return &remoteexecution.ServerCapabilities{
		CacheCapabilities: &remoteexecution.CacheCapabilities{
			DigestFunction: []remoteexecution.DigestFunction{
				remoteexecution.DigestFunction_MD5,
				remoteexecution.DigestFunction_SHA1,
				remoteexecution.DigestFunction_SHA256,
			},
			ActionCacheUpdateCapabilities: &remoteexecution.ActionCacheUpdateCapabilities{
				// TODO(edsch): Let the frontend flip this to true when enabled?
				UpdateEnabled: false,
			},
			// CachePriorityCapabilities: Priorities not supported.
			// MaxBatchTotalSize: Not used by Bazel yet.
			SymlinkAbsolutePathStrategy: remoteexecution.CacheCapabilities_ALLOWED,
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
		// TODO(edsch): DeprecatedApiVersion.
		LowApiVersion:  &semver.SemVer{Major: 2},
		HighApiVersion: &semver.SemVer{Major: 2},
	}, nil
}

func (bq *workerBuildQueue) Execute(in *remoteexecution.ExecuteRequest, out remoteexecution.Execution_ExecuteServer) error {
	digest, err := util.NewDigest(in.InstanceName, in.ActionDigest)

	if err != nil {
		return err
	}
	deduplicationKey := digest.GetKey(bq.deduplicationKeyFormat)

	bq.jobsLock.Lock()
	defer bq.jobsLock.Unlock()

	job, ok := bq.jobsDeduplicationMap[deduplicationKey]
	if !ok {
		// TODO(edsch): Maybe let the number of workers influence this?
		if uint64(bq.jobsPending.Len()) >= bq.jobsPendingMax {
			return status.Errorf(codes.Unavailable, "Too many jobs pending")
		}

		job = &workerBuildJob{
			name:                    in.ActionDigest.Hash, //uuid.Must(uuid.NewRandom()).String(),
			actionDigest:            in.ActionDigest,
			deduplicationKey:        deduplicationKey,
			executeRequest:          *in,
			insertionOrder:          bq.nextInsertionOrder,
			stage:                   remoteexecution.ExecuteOperationMetadata_QUEUED,
			executeTransitionWakeup: sync.NewCond(&bq.jobsLock),
		}
		bq.jobsNameMap[job.name] = job
		bq.jobsDeduplicationMap[deduplicationKey] = job
		heap.Push(&bq.jobsPending, job)
		bq.jobsPendingInsertionWakeup.Signal()
		bq.nextInsertionOrder++
	}
	return job.waitExecution(out)
}

func (bq *workerBuildQueue) WaitExecution(in *remoteexecution.WaitExecutionRequest, out remoteexecution.Execution_WaitExecutionServer) error {
	bq.jobsLock.Lock()
	defer bq.jobsLock.Unlock()

	job, ok := bq.jobsNameMap[in.Name]
	if !ok {
		return status.Errorf(codes.NotFound, "Build job with name %s not found", in.Name)
	}
	return job.waitExecution(out)
}
