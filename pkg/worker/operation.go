package worker

import (
	"context"
	"fmt"
	"log"
	"net/url"
	"strconv"
	"time"

	"github.com/google/uuid"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-remote-execution/pkg/builder"
	remoteworker "github.com/buildbarn/bb-remote-execution/pkg/proto/worker"
	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes"
	"google.golang.org/genproto/googleapis/longrunning"
	stat "google.golang.org/genproto/googleapis/rpc/status"
)

// Client contains everything needed to contact the scheduler and execute tasks
type Client struct {
	instanceName    string
	workerID        map[string]string
	platform        *remoteexecution.Platform
	schedulerClient remoteworker.OperationQueueClient
	buildExecutor   builder.BuildExecutor
	browserURL      *url.URL
}

// NewClient creates a new client for a worker
func NewClient(schedulerClient remoteworker.OperationQueueClient, buildExecutor builder.BuildExecutor, browserURL *url.URL, instanceName string, platform *remoteexecution.Platform) *Client {

	workerID := map[string]string{
		"uuid":          uuid.Must(uuid.NewRandom()).String(),
		"creation_time": time.Now().String(),
	}

	return &Client{
		instanceName:    instanceName,
		workerID:        workerID,
		platform:        platform,
		schedulerClient: schedulerClient,
		buildExecutor:   buildExecutor,
		browserURL:      browserURL,
	}
}

func (wc *Client) heartbeat(pollingInterval time.Duration, operation *longrunning.Operation, in <-chan *remoteworker.WorkerOperationMetadata, out chan<- *remoteworker.WorkerOperationMetadata) {
	timer := time.NewTimer(pollingInterval)
	workerMetadata := &remoteworker.WorkerOperationMetadata{WorkerId: wc.workerID}

	for {
		select {
		case msg, ok := <-in:
			if !ok {
				workerMetadata.Stage = remoteworker.WorkerOperationMetadata_COMPLETED
				out <- workerMetadata
				break
			}

			proto.Merge(workerMetadata, msg)
			operationMetadata, _ := ptypes.MarshalAny(workerMetadata)
			operation.Metadata = operationMetadata
		case <-timer.C:
			wc.Poll(operation)
			timer = time.NewTimer(pollingInterval)
		}
	}
}

// Take a job from the client
func (wc *Client) Take() error {

	takeRequest := &remoteworker.TakeOperationRequest{
		WorkerId:     wc.workerID,
		InstanceName: wc.instanceName,
		Platform:     wc.platform,
	}
	takeResponse, err := wc.schedulerClient.Take(context.Background(), takeRequest)

	if err != nil {
		return err
	}

	executeRequest := takeResponse.ExecuteRequest
	operation := &longrunning.Operation{
		Name: executeRequest.ActionDigest.Hash,
	}

	// Print URL of the action into the log before execution.
	actionURL, err := wc.browserURL.Parse(
		fmt.Sprintf(
			"/action/%s/%s/%d/",
			wc.instanceName,
			executeRequest.ActionDigest.Hash,
			executeRequest.ActionDigest.SizeBytes))
	if err != nil {
		return err
	}

	log.Print("Action: ", actionURL.String())

	channel := make(chan *remoteworker.WorkerOperationMetadata)
	defer close(channel)

	pollingInterval := uint64(60)

	if pollingIntervalConfig, ok := takeResponse.Config["poll_interval_seconds"]; ok {
		if pollingInterval, err = strconv.ParseUint(pollingIntervalConfig, 0, 64); err != nil {
			return err
		}
	}
	in := make(chan *remoteworker.WorkerOperationMetadata)
	out := make(chan *remoteworker.WorkerOperationMetadata, 1)

	go wc.heartbeat(time.Duration(pollingInterval)*time.Second, operation, in, out)
	executeResponse, _ := wc.buildExecutor.Execute(context.Background(), executeRequest, in)
	close(in)

	workerMetadata := <-out

	result, err := ptypes.MarshalAny(executeResponse)
	if err != nil {
		return err
	}
	operation.Result = &longrunning.Operation_Response{result}

	resultMeta, err := ptypes.MarshalAny(workerMetadata)
	if err != nil {
		return err
	}

	operation.Metadata = resultMeta
	operation.Done = true

	if _, err := wc.Poll(operation); err != nil {
		return err
	}

	return err
}

// Poll the operation
func (wc *Client) Poll(operation *longrunning.Operation) (*stat.Status, error) {
	return wc.schedulerClient.Poll(context.Background(), operation)
}
