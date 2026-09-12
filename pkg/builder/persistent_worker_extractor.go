package builder

import (
	"context"
	"sync"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-remote-execution/pkg/cas"
	"github.com/buildbarn/bb-remote-execution/pkg/proto/bazelworker"
	runner_pb "github.com/buildbarn/bb-remote-execution/pkg/proto/runner"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/filesystem/path"
	"github.com/buildbarn/bb-storage/pkg/util"
	"github.com/prometheus/client_golang/prometheus"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	// PersistentWorkerKeyPlatformProperty is the name of the REv2
	// platform property that Bazel sets when
	// --experimental_remote_mark_tool_inputs is provided. Its value
	// is an opaque identifier of the tool that needs to execute the
	// build action.
	PersistentWorkerKeyPlatformProperty = "persistentWorkerKey"

	// PersistentWorkerProtocolPlatformProperty is the name of the
	// REv2 platform property that may be used to indicate that the
	// tool expects WorkRequest and WorkResponse messages to be
	// encoded as JSON, as opposed to length delimited Protobuf
	// messages.
	//
	// Bazel does not set this property automatically. Users need to
	// declare it explicitly through exec_properties, matching the
	// 'requires-worker-protocol' execution requirement of the tool.
	PersistentWorkerProtocolPlatformProperty = "persistentWorkerProtocol"
)

var (
	persistentWorkerExtractorPrometheusMetrics sync.Once

	persistentWorkerExtractorActionsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "buildbarn",
			Subsystem: "builder",
			Name:      "persistent_worker_extractor_actions_total",
			Help:      "Number of build actions that requested execution by a persistent worker, labeled by whether the request was honored.",
		},
		[]string{"result"},
	)
)

// PersistentWorkerExtractor determines whether a build action needs to
// be executed by a persistent worker process, and computes the options
// that the runner needs to do so.
type PersistentWorkerExtractor struct {
	directoryFetcher      cas.DirectoryFetcher
	maximumInputFileCount int
}

// NewPersistentWorkerExtractor creates a PersistentWorkerExtractor that
// obtains the input files of build actions from the Content Addressable
// Storage.
//
// Build actions whose input root contains more than
// maximumInputFileCount files are executed as regular processes, as
// providing the full list of input files to the runner would require an
// excessive amount of memory and network bandwidth. A value of zero or
// less disables this limit, which callers should only do if the size of
// input roots is bounded by other means.
func NewPersistentWorkerExtractor(directoryFetcher cas.DirectoryFetcher, maximumInputFileCount int) *PersistentWorkerExtractor {
	persistentWorkerExtractorPrometheusMetrics.Do(func() {
		prometheus.MustRegister(persistentWorkerExtractorActionsTotal)
	})

	return &PersistentWorkerExtractor{
		directoryFetcher:      directoryFetcher,
		maximumInputFileCount: maximumInputFileCount,
	}
}

// getPlatformProperty returns the value of a single REv2 platform
// property. Only the platform properties stored in the Action message
// are considered, as those are the ones the scheduler uses to select a
// worker (REv2.2 and later).
func getPlatformProperty(platform *remoteexecution.Platform, name string) string {
	for _, property := range platform.GetProperties() {
		if property.Name == name {
			return property.Value
		}
	}
	return ""
}

// Extract returns the persistent worker options that need to be
// attached to the request that is sent to the runner. It returns nil if
// the build action does not need to be executed by a persistent worker.
func (e *PersistentWorkerExtractor) Extract(ctx context.Context, digestFunction digest.Function, action *remoteexecution.Action) (*runner_pb.PersistentWorker, error) {
	key := getPlatformProperty(action.GetPlatform(), PersistentWorkerKeyPlatformProperty)
	if key == "" {
		return nil, nil
	}

	var protocol runner_pb.PersistentWorker_Protocol
	switch value := getPlatformProperty(action.GetPlatform(), PersistentWorkerProtocolPlatformProperty); value {
	case "", "proto":
		protocol = runner_pb.PersistentWorker_PROTO
	case "json":
		protocol = runner_pb.PersistentWorker_JSON
	default:
		return nil, status.Errorf(codes.InvalidArgument, "Platform property %#v has unsupported value %#v", PersistentWorkerProtocolPlatformProperty, value)
	}

	inputRootDigest, err := digestFunction.NewDigestFromProto(action.InputRootDigest)
	if err != nil {
		return nil, util.StatusWrap(err, "Failed to extract digest for input root")
	}
	inputs, err := e.getInputs(ctx, digestFunction, inputRootDigest)
	if err != nil {
		return nil, err
	}
	if inputs == nil {
		// The input root is too large to report to the worker
		// process. Fall back to executing the tool as a regular
		// process, as tools that perform incremental
		// compilation may return incorrect results if they are
		// provided an incomplete list of inputs.
		persistentWorkerExtractorActionsTotal.WithLabelValues("TooManyInputFiles").Inc()
		return nil, nil
	}

	persistentWorkerExtractorActionsTotal.WithLabelValues("Used").Inc()
	return &runner_pb.PersistentWorker{
		Key:      key,
		Protocol: protocol,
		Inputs:   inputs,
	}, nil
}

// getInputs returns the list of files stored in the input root of a
// build action, including their digests. It returns nil if the input
// root contains more files than the configured maximum.
func (e *PersistentWorkerExtractor) getInputs(ctx context.Context, digestFunction digest.Function, inputRootDigest digest.Digest) ([]*bazelworker.Input, error) {
	// Always return a non-nil slice on success, so that callers can
	// distinguish an empty input root from one that is too large.
	inputs := []*bazelworker.Input{}
	inputs, exceeded, err := e.appendInputs(ctx, inputs, digestFunction, inputRootDigest, nil)
	if err != nil {
		return nil, err
	}
	if exceeded {
		return nil, nil
	}
	return inputs, nil
}

func (e *PersistentWorkerExtractor) appendInputs(ctx context.Context, inputs []*bazelworker.Input, digestFunction digest.Function, directoryDigest digest.Digest, directoryPath *path.Trace) ([]*bazelworker.Input, bool, error) {
	directory, err := e.directoryFetcher.GetDirectory(ctx, directoryDigest)
	if err != nil {
		return nil, false, util.StatusWrapf(err, "Failed to obtain input directory %#v", directoryPath.GetUNIXString())
	}

	for _, file := range directory.Files {
		component, ok := path.NewComponent(file.Name)
		if !ok {
			return nil, false, status.Errorf(codes.InvalidArgument, "Input directory %#v contains file with invalid name %#v", directoryPath.GetUNIXString(), file.Name)
		}
		filePath := directoryPath.Append(component)
		fileDigest, err := digestFunction.NewDigestFromProto(file.Digest)
		if err != nil {
			return nil, false, util.StatusWrapf(err, "Failed to extract digest for input file %#v", filePath.GetUNIXString())
		}
		if e.maximumInputFileCount > 0 && len(inputs) >= e.maximumInputFileCount {
			return nil, true, nil
		}
		inputs = append(inputs, &bazelworker.Input{
			Path: filePath.GetUNIXString(),
			// Bazel provides persistent workers with the
			// hexadecimal representation of the digest of
			// the file's contents, encoded as UTF-8.
			Digest: []byte(fileDigest.GetHashString()),
		})
	}

	for _, child := range directory.Directories {
		component, ok := path.NewComponent(child.Name)
		if !ok {
			return nil, false, status.Errorf(codes.InvalidArgument, "Input directory %#v contains directory with invalid name %#v", directoryPath.GetUNIXString(), child.Name)
		}
		childPath := directoryPath.Append(component)
		childDigest, err := digestFunction.NewDigestFromProto(child.Digest)
		if err != nil {
			return nil, false, util.StatusWrapf(err, "Failed to extract digest for input directory %#v", childPath.GetUNIXString())
		}
		var exceeded bool
		inputs, exceeded, err = e.appendInputs(ctx, inputs, digestFunction, childDigest, childPath)
		if err != nil {
			return nil, false, err
		}
		if exceeded {
			return nil, true, nil
		}
	}
	return inputs, false, nil
}
