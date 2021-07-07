package builder

import (
	"context"
	"sync"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-remote-execution/pkg/filesystem"
	"github.com/buildbarn/bb-remote-execution/pkg/proto/remoteworker"
	"github.com/buildbarn/bb-remote-execution/pkg/proto/resourceusage"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/util"
	"github.com/prometheus/client_golang/prometheus"

	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

var (
	buildExecutorPrometheusMetrics sync.Once

	// Timestamps stored in ExecutedActionMetadata.
	buildExecutorDurationSeconds = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "buildbarn",
			Subsystem: "builder",
			Name:      "build_executor_duration_seconds",
			Help:      "Amount of time spent per build execution stage, in seconds.",
			Buckets:   util.DecimalExponentialBuckets(-3, 6, 2),
		},
		[]string{"result", "grpc_code", "stage"})

	// Metrics for BuildExecutorResourceUsage.
	buildExecutorExecutionTimeoutCompensation = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "buildbarn",
			Subsystem: "builder",
			Name:      "build_executor_execution_timeout_compensation",
			Help:      "Amount of time the execution timeout was compensated, in seconds.",
			Buckets:   util.DecimalExponentialBuckets(-3, 6, 2),
		},
		[]string{"result", "grpc_code"})

	// Metrics for FilePoolResourceUsage.
	buildExecutorFilePoolFilesCreated = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "buildbarn",
			Subsystem: "builder",
			Name:      "build_executor_file_pool_files_created",
			Help:      "Number of files created by a build action.",
			Buckets:   util.DecimalExponentialBuckets(0, 6, 2),
		},
		[]string{"result", "grpc_code"})
	buildExecutorFilePoolFilesCountPeak = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "buildbarn",
			Subsystem: "builder",
			Name:      "build_executor_file_pool_files_count_peak",
			Help:      "Peak number of files created by a build action.",
			Buckets:   util.DecimalExponentialBuckets(0, 6, 2),
		},
		[]string{"result", "grpc_code"})
	buildExecutorFilePoolFilesSizeBytesPeak = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "buildbarn",
			Subsystem: "builder",
			Name:      "build_executor_file_pool_files_size_bytes_peak",
			Help:      "Peak size of files created by a build action, in bytes.",
			Buckets:   prometheus.ExponentialBuckets(1.0, 2.0, 33),
		},
		[]string{"result", "grpc_code"})
	buildExecutorFilePoolFilesOperationsCount = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "buildbarn",
			Subsystem: "builder",
			Name:      "build_executor_file_pool_operations_count",
			Help:      "Number of file pool operations performed by build actions.",
			Buckets:   util.DecimalExponentialBuckets(0, 6, 2),
		},
		[]string{"result", "grpc_code", "operation"})
	buildExecutorFilePoolFilesOperationsSizeBytes = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "buildbarn",
			Subsystem: "builder",
			Name:      "build_executor_file_pool_operations_size_bytes",
			Help:      "Total size of file pool operations performed by build actions, in bytes.",
			Buckets:   prometheus.ExponentialBuckets(1.0, 2.0, 33),
		},
		[]string{"result", "grpc_code", "operation"})

	// Metrics for POSIXResourceUsage.
	buildExecutorPOSIXUserTime = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "buildbarn",
			Subsystem: "builder",
			Name:      "build_executor_posix_user_time",
			Help:      "Amount of time spent in userspace by build actions, in seconds.",
			Buckets:   util.DecimalExponentialBuckets(-3, 6, 2),
		},
		[]string{"result", "grpc_code"})
	buildExecutorPOSIXSystemTime = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "buildbarn",
			Subsystem: "builder",
			Name:      "build_executor_posix_system_time",
			Help:      "Amount of time spent in kernelspace by build actions, in seconds.",
			Buckets:   util.DecimalExponentialBuckets(-3, 6, 2),
		},
		[]string{"result", "grpc_code"})
	buildExecutorPOSIXMaximumResidentSetSize = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "buildbarn",
			Subsystem: "builder",
			Name:      "build_executor_posix_maximum_resident_set_size",
			Help:      "Maximum resident set size of build actions, in bytes.",
			Buckets:   prometheus.ExponentialBuckets(1024.0, 2.0, 23),
		},
		[]string{"result", "grpc_code"})
	buildExecutorPOSIXPageReclaims = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "buildbarn",
			Subsystem: "builder",
			Name:      "build_executor_posix_page_reclaims",
			Help:      "Number of page reclaims caused by build actions.",
			Buckets:   util.DecimalExponentialBuckets(0, 9, 2),
		},
		[]string{"result", "grpc_code"})
	buildExecutorPOSIXPageFaults = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "buildbarn",
			Subsystem: "builder",
			Name:      "build_executor_posix_page_faults",
			Help:      "Number of page faults caused by build actions.",
			Buckets:   util.DecimalExponentialBuckets(0, 9, 2),
		},
		[]string{"result", "grpc_code"})
	buildExecutorPOSIXSwaps = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "buildbarn",
			Subsystem: "builder",
			Name:      "build_executor_posix_swaps",
			Help:      "Number of swaps caused by build actions.",
			Buckets:   util.DecimalExponentialBuckets(0, 9, 2),
		},
		[]string{"result", "grpc_code"})
	buildExecutorPOSIXBlockInputOperations = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "buildbarn",
			Subsystem: "builder",
			Name:      "build_executor_posix_block_input_operations",
			Help:      "Number of block input operations performed by build actions.",
			Buckets:   util.DecimalExponentialBuckets(0, 9, 2),
		},
		[]string{"result", "grpc_code"})
	buildExecutorPOSIXBlockOutputOperations = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "buildbarn",
			Subsystem: "builder",
			Name:      "build_executor_posix_block_output_operations",
			Help:      "Number of block output operations performed by build actions.",
			Buckets:   util.DecimalExponentialBuckets(0, 9, 2),
		},
		[]string{"result", "grpc_code"})
	buildExecutorPOSIXMessagesSent = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "buildbarn",
			Subsystem: "builder",
			Name:      "build_executor_posix_messages_sent",
			Help:      "Number of messages sent by build actions.",
			Buckets:   util.DecimalExponentialBuckets(0, 9, 2),
		},
		[]string{"result", "grpc_code"})
	buildExecutorPOSIXMessagesReceived = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "buildbarn",
			Subsystem: "builder",
			Name:      "build_executor_posix_messages_received",
			Help:      "Number of messages received by build actions.",
			Buckets:   util.DecimalExponentialBuckets(0, 9, 2),
		},
		[]string{"result", "grpc_code"})
	buildExecutorPOSIXSignalsReceived = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "buildbarn",
			Subsystem: "builder",
			Name:      "build_executor_posix_signals_received",
			Help:      "Number of signals received by build actions.",
			Buckets:   util.DecimalExponentialBuckets(0, 6, 2),
		},
		[]string{"result", "grpc_code"})
	buildExecutorPOSIXVoluntaryContextSwitches = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "buildbarn",
			Subsystem: "builder",
			Name:      "build_executor_posix_voluntary_context_switches",
			Help:      "Number of voluntary context switches caused by build actions.",
			Buckets:   util.DecimalExponentialBuckets(0, 9, 2),
		},
		[]string{"result", "grpc_code"})
	buildExecutorPOSIXInvoluntaryContextSwitches = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "buildbarn",
			Subsystem: "builder",
			Name:      "build_executor_posix_involuntary_context_switches",
			Help:      "Number of involuntary context switches caused by build actions.",
			Buckets:   util.DecimalExponentialBuckets(0, 9, 2),
		},
		[]string{"result", "grpc_code"})
)

type metricsBuildExecutor struct {
	buildExecutor BuildExecutor
}

// NewMetricsBuildExecutor creates a decorator for BuildExecutor that
// exposes the statistics stored in ExecutedActionMetadata as Prometheus
// metrics.
func NewMetricsBuildExecutor(buildExecutor BuildExecutor) BuildExecutor {
	buildExecutorPrometheusMetrics.Do(func() {
		prometheus.MustRegister(buildExecutorDurationSeconds)

		prometheus.MustRegister(buildExecutorExecutionTimeoutCompensation)

		prometheus.MustRegister(buildExecutorFilePoolFilesCreated)
		prometheus.MustRegister(buildExecutorFilePoolFilesCountPeak)
		prometheus.MustRegister(buildExecutorFilePoolFilesSizeBytesPeak)
		prometheus.MustRegister(buildExecutorFilePoolFilesOperationsCount)
		prometheus.MustRegister(buildExecutorFilePoolFilesOperationsSizeBytes)

		prometheus.MustRegister(buildExecutorPOSIXUserTime)
		prometheus.MustRegister(buildExecutorPOSIXSystemTime)
		prometheus.MustRegister(buildExecutorPOSIXMaximumResidentSetSize)
		prometheus.MustRegister(buildExecutorPOSIXPageReclaims)
		prometheus.MustRegister(buildExecutorPOSIXPageFaults)
		prometheus.MustRegister(buildExecutorPOSIXSwaps)
		prometheus.MustRegister(buildExecutorPOSIXBlockInputOperations)
		prometheus.MustRegister(buildExecutorPOSIXBlockOutputOperations)
		prometheus.MustRegister(buildExecutorPOSIXMessagesSent)
		prometheus.MustRegister(buildExecutorPOSIXMessagesReceived)
		prometheus.MustRegister(buildExecutorPOSIXSignalsReceived)
		prometheus.MustRegister(buildExecutorPOSIXVoluntaryContextSwitches)
		prometheus.MustRegister(buildExecutorPOSIXInvoluntaryContextSwitches)
	})

	return &metricsBuildExecutor{
		buildExecutor: buildExecutor,
	}
}

func observeDuration(histogram prometheus.Observer, pb *durationpb.Duration) {
	if pb == nil {
		return
	}
	histogram.Observe(pb.AsDuration().Seconds())
}

func observeTimestampDelta(histogram prometheus.Observer, pbStart, pbCompleted *timestamppb.Timestamp) {
	if pbStart == nil || pbCompleted == nil {
		return
	}
	histogram.Observe(pbCompleted.AsTime().Sub(pbStart.AsTime()).Seconds())
}

func (be *metricsBuildExecutor) Execute(ctx context.Context, filePool filesystem.FilePool, instanceName digest.InstanceName, request *remoteworker.DesiredState_Executing, executionStateUpdates chan<- *remoteworker.CurrentState_Executing) *remoteexecution.ExecuteResponse {
	response := be.buildExecutor.Execute(ctx, filePool, instanceName, request, executionStateUpdates)
	result, grpcCode := getResultAndGRPCCodeFromExecuteResponse(response)

	// Expose metrics for timestamps stored in ExecutedActionMetadata.
	metadata := response.Result.ExecutionMetadata
	observeTimestampDelta(
		buildExecutorDurationSeconds.WithLabelValues(result, grpcCode, "FetchingInputs"),
		metadata.InputFetchStartTimestamp, metadata.InputFetchCompletedTimestamp)
	observeTimestampDelta(
		buildExecutorDurationSeconds.WithLabelValues(result, grpcCode, "Running"),
		metadata.ExecutionStartTimestamp, metadata.ExecutionCompletedTimestamp)
	observeTimestampDelta(
		buildExecutorDurationSeconds.WithLabelValues(result, grpcCode, "UploadingOutputs"),
		metadata.OutputUploadStartTimestamp, metadata.OutputUploadCompletedTimestamp)

	for _, auxiliaryMetadata := range metadata.AuxiliaryMetadata {
		var buildExecutor resourceusage.BuildExecutorResourceUsage
		var filePool resourceusage.FilePoolResourceUsage
		var posix resourceusage.POSIXResourceUsage
		if auxiliaryMetadata.UnmarshalTo(&buildExecutor) == nil {
			// Expose metrics stored in BuildExecutorResourceUsage.
			observeDuration(buildExecutorExecutionTimeoutCompensation.WithLabelValues(result, grpcCode), buildExecutor.ExecutionTimeoutCompensation)
		} else if auxiliaryMetadata.UnmarshalTo(&filePool) == nil {
			// Expose metrics stored in FilePoolResourceUsage.
			buildExecutorFilePoolFilesCreated.WithLabelValues(result, grpcCode).Observe(float64(filePool.FilesCreated))
			buildExecutorFilePoolFilesCountPeak.WithLabelValues(result, grpcCode).Observe(float64(filePool.FilesCountPeak))
			buildExecutorFilePoolFilesSizeBytesPeak.WithLabelValues(result, grpcCode).Observe(float64(filePool.FilesSizeBytesPeak))
			buildExecutorFilePoolFilesOperationsCount.WithLabelValues(result, grpcCode, "Read").Observe(float64(filePool.ReadsCount))
			buildExecutorFilePoolFilesOperationsSizeBytes.WithLabelValues(result, grpcCode, "Read").Observe(float64(filePool.ReadsSizeBytes))
			buildExecutorFilePoolFilesOperationsCount.WithLabelValues(result, grpcCode, "Write").Observe(float64(filePool.WritesCount))
			buildExecutorFilePoolFilesOperationsSizeBytes.WithLabelValues(result, grpcCode, "Write").Observe(float64(filePool.WritesSizeBytes))
			buildExecutorFilePoolFilesOperationsCount.WithLabelValues(result, grpcCode, "Truncate").Observe(float64(filePool.TruncatesCount))
		} else if auxiliaryMetadata.UnmarshalTo(&posix) == nil {
			// Expose metrics stored in POSIXResourceUsage.
			observeDuration(buildExecutorPOSIXUserTime.WithLabelValues(result, grpcCode), posix.UserTime)
			observeDuration(buildExecutorPOSIXSystemTime.WithLabelValues(result, grpcCode), posix.SystemTime)
			buildExecutorPOSIXMaximumResidentSetSize.WithLabelValues(result, grpcCode).Observe(float64(posix.MaximumResidentSetSize))
			buildExecutorPOSIXPageReclaims.WithLabelValues(result, grpcCode).Observe(float64(posix.PageReclaims))
			buildExecutorPOSIXPageFaults.WithLabelValues(result, grpcCode).Observe(float64(posix.PageFaults))
			buildExecutorPOSIXSwaps.WithLabelValues(result, grpcCode).Observe(float64(posix.Swaps))
			buildExecutorPOSIXBlockInputOperations.WithLabelValues(result, grpcCode).Observe(float64(posix.BlockInputOperations))
			buildExecutorPOSIXBlockOutputOperations.WithLabelValues(result, grpcCode).Observe(float64(posix.BlockOutputOperations))
			buildExecutorPOSIXMessagesSent.WithLabelValues(result, grpcCode).Observe(float64(posix.MessagesSent))
			buildExecutorPOSIXMessagesReceived.WithLabelValues(result, grpcCode).Observe(float64(posix.MessagesReceived))
			buildExecutorPOSIXSignalsReceived.WithLabelValues(result, grpcCode).Observe(float64(posix.SignalsReceived))
			buildExecutorPOSIXVoluntaryContextSwitches.WithLabelValues(result, grpcCode).Observe(float64(posix.VoluntaryContextSwitches))
			buildExecutorPOSIXInvoluntaryContextSwitches.WithLabelValues(result, grpcCode).Observe(float64(posix.InvoluntaryContextSwitches))
		}
	}

	return response
}
