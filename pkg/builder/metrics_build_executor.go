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
	"github.com/golang/protobuf/ptypes"
	"github.com/golang/protobuf/ptypes/duration"
	"github.com/golang/protobuf/ptypes/timestamp"
	"github.com/prometheus/client_golang/prometheus"
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
		[]string{"stage"})
	buildExecutorDurationSecondsInputFetch   = buildExecutorDurationSeconds.WithLabelValues("FetchingInputs")
	buildExecutorDurationSecondsExecution    = buildExecutorDurationSeconds.WithLabelValues("Running")
	buildExecutorDurationSecondsOutputUpload = buildExecutorDurationSeconds.WithLabelValues("UploadingOutputs")

	// Metrics for FilePoolResourceUsage.
	buildExecutorFilePoolFilesCreated = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "buildbarn",
			Subsystem: "builder",
			Name:      "build_executor_file_pool_files_created",
			Help:      "Number of files created by a build action.",
			Buckets:   util.DecimalExponentialBuckets(0, 6, 2),
		})
	buildExecutorFilePoolFilesCountPeak = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "buildbarn",
			Subsystem: "builder",
			Name:      "build_executor_file_pool_files_count_peak",
			Help:      "Peak number of files created by a build action.",
			Buckets:   util.DecimalExponentialBuckets(0, 6, 2),
		})
	buildExecutorFilePoolFilesSizeBytesPeak = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "buildbarn",
			Subsystem: "builder",
			Name:      "build_executor_file_pool_files_size_bytes_peak",
			Help:      "Peak size of files created by a build action, in bytes.",
			Buckets:   prometheus.ExponentialBuckets(1.0, 2.0, 33),
		})
	buildExecutorFilePoolFilesOperationsCount = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "buildbarn",
			Subsystem: "builder",
			Name:      "build_executor_file_pool_operations_count",
			Help:      "Number of file pool operations performed by build actions.",
			Buckets:   util.DecimalExponentialBuckets(0, 6, 2),
		},
		[]string{"operation"})
	buildExecutorFilePoolFilesOperationsCountRead     = buildExecutorFilePoolFilesOperationsCount.WithLabelValues("Read")
	buildExecutorFilePoolFilesOperationsCountWrite    = buildExecutorFilePoolFilesOperationsCount.WithLabelValues("Write")
	buildExecutorFilePoolFilesOperationsCountTruncate = buildExecutorFilePoolFilesOperationsCount.WithLabelValues("Truncate")
	buildExecutorFilePoolFilesOperationsSizeBytes     = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "buildbarn",
			Subsystem: "builder",
			Name:      "build_executor_file_pool_operations_size_bytes",
			Help:      "Total size of file pool operations performed by build actions, in bytes.",
			Buckets:   prometheus.ExponentialBuckets(1.0, 2.0, 33),
		},
		[]string{"operation"})
	buildExecutorFilePoolFilesOperationsSizeBytesRead  = buildExecutorFilePoolFilesOperationsSizeBytes.WithLabelValues("Read")
	buildExecutorFilePoolFilesOperationsSizeBytesWrite = buildExecutorFilePoolFilesOperationsSizeBytes.WithLabelValues("Write")

	// Metrics for POSIXResourceUsage.
	buildExecutorPOSIXUserTime = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "buildbarn",
			Subsystem: "builder",
			Name:      "build_executor_posix_user_time",
			Help:      "Amount of time spent in userspace by build actions, in seconds.",
			Buckets:   util.DecimalExponentialBuckets(-3, 6, 2),
		})
	buildExecutorPOSIXSystemTime = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "buildbarn",
			Subsystem: "builder",
			Name:      "build_executor_posix_system_time",
			Help:      "Amount of time spent in kernelspace by build actions, in seconds.",
			Buckets:   util.DecimalExponentialBuckets(-3, 6, 2),
		})
	buildExecutorPOSIXMaximumResidentSetSize = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "buildbarn",
			Subsystem: "builder",
			Name:      "build_executor_posix_maximum_resident_set_size",
			Help:      "Maximum resident set size of build actions, in bytes.",
			Buckets:   prometheus.ExponentialBuckets(1024.0, 2.0, 23),
		})
	buildExecutorPOSIXPageReclaims = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "buildbarn",
			Subsystem: "builder",
			Name:      "build_executor_posix_page_reclaims",
			Help:      "Number of page reclaims caused by build actions.",
			Buckets:   util.DecimalExponentialBuckets(0, 9, 2),
		})
	buildExecutorPOSIXPageFaults = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "buildbarn",
			Subsystem: "builder",
			Name:      "build_executor_posix_page_faults",
			Help:      "Number of page faults caused by build actions.",
			Buckets:   util.DecimalExponentialBuckets(0, 9, 2),
		})
	buildExecutorPOSIXSwaps = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "buildbarn",
			Subsystem: "builder",
			Name:      "build_executor_posix_swaps",
			Help:      "Number of swaps caused by build actions.",
			Buckets:   util.DecimalExponentialBuckets(0, 9, 2),
		})
	buildExecutorPOSIXBlockInputOperations = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "buildbarn",
			Subsystem: "builder",
			Name:      "build_executor_posix_block_input_operations",
			Help:      "Number of block input operations performed by build actions.",
			Buckets:   util.DecimalExponentialBuckets(0, 9, 2),
		})
	buildExecutorPOSIXBlockOutputOperations = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "buildbarn",
			Subsystem: "builder",
			Name:      "build_executor_posix_block_output_operations",
			Help:      "Number of block output operations performed by build actions.",
			Buckets:   util.DecimalExponentialBuckets(0, 9, 2),
		})
	buildExecutorPOSIXMessagesSent = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "buildbarn",
			Subsystem: "builder",
			Name:      "build_executor_posix_messages_sent",
			Help:      "Number of messages sent by build actions.",
			Buckets:   util.DecimalExponentialBuckets(0, 9, 2),
		})
	buildExecutorPOSIXMessagesReceived = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "buildbarn",
			Subsystem: "builder",
			Name:      "build_executor_posix_messages_received",
			Help:      "Number of messages received by build actions.",
			Buckets:   util.DecimalExponentialBuckets(0, 9, 2),
		})
	buildExecutorPOSIXSignalsReceived = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "buildbarn",
			Subsystem: "builder",
			Name:      "build_executor_posix_signals_received",
			Help:      "Number of signals received by build actions.",
			Buckets:   util.DecimalExponentialBuckets(0, 6, 2),
		})
	buildExecutorPOSIXVoluntaryContextSwitches = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "buildbarn",
			Subsystem: "builder",
			Name:      "build_executor_posix_voluntary_context_switches",
			Help:      "Number of voluntary context switches caused by build actions.",
			Buckets:   util.DecimalExponentialBuckets(0, 9, 2),
		})
	buildExecutorPOSIXInvoluntaryContextSwitches = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "buildbarn",
			Subsystem: "builder",
			Name:      "build_executor_posix_involuntary_context_switches",
			Help:      "Number of involuntary context switches caused by build actions.",
			Buckets:   util.DecimalExponentialBuckets(0, 9, 2),
		})
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

func observeDuration(histogram prometheus.Observer, pb *duration.Duration) {
	if pb == nil {
		return
	}
	d, err := ptypes.Duration(pb)
	if err != nil {
		return
	}
	histogram.Observe(d.Seconds())
}

func observeTimestampDelta(histogram prometheus.Observer, pbStart *timestamp.Timestamp, pbCompleted *timestamp.Timestamp) {
	if pbStart == nil || pbCompleted == nil {
		return
	}
	tStart, err := ptypes.Timestamp(pbStart)
	if err != nil {
		return
	}
	tCompleted, err := ptypes.Timestamp(pbCompleted)
	if err != nil {
		return
	}
	histogram.Observe(tCompleted.Sub(tStart).Seconds())
}

func (be *metricsBuildExecutor) Execute(ctx context.Context, filePool filesystem.FilePool, instanceName digest.InstanceName, request *remoteworker.DesiredState_Executing, executionStateUpdates chan<- *remoteworker.CurrentState_Executing) *remoteexecution.ExecuteResponse {
	response := be.buildExecutor.Execute(ctx, filePool, instanceName, request, executionStateUpdates)

	// Expose metrics for timestamps stored in ExecutedActionMetadata.
	metadata := response.Result.ExecutionMetadata
	observeTimestampDelta(buildExecutorDurationSecondsInputFetch, metadata.InputFetchStartTimestamp, metadata.InputFetchCompletedTimestamp)
	observeTimestampDelta(buildExecutorDurationSecondsExecution, metadata.ExecutionStartTimestamp, metadata.ExecutionCompletedTimestamp)
	observeTimestampDelta(buildExecutorDurationSecondsOutputUpload, metadata.OutputUploadStartTimestamp, metadata.OutputUploadCompletedTimestamp)

	for _, auxiliaryMetadata := range metadata.AuxiliaryMetadata {
		var filePool resourceusage.FilePoolResourceUsage
		var posix resourceusage.POSIXResourceUsage
		if ptypes.UnmarshalAny(auxiliaryMetadata, &filePool) == nil {
			// Expose metrics stored in FilePoolResourceUsage.
			buildExecutorFilePoolFilesCreated.Observe(float64(filePool.FilesCreated))
			buildExecutorFilePoolFilesCountPeak.Observe(float64(filePool.FilesCountPeak))
			buildExecutorFilePoolFilesSizeBytesPeak.Observe(float64(filePool.FilesSizeBytesPeak))
			buildExecutorFilePoolFilesOperationsCountRead.Observe(float64(filePool.ReadsCount))
			buildExecutorFilePoolFilesOperationsSizeBytesRead.Observe(float64(filePool.ReadsSizeBytes))
			buildExecutorFilePoolFilesOperationsCountWrite.Observe(float64(filePool.WritesCount))
			buildExecutorFilePoolFilesOperationsSizeBytesWrite.Observe(float64(filePool.WritesSizeBytes))
			buildExecutorFilePoolFilesOperationsCountTruncate.Observe(float64(filePool.TruncatesCount))
		} else if ptypes.UnmarshalAny(auxiliaryMetadata, &posix) == nil {
			// Expose metrics stored in POSIXResourceUsage.
			observeDuration(buildExecutorPOSIXUserTime, posix.UserTime)
			observeDuration(buildExecutorPOSIXSystemTime, posix.SystemTime)
			buildExecutorPOSIXMaximumResidentSetSize.Observe(float64(posix.MaximumResidentSetSize))
			buildExecutorPOSIXPageReclaims.Observe(float64(posix.PageReclaims))
			buildExecutorPOSIXPageFaults.Observe(float64(posix.PageFaults))
			buildExecutorPOSIXSwaps.Observe(float64(posix.Swaps))
			buildExecutorPOSIXBlockInputOperations.Observe(float64(posix.BlockInputOperations))
			buildExecutorPOSIXBlockOutputOperations.Observe(float64(posix.BlockOutputOperations))
			buildExecutorPOSIXMessagesSent.Observe(float64(posix.MessagesSent))
			buildExecutorPOSIXMessagesReceived.Observe(float64(posix.MessagesReceived))
			buildExecutorPOSIXSignalsReceived.Observe(float64(posix.SignalsReceived))
			buildExecutorPOSIXVoluntaryContextSwitches.Observe(float64(posix.VoluntaryContextSwitches))
			buildExecutorPOSIXInvoluntaryContextSwitches.Observe(float64(posix.InvoluntaryContextSwitches))
		}
	}

	return response
}
