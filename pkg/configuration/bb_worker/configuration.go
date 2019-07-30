package configuration

import (
	pb "github.com/buildbarn/bb-remote-execution/pkg/proto/configuration/bb_worker"
	"github.com/buildbarn/bb-storage/pkg/util"
)

// GetWorkerConfiguration reads the configuration from file and fill in default values.
func GetWorkerConfiguration(path string) (*pb.WorkerConfiguration, error) {
	var workerConfiguration pb.WorkerConfiguration
	if err := util.UnmarshalConfigurationFromFile(path, &workerConfiguration); err != nil {
		return nil, util.StatusWrap(err, "Failed to retrieve configuration")
	}
	setDefaultWorkerValues(&workerConfiguration)
	return &workerConfiguration, nil
}

func setDefaultWorkerValues(workerConfiguration *pb.WorkerConfiguration) {
	if workerConfiguration.BuildDirectoryPath == "" {
		workerConfiguration.BuildDirectoryPath = "/worker/build"
	}
	if workerConfiguration.CacheDirectoryPath == "" {
		workerConfiguration.CacheDirectoryPath = "/worker/cache"
	}
	if workerConfiguration.Concurrency == 0 {
		workerConfiguration.Concurrency = 1
	}
	if workerConfiguration.MaximumMessageSizeBytes == 0 {
		workerConfiguration.MaximumMessageSizeBytes = 16*1024*1024
	}
	if workerConfiguration.RunnerAddress == "" {
		workerConfiguration.RunnerAddress = "unix:///worker/runner"
	}
	if workerConfiguration.MetricsListenAddress == "" {
		workerConfiguration.MetricsListenAddress = ":80"
	}
}

