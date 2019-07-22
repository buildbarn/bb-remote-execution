package configuration

import (
	"os"

	pb "github.com/buildbarn/bb-remote-execution/pkg/proto/configuration/bb_worker"
	"github.com/golang/protobuf/jsonpb"
)

// GetWorkerConfiguration reads the configuration from file and fill in default values.
func GetWorkerConfiguration(path string) (*pb.WorkerConfiguration, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	var workerConfiguration pb.WorkerConfiguration
	if err := jsonpb.Unmarshal(file, &workerConfiguration); err != nil {
		return nil, err
	}
	setDefaultWorkerValues(&workerConfiguration)
	return &workerConfiguration, err
}

func setDefaultWorkerValues(workerConfiguration *pb.WorkerConfiguration) {
	if workerConfiguration.BuildDirectoryPath == "" {
		workerConfiguration.BuildDirectoryPath = "/worker/build"
	}
	if workerConfiguration.CacheDirectoryPath == "" {
		workerConfiguration.CacheDirectoryPath = "/worker/cache"
	}
	if workerConfiguration.MaximumCacheFileCount == 0 {
		workerConfiguration.MaximumCacheFileCount = 10000
	}
	if workerConfiguration.MaximumCacheSizeBytes == 0 {
		workerConfiguration.MaximumCacheSizeBytes = 1024 * 1024 * 1024
	}
	if workerConfiguration.MaximumMemoryCachedDirectories == 0 {
		workerConfiguration.MaximumMemoryCachedDirectories = 1000
	}
	if workerConfiguration.Concurrency == 0 {
		workerConfiguration.Concurrency = 1
	}
	if workerConfiguration.MaximumMessageSizeBytes == 0 {
		workerConfiguration.MaximumMessageSizeBytes = 16 * 1024 * 1024
	}
	if workerConfiguration.RunnerAddress == "" {
		workerConfiguration.RunnerAddress = "unix:///worker/runner"
	}
	if workerConfiguration.MetricsListenAddress == "" {
		workerConfiguration.MetricsListenAddress = ":80"
	}
}
