package configuration

import (
	pb "github.com/buildbarn/bb-remote-execution/pkg/proto/configuration/bb_runner"
	"github.com/buildbarn/bb-storage/pkg/util"
)

// GetRunnerConfiguration reads the configuration from file and fill in default values.
func GetRunnerConfiguration(path string) (*pb.RunnerConfiguration, error) {
	var runnerConfiguration pb.RunnerConfiguration
	if err := util.UnmarshalConfigurationFromFile(path, &runnerConfiguration); err != nil {
		return nil, util.StatusWrap(err, "Failed to retrieve configuration")
	}
	setDefaultRunnerValues(&runnerConfiguration)
	return &runnerConfiguration, nil
}

func setDefaultRunnerValues(runnerConfiguration *pb.RunnerConfiguration) {
	if runnerConfiguration.ListenPath == "" {
		runnerConfiguration.ListenPath = "/worker/runner"
	}
	if runnerConfiguration.BuildDirectoryPath == "" {
		runnerConfiguration.BuildDirectoryPath = "/worker/build"
	}
}
