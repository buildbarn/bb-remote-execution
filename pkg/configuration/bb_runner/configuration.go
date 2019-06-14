package configuration

import (
	"os"

	pb "github.com/buildbarn/bb-remote-execution/pkg/proto/configuration/bb_runner"
	"github.com/golang/protobuf/jsonpb"
)

// GetRunnerConfiguration reads the configuration from file and fill in default values.
func GetRunnerConfiguration(path string) (*pb.RunnerConfiguration, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	var runnerConfiguration pb.RunnerConfiguration
	if err := jsonpb.Unmarshal(file, &runnerConfiguration); err != nil {
		return nil, err
	}
	setDefaultRunnerValues(&runnerConfiguration)
	return &runnerConfiguration, err
}

func setDefaultRunnerValues(runnerConfiguration *pb.RunnerConfiguration) {
	if runnerConfiguration.ListenPath == "" {
		runnerConfiguration.ListenPath = "/worker/runner"
	}
	if runnerConfiguration.BuildDirectoryPath == "" {
		runnerConfiguration.BuildDirectoryPath = "/worker/build"
	}
}
