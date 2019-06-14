package configuration

import (
	"os"

	pb "github.com/buildbarn/bb-remote-execution/pkg/proto/configuration/bb_scheduler"
	"github.com/golang/protobuf/jsonpb"
)

// GetSchedulerConfiguration reads the configuration from file and fill in default values.
func GetSchedulerConfiguration(path string) (*pb.SchedulerConfiguration, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	var schedulerConfiguration pb.SchedulerConfiguration
	if err := jsonpb.Unmarshal(file, &schedulerConfiguration); err != nil {
		return nil, err
	}
	setDefaultSchedulerValues(&schedulerConfiguration)
	return &schedulerConfiguration, err
}

func setDefaultSchedulerValues(schedulerConfiguration *pb.SchedulerConfiguration) {
	if schedulerConfiguration.JobsPendingMax == 0 {
		schedulerConfiguration.JobsPendingMax = 100
	}
	if schedulerConfiguration.MetricsListenAddress == "" {
		schedulerConfiguration.MetricsListenAddress = ":80"
	}
	if schedulerConfiguration.GrpcListenAddress == "" {
		schedulerConfiguration.GrpcListenAddress = ":8981"
	}
}
