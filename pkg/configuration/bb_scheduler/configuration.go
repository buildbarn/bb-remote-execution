package configuration

import (
	pb "github.com/buildbarn/bb-remote-execution/pkg/proto/configuration/bb_scheduler"
	"github.com/buildbarn/bb-storage/pkg/util"
)

// GetSchedulerConfiguration reads the configuration from file and fill in default values.
func GetSchedulerConfiguration(path string) (*pb.SchedulerConfiguration, error) {
	var schedulerConfiguration pb.SchedulerConfiguration
	if err := util.UnmarshalConfigurationFromFile(path, &schedulerConfiguration); err != nil {
		return nil, util.StatusWrap(err, "Failed to retrieve configuration")
	}
	setDefaultSchedulerValues(&schedulerConfiguration)
	return &schedulerConfiguration, nil
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
