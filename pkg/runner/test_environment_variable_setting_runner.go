package runner

import (
	"context"
	"path"
	"sync"

	runner_pb "github.com/buildbarn/bb-remote-execution/pkg/proto/runner"
	"github.com/prometheus/client_golang/prometheus"
)

var (
	testEnvironmentVariableSettingRunnerPrometheusMetrics sync.Once
	testEnvironmentVariableSettingRunnerMatches           = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "buildbarn",
			Subsystem: "runner",
			Name:      "specific_test_environment_variable_setting_runner_matches_total",
			Help:      "Number of times a test was executed that had specific environment variables set.",
		},
		[]string{"test_target"})
)

type testEnvironmentVariableSettingRunner struct {
	runner_pb.RunnerServer
	testTargets          map[string]struct{}
	environmentVariables map[string]string
}

// NewTestEnvironmentVariableSettingRunner creates a decorator for
// RunnerServer that injects environment variables into a specific set
// of Bazel tests.
//
// This can be used to conditionally give Bazel tests access to security
// tokens that are set through environment variables.
func NewTestEnvironmentVariableSettingRunner(base runner_pb.RunnerServer, testTargets map[string]struct{}, environmentVariables map[string]string) runner_pb.RunnerServer {
	testEnvironmentVariableSettingRunnerPrometheusMetrics.Do(func() {
		prometheus.MustRegister(testEnvironmentVariableSettingRunnerMatches)
	})
	for testTarget := range testTargets {
		testEnvironmentVariableSettingRunnerMatches.WithLabelValues(testTarget)
	}

	return &testEnvironmentVariableSettingRunner{
		RunnerServer:         base,
		testTargets:          testTargets,
		environmentVariables: environmentVariables,
	}
}

func (r *testEnvironmentVariableSettingRunner) Run(ctx context.Context, request *runner_pb.RunRequest) (*runner_pb.RunResponse, error) {
	if len(request.Arguments) >= 1 && path.Base(request.Arguments[0]) == "test-setup.sh" {
		if testTarget, ok := request.EnvironmentVariables["TEST_TARGET"]; ok {
			if _, ok := r.testTargets[testTarget]; ok {
				// Attempted to execute a test that has a
				// matching target name. Copy the request and
				// add the desired environment variables.
				testEnvironmentVariableSettingRunnerMatches.WithLabelValues(testTarget).Inc()

				newRequest := *request
				newRequest.EnvironmentVariables = make(map[string]string, len(r.environmentVariables)+len(request.EnvironmentVariables))
				for name, value := range r.environmentVariables {
					newRequest.EnvironmentVariables[name] = value
				}
				for name, value := range request.EnvironmentVariables {
					newRequest.EnvironmentVariables[name] = value
				}
				return r.RunnerServer.Run(ctx, &newRequest)
			}
		}
	}
	return r.RunnerServer.Run(ctx, request)
}
