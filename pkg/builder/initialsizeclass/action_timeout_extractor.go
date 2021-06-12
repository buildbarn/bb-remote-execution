package initialsizeclass

import (
	"time"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-storage/pkg/util"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// ActionTimeoutExtractor is a helper type for extracting the execution
// timeout from an REv2 Action, taking configured default and maximum
// timeout values into account.
type ActionTimeoutExtractor struct {
	defaultExecutionTimeout time.Duration
	maximumExecutionTimeout time.Duration
}

// NewActionTimeoutExtractor creates a new ActionTimeoutExtractor using
// the parameters provided.
func NewActionTimeoutExtractor(defaultExecutionTimeout, maximumExecutionTimeout time.Duration) *ActionTimeoutExtractor {
	return &ActionTimeoutExtractor{
		defaultExecutionTimeout: defaultExecutionTimeout,
		maximumExecutionTimeout: maximumExecutionTimeout,
	}
}

// ExtractTimeout extracts the execution timeout field from an REv2
// Action, converting it to a time.Duration. It returns errors in case
// the provided execution timeout is invalid or out of bounds.
func (e ActionTimeoutExtractor) ExtractTimeout(action *remoteexecution.Action) (time.Duration, error) {
	if action.Timeout == nil {
		return e.defaultExecutionTimeout, nil
	}
	if err := action.Timeout.CheckValid(); err != nil {
		return 0, util.StatusWrapWithCode(err, codes.InvalidArgument, "Invalid execution timeout")
	}
	executionTimeout := action.Timeout.AsDuration()
	if executionTimeout < 0 || executionTimeout > e.maximumExecutionTimeout {
		return 0, status.Errorf(
			codes.InvalidArgument,
			"Execution timeout of %s is outside permitted range [0s, %s]",
			executionTimeout,
			e.maximumExecutionTimeout)
	}
	return executionTimeout, nil
}
