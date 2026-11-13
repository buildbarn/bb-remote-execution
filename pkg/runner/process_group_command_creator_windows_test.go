//go:build windows
// +build windows

package runner_test

import (
	"testing"

	"github.com/buildbarn/bb-remote-execution/pkg/runner"
	"github.com/buildbarn/bb-storage/pkg/testutil"
	"github.com/stretchr/testify/require"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestProcessGroupCommandCreatorUnsupported(t *testing.T) {
	creator, err := runner.NewProcessGroupCommandCreator(runner.NewPlainCommandCreator(nil))
	require.Nil(t, creator)
	testutil.RequireEqualStatus(t, status.Error(codes.InvalidArgument, "Process group cancellation is not supported on Windows"), err)
}
