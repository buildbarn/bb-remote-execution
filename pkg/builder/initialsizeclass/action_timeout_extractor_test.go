package initialsizeclass_test

import (
	"testing"
	"time"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-remote-execution/pkg/builder/initialsizeclass"
	"github.com/buildbarn/bb-storage/pkg/testutil"
	"github.com/stretchr/testify/require"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"
)

func TestActionTimeoutExtractor(t *testing.T) {
	actionTimeoutExtractor := initialsizeclass.NewActionTimeoutExtractor(
		/* defaultExecutionTimeout = */ time.Hour,
		/* maximumExecutionTimeout = */ 2*time.Hour)

	t.Run("DefaultExecutionTimeout", func(t *testing.T) {
		// Specifying no timeout should yield the default.
		timeout, err := actionTimeoutExtractor.ExtractTimeout(&remoteexecution.Action{
			CommandDigest: &remoteexecution.Digest{
				Hash:      "3f146c6bc1789053afb12db35f6e91ff",
				SizeBytes: 104,
			},
			InputRootDigest: &remoteexecution.Digest{
				Hash:      "6fb86fab077bc2023cc1419cbc28998c",
				SizeBytes: 493,
			},
		})
		require.NoError(t, err)
		require.Equal(t, time.Hour, timeout)
	})

	t.Run("InvalidTimeout", func(t *testing.T) {
		_, err := actionTimeoutExtractor.ExtractTimeout(&remoteexecution.Action{
			CommandDigest: &remoteexecution.Digest{
				Hash:      "3f146c6bc1789053afb12db35f6e91ff",
				SizeBytes: 104,
			},
			InputRootDigest: &remoteexecution.Digest{
				Hash:      "6fb86fab077bc2023cc1419cbc28998c",
				SizeBytes: 493,
			},
			Timeout: &durationpb.Duration{Nanos: 1000000000},
		})
		testutil.RequirePrefixedStatus(t, status.Error(codes.InvalidArgument, "Invalid execution timeout: "), err)
	})

	t.Run("TimeoutTooLow", func(t *testing.T) {
		_, err := actionTimeoutExtractor.ExtractTimeout(&remoteexecution.Action{
			CommandDigest: &remoteexecution.Digest{
				Hash:      "3f146c6bc1789053afb12db35f6e91ff",
				SizeBytes: 104,
			},
			InputRootDigest: &remoteexecution.Digest{
				Hash:      "6fb86fab077bc2023cc1419cbc28998c",
				SizeBytes: 493,
			},
			Timeout: &durationpb.Duration{Seconds: -1},
		})
		testutil.RequireEqualStatus(t, status.Error(codes.InvalidArgument, "Execution timeout of -1s is outside permitted range [0s, 2h0m0s]"), err)
	})

	t.Run("TimeoutTooHigh", func(t *testing.T) {
		_, err := actionTimeoutExtractor.ExtractTimeout(&remoteexecution.Action{
			CommandDigest: &remoteexecution.Digest{
				Hash:      "3f146c6bc1789053afb12db35f6e91ff",
				SizeBytes: 104,
			},
			InputRootDigest: &remoteexecution.Digest{
				Hash:      "6fb86fab077bc2023cc1419cbc28998c",
				SizeBytes: 493,
			},
			Timeout: &durationpb.Duration{Seconds: 7201},
		})
		testutil.RequireEqualStatus(t, status.Error(codes.InvalidArgument, "Execution timeout of 2h0m1s is outside permitted range [0s, 2h0m0s]"), err)
	})

	t.Run("Success", func(t *testing.T) {
		timeout, err := actionTimeoutExtractor.ExtractTimeout(&remoteexecution.Action{
			CommandDigest: &remoteexecution.Digest{
				Hash:      "3f146c6bc1789053afb12db35f6e91ff",
				SizeBytes: 104,
			},
			InputRootDigest: &remoteexecution.Digest{
				Hash:      "6fb86fab077bc2023cc1419cbc28998c",
				SizeBytes: 493,
			},
			Timeout: &durationpb.Duration{Seconds: 900},
		})
		require.NoError(t, err)
		require.Equal(t, 15*time.Minute, timeout)
	})
}
