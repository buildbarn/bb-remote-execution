package cleaner_test

import (
	"testing"
	"time"

	"github.com/buildbarn/bb-remote-execution/internal/mock"
	"github.com/buildbarn/bb-remote-execution/pkg/cleaner"
	"github.com/stretchr/testify/require"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"go.uber.org/mock/gomock"
)

func TestFilteringProcessTable(t *testing.T) {
	ctrl := gomock.NewController(t)

	baseProcessTable := mock.NewMockProcessTable(ctrl)
	processTable := cleaner.NewFilteringProcessTable(
		baseProcessTable,
		func(process *cleaner.Process) bool {
			return process.UserID == 123 &&
				process.CreationTime.After(time.Unix(1500000000, 0))
		})

	t.Run("Failure", func(t *testing.T) {
		baseProcessTable.EXPECT().GetProcesses().Return(nil, status.Error(codes.Internal, "Out of memory"))

		// Errors from the base process table should be forwarded.
		_, err := processTable.GetProcesses()
		require.Equal(t, status.Error(codes.Internal, "Out of memory"), err)
	})

	t.Run("Success", func(t *testing.T) {
		baseProcessTable.EXPECT().GetProcesses().Return([]cleaner.Process{
			// Process is running as a different user. It
			// should be left alone.
			{
				ProcessID:    1,
				UserID:       122,
				CreationTime: time.Unix(1600000000, 0),
			},
			// Process is running as the right user, but it
			// was started earlier on. It should be left
			// alone, as it may be important to the system.
			{
				ProcessID:    2,
				UserID:       123,
				CreationTime: time.Unix(1400000000, 0),
			},
			// Process that should be matched.
			{
				ProcessID:    3,
				UserID:       123,
				CreationTime: time.Unix(1600000000, 0),
			},
		}, nil)

		processes, err := processTable.GetProcesses()
		require.NoError(t, err)
		require.Equal(t, []cleaner.Process{
			{
				ProcessID:    3,
				UserID:       123,
				CreationTime: time.Unix(1600000000, 0),
			},
		}, processes)
	})
}
