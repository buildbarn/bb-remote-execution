package cleaner_test

import (
	"context"
	"testing"

	"github.com/buildbarn/bb-remote-execution/pkg/cleaner"
	"github.com/buildbarn/bb-storage/pkg/testutil"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestCommandRunningCleaner(t *testing.T) {
	_, ctx := gomock.WithContext(context.Background(), t)

	t.Run("Failure", func(t *testing.T) {
		testutil.RequireEqualStatus(
			t,
			status.Error(codes.Internal, "Failed to run cleaning command: exit status 1"),
			cleaner.NewCommandRunningCleaner("false", nil)(ctx))
	})

	t.Run("Success", func(t *testing.T) {
		require.NoError(t, cleaner.NewCommandRunningCleaner("true", nil)(ctx))
	})
}
