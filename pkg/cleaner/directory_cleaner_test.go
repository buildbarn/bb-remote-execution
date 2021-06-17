package cleaner_test

import (
	"context"
	"syscall"
	"testing"

	"github.com/buildbarn/bb-remote-execution/internal/mock"
	"github.com/buildbarn/bb-remote-execution/pkg/cleaner"
	"github.com/buildbarn/bb-storage/pkg/testutil"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestDirectoryCleaner(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	t.Run("Failure", func(t *testing.T) {
		directory := mock.NewMockDirectory(ctrl)
		directory.EXPECT().RemoveAllChildren().Return(syscall.EACCES)

		testutil.RequireEqualStatus(
			t,
			status.Error(codes.Internal, "Failed to clean directory \"/tmp\": permission denied"),
			cleaner.NewDirectoryCleaner(directory, "/tmp")(ctx))
	})

	t.Run("Success", func(t *testing.T) {
		directory := mock.NewMockDirectory(ctrl)
		directory.EXPECT().RemoveAllChildren()

		require.NoError(t, cleaner.NewDirectoryCleaner(directory, "/tmp")(ctx))
	})
}
