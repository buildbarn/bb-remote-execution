package pool_test

import (
	"testing"

	"github.com/buildbarn/bb-remote-execution/internal/mock"
	"github.com/buildbarn/bb-remote-execution/pkg/filesystem/pool"
	"github.com/buildbarn/bb-storage/pkg/filesystem"
	"github.com/stretchr/testify/require"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"go.uber.org/mock/gomock"
)

// testRemainingFileQuota is a helper function for the
// QuotaEnforcingFilePool tests to check that a certain amount of space
// is available within the pool.
func testRemainingFileQuota(t *testing.T, ctrl *gomock.Controller, underlyingPool *mock.MockFilePool, filePool pool.FilePool, filesRemaining int) {
	// Check that the remaining number of files is available by
	// allocating all of them.
	underlyingFiles := make([]*mock.MockFileReadWriter, filesRemaining)
	files := make([]filesystem.FileReadWriter, filesRemaining)
	for i := 0; i < filesRemaining; i++ {
		underlyingFiles[i] = mock.NewMockFileReadWriter(ctrl)
		underlyingPool.EXPECT().NewFile(pool.ZeroHoleSource, uint64(0)).Return(underlyingFiles[i], nil)
		var err error
		files[i], err = filePool.NewFile(pool.ZeroHoleSource, 0)
		require.NoError(t, err)
	}
	_, err := filePool.NewFile(pool.ZeroHoleSource, 0)
	require.Equal(t, err, status.Error(codes.InvalidArgument, "File count quota reached"))
	for i := 0; i < filesRemaining; i++ {
		underlyingFiles[i].EXPECT().Close()
		require.NoError(t, files[i].Close())
	}
}

func TestQuotaEnforcingFilePoolExample(t *testing.T) {
	ctrl := gomock.NewController(t)

	// An empty pool should have the advertised amount of space available.
	underlyingPool := mock.NewMockFilePool(ctrl)
	filePool := pool.NewQuotaEnforcingFilePool(underlyingPool, 10)
	testRemainingFileQuota(t, ctrl, underlyingPool, filePool, 10)

	// Failure to allocate a file from the underlying pool should
	// not affect the quota.
	underlyingPool.EXPECT().NewFile(pool.ZeroHoleSource, uint64(0)).Return(nil, status.Error(codes.Internal, "I/O error"))
	_, err := filePool.NewFile(pool.ZeroHoleSource, 0)
	require.Equal(t, err, status.Error(codes.Internal, "I/O error"))
	testRemainingFileQuota(t, ctrl, underlyingPool, filePool, 10)

	// Successfully allocate a file.
	underlyingFile1 := mock.NewMockFileReadWriter(ctrl)
	underlyingPool.EXPECT().NewFile(pool.ZeroHoleSource, uint64(0)).Return(underlyingFile1, nil)
	f1, err := filePool.NewFile(pool.ZeroHoleSource, 0)
	require.NoError(t, err)
	testRemainingFileQuota(t, ctrl, underlyingPool, filePool, 9)

	// Closing the file should bring the pool back in the initial
	// state.
	underlyingFile1.EXPECT().Close().Return(nil)
	require.NoError(t, f1.Close())
	testRemainingFileQuota(t, ctrl, underlyingPool, filePool, 10)
}
