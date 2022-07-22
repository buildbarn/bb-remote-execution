package filesystem_test

import (
	"io"
	"testing"

	"github.com/buildbarn/bb-remote-execution/internal/mock"
	re_filesystem "github.com/buildbarn/bb-remote-execution/pkg/filesystem"
	"github.com/buildbarn/bb-storage/pkg/filesystem"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// testRemainingQuota is a helper function for the
// QuotaEnforcingFilePool tests to check that a certain amount of space
// is available within the pool.
func testRemainingQuota(t *testing.T, ctrl *gomock.Controller, underlyingPool *mock.MockFilePool, pool re_filesystem.FilePool, filesRemaining int, bytesRemaining int64) {
	// Check that the remaining number of files is available by
	// allocating all of them.
	underlyingFiles := make([]*mock.MockFileReadWriter, filesRemaining)
	files := make([]filesystem.FileReadWriter, filesRemaining)
	for i := 0; i < filesRemaining; i++ {
		underlyingFiles[i] = mock.NewMockFileReadWriter(ctrl)
		underlyingPool.EXPECT().NewFile().Return(underlyingFiles[i], nil)
		var err error
		files[i], err = pool.NewFile()
		require.NoError(t, err)
	}
	_, err := pool.NewFile()
	require.Equal(t, err, status.Error(codes.InvalidArgument, "File count quota reached"))
	for i := 0; i < filesRemaining; i++ {
		underlyingFiles[i].EXPECT().Close().Return(nil)
		require.NoError(t, files[i].Close())
	}

	// Check that the remaining amount of space is available by
	// allocating one file and truncating it to the exact size.
	underlyingFile := mock.NewMockFileReadWriter(ctrl)
	underlyingPool.EXPECT().NewFile().Return(underlyingFile, nil)
	f, err := pool.NewFile()
	require.NoError(t, err)
	if bytesRemaining != 0 {
		underlyingFile.EXPECT().Truncate(bytesRemaining).Return(nil)
	}
	require.NoError(t, f.Truncate(bytesRemaining))
	require.Equal(t, f.Truncate(bytesRemaining+1), status.Error(codes.InvalidArgument, "File size quota reached"))
	underlyingFile.EXPECT().Close().Return(nil)
	require.NoError(t, f.Close())
}

func TestQuotaEnforcingFilePoolExample(t *testing.T) {
	ctrl := gomock.NewController(t)

	// An empty pool should have the advertised amount of space available.
	underlyingPool := mock.NewMockFilePool(ctrl)
	pool := re_filesystem.NewQuotaEnforcingFilePool(underlyingPool, 10, 1000)
	testRemainingQuota(t, ctrl, underlyingPool, pool, 10, 1000)

	// Failure to allocate a file from the underlying pool should
	// not affect the quota.
	underlyingPool.EXPECT().NewFile().Return(nil, status.Error(codes.Internal, "I/O error"))
	_, err := pool.NewFile()
	require.Equal(t, err, status.Error(codes.Internal, "I/O error"))
	testRemainingQuota(t, ctrl, underlyingPool, pool, 10, 1000)

	// Successfully allocate a file.
	underlyingFile := mock.NewMockFileReadWriter(ctrl)
	underlyingPool.EXPECT().NewFile().Return(underlyingFile, nil)
	f, err := pool.NewFile()
	require.NoError(t, err)
	testRemainingQuota(t, ctrl, underlyingPool, pool, 9, 1000)

	// Read calls should be forwarded properly.
	var p [10]byte
	underlyingFile.EXPECT().ReadAt(p[:], int64(123)).Return(0, io.EOF)
	n, err := f.ReadAt(p[:], 123)
	require.Equal(t, 0, n)
	require.Equal(t, io.EOF, err)
	testRemainingQuota(t, ctrl, underlyingPool, pool, 9, 1000)

	// Writes that would cause the file to grow beyond the maximum
	// size should be disallowed.
	n, err = f.WriteAt(p[:], 991)
	require.Equal(t, 0, n)
	require.Equal(t, err, status.Error(codes.InvalidArgument, "File size quota reached"))
	testRemainingQuota(t, ctrl, underlyingPool, pool, 9, 1000)

	// A failed write should initially allocate all of the required
	// space, but release the full amount once more.
	underlyingFile.EXPECT().WriteAt(p[:], int64(990)).Return(0, status.Error(codes.Internal, "Cannot write data at all"))
	n, err = f.WriteAt(p[:], 990)
	require.Equal(t, 0, n)
	require.Equal(t, err, status.Error(codes.Internal, "Cannot write data at all"))
	testRemainingQuota(t, ctrl, underlyingPool, pool, 9, 1000)

	// A short write should initially allocate all of the required
	// space, but release the amount of data that was not written.
	underlyingFile.EXPECT().WriteAt(p[:], int64(990)).Return(7, status.Error(codes.Internal, "Disk died in the middle of the write"))
	n, err = f.WriteAt(p[:], 990)
	require.Equal(t, 7, n)
	require.Equal(t, err, status.Error(codes.Internal, "Disk died in the middle of the write"))
	testRemainingQuota(t, ctrl, underlyingPool, pool, 9, 3)

	// I/O error while shrinking file should not cause the quotas to
	// be affected.
	underlyingFile.EXPECT().Truncate(int64(123)).Return(status.Error(codes.Internal, "Failed to adjust inode"))
	require.Equal(t, f.Truncate(123), status.Error(codes.Internal, "Failed to adjust inode"))
	testRemainingQuota(t, ctrl, underlyingPool, pool, 9, 3)

	// Successfully shrinking the file.
	underlyingFile.EXPECT().Truncate(int64(123)).Return(nil)
	require.NoError(t, f.Truncate(123))
	testRemainingQuota(t, ctrl, underlyingPool, pool, 9, 877)

	// Growing the file past the permitted size should not be
	// allowed.
	require.Equal(t, f.Truncate(1001), status.Error(codes.InvalidArgument, "File size quota reached"))
	testRemainingQuota(t, ctrl, underlyingPool, pool, 9, 877)

	// I/O error while growing file should not cause the quotas to
	// be affected.
	underlyingFile.EXPECT().Truncate(int64(1000)).Return(status.Error(codes.Internal, "Failed to adjust inode"))
	require.Equal(t, f.Truncate(1000), status.Error(codes.Internal, "Failed to adjust inode"))
	testRemainingQuota(t, ctrl, underlyingPool, pool, 9, 877)

	// Successfully growing the file.
	underlyingFile.EXPECT().Truncate(int64(1000)).Return(nil)
	require.NoError(t, f.Truncate(1000))
	testRemainingQuota(t, ctrl, underlyingPool, pool, 9, 0)

	// Closing the file should bring the pool back in the initial
	// state.
	underlyingFile.EXPECT().Close().Return(nil)
	require.NoError(t, f.Close())
	testRemainingQuota(t, ctrl, underlyingPool, pool, 10, 1000)
}
