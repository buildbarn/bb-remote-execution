package filesystem_test

import (
	"io"
	"syscall"
	"testing"

	"github.com/buildbarn/bb-remote-execution/internal/mock"
	re_filesystem "github.com/buildbarn/bb-remote-execution/pkg/filesystem"
	"github.com/buildbarn/bb-storage/pkg/filesystem"
	"github.com/buildbarn/bb-storage/pkg/filesystem/path"
	"github.com/stretchr/testify/require"

	"go.uber.org/mock/gomock"
)

func TestDirectoryBackedFilePool(t *testing.T) {
	ctrl := gomock.NewController(t)

	directory := mock.NewMockDirectory(ctrl)
	fp := re_filesystem.NewDirectoryBackedFilePool(directory)

	t.Run("EmptyFile", func(t *testing.T) {
		f, err := fp.NewFile()
		require.NoError(t, err)

		// Underlying file should not yet exist. This should be
		// interpreted as if the file is empty.
		directory.EXPECT().OpenRead(path.MustNewComponent("1")).Return(nil, syscall.ENOENT)
		var p [10]byte
		n, err := f.ReadAt(p[:], 0)
		require.Equal(t, 0, n)
		require.Equal(t, io.EOF, err)

		// GetNextRegionOffset() should behave similarly.
		directory.EXPECT().OpenRead(path.MustNewComponent("1")).Return(nil, syscall.ENOENT)
		_, err = f.GetNextRegionOffset(0, filesystem.Data)
		require.Equal(t, io.EOF, err)

		directory.EXPECT().OpenRead(path.MustNewComponent("1")).Return(nil, syscall.ENOENT)
		_, err = f.GetNextRegionOffset(0, filesystem.Hole)
		require.Equal(t, io.EOF, err)

		directory.EXPECT().Remove(path.MustNewComponent("1")).Return(syscall.ENOENT)
		require.NoError(t, f.Close())
	})

	t.Run("NonEmptyFile", func(t *testing.T) {
		f, err := fp.NewFile()
		require.NoError(t, err)

		// Write a piece of text into the file.
		fileWriter := mock.NewMockFileWriter(ctrl)
		directory.EXPECT().OpenWrite(path.MustNewComponent("2"), filesystem.CreateReuse(0o600)).Return(fileWriter, nil)
		fileWriter.EXPECT().WriteAt([]byte("Hello, world"), int64(123)).Return(12, nil)
		fileWriter.EXPECT().Close()
		n, err := f.WriteAt([]byte("Hello, world"), 123)
		require.Equal(t, 12, n)
		require.NoError(t, err)

		// Truncate a part of it.
		fileWriter = mock.NewMockFileWriter(ctrl)
		directory.EXPECT().OpenWrite(path.MustNewComponent("2"), filesystem.CreateReuse(0o600)).Return(fileWriter, nil)
		fileWriter.EXPECT().Truncate(int64(128))
		fileWriter.EXPECT().Close()
		require.NoError(t, f.Truncate(128))

		// Read back the end of the file.
		fileReader := mock.NewMockFileReader(ctrl)
		directory.EXPECT().OpenRead(path.MustNewComponent("2")).Return(fileReader, nil)
		fileReader.EXPECT().ReadAt(gomock.Any(), int64(120)).DoAndReturn(
			func(p []byte, off int64) (int, error) {
				require.Len(t, p, 10)
				copy(p, "\x00\x00\x00Hello")
				return 8, io.EOF
			})
		fileReader.EXPECT().Close()
		var p [10]byte
		n, err = f.ReadAt(p[:], 120)
		require.Equal(t, 8, n)
		require.Equal(t, io.EOF, err)
		require.Equal(t, []byte("\x00\x00\x00Hello"), p[:8])

		// Calls for GetNextRegionOffset() should be forwarded.
		fileReader = mock.NewMockFileReader(ctrl)
		directory.EXPECT().OpenRead(path.MustNewComponent("2")).Return(fileReader, nil)
		fileReader.EXPECT().GetNextRegionOffset(int64(0), filesystem.Hole).Return(int64(123), nil)
		fileReader.EXPECT().Close()
		off, err := f.GetNextRegionOffset(0, filesystem.Hole)
		require.NoError(t, err)
		require.Equal(t, int64(123), off)

		directory.EXPECT().Remove(path.MustNewComponent("2")).Return(nil)
		require.NoError(t, f.Close())
	})
}
