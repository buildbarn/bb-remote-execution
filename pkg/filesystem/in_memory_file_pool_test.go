package filesystem_test

import (
	"io"
	"testing"

	"github.com/buildbarn/bb-remote-execution/pkg/filesystem"
	"github.com/stretchr/testify/require"
)

func TestInMemoryFilePool(t *testing.T) {
	fp := filesystem.NewInMemoryFilePool()

	t.Run("EmptyFile", func(t *testing.T) {
		f, err := fp.NewFile()
		require.NoError(t, err)

		var p [10]byte
		n, err := f.ReadAt(p[:], 0)
		require.Equal(t, 0, n)
		require.Equal(t, io.EOF, err)

		require.NoError(t, f.Close())
	})

	t.Run("NonEmptyFile", func(t *testing.T) {
		f, err := fp.NewFile()
		require.NoError(t, err)

		// Write a piece of text into the file.
		n, err := f.WriteAt([]byte("Hello, world"), 123)
		require.Equal(t, 12, n)
		require.NoError(t, err)

		// Truncate a part of it.
		require.NoError(t, f.Truncate(128))

		// Read back the end of the file.
		var p [10]byte
		n, err = f.ReadAt(p[:], 120)
		require.Equal(t, 8, n)
		require.Equal(t, io.EOF, err)
		require.Equal(t, []byte("\x00\x00\x00Hello"), p[:8])

		require.NoError(t, f.Close())
	})

	t.Run("ZeroSizedWrite", func(t *testing.T) {
		f, err := fp.NewFile()
		require.NoError(t, err)

		// A zero-sized write should not cause the file to
		// actually grow. The read should still return EOF.
		n, err := f.WriteAt(nil, 123)
		require.Equal(t, 0, n)
		require.NoError(t, err)

		var p [10]byte
		n, err = f.ReadAt(p[:], 0)
		require.Equal(t, 0, n)
		require.Equal(t, io.EOF, err)

		require.NoError(t, f.Close())
	})
}
