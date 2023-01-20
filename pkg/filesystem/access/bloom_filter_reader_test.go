package access_test

import (
	"testing"

	"github.com/buildbarn/bb-remote-execution/pkg/filesystem/access"
	"github.com/buildbarn/bb-storage/pkg/filesystem/path"
	"github.com/buildbarn/bb-storage/pkg/testutil"
	"github.com/stretchr/testify/require"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestBloomFilterReader(t *testing.T) {
	t.Run("InvalidBloomFilter", func(t *testing.T) {
		_, err := access.NewBloomFilterReader(nil, 123)
		testutil.RequireEqualStatus(t, status.Error(codes.InvalidArgument, "Bloom filter is empty"), err)

		_, err = access.NewBloomFilterReader([]byte{0x12, 0x00}, 123)
		testutil.RequireEqualStatus(t, status.Error(codes.InvalidArgument, "Bloom filter's trailing byte is not properly padded"), err)
	})

	t.Run("Success", func(t *testing.T) {
		// Attempt to read one of the Bloom filters created by
		// TestBloomFilterComputingUnreadDirectoryMonitor.
		reader, err := access.NewBloomFilterReader([]byte{0x1d, 0xb2, 0x43, 0xf1, 0x61, 0xfa, 0x18, 0x3f}, 11)
		require.NoError(t, err)

		require.True(t, reader.Contains(access.RootPathHashes))
		require.True(t, reader.Contains(access.RootPathHashes.
			AppendComponent(path.MustNewComponent("dir"))))
		require.True(t, reader.Contains(access.RootPathHashes.
			AppendComponent(path.MustNewComponent("file"))))
		require.True(t, reader.Contains(access.RootPathHashes.
			AppendComponent(path.MustNewComponent("dir")).
			AppendComponent(path.MustNewComponent("file"))))
		require.False(t, reader.Contains(access.RootPathHashes.
			AppendComponent(path.MustNewComponent("nonexistent"))))
	})
}
