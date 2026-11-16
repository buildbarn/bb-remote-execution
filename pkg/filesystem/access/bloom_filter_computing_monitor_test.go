package access_test

import (
	"testing"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-remote-execution/internal/mock"
	"github.com/buildbarn/bb-remote-execution/pkg/filesystem/access"
	"github.com/buildbarn/bb-remote-execution/pkg/proto/resourceusage"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/testutil"
	"github.com/stretchr/testify/require"

	"go.uber.org/mock/gomock"
)

func TestBloomFilterComputingUnreadDirectoryMonitor(t *testing.T) {
	ctrl := gomock.NewController(t)
	rootDirectoryDigest := digest.MustNewDigest("example", remoteexecution.DigestFunction_MD5, "7f390b0d6fb7831b0172bd7ce3e54256", 12)
	childDirectoryDigest := digest.MustNewDigest("example", remoteexecution.DigestFunction_MD5, "4b3b03436604cb9d831b91c71a8c1952", 123)
	childDirectory2Digest := digest.MustNewDigest("example", remoteexecution.DigestFunction_MD5, "cd425f1794557dfbc5c00dd8228deeab", 456)
	fileDigest := digest.MustNewDigest("example", remoteexecution.DigestFunction_MD5, "c0607941dd5b3ca8e175a1bfbfd1c0ea", 789)

	directoryFetcher := mock.NewMockDirectoryFetcher(ctrl)
	directoryFetcher.EXPECT().GetDirectory(gomock.Any(), rootDirectoryDigest).AnyTimes().Return(&remoteexecution.Directory{
		Directories: []*remoteexecution.DirectoryNode{
			{
				Name:   "dir",
				Digest: childDirectoryDigest.GetProto(),
			},
			{
				Name:   "dir2",
				Digest: childDirectory2Digest.GetProto(),
			},
		},
	}, nil)
	directoryFetcher.EXPECT().GetDirectory(gomock.Any(), childDirectoryDigest).AnyTimes().Return(&remoteexecution.Directory{
		Files: []*remoteexecution.FileNode{
			{
				Name:   "file",
				Digest: fileDigest.GetProto(),
			},
		},
	}, nil)
	directoryFetcher.EXPECT().GetDirectory(gomock.Any(), childDirectory2Digest).AnyTimes().Return(&remoteexecution.Directory{
		Directories: []*remoteexecution.DirectoryNode{
			{
				Name:   "dir",
				Digest: childDirectoryDigest.GetProto(),
			},
		},
		Files: []*remoteexecution.FileNode{
			{
				Name:   "file",
				Digest: fileDigest.GetProto(),
			},
		},
	}, nil)

	rootUnreadDirectoryMonitor := access.NewBloomFilterComputingUnreadDirectoryMonitor(rootDirectoryDigest)
	testutil.RequireEqualProto(t, &resourceusage.InputRootResourceUsage{
		DirectoriesResolved: 1,
		DirectoriesRead:     0,
		FilesRead:           0,
	}, rootUnreadDirectoryMonitor.GetInputRootResourceUsage())

	t.Run("Empty", func(t *testing.T) {
		// Bloom filters without any paths in them should
		// always be 1 byte in size, with no elements set. The
		// number of hash functions should be exactly 1, as it
		// causes consumers to do the minimal amount of work,
		// while ensuring a zero percent false positive rate.
		bloomFilter, hashFunctions := rootUnreadDirectoryMonitor.GetBloomFilter(directoryFetcher, 10, 1000)
		require.Equal(t, []byte{0x80}, bloomFilter)
		require.Equal(t, uint32(1), hashFunctions)
	})

	// Read the contents of the root directory. This should cause us
	// to return Bloom filters for a single element.
	rootReadDirectoryMonitor := rootUnreadDirectoryMonitor.ReadDirectory()
	testutil.RequireEqualProto(t, &resourceusage.InputRootResourceUsage{
		DirectoriesResolved: 1,
		DirectoriesRead:     1,
		FilesRead:           0,
	}, rootUnreadDirectoryMonitor.GetInputRootResourceUsage())

	t.Run("RootDirectoryRead", func(t *testing.T) {
		t.Run("SingleByte", func(t *testing.T) {
			bloomFilter, hashFunctions := rootUnreadDirectoryMonitor.GetBloomFilter(directoryFetcher, 1, 1000)
			require.Equal(t, []byte{0xc6}, bloomFilter)
			require.Equal(t, uint32(5), hashFunctions)
		})

		t.Run("TwoBytes", func(t *testing.T) {
			bloomFilter, hashFunctions := rootUnreadDirectoryMonitor.GetBloomFilter(directoryFetcher, 10, 1000)
			require.Equal(t, []byte{0x0b, 0x2a}, bloomFilter)
			require.Equal(t, uint32(9), hashFunctions)
		})

		t.Run("FourBytes", func(t *testing.T) {
			bloomFilter, hashFunctions := rootUnreadDirectoryMonitor.GetBloomFilter(directoryFetcher, 20, 1000)
			require.Equal(t, []byte{0xa0, 0xa6, 0xb2, 0xd8}, bloomFilter)
			require.Equal(t, uint32(21), hashFunctions)
		})

		t.Run("EightBytes", func(t *testing.T) {
			bloomFilter, hashFunctions := rootUnreadDirectoryMonitor.GetBloomFilter(directoryFetcher, 40, 1000)
			require.Equal(t, []byte{0x5f, 0x19, 0x50, 0xab, 0x53, 0x4b, 0x19, 0x3b}, bloomFilter)
			require.Equal(t, uint32(42), hashFunctions)
		})

		t.Run("SizeConstrained", func(t *testing.T) {
			// Using 40 bits per elements should normally
			// yield an 8 byte Bloom filter. Because the
			// maximum is set to 2 bytes, we should get the
			// same results as using 10 bits per element.
			bloomFilter, hashFunctions := rootUnreadDirectoryMonitor.GetBloomFilter(directoryFetcher, 40, 2)
			require.Equal(t, []byte{0x0b, 0x2a}, bloomFilter)
			require.Equal(t, uint32(9), hashFunctions)
		})
	})

	// Merely resolving a child directory contained in the root
	// directory should not cause the Bloom filter to change. Its
	// contents should be read.
	childUnreadDirectoryMonitor := rootReadDirectoryMonitor.ResolvedDirectory(childDirectoryDigest)

	testutil.RequireEqualProto(t, &resourceusage.InputRootResourceUsage{
		DirectoriesResolved: 2,
		DirectoriesRead:     1,
		FilesRead:           0,
	}, rootUnreadDirectoryMonitor.GetInputRootResourceUsage())

	t.Run("ChildDirectoryResolved", func(t *testing.T) {
		bloomFilter, hashFunctions := rootUnreadDirectoryMonitor.GetBloomFilter(directoryFetcher, 40, 1000)
		require.Equal(t, []byte{0x5f, 0x19, 0x50, 0xab, 0x53, 0x4b, 0x19, 0x3b}, bloomFilter)
		require.Equal(t, uint32(42), hashFunctions)
	})

	// Read the child directory's contents. This should cause the
	// Bloom filter to get updated.
	childReadDirectoryMonitor := childUnreadDirectoryMonitor.ReadDirectory()

	testutil.RequireEqualProto(t, &resourceusage.InputRootResourceUsage{
		DirectoriesResolved: 2,
		DirectoriesRead:     2,
		FilesRead:           0,
	}, rootUnreadDirectoryMonitor.GetInputRootResourceUsage())

	t.Run("ChildDirectoryRead", func(t *testing.T) {
		t.Run("SingleByte", func(t *testing.T) {
			bloomFilter, hashFunctions := rootUnreadDirectoryMonitor.GetBloomFilter(directoryFetcher, 1, 1000)
			require.Equal(t, []byte{0xd4}, bloomFilter)
			require.Equal(t, uint32(2), hashFunctions)
		})

		t.Run("TwoBytes", func(t *testing.T) {
			bloomFilter, hashFunctions := rootUnreadDirectoryMonitor.GetBloomFilter(directoryFetcher, 5, 1000)
			require.Equal(t, uint32(5), hashFunctions)
			require.Equal(t, []byte{0x8a, 0x3a}, bloomFilter)
		})

		t.Run("FourBytes", func(t *testing.T) {
			bloomFilter, hashFunctions := rootUnreadDirectoryMonitor.GetBloomFilter(directoryFetcher, 10, 1000)
			require.Equal(t, []byte{0x92, 0xb7, 0x32, 0xc2}, bloomFilter)
			require.Equal(t, uint32(11), hashFunctions)
		})

		t.Run("EightBytes", func(t *testing.T) {
			bloomFilter, hashFunctions := rootUnreadDirectoryMonitor.GetBloomFilter(directoryFetcher, 20, 1000)
			require.Equal(t, []byte{0x1f, 0x59, 0x51, 0xf0, 0x43, 0xde, 0x18, 0x3f}, bloomFilter)
			require.Equal(t, uint32(21), hashFunctions)
		})
	})

	// Read a file in the child directory.
	childReadDirectoryMonitor.ReadFile(fileDigest)

	testutil.RequireEqualProto(t, &resourceusage.InputRootResourceUsage{
		DirectoriesResolved: 2,
		DirectoriesRead:     2,
		FilesRead:           1,
	}, rootUnreadDirectoryMonitor.GetInputRootResourceUsage())

	t.Run("RootFileRead", func(t *testing.T) {
		t.Run("SingleByte", func(t *testing.T) {
			bloomFilter, hashFunctions := rootUnreadDirectoryMonitor.GetBloomFilter(directoryFetcher, 1, 1000)
			require.Equal(t, []byte{0xd6}, bloomFilter)
			require.Equal(t, uint32(2), hashFunctions)
		})

		t.Run("TwoBytes", func(t *testing.T) {
			bloomFilter, hashFunctions := rootUnreadDirectoryMonitor.GetBloomFilter(directoryFetcher, 3, 1000)
			require.Equal(t, []byte{0x6a, 0x3a}, bloomFilter)
			require.Equal(t, uint32(3), hashFunctions)
		})

		t.Run("FourBytes", func(t *testing.T) {
			bloomFilter, hashFunctions := rootUnreadDirectoryMonitor.GetBloomFilter(directoryFetcher, 6, 1000)
			require.Equal(t, []byte{0xb6, 0xa5, 0x72, 0xd2}, bloomFilter)
			require.Equal(t, uint32(7), hashFunctions)
		})

		t.Run("EightBytes", func(t *testing.T) {
			bloomFilter, hashFunctions := rootUnreadDirectoryMonitor.GetBloomFilter(directoryFetcher, 11, 1000)
			require.Equal(t, []byte{0x1f, 0xf0, 0x03, 0xf1, 0x63, 0xda, 0x18, 0x3f}, bloomFilter)
			require.Equal(t, uint32(14), hashFunctions)
		})
	})

	// Read the file digest, which is referenced in both the root and
	// child directory.
	// childReadDirectoryMonitor.ReadFile(fileDigest)

	// Identical file contents in different directories have distinct hashes.
	childFileDigest := digest.MustNewDigest("example", remoteexecution.DigestFunction_MD5, "19dc69325bd8dfcd75cefbb6144ea3bb", 42)
	childReadDirectoryMonitor.ReadFile(childFileDigest)

	testutil.RequireEqualProto(t, &resourceusage.InputRootResourceUsage{
		DirectoriesResolved: 2,
		DirectoriesRead:     2,
		FilesRead:           2,
	}, rootUnreadDirectoryMonitor.GetInputRootResourceUsage())

	t.Run("RootFileRead", func(t *testing.T) {
		t.Run("SingleByte", func(t *testing.T) {
			bloomFilter, hashFunctions := rootUnreadDirectoryMonitor.GetBloomFilter(directoryFetcher, 1, 1000)
			require.Equal(t, []byte{0xd6}, bloomFilter)
			require.Equal(t, uint32(2), hashFunctions)
		})

		t.Run("TwoBytes", func(t *testing.T) {
			bloomFilter, hashFunctions := rootUnreadDirectoryMonitor.GetBloomFilter(directoryFetcher, 3, 1000)
			require.Equal(t, []byte{0x6a, 0x3a}, bloomFilter)
			require.Equal(t, uint32(3), hashFunctions)
		})

		t.Run("FourBytes", func(t *testing.T) {
			bloomFilter, hashFunctions := rootUnreadDirectoryMonitor.GetBloomFilter(directoryFetcher, 5, 1000)
			require.Equal(t, []byte{0xb6, 0xa5, 0x72, 0xd2}, bloomFilter)
			require.Equal(t, uint32(7), hashFunctions)
		})

		t.Run("EightBytes", func(t *testing.T) {
			bloomFilter, hashFunctions := rootUnreadDirectoryMonitor.GetBloomFilter(directoryFetcher, 9, 1000)
			require.Equal(t, []byte{0xb6, 0xa5, 0x72, 0xd2}, bloomFilter)
			require.Equal(t, uint32(7), hashFunctions)
		})
	})
}
