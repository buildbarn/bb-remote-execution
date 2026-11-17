package access_test

import (
	"fmt"
	"slices"
	"strings"
	"testing"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-remote-execution/internal/mock"
	"github.com/buildbarn/bb-remote-execution/pkg/filesystem/access"
	"github.com/buildbarn/bb-remote-execution/pkg/proto/resourceusage"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/filesystem/path"
	"github.com/buildbarn/bb-storage/pkg/testutil"
	"github.com/stretchr/testify/require"

	"go.uber.org/mock/gomock"
)

func verifyBloomFilter(t *testing.T, bloomFilter []byte, hashFunctions uint32, testPaths map[string]bool, hashCollisions []string) {
	reader, err := access.NewBloomFilterReader(bloomFilter, hashFunctions)
	require.NoError(t, err)
	for testPath, expectExistence := range testPaths {
		pathHashes := access.RootPathHashes
		if testPath != "" {
			for component := range strings.SplitSeq(testPath, "/") {
				pathHashes = pathHashes.AppendComponent(path.MustNewComponent(component))
			}
		}
		if expectExistence == true {
			require.True(t, reader.Contains(pathHashes), "Expected path %q to be in the Bloom filter", testPath)
		} else if slices.Contains(hashCollisions, testPath) {
			// Hash collision in this Bloom filter instance, not much to do about it.
			require.True(t, reader.Contains(pathHashes), "Expected path %q to be in the Bloom filter due to hash collision", testPath)
		} else if expectExistence == false {
			require.False(t, reader.Contains(pathHashes), "Expected path %q to not be in the Bloom filter", testPath)
		} else {
			panic(fmt.Sprintf("Unexpected expectExistence value: %s=%s", testPath, expectExistence))
		}
	}
}

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
		verifyBloomFilter(t, bloomFilter, hashFunctions, map[string]bool{
			"nonexistent": false,
		}, []string{})
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
		expectedExistence := map[string]bool{
			"":            true,
			"nonexistent": false,
		}
		t.Run("SingleByte", func(t *testing.T) {
			bloomFilter, hashFunctions := rootUnreadDirectoryMonitor.GetBloomFilter(directoryFetcher, 1, 1000)
			verifyBloomFilter(t, bloomFilter, hashFunctions, expectedExistence, []string{})
			require.Equal(t, uint32(5), hashFunctions)
		})

		t.Run("TwoBytes", func(t *testing.T) {
			bloomFilter, hashFunctions := rootUnreadDirectoryMonitor.GetBloomFilter(directoryFetcher, 10, 1000)
			verifyBloomFilter(t, bloomFilter, hashFunctions, expectedExistence, []string{})
			require.Equal(t, uint32(9), hashFunctions)
		})

		t.Run("FourBytes", func(t *testing.T) {
			bloomFilter, hashFunctions := rootUnreadDirectoryMonitor.GetBloomFilter(directoryFetcher, 20, 1000)
			verifyBloomFilter(t, bloomFilter, hashFunctions, expectedExistence, []string{})
			require.Equal(t, uint32(21), hashFunctions)
		})

		t.Run("EightBytes", func(t *testing.T) {
			bloomFilter, hashFunctions := rootUnreadDirectoryMonitor.GetBloomFilter(directoryFetcher, 40, 1000)
			verifyBloomFilter(t, bloomFilter, hashFunctions, expectedExistence, []string{})
			require.Equal(t, uint32(42), hashFunctions)
		})

		t.Run("SizeConstrained", func(t *testing.T) {
			// Using 40 bits per elements should normally
			// yield an 8 byte Bloom filter. Because the
			// maximum is set to 2 bytes, we should get the
			// same results as using 10 bits per element.
			bloomFilter, hashFunctions := rootUnreadDirectoryMonitor.GetBloomFilter(directoryFetcher, 40, 2)
			verifyBloomFilter(t, bloomFilter, hashFunctions, expectedExistence, []string{})
			require.Equal(t, uint32(9), hashFunctions)

			bloomFilterOther, _ := rootUnreadDirectoryMonitor.GetBloomFilter(directoryFetcher, 10, 1000)
			require.Equal(t, bloomFilter, bloomFilterOther)
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
		verifyBloomFilter(t, bloomFilter, hashFunctions, map[string]bool{
			"":            true,
			"nonexistent": false,
		}, []string{})
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
		expectedExistence := map[string]bool{
			"":                true,
			"dir":             true,
			"nonexistent":     false,
			"dir/nonexistent": false,
		}
		t.Run("SingleByte", func(t *testing.T) {
			bloomFilter, hashFunctions := rootUnreadDirectoryMonitor.GetBloomFilter(directoryFetcher, 1, 1000)
			verifyBloomFilter(t, bloomFilter, hashFunctions, expectedExistence, []string{})
			require.Equal(t, uint32(2), hashFunctions)
		})

		t.Run("TwoBytes", func(t *testing.T) {
			bloomFilter, hashFunctions := rootUnreadDirectoryMonitor.GetBloomFilter(directoryFetcher, 5, 1000)
			require.Equal(t, uint32(5), hashFunctions)
			verifyBloomFilter(t, bloomFilter, hashFunctions, expectedExistence, []string{"dir/nonexistent"})
		})

		t.Run("FourBytes", func(t *testing.T) {
			bloomFilter, hashFunctions := rootUnreadDirectoryMonitor.GetBloomFilter(directoryFetcher, 10, 1000)
			verifyBloomFilter(t, bloomFilter, hashFunctions, expectedExistence, []string{})
			require.Equal(t, uint32(11), hashFunctions)
		})

		t.Run("EightBytes", func(t *testing.T) {
			bloomFilter, hashFunctions := rootUnreadDirectoryMonitor.GetBloomFilter(directoryFetcher, 20, 1000)
			verifyBloomFilter(t, bloomFilter, hashFunctions, expectedExistence, []string{})
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

	t.Run("ChildDirFileRead", func(t *testing.T) {
		expectedExistence := map[string]bool{
			"":                true,
			"dir":             true,
			"dir/file":        true,
			"file":            false,
			"nonexistent":     false,
			"dir/nonexistent": false,
		}
		t.Run("SingleByte", func(t *testing.T) {
			bloomFilter, hashFunctions := rootUnreadDirectoryMonitor.GetBloomFilter(directoryFetcher, 1, 1000)
			verifyBloomFilter(t, bloomFilter, hashFunctions, expectedExistence, []string{})
			require.Equal(t, uint32(2), hashFunctions)
		})

		t.Run("TwoBytes", func(t *testing.T) {
			bloomFilter, hashFunctions := rootUnreadDirectoryMonitor.GetBloomFilter(directoryFetcher, 3, 1000)
			verifyBloomFilter(t, bloomFilter, hashFunctions, expectedExistence, []string{"nonexistent", "dir/nonexistent"})
			require.Equal(t, uint32(3), hashFunctions)
		})

		t.Run("FourBytes", func(t *testing.T) {
			bloomFilter, hashFunctions := rootUnreadDirectoryMonitor.GetBloomFilter(directoryFetcher, 6, 1000)
			verifyBloomFilter(t, bloomFilter, hashFunctions, expectedExistence, []string{})
			require.Equal(t, uint32(7), hashFunctions)
		})

		t.Run("EightBytes", func(t *testing.T) {
			bloomFilter, hashFunctions := rootUnreadDirectoryMonitor.GetBloomFilter(directoryFetcher, 11, 1000)
			verifyBloomFilter(t, bloomFilter, hashFunctions, expectedExistence, []string{})
			require.Equal(t, uint32(14), hashFunctions)
		})
	})

	// List the entries in dir2, with duplicated paths.
	childUnreadDirectory2Monitor := rootReadDirectoryMonitor.ResolvedDirectory(childDirectory2Digest)
	childUnreadDirectory2Monitor.ReadDirectory()

	testutil.RequireEqualProto(t, &resourceusage.InputRootResourceUsage{
		DirectoriesResolved: 3,
		DirectoriesRead:     3,
		FilesRead:           1,
	}, rootUnreadDirectoryMonitor.GetInputRootResourceUsage())

	t.Run("RootFileRead", func(t *testing.T) {
		expectedExistence := map[string]bool{
			"":              true,
			"dir":           true,
			"file":          false,
			"dir/file":      true,
			"dir2":          true,
			"dir2/dir":      true,
			"dir2/dir/file": true,
			"dir2/file":     true,
		}
		t.Run("SingleByte", func(t *testing.T) {
			bloomFilter, hashFunctions := rootUnreadDirectoryMonitor.GetBloomFilter(directoryFetcher, 1, 1000)
			verifyBloomFilter(t, bloomFilter, hashFunctions, expectedExistence, []string{"file"})
			require.Equal(t, uint32(1), hashFunctions)
		})

		t.Run("TwoBytes", func(t *testing.T) {
			bloomFilter, hashFunctions := rootUnreadDirectoryMonitor.GetBloomFilter(directoryFetcher, 3, 1000)
			verifyBloomFilter(t, bloomFilter, hashFunctions, expectedExistence, []string{})
			require.Equal(t, uint32(3), hashFunctions)
		})

		t.Run("FourBytes", func(t *testing.T) {
			bloomFilter, hashFunctions := rootUnreadDirectoryMonitor.GetBloomFilter(directoryFetcher, 5, 1000)
			verifyBloomFilter(t, bloomFilter, hashFunctions, expectedExistence, []string{})
			require.Equal(t, uint32(6), hashFunctions)
		})

		t.Run("EightBytes", func(t *testing.T) {
			bloomFilter, hashFunctions := rootUnreadDirectoryMonitor.GetBloomFilter(directoryFetcher, 9, 1000)
			verifyBloomFilter(t, bloomFilter, hashFunctions, expectedExistence, []string{})
			require.Equal(t, uint32(13), hashFunctions)
		})

		t.Run("FourBytes", func(t *testing.T) {
			bloomFilter, hashFunctions := rootUnreadDirectoryMonitor.GetBloomFilter(directoryFetcher, 5, 1000)
			verifyBloomFilter(t, bloomFilter, hashFunctions, expectedExistence, []string{})
			require.Equal(t, uint32(6), hashFunctions)
		})

		t.Run("EightBytes", func(t *testing.T) {
			bloomFilter, hashFunctions := rootUnreadDirectoryMonitor.GetBloomFilter(directoryFetcher, 9, 1000)
			verifyBloomFilter(t, bloomFilter, hashFunctions, expectedExistence, []string{})
			require.Equal(t, uint32(13), hashFunctions)
		})
	})
}
