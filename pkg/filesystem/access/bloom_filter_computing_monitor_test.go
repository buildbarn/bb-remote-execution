package access_test

import (
	"strings"
	"testing"

	"github.com/buildbarn/bb-remote-execution/pkg/filesystem/access"
	"github.com/buildbarn/bb-remote-execution/pkg/proto/resourceusage"
	"github.com/buildbarn/bb-storage/pkg/filesystem/path"
	"github.com/buildbarn/bb-storage/pkg/testutil"
	"github.com/stretchr/testify/require"
)

func verifyBloomFilter(t *testing.T, bloomFilter []byte, hashFunctions uint32, testPaths map[string]bool) {
	reader, err := access.NewBloomFilterReader(bloomFilter, hashFunctions)
	require.NoError(t, err)
	for testPath, expectExistence := range testPaths {
		pathHashes := access.RootPathHashes
		if testPath != "" {
			for component := range strings.SplitSeq(testPath, "/") {
				pathHashes = pathHashes.AppendComponent(path.MustNewComponent(component))
			}
		}
		if expectExistence {
			require.True(t, reader.Contains(pathHashes), "Expected path %q to be in the Bloom filter", testPath)
		} else {
			require.False(t, reader.Contains(pathHashes), "Expected path %q to not be in the Bloom filter", testPath)
		}
	}
}

func TestBloomFilterComputingUnreadDirectoryMonitor(t *testing.T) {
	rootUnreadDirectoryMonitor := access.NewBloomFilterComputingUnreadDirectoryMonitor()
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
		bloomFilter, hashFunctions := rootUnreadDirectoryMonitor.GetBloomFilter(10, 1000)
		verifyBloomFilter(t, bloomFilter, hashFunctions, map[string]bool{
			"nonexistent": false,
		})
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
			bloomFilter, hashFunctions := rootUnreadDirectoryMonitor.GetBloomFilter(1, 1000)
			verifyBloomFilter(t, bloomFilter, hashFunctions, map[string]bool{
				"":            true,
				"nonexistent": false,
			})
			require.Equal(t, uint32(5), hashFunctions)
		})

		t.Run("TwoBytes", func(t *testing.T) {
			bloomFilter, hashFunctions := rootUnreadDirectoryMonitor.GetBloomFilter(10, 1000)
			verifyBloomFilter(t, bloomFilter, hashFunctions, map[string]bool{
				"":            true,
				"nonexistent": false,
			})
			require.Equal(t, uint32(9), hashFunctions)
		})

		t.Run("FourBytes", func(t *testing.T) {
			bloomFilter, hashFunctions := rootUnreadDirectoryMonitor.GetBloomFilter(20, 1000)
			verifyBloomFilter(t, bloomFilter, hashFunctions, map[string]bool{
				"":            true,
				"nonexistent": false,
			})
			require.Equal(t, uint32(21), hashFunctions)
		})

		t.Run("EightBytes", func(t *testing.T) {
			bloomFilter, hashFunctions := rootUnreadDirectoryMonitor.GetBloomFilter(40, 1000)
			verifyBloomFilter(t, bloomFilter, hashFunctions, map[string]bool{
				"":            true,
				"nonexistent": false,
			})
			require.Equal(t, uint32(42), hashFunctions)
		})

		t.Run("SizeConstrained", func(t *testing.T) {
			// Using 40 bits per elements should normally
			// yield an 8 byte Bloom filter. Because the
			// maximum is set to 2 bytes, we should get the
			// same results as using 10 bits per element.
			bloomFilter, hashFunctions := rootUnreadDirectoryMonitor.GetBloomFilter(40, 2)
			verifyBloomFilter(t, bloomFilter, hashFunctions, map[string]bool{
				"":            true,
				"nonexistent": false,
			})
			require.Equal(t, uint32(9), hashFunctions)

			bloomFilterOther, _ := rootUnreadDirectoryMonitor.GetBloomFilter(10, 1000)
			require.Equal(t, bloomFilter, bloomFilterOther)
		})
	})

	// Merely resolving a child directory contained in the root
	// directory should not cause the Bloom filter to change. Its
	// contents should be read.
	childUnreadDirectoryMonitor := rootReadDirectoryMonitor.ResolvedDirectory(path.MustNewComponent("dir"))
	testutil.RequireEqualProto(t, &resourceusage.InputRootResourceUsage{
		DirectoriesResolved: 2,
		DirectoriesRead:     1,
		FilesRead:           0,
	}, rootUnreadDirectoryMonitor.GetInputRootResourceUsage())

	t.Run("ChildDirectoryResolved", func(t *testing.T) {
		bloomFilter, hashFunctions := rootUnreadDirectoryMonitor.GetBloomFilter(40, 1000)
		verifyBloomFilter(t, bloomFilter, hashFunctions, map[string]bool{
			"":            true,
			"nonexistent": false,
		})
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
			bloomFilter, hashFunctions := rootUnreadDirectoryMonitor.GetBloomFilter(1, 1000)
			verifyBloomFilter(t, bloomFilter, hashFunctions, map[string]bool{
				"":                true,
				"dir":             true,
				"nonexistent":     false,
				"dir/nonexistent": false,
			})
			require.Equal(t, uint32(2), hashFunctions)
		})

		t.Run("TwoBytes", func(t *testing.T) {
			bloomFilter, hashFunctions := rootUnreadDirectoryMonitor.GetBloomFilter(5, 1000)
			require.Equal(t, uint32(5), hashFunctions)
			verifyBloomFilter(t, bloomFilter, hashFunctions, map[string]bool{
				"":            true,
				"dir":         true,
				"nonexistent": false,
				// Hash collision in this Bloom filter instance.
				// "dir/nonexistent": false,
			})
		})

		t.Run("FourBytes", func(t *testing.T) {
			bloomFilter, hashFunctions := rootUnreadDirectoryMonitor.GetBloomFilter(10, 1000)
			verifyBloomFilter(t, bloomFilter, hashFunctions, map[string]bool{
				"":                true,
				"dir":             true,
				"nonexistent":     false,
				"dir/nonexistent": false,
			})
			require.Equal(t, uint32(11), hashFunctions)
		})

		t.Run("EightBytes", func(t *testing.T) {
			bloomFilter, hashFunctions := rootUnreadDirectoryMonitor.GetBloomFilter(20, 1000)
			verifyBloomFilter(t, bloomFilter, hashFunctions, map[string]bool{
				"":                true,
				"dir":             true,
				"nonexistent":     false,
				"dir/nonexistent": false,
			})
			require.Equal(t, uint32(21), hashFunctions)
		})
	})

	// Read a file in the root directory.
	rootReadDirectoryMonitor.ReadFile(path.MustNewComponent("file"))
	testutil.RequireEqualProto(t, &resourceusage.InputRootResourceUsage{
		DirectoriesResolved: 2,
		DirectoriesRead:     2,
		FilesRead:           1,
	}, rootUnreadDirectoryMonitor.GetInputRootResourceUsage())

	t.Run("RootFileRead", func(t *testing.T) {
		t.Run("SingleByte", func(t *testing.T) {
			bloomFilter, hashFunctions := rootUnreadDirectoryMonitor.GetBloomFilter(1, 1000)
			verifyBloomFilter(t, bloomFilter, hashFunctions, map[string]bool{
				"":     true,
				"dir":  true,
				"file": true,
				// Hash collision in this Bloom filter instance.
				// "nonexistent":     false,
				// "dir/nonexistent": false,
			})
			require.Equal(t, uint32(2), hashFunctions)
		})

		t.Run("TwoBytes", func(t *testing.T) {
			bloomFilter, hashFunctions := rootUnreadDirectoryMonitor.GetBloomFilter(3, 1000)
			verifyBloomFilter(t, bloomFilter, hashFunctions, map[string]bool{
				"":            true,
				"dir":         true,
				"file":        true,
				"nonexistent": false,
				// Hash collision in this Bloom filter instance.
				// "dir/nonexistent": false,
			})
			require.Equal(t, uint32(3), hashFunctions)
		})

		t.Run("FourBytes", func(t *testing.T) {
			bloomFilter, hashFunctions := rootUnreadDirectoryMonitor.GetBloomFilter(6, 1000)
			verifyBloomFilter(t, bloomFilter, hashFunctions, map[string]bool{
				"":                true,
				"dir":             true,
				"file":            true,
				"nonexistent":     false,
				"dir/nonexistent": false,
			})
			require.Equal(t, uint32(7), hashFunctions)
		})

		t.Run("EightBytes", func(t *testing.T) {
			bloomFilter, hashFunctions := rootUnreadDirectoryMonitor.GetBloomFilter(11, 1000)
			verifyBloomFilter(t, bloomFilter, hashFunctions, map[string]bool{
				"":                true,
				"dir":             true,
				"file":            true,
				"nonexistent":     false,
				"dir/nonexistent": false,
			})
			require.Equal(t, uint32(14), hashFunctions)
		})
	})

	// Read a file in the child directory. Even though its name is
	// identical to the file in the root directory, the full path
	// differs. The resulting Bloom filters should thus differ.
	childReadDirectoryMonitor.ReadFile(path.MustNewComponent("file"))
	testutil.RequireEqualProto(t, &resourceusage.InputRootResourceUsage{
		DirectoriesResolved: 2,
		DirectoriesRead:     2,
		FilesRead:           2,
	}, rootUnreadDirectoryMonitor.GetInputRootResourceUsage())

	t.Run("RootFileRead", func(t *testing.T) {
		t.Run("SingleByte", func(t *testing.T) {
			bloomFilter, hashFunctions := rootUnreadDirectoryMonitor.GetBloomFilter(1, 1000)
			verifyBloomFilter(t, bloomFilter, hashFunctions, map[string]bool{
				"":         true,
				"dir":      true,
				"file":     true,
				"dir/file": true,
				// Hash collision in this Bloom filter instance.
				// "nonexistent": false,
				// "dir/nonexistent": false,
			})
			require.Equal(t, uint32(1), hashFunctions)
		})

		t.Run("TwoBytes", func(t *testing.T) {
			bloomFilter, hashFunctions := rootUnreadDirectoryMonitor.GetBloomFilter(3, 1000)
			verifyBloomFilter(t, bloomFilter, hashFunctions, map[string]bool{
				"":            true,
				"dir":         true,
				"file":        true,
				"dir/file":    true,
				"nonexistent": false,
				// Hash collision in this Bloom filter instance.
				// "dir/nonexistent": false,
			})
			require.Equal(t, uint32(2), hashFunctions)
		})

		t.Run("FourBytes", func(t *testing.T) {
			bloomFilter, hashFunctions := rootUnreadDirectoryMonitor.GetBloomFilter(5, 1000)
			verifyBloomFilter(t, bloomFilter, hashFunctions, map[string]bool{
				"":                true,
				"dir":             true,
				"file":            true,
				"dir/file":        true,
				"nonexistent":     false,
				"dir/nonexistent": false,
			})
			require.Equal(t, uint32(5), hashFunctions)
		})

		t.Run("EightBytes", func(t *testing.T) {
			bloomFilter, hashFunctions := rootUnreadDirectoryMonitor.GetBloomFilter(9, 1000)
			verifyBloomFilter(t, bloomFilter, hashFunctions, map[string]bool{
				"":                true,
				"dir":             true,
				"file":            true,
				"dir/file":        true,
				"nonexistent":     false,
				"dir/nonexistent": false,
			})
			require.Equal(t, uint32(11), hashFunctions)
		})
	})
}
