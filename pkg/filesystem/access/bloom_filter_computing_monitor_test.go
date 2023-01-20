package access_test

import (
	"testing"

	"github.com/buildbarn/bb-remote-execution/pkg/filesystem/access"
	"github.com/buildbarn/bb-storage/pkg/filesystem/path"
	"github.com/stretchr/testify/require"
)

func TestBloomFilterComputingUnreadDirectoryMonitor(t *testing.T) {
	rootUnreadDirectoryMonitor := access.NewBloomFilterComputingUnreadDirectoryMonitor()

	t.Run("Empty", func(t *testing.T) {
		// Bloom filters without any paths in them should
		// always be 1 byte in size, with no elements set. The
		// number of hash functions should be exactly 1, as it
		// causes consumers to do the minimal amount of work,
		// while ensuring a zero percent false positive rate.
		bloomFilter, hashFunctions := rootUnreadDirectoryMonitor.GetBloomFilter(10, 1000)
		require.Equal(t, []byte{0x80}, bloomFilter)
		require.Equal(t, uint32(1), hashFunctions)
	})

	// Read the contents of the root directory. This should cause us
	// to return Bloom filters for a single element.
	rootReadDirectoryMonitor := rootUnreadDirectoryMonitor.ReadDirectory()

	t.Run("RootDirectoryRead", func(t *testing.T) {
		t.Run("SingleByte", func(t *testing.T) {
			bloomFilter, hashFunctions := rootUnreadDirectoryMonitor.GetBloomFilter(1, 1000)
			require.Equal(t, []byte{0xc6}, bloomFilter)
			require.Equal(t, uint32(5), hashFunctions)
		})

		t.Run("TwoBytes", func(t *testing.T) {
			bloomFilter, hashFunctions := rootUnreadDirectoryMonitor.GetBloomFilter(10, 1000)
			require.Equal(t, []byte{0x0b, 0x2a}, bloomFilter)
			require.Equal(t, uint32(9), hashFunctions)
		})

		t.Run("FourBytes", func(t *testing.T) {
			bloomFilter, hashFunctions := rootUnreadDirectoryMonitor.GetBloomFilter(20, 1000)
			require.Equal(t, []byte{0xa0, 0xa6, 0xb2, 0xd8}, bloomFilter)
			require.Equal(t, uint32(21), hashFunctions)
		})

		t.Run("EightBytes", func(t *testing.T) {
			bloomFilter, hashFunctions := rootUnreadDirectoryMonitor.GetBloomFilter(40, 1000)
			require.Equal(t, []byte{0x5f, 0x19, 0x50, 0xab, 0x53, 0x4b, 0x19, 0x3b}, bloomFilter)
			require.Equal(t, uint32(42), hashFunctions)
		})

		t.Run("SizeConstrained", func(t *testing.T) {
			// Using 40 bits per elements should normally
			// yield an 8 byte Bloom filter. Because the
			// maximum is set to 2 bytes, we should get the
			// same results as using 10 bits per element.
			bloomFilter, hashFunctions := rootUnreadDirectoryMonitor.GetBloomFilter(40, 2)
			require.Equal(t, []byte{0x0b, 0x2a}, bloomFilter)
			require.Equal(t, uint32(9), hashFunctions)
		})
	})

	// Merely resolving a child directory contained in the root
	// directory should not cause the Bloom filter to change. Its
	// contents should be read.
	childUnreadDirectoryMonitor := rootReadDirectoryMonitor.ResolvedDirectory(path.MustNewComponent("dir"))

	t.Run("ChildDirectoryResolved", func(t *testing.T) {
		bloomFilter, hashFunctions := rootUnreadDirectoryMonitor.GetBloomFilter(40, 1000)
		require.Equal(t, []byte{0x5f, 0x19, 0x50, 0xab, 0x53, 0x4b, 0x19, 0x3b}, bloomFilter)
		require.Equal(t, uint32(42), hashFunctions)
	})

	// Read the child directory's contents. This should cause the
	// Bloom filter to get updated.
	childReadDirectoryMonitor := childUnreadDirectoryMonitor.ReadDirectory()

	t.Run("ChildDirectoryRead", func(t *testing.T) {
		t.Run("SingleByte", func(t *testing.T) {
			bloomFilter, hashFunctions := rootUnreadDirectoryMonitor.GetBloomFilter(1, 1000)
			require.Equal(t, []byte{0xd4}, bloomFilter)
			require.Equal(t, uint32(2), hashFunctions)
		})

		t.Run("TwoBytes", func(t *testing.T) {
			bloomFilter, hashFunctions := rootUnreadDirectoryMonitor.GetBloomFilter(5, 1000)
			require.Equal(t, uint32(5), hashFunctions)
			require.Equal(t, []byte{0x8a, 0x3a}, bloomFilter)
		})

		t.Run("FourBytes", func(t *testing.T) {
			bloomFilter, hashFunctions := rootUnreadDirectoryMonitor.GetBloomFilter(10, 1000)
			require.Equal(t, []byte{0x92, 0xb7, 0x32, 0xc2}, bloomFilter)
			require.Equal(t, uint32(11), hashFunctions)
		})

		t.Run("EightBytes", func(t *testing.T) {
			bloomFilter, hashFunctions := rootUnreadDirectoryMonitor.GetBloomFilter(20, 1000)
			require.Equal(t, []byte{0x1f, 0x59, 0x51, 0xf0, 0x43, 0xde, 0x18, 0x3f}, bloomFilter)
			require.Equal(t, uint32(21), hashFunctions)
		})
	})

	// Read a file in the root directory.
	rootReadDirectoryMonitor.ReadFile(path.MustNewComponent("file"))

	t.Run("RootFileRead", func(t *testing.T) {
		t.Run("SingleByte", func(t *testing.T) {
			bloomFilter, hashFunctions := rootUnreadDirectoryMonitor.GetBloomFilter(1, 1000)
			require.Equal(t, []byte{0xf6}, bloomFilter)
			require.Equal(t, uint32(2), hashFunctions)
		})

		t.Run("TwoBytes", func(t *testing.T) {
			bloomFilter, hashFunctions := rootUnreadDirectoryMonitor.GetBloomFilter(3, 1000)
			require.Equal(t, []byte{0x0a, 0x36}, bloomFilter)
			require.Equal(t, uint32(3), hashFunctions)
		})

		t.Run("FourBytes", func(t *testing.T) {
			bloomFilter, hashFunctions := rootUnreadDirectoryMonitor.GetBloomFilter(6, 1000)
			require.Equal(t, []byte{0x92, 0xe5, 0x13, 0xc2}, bloomFilter)
			require.Equal(t, uint32(7), hashFunctions)
		})

		t.Run("EightBytes", func(t *testing.T) {
			bloomFilter, hashFunctions := rootUnreadDirectoryMonitor.GetBloomFilter(11, 1000)
			require.Equal(t, []byte{0x1d, 0xd2, 0x43, 0xf0, 0x63, 0xf2, 0x18, 0x3e}, bloomFilter)
			require.Equal(t, uint32(14), hashFunctions)
		})
	})

	// Read a file in the child directory. Even though its name is
	// identical to the file in the root directory, the full path
	// differs. The resulting Bloom filters should thus differ.
	childReadDirectoryMonitor.ReadFile(path.MustNewComponent("file"))

	t.Run("RootFileRead", func(t *testing.T) {
		t.Run("SingleByte", func(t *testing.T) {
			bloomFilter, hashFunctions := rootUnreadDirectoryMonitor.GetBloomFilter(1, 1000)
			require.Equal(t, []byte{0xd6}, bloomFilter)
			require.Equal(t, uint32(1), hashFunctions)
		})

		t.Run("TwoBytes", func(t *testing.T) {
			bloomFilter, hashFunctions := rootUnreadDirectoryMonitor.GetBloomFilter(3, 1000)
			require.Equal(t, []byte{0x4a, 0x2e}, bloomFilter)
			require.Equal(t, uint32(2), hashFunctions)
		})

		t.Run("FourBytes", func(t *testing.T) {
			bloomFilter, hashFunctions := rootUnreadDirectoryMonitor.GetBloomFilter(5, 1000)
			require.Equal(t, []byte{0x36, 0xa5, 0x73, 0xc2}, bloomFilter)
			require.Equal(t, uint32(5), hashFunctions)
		})

		t.Run("EightBytes", func(t *testing.T) {
			bloomFilter, hashFunctions := rootUnreadDirectoryMonitor.GetBloomFilter(9, 1000)
			require.Equal(t, []byte{0x1d, 0xb2, 0x43, 0xf1, 0x61, 0xfa, 0x18, 0x3f}, bloomFilter)
			require.Equal(t, uint32(11), hashFunctions)
		})
	})
}
