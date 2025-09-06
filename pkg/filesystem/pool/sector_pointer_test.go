package pool_test

import (
	"io"
	"testing"

	"github.com/buildbarn/bb-remote-execution/pkg/filesystem/pool"
	"github.com/stretchr/testify/require"
)

func TestSectorPointer(t *testing.T) {
	t.Run("InsertContiguousAndVerify", func(t *testing.T) {
		sm := pool.NewSectorPointer()

		logicalStart := uint32(10)
		physicalStart := uint32(5000)
		length := uint32(2000)
		err := sm.InsertSectorsContiguous(logicalStart, physicalStart, length)
		// for i = 0 to 2000
		// verify that pointer[i+logicalStart] == i+physicalStart
		require.NoError(t, err)
		startsAt, err := sm.GetNextMappedSector(0)
		require.NoError(t, err)
		require.Equal(t, logicalStart, startsAt)
		// Ensure that the entire length of logical indexes is
		// mapped to the corresponding physical index.
		for i := uint32(0); i < length; i++ {
			x, err := sm.GetNextMappedSector(i + logicalStart)
			require.NoError(t, err)
			require.Equal(t, i+logicalStart, x)
			val := sm.GetPhysicalIndex(i + logicalStart)
			require.Equal(t, physicalStart+i, val)
		}
		finalIndex, err := sm.GetNextMappedSector(logicalStart + length)
		require.Equal(t, uint32(0), finalIndex)
		require.Equal(t, io.EOF, err)
	})

	t.Run("TestOverwriteError", func(t *testing.T) {
		sm := pool.NewSectorPointer()
		err := sm.InsertSectorsContiguous(10, 100, 5)
		require.NoError(t, err)
		err = sm.InsertSectorsContiguous(12, 200, 5) // Overlaps at index 12.
		require.ErrorContains(t, err, "Attempted to insert a sector at logical adress 12 but it is already occupied by 102")
	})

	t.Run("Truncate", func(t *testing.T) {
		sm := pool.NewSectorPointer()
		// Insert 10 sectors every 1 million sectors.
		for i := uint32(0); i < 4000; i++ {
			err := sm.InsertSectorsContiguous(i*1_000_000, 100+i*1_000_000, 10)
			length := sm.GetLogicalSize()
			require.NoError(t, err)
			require.Equal(t, i*1_000_000+10, length)
		}
		// Truncate away the inserted sectors.
		for i := uint32(4000); i >= 1; i-- {
			sm.Truncate(i * 1_000_000)
			length := sm.GetLogicalSize()
			require.Equal(t, (i-1)*1_000_000+10, length)
		}
	})
	t.Run("NextMappedSector", func(t *testing.T) {
		sm := pool.NewSectorPointer()
		// Insert 10 sectors every 1 million sectors with offset 50.
		for i := uint32(0); i < 4000; i++ {
			err := sm.InsertSectorsContiguous(i*1_000_000+50, 100+i*1_000_000, 10)
			length := sm.GetLogicalSize()
			require.NoError(t, err)
			require.Equal(t, i*1_000_000+10+50, length)
		}
		// Search for next Mapped sector every 1 million sectors with
		// offset 0.
		for i := uint32(0); i < 4000; i++ {
			x, err := sm.GetNextMappedSector(i * 1_000_000)
			require.NoError(t, err)
			require.Equal(t, i*1_000_000+50, x)
		}
	})

	t.Run("NextUnmappedSector", func(t *testing.T) {
		sm := pool.NewSectorPointer()
		// Insert 10 sectors every 1 million sectors.
		for i := uint32(0); i < 4000; i++ {
			err := sm.InsertSectorsContiguous(i*1_000_000, 100+i*1_000_000, 10)
			length := sm.GetLogicalSize()
			require.NoError(t, err)
			require.Equal(t, i*1_000_000+10, length)
		}
		// Search for next unmapped sector every 1 million sectors.
		for i := uint32(0); i < 4000; i++ {
			x, err := sm.GetNextUnmappedSector(i * 1_000_000)
			require.NoError(t, err)
			require.Equal(t, i*1_000_000+10, x)
		}
	})

	t.Run("TestGetSet", func(t *testing.T) {
		sm := pool.NewSectorPointer()

		expectedMappings := make(map[uint32]uint32)
		const n = 4000

		// insert a single mapping every 1 million sectors
		for i := uint32(0); i < n; i++ {
			logical := i * 1_000_000
			physical := i + 1
			err := sm.InsertSectorsContiguous(logical, physical, 1)
			require.NoError(t, err)
			expectedMappings[logical] = physical
		}

		for logical, expected := range expectedMappings {
			physical := sm.GetPhysicalIndex(logical)
			require.Equal(t, expected, physical, "mismatch for logical index %d", logical)
		}
	})
}
