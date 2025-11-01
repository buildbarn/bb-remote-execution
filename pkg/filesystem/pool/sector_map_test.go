package pool_test

import (
	"fmt"
	"io"
	"math"
	"testing"

	"github.com/buildbarn/bb-remote-execution/pkg/filesystem/pool"
	"github.com/stretchr/testify/require"
)

func TestSectorMap(t *testing.T) {
	// Implementation information used to test verify the logic
	// correctly handles the edge cases of the indirection array
	// size.
	const (
		indirectionArraySize     = 2048
		directSectors            = 12
		singleIndirectionSectors = directSectors + indirectionArraySize
		doubleIndirectionSectors = singleIndirectionSectors + indirectionArraySize*indirectionArraySize
		tripleIndirectionSectors = doubleIndirectionSectors + indirectionArraySize*indirectionArraySize*indirectionArraySize
	)
	t.Run("InsertContiguousAndVerify", func(t *testing.T) {
		sm := pool.SectorMap{}

		logicalStart := uint64(10)
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
			logical := uint64(i) + logicalStart
			physical := uint32(i) + physicalStart
			x, err := sm.GetNextMappedSector(logical)
			require.NoError(t, err)
			require.Equal(t, logical, x)
			val := sm.GetPhysicalIndex(logical)
			require.Equal(t, physical, val)
		}
		finalIndex, err := sm.GetNextMappedSector(logicalStart + uint64(length))
		require.Equal(t, uint64(0), finalIndex)
		require.Equal(t, io.EOF, err)
	})

	t.Run("InsertSectorErrors", func(t *testing.T) {
		sm := pool.SectorMap{}
		err := sm.InsertSectorsContiguous(10, 100, 5)
		require.NoError(t, err)
		err = sm.InsertSectorsContiguous(9, 200, 3) // Overlaps at index 10.
		require.ErrorContains(t, err, "Attempted to insert a sector at logical adress 10 but it is already occupied by 100")
		require.Equal(t, uint32(0), sm.GetPhysicalIndex(9))
		require.Equal(t, uint32(100), sm.GetPhysicalIndex(10))
		require.Equal(t, uint32(101), sm.GetPhysicalIndex(11))
		err = sm.InsertSectorsContiguous(math.MaxUint64, 1, 1)
		require.ErrorContains(t, err, fmt.Sprintf("Attempted to insert %d sectors from logical index %d but this would overflow", 1, uint64(math.MaxUint64)))
		err = sm.InsertSectorsContiguous(tripleIndirectionSectors-100, 1, 1000)
		require.ErrorContains(t, err, fmt.Sprintf("Attempted to insert %d sectors from logical index %d but the sector map can only map indexes less than %d", 1000, uint64(tripleIndirectionSectors-100), uint64(tripleIndirectionSectors)))
		err = sm.InsertSectorsContiguous(1, 4_000_000_000, 1_000_000_000)
		require.ErrorContains(t, err, "Attempted to insert 1000000000 sectors from physical index 4000000000 but this would overflow")
	})

	t.Run("Truncate", func(t *testing.T) {
		sm := pool.SectorMap{}
		// Insert 10 sectors every 1 million sectors.
		for i := uint64(0); i < 4000; i++ {
			err := sm.InsertSectorsContiguous(i*1_000_000, 100+uint32(i)*1_000_000, 10)
			require.NoError(t, err)
		}
		// Truncate away the inserted sectors.
		for i := uint64(4000); i >= 1; i-- {
			sm.Truncate(i * 1_000_000)
			_, err := sm.GetNextMappedSector(i*1_000_000 + 1)
			require.Equal(t, io.EOF, err)
		}
	})
	t.Run("NextMappedSector", func(t *testing.T) {
		sm := pool.SectorMap{}
		// Insert 10 sectors every 1 million sectors with offset 50.
		for i := uint64(0); i < 4000; i++ {
			err := sm.InsertSectorsContiguous(i*1_000_000+50, 100+uint32(i)*1_000_000, 10)
			require.NoError(t, err)
		}
		// Search for next Mapped sector every 1 million sectors with
		// offset 0.
		for i := uint64(0); i < 4000; i++ {
			x, err := sm.GetNextMappedSector(i * 1_000_000)
			require.NoError(t, err)
			require.Equal(t, i*1_000_000+50, x)
		}
	})

	t.Run("NextUnmappedSector", func(t *testing.T) {
		sm := pool.SectorMap{}
		// Densely insert sectors for the first 2000 sectors.
		err := sm.InsertSectorsContiguous(0, 100, 2000)
		require.NoError(t, err)
		// Find the first unmapped sector
		x, err := sm.GetNextUnmappedSector(0)
		require.NoError(t, err)
		require.Equal(t, uint64(2000), x)

		// Insert 10 sectors every 1 million sectors.
		for i := uint64(1); i < 4000; i++ {
			err := sm.InsertSectorsContiguous(i*1_000_000, 100+uint32(i)*1_000_000, 10)
			require.NoError(t, err)
		}
		// Search for next unmapped sector every 1 million sectors.
		for i := uint64(1); i < 4000; i++ {
			x, err := sm.GetNextUnmappedSector(i * 1_000_000)
			require.NoError(t, err)
			require.Equal(t, i*1_000_000+10, x)
		}
	})

	t.Run("TestGetSet", func(t *testing.T) {
		sm := pool.SectorMap{}

		expectedMappings := make(map[uint64]uint32)
		const n = 4000

		// insert a single mapping every 1 million sectors
		for i := 0; i < n; i++ {
			logical := uint64(i) * 1_000_000
			physical := uint32(i) + 1
			err := sm.InsertSectorsContiguous(logical, physical, 1)
			require.NoError(t, err)
			expectedMappings[logical] = physical
		}

		for logical, expected := range expectedMappings {
			physical := sm.GetPhysicalIndex(logical)
			require.Equal(t, expected, physical, "mismatch for logical index %d", logical)
		}
	})

	t.Run("TestAtBoundaries", func(t *testing.T) {
		sm := pool.SectorMap{}
		// Corner cases
		mappingsToTest := []uint64{
			0,                            // First sector.
			directSectors - 1,            // Last direct sector.
			directSectors,                // First single indirect sector.
			singleIndirectionSectors - 1, // Last single indirect sector.
		}
		// First and last index of 100 single indirection inside
		// of the double indirection.
		for i := uint64(0); i < 100; i++ {
			mappingsToTest = append(mappingsToTest,
				singleIndirectionSectors+i*indirectionArraySize,
				singleIndirectionSectors+(i+1)*indirectionArraySize-1,
			)
		}
		// Last index of the double indirection.
		mappingsToTest = append(mappingsToTest, doubleIndirectionSectors-1)
		// First and last index of 100 double indirections in
		// the triple indirection.
		for i := uint64(0); i < 100; i++ {
			mappingsToTest = append(mappingsToTest,
				doubleIndirectionSectors+i*indirectionArraySize*indirectionArraySize,
				doubleIndirectionSectors+(i+1)*indirectionArraySize*indirectionArraySize-1,
			)
		}
		// Test special values.
		mappingsToTest = append(mappingsToTest,
			math.MaxUint32-1,
			math.MaxUint32,
			math.MaxUint32+1,
			tripleIndirectionSectors-1, // last possible mapped sector
		)

		// Sweep forwards, inserting a sector at a boundary
		// point and asserting the values.
		physicalIndex := uint32(1)
		for i := range mappingsToTest {
			logicalIndex := mappingsToTest[i]
			err := sm.InsertSectorsContiguous(logicalIndex, physicalIndex, 1)
			val := sm.GetPhysicalIndex(logicalIndex)
			require.NoError(t, err)
			require.Equal(t, physicalIndex, val)
			mapped, err := sm.GetNextMappedSector(logicalIndex + 1)
			require.Equal(t, io.EOF, err)
			require.Equal(t, uint64(0), mapped)
			unmapped, err := sm.GetNextUnmappedSector(logicalIndex)
			require.NoError(t, err)
			require.Equal(t, logicalIndex+1, unmapped)
			physicalIndex += 1
		}
		// Sweep backwards, dropping a sector at a boundary
		// point and asserting the values.
		physicalIndex -= 1
		for i := len(mappingsToTest) - 1; i > 0; i-- {
			logicalIndexToDrop := mappingsToTest[i]
			logicalIndexToRemain := mappingsToTest[i-1]
			sm.Truncate(logicalIndexToDrop)
			dropped := sm.GetPhysicalIndex(logicalIndexToDrop)
			remaining := sm.GetPhysicalIndex(logicalIndexToRemain)
			require.Equal(t, uint32(0), dropped)
			require.Equal(t, physicalIndex-1, remaining)
			mapped, err := sm.GetNextMappedSector(logicalIndexToRemain + 1)
			require.Equal(t, err, io.EOF)
			require.Equal(t, uint64(0), mapped)
			unmapped, err := sm.GetNextUnmappedSector(logicalIndexToRemain)
			require.NoError(t, err)
			require.Equal(t, logicalIndexToRemain+1, unmapped)
			physicalIndex -= 1
		}
		// Test outside of the mappable area, all operations
		// except insert should be succesful.
		for i := uint64(tripleIndirectionSectors); i < tripleIndirectionSectors+100; i++ {
			err := sm.InsertSectorsContiguous(i, physicalIndex, 1)
			require.Error(t, err)
			sm.Truncate(i + 1)
			val := sm.GetPhysicalIndex(i)
			require.Equal(t, uint32(0), val)
			mapped, err := sm.GetNextMappedSector(i)
			require.Equal(t, io.EOF, err)
			require.Equal(t, uint64(0), mapped)
			unmapped, err := sm.GetNextUnmappedSector(i)
			require.NoError(t, err)
			require.Equal(t, i, unmapped)
		}
		// Test behavior at math.MaxUint64
		err := sm.InsertSectorsContiguous(math.MaxUint64, physicalIndex, 1)
		require.Error(t, err)
		val := sm.GetPhysicalIndex(math.MaxUint64)
		require.Equal(t, uint32(0), val)
		mapped, err := sm.GetNextMappedSector(math.MaxUint64)
		require.Equal(t, io.EOF, err)
		require.Equal(t, uint64(0), mapped)
		unmapped, err := sm.GetNextUnmappedSector(math.MaxUint64)
		require.NoError(t, err)
		require.Equal(t, uint64(math.MaxUint64), unmapped)
	})
}
