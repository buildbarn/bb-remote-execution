package pool_test

import (
	"io"
	"testing"

	"github.com/buildbarn/bb-remote-execution/pkg/filesystem/pool"
	"github.com/stretchr/testify/require"
)

func TestSectorPointer(t *testing.T) {
	t.Run("InsertAndGet", func(t *testing.T) {
		sm := pool.NewSectorPointer()

		logicalStart := uint32(10)
		physicalStart := uint32(5000)
		length := uint32(2000)
		err := sm.InsertSectorsContiguous(logicalStart, physicalStart, length)
		require.NoError(t, err)
		at, err := sm.GetNextMappedSector(0)
		require.NoError(t, err)
		require.Equal(t, logicalStart, at)
		for i := uint32(0); i < length; i++ {
			x, err := sm.GetNextMappedSector(i + at)
			require.NoError(t, err)
			require.Equal(t, i+at, x)
			val := sm.GetPhysicalIndex(i + at)
			require.Equal(t, physicalStart+i, val)
		}
		at, err = sm.GetNextMappedSector(at + length)
		require.Equal(t, uint32(0), at)
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

	// t.Run("LengthByLastMapped", func(t *testing.T) {
	// })
	// t.Run("NextUnmappedSector", func(t *testing.T) {
	// })
	// t.Run("DirectSectorListIteration", func(t *testing.T) {
	// })
}

// func testTruncate(t *testing.T, sm pool.SectorMapper) {
// 	sm.InsertSectorsContiguous(100, 10, 20)

// 	// 1. Truncate in the middle of the mapped block.
// 	truncationPoint := uint32(20) // Keep sectors with index < 20.
// 	sm.Truncate(truncationPoint)

// 	// Verify sectors before the truncation point are still mapped.
// 	if sm.GetPhysicalIndex(19) == 0 {
// 		t.Error("Sector 19 should still be mapped after truncate, but it's not")
// 	}

// 	// Verify sectors at and after the truncation point are unmapped.
// 	if sm.GetPhysicalIndex(20) != 0 {
// 		t.Error("Sector 20 should be unmapped after truncate, but it is still mapped")
// 	}

// 	// 2. Truncate to zero, removing all mappings.
// 	sm.Truncate(0)
// 	if sm.GetPhysicalIndex(10) != 0 {
// 		t.Error("Sector 10 should be unmapped after truncating to 0")
// 	}
// }

// // testLengthByLastMapped checks that the reported length is accurate.
// func testLengthByLastMapped(t *testing.T, sm pool.SectorMapper) {
// 	// An empty mapper should have a length of 0.
// 	if length := sm.GetLengthByLastMappedSector(); length != 0 {
// 		t.Errorf("Expected initial length of 0, got %d", length)
// 	}

// 	// Insert sectors and check if length is updated.
// 	sm.InsertSectorsContiguous(100, 10, 5) // Last mapped index is 14, so length should be 15.
// 	if length := sm.GetLengthByLastMappedSector(); length != 15 {
// 		t.Errorf("Expected length of 15, got %d", length)
// 	}

// 	// Insert sectors at a higher logical address.
// 	sm.InsertSectorsContiguous(200, 50, 10) // Last mapped is 59, length should be 60.
// 	if length := sm.GetLengthByLastMappedSector(); length != 60 {
// 		t.Errorf("Expected length of 60, got %d", length)
// 	}
// }

// // testNextMappedSector verifies the logic for finding the next mapped sector.
// func testNextMappedSector(t *testing.T, sm pool.SectorMapper) {
// 	// Setup: Insert two blocks of sectors with a gap in between.
// 	sm.InsertSectorsContiguous(100, 10, 5) // Mapped: 10..14
// 	sm.InsertSectorsContiguous(200, 20, 5) // Mapped: 20..24

// 	testCases := []struct {
// 		start    uint32
// 		expected uint32
// 		err      error
// 	}{
// 		{0, 10, nil},    // Find first block from the beginning.
// 		{10, 10, nil},   // Start on a mapped sector, should return itself.
// 		{15, 20, nil},   // Start in the gap, should find the next block.
// 		{24, 24, nil},   // Start on the last mapped sector.
// 		{25, 0, io.EOF}, // Start after all mapped sectors, expect EOF.
// 	}

// 	for _, tc := range testCases {
// 		t.Run(fmt.Sprintf("Start_%d", tc.start), func(t *testing.T) {
// 			next, err := sm.GetNextMappedSector(tc.start)
// 			if err != tc.err {
// 				t.Errorf("Expected error '%v', got '%v'", tc.err, err)
// 			}
// 			if err == nil && next != tc.expected {
// 				t.Errorf("Expected next mapped sector to be %d, but got %d", tc.expected, next)
// 			}
// 		})
// 	}
// }

// // testNextUnmappedSector verifies the logic for finding the next unmapped sector.
// func testNextUnmappedSector(t *testing.T, sm pool.SectorMapper) {
// 	sm.InsertSectorsContiguous(100, 5, 3) // Mapped: 5, 6, 7

// 	testCases := []struct {
// 		start    uint32
// 		expected uint32
// 	}{
// 		{0, 0},   // Unmapped at start, should return itself.
// 		{4, 4},   // Unmapped just before a block.
// 		{5, 8},   // Start on a mapped sector, should find the next unmapped one.
// 		{7, 8},   // Start on the last mapped sector.
// 		{10, 10}, // Already in an unmapped region.
// 	}

// 	for _, tc := range testCases {
// 		t.Run(fmt.Sprintf("Start_%d", tc.start), func(t *testing.T) {
// 			next, err := sm.GetNextUnmappedSector(tc.start)
// 			if err != nil {
// 				t.Errorf("Expected no error, got '%v'", err)
// 			}
// 			if next != tc.expected {
// 				t.Errorf("Expected next unmapped sector to be %d, but got %d", tc.expected, next)
// 			}
// 		})
// 	}
// }

// // testDirectSectorListIteration verifies that we can iterate through all mapped
// // sectors using GetNextDirectSectorList.
// func testDirectSectorListIteration(t *testing.T, sm pool.SectorMapper) {
// 	// Insert a few separate blocks to test iteration.
// 	sm.InsertSectorsContiguous(100, 10, 5)
// 	sm.InsertSectorsContiguous(200, 20, 10)
// 	sm.InsertSectorsContiguous(300, 100, 2)
// 	expectedTotalSectors := 5 + 10 + 2

// 	var sectorsFound int
// 	var nextLogical uint32 = 0
// 	for {
// 		var list []uint32
// 		nextLogical, list = sm.GetNextDirectSectorList(nextLogical)
// 		if len(list) == 0 {
// 			break // End of iteration
// 		}
// 		sectorsFound += len(list)
// 	}

// 	if sectorsFound != expectedTotalSectors {
// 		t.Errorf("Expected to iterate over %d total sectors, but found %d", expectedTotalSectors, sectorsFound)
// 	}
// }
