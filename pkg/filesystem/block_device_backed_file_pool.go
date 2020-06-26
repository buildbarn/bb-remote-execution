package filesystem

import (
	"fmt"
	"io"

	"github.com/buildbarn/bb-storage/pkg/blockdevice"
	"github.com/buildbarn/bb-storage/pkg/filesystem"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type blockDeviceBackedFilePool struct {
	blockDevice     blockdevice.ReadWriterAt
	sectorAllocator SectorAllocator
	sectorSizeBytes int
	zeroSector      []byte
}

// NewBlockDeviceBackedFilePool creates a FilePool that stores all
// temporary file contents directly on a block device. Using a block
// device tends to be faster than using a directory on a file system,
// for the reason that no metadata (e.g., a directory hierarchy and
// inode attributes) needs to be stored.
func NewBlockDeviceBackedFilePool(blockDevice blockdevice.ReadWriterAt, sectorAllocator SectorAllocator, sectorSizeBytes int) FilePool {
	return &blockDeviceBackedFilePool{
		blockDevice:     blockDevice,
		sectorAllocator: sectorAllocator,
		sectorSizeBytes: sectorSizeBytes,
		zeroSector:      make([]byte, sectorSizeBytes),
	}
}

func (fp *blockDeviceBackedFilePool) NewFile() (filesystem.FileReadWriter, error) {
	return &blockDeviceBackedFile{
		fp: fp,
	}, nil
}

type blockDeviceBackedFile struct {
	fp        *blockDeviceBackedFilePool
	sizeBytes uint64
	sectors   []uint32
}

func (f *blockDeviceBackedFile) Close() error {
	if len(f.sectors) > 0 {
		f.fp.sectorAllocator.FreeList(f.sectors)
	}
	f.fp = nil
	f.sectors = nil
	return nil
}

// toDeviceOffset converts a sector number and offset within a sector to
// a byte offset on the block device.
func (f *blockDeviceBackedFile) toDeviceOffset(sector uint32, offsetWithinSector int) int64 {
	return int64(sector-1)*int64(f.fp.sectorSizeBytes) + int64(offsetWithinSector)
}

// getInitialSectorIndex is called by ReadAt() and WriteAt() to
// determine which sectors in a file are affected by the operation.
func (f *blockDeviceBackedFile) getInitialSectorIndex(off int64, n int) (int, int, int) {
	firstSectorIndex := int(off / int64(f.fp.sectorSizeBytes))
	endSectorIndex := int((uint64(off) + uint64(n) + uint64(f.fp.sectorSizeBytes) - 1) / uint64(f.fp.sectorSizeBytes))
	if endSectorIndex > len(f.sectors) {
		endSectorIndex = len(f.sectors)
	}
	offsetWithinSector := int(off % int64(f.fp.sectorSizeBytes))
	return firstSectorIndex, endSectorIndex - 1, offsetWithinSector
}

// incrementSectorIndex is called by ReadAt() and WriteAt() to progress
// to the next sequence of contiguously stored sectors.
func (f *blockDeviceBackedFile) incrementSectorIndex(sectorIndex *int, offsetWithinSector *int, n int) {
	if (*offsetWithinSector+n)%f.fp.sectorSizeBytes != 0 {
		panic("Read or write did not finish at sector boundary")
	}
	*sectorIndex += (*offsetWithinSector + n) / f.fp.sectorSizeBytes
	*offsetWithinSector = 0
}

// getSectorsContiguous converts an index of a sector in a file to the
// on-disk sector number. It also computes how many sectors are stored
// contiguously starting at this point.
func (f *blockDeviceBackedFile) getSectorsContiguous(firstSectorIndex int, lastSectorIndex int) (uint32, int) {
	firstSector := f.sectors[firstSectorIndex]
	nContiguous := 1
	if firstSector == 0 {
		// A hole in a sparse file. Determine the size of the hole.
		for firstSectorIndex+nContiguous <= lastSectorIndex &&
			f.sectors[firstSectorIndex+nContiguous] == 0 {
			nContiguous++
		}
	} else {
		// A region that contains actual data. Determine how
		// many sectors are contiguous.
		for firstSectorIndex+nContiguous <= lastSectorIndex &&
			uint64(f.sectors[firstSectorIndex+nContiguous]) == uint64(firstSector)+uint64(nContiguous) {
			nContiguous++
		}
	}
	return firstSector, nContiguous
}

// limitBufferToSectorBoundary limits the size of a buffer to a given
// number of sectors. This function is used to restrict the size of a
// write to just that part that can be written contiguously.
func (f *blockDeviceBackedFile) limitBufferToSectorBoundary(p []byte, sectorCount int, offsetWithinSector int) []byte {
	if n := sectorCount*f.fp.sectorSizeBytes - offsetWithinSector; n < len(p) {
		return p[:n]
	}
	return p
}

// readFromSectors performs a single read against the block device. It
// attempts to read as much data into the output buffer as is possible
// in a single read operation. If the file is fragmented, multiple reads
// are necessary, requiring this function to be called repeatedly.
func (f *blockDeviceBackedFile) readFromSectors(p []byte, sectorIndex int, lastSectorIndex int, offsetWithinSector int) (int, error) {
	if sectorIndex >= len(f.sectors) {
		// Attempted to read from a hole located at the
		// end of the file. Fill up all of the remaining
		// space with zero bytes.
		for i := 0; i < len(p); i++ {
			p[i] = 0
		}
		return len(p), nil
	}

	sector, sectorsToRead := f.getSectorsContiguous(sectorIndex, lastSectorIndex)
	p = f.limitBufferToSectorBoundary(p, sectorsToRead, offsetWithinSector)
	if sector == 0 {
		// Attempted to read from a sparse region of the file.
		// Fill in zero bytes.
		for i := 0; i < len(p); i++ {
			p[i] = 0
		}
		return len(p), nil
	}

	// Attempted to read from a region of the file that contains
	// actual data. Read data from the block device.
	n, err := f.fp.blockDevice.ReadAt(p, f.toDeviceOffset(sector, offsetWithinSector))
	if err != nil && err != io.EOF {
		return n, err
	}
	if n != len(p) {
		return n, status.Errorf(codes.Internal, "Read against block device returned %d bytes, while %d bytes were expected", n, len(p))
	}
	return n, nil
}

func (f *blockDeviceBackedFile) ReadAt(p []byte, off int64) (int, error) {
	// Short circuit calls that are out of bounds.
	if off < 0 {
		return 0, status.Errorf(codes.InvalidArgument, "Negative read offset: %d", off)
	}
	if len(p) == 0 {
		return 0, nil
	}

	// Limit the read operation to the size of the file. Already
	// determine whether this operation will return nil or io.EOF.
	if uint64(off) >= f.sizeBytes {
		return 0, io.EOF
	}
	var success error
	if end := uint64(off) + uint64(len(p)); end >= f.sizeBytes {
		success = io.EOF
		p = p[:f.sizeBytes-uint64(off)]
	}

	// As the file may be stored on disk non-contiguously or may be
	// a sparse file with holes, the read may need to be decomposed
	// into smaller ones. Each loop iteration performs one read.
	sectorIndex, lastSectorIndex, offsetWithinSector := f.getInitialSectorIndex(off, len(p))
	nTotal := 0
	for {
		n, err := f.readFromSectors(p, sectorIndex, lastSectorIndex, offsetWithinSector)
		nTotal += n
		p = p[n:]
		if err != nil {
			return nTotal, err
		}
		if len(p) == 0 {
			return nTotal, success
		}
		f.incrementSectorIndex(&sectorIndex, &offsetWithinSector, n)
	}
}

// truncateSectors truncates a file to a given number of sectors.
func (f *blockDeviceBackedFile) truncateSectors(sectorCount int) {
	if len(f.sectors) > sectorCount {
		f.fp.sectorAllocator.FreeList(f.sectors[sectorCount:])
		f.sectors = f.sectors[:sectorCount]

		// Ensure that no hole remains at the end, as that would
		// lead to unnecessary fragmentation when growing the
		// file again.
		for len(f.sectors) > 0 && f.sectors[len(f.sectors)-1] == 0 {
			f.sectors = f.sectors[:len(f.sectors)-1]
		}
	}
}

func (f *blockDeviceBackedFile) Truncate(size int64) error {
	if size < 0 {
		return status.Errorf(codes.InvalidArgument, "Negative truncation size: %d", size)
	}

	sectorIndex := int(size / int64(f.fp.sectorSizeBytes))
	offsetWithinSector := int(size % int64(f.fp.sectorSizeBytes))
	if offsetWithinSector == 0 {
		// Truncating to an exact number of sectors.
		f.truncateSectors(sectorIndex)
	} else {
		// Truncating to partially into a sector.
		if uint64(size) < f.sizeBytes && sectorIndex < len(f.sectors) && f.sectors[sectorIndex] != 0 {
			// The file is being shrunk and the new last
			// sector is not a hole. Zero the trailing part
			// of the last sector to ensure that growing the
			// file later on doesn't bring back old data.
			sector := f.sectors[sectorIndex]
			zeroes := f.fp.zeroSector[:f.fp.sectorSizeBytes-offsetWithinSector]
			if diff := f.sizeBytes - uint64(size); uint64(len(zeroes)) > diff {
				zeroes = zeroes[:diff]
			}
			if _, err := f.fp.blockDevice.WriteAt(zeroes, f.toDeviceOffset(sector, offsetWithinSector)); err != nil {
				return err
			}
		}
		f.truncateSectors(sectorIndex + 1)
	}

	f.sizeBytes = uint64(size)
	return nil
}

// writeToNewSectors is used to write data into new sectors. This
// function is called when holes in a sparse file are filled up or when
// data is appended to the end of a file.
func (f *blockDeviceBackedFile) writeToNewSectors(p []byte, offsetWithinSector int) (int, uint32, int, error) {
	// Allocate space to store the data.
	sectorsToAllocate := int((uint64(offsetWithinSector) + uint64(len(p)) + uint64(f.fp.sectorSizeBytes) - 1) / uint64(f.fp.sectorSizeBytes))
	firstSector, sectorsAllocated, err := f.fp.sectorAllocator.AllocateContiguous(sectorsToAllocate)
	if err != nil {
		return 0, 0, 0, err
	}

	// We may not have been able to allocate the desired amount of
	// space contiguously. Restrict the write to just the space we
	// managed to allocate.
	p = f.limitBufferToSectorBoundary(p, sectorsAllocated, offsetWithinSector)
	nWritten := len(p)

	// Write the first sector separately when we need to introduce
	// leading zero padding.
	sector := firstSector
	if offsetWithinSector > 0 {
		buf := make([]byte, f.fp.sectorSizeBytes)
		nWritten := copy(buf[offsetWithinSector:], p)
		if _, err := f.fp.blockDevice.WriteAt(buf, f.toDeviceOffset(sector, 0)); err != nil {
			f.fp.sectorAllocator.FreeContiguous(firstSector, sectorsAllocated)
			return 0, 0, 0, err
		}

		p = p[nWritten:]
		sector++
	}

	// Write as many sectors to the block device as possible.
	if fullSectors := len(p) / f.fp.sectorSizeBytes; fullSectors > 0 {
		fullSectorsSize := fullSectors * f.fp.sectorSizeBytes
		if _, err := f.fp.blockDevice.WriteAt(p[:fullSectorsSize], f.toDeviceOffset(sector, 0)); err != nil {
			f.fp.sectorAllocator.FreeContiguous(firstSector, sectorsAllocated)
			return 0, 0, 0, err
		}
		p = p[fullSectorsSize:]
		sector += uint32(fullSectors)
	}

	// Write the last sector separately when we need to introduce
	// trailing zero padding.
	if len(p) > 0 {
		buf := make([]byte, f.fp.sectorSizeBytes)
		copy(buf, p)
		if _, err := f.fp.blockDevice.WriteAt(buf, f.toDeviceOffset(sector, 0)); err != nil {
			f.fp.sectorAllocator.FreeContiguous(firstSector, sectorsAllocated)
			return 0, 0, 0, err
		}
	}
	return nWritten, firstSector, sectorsAllocated, nil
}

// insertSectorsContiguous inserts a series of contiguous sectors into a
// file. This function is used to update a file after appending data to
// it or filling up a hole in a sparse file.
func (f *blockDeviceBackedFile) insertSectorsContiguous(firstSectorIndex int, firstSector uint32, count int) {
	for i := 0; i < count; i++ {
		sectorIndex := firstSectorIndex + i
		if f.sectors[sectorIndex] != 0 {
			panic(fmt.Sprintf("Attempted to replace existing sector at index %d", sectorIndex))
		}
		f.sectors[sectorIndex] = firstSector + uint32(i)
	}
}

// writeToSectors performs a single write against the block device. It
// attempts to write as much data from the input buffer as is possible
// in a single write operation. If the file is fragmented, multiple
// writes are necessary, requiring this function to be called
// repeatedly.
func (f *blockDeviceBackedFile) writeToSectors(p []byte, sectorIndex int, lastSectorIndex int, offsetWithinSector int) (int, error) {
	if sectorIndex >= len(f.sectors) {
		// Attempted to write past the end-of-file or within a
		// hole located at the end of a sparse file. Allocate
		// space and grow the file.
		bytesWritten, firstSector, sectorsAllocated, err := f.writeToNewSectors(p, offsetWithinSector)
		if err != nil {
			return 0, err
		}
		f.sectors = append(f.sectors, make([]uint32, sectorIndex+sectorsAllocated-len(f.sectors))...)
		f.insertSectorsContiguous(sectorIndex, firstSector, sectorsAllocated)
		return bytesWritten, nil
	}

	sector, sectorsToWrite := f.getSectorsContiguous(sectorIndex, lastSectorIndex)
	p = f.limitBufferToSectorBoundary(p, sectorsToWrite, offsetWithinSector)
	if sector == 0 {
		// Attempted to write to a hole within a sparse file.
		// Allocate space and insert sectors into the file.
		bytesWritten, firstSector, sectorsAllocated, err := f.writeToNewSectors(p, offsetWithinSector)
		if err != nil {
			return 0, err
		}
		f.insertSectorsContiguous(sectorIndex, firstSector, sectorsAllocated)
		return bytesWritten, nil
	}

	// Attempted to overwrite existing sectors of the file.
	return f.fp.blockDevice.WriteAt(p, f.toDeviceOffset(sector, offsetWithinSector))
}

func (f *blockDeviceBackedFile) WriteAt(p []byte, off int64) (int, error) {
	// Short circuit calls that are out of bounds.
	if off < 0 {
		status.Errorf(codes.InvalidArgument, "Negative write offset: %d", off)
	}
	if len(p) == 0 {
		return 0, nil
	}

	// As the file may be stored on disk non-contiguously or may be
	// a sparse file with holes, the write may need to be decomposed
	// into smaller ones. Each loop iteration performs one write.
	sectorIndex, lastSectorIndex, offsetWithinSector := f.getInitialSectorIndex(off, len(p))
	nTotal := 0
	for {
		n, err := f.writeToSectors(p, sectorIndex, lastSectorIndex, offsetWithinSector)
		nTotal += n
		p = p[n:]
		if len(p) == 0 || err != nil {
			// Adjust file size if needed.
			if newSize := uint64(off) + uint64(nTotal); nTotal > 0 && f.sizeBytes < newSize {
				f.sizeBytes = newSize
			}
			return nTotal, err
		}
		f.incrementSectorIndex(&sectorIndex, &offsetWithinSector, n)
	}
}
