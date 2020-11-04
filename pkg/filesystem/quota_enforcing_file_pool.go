package filesystem

import (
	"sync/atomic"

	"github.com/buildbarn/bb-storage/pkg/filesystem"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// quotaMetric is a simple 64-bit counter from/to which can be
// subtracted/added atomically. It is used to store the number of files
// and bytes of space available.
type quotaMetric struct {
	remaining int64
}

func (m *quotaMetric) allocate(v int64) bool {
	for m.remaining >= v {
		if atomic.CompareAndSwapInt64(&m.remaining, m.remaining, m.remaining-v) {
			return true
		}
	}
	return false
}

func (m *quotaMetric) release(v int64) {
	atomic.AddInt64(&m.remaining, v)
}

type quotaEnforcingFilePool struct {
	base FilePool

	filesRemaining quotaMetric
	bytesRemaining quotaMetric
}

// NewQuotaEnforcingFilePool creates a FilePool that enforces disk
// quotas. It limits how many files may be extracted from an underlying
// FilePool, while also limiting the total size of all files that are
// extracted. Space is reclaimed by either truncating files or closing
// them.
func NewQuotaEnforcingFilePool(base FilePool, maximumFileCount, maximumTotalSize int64) FilePool {
	return &quotaEnforcingFilePool{
		base:           base,
		filesRemaining: quotaMetric{remaining: maximumFileCount},
		bytesRemaining: quotaMetric{remaining: maximumTotalSize},
	}
}

func (fp *quotaEnforcingFilePool) NewFile() (filesystem.FileReadWriter, error) {
	if !fp.filesRemaining.allocate(1) {
		return nil, status.Error(codes.ResourceExhausted, "File count quota reached")
	}
	f, err := fp.base.NewFile()
	if err != nil {
		fp.filesRemaining.release(1)
		return nil, err
	}
	return &quotaEnforcingFile{
		FileReadWriter: f,
		pool:           fp,
	}, nil
}

type quotaEnforcingFile struct {
	filesystem.FileReadWriter

	pool *quotaEnforcingFilePool
	size int64
}

func (f *quotaEnforcingFile) Close() error {
	// Close underlying file.
	err := f.FileReadWriter.Close()
	f.FileReadWriter = nil

	// Release associated resources.
	f.pool.filesRemaining.release(1)
	f.pool.bytesRemaining.release(f.size)
	f.pool = nil
	return err
}

func (f *quotaEnforcingFile) Truncate(size int64) error {
	if size < f.size {
		// File is shrinking.
		if err := f.FileReadWriter.Truncate(size); err != nil {
			return err
		}
		f.pool.bytesRemaining.release(f.size - size)
	} else if size > f.size {
		// File is growing.
		additionalSpace := size - f.size
		if !f.pool.bytesRemaining.allocate(additionalSpace) {
			return status.Error(codes.ResourceExhausted, "File size quota reached")
		}
		if err := f.FileReadWriter.Truncate(size); err != nil {
			f.pool.bytesRemaining.release(additionalSpace)
			return err
		}
	}
	f.size = size
	return nil
}

func (f *quotaEnforcingFile) WriteAt(p []byte, off int64) (int, error) {
	// No need to allocate space if the file is not growing.
	desiredSize := off + int64(len(p))
	if desiredSize <= f.size {
		return f.FileReadWriter.WriteAt(p, off)
	}

	// File is growing. Allocate space prior to writing. Release it,
	// potentially partially, upon failure.
	if !f.pool.bytesRemaining.allocate(desiredSize - f.size) {
		return 0, status.Error(codes.ResourceExhausted, "File size quota reached")
	}
	n, err := f.FileReadWriter.WriteAt(p, off)
	actualSize := int64(0)
	if n > 0 {
		actualSize = off + int64(n)
	}
	if actualSize < f.size {
		actualSize = f.size
	}
	if actualSize < desiredSize {
		f.pool.bytesRemaining.release(desiredSize - actualSize)
	}
	f.size = actualSize
	return n, err
}
