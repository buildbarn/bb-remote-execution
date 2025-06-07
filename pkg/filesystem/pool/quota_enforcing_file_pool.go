package pool

import (
	"github.com/buildbarn/bb-remote-execution/pkg/util"
	"github.com/buildbarn/bb-storage/pkg/filesystem"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type quotaEnforcingFilePool struct {
	base FilePool

	filesRemaining util.QuotaMetric
}

// NewQuotaEnforcingFilePool creates a FilePool that enforces disk
// quotas. It limits how many files may be extracted from an underlying
// FilePool, while also limiting the total size of all files that are
// extracted. Space is reclaimed by either truncating files or closing
// them.
func NewQuotaEnforcingFilePool(base FilePool, maximumFileCount int64) FilePool {
	fp := &quotaEnforcingFilePool{
		base:           base,
		filesRemaining: util.NewQuotaMetric(maximumFileCount),
	}
	return fp
}

func (fp *quotaEnforcingFilePool) NewFile() (filesystem.FileReadWriter, error) {
	if !fp.filesRemaining.Allocate(1) {
		return nil, status.Error(codes.InvalidArgument, "File count quota reached")
	}
	f, err := fp.base.NewFile()
	if err != nil {
		fp.filesRemaining.Release(1)
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
	f.pool.filesRemaining.Release(1)
	f.pool = nil
	return err
}
