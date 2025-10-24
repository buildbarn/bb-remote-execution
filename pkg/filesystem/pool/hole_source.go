package pool

import (
	"io"

	"github.com/buildbarn/bb-storage/pkg/filesystem"
)

type HoleSource interface {
	filesystem.FileReader
	Truncate(int64) error
}

var DefaultHoleSource HoleSource = zeroHoleSource{}

type zeroHoleSource struct{}

func (zeroHoleSource) Close() error {
	return nil
}

func (zeroHoleSource) Truncate(size int64) error {
	return nil
}

func (zeroHoleSource) ReadAt(p []byte, off int64) (int, error) {
	clear(p)
	return len(p), nil
}

func (zeroHoleSource) GetNextRegionOffset(off int64, regionType filesystem.RegionType) (int64, error) {
	switch regionType {
	case filesystem.Data:
		return 0, io.EOF
	case filesystem.Hole:
		return off, nil
	default:
		panic("Unknown region type")
	}
}
