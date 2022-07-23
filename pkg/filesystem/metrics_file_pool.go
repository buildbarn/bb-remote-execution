package filesystem

import (
	"sync"

	"github.com/buildbarn/bb-storage/pkg/filesystem"
	"github.com/prometheus/client_golang/prometheus"
)

var (
	filePoolPrometheusMetrics sync.Once

	filePoolFilesCreated = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: "buildbarn",
			Subsystem: "filesystem",
			Name:      "file_pool_files_created_total",
			Help:      "Number of times a file was created that is backed by a file pool.",
		})
	filePoolFilesClosed = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: "buildbarn",
			Subsystem: "filesystem",
			Name:      "file_pool_files_closed_total",
			Help:      "Number of times a file was closed that is backed by a file pool.",
		})
)

type metricsFilePool struct {
	base FilePool
}

// NewMetricsFilePool creates a decorator for FilePool that exposes
// Prometheus metrics on how many files are created and closed.
func NewMetricsFilePool(base FilePool) FilePool {
	filePoolPrometheusMetrics.Do(func() {
		prometheus.MustRegister(filePoolFilesCreated)
		prometheus.MustRegister(filePoolFilesClosed)
	})

	return &metricsFilePool{
		base: base,
	}
}

func (fp *metricsFilePool) NewFile() (filesystem.FileReadWriter, error) {
	f, err := fp.base.NewFile()
	if err != nil {
		return nil, err
	}
	filePoolFilesCreated.Inc()
	return &metricsFile{
		FileReadWriter: f,
	}, nil
}

type metricsFile struct {
	filesystem.FileReadWriter
}

func (f *metricsFile) Close() error {
	err := f.FileReadWriter.Close()
	f.FileReadWriter = nil
	filePoolFilesClosed.Inc()
	return err
}
