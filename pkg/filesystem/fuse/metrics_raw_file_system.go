// +build darwin linux

package fuse

import (
	"sync"
	"syscall"
	"time"

	"github.com/buildbarn/bb-storage/pkg/clock"
	"github.com/buildbarn/bb-storage/pkg/util"
	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/prometheus/client_golang/prometheus"

	"golang.org/x/sys/unix"
)

var (
	rawFileSystemOperationsPrometheusMetrics sync.Once

	rawFileSystemOperationsDurationSeconds = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "buildbarn",
			Subsystem: "fuse",
			Name:      "raw_file_system_operations_duration_seconds",
			Help:      "Amount of time spent per operation on raw file system objects, in seconds.",
			Buckets:   util.DecimalExponentialBuckets(-3, 6, 2),
		},
		[]string{"operation", "status_code"})
	rawFileSystemCallbacks = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "buildbarn",
			Subsystem: "fuse",
			Name:      "raw_file_system_callbacks_total",
			Help:      "Total number of callbacks invoked by raw file system objects.",
		},
		[]string{"callback", "status_code"})
)

// operationHistogram holds references to Prometheus metrics for a single
// FUSE operation that can never fail.
type operationHistogram struct {
	ok prometheus.Observer
}

func newOperationHistogram(operation string) operationHistogram {
	return operationHistogram{
		ok: rawFileSystemOperationsDurationSeconds.WithLabelValues(operation, "OK"),
	}
}

func (m *operationHistogram) observe(timeStart, timeStop time.Time) {
	m.ok.Observe(timeStop.Sub(timeStart).Seconds())
}

// operationHistogramWithStatus holds references to Prometheus metrics for
// a single FUSE operation that can fail with a fuse.Status.
type operationHistogramWithStatus struct {
	ok      prometheus.Observer
	failure prometheus.ObserverVec
}

func newOperationHistogramWithStatus(operation string) operationHistogramWithStatus {
	return operationHistogramWithStatus{
		ok:      rawFileSystemOperationsDurationSeconds.WithLabelValues(operation, "OK"),
		failure: rawFileSystemOperationsDurationSeconds.MustCurryWith(map[string]string{"operation": operation}),
	}
}

func (m *operationHistogramWithStatus) observe(s fuse.Status, timeStart, timeStop time.Time) {
	d := timeStop.Sub(timeStart).Seconds()
	if s == fuse.OK {
		m.ok.Observe(d)
	} else {
		// Use unix.ErrnoName() instead of fuse.Status.String().
		// The latter inserts OS specific errno integer values
		// into the error message, which is not desirable in
		// heterogeneous environments.
		m.failure.WithLabelValues(unix.ErrnoName(syscall.Errno(s))).Observe(d)
	}
}

// callbackCounterWithStatus holds references to Prometheus metrics for
// a single FUSE server callback that can fail with a fuse.Status.
type callbackCounterWithStatus struct {
	ok      prometheus.Counter
	failure *prometheus.CounterVec
}

func newCallbackCounterWithStatus(callback string) callbackCounterWithStatus {
	return callbackCounterWithStatus{
		ok:      rawFileSystemCallbacks.WithLabelValues(callback, "OK"),
		failure: rawFileSystemCallbacks.MustCurryWith(map[string]string{"callback": callback}),
	}
}

func (m *callbackCounterWithStatus) inc(s fuse.Status) {
	if s == fuse.OK {
		m.ok.Inc()
	} else {
		m.failure.WithLabelValues(unix.ErrnoName(syscall.Errno(s))).Inc()
	}
}

var (
	// Already populate the HistogramVec with entries for all operations.
	operationHistogramLookup        = newOperationHistogramWithStatus("Lookup")
	operationHistogramForget        = newOperationHistogram("Forget")
	operationHistogramGetAttr       = newOperationHistogramWithStatus("GetAttr")
	operationHistogramSetAttr       = newOperationHistogramWithStatus("SetAttr")
	operationHistogramMknod         = newOperationHistogramWithStatus("Mknod")
	operationHistogramMkdir         = newOperationHistogramWithStatus("Mkdir")
	operationHistogramUnlink        = newOperationHistogramWithStatus("Unlink")
	operationHistogramRmdir         = newOperationHistogramWithStatus("Rmdir")
	operationHistogramRename        = newOperationHistogramWithStatus("Rename")
	operationHistogramLink          = newOperationHistogramWithStatus("Link")
	operationHistogramSymlink       = newOperationHistogramWithStatus("Symlink")
	operationHistogramReadlink      = newOperationHistogramWithStatus("Readlink")
	operationHistogramAccess        = newOperationHistogramWithStatus("Access")
	operationHistogramGetXAttr      = newOperationHistogramWithStatus("GetXAttr")
	operationHistogramListXAttr     = newOperationHistogramWithStatus("ListXAttr")
	operationHistogramSetXAttr      = newOperationHistogramWithStatus("SetXAttr")
	operationHistogramRemoveXAttr   = newOperationHistogramWithStatus("RemoveXAttr")
	operationHistogramCreate        = newOperationHistogramWithStatus("Create")
	operationHistogramOpen          = newOperationHistogramWithStatus("Open")
	operationHistogramRead          = newOperationHistogramWithStatus("Read")
	operationHistogramLseek         = newOperationHistogramWithStatus("Lseek")
	operationHistogramGetLk         = newOperationHistogramWithStatus("GetLk")
	operationHistogramSetLk         = newOperationHistogramWithStatus("SetLk")
	operationHistogramSetLkw        = newOperationHistogramWithStatus("SetLkw")
	operationHistogramRelease       = newOperationHistogram("Release")
	operationHistogramWrite         = newOperationHistogramWithStatus("Write")
	operationHistogramCopyFileRange = newOperationHistogramWithStatus("CopyFileRange")
	operationHistogramFlush         = newOperationHistogramWithStatus("Flush")
	operationHistogramFsync         = newOperationHistogramWithStatus("Fsync")
	operationHistogramFallocate     = newOperationHistogramWithStatus("Fallocate")
	operationHistogramOpenDir       = newOperationHistogramWithStatus("OpenDir")
	operationHistogramReadDir       = newOperationHistogramWithStatus("ReadDir")
	operationHistogramReadDirPlus   = newOperationHistogramWithStatus("ReadDirPlus")
	operationHistogramReleaseDir    = newOperationHistogram("ReleaseDir")
	operationHistogramFsyncDir      = newOperationHistogramWithStatus("FsyncDir")
	operationHistogramStatFs        = newOperationHistogramWithStatus("StatFs")

	callbackCounterDeleteNotify          = newCallbackCounterWithStatus("DeleteNotify")
	callbackCounterEntryNotify           = newCallbackCounterWithStatus("EntryNotify")
	callbackCounterInodeNotify           = newCallbackCounterWithStatus("InodeNotify")
	callbackCounterInodeRetrieveCache    = newCallbackCounterWithStatus("InodeRetrieveCache")
	callbackCounterInodeNotifyStoreCache = newCallbackCounterWithStatus("InodeNotifyStoreCache")
)

type metricsRawFileSystem struct {
	base  fuse.RawFileSystem
	clock clock.Clock
}

// NewMetricsRawFileSystem creates a decorator for fuse.RawFileSystem
// that exposes Prometheus metrics for each of the operations invoked.
func NewMetricsRawFileSystem(base fuse.RawFileSystem, clock clock.Clock) fuse.RawFileSystem {
	rawFileSystemOperationsPrometheusMetrics.Do(func() {
		prometheus.MustRegister(rawFileSystemOperationsDurationSeconds)
		prometheus.MustRegister(rawFileSystemCallbacks)
	})

	return &metricsRawFileSystem{
		base:  base,
		clock: clock,
	}
}

func (rfs *metricsRawFileSystem) String() string {
	return rfs.base.String()
}

func (rfs *metricsRawFileSystem) SetDebug(debug bool) {
	rfs.base.SetDebug(debug)
}

func (rfs *metricsRawFileSystem) Lookup(cancel <-chan struct{}, header *fuse.InHeader, name string, out *fuse.EntryOut) fuse.Status {
	timeStart := rfs.clock.Now()
	s := rfs.base.Lookup(cancel, header, name, out)
	operationHistogramLookup.observe(s, timeStart, rfs.clock.Now())
	return s
}

func (rfs *metricsRawFileSystem) Forget(nodeID, nLookup uint64) {
	timeStart := rfs.clock.Now()
	rfs.base.Forget(nodeID, nLookup)
	operationHistogramForget.observe(timeStart, rfs.clock.Now())
}

func (rfs *metricsRawFileSystem) GetAttr(cancel <-chan struct{}, input *fuse.GetAttrIn, out *fuse.AttrOut) fuse.Status {
	timeStart := rfs.clock.Now()
	s := rfs.base.GetAttr(cancel, input, out)
	operationHistogramGetAttr.observe(s, timeStart, rfs.clock.Now())
	return s
}

func (rfs *metricsRawFileSystem) SetAttr(cancel <-chan struct{}, input *fuse.SetAttrIn, out *fuse.AttrOut) fuse.Status {
	timeStart := rfs.clock.Now()
	s := rfs.base.SetAttr(cancel, input, out)
	operationHistogramSetAttr.observe(s, timeStart, rfs.clock.Now())
	return s
}

func (rfs *metricsRawFileSystem) Mknod(cancel <-chan struct{}, input *fuse.MknodIn, name string, out *fuse.EntryOut) fuse.Status {
	timeStart := rfs.clock.Now()
	s := rfs.base.Mknod(cancel, input, name, out)
	operationHistogramMknod.observe(s, timeStart, rfs.clock.Now())
	return s
}

func (rfs *metricsRawFileSystem) Mkdir(cancel <-chan struct{}, input *fuse.MkdirIn, name string, out *fuse.EntryOut) fuse.Status {
	timeStart := rfs.clock.Now()
	s := rfs.base.Mkdir(cancel, input, name, out)
	operationHistogramMkdir.observe(s, timeStart, rfs.clock.Now())
	return s
}

func (rfs *metricsRawFileSystem) Unlink(cancel <-chan struct{}, header *fuse.InHeader, name string) fuse.Status {
	timeStart := rfs.clock.Now()
	s := rfs.base.Unlink(cancel, header, name)
	operationHistogramUnlink.observe(s, timeStart, rfs.clock.Now())
	return s
}

func (rfs *metricsRawFileSystem) Rmdir(cancel <-chan struct{}, header *fuse.InHeader, name string) fuse.Status {
	timeStart := rfs.clock.Now()
	s := rfs.base.Rmdir(cancel, header, name)
	operationHistogramRmdir.observe(s, timeStart, rfs.clock.Now())
	return s
}

func (rfs *metricsRawFileSystem) Rename(cancel <-chan struct{}, input *fuse.RenameIn, oldName, newName string) fuse.Status {
	timeStart := rfs.clock.Now()
	s := rfs.base.Rename(cancel, input, oldName, newName)
	operationHistogramRename.observe(s, timeStart, rfs.clock.Now())
	return s
}

func (rfs *metricsRawFileSystem) Link(cancel <-chan struct{}, input *fuse.LinkIn, filename string, out *fuse.EntryOut) fuse.Status {
	timeStart := rfs.clock.Now()
	s := rfs.base.Link(cancel, input, filename, out)
	operationHistogramLink.observe(s, timeStart, rfs.clock.Now())
	return s
}

func (rfs *metricsRawFileSystem) Symlink(cancel <-chan struct{}, header *fuse.InHeader, pointedTo, linkName string, out *fuse.EntryOut) fuse.Status {
	timeStart := rfs.clock.Now()
	s := rfs.base.Symlink(cancel, header, pointedTo, linkName, out)
	operationHistogramSymlink.observe(s, timeStart, rfs.clock.Now())
	return s
}

func (rfs *metricsRawFileSystem) Readlink(cancel <-chan struct{}, header *fuse.InHeader) ([]byte, fuse.Status) {
	timeStart := rfs.clock.Now()
	out, s := rfs.base.Readlink(cancel, header)
	operationHistogramReadlink.observe(s, timeStart, rfs.clock.Now())
	return out, s
}

func (rfs *metricsRawFileSystem) Access(cancel <-chan struct{}, input *fuse.AccessIn) fuse.Status {
	timeStart := rfs.clock.Now()
	s := rfs.base.Access(cancel, input)
	operationHistogramAccess.observe(s, timeStart, rfs.clock.Now())
	return s
}

func (rfs *metricsRawFileSystem) GetXAttr(cancel <-chan struct{}, header *fuse.InHeader, attr string, dest []byte) (uint32, fuse.Status) {
	timeStart := rfs.clock.Now()
	r, s := rfs.base.GetXAttr(cancel, header, attr, dest)
	operationHistogramGetXAttr.observe(s, timeStart, rfs.clock.Now())
	return r, s
}

func (rfs *metricsRawFileSystem) ListXAttr(cancel <-chan struct{}, header *fuse.InHeader, dest []byte) (uint32, fuse.Status) {
	timeStart := rfs.clock.Now()
	r, s := rfs.base.ListXAttr(cancel, header, dest)
	operationHistogramListXAttr.observe(s, timeStart, rfs.clock.Now())
	return r, s
}

func (rfs *metricsRawFileSystem) SetXAttr(cancel <-chan struct{}, input *fuse.SetXAttrIn, attr string, data []byte) fuse.Status {
	timeStart := rfs.clock.Now()
	s := rfs.base.SetXAttr(cancel, input, attr, data)
	operationHistogramSetXAttr.observe(s, timeStart, rfs.clock.Now())
	return s
}

func (rfs *metricsRawFileSystem) RemoveXAttr(cancel <-chan struct{}, header *fuse.InHeader, attr string) fuse.Status {
	timeStart := rfs.clock.Now()
	s := rfs.base.RemoveXAttr(cancel, header, attr)
	operationHistogramRemoveXAttr.observe(s, timeStart, rfs.clock.Now())
	return s
}

func (rfs *metricsRawFileSystem) Create(cancel <-chan struct{}, input *fuse.CreateIn, name string, out *fuse.CreateOut) fuse.Status {
	timeStart := rfs.clock.Now()
	s := rfs.base.Create(cancel, input, name, out)
	operationHistogramCreate.observe(s, timeStart, rfs.clock.Now())
	return s
}

func (rfs *metricsRawFileSystem) Open(cancel <-chan struct{}, input *fuse.OpenIn, out *fuse.OpenOut) fuse.Status {
	timeStart := rfs.clock.Now()
	s := rfs.base.Open(cancel, input, out)
	operationHistogramOpen.observe(s, timeStart, rfs.clock.Now())
	return s
}

func (rfs *metricsRawFileSystem) Read(cancel <-chan struct{}, input *fuse.ReadIn, buf []byte) (fuse.ReadResult, fuse.Status) {
	timeStart := rfs.clock.Now()
	r, s := rfs.base.Read(cancel, input, buf)
	operationHistogramRead.observe(s, timeStart, rfs.clock.Now())
	return r, s
}

func (rfs *metricsRawFileSystem) Lseek(cancel <-chan struct{}, in *fuse.LseekIn, out *fuse.LseekOut) fuse.Status {
	timeStart := rfs.clock.Now()
	s := rfs.base.Lseek(cancel, in, out)
	operationHistogramLseek.observe(s, timeStart, rfs.clock.Now())
	return s
}

func (rfs *metricsRawFileSystem) GetLk(cancel <-chan struct{}, input *fuse.LkIn, out *fuse.LkOut) fuse.Status {
	timeStart := rfs.clock.Now()
	s := rfs.base.GetLk(cancel, input, out)
	operationHistogramGetLk.observe(s, timeStart, rfs.clock.Now())
	return s
}

func (rfs *metricsRawFileSystem) SetLk(cancel <-chan struct{}, input *fuse.LkIn) fuse.Status {
	timeStart := rfs.clock.Now()
	s := rfs.base.SetLk(cancel, input)
	operationHistogramSetLk.observe(s, timeStart, rfs.clock.Now())
	return s
}

func (rfs *metricsRawFileSystem) SetLkw(cancel <-chan struct{}, input *fuse.LkIn) fuse.Status {
	timeStart := rfs.clock.Now()
	s := rfs.base.SetLkw(cancel, input)
	operationHistogramSetLkw.observe(s, timeStart, rfs.clock.Now())
	return s
}

func (rfs *metricsRawFileSystem) Release(cancel <-chan struct{}, input *fuse.ReleaseIn) {
	timeStart := rfs.clock.Now()
	rfs.base.Release(cancel, input)
	operationHistogramRelease.observe(timeStart, rfs.clock.Now())
}

func (rfs *metricsRawFileSystem) Write(cancel <-chan struct{}, input *fuse.WriteIn, data []byte) (uint32, fuse.Status) {
	timeStart := rfs.clock.Now()
	r, s := rfs.base.Write(cancel, input, data)
	operationHistogramWrite.observe(s, timeStart, rfs.clock.Now())
	return r, s
}

func (rfs *metricsRawFileSystem) CopyFileRange(cancel <-chan struct{}, input *fuse.CopyFileRangeIn) (uint32, fuse.Status) {
	timeStart := rfs.clock.Now()
	r, s := rfs.base.CopyFileRange(cancel, input)
	operationHistogramCopyFileRange.observe(s, timeStart, rfs.clock.Now())
	return r, s
}

func (rfs *metricsRawFileSystem) Flush(cancel <-chan struct{}, input *fuse.FlushIn) fuse.Status {
	timeStart := rfs.clock.Now()
	s := rfs.base.Flush(cancel, input)
	operationHistogramFlush.observe(s, timeStart, rfs.clock.Now())
	return s
}

func (rfs *metricsRawFileSystem) Fsync(cancel <-chan struct{}, input *fuse.FsyncIn) fuse.Status {
	timeStart := rfs.clock.Now()
	s := rfs.base.Fsync(cancel, input)
	operationHistogramFsync.observe(s, timeStart, rfs.clock.Now())
	return s
}

func (rfs *metricsRawFileSystem) Fallocate(cancel <-chan struct{}, input *fuse.FallocateIn) fuse.Status {
	timeStart := rfs.clock.Now()
	s := rfs.base.Fallocate(cancel, input)
	operationHistogramFallocate.observe(s, timeStart, rfs.clock.Now())
	return s
}

func (rfs *metricsRawFileSystem) OpenDir(cancel <-chan struct{}, input *fuse.OpenIn, out *fuse.OpenOut) fuse.Status {
	timeStart := rfs.clock.Now()
	s := rfs.base.OpenDir(cancel, input, out)
	operationHistogramOpenDir.observe(s, timeStart, rfs.clock.Now())
	return s
}

func (rfs *metricsRawFileSystem) ReadDir(cancel <-chan struct{}, input *fuse.ReadIn, out fuse.ReadDirEntryList) fuse.Status {
	timeStart := rfs.clock.Now()
	s := rfs.base.ReadDir(cancel, input, out)
	operationHistogramReadDir.observe(s, timeStart, rfs.clock.Now())
	return s
}

func (rfs *metricsRawFileSystem) ReadDirPlus(cancel <-chan struct{}, input *fuse.ReadIn, out fuse.ReadDirPlusEntryList) fuse.Status {
	timeStart := rfs.clock.Now()
	s := rfs.base.ReadDirPlus(cancel, input, out)
	operationHistogramReadDirPlus.observe(s, timeStart, rfs.clock.Now())
	return s
}

func (rfs *metricsRawFileSystem) ReleaseDir(input *fuse.ReleaseIn) {
	timeStart := rfs.clock.Now()
	rfs.base.ReleaseDir(input)
	operationHistogramReleaseDir.observe(timeStart, rfs.clock.Now())
}

func (rfs *metricsRawFileSystem) FsyncDir(cancel <-chan struct{}, input *fuse.FsyncIn) fuse.Status {
	timeStart := rfs.clock.Now()
	s := rfs.base.FsyncDir(cancel, input)
	operationHistogramFsyncDir.observe(s, timeStart, rfs.clock.Now())
	return s
}

func (rfs *metricsRawFileSystem) StatFs(cancel <-chan struct{}, input *fuse.InHeader, out *fuse.StatfsOut) fuse.Status {
	timeStart := rfs.clock.Now()
	s := rfs.base.StatFs(cancel, input, out)
	operationHistogramStatFs.observe(s, timeStart, rfs.clock.Now())
	return s
}

func (rfs *metricsRawFileSystem) Init(server fuse.ServerCallbacks) {
	rfs.base.Init(&metricsServerCallbacks{
		base: server,
	})
}

type metricsServerCallbacks struct {
	base fuse.ServerCallbacks
}

func (sc *metricsServerCallbacks) DeleteNotify(parent, child uint64, name string) fuse.Status {
	s := sc.base.DeleteNotify(parent, child, name)
	callbackCounterDeleteNotify.inc(s)
	return s
}

func (sc *metricsServerCallbacks) EntryNotify(parent uint64, name string) fuse.Status {
	s := sc.base.EntryNotify(parent, name)
	callbackCounterEntryNotify.inc(s)
	return s
}

func (sc *metricsServerCallbacks) InodeNotify(node uint64, off, length int64) fuse.Status {
	s := sc.base.InodeNotify(node, off, length)
	callbackCounterInodeNotify.inc(s)
	return s
}

func (sc *metricsServerCallbacks) InodeRetrieveCache(node uint64, offset int64, dest []byte) (int, fuse.Status) {
	r, s := sc.base.InodeRetrieveCache(node, offset, dest)
	callbackCounterInodeRetrieveCache.inc(s)
	return r, s
}

func (sc *metricsServerCallbacks) InodeNotifyStoreCache(node uint64, offset int64, data []byte) fuse.Status {
	s := sc.base.InodeNotifyStoreCache(node, offset, data)
	callbackCounterInodeNotifyStoreCache.inc(s)
	return s
}
