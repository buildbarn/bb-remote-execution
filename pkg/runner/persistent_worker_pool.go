package runner

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"io"
	"log"
	"os"
	"os/exec"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/buildbarn/bb-remote-execution/pkg/proto/bazelworker"
	runner_pb "github.com/buildbarn/bb-remote-execution/pkg/proto/runner"
	"github.com/buildbarn/bb-storage/pkg/clock"
	"github.com/buildbarn/bb-storage/pkg/filesystem"
	"github.com/buildbarn/bb-storage/pkg/filesystem/path"
	"github.com/buildbarn/bb-storage/pkg/util"
	"github.com/prometheus/client_golang/prometheus"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var (
	persistentWorkerPoolPrometheusMetrics sync.Once

	persistentWorkerPoolOperationsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "buildbarn",
			Subsystem: "runner",
			Name:      "persistent_worker_pool_operations_total",
			Help:      "Number of times a persistent worker process was requested from the pool, labeled by whether an existing process could be reused.",
		},
		[]string{"result"},
	)
	persistentWorkerPoolTerminationsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "buildbarn",
			Subsystem: "runner",
			Name:      "persistent_worker_pool_terminations_total",
			Help:      "Number of persistent worker processes that were terminated, labeled by the reason for termination.",
		},
		[]string{"reason"},
	)
	persistentWorkerPoolProcesses = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "buildbarn",
			Subsystem: "runner",
			Name:      "persistent_worker_pool_processes",
			Help:      "Number of persistent worker processes that are currently running, labeled by whether they are executing a build action.",
		},
		[]string{"state"},
	)
)

// Filenames of objects that are created inside the directory of a
// single persistent worker process.
var (
	persistentWorkerExecRootComponent           = path.MustNewComponent("root")
	persistentWorkerStderrComponent             = path.MustNewComponent("stderr")
	persistentWorkerTemporaryDirectoryComponent = path.MustNewComponent("tmp")
)

// persistentWorkerKey identifies a set of interchangeable persistent
// worker processes. In addition to the key that the client provided
// (i.e., Bazel's 'persistentWorkerKey' platform property), it covers
// every property of the environment in which the worker process runs.
// This ensures that a worker process is never reused for a build action
// that expects to run in a different environment, even if the client
// computed its key incorrectly.
type persistentWorkerKey struct {
	toolKey     string
	fingerprint [sha256.Size]byte
}

func newPersistentWorkerKey(toolKey string, protocol runner_pb.PersistentWorker_Protocol, workingDirectory string, arguments, toolInputPaths []string, environmentVariables map[string]string) persistentWorkerKey {
	hasher := sha256.New()
	writeString := func(s string) {
		// Prefix every string with its length, so that no two
		// distinct inputs can yield the same byte stream.
		var lengthBuffer [binary.MaxVarintLen64]byte
		hasher.Write(lengthBuffer[:binary.PutUvarint(lengthBuffer[:], uint64(len(s)))])
		io.WriteString(hasher, s)
	}
	writeString(toolKey)
	writeString(protocol.String())
	writeString(workingDirectory)
	writeString(strconv.Itoa(len(arguments)))
	for _, argument := range arguments {
		writeString(argument)
	}
	// The contents of the tool are already covered by the key that
	// the client provided. Its layout is not, while the worker
	// process only materializes it once.
	writeString(strconv.Itoa(len(toolInputPaths)))
	for _, toolInputPath := range toolInputPaths {
		writeString(toolInputPath)
	}
	names := make([]string, 0, len(environmentVariables))
	for name := range environmentVariables {
		names = append(names, name)
	}
	sort.Strings(names)
	writeString(strconv.Itoa(len(names)))
	for _, name := range names {
		writeString(name)
		writeString(environmentVariables[name])
	}

	key := persistentWorkerKey{toolKey: toolKey}
	hasher.Sum(key.fingerprint[:0])
	return key
}

// PersistentWorkerPool holds a set of persistent worker processes that
// bb_runner keeps running in between build actions.
//
// Every worker process is given a directory of its own, containing an
// execution root, a temporary directory and a log file holding the data
// that the worker process wrote to standard error. Because a worker
// process cannot change its working directory after it has been
// launched, its execution root is populated with symbolic links that
// point into the input root of the build action that is currently being
// executed. This is the same strategy that Bazel's symlinked sandbox
// uses.
type PersistentWorkerPool struct {
	directory                    filesystem.Directory
	directoryPath                *path.Builder
	commandCreator               CommandCreator
	clock                        clock.Clock
	maximumWorkerCount           int
	idleTimeout                  time.Duration
	setTmpdirEnvironmentVariable bool

	lock              sync.Mutex
	nextWorkerID      uint64
	idleWorkers       map[persistentWorkerKey][]*persistentWorkerProcess
	idleWorkerCount   int
	busyWorkerCount   int
	runningProcessIDs map[int]struct{}
}

// NewPersistentWorkerPool creates a PersistentWorkerPool that stores the
// state of worker processes in a directory on the local system. Any
// data left behind in this directory by a previous invocation of
// bb_runner is removed.
func NewPersistentWorkerPool(directory filesystem.Directory, directoryPath *path.Builder, commandCreator CommandCreator, clock clock.Clock, maximumWorkerCount int, idleTimeout time.Duration, setTmpdirEnvironmentVariable bool) (*PersistentWorkerPool, error) {
	persistentWorkerPoolPrometheusMetrics.Do(func() {
		prometheus.MustRegister(persistentWorkerPoolOperationsTotal)
		prometheus.MustRegister(persistentWorkerPoolTerminationsTotal)
		prometheus.MustRegister(persistentWorkerPoolProcesses)
	})

	if maximumWorkerCount < 1 {
		return nil, status.Error(codes.InvalidArgument, "The maximum number of persistent worker processes must be positive")
	}
	if idleTimeout <= 0 {
		return nil, status.Error(codes.InvalidArgument, "The idle timeout of persistent worker processes must be positive")
	}
	if err := directory.RemoveAllChildren(); err != nil {
		return nil, util.StatusWrapWithCode(err, codes.Internal, "Failed to clean persistent worker directory")
	}
	return &PersistentWorkerPool{
		directory:                    directory,
		directoryPath:                directoryPath,
		commandCreator:               commandCreator,
		clock:                        clock,
		maximumWorkerCount:           maximumWorkerCount,
		idleTimeout:                  idleTimeout,
		setTmpdirEnvironmentVariable: setTmpdirEnvironmentVariable,

		idleWorkers:       map[persistentWorkerKey][]*persistentWorkerProcess{},
		runningProcessIDs: map[int]struct{}{},
	}, nil
}

// Run terminates persistent worker processes that have remained idle
// for longer than the configured idle timeout. Because expiration is
// only evaluated at a fixed interval, a worker process may remain alive
// for up to twice the idle timeout.
//
// Upon cancelation of the context, all idle worker processes are
// terminated. Worker processes that are still executing a build action
// are left alone, as bb_runner's gRPC server is shut down before this
// function returns.
func (p *PersistentWorkerPool) Run(ctx context.Context) error {
	for {
		timer, timerChannel := p.clock.NewTimer(p.idleTimeout)
		select {
		case <-timerChannel:
			p.terminateIdleWorkers(false)
		case <-ctx.Done():
			timer.Stop()
			p.terminateIdleWorkers(true)
			return nil
		}
	}
}

// ContainsProcessID returns whether a given process ID belongs to one of
// the persistent worker processes that are currently running. This may
// be used to prevent bb_runner's process table cleaner from terminating
// persistent worker processes in between build actions.
func (p *PersistentWorkerPool) ContainsProcessID(processID int) bool {
	p.lock.Lock()
	defer p.lock.Unlock()
	_, ok := p.runningProcessIDs[processID]
	return ok
}

func (p *PersistentWorkerPool) acquire(key persistentWorkerKey, protocol runner_pb.PersistentWorker_Protocol) (*persistentWorkerProcess, error) {
	p.lock.Lock()

	// Reuse an idle worker process if one is available. Prefer the
	// one that was released most recently, as its caches are most
	// likely to still be warm.
	if workers := p.idleWorkers[key]; len(workers) > 0 {
		w := workers[len(workers)-1]
		workers[len(workers)-1] = nil
		if len(workers) == 1 {
			delete(p.idleWorkers, key)
		} else {
			p.idleWorkers[key] = workers[:len(workers)-1]
		}
		p.idleWorkerCount--
		p.busyWorkerCount++
		p.updateProcessCountsLocked()
		p.lock.Unlock()
		persistentWorkerPoolOperationsTotal.WithLabelValues("Reused").Inc()
		return w, nil
	}

	// Make space for a new worker process by terminating the ones
	// that have been idle for the longest amount of time. Note that
	// the maximum cannot be enforced strictly, as worker processes
	// that are executing a build action cannot be terminated.
	var expired []*persistentWorkerProcess
	for p.idleWorkerCount > 0 && p.idleWorkerCount+p.busyWorkerCount >= p.maximumWorkerCount {
		expired = append(expired, p.removeLeastRecentlyUsedIdleWorkerLocked())
	}
	p.busyWorkerCount++
	workerID := p.nextWorkerID
	p.nextWorkerID++
	p.updateProcessCountsLocked()
	p.lock.Unlock()

	for _, w := range expired {
		w.terminate("Evicted")
	}

	w, err := p.newWorkerProcess(key, protocol, workerID)
	if err != nil {
		p.lock.Lock()
		p.busyWorkerCount--
		p.updateProcessCountsLocked()
		p.lock.Unlock()
		return nil, err
	}
	persistentWorkerPoolOperationsTotal.WithLabelValues("Created").Inc()
	return w, nil
}

func (p *PersistentWorkerPool) release(w *persistentWorkerProcess, healthy bool) {
	p.lock.Lock()
	p.busyWorkerCount--
	if !healthy {
		p.updateProcessCountsLocked()
		p.lock.Unlock()
		w.terminate("Failed")
		return
	}

	w.lastUsed = p.clock.Now()
	p.idleWorkers[w.key] = append(p.idleWorkers[w.key], w)
	p.idleWorkerCount++

	var expired []*persistentWorkerProcess
	for p.idleWorkerCount > 0 && p.idleWorkerCount+p.busyWorkerCount > p.maximumWorkerCount {
		expired = append(expired, p.removeLeastRecentlyUsedIdleWorkerLocked())
	}
	p.updateProcessCountsLocked()
	p.lock.Unlock()

	for _, w := range expired {
		w.terminate("Evicted")
	}
}

// terminateIdleWorkers terminates all worker processes that are
// currently idle. Unless 'all' is set, only the ones that have not been
// used for at least the idle timeout are terminated.
func (p *PersistentWorkerPool) terminateIdleWorkers(all bool) {
	now := p.clock.Now()
	p.lock.Lock()
	var expired []*persistentWorkerProcess
	for key, workers := range p.idleWorkers {
		retained := 0
		for _, w := range workers {
			if all || !now.Before(w.lastUsed.Add(p.idleTimeout)) {
				expired = append(expired, w)
				p.idleWorkerCount--
			} else {
				workers[retained] = w
				retained++
			}
		}
		if retained == 0 {
			delete(p.idleWorkers, key)
		} else {
			p.idleWorkers[key] = workers[:retained]
		}
	}
	p.updateProcessCountsLocked()
	p.lock.Unlock()

	for _, w := range expired {
		w.terminate("IdleTimeout")
	}
}

func (p *PersistentWorkerPool) removeLeastRecentlyUsedIdleWorkerLocked() *persistentWorkerProcess {
	var bestKey persistentWorkerKey
	bestIndex := -1
	var bestWorker *persistentWorkerProcess
	for key, workers := range p.idleWorkers {
		for i, w := range workers {
			if bestWorker == nil || w.lastUsed.Before(bestWorker.lastUsed) {
				bestKey, bestIndex, bestWorker = key, i, w
			}
		}
	}
	if bestWorker == nil {
		panic("Attempted to remove an idle worker process while none exist")
	}
	workers := p.idleWorkers[bestKey]
	workers = append(workers[:bestIndex], workers[bestIndex+1:]...)
	if len(workers) == 0 {
		delete(p.idleWorkers, bestKey)
	} else {
		p.idleWorkers[bestKey] = workers
	}
	p.idleWorkerCount--
	return bestWorker
}

func (p *PersistentWorkerPool) updateProcessCountsLocked() {
	persistentWorkerPoolProcesses.WithLabelValues("Idle").Set(float64(p.idleWorkerCount))
	persistentWorkerPoolProcesses.WithLabelValues("Busy").Set(float64(p.busyWorkerCount))
}

// newWorkerProcess creates the directory in which a new persistent
// worker process will run. The process itself is only launched once the
// execution root has been populated.
func (p *PersistentWorkerPool) newWorkerProcess(key persistentWorkerKey, protocol runner_pb.PersistentWorker_Protocol, workerID uint64) (*persistentWorkerProcess, error) {
	directoryName := path.MustNewComponent(strconv.FormatUint(workerID, 10))
	if err := p.directory.Mkdir(directoryName, 0o777); err != nil {
		return nil, util.StatusWrapfWithCode(err, codes.Internal, "Failed to create directory for persistent worker %#v", directoryName.String())
	}
	w := &persistentWorkerProcess{
		pool:          p,
		key:           key,
		protocolKind:  protocol,
		directoryName: directoryName,
	}
	if err := w.createDirectories(); err != nil {
		w.removeDirectory()
		return nil, err
	}
	return w, nil
}

// persistentWorkerProcess corresponds to a single persistent worker
// process. Instances are only accessed by a single goroutine at a time,
// as the Bazel persistent worker protocol only permits a single request
// to be in flight for workers that do not implement multiplexing.
type persistentWorkerProcess struct {
	pool          *PersistentWorkerPool
	key           persistentWorkerKey
	protocolKind  runner_pb.PersistentWorker_Protocol
	directoryName path.Component

	directory              filesystem.DirectoryCloser
	execRoot               filesystem.DirectoryCloser
	execRootPath           *path.Builder
	stderrPath             string
	temporaryDirectoryPath string

	// Set once the input files of the tool have been copied into
	// the execution root. Every build action that reaches the same
	// worker process provides the same tool, as both its contents
	// and its layout are covered by the worker's key.
	toolInputsMaterialized bool

	cmd          *exec.Cmd
	stdinWriter  *os.File
	stdoutReader *os.File
	stderrFile   *os.File
	protocol     persistentWorkerProtocol

	// Time at which the process was last released into the pool.
	// Only accessed while holding the pool's lock.
	lastUsed time.Time
}

func (w *persistentWorkerProcess) createDirectories() error {
	p := w.pool
	directory, err := p.directory.EnterDirectory(w.directoryName)
	if err != nil {
		return util.StatusWrapWithCode(err, codes.Internal, "Failed to enter directory of persistent worker")
	}
	w.directory = directory

	for _, name := range []path.Component{persistentWorkerExecRootComponent, persistentWorkerTemporaryDirectoryComponent} {
		if err := directory.Mkdir(name, 0o777); err != nil {
			return util.StatusWrapfWithCode(err, codes.Internal, "Failed to create %#v directory of persistent worker", name.String())
		}
	}
	execRoot, err := directory.EnterDirectory(persistentWorkerExecRootComponent)
	if err != nil {
		return util.StatusWrapWithCode(err, codes.Internal, "Failed to enter execution root of persistent worker")
	}
	w.execRoot = execRoot

	directoryPath, err := appendComponentsToPath(p.directoryPath, w.directoryName)
	if err != nil {
		return err
	}
	if w.execRootPath, err = appendComponentsToPath(directoryPath, persistentWorkerExecRootComponent); err != nil {
		return err
	}
	if w.stderrPath, err = getLocalPathString(directoryPath, persistentWorkerStderrComponent); err != nil {
		return err
	}
	if w.temporaryDirectoryPath, err = getLocalPathString(directoryPath, persistentWorkerTemporaryDirectoryComponent); err != nil {
		return err
	}
	return nil
}

// prepareExecRoot repopulates the execution root of the worker process,
// so that it provides a view of the input root of the build action that
// is about to be executed. Because the working directory of a running
// process cannot be changed, the execution root is not replaced, but
// filled with symbolic links that point into the input root.
func (w *persistentWorkerProcess) prepareExecRoot(inputRootDirectory filesystem.Directory, inputRootPath *path.Builder, workingDirectory []path.Component, toolInputs *toolInputTree) error {
	if !w.toolInputsMaterialized {
		if err := materializeToolInputs(w.execRoot, inputRootDirectory, toolInputs); err != nil {
			return err
		}
		w.toolInputsMaterialized = true
	}
	return refreshSymlinkFarm(w.execRoot, inputRootDirectory, inputRootPath, workingDirectory, toolInputs)
}

// refreshSymlinkFarm replaces the contents of the target directory with
// symbolic links that point to the children of the source directory.
//
// If a working directory is provided, its first component is retained
// as a real directory, into which this function recurses. This ensures
// that the process is capable of resolving paths relative to the input
// root, even if its working directory is nested inside of it. These
// directories are reused if they already exist, as replacing them would
// leave a worker process that is already running with a working
// directory that has been unlinked.
//
// The input files of the tool are provided by the execution root
// itself, as the worker process outlives the input root that they were
// obtained from. They are therefore left in place, and no symbolic
// links are created for them.
func refreshSymlinkFarm(target, source filesystem.Directory, sourcePath *path.Builder, workingDirectory []path.Component, toolInputs *toolInputTree) error {
	// Discard the contents of the previous build action.
	targetEntries, err := target.ReadDir()
	if err != nil {
		return util.StatusWrapWithCode(err, codes.Internal, "Failed to read contents of the execution root of the persistent worker")
	}
	for _, entry := range targetEntries {
		name := entry.Name()
		if len(workingDirectory) > 0 && name == workingDirectory[0] && entry.Type() == filesystem.FileTypeDirectory {
			continue
		}
		if toolInputs.contains(name) {
			continue
		}
		if err := target.RemoveAll(name); err != nil {
			return util.StatusWrapfWithCode(err, codes.Internal, "Failed to remove %#v from the execution root of the persistent worker", name.String())
		}
	}

	// Create symbolic links to the contents of the input root.
	sourceEntries, err := source.ReadDir()
	if err != nil {
		return util.StatusWrapfWithCode(err, codes.Internal, "Failed to read contents of directory %#v", sourcePath.GetUNIXString())
	}
	for _, entry := range sourceEntries {
		name := entry.Name()
		if len(workingDirectory) > 0 && name == workingDirectory[0] {
			continue
		}
		if toolInputs.contains(name) {
			continue
		}
		targetPath, err := getLocalPathString(sourcePath, name)
		if err != nil {
			return err
		}
		if err := target.Symlink(path.LocalFormat.NewParser(targetPath), name); err != nil {
			return util.StatusWrapfWithCode(err, codes.Internal, "Failed to create symbolic link to %#v", targetPath)
		}
	}

	// Descend into the directories that merely lead to tool inputs,
	// as those hold entries belonging to this build action as well.
	// Directories that consist of nothing but tool inputs are left
	// untouched, which is what keeps this cheap.
	if toolInputs != nil {
		for _, name := range sortedComponents(toolInputs.directories) {
			if len(workingDirectory) > 0 && name == workingDirectory[0] {
				// Handled below, so that both the working
				// directory and the tool are accounted for.
				continue
			}
			sourceChildPath, err := appendComponentsToPath(sourcePath, name)
			if err != nil {
				return err
			}
			if err := enterBoth(target, source, name, func(targetChild, sourceChild filesystem.Directory) error {
				return refreshSymlinkFarm(targetChild, sourceChild, sourceChildPath, nil, toolInputs.directories[name])
			}); err != nil {
				return err
			}
		}
	}

	if len(workingDirectory) == 0 {
		return nil
	}

	// Descend into the working directory of the build action.
	name := workingDirectory[0]
	if err := target.Mkdir(name, 0o777); err != nil && !os.IsExist(err) {
		return util.StatusWrapfWithCode(err, codes.Internal, "Failed to create directory %#v in the execution root of the persistent worker", name.String())
	}
	targetChild, err := target.EnterDirectory(name)
	if err != nil {
		return util.StatusWrapfWithCode(err, codes.Internal, "Failed to enter directory %#v in the execution root of the persistent worker", name.String())
	}
	defer targetChild.Close()
	sourceChild, err := source.EnterDirectory(name)
	if err != nil {
		return util.StatusWrapfWithCode(err, codes.Internal, "Failed to enter directory %#v of the input root", name.String())
	}
	defer sourceChild.Close()
	sourceChildPath, err := appendComponentsToPath(sourcePath, name)
	if err != nil {
		return err
	}
	return refreshSymlinkFarm(targetChild, sourceChild, sourceChildPath, workingDirectory[1:], toolInputs.child(name))
}

// harvestExecRoot moves files that the build action created in the
// execution root of the worker process into the input root of the build
// action, which is the directory from which bb_worker collects output
// files.
//
// Every entry that we place in the execution root is either a symbolic
// link that points into the input root, one of the real directories
// that make up the working directory of the build action, or part of
// the tool. Data written through the first already ends up in the input
// root, and the tool has to stay where it is, as the worker process
// keeps running. Anything else was created by the build action itself,
// and would be discarded when the execution root is emptied before the
// next build action runs.
func (w *persistentWorkerProcess) harvestExecRoot(inputRootDirectory filesystem.Directory, workingDirectory []path.Component, toolInputs *toolInputTree) error {
	return harvestSymlinkFarm(w.execRoot, inputRootDirectory, workingDirectory, toolInputs)
}

func harvestSymlinkFarm(target, source filesystem.Directory, workingDirectory []path.Component, toolInputs *toolInputTree) error {
	entries, err := target.ReadDir()
	if err != nil {
		return util.StatusWrapWithCode(err, codes.Internal, "Failed to read the execution root of the persistent worker")
	}
	for _, entry := range entries {
		name := entry.Name()
		if entry.Type() == filesystem.FileTypeSymlink {
			// One of the symbolic links that we created.
			continue
		}
		if len(workingDirectory) > 0 && name == workingDirectory[0] && entry.Type() == filesystem.FileTypeDirectory {
			if err := harvestWorkingDirectory(target, source, workingDirectory, toolInputs); err != nil {
				return err
			}
			continue
		}
		if child := toolInputs.child(name); child != nil {
			// A directory that leads to tool inputs. The tool
			// needs to stay in place, but the build action
			// may have created files next to it.
			if err := enterBoth(target, source, name, func(targetChild, sourceChild filesystem.Directory) error {
				return harvestSymlinkFarm(targetChild, sourceChild, nil, child)
			}); err != nil {
				return err
			}
			continue
		}
		if toolInputs.contains(name) {
			// Part of the tool. Moving it into the input root
			// would unlink the executable of the very process
			// that is expected to handle the next build
			// action.
			continue
		}
		if err := moveIntoInputRoot(target, source, name); err != nil {
			return err
		}
	}
	return nil
}

func harvestWorkingDirectory(target, source filesystem.Directory, workingDirectory []path.Component, toolInputs *toolInputTree) error {
	name := workingDirectory[0]
	targetChild, err := target.EnterDirectory(name)
	if err != nil {
		return util.StatusWrapfWithCode(err, codes.Internal, "Failed to enter directory %#v in the execution root of the persistent worker", name.String())
	}
	defer targetChild.Close()
	sourceChild, err := source.EnterDirectory(name)
	if err != nil {
		return util.StatusWrapfWithCode(err, codes.Internal, "Failed to enter directory %#v of the input root", name.String())
	}
	defer sourceChild.Close()
	return harvestSymlinkFarm(targetChild, sourceChild, workingDirectory[1:], toolInputs.child(name))
}

// moveIntoInputRoot moves a single file or directory from the execution
// root of a persistent worker process into the input root of the build
// action.
func moveIntoInputRoot(target, source filesystem.Directory, name path.Component) error {
	err := target.Rename(name, source, name)
	if err == nil {
		return nil
	}
	if isCrossDevice(err) {
		return copyIntoInputRoot(target, source, name)
	}

	// Renaming may also fail if the input root already contains a
	// non-empty directory under the same name, which happens if the
	// build action replaced one of the symbolic links with a
	// directory of its own. The version created by the build action
	// takes precedence.
	if err := source.RemoveAll(name); err != nil {
		return util.StatusWrapfWithCode(err, codes.Internal, "Failed to remove %#v from the input root", name.String())
	}
	if err := target.Rename(name, source, name); err != nil {
		if isCrossDevice(err) {
			return copyIntoInputRoot(target, source, name)
		}
		return util.StatusWrapfWithCode(err, codes.Internal, "Failed to move %#v from the execution root of the persistent worker into the input root", name.String())
	}
	return nil
}

// isCrossDevice returns whether an attempt to rename a file failed
// because the source and target directory are not backed by the same
// file system.
//
// This is the common case for production deployments, which place the
// directories of persistent worker processes on local disk, while build
// directories are provided by a virtual file system. The same error is
// reported when the two directories are backed by different
// implementations of filesystem.Directory, which cannot rename between
// each other either.
func isCrossDevice(err error) bool {
	return errors.Is(err, syscall.EXDEV)
}

// copyIntoInputRoot copies a file or directory from the execution root
// of a persistent worker process into the input root of the build
// action, removing the original afterwards. It is the fallback that is
// used when the two directories are not backed by the same file system,
// which rules out renaming.
func copyIntoInputRoot(target, source filesystem.Directory, name path.Component) error {
	// Anything the build action created takes precedence over what
	// the input root already holds under the same name. Note that
	// filesystem.Directory.RemoveAll() reports an error when the
	// entry is absent, which is the common case here.
	if err := source.RemoveAll(name); err != nil && !os.IsNotExist(err) {
		return util.StatusWrapfWithCode(err, codes.Internal, "Failed to remove %#v from the input root", name.String())
	}
	// Hard linking is not attempted, as it fails for the very same
	// reason that renaming did.
	if err := copyEntry(source, target, name, false); err != nil {
		return util.StatusWrapfWithCode(err, codes.Internal, "Failed to copy %#v from the execution root of the persistent worker into the input root", name.String())
	}
	if err := target.RemoveAll(name); err != nil {
		return util.StatusWrapfWithCode(err, codes.Internal, "Failed to remove %#v from the execution root of the persistent worker", name.String())
	}
	return nil
}

// ensureStarted launches the worker process, if this hasn't happened
// yet. This must only be called after the execution root has been
// populated, as the working directory of the process needs to exist and
// argv[0] may refer to an executable stored in the input root.
func (w *persistentWorkerProcess) ensureStarted(arguments []string, environmentVariables map[string]string, workingDirectoryParser path.Parser) error {
	if w.cmd != nil {
		return nil
	}
	p := w.pool

	// Note that the process is deliberately not associated with the
	// context of the build action that causes it to be launched, as
	// the process needs to outlive it.
	cmd, err := p.commandCreator(context.Background(), arguments, w.execRootPath, workingDirectoryParser, environmentVariables["PATH"])
	if err != nil {
		return err
	}
	cmd.Env = make([]string, 0, len(environmentVariables)+len(temporaryDirectoryEnvironmentVariablePrefixes))
	if p.setTmpdirEnvironmentVariable {
		for _, prefix := range temporaryDirectoryEnvironmentVariablePrefixes {
			cmd.Env = append(cmd.Env, prefix+w.temporaryDirectoryPath)
		}
	}
	for name, value := range environmentVariables {
		cmd.Env = append(cmd.Env, name+"="+value)
	}

	// Attach pipes to the standard input and output of the worker
	// process. Regular files are used, as opposed to the pipes
	// created by exec.Cmd.StdinPipe(), so that exec.Cmd.Wait() does
	// not need to wait for any I/O to complete. That would make it
	// impossible to terminate worker processes reliably.
	stdinReader, stdinWriter, err := os.Pipe()
	if err != nil {
		return util.StatusWrapWithCode(err, codes.Internal, "Failed to create standard input pipe")
	}
	defer stdinReader.Close()
	stdoutReader, stdoutWriter, err := os.Pipe()
	if err != nil {
		stdinWriter.Close()
		return util.StatusWrapWithCode(err, codes.Internal, "Failed to create standard output pipe")
	}
	defer stdoutWriter.Close()

	// Persistent workers use standard error to emit diagnostics that
	// are not associated with any single build action. Capture them
	// in a log file, so that they can be inspected by operators.
	stderrFile, err := os.OpenFile(w.stderrPath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o666)
	if err != nil {
		stdinWriter.Close()
		stdoutReader.Close()
		return util.StatusWrapfWithCode(err, codes.Internal, "Failed to create log file %#v", w.stderrPath)
	}

	cmd.Stdin = stdinReader
	cmd.Stdout = stdoutWriter
	cmd.Stderr = stderrFile
	if err := cmd.Start(); err != nil {
		stdinWriter.Close()
		stdoutReader.Close()
		stderrFile.Close()
		code := codes.Internal
		for _, invalidArgumentErr := range invalidArgumentErrs {
			if errors.Is(err, invalidArgumentErr) {
				code = codes.InvalidArgument
				break
			}
		}
		return util.StatusWrapfWithCode(err, code, "Failed to start persistent worker process %#v", strings.Join(arguments, " "))
	}

	protocol, err := newPersistentWorkerProtocol(w.protocolKind, stdinWriter, stdoutReader)
	if err != nil {
		// Should not happen, as the protocol is validated before
		// the process is created.
		cmd.Process.Kill()
		cmd.Wait()
		stdinWriter.Close()
		stdoutReader.Close()
		stderrFile.Close()
		return err
	}

	w.cmd = cmd
	w.stdinWriter = stdinWriter
	w.stdoutReader = stdoutReader
	w.stderrFile = stderrFile
	w.protocol = protocol

	p.lock.Lock()
	p.runningProcessIDs[cmd.Process.Pid] = struct{}{}
	p.lock.Unlock()
	return nil
}

// exchange sends a WorkRequest to the worker process and waits for the
// corresponding WorkResponse to be returned. Any error returned by this
// function leaves the worker process in an undefined state, meaning
// that the caller must terminate it.
func (w *persistentWorkerProcess) exchange(ctx context.Context, request *bazelworker.WorkRequest) (*bazelworker.WorkResponse, error) {
	type exchangeResult struct {
		response *bazelworker.WorkResponse
		err      error
	}
	// The channel is buffered, so that the goroutine below does not
	// leak if we stop waiting for it.
	results := make(chan exchangeResult, 1)
	go func() {
		if err := w.protocol.WriteWorkRequest(request); err != nil {
			results <- exchangeResult{err: util.StatusWrapWithCode(err, codes.Internal, "Failed to send work request to persistent worker")}
			return
		}
		response, err := w.protocol.ReadWorkResponse()
		if err != nil {
			results <- exchangeResult{err: util.StatusWrapWithCode(err, codes.Internal, "Failed to receive work response from persistent worker")}
			return
		}
		results <- exchangeResult{response: response}
	}()

	select {
	case result := <-results:
		return result.response, result.err
	case <-ctx.Done():
		return nil, util.StatusWrap(status.FromContextError(ctx.Err()).Err(), "Persistent worker did not return a work response")
	}
}

// terminate kills the worker process and removes all state associated
// with it.
func (w *persistentWorkerProcess) terminate(reason string) {
	if w.cmd != nil {
		// Closing the standard input of the worker process
		// requests it to shut down gracefully. Don't wait for
		// that to happen, as a worker process that is
		// unresponsive would block progress indefinitely.
		w.stdinWriter.Close()
		if err := w.cmd.Process.Kill(); err != nil && !errors.Is(err, os.ErrProcessDone) {
			log.Printf("Failed to kill persistent worker process %d: %s", w.cmd.Process.Pid, err)
		}
		w.cmd.Wait()
		w.stdoutReader.Close()
		w.stderrFile.Close()

		w.pool.lock.Lock()
		delete(w.pool.runningProcessIDs, w.cmd.Process.Pid)
		w.pool.lock.Unlock()
		w.cmd = nil
	}
	if w.execRoot != nil {
		w.execRoot.Close()
		w.execRoot = nil
	}
	if w.directory != nil {
		w.directory.Close()
		w.directory = nil
	}
	w.removeDirectory()
	persistentWorkerPoolTerminationsTotal.WithLabelValues(reason).Inc()
}

func (w *persistentWorkerProcess) removeDirectory() {
	if err := w.pool.directory.RemoveAll(w.directoryName); err != nil {
		log.Printf("Failed to remove directory %#v of persistent worker: %s", w.directoryName.String(), err)
	}
}

// appendComponentsToPath extends a path with one or more pathname
// components.
func appendComponentsToPath(base *path.Builder, components ...path.Component) (*path.Builder, error) {
	names := make([]string, 0, len(components))
	for _, component := range components {
		names = append(names, component.String())
	}
	extended, scopeWalker := base.Join(path.VoidScopeWalker)
	if err := path.Resolve(path.UNIXFormat.NewParser(strings.Join(names, "/")), scopeWalker); err != nil {
		return nil, util.StatusWrapWithCode(err, codes.Internal, "Failed to construct pathname")
	}
	return extended, nil
}

// getLocalPathString returns the pathname of a file inside a directory,
// in the format that is used by the local operating system.
func getLocalPathString(base *path.Builder, components ...path.Component) (string, error) {
	extended, err := appendComponentsToPath(base, components...)
	if err != nil {
		return "", err
	}
	s, err := path.LocalFormat.GetString(extended)
	if err != nil {
		return "", util.StatusWrapWithCode(err, codes.Internal, "Failed to create local representation of pathname")
	}
	return s, nil
}
