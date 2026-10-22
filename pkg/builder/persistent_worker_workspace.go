package builder

import (
	"context"
	"errors"
	"maps"
	"os"
	"strings"
	"sync"

	"github.com/buildbarn/bb-remote-execution/pkg/filesystem/pool"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/filesystem"
	"github.com/buildbarn/bb-storage/pkg/filesystem/path"
	"github.com/buildbarn/bb-storage/pkg/util"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var persistentWorkerSessionLogsComponent = path.MustNewComponent("session_logs")

// PersistentWorkerWorkspace retains a build directory for one slot's
// compiler session. It must not be shared between incompatible sessions.
type PersistentWorkerWorkspace struct {
	context          context.Context
	cancel           context.CancelFunc
	errorLogger      capturingErrorLogger
	buildDirectory   BuildDirectory
	inputRoot        BuildDirectory
	buildPath        *path.Trace
	workingDirectory []path.Component
	characterDevices map[path.Component]filesystem.DeviceNumber
	preservedPaths   map[string]filesystem.FileType
	toolInputPaths   map[string]struct{}

	lock        sync.Mutex
	busy        bool
	initialized bool
	retiring    bool
	closing     bool
	closed      bool
	closeError  error
}

// NewPersistentWorkerWorkspace acquires a uniquely named directory for a
// compiler session. The context and file pool must outlive individual
// actions. WorkingDirectory and toolInputPaths must be canonical and
// input-root-relative. Tool inputs must remain immutable and identical
// across every action using the workspace.
func NewPersistentWorkerWorkspace(ctx context.Context, creator BuildDirectoryCreator, filePool pool.FilePool, workingDirectory string, toolInputPaths []string, characterDevices map[path.Component]filesystem.DeviceNumber) (*PersistentWorkerWorkspace, error) {
	var workingDirectoryComponents []path.Component
	if workingDirectory != "" {
		if !validPersistentWorkerInputPath(workingDirectory) {
			return nil, status.Error(codes.InvalidArgument, "Invalid persistent worker working directory")
		}
		for _, name := range strings.Split(workingDirectory, "/") {
			component, ok := path.NewComponent(name)
			if !ok {
				return nil, status.Error(codes.InvalidArgument, "Invalid persistent worker working directory component")
			}
			workingDirectoryComponents = append(workingDirectoryComponents, component)
		}
	}
	preservedPaths := map[string]filesystem.FileType{}
	var workingPath *path.Trace
	for _, component := range workingDirectoryComponents {
		workingPath = workingPath.Append(component)
		preservedPaths[workingPath.GetUNIXString()] = filesystem.FileTypeDirectory
	}
	toolFiles := map[string]struct{}{}
	for _, toolPath := range toolInputPaths {
		if !validPersistentWorkerInputPath(toolPath) {
			return nil, status.Error(codes.InvalidArgument, "Invalid persistent worker tool path")
		}
		var toolTrace *path.Trace
		components := strings.Split(toolPath, "/")
		for componentIndex, name := range components {
			component, ok := path.NewComponent(name)
			if !ok {
				return nil, status.Error(codes.InvalidArgument, "Invalid persistent worker tool path component")
			}
			toolTrace = toolTrace.Append(component)
			fileType := filesystem.FileTypeDirectory
			if componentIndex == len(components)-1 {
				fileType = filesystem.FileTypeRegularFile
			}
			if existing, ok := preservedPaths[toolTrace.GetUNIXString()]; ok && existing != fileType {
				return nil, status.Error(codes.InvalidArgument, "Conflicting persistent worker tool paths")
			}
			preservedPaths[toolTrace.GetUNIXString()] = fileType
		}
		toolFiles[toolPath] = struct{}{}
	}
	if err := ctx.Err(); err != nil {
		return nil, status.FromContextError(err).Err()
	}
	buildDirectory, buildPath, err := creator.GetBuildDirectory(ctx, nil)
	if err != nil {
		return nil, util.StatusWrap(err, "Failed to acquire persistent worker workspace")
	}
	sessionContext, cancel := context.WithCancel(ctx)
	workspace := &PersistentWorkerWorkspace{
		context:          sessionContext,
		cancel:           cancel,
		errorLogger:      capturingErrorLogger{cancel: cancel},
		buildDirectory:   buildDirectory,
		buildPath:        buildPath,
		workingDirectory: workingDirectoryComponents,
		characterDevices: characterDevices,
		preservedPaths:   preservedPaths,
		toolInputPaths:   toolFiles,
	}
	buildDirectory.InstallHooks(filePool, &workspace.errorLogger)
	for _, name := range []path.Component{inputRootDirectoryComponent, temporaryDirectoryComponent, serverLogsDirectoryComponent, persistentWorkerSessionLogsComponent} {
		if err := buildDirectory.Mkdir(name, 0o777); err != nil {
			cancel()
			return nil, errors.Join(util.StatusWrapf(err, "Failed to create persistent worker directory %q", name.String()), buildDirectory.Close())
		}
	}
	workspace.inputRoot, err = buildDirectory.EnterBuildDirectory(inputRootDirectoryComponent)
	if err != nil {
		cancel()
		return nil, errors.Join(util.StatusWrap(err, "Failed to enter persistent worker input root"), buildDirectory.Close())
	}
	return workspace, nil
}

// GetBuildDirectoryPath returns the session's stable, runner-relative
// build directory path. Compiler stderr and process-wide logs belong in
// session_logs, not the per-action stdout, stderr, or server_logs paths.
func (workspace *PersistentWorkerWorkspace) GetBuildDirectoryPath() *path.Trace {
	return workspace.buildPath
}

// PersistentWorkerWorkspaceLease holds exclusive access through input
// preparation, execution, and output upload. Its directory handles are
// borrowed and must not be closed by the caller.
type PersistentWorkerWorkspaceLease struct {
	Context            context.Context
	BuildDirectory     BuildDirectory
	InputRootDirectory BuildDirectory

	workspace        *PersistentWorkerWorkspace
	cancel           context.CancelFunc
	stopActionWatch  func() bool
	stopSessionWatch func() bool
	releaseOnce      sync.Once
}

// Prepare removes previous action files and populates the new input
// root, preserving tool files and the compiler's working directory
// identity. A lease remains held until Release, including while outputs
// are uploaded.
// Preparation failures make the workspace unusable until it is closed.
func (workspace *PersistentWorkerWorkspace) Prepare(ctx context.Context, inputRootDigest digest.Digest, outputHierarchy *OutputHierarchy) (*PersistentWorkerWorkspaceLease, error) {
	if err := ctx.Err(); err != nil {
		return nil, status.FromContextError(err).Err()
	}
	if outputHierarchy == nil {
		return nil, status.Error(codes.InvalidArgument, "Missing persistent worker output hierarchy")
	}
	workspace.lock.Lock()
	if workspace.busy || workspace.retiring || workspace.closed || workspace.context.Err() != nil {
		workspace.lock.Unlock()
		return nil, status.Error(codes.FailedPrecondition, "Persistent worker workspace is not available")
	}
	workspace.busy = true
	workspace.lock.Unlock()

	actionContext, cancel := context.WithCancel(ctx)
	lease := &PersistentWorkerWorkspaceLease{
		Context:            actionContext,
		BuildDirectory:     workspace.buildDirectory,
		InputRootDirectory: workspace.inputRoot,
		workspace:          workspace,
		cancel:             cancel,
		stopActionWatch:    context.AfterFunc(ctx, workspace.cancel),
		stopSessionWatch:   context.AfterFunc(workspace.context, cancel),
	}
	if err := workspace.prepare(inputRootDigest, outputHierarchy); err != nil {
		workspace.cancel()
		lease.Release()
		return nil, err
	}
	if err := workspace.context.Err(); err != nil {
		lease.Release()
		return nil, status.FromContextError(err).Err()
	}
	if err := ctx.Err(); err != nil {
		workspace.cancel()
		lease.Release()
		return nil, status.FromContextError(err).Err()
	}
	workspace.initialized = true
	return lease, nil
}

func (workspace *PersistentWorkerWorkspace) prepare(inputRootDigest digest.Digest, outputHierarchy *OutputHierarchy) error {
	var remainingPaths map[string]filesystem.FileType
	var preservedFiles map[string]struct{}
	if workspace.initialized {
		remainingPaths = maps.Clone(workspace.preservedPaths)
		preservedFiles = workspace.toolInputPaths
	}
	if err := cleanPersistentWorkerDirectory(workspace.context, workspace.inputRoot, nil, remainingPaths); err != nil {
		return util.StatusWrap(err, "Failed to clean persistent worker input root")
	}
	if len(remainingPaths) != 0 {
		return status.Error(codes.FailedPrecondition, "Persistent worker working directory or tool input was removed")
	}
	for _, name := range []path.Component{stdoutComponent, stderrComponent} {
		if err := workspace.buildDirectory.RemoveAll(name); err != nil && !os.IsNotExist(err) {
			return util.StatusWrapf(err, "Failed to remove previous %s", name.String())
		}
	}
	serverLogs, err := workspace.buildDirectory.EnterBuildDirectory(serverLogsDirectoryComponent)
	if err != nil {
		return util.StatusWrap(err, "Failed to enter per-action server logs")
	}
	cleanError := cleanPersistentWorkerDirectory(workspace.context, serverLogs, nil, nil)
	closeError := serverLogs.Close()
	if err := errors.Join(cleanError, closeError); err != nil {
		return util.StatusWrap(err, "Failed to clean per-action server logs")
	}
	if err := populateInputRoot(workspace.context, workspace.inputRoot, &workspace.errorLogger, inputRootDigest.GetDigestFunction(), inputRootDigest.GetProto(), nil, workspace.characterDevices, preservedFiles); err != nil {
		return err
	}
	if err := createPersistentWorkerWorkingDirectory(workspace.inputRoot, workspace.workingDirectory); err != nil {
		return err
	}
	return outputHierarchy.CreateParentDirectories(workspace.inputRoot)
}

func cleanPersistentWorkerDirectory(ctx context.Context, directory BuildDirectory, directoryPath *path.Trace, remainingPaths map[string]filesystem.FileType) error {
	children, err := directory.ReadDir()
	if err != nil {
		return err
	}
	for _, child := range children {
		if err := ctx.Err(); err != nil {
			return status.FromContextError(err).Err()
		}
		childPath := directoryPath.Append(child.Name())
		if fileType, ok := remainingPaths[childPath.GetUNIXString()]; ok {
			if child.Type() != fileType {
				return status.Error(codes.FailedPrecondition, "Persistent worker working directory or tool input was replaced")
			}
			delete(remainingPaths, childPath.GetUNIXString())
			if fileType == filesystem.FileTypeRegularFile {
				continue
			}
			childDirectory, err := directory.EnterBuildDirectory(child.Name())
			if err != nil {
				return err
			}
			cleanError := cleanPersistentWorkerDirectory(ctx, childDirectory, childPath, remainingPaths)
			closeError := childDirectory.Close()
			if err := errors.Join(cleanError, closeError); err != nil {
				return err
			}
		} else if err := directory.RemoveAll(child.Name()); err != nil {
			return err
		}
	}
	return nil
}

func createPersistentWorkerWorkingDirectory(directory BuildDirectory, components []path.Component) error {
	if len(components) == 0 {
		return nil
	}
	if err := directory.Mkdir(components[0], 0o777); err != nil && !os.IsExist(err) {
		return err
	}
	child, err := directory.EnterBuildDirectory(components[0])
	if err != nil {
		return err
	}
	createError := createPersistentWorkerWorkingDirectory(child, components[1:])
	return errors.Join(createError, child.Close())
}

// GetIOError returns the first filesystem error reported during the
// session, including errors from retained files accessed between actions.
func (workspace *PersistentWorkerWorkspace) GetIOError() error {
	return workspace.errorLogger.GetError()
}

// Release ends exclusive access after execution and output processing.
// It leaves the compiler and workspace intact and may be called repeatedly.
func (lease *PersistentWorkerWorkspaceLease) Release() {
	lease.releaseOnce.Do(func() {
		lease.stopActionWatch()
		if lease.Context.Err() != nil {
			lease.workspace.cancel()
		}
		lease.stopSessionWatch()
		lease.cancel()
		lease.workspace.lock.Lock()
		lease.workspace.busy = false
		lease.workspace.lock.Unlock()
	})
}

// Close retires the workspace, calling stopWorker before closing or
// deleting directories. The callback must confirm that the compiler has
// stopped, or return an error. A failed stop retains the workspace and
// may be retried; it never permits further Prepare calls. Close rejects
// active leases. The caller must supply an independent cleanup context.
func (workspace *PersistentWorkerWorkspace) Close(ctx context.Context, stopWorker func(context.Context) error) error {
	workspace.lock.Lock()
	if workspace.closed {
		err := workspace.closeError
		workspace.lock.Unlock()
		return err
	}
	if workspace.busy || workspace.closing {
		workspace.lock.Unlock()
		return status.Error(codes.FailedPrecondition, "Persistent worker workspace is still in use")
	}
	if stopWorker == nil {
		workspace.lock.Unlock()
		return status.Error(codes.InvalidArgument, "Missing persistent worker stop callback")
	}
	workspace.retiring = true
	workspace.closing = true
	workspace.lock.Unlock()

	err := stopWorker(ctx)
	if err == nil {
		workspace.cancel()
		inputRootCloseError := workspace.inputRoot.Close()
		err = errors.Join(inputRootCloseError, workspace.buildDirectory.Close())
		workspace.lock.Lock()
		workspace.closed = true
		workspace.closeError = err
	} else {
		workspace.lock.Lock()
	}
	workspace.closing = false
	workspace.lock.Unlock()
	return err
}
