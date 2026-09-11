package builder

import (
	"context"
	"errors"
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
// actions. WorkingDirectory must be canonical and input-root-relative.
func NewPersistentWorkerWorkspace(ctx context.Context, creator BuildDirectoryCreator, filePool pool.FilePool, workingDirectory string, characterDevices map[path.Component]filesystem.DeviceNumber) (*PersistentWorkerWorkspace, error) {
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
// root, preserving the compiler's working directory identity. A lease
// remains held until Release, including while outputs are uploaded.
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
	var preservedPath []path.Component
	if workspace.initialized {
		preservedPath = workspace.workingDirectory
	}
	if err := cleanPersistentWorkerDirectory(workspace.context, workspace.inputRoot, preservedPath); err != nil {
		return util.StatusWrap(err, "Failed to clean persistent worker input root")
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
	cleanError := cleanPersistentWorkerDirectory(workspace.context, serverLogs, nil)
	closeError := serverLogs.Close()
	if err := errors.Join(cleanError, closeError); err != nil {
		return util.StatusWrap(err, "Failed to clean per-action server logs")
	}
	if err := populateInputRoot(workspace.context, workspace.inputRoot, &workspace.errorLogger, inputRootDigest.GetDigestFunction(), inputRootDigest.GetProto(), nil, workspace.characterDevices); err != nil {
		return err
	}
	if err := createPersistentWorkerWorkingDirectory(workspace.inputRoot, workspace.workingDirectory); err != nil {
		return err
	}
	return outputHierarchy.CreateParentDirectories(workspace.inputRoot)
}

func cleanPersistentWorkerDirectory(ctx context.Context, directory BuildDirectory, preservedPath []path.Component) error {
	children, err := directory.ReadDir()
	if err != nil {
		return err
	}
	foundPreservedDirectory := len(preservedPath) == 0
	for _, child := range children {
		if err := ctx.Err(); err != nil {
			return status.FromContextError(err).Err()
		}
		if len(preservedPath) > 0 && child.Name() == preservedPath[0] {
			if child.Type() != filesystem.FileTypeDirectory {
				return status.Error(codes.FailedPrecondition, "Persistent worker working directory was replaced")
			}
			childDirectory, err := directory.EnterBuildDirectory(child.Name())
			if err != nil {
				return err
			}
			cleanError := cleanPersistentWorkerDirectory(ctx, childDirectory, preservedPath[1:])
			closeError := childDirectory.Close()
			if err := errors.Join(cleanError, closeError); err != nil {
				return err
			}
			foundPreservedDirectory = true
		} else if err := directory.RemoveAll(child.Name()); err != nil {
			return err
		}
	}
	if !foundPreservedDirectory {
		return status.Error(codes.FailedPrecondition, "Persistent worker working directory was removed")
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
