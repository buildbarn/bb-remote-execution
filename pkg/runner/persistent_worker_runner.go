package runner

import (
	"context"

	"github.com/buildbarn/bb-remote-execution/pkg/proto/bazelworker"
	runner_pb "github.com/buildbarn/bb-remote-execution/pkg/proto/runner"
	"github.com/buildbarn/bb-storage/pkg/filesystem"
	"github.com/buildbarn/bb-storage/pkg/filesystem/path"
	"github.com/buildbarn/bb-storage/pkg/util"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
)

type persistentWorkerRunner struct {
	base               runner_pb.RunnerServer
	buildDirectory     filesystem.Directory
	buildDirectoryPath *path.Builder
	pool               *PersistentWorkerPool
}

// NewPersistentWorkerRunner creates a decorator for Runner that causes
// build actions that carry persistent worker options to be executed by
// a long running tool process, using the Bazel persistent worker
// protocol. All other build actions are forwarded to the underlying
// Runner unmodified.
//
// Bazel only emits build actions that can be executed this way if the
// --experimental_remote_mark_tool_inputs command line flag is provided.
func NewPersistentWorkerRunner(base runner_pb.RunnerServer, buildDirectory filesystem.Directory, buildDirectoryPath *path.Builder, pool *PersistentWorkerPool) runner_pb.RunnerServer {
	return &persistentWorkerRunner{
		base:               base,
		buildDirectory:     buildDirectory,
		buildDirectoryPath: buildDirectoryPath,
		pool:               pool,
	}
}

func (r *persistentWorkerRunner) Run(ctx context.Context, request *runner_pb.RunRequest) (*runner_pb.RunResponse, error) {
	persistentWorker := request.PersistentWorker
	if persistentWorker == nil {
		return r.base.Run(ctx, request)
	}
	if persistentWorker.Key == "" {
		return nil, status.Error(codes.InvalidArgument, "Persistent worker options do not contain a key")
	}

	// Split the command line arguments into the ones that are used
	// to launch the tool, and the ones that describe the work that
	// needs to be performed.
	workerArguments, flagFileArguments, err := SplitPersistentWorkerArguments(request.Arguments)
	if err != nil {
		return nil, err
	}

	inputRootPath, scopeWalker := r.buildDirectoryPath.Join(path.VoidScopeWalker)
	if err := path.Resolve(path.UNIXFormat.NewParser(request.InputRootDirectory), scopeWalker); err != nil {
		return nil, util.StatusWrap(err, "Failed to resolve input root directory")
	}
	inputRootDirectory, err := enterDirectoryInDirectory(r.buildDirectory, request.InputRootDirectory)
	if err != nil {
		return nil, util.StatusWrapf(err, "Failed to enter input root directory %#v", request.InputRootDirectory)
	}
	defer inputRootDirectory.Close()

	workingDirectory, err := parseWorkingDirectoryComponents(request.WorkingDirectory)
	if err != nil {
		return nil, util.StatusWrapf(err, "Failed to resolve working directory %#v", request.WorkingDirectory)
	}

	workRequestArguments, err := ExpandFlagFileArguments(inputRootDirectory, workingDirectory, flagFileArguments)
	if err != nil {
		return nil, err
	}

	// Obtain a worker process. Its execution root is repopulated to
	// provide a view of the input root of this build action, as the
	// process may previously have been used to execute a build
	// action having a different input root.
	key := newPersistentWorkerKey(
		persistentWorker.Key,
		persistentWorker.Protocol,
		request.WorkingDirectory,
		workerArguments,
		request.EnvironmentVariables,
	)
	w, err := r.pool.acquire(key, persistentWorker.Protocol)
	if err != nil {
		return nil, err
	}
	healthy := false
	defer func() {
		r.pool.release(w, healthy)
	}()

	if err := w.prepareExecRoot(inputRootDirectory, inputRootPath, workingDirectory); err != nil {
		return nil, err
	}
	if err := w.ensureStarted(workerArguments, request.EnvironmentVariables, path.UNIXFormat.NewParser(request.WorkingDirectory)); err != nil {
		return nil, err
	}

	response, err := w.exchange(ctx, &bazelworker.WorkRequest{
		Arguments: workRequestArguments,
		Inputs:    persistentWorker.Inputs,
		// Only a single work request is in flight at a time,
		// meaning the worker process is used in singleplex mode.
		// The Bazel persistent worker protocol requires the
		// request ID to be zero in that case.
		RequestId: 0,
	})
	if err != nil {
		return nil, err
	}
	if response.RequestId != 0 {
		return nil, status.Errorf(codes.Internal, "Persistent worker returned a work response for request %d, while request 0 was sent", response.RequestId)
	}
	if response.WasCancelled {
		return nil, status.Error(codes.Internal, "Persistent worker returned a cancelation work response, while no cancelation was requested")
	}
	healthy = true

	// Tools running as a persistent worker are required to report
	// all of the output of a single build action through the work
	// response, as opposed to writing it to their own standard
	// output or error. Store it in the file that bb_worker uses to
	// capture standard error, which is what Bazel does as well.
	if err := writeLogFile(r.buildDirectory, request.StdoutPath, ""); err != nil {
		return nil, util.StatusWrapf(err, "Failed to create stdout path %#v", request.StdoutPath)
	}
	if err := writeLogFile(r.buildDirectory, request.StderrPath, response.Output); err != nil {
		return nil, util.StatusWrapf(err, "Failed to create stderr path %#v", request.StderrPath)
	}

	return &runner_pb.RunResponse{
		ExitCode: int64(response.ExitCode),
	}, nil
}

func (r *persistentWorkerRunner) CheckReadiness(ctx context.Context, request *runner_pb.CheckReadinessRequest) (*emptypb.Empty, error) {
	return r.base.CheckReadiness(ctx, request)
}

// enterDirectoryInDirectory returns a handle to a directory, using a
// path that is resolved relative to another directory. Symbolic links
// and paths escaping the directory are not followed.
func enterDirectoryInDirectory(base filesystem.Directory, directoryPath string) (filesystem.DirectoryCloser, error) {
	resolver := buildDirectoryPathResolver{
		stack: util.NewNonEmptyStack(filesystem.NopDirectoryCloser(base)),
	}
	defer resolver.closeAll()
	if err := path.Resolve(path.UNIXFormat.NewParser(directoryPath), path.NewRelativeScopeWalker(&resolver)); err != nil {
		return nil, err
	}
	if resolver.TerminalName == nil {
		return nil, status.Error(codes.InvalidArgument, "Path does not refer to a directory inside the build directory")
	}
	return resolver.stack.Peek().EnterDirectory(*resolver.TerminalName)
}

// componentCollectingComponentWalker is an implementation of
// path.ComponentWalker that converts a relative pathname string to a
// list of pathname components, rejecting any paths that escape the
// directory in which resolution started.
type componentCollectingComponentWalker struct {
	components []path.Component
}

func (cw *componentCollectingComponentWalker) OnDirectory(name path.Component) (path.GotDirectoryOrSymlink, error) {
	cw.components = append(cw.components, name)
	return path.GotDirectory{
		Child:        cw,
		IsReversible: false,
	}, nil
}

func (cw *componentCollectingComponentWalker) OnTerminal(name path.Component) (*path.GotSymlink, error) {
	cw.components = append(cw.components, name)
	return nil, nil
}

func (componentCollectingComponentWalker) OnUp() (path.ComponentWalker, error) {
	return nil, status.Error(codes.InvalidArgument, "Path resolves to a location outside the input root directory")
}

func parseWorkingDirectoryComponents(workingDirectory string) ([]path.Component, error) {
	var componentWalker componentCollectingComponentWalker
	if err := path.Resolve(path.UNIXFormat.NewParser(workingDirectory), path.NewRelativeScopeWalker(&componentWalker)); err != nil {
		return nil, err
	}
	return componentWalker.components, nil
}

// writeLogFile creates one of the log files that bb_worker attaches to
// the REv2 ExecuteResponse, and fills it with the provided contents.
func writeLogFile(buildDirectory filesystem.Directory, logPath, contents string) error {
	f, err := openLogFile(buildDirectory, logPath)
	if err != nil {
		return err
	}
	defer f.Close()
	if contents != "" {
		if _, err := f.Write([]byte(contents)); err != nil {
			return err
		}
	}
	return nil
}
