package runner

import (
	"context"
	"errors"
	"os/exec"
	"path/filepath"

	"github.com/buildbarn/bb-remote-execution/pkg/proto/runner"
	"github.com/buildbarn/bb-storage/pkg/filesystem"
	"github.com/buildbarn/bb-storage/pkg/filesystem/path"
	"github.com/buildbarn/bb-storage/pkg/util"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/emptypb"
)

// buildDirectoryPathResolver is an implementation of
// path.ComponentWalker that is used by localRunner.Run() to resolve
// paths inside the build directory, such as stdout and stderr log files
// so that they may be opened.
//
// TODO: This code seems fairly generic. Should move it to the
// filesystem package?
type buildDirectoryPathResolver struct {
	path.TerminalNameTrackingComponentWalker
	stack util.NonEmptyStack[filesystem.DirectoryCloser]
}

func (r *buildDirectoryPathResolver) OnDirectory(name path.Component) (path.GotDirectoryOrSymlink, error) {
	child, err := r.stack.Peek().EnterDirectory(name)
	if err != nil {
		return nil, err
	}
	r.stack.Push(child)
	return path.GotDirectory{
		Child:        r,
		IsReversible: true,
	}, nil
}

func (r *buildDirectoryPathResolver) OnUp() (path.ComponentWalker, error) {
	if d, ok := r.stack.PopSingle(); ok {
		if err := d.Close(); err != nil {
			r.stack.Push(d)
			return nil, err
		}
		return r, nil
	}
	return nil, status.Error(codes.InvalidArgument, "Path resolves to a location outside the build directory")
}

func (r *buildDirectoryPathResolver) closeAll() {
	for {
		d, ok := r.stack.PopSingle()
		if !ok {
			break
		}
		d.Close()
	}
}

type localRunner struct {
	buildDirectory               filesystem.Directory
	buildDirectoryPath           *path.Builder
	commandCreator               CommandCreator
	setTmpdirEnvironmentVariable bool
}

func (r *localRunner) openLog(logPath string) (filesystem.FileAppender, error) {
	logPathParser, err := path.NewUNIXParser(logPath)
	if err != nil {
		return nil, err
	}
	logFileResolver := buildDirectoryPathResolver{
		stack: util.NewNonEmptyStack(filesystem.NopDirectoryCloser(r.buildDirectory)),
	}
	defer logFileResolver.closeAll()
	if err := path.Resolve(logPathParser, path.NewRelativeScopeWalker(&logFileResolver)); err != nil {
		return nil, err
	}
	if logFileResolver.TerminalName == nil {
		return nil, status.Error(codes.InvalidArgument, "Path resolves to a directory")
	}
	return logFileResolver.stack.Peek().OpenAppend(*logFileResolver.TerminalName, filesystem.CreateExcl(0o666))
}

// CommandCreator is a type alias for a function that creates the
// exec.Cmd in localRunner.Run(). It may use different strategies for
// resolving the paths of argv[0] and the working directory, depending
// on whether the action needs to be run in a chroot() or not.
type CommandCreator func(ctx context.Context, arguments []string, inputRootDirectory *path.Builder, workingDirectoryParser path.Parser, pathVariable string) (*exec.Cmd, error)

// NewLocalRunner returns a Runner capable of running commands on the
// local system directly.
func NewLocalRunner(buildDirectory filesystem.Directory, buildDirectoryPath *path.Builder, commandCreator CommandCreator, setTmpdirEnvironmentVariable bool) runner.RunnerServer {
	return &localRunner{
		buildDirectory:               buildDirectory,
		buildDirectoryPath:           buildDirectoryPath,
		commandCreator:               commandCreator,
		setTmpdirEnvironmentVariable: setTmpdirEnvironmentVariable,
	}
}

func (r *localRunner) Run(ctx context.Context, request *runner.RunRequest) (*runner.RunResponse, error) {
	if len(request.Arguments) < 1 {
		return nil, status.Error(codes.InvalidArgument, "Insufficient number of command arguments")
	}

	inputRootDirectoryParser, err := path.NewUNIXParser(request.InputRootDirectory)
	if err != nil {
		return nil, util.StatusWrap(err, "Invalid input root directory")
	}
	inputRootDirectory, scopeWalker := r.buildDirectoryPath.Join(path.VoidScopeWalker)
	if err := path.Resolve(inputRootDirectoryParser, scopeWalker); err != nil {
		return nil, util.StatusWrap(err, "Failed to resolve input root directory")
	}

	workingDirectoryParser, err := path.NewUNIXParser(request.WorkingDirectory)
	if err != nil {
		return nil, util.StatusWrap(err, "Invalid working directory")
	}

	cmd, err := r.commandCreator(ctx, request.Arguments, inputRootDirectory, workingDirectoryParser, request.EnvironmentVariables["PATH"])
	if err != nil {
		return nil, err
	}

	// Set the environment variables.
	cmd.Env = make([]string, 0, len(request.EnvironmentVariables)+1)
	if r.setTmpdirEnvironmentVariable && request.TemporaryDirectory != "" {
		temporaryDirectoryParser, err := path.NewUNIXParser(request.TemporaryDirectory)
		if err != nil {
			return nil, util.StatusWrap(err, "Invalid temporary directory")
		}
		temporaryDirectory, scopeWalker := r.buildDirectoryPath.Join(path.VoidScopeWalker)
		if err := path.Resolve(temporaryDirectoryParser, scopeWalker); err != nil {
			return nil, util.StatusWrap(err, "Failed to resolve temporary directory")
		}
		for _, prefix := range temporaryDirectoryEnvironmentVariablePrefixes {
			cmd.Env = append(cmd.Env, prefix+filepath.FromSlash(temporaryDirectory.String()))
		}
	}
	for name, value := range request.EnvironmentVariables {
		cmd.Env = append(cmd.Env, name+"="+value)
	}

	// Open output files for logging.
	stdout, err := r.openLog(request.StdoutPath)
	if err != nil {
		return nil, util.StatusWrapf(err, "Failed to open stdout path %q", request.StdoutPath)
	}
	cmd.Stdout = stdout

	stderr, err := r.openLog(request.StderrPath)
	if err != nil {
		stdout.Close()
		return nil, util.StatusWrapf(err, "Failed to open stderr path %q", request.StderrPath)
	}
	cmd.Stderr = stderr

	// Start the subprocess. We can already close the output files
	// while the process is running.
	err = cmd.Start()
	stdout.Close()
	stderr.Close()
	if err != nil {
		code := codes.Internal
		for _, invalidArgumentErr := range invalidArgumentErrs {
			if errors.Is(err, invalidArgumentErr) {
				code = codes.InvalidArgument
				break
			}
		}
		return nil, util.StatusWrapWithCode(err, code, "Failed to start process")
	}

	// Wait for execution to complete. Permit non-zero exit codes.
	if err := cmd.Wait(); err != nil {
		if _, ok := err.(*exec.ExitError); !ok {
			return nil, err
		}
	}

	// Attach rusage information to the response.
	posixResourceUsage, err := anypb.New(getPOSIXResourceUsage(cmd))
	if err != nil {
		return nil, util.StatusWrap(err, "Failed to marshal POSIX resource usage")
	}
	return &runner.RunResponse{
		ExitCode:      int32(cmd.ProcessState.ExitCode()),
		ResourceUsage: []*anypb.Any{posixResourceUsage},
	}, nil
}

func (r *localRunner) CheckReadiness(ctx context.Context, request *runner.CheckReadinessRequest) (*emptypb.Empty, error) {
	// Check that the path that the worker provided as part of the
	// request exists in the build directory. This ensures that
	// trivial misconfigurations of the build directory don't lead
	// to repeated build failures.
	pathParser, err := path.NewUNIXParser(request.Path)
	if err != nil {
		return nil, util.StatusWrapfWithCode(err, codes.Internal, "Invalid path %#v", request.Path)
	}
	pathResolver := buildDirectoryPathResolver{
		stack: util.NewNonEmptyStack(filesystem.NopDirectoryCloser(r.buildDirectory)),
	}
	defer pathResolver.closeAll()
	if err := path.Resolve(pathParser, path.NewRelativeScopeWalker(&pathResolver)); err != nil {
		return nil, util.StatusWrapfWithCode(err, codes.Internal, "Failed to resolve path %#v in build directory", request.Path)
	}
	if name := pathResolver.TerminalName; name != nil {
		if _, err := pathResolver.stack.Peek().Lstat(*name); err != nil {
			return nil, util.StatusWrapfWithCode(err, codes.Internal, "Failed to check existence of path %#v in build directory", request.Path)
		}
	}

	return &emptypb.Empty{}, nil
}
