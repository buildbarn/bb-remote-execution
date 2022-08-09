package runner

import (
	"context"
	"errors"
	"os/exec"
	"path/filepath"
	"syscall"

	"github.com/buildbarn/bb-remote-execution/pkg/proto/runner"
	"github.com/buildbarn/bb-storage/pkg/filesystem"
	"github.com/buildbarn/bb-storage/pkg/filesystem/path"
	"github.com/buildbarn/bb-storage/pkg/util"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/emptypb"
)

// logFileResolver is an implementation of path.ComponentWalker that is
// used by localRunner.Run() to traverse to the directory of stdout and
// stderr log files, so that they may be opened.
//
// TODO: This code seems fairly generic. Should move it to the
// filesystem package?
type logFileResolver struct {
	path.TerminalNameTrackingComponentWalker
	stack util.NonEmptyStack[filesystem.DirectoryCloser]
}

func (r *logFileResolver) OnDirectory(name path.Component) (path.GotDirectoryOrSymlink, error) {
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

func (r *logFileResolver) OnUp() (path.ComponentWalker, error) {
	if d, ok := r.stack.PopSingle(); ok {
		if err := d.Close(); err != nil {
			r.stack.Push(d)
			return nil, err
		}
		return r, nil
	}
	return nil, status.Error(codes.InvalidArgument, "Path resolves to a location outside the build directory")
}

func (r *logFileResolver) closeAll() {
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
	logFileResolver := logFileResolver{
		stack: util.NewNonEmptyStack(filesystem.NopDirectoryCloser(r.buildDirectory)),
	}
	defer logFileResolver.closeAll()
	if err := path.Resolve(logPath, path.NewRelativeScopeWalker(&logFileResolver)); err != nil {
		return nil, err
	}
	if logFileResolver.TerminalName == nil {
		return nil, status.Error(codes.InvalidArgument, "Path resolves to a directory")
	}
	return logFileResolver.stack.Peek().OpenAppend(*logFileResolver.TerminalName, filesystem.CreateExcl(0o666))
}

// CommandCreator is a type alias for a function that creates the cmd and
// the workingDirectoryBase in localRunner.Run(). There is a shared
// implementation for cases where chroot is not required and platform-specific
// implementations for cases where it is.
type CommandCreator func(ctx context.Context, arguments []string, inputRootDirectory *path.Builder) (*exec.Cmd, *path.Builder)

// NewPlainCommandCreator returns a CommandCreator for cases where we don't
// need to chroot into the input root directory.
func NewPlainCommandCreator(sysProcAttr *syscall.SysProcAttr) CommandCreator {
	return func(ctx context.Context, arguments []string, inputRootDirectory *path.Builder) (*exec.Cmd, *path.Builder) {
		cmd := exec.CommandContext(ctx, arguments[0], arguments[1:]...)
		cmd.SysProcAttr = sysProcAttr
		return cmd, inputRootDirectory
	}
}

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

	inputRootDirectory, scopeWalker := r.buildDirectoryPath.Join(path.VoidScopeWalker)
	if err := path.Resolve(request.InputRootDirectory, scopeWalker); err != nil {
		return nil, util.StatusWrap(err, "Failed to resolve input root directory")
	}

	cmd, workingDirectoryBase := r.commandCreator(ctx, request.Arguments, inputRootDirectory)

	// Set the environment variable.
	cmd.Env = make([]string, 0, len(request.EnvironmentVariables)+1)
	if r.setTmpdirEnvironmentVariable && request.TemporaryDirectory != "" {
		temporaryDirectory, scopeWalker := r.buildDirectoryPath.Join(path.VoidScopeWalker)
		if err := path.Resolve(request.TemporaryDirectory, scopeWalker); err != nil {
			return nil, util.StatusWrap(err, "Failed to resolve temporary directory")
		}
		for _, prefix := range temporaryDirectoryEnvironmentVariablePrefixes {
			cmd.Env = append(cmd.Env, prefix+filepath.FromSlash(temporaryDirectory.String()))
		}
	}
	for name, value := range request.EnvironmentVariables {
		cmd.Env = append(cmd.Env, name+"="+value)
	}

	// Set the working directory.
	workingDirectory, scopeWalker := workingDirectoryBase.Join(path.VoidScopeWalker)
	if err := path.Resolve(request.WorkingDirectory, scopeWalker); err != nil {
		return nil, util.StatusWrap(err, "Failed to resolve working directory")
	}
	cmd.Dir = filepath.FromSlash(workingDirectory.String())

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

func (r *localRunner) CheckReadiness(ctx context.Context, request *emptypb.Empty) (*emptypb.Empty, error) {
	return &emptypb.Empty{}, nil
}
