package runner

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

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
	logFileResolver := buildDirectoryPathResolver{
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

// CommandCreator is a type alias for a function that creates the
// exec.Cmd in localRunner.Run(). It may use different strategies for
// resolving the paths of argv[0] and the working directory, depending
// on whether the action needs to be run in a chroot() or not.
type CommandCreator func(ctx context.Context, arguments []string, inputRootDirectory *path.Builder, workingDirectory, pathVariable string) (*exec.Cmd, error)

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

	cmd, err := r.commandCreator(ctx, request.Arguments, inputRootDirectory, request.WorkingDirectory, request.EnvironmentVariables["PATH"])
	if err != nil {
		return nil, err
	}

	// Set the environment variables.
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
	pathResolver := buildDirectoryPathResolver{
		stack: util.NewNonEmptyStack(filesystem.NopDirectoryCloser(r.buildDirectory)),
	}
	defer pathResolver.closeAll()
	if err := path.Resolve(request.Path, path.NewRelativeScopeWalker(&pathResolver)); err != nil {
		return nil, util.StatusWrapfWithCode(err, codes.Internal, "Failed to resolve path %#v in build directory", request.Path)
	}
	if name := pathResolver.TerminalName; name != nil {
		if _, err := pathResolver.stack.Peek().Lstat(*name); err != nil {
			return nil, util.StatusWrapfWithCode(err, codes.Internal, "Failed to check existence of path %#v in build directory", request.Path)
		}
	}

	return &emptypb.Empty{}, nil
}

// getExecutablePath returns the path of an executable within a given
// search path that is part of the PATH environment variable.
func getExecutablePath(baseDirectory *path.Builder, searchPathStr, argv0 string) (string, error) {
	searchPath, scopeWalker := baseDirectory.Join(path.VoidScopeWalker)
	if err := path.Resolve(searchPathStr, scopeWalker); err != nil {
		return "", err
	}
	executablePath, scopeWalker := searchPath.Join(path.VoidScopeWalker)
	if err := path.Resolve(argv0, scopeWalker); err != nil {
		return "", err
	}
	return executablePath.String(), nil
}

// lookupExecutable returns the path of an executable, taking the PATH
// environment variable into account.
// This operates on platform native paths, or unix-style slash paths.
// The latter are customarily sent by Bazel in multiplatform builds.
func lookupExecutable(workingDirectory *path.Builder, pathVariable, argv0 string) (string, error) {
	// TODO(nils): This smells bad, we should canonicalize the paths somewhere
	// In general the tests are setup to use correct platform-specific separators,
	// and this failing unit test could have been fixed by updating the test.
	// But, to support cross-compilation from a Linux host we need it to work,
	// and the general point about canonicalization stands.
	// Should the worker make sure to convert all paths before calling the runner?
	if strings.ContainsRune(argv0, os.PathSeparator) || strings.ContainsRune(argv0, '/') {
		// No PATH processing needs to be performed.
		return argv0, nil
	}

	// Executable path does not contain any slashes. Perform PATH
	// lookups.
	//
	// We cannot use exec.LookPath() directly, as that function
	// disregards the working directory of the action. It also uses
	// the PATH environment variable of the current process, as
	// opposed to respecting the value that is provided as part of
	// the action. Do call into this function to validate the
	// existence of the executable.
	for _, searchPathStr := range filepath.SplitList(pathVariable) {
		executablePathAbs, err := getExecutablePath(workingDirectory, searchPathStr, argv0)
		if err != nil {
			return "", util.StatusWrapf(err, "Failed to resolve executable %#v in search path %#v", argv0, searchPathStr)
		}
		if _, err := exec.LookPath(executablePathAbs); err == nil {
			// Regular compiled executables will receive the
			// argv[0] that we provide, but scripts starting
			// with '#!' will receive the literal executable
			// path.
			//
			// Most shells seem to guarantee that if argv[0]
			// is relative, the executable path is relative
			// as well. Prevent these scripts from breaking
			// by recomputing the executable path once more,
			// but relative.
			executablePathRel, err := getExecutablePath(&path.EmptyBuilder, searchPathStr, argv0)
			if err != nil {
				return "", util.StatusWrapf(err, "Failed to resolve executable %#v in search path %#v", argv0, searchPathStr)
			}
			return executablePathRel, nil
		}
	}
	return "", status.Errorf(codes.InvalidArgument, "Cannot find executable %#v in search paths %#v", argv0, pathVariable)
}
