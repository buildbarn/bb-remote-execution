// +build windows

package runner

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"

	"github.com/buildbarn/bb-remote-execution/pkg/proto/resourceusage"
	"github.com/buildbarn/bb-remote-execution/pkg/proto/runner"
	"github.com/buildbarn/bb-storage/pkg/filesystem/path"
	"github.com/buildbarn/bb-storage/pkg/util"

	"golang.org/x/sys/windows"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/durationpb"
)

func (r *localRunner) run(ctx context.Context, request *runner.RunRequest) (*runner.RunResponse, error) {
	if len(request.Arguments) < 1 {
		return nil, status.Error(codes.InvalidArgument, "Insufficient number of command arguments")
	}

	inputRootDirectory, scopeWalker := r.buildDirectoryPath.Join(path.VoidScopeWalker)
	if err := path.Resolve(request.InputRootDirectory, scopeWalker); err != nil {
		return nil, util.StatusWrap(err, "Failed to resolve input root directory")
	}

	var cmd *exec.Cmd
	var workingDirectoryBase *path.Builder
	if r.chrootIntoInputRoot {
		return nil, status.Error(codes.InvalidArgument, "Chroot not supported on Windows")
	} else {
		cmd = exec.CommandContext(ctx, request.Arguments[0], request.Arguments[1:]...)
		workingDirectoryBase = inputRootDirectory
	}

	// Set the environment variable.
	cmd.Env = make([]string, 0, len(request.EnvironmentVariables)+2)
	if r.setTmpdirEnvironmentVariable && request.TemporaryDirectory != "" {
		temporaryDirectory, scopeWalker := r.buildDirectoryPath.Join(path.VoidScopeWalker)
		if err := path.Resolve(request.TemporaryDirectory, scopeWalker); err != nil {
			return nil, util.StatusWrap(err, "Failed to resolve temporary directory")
		}
		cmd.Env = append(cmd.Env, "TMP="+filepath.FromSlash(temporaryDirectory.String()))
		cmd.Env = append(cmd.Env, "TEMP="+filepath.FromSlash(temporaryDirectory.String()))
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
		if errors.Is(err, exec.ErrNotFound) || errors.Is(err, os.ErrPermission) || errors.Is(err, os.ErrNotExist) || errors.Is(err, windows.ERROR_BAD_EXE_FORMAT) {
			code = codes.InvalidArgument
		}
		return nil, util.StatusWrapWithCode(err, code, "Failed to start process")
	}

	// Wait for execution to complete. Permit non-zero exit codes.
	if err := cmd.Wait(); err != nil {
		if _, ok := err.(*exec.ExitError); !ok {
			return nil, err
		}
	}

	// Attach rusage information to the response (to the extent possible on Windows).
	processState := cmd.ProcessState
	posixResourceUsage, err := anypb.New(&resourceusage.POSIXResourceUsage{
		UserTime:   durationpb.New(processState.SystemTime()),
		SystemTime: durationpb.New(processState.UserTime()),
	})
	if err != nil {
		return nil, util.StatusWrap(err, "Failed to marshal POSIX resource usage")
	}
	return &runner.RunResponse{
		ExitCode:      int32(cmd.ProcessState.ExitCode()),
		ResourceUsage: []*anypb.Any{posixResourceUsage},
	}, nil
}
