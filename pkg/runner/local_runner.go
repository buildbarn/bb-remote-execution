package runner

import (
	"context"
	"os/exec"
	"path"
	"path/filepath"
	"strings"
	"syscall"

	"github.com/buildbarn/bb-remote-execution/pkg/proto/resourceusage"
	"github.com/buildbarn/bb-remote-execution/pkg/proto/runner"
	"github.com/buildbarn/bb-storage/pkg/filesystem"
	"github.com/buildbarn/bb-storage/pkg/util"
	"github.com/golang/protobuf/ptypes"
	"github.com/golang/protobuf/ptypes/any"
	"github.com/golang/protobuf/ptypes/duration"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type localRunner struct {
	buildDirectory               filesystem.Directory
	buildDirectoryPath           string
	setTmpdirEnvironmentVariable bool
	chrootIntoInputRoot          bool
}

// NewLocalRunner returns a Runner capable of running commands on the
// local system directly.
func NewLocalRunner(buildDirectory filesystem.Directory, buildDirectoryPath string, setTmpdirEnvironmentVariable bool, chrootIntoInputRoot bool) Runner {
	return &localRunner{
		buildDirectory:               buildDirectory,
		buildDirectoryPath:           buildDirectoryPath,
		setTmpdirEnvironmentVariable: setTmpdirEnvironmentVariable,
		chrootIntoInputRoot:          chrootIntoInputRoot,
	}
}

func (r *localRunner) openLog(logPath string) (filesystem.FileAppender, error) {
	components := strings.FieldsFunc(logPath, func(r rune) bool { return r == '/' })
	if len(components) < 1 {
		return nil, status.Error(codes.InvalidArgument, "Insufficient pathname components in filename")
	}

	// Traverse to directory where log should be created.
	d := filesystem.NopDirectoryCloser(r.buildDirectory)
	for n, component := range components[:len(components)-1] {
		d2, err := d.EnterDirectory(component)
		d.Close()
		if err != nil {
			return nil, util.StatusWrapf(err, "Failed to enter directory %#v", path.Join(components[:n+1]...))
		}
		d = d2
	}

	// Create log file within.
	f, err := d.OpenAppend(components[len(components)-1], filesystem.CreateExcl(0666))
	d.Close()
	return f, err
}

func convertTimeval(t syscall.Timeval) *duration.Duration {
	return &duration.Duration{
		Seconds: t.Sec,
		Nanos:   int32(t.Usec) * 1000,
	}
}

func (r *localRunner) Run(ctx context.Context, request *runner.RunRequest) (*runner.RunResponse, error) {
	if len(request.Arguments) < 1 {
		return nil, status.Error(codes.InvalidArgument, "Insufficient number of command arguments")
	}
	var cmd *exec.Cmd
	if r.chrootIntoInputRoot {
		// The addition of /usr/bin/env is necessary as the PATH resolution
		// will take place prior to the chroot, so the executable may not be
		// found by exec.LookPath() inside exec.CommandContext() and may
		// cause cmd.Start() to fail when it shouldn't.
		// https://github.com/golang/go/issues/39341
		envPrependedArguments := []string{"/usr/bin/env", "--"}
		envPrependedArguments = append(envPrependedArguments, request.Arguments...)
		cmd = exec.CommandContext(ctx, envPrependedArguments[0], envPrependedArguments[1:]...)
		// TODO: Convert WorkingDirectory and TemporaryDirectory to use
		// platform specific path delimiters.
		cmd.Dir = filepath.Join("/", request.WorkingDirectory)
	} else {
		cmd = exec.CommandContext(ctx, request.Arguments[0], request.Arguments[1:]...)
		cmd.Dir = filepath.Join(r.buildDirectoryPath, request.InputRootDirectory, request.WorkingDirectory)
	}
	cmd.Env = make([]string, 0, len(request.EnvironmentVariables)+1)
	if r.setTmpdirEnvironmentVariable && request.TemporaryDirectory != "" {
		cmd.Env = append(cmd.Env, "TMPDIR="+filepath.Join(r.buildDirectoryPath, request.TemporaryDirectory))
	}
	for name, value := range request.EnvironmentVariables {
		cmd.Env = append(cmd.Env, name+"="+value)
	}

	// Open output files for logging.
	stdout, err := r.openLog(request.StdoutPath)
	if err != nil {
		return nil, util.StatusWrap(err, "Failed to open stdout")
	}
	cmd.Stdout = stdout

	stderr, err := r.openLog(request.StderrPath)
	if err != nil {
		stdout.Close()
		return nil, util.StatusWrap(err, "Failed to open stderr")
	}
	cmd.Stderr = stderr
	if r.chrootIntoInputRoot {
		cmd.SysProcAttr = &syscall.SysProcAttr{
			Chroot:  path.Join(r.buildDirectoryPath, request.InputRootDirectory),
			Setpgid: true,
		}
	} else {
		cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	}
	// Start the subprocess. We can already close the output files
	// while the process is running.
	err = cmd.Start()
	stdout.Close()
	stderr.Close()
	if err != nil {
		return nil, util.StatusWrap(err, "Failed to start process")
	}

	// Wait for execution to complete. Permit non-zero exit codes.
	if err := cmd.Wait(); err != nil {
		if _, ok := err.(*exec.ExitError); !ok {
			return nil, err
		}
	}

	// Give subprocesses spawned by the action a chance to cleanup.
	// Ignore errors since the processes group may have already been
	// reaped by the init system.
	syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)

	// Attach rusage information to the response.
	rusage := cmd.ProcessState.SysUsage().(*syscall.Rusage)
	posixResourceUsage, err := ptypes.MarshalAny(&resourceusage.POSIXResourceUsage{
		UserTime:                   convertTimeval(rusage.Utime),
		SystemTime:                 convertTimeval(rusage.Stime),
		MaximumResidentSetSize:     rusage.Maxrss * maximumResidentSetSizeUnit,
		PageReclaims:               rusage.Minflt,
		PageFaults:                 rusage.Majflt,
		Swaps:                      rusage.Nswap,
		BlockInputOperations:       rusage.Inblock,
		BlockOutputOperations:      rusage.Oublock,
		MessagesSent:               rusage.Msgsnd,
		MessagesReceived:           rusage.Msgrcv,
		SignalsReceived:            rusage.Nsignals,
		VoluntaryContextSwitches:   rusage.Nvcsw,
		InvoluntaryContextSwitches: rusage.Nivcsw,
	})
	if err != nil {
		return nil, util.StatusWrap(err, "Failed to marshal POSIX resource usage")
	}
	return &runner.RunResponse{
		ExitCode:      int32(cmd.ProcessState.ExitCode()),
		ResourceUsage: []*any.Any{posixResourceUsage},
	}, nil
}
