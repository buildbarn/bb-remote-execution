package runner_test

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/buildbarn/bb-remote-execution/internal/mock"
	"github.com/buildbarn/bb-remote-execution/pkg/proto/resourceusage"
	runner_pb "github.com/buildbarn/bb-remote-execution/pkg/proto/runner"
	"github.com/buildbarn/bb-remote-execution/pkg/runner"
	"github.com/buildbarn/bb-storage/pkg/filesystem"
	"github.com/buildbarn/bb-storage/pkg/filesystem/path"
	"github.com/buildbarn/bb-storage/pkg/testutil"
	"github.com/stretchr/testify/require"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"go.uber.org/mock/gomock"
)

func TestMain(m *testing.M) {
	if mode := os.Getenv("BB_RE_TEST_HELPER"); mode != "" {
		if err := runWindowsProcessTreeHelper(mode); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		os.Exit(0)
	}
	os.Exit(m.Run())
}

func TestLocalRunnerCheckReadiness(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	buildDirectory := mock.NewMockDirectory(ctrl)
	runner := runner.NewLocalRunner(buildDirectory, &path.EmptyBuilder, runner.NewPlainCommandCreator(&syscall.SysProcAttr{}), false)

	t.Run("NoPathSpecified", func(t *testing.T) {
		_, err := runner.CheckReadiness(ctx, &runner_pb.CheckReadinessRequest{})
		require.NoError(t, err)
	})

	t.Run("RootPath", func(t *testing.T) {
		_, err := runner.CheckReadiness(ctx, &runner_pb.CheckReadinessRequest{
			Path: ".",
		})
		require.NoError(t, err)
	})

	t.Run("NonExistentPath", func(t *testing.T) {
		buildDirectory.EXPECT().Lstat(path.MustNewComponent("does_not_exist")).
			Return(filesystem.FileInfo{}, syscall.ENOENT)

		_, err := runner.CheckReadiness(ctx, &runner_pb.CheckReadinessRequest{
			Path: "does_not_exist",
		})
		testutil.RequirePrefixedStatus(t, status.Error(codes.Internal, "Failed to check existence of path \"does_not_exist\" in build directory: "), err)
	})

	t.Run("NonExistentDirectory", func(t *testing.T) {
		buildDirectory.EXPECT().EnterDirectory(path.MustNewComponent("does")).
			Return(nil, syscall.ENOENT)

		_, err := runner.CheckReadiness(ctx, &runner_pb.CheckReadinessRequest{
			Path: "does/not/exist",
		})
		testutil.RequirePrefixedStatus(t, status.Error(codes.Internal, "Failed to resolve path \"does/not/exist\" in build directory: "), err)
	})

	t.Run("Success", func(t *testing.T) {
		someDirectory := mock.NewMockDirectoryCloser(ctrl)
		buildDirectory.EXPECT().EnterDirectory(path.MustNewComponent("some")).
			Return(someDirectory, nil)
		nestedDirectory := mock.NewMockDirectoryCloser(ctrl)
		someDirectory.EXPECT().EnterDirectory(path.MustNewComponent("nested")).
			Return(nestedDirectory, nil)
		nestedDirectory.EXPECT().Lstat(path.MustNewComponent("file")).
			Return(filesystem.NewFileInfo(path.MustNewComponent("file"), filesystem.FileTypeRegularFile, false), nil)
		nestedDirectory.EXPECT().Close()
		someDirectory.EXPECT().Close()

		_, err := runner.CheckReadiness(ctx, &runner_pb.CheckReadinessRequest{
			Path: "some/nested/file",
		})
		require.NoError(t, err)
	})
}

func TestLocalRunnerRun(t *testing.T) {
	ctrl := gomock.NewController(t)

	buildDirectoryPath := t.TempDir()
	buildDirectory, err := filesystem.NewLocalDirectory(path.LocalFormat.NewParser(buildDirectoryPath))
	require.NoError(t, err)
	defer buildDirectory.Close()

	buildDirectoryPathBuilder, scopeWalker := path.EmptyBuilder.Join(path.VoidScopeWalker)
	require.NoError(t, path.Resolve(path.LocalFormat.NewParser(buildDirectoryPath), scopeWalker))

	var cmdPath string
	var getEnvCommand []string
	if runtime.GOOS == "windows" {
		cmdPath = filepath.Join(os.Getenv("SYSTEMROOT"), "system32\\cmd.exe")
		getEnvCommand = []string{cmdPath, "/d", "/c", "set"}
	} else {
		getEnvCommand = []string{"/usr/bin/env"}
	}

	t.Run("EmptyEnvironment", func(t *testing.T) {
		if runtime.GOOS == "windows" {
			return
		}

		testPath := filepath.Join(buildDirectoryPath, "EmptyEnvironment")
		require.NoError(t, os.Mkdir(testPath, 0o777))
		require.NoError(t, os.Mkdir(filepath.Join(testPath, "root"), 0o777))
		require.NoError(t, os.Mkdir(filepath.Join(testPath, "tmp"), 0o777))

		// Running a command without specifying any environment
		// variables should cause the process to be executed in
		// an empty environment. It should not inherit the
		// environment of the runner.
		runner := runner.NewLocalRunner(buildDirectory, buildDirectoryPathBuilder, runner.NewPlainCommandCreator(&syscall.SysProcAttr{}), false)
		response, err := runner.Run(context.Background(), &runner_pb.RunRequest{
			Arguments:          getEnvCommand,
			StdoutPath:         "EmptyEnvironment/stdout",
			StderrPath:         "EmptyEnvironment/stderr",
			InputRootDirectory: "EmptyEnvironment/root",
			TemporaryDirectory: "EmptyEnvironment/tmp",
		})
		require.NoError(t, err)
		require.Equal(t, int64(0), response.ExitCode)

		stdout, err := os.ReadFile(filepath.Join(testPath, "stdout"))
		require.NoError(t, err)
		require.Empty(t, stdout)

		stderr, err := os.ReadFile(filepath.Join(testPath, "stderr"))
		require.NoError(t, err)
		require.Empty(t, stderr)
	})

	t.Run("NonEmptyEnvironment", func(t *testing.T) {
		testPath := filepath.Join(buildDirectoryPath, "NonEmptyEnvironment")
		require.NoError(t, os.Mkdir(testPath, 0o777))
		require.NoError(t, os.Mkdir(filepath.Join(testPath, "root"), 0o777))
		tmpPath := filepath.Join(testPath, "tmp")
		require.NoError(t, os.Mkdir(tmpPath, 0o777))

		// The environment variables provided in the RunRequest
		// should be respected. If automatic injection of TMPDIR
		// is enabled, that variable should also be added.
		runner := runner.NewLocalRunner(buildDirectory, buildDirectoryPathBuilder, runner.NewPlainCommandCreator(&syscall.SysProcAttr{}), true)
		response, err := runner.Run(context.Background(), &runner_pb.RunRequest{
			Arguments: getEnvCommand,
			EnvironmentVariables: map[string]string{
				"FOO": "bar",
				"BAZ": "xyzzy",
			},
			StdoutPath:         "NonEmptyEnvironment/stdout",
			StderrPath:         "NonEmptyEnvironment/stderr",
			InputRootDirectory: "NonEmptyEnvironment/root",
			TemporaryDirectory: "NonEmptyEnvironment/tmp",
		})
		require.NoError(t, err)
		require.Equal(t, int64(0), response.ExitCode)

		stdout, err := os.ReadFile(filepath.Join(testPath, "stdout"))
		require.NoError(t, err)
		if runtime.GOOS == "windows" {
			require.Subset(t, strings.Fields(string(stdout)), []string{
				"FOO=bar",
				"BAZ=xyzzy",
				"TMP=" + tmpPath,
				"TEMP=" + tmpPath,
			})
		} else {
			require.ElementsMatch(t, []string{
				"FOO=bar",
				"BAZ=xyzzy",
				"TMPDIR=" + tmpPath,
			}, strings.Fields(string(stdout)))
		}

		stderr, err := os.ReadFile(filepath.Join(testPath, "stderr"))
		require.NoError(t, err)
		require.Empty(t, stderr)
	})

	t.Run("OverridingTmpdir", func(t *testing.T) {
		testPath := filepath.Join(buildDirectoryPath, "OverridingTmpdir")
		require.NoError(t, os.Mkdir(testPath, 0o777))
		require.NoError(t, os.Mkdir(filepath.Join(testPath, "root"), 0o777))
		tmpPath := filepath.Join(testPath, "tmp")
		require.NoError(t, os.Mkdir(tmpPath, 0o777))

		var envMap map[string]string
		if runtime.GOOS == "windows" {
			envMap = map[string]string{
				"TMP":  "\\somewhere\\else",
				"TEMP": "\\somewhere\\else",
			}
		} else {
			envMap = map[string]string{
				"TMPDIR": "/somewhere/else",
			}
		}

		// Automatic injection of TMPDIR should have no effect
		// if the command to be run provides its own TMPDIR.
		runner := runner.NewLocalRunner(buildDirectory, buildDirectoryPathBuilder, runner.NewPlainCommandCreator(&syscall.SysProcAttr{}), true)
		response, err := runner.Run(context.Background(), &runner_pb.RunRequest{
			Arguments:            getEnvCommand,
			EnvironmentVariables: envMap,
			StdoutPath:           "OverridingTmpdir/stdout",
			StderrPath:           "OverridingTmpdir/stderr",
			InputRootDirectory:   "OverridingTmpdir/root",
			TemporaryDirectory:   "OverridingTmpdir/tmp",
		})
		require.NoError(t, err)
		require.Equal(t, int64(0), response.ExitCode)

		stdout, err := os.ReadFile(filepath.Join(testPath, "stdout"))
		require.NoError(t, err)
		if runtime.GOOS == "windows" {
			require.Subset(t, strings.Fields(string(stdout)), []string{
				"TMP=\\somewhere\\else",
				"TEMP=\\somewhere\\else",
			})
		} else {
			require.Equal(t, "TMPDIR=/somewhere/else\n", string(stdout))
		}

		stderr, err := os.ReadFile(filepath.Join(testPath, "stderr"))
		require.NoError(t, err)
		require.Empty(t, stderr)
	})

	t.Run("NonZeroExitCode", func(t *testing.T) {
		testPath := filepath.Join(buildDirectoryPath, "NonZeroExitCode")
		require.NoError(t, os.Mkdir(testPath, 0o777))
		require.NoError(t, os.Mkdir(filepath.Join(testPath, "root"), 0o777))
		require.NoError(t, os.Mkdir(filepath.Join(testPath, "tmp"), 0o777))

		// Non-zero exit codes should be captured in the
		// RunResponse. POSIX 2008 and later added support for
		// 32-bit signed exit codes. Most implementations still
		// truncate the exit code to 8 bits.
		var exit255Command []string
		if runtime.GOOS == "windows" {
			exit255Command = []string{cmdPath, "/d", "/c", "exit 255"}
		} else {
			exit255Command = []string{"/bin/sh", "-c", "exit 255"}
		}
		runner := runner.NewLocalRunner(buildDirectory, buildDirectoryPathBuilder, runner.NewPlainCommandCreator(&syscall.SysProcAttr{}), false)
		response, err := runner.Run(context.Background(), &runner_pb.RunRequest{
			Arguments:          exit255Command,
			StdoutPath:         "NonZeroExitCode/stdout",
			StderrPath:         "NonZeroExitCode/stderr",
			InputRootDirectory: "NonZeroExitCode/root",
			TemporaryDirectory: "NonZeroExitCode/tmp",
		})
		require.NoError(t, err)
		require.Equal(t, int64(255), response.ExitCode)

		stdout, err := os.ReadFile(filepath.Join(testPath, "stdout"))
		require.NoError(t, err)
		require.Empty(t, stdout)

		stderr, err := os.ReadFile(filepath.Join(testPath, "stderr"))
		require.NoError(t, err)
		require.Empty(t, stderr)
	})

	t.Run("SigKill", func(t *testing.T) {
		if runtime.GOOS == "windows" {
			return
		}

		testPath := filepath.Join(buildDirectoryPath, "SigKill")
		require.NoError(t, os.Mkdir(testPath, 0o777))
		require.NoError(t, os.Mkdir(filepath.Join(testPath, "root"), 0o777))
		require.NoError(t, os.Mkdir(filepath.Join(testPath, "tmp"), 0o777))

		// If the process terminates due to a signal, the name
		// of the signal should be set as part of the POSIX
		// resource usage message.
		runner := runner.NewLocalRunner(buildDirectory, buildDirectoryPathBuilder, runner.NewPlainCommandCreator(&syscall.SysProcAttr{}), false)
		response, err := runner.Run(context.Background(), &runner_pb.RunRequest{
			Arguments:          []string{"/bin/sh", "-c", "kill -s KILL $$"},
			StdoutPath:         "SigKill/stdout",
			StderrPath:         "SigKill/stderr",
			InputRootDirectory: "SigKill/root",
			TemporaryDirectory: "SigKill/tmp",
		})
		require.NoError(t, err)
		require.NotEqual(t, int64(0), response.ExitCode)

		require.Len(t, response.ResourceUsage, 1)
		var posixResourceUsage resourceusage.POSIXResourceUsage
		require.NoError(t, response.ResourceUsage[0].UnmarshalTo(&posixResourceUsage))
		require.Equal(t, "KILL", posixResourceUsage.TerminationSignal)

		stdout, err := os.ReadFile(filepath.Join(testPath, "stdout"))
		require.NoError(t, err)
		require.Empty(t, stdout)

		stderr, err := os.ReadFile(filepath.Join(testPath, "stderr"))
		require.NoError(t, err)
		require.Empty(t, stderr)
	})

	t.Run("UnknownCommandWithEmptyPath", func(t *testing.T) {
		testPath := filepath.Join(buildDirectoryPath, "UnknownCommandWithEmptyPath")
		require.NoError(t, os.Mkdir(testPath, 0o777))
		require.NoError(t, os.Mkdir(filepath.Join(testPath, "root"), 0o777))
		require.NoError(t, os.Mkdir(filepath.Join(testPath, "tmp"), 0o777))

		// If argv[0] consists of a single filename, lookups
		// against $PATH need to be performed. If PATH is not
		// set, the action should fail with a non-retriable
		// error.
		runner := runner.NewLocalRunner(buildDirectory, buildDirectoryPathBuilder, runner.NewPlainCommandCreator(&syscall.SysProcAttr{}), false)
		_, err := runner.Run(context.Background(), &runner_pb.RunRequest{
			Arguments:          []string{"nonexistent_command"},
			StdoutPath:         "UnknownCommandWithEmptyPath/stdout",
			StderrPath:         "UnknownCommandWithEmptyPath/stderr",
			InputRootDirectory: "UnknownCommandWithEmptyPath/root",
			TemporaryDirectory: "UnknownCommandWithEmptyPath/tmp",
		})
		testutil.RequirePrefixedStatus(t, status.Error(codes.InvalidArgument, "Cannot find executable \"nonexistent_command\" in search paths \"\""), err)
	})

	t.Run("UnknownCommandWithBadPath", func(t *testing.T) {
		testPath := filepath.Join(buildDirectoryPath, "UnknownCommandWithBadPath")
		require.NoError(t, os.Mkdir(testPath, 0o777))
		require.NoError(t, os.Mkdir(filepath.Join(testPath, "root"), 0o777))
		require.NoError(t, os.Mkdir(filepath.Join(testPath, "tmp"), 0o777))

		// Even invoking known shell utilities shouldn't be
		// permitted if PATH points to a nonexistent location.
		runner := runner.NewLocalRunner(buildDirectory, buildDirectoryPathBuilder, runner.NewPlainCommandCreator(&syscall.SysProcAttr{}), false)
		_, err := runner.Run(context.Background(), &runner_pb.RunRequest{
			Arguments:            []string{"sh", "-c", "exit 123"},
			EnvironmentVariables: map[string]string{"PATH": "/nonexistent"},
			StdoutPath:           "UnknownCommandWithBadPath/stdout",
			StderrPath:           "UnknownCommandWithBadPath/stderr",
			InputRootDirectory:   "UnknownCommandWithBadPath/root",
			TemporaryDirectory:   "UnknownCommandWithBadPath/tmp",
		})
		testutil.RequirePrefixedStatus(t, status.Error(codes.InvalidArgument, "Cannot find executable \"sh\" in search paths \"/nonexistent\""), err)
	})

	t.Run("RelativeSearchPath", func(t *testing.T) {
		if runtime.GOOS == "windows" {
			return
		}

		testPath := filepath.Join(buildDirectoryPath, "RelativeSearchPath")
		require.NoError(t, os.Mkdir(testPath, 0o777))
		require.NoError(t, os.MkdirAll(filepath.Join(testPath, "root", "subdirectory"), 0o777))
		require.NoError(t, os.Mkdir(filepath.Join(testPath, "tmp"), 0o777))
		require.NoError(t, os.WriteFile(filepath.Join(testPath, "root", "subdirectory", "hello.sh"), []byte("#!/bin/sh\necho $0\nexit 42\n"), 0o777))

		// If the PATH environment variable contains a relative
		// path, it should be treated as being relative to the
		// working directory. Because the search path is
		// relative, execve() should be called with a relative
		// path as well.
		runner := runner.NewLocalRunner(buildDirectory, buildDirectoryPathBuilder, runner.NewPlainCommandCreator(&syscall.SysProcAttr{}), false)
		response, err := runner.Run(context.Background(), &runner_pb.RunRequest{
			Arguments:            []string{"hello.sh"},
			EnvironmentVariables: map[string]string{"PATH": "subdirectory"},
			StdoutPath:           "RelativeSearchPath/stdout",
			StderrPath:           "RelativeSearchPath/stderr",
			InputRootDirectory:   "RelativeSearchPath/root",
			TemporaryDirectory:   "RelativeSearchPath/tmp",
		})
		require.NoError(t, err)
		require.Equal(t, int64(42), response.ExitCode)

		stdout, err := os.ReadFile(filepath.Join(testPath, "stdout"))
		require.NoError(t, err)
		require.Equal(t, "subdirectory/hello.sh\n", string(stdout))

		stderr, err := os.ReadFile(filepath.Join(testPath, "stderr"))
		require.NoError(t, err)
		require.Empty(t, stderr)
	})

	t.Run("UnknownCommandRelative", func(t *testing.T) {
		testPath := filepath.Join(buildDirectoryPath, "UnknownCommandRelative")
		require.NoError(t, os.Mkdir(testPath, 0o777))
		require.NoError(t, os.Mkdir(filepath.Join(testPath, "root"), 0o777))
		require.NoError(t, os.Mkdir(filepath.Join(testPath, "tmp"), 0o777))

		// If argv[0] is not an absolute path, but does consist
		// of multiple components, no $PATH lookup is performed.
		// If the path does not exist, the action should fail
		// with a non-retriable error.
		runner := runner.NewLocalRunner(buildDirectory, buildDirectoryPathBuilder, runner.NewPlainCommandCreator(&syscall.SysProcAttr{}), false)
		_, err := runner.Run(context.Background(), &runner_pb.RunRequest{
			Arguments:          []string{"./nonexistent_command"},
			StdoutPath:         "UnknownCommandRelative/stdout",
			StderrPath:         "UnknownCommandRelative/stderr",
			InputRootDirectory: "UnknownCommandRelative/root",
			TemporaryDirectory: "UnknownCommandRelative/tmp",
		})
		testutil.RequirePrefixedStatus(t, status.Error(codes.InvalidArgument, "Failed to start process: "), err)
	})

	t.Run("UnknownCommandAbsolute", func(t *testing.T) {
		testPath := filepath.Join(buildDirectoryPath, "UnknownCommandAbsolute")
		require.NoError(t, os.Mkdir(testPath, 0o777))
		require.NoError(t, os.Mkdir(filepath.Join(testPath, "root"), 0o777))
		require.NoError(t, os.Mkdir(filepath.Join(testPath, "tmp"), 0o777))

		// If argv[0] is an absolute path that does not exist,
		// we should also return a non-retriable error.
		runner := runner.NewLocalRunner(buildDirectory, buildDirectoryPathBuilder, runner.NewPlainCommandCreator(&syscall.SysProcAttr{}), false)
		_, err := runner.Run(context.Background(), &runner_pb.RunRequest{
			Arguments:          []string{"/nonexistent_command"},
			StdoutPath:         "UnknownCommandAbsolute/stdout",
			StderrPath:         "UnknownCommandAbsolute/stderr",
			InputRootDirectory: "UnknownCommandAbsolute/root",
			TemporaryDirectory: "UnknownCommandAbsolute/tmp",
		})
		testutil.RequirePrefixedStatus(t, status.Error(codes.InvalidArgument, "Failed to start process: "), err)
	})

	t.Run("ExecFormatErrorJPEG", func(t *testing.T) {
		testPath := filepath.Join(buildDirectoryPath, "ExecFormatErrorJPEG")
		require.NoError(t, os.Mkdir(testPath, 0o777))
		require.NoError(t, os.Mkdir(filepath.Join(testPath, "root"), 0o777))
		require.NoError(t, os.Mkdir(filepath.Join(testPath, "tmp"), 0o777))
		require.NoError(t, os.WriteFile(filepath.Join(testPath, "root", "not_a.binary"), []byte{
			0x89, 0x50, 0x4e, 0x47, 0x0d, 0x0a, 0x1a, 0x0a,
		}, 0o777))

		// If argv[0] is a binary that cannot be executed we
		// should also return a non-retriable error. In this
		// case it's a JPEG file.
		runner := runner.NewLocalRunner(buildDirectory, buildDirectoryPathBuilder, runner.NewPlainCommandCreator(&syscall.SysProcAttr{}), false)
		_, err := runner.Run(context.Background(), &runner_pb.RunRequest{
			Arguments:          []string{"./not_a.binary"},
			StdoutPath:         "ExecFormatErrorJPEG/stdout",
			StderrPath:         "ExecFormatErrorJPEG/stderr",
			InputRootDirectory: "ExecFormatErrorJPEG/root",
			TemporaryDirectory: "ExecFormatErrorJPEG/tmp",
		})
		testutil.RequirePrefixedStatus(t, status.Error(codes.InvalidArgument, "Failed to start process: "), err)
	})

	t.Run("ExecFormatErrorMachOBadArch", func(t *testing.T) {
		testPath := filepath.Join(buildDirectoryPath, "ExecFormatErrorMachOBadArch")
		require.NoError(t, os.Mkdir(testPath, 0o777))
		require.NoError(t, os.Mkdir(filepath.Join(testPath, "root"), 0o777))
		require.NoError(t, os.Mkdir(filepath.Join(testPath, "tmp"), 0o777))
		require.NoError(t, os.WriteFile(filepath.Join(testPath, "root", "not_a.binary"), []byte{
			0xcf, 0xfa, 0xed, 0xfe, 0x01, 0x00, 0x00, 0x00, 0x03,
			0x00, 0x00, 0x80, 0x02, 0x00, 0x00, 0x00, 0x02, 0x00,
			0x00, 0x00, 0xf1, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00,
			0x00, 0x00, 0x00, 0x00, 0x00, 0x19, 0x00, 0x00, 0x00,
			0x48, 0x00, 0x00, 0x00, 0x48, 0x65, 0x6c, 0x6c, 0x6f,
			0x2c, 0x20, 0x57, 0x6f, 0x72, 0x6c, 0x64, 0x21, 0x0a,
			0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
			0x00, 0x00, 0x10, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
			0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x11,
			0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x07, 0x00,
			0x00, 0x00, 0x05, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
			0x00, 0x00, 0x00, 0x00, 0x00, 0x05, 0x00, 0x00, 0x00,
			0xb8, 0x00, 0x00, 0x00, 0x04, 0x00, 0x00, 0x00, 0x2a,
			0x00, 0x00, 0x00, 0xba, 0x0e, 0x00, 0x00, 0x00, 0xb8,
			0x04, 0x00, 0x00, 0x02, 0x0f, 0x05, 0xeb, 0x28, 0x00,
			0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00,
			0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x28, 0x00, 0x00,
			0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
			0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
			0x00, 0x00, 0x00, 0x48, 0x31, 0xff, 0xb8, 0x01, 0x00,
			0x00, 0x02, 0x0f, 0x05, 0x00, 0x00, 0x00, 0x00, 0x00,
			0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
			0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
			0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
			0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
			0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x78,
			0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
			0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
			0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
			0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
			0x00, 0x00, 0x00,
		}, 0o777))

		// On macOS, running a Mach-O executable that was
		// compiled for a different CPU will return EBADARCH
		// instead of ENOEXEC. This should still cause a
		// non-retriable error to be returned.
		//
		// Test this by attempting to run a tiny Mach-O
		// executable that uses CPU_TYPE_VAX.
		runner := runner.NewLocalRunner(buildDirectory, buildDirectoryPathBuilder, runner.NewPlainCommandCreator(&syscall.SysProcAttr{}), false)
		_, err := runner.Run(context.Background(), &runner_pb.RunRequest{
			Arguments:          []string{"./not_a.binary"},
			StdoutPath:         "ExecFormatErrorMachOBadArch/stdout",
			StderrPath:         "ExecFormatErrorMachOBadArch/stderr",
			InputRootDirectory: "ExecFormatErrorMachOBadArch/root",
			TemporaryDirectory: "ExecFormatErrorMachOBadArch/tmp",
		})
		testutil.RequirePrefixedStatus(t, status.Error(codes.InvalidArgument, "Failed to start process: "), err)
	})

	t.Run("UnknownCommandDirectory", func(t *testing.T) {
		testPath := filepath.Join(buildDirectoryPath, "UnknownCommandDirectory")
		require.NoError(t, os.Mkdir(testPath, 0o777))
		require.NoError(t, os.Mkdir(filepath.Join(testPath, "root"), 0o777))
		require.NoError(t, os.Mkdir(filepath.Join(testPath, "tmp"), 0o777))

		// If argv[0] refers to a directory, we should also
		// return a non-retriable error.
		runner := runner.NewLocalRunner(buildDirectory, buildDirectoryPathBuilder, runner.NewPlainCommandCreator(&syscall.SysProcAttr{}), false)
		_, err := runner.Run(context.Background(), &runner_pb.RunRequest{
			Arguments:          []string{"/"},
			StdoutPath:         "UnknownCommandDirectory/stdout",
			StderrPath:         "UnknownCommandDirectory/stderr",
			InputRootDirectory: "UnknownCommandDirectory/root",
			TemporaryDirectory: "UnknownCommandDirectory/tmp",
		})
		testutil.RequirePrefixedStatus(t, status.Error(codes.InvalidArgument, "Failed to start process: "), err)
	})

	t.Run("BuildDirectoryEscape", func(t *testing.T) {
		buildDirectory := mock.NewMockDirectory(ctrl)
		helloDirectory := mock.NewMockDirectoryCloser(ctrl)
		buildDirectory.EXPECT().EnterDirectory(path.MustNewComponent("hello")).Return(helloDirectory, nil)
		helloDirectory.EXPECT().Close()

		// The runner process may need to run with elevated
		// privileges. It shouldn't be possible to trick the
		// runner into opening files outside the build
		// directory.
		runner := runner.NewLocalRunner(buildDirectory, &path.EmptyBuilder, runner.NewPlainCommandCreator(&syscall.SysProcAttr{}), false)
		_, err := runner.Run(context.Background(), &runner_pb.RunRequest{
			Arguments:          getEnvCommand,
			StdoutPath:         "hello/../../../../../../etc/passwd",
			StderrPath:         "stderr",
			InputRootDirectory: ".",
			TemporaryDirectory: ".",
		})
		testutil.RequireEqualStatus(
			t,
			status.Error(codes.InvalidArgument, "Failed to open stdout path \"hello/../../../../../../etc/passwd\": Path resolves to a location outside the build directory"),
			err,
		)
	})

	// TODO: Improve testing coverage of LocalRunner.
}

func TestLocalRunnerRunWindowsSubprocessCleanup(t *testing.T) {
	if runtime.GOOS != "windows" {
		return
	}

	// The child helper keeps its current directory inside the input root and
	// opens a file there. Go's Windows syscall.Open shares read/write but not
	// delete access, so RemoveAll fails while that descendant is alive. If this
	// test can immediately remove the root, Run() waited for descendant cleanup.
	buildDirectoryPath := t.TempDir()
	buildDirectory, err := filesystem.NewLocalDirectory(path.LocalFormat.NewParser(buildDirectoryPath))
	require.NoError(t, err)
	defer buildDirectory.Close()

	buildDirectoryPathBuilder, scopeWalker := path.EmptyBuilder.Join(path.VoidScopeWalker)
	require.NoError(t, path.Resolve(path.LocalFormat.NewParser(buildDirectoryPath), scopeWalker))

	testBinaryPath, err := os.Executable()
	require.NoError(t, err)

	testName := "WindowsSubprocessCleanup"
	testPath := filepath.Join(buildDirectoryPath, testName)
	rootPath := filepath.Join(testPath, "root")
	require.NoError(t, os.Mkdir(testPath, 0o777))
	require.NoError(t, os.Mkdir(rootPath, 0o777))
	require.NoError(t, os.Mkdir(filepath.Join(testPath, "tmp"), 0o777))

	environmentVariables := map[string]string{
		"BB_RE_TEST_HELPER":      "parent",
		"BB_RE_TEST_BINARY":      testBinaryPath,
		"BB_RE_TEST_LOCKED_FILE": filepath.Join(rootPath, "locked"),
		"BB_RE_TEST_READY_FILE":  filepath.Join(rootPath, "ready"),
	}
	for _, name := range []string{"COMSPEC", "PATH", "SYSTEMROOT", "TEMP", "TMP", "WINDIR"} {
		if value, ok := os.LookupEnv(name); ok {
			environmentVariables[name] = value
		}
	}

	runner := runner.NewLocalRunner(buildDirectory, buildDirectoryPathBuilder, runner.NewPlainCommandCreator(&syscall.SysProcAttr{}), false)
	response, err := runner.Run(context.Background(), &runner_pb.RunRequest{
		Arguments:            []string{testBinaryPath},
		EnvironmentVariables: environmentVariables,
		StdoutPath:           testName + "/stdout",
		StderrPath:           testName + "/stderr",
		InputRootDirectory:   testName + "/root",
		TemporaryDirectory:   testName + "/tmp",
	})
	require.NoError(t, err)
	require.Equal(t, int64(0), response.ExitCode)
	require.FileExists(t, filepath.Join(rootPath, "ready"))

	require.NoError(t, os.RemoveAll(rootPath))
}

func TestLocalRunnerRunWindowsCancellationCleanup(t *testing.T) {
	if runtime.GOOS != "windows" {
		return
	}

	// The root helper starts a descendant that holds an input-root file open,
	// then remains alive. Cancellation must terminate both processes, and Run
	// must wait for the entire job to be reaped so the root is immediately
	// removable.
	buildDirectoryPath := t.TempDir()
	buildDirectory, err := filesystem.NewLocalDirectory(path.LocalFormat.NewParser(buildDirectoryPath))
	require.NoError(t, err)
	defer buildDirectory.Close()

	buildDirectoryPathBuilder, scopeWalker := path.EmptyBuilder.Join(path.VoidScopeWalker)
	require.NoError(t, path.Resolve(path.LocalFormat.NewParser(buildDirectoryPath), scopeWalker))

	testName := "CancellationCleanup"
	testPath := filepath.Join(buildDirectoryPath, testName)
	rootPath := filepath.Join(testPath, "root")
	require.NoError(t, os.Mkdir(testPath, 0o777))
	require.NoError(t, os.Mkdir(rootPath, 0o777))
	require.NoError(t, os.Mkdir(filepath.Join(testPath, "tmp"), 0o777))

	testBinaryPath, err := os.Executable()
	require.NoError(t, err)
	readyFilePath := filepath.Join(rootPath, "ready")
	environmentVariables := map[string]string{
		"BB_RE_TEST_HELPER":      "parent_wait",
		"BB_RE_TEST_BINARY":      testBinaryPath,
		"BB_RE_TEST_LOCKED_FILE": filepath.Join(rootPath, "locked"),
		"BB_RE_TEST_READY_FILE":  readyFilePath,
	}
	for _, name := range []string{"COMSPEC", "PATH", "SYSTEMROOT", "TEMP", "TMP", "WINDIR"} {
		if value, ok := os.LookupEnv(name); ok {
			environmentVariables[name] = value
		}
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	type runResult struct {
		response *runner_pb.RunResponse
		err      error
	}
	runResultChannel := make(chan runResult, 1)
	localRunner := runner.NewLocalRunner(buildDirectory, buildDirectoryPathBuilder, runner.NewPlainCommandCreator(&syscall.SysProcAttr{}), false)
	go func() {
		response, err := localRunner.Run(ctx, &runner_pb.RunRequest{
			Arguments:            []string{testBinaryPath},
			EnvironmentVariables: environmentVariables,
			StdoutPath:           testName + "/stdout",
			StderrPath:           testName + "/stderr",
			InputRootDirectory:   testName + "/root",
			TemporaryDirectory:   testName + "/tmp",
		})
		runResultChannel <- runResult{response: response, err: err}
	}()

	deadline := time.Now().Add(10 * time.Second)
	for {
		if _, err := os.Stat(readyFilePath); err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("timed out waiting for the descendant helper")
		}
		time.Sleep(10 * time.Millisecond)
	}
	cancel()

	var result runResult
	select {
	case result = <-runResultChannel:
	case <-time.After(10 * time.Second):
		t.Fatal("Run() hung after cancellation")
	}
	if result.err != nil {
		require.NotContains(t, result.err.Error(), "Failed to finish process startup")
	} else {
		require.NotNil(t, result.response)
	}

	require.NoError(t, os.RemoveAll(rootPath))
	require.NoDirExists(t, rootPath)
}

func runWindowsProcessTreeHelper(mode string) error {
	// parent spawns child and exits, parent_wait spawns child and remains alive,
	// and child locks an input-root file before signaling that it is ready.
	switch mode {
	case "parent", "parent_wait":
		testBinaryPath := os.Getenv("BB_RE_TEST_BINARY")
		if testBinaryPath == "" {
			return fmt.Errorf("BB_RE_TEST_BINARY is not set")
		}
		cmd := exec.Command(testBinaryPath)
		cmd.Env = append(os.Environ(), "BB_RE_TEST_HELPER=child")
		cmd.Dir = "."
		if err := cmd.Start(); err != nil {
			return fmt.Errorf("failed to start child helper: %w", err)
		}
		if mode == "parent_wait" {
			time.Sleep(time.Minute)
			return nil
		}
		readyFilePath := os.Getenv("BB_RE_TEST_READY_FILE")
		deadline := time.Now().Add(10 * time.Second)
		for {
			if _, err := os.Stat(readyFilePath); err == nil {
				return nil
			}
			if time.Now().After(deadline) {
				return fmt.Errorf("timed out waiting for child helper")
			}
			time.Sleep(10 * time.Millisecond)
		}
	case "child":
		lockedFilePath := os.Getenv("BB_RE_TEST_LOCKED_FILE")
		readyFilePath := os.Getenv("BB_RE_TEST_READY_FILE")
		lockedFile, err := os.OpenFile(lockedFilePath, os.O_CREATE|os.O_RDWR, 0o666)
		if err != nil {
			return fmt.Errorf("failed to open locked file: %w", err)
		}
		defer lockedFile.Close()
		if _, err := lockedFile.WriteString("locked"); err != nil {
			return fmt.Errorf("failed to write locked file: %w", err)
		}
		if err := os.WriteFile(readyFilePath, []byte("ready"), 0o666); err != nil {
			return fmt.Errorf("failed to write ready file: %w", err)
		}
		time.Sleep(time.Minute)
		return nil
	default:
		return fmt.Errorf("unknown helper mode %#v", mode)
	}
}
