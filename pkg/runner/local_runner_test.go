package runner_test

import (
	"context"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"testing"

	runner_pb "github.com/buildbarn/bb-remote-execution/pkg/proto/runner"
	"github.com/buildbarn/bb-remote-execution/pkg/runner"
	"github.com/buildbarn/bb-storage/pkg/filesystem"
	"github.com/stretchr/testify/require"
)

func TestLocalRunner(t *testing.T) {
	buildDirectoryPath := filepath.Join(os.Getenv("TEST_TMPDIR"), t.Name())
	require.NoError(t, os.Mkdir(buildDirectoryPath, 0777))
	buildDirectory, err := filesystem.NewLocalDirectory(buildDirectoryPath)
	require.NoError(t, err)
	defer buildDirectory.Close()

	t.Run("EmptyEnvironment", func(t *testing.T) {
		testPath := filepath.Join(buildDirectoryPath, "EmptyEnvironment")
		require.NoError(t, os.Mkdir(testPath, 0777))
		require.NoError(t, os.Mkdir(filepath.Join(testPath, "root"), 0777))
		require.NoError(t, os.Mkdir(filepath.Join(testPath, "tmp"), 0777))

		// Running a command without specifying any environment
		// variables should cause the process to be executed in
		// an empty environment. It should not inherit the
		// environment of the runner.
		runner := runner.NewLocalRunner(buildDirectory, buildDirectoryPath, false, false)
		response, err := runner.Run(context.Background(), &runner_pb.RunRequest{
			Arguments:          []string{"/usr/bin/env"},
			StdoutPath:         "EmptyEnvironment/stdout",
			StderrPath:         "EmptyEnvironment/stderr",
			InputRootDirectory: "EmptyEnvironment/root",
			TemporaryDirectory: "EmptyEnvironment/tmp",
		})
		require.NoError(t, err)
		require.Equal(t, int32(0), response.ExitCode)

		stdout, err := ioutil.ReadFile(filepath.Join(testPath, "stdout"))
		require.NoError(t, err)
		require.Empty(t, stdout)

		stderr, err := ioutil.ReadFile(filepath.Join(testPath, "stderr"))
		require.NoError(t, err)
		require.Empty(t, stderr)
	})

	t.Run("NonEmptyEnvironment", func(t *testing.T) {
		testPath := filepath.Join(buildDirectoryPath, "NonEmptyEnvironment")
		require.NoError(t, os.Mkdir(testPath, 0777))
		require.NoError(t, os.Mkdir(filepath.Join(testPath, "root"), 0777))
		tmpPath := filepath.Join(testPath, "tmp")
		require.NoError(t, os.Mkdir(tmpPath, 0777))

		// The environment variables provided in the RunRequest
		// should be respected. If automatic injection of TMPDIR
		// is enabled, that variable should also be added.
		runner := runner.NewLocalRunner(buildDirectory, buildDirectoryPath, true, false)
		response, err := runner.Run(context.Background(), &runner_pb.RunRequest{
			Arguments: []string{"/usr/bin/env"},
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
		require.Equal(t, int32(0), response.ExitCode)

		stdout, err := ioutil.ReadFile(filepath.Join(testPath, "stdout"))
		require.NoError(t, err)
		require.ElementsMatch(t, []string{
			"FOO=bar",
			"BAZ=xyzzy",
			"TMPDIR=" + tmpPath,
		}, strings.Fields(string(stdout)))

		stderr, err := ioutil.ReadFile(filepath.Join(testPath, "stderr"))
		require.NoError(t, err)
		require.Empty(t, stderr)
	})

	t.Run("OverridingTmpdir", func(t *testing.T) {
		testPath := filepath.Join(buildDirectoryPath, "OverridingTmpdir")
		require.NoError(t, os.Mkdir(testPath, 0777))
		require.NoError(t, os.Mkdir(filepath.Join(testPath, "root"), 0777))
		tmpPath := filepath.Join(testPath, "tmp")
		require.NoError(t, os.Mkdir(tmpPath, 0777))

		// Automatic injection of TMPDIR should have no effect
		// if the command to be run provides its own TMPDIR.
		runner := runner.NewLocalRunner(buildDirectory, buildDirectoryPath, true, false)
		response, err := runner.Run(context.Background(), &runner_pb.RunRequest{
			Arguments: []string{"/usr/bin/env"},
			EnvironmentVariables: map[string]string{
				"TMPDIR": "/somewhere/else",
			},
			StdoutPath:         "OverridingTmpdir/stdout",
			StderrPath:         "OverridingTmpdir/stderr",
			InputRootDirectory: "OverridingTmpdir/root",
			TemporaryDirectory: "OverridingTmpdir/tmp",
		})
		require.NoError(t, err)
		require.Equal(t, int32(0), response.ExitCode)

		stdout, err := ioutil.ReadFile(filepath.Join(testPath, "stdout"))
		require.NoError(t, err)
		require.Equal(t, "TMPDIR=/somewhere/else\n", string(stdout))

		stderr, err := ioutil.ReadFile(filepath.Join(testPath, "stderr"))
		require.NoError(t, err)
		require.Empty(t, stderr)
	})

	t.Run("NonZeroExitCode", func(t *testing.T) {
		testPath := filepath.Join(buildDirectoryPath, "NonZeroExitCode")
		require.NoError(t, os.Mkdir(testPath, 0777))
		require.NoError(t, os.Mkdir(filepath.Join(testPath, "root"), 0777))
		require.NoError(t, os.Mkdir(filepath.Join(testPath, "tmp"), 0777))

		// Non-zero exit codes should be captured in the
		// RunResponse. POSIX 2008 and later added support for
		// 32-bit signed exit codes. Most implementations still
		// truncate the exit code to 8 bits.
		runner := runner.NewLocalRunner(buildDirectory, buildDirectoryPath, false, false)
		response, err := runner.Run(context.Background(), &runner_pb.RunRequest{
			Arguments:          []string{"/bin/sh", "-c", "exit 255"},
			StdoutPath:         "NonZeroExitCode/stdout",
			StderrPath:         "NonZeroExitCode/stderr",
			InputRootDirectory: "NonZeroExitCode/root",
			TemporaryDirectory: "NonZeroExitCode/tmp",
		})
		require.NoError(t, err)
		require.Equal(t, int32(255), response.ExitCode)

		stdout, err := ioutil.ReadFile(filepath.Join(testPath, "stdout"))
		require.NoError(t, err)
		require.Empty(t, stdout)

		stderr, err := ioutil.ReadFile(filepath.Join(testPath, "stderr"))
		require.NoError(t, err)
		require.Empty(t, stderr)
	})

	// TODO: Improve testing coverage of LocalRunner.
}
