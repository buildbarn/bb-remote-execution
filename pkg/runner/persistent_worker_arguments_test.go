package runner_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/buildbarn/bb-remote-execution/pkg/runner"
	"github.com/buildbarn/bb-storage/pkg/filesystem"
	"github.com/buildbarn/bb-storage/pkg/filesystem/path"
	"github.com/buildbarn/bb-storage/pkg/testutil"
	"github.com/stretchr/testify/require"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestSplitPersistentWorkerArguments(t *testing.T) {
	t.Run("Success", func(t *testing.T) {
		// The arguments that Bazel sends to a remote execution
		// service are the ones that would be used to run the
		// action as a regular process, meaning they contain the
		// flag files and no "--persistent_worker". Splitting
		// them needs to yield the command line that Bazel would
		// have used to launch a worker process locally.
		workerArguments, flagFileArguments, err := runner.SplitPersistentWorkerArguments([]string{
			"external/remotejdk/bin/java",
			"-jar",
			"JavaBuilder_deploy.jar",
			"@bazel-out/k8-fastbuild/bin/hello.jar-0.params",
		})
		require.NoError(t, err)
		require.Equal(t, []string{
			"external/remotejdk/bin/java",
			"-jar",
			"JavaBuilder_deploy.jar",
			"--persistent_worker",
		}, workerArguments)
		require.Equal(t, []string{"@bazel-out/k8-fastbuild/bin/hello.jar-0.params"}, flagFileArguments)
	})

	t.Run("PersistentWorkerFlagAlreadyPresent", func(t *testing.T) {
		// Bazel appends "--persistent_worker" unconditionally,
		// meaning a tool whose command line already contains it
		// receives it twice. Behave identically, so that the
		// command line remains the one the tool would have been
		// launched with locally.
		workerArguments, _, err := runner.SplitPersistentWorkerArguments([]string{
			"tool",
			"--persistent_worker",
			"@params",
		})
		require.NoError(t, err)
		require.Equal(t, []string{"tool", "--persistent_worker", "--persistent_worker"}, workerArguments)
	})

	t.Run("FlagFileOptions", func(t *testing.T) {
		// Both "-flagfile=" and "--flagfile=" are recognised by
		// Bazel, and may appear anywhere on the command line.
		workerArguments, flagFileArguments, err := runner.SplitPersistentWorkerArguments([]string{
			"tool",
			"--flagfile=first.params",
			"--worker",
			"-flagfile=second.params",
		})
		require.NoError(t, err)
		require.Equal(t, []string{"tool", "--worker", "--persistent_worker"}, workerArguments)
		require.Equal(t, []string{"--flagfile=first.params", "-flagfile=second.params"}, flagFileArguments)
	})

	t.Run("EscapedFlagFile", func(t *testing.T) {
		// Arguments starting with "@@" are still treated as
		// flag file arguments by Bazel's splitting logic, even
		// though they are never expanded.
		workerArguments, flagFileArguments, err := runner.SplitPersistentWorkerArguments([]string{
			"tool",
			"@@literal",
			"@real.params",
		})
		require.NoError(t, err)
		require.Equal(t, []string{"tool", "--persistent_worker"}, workerArguments)
		require.Equal(t, []string{"@@literal", "@real.params"}, flagFileArguments)
	})

	t.Run("EmptyArgumentsAreNotFlagFiles", func(t *testing.T) {
		workerArguments, flagFileArguments, err := runner.SplitPersistentWorkerArguments([]string{
			"tool",
			"@",
			"--flagfile=",
			"@params",
		})
		require.NoError(t, err)
		require.Equal(t, []string{"tool", "@", "--flagfile=", "--persistent_worker"}, workerArguments)
		require.Equal(t, []string{"@params"}, flagFileArguments)
	})

	t.Run("NoArguments", func(t *testing.T) {
		_, _, err := runner.SplitPersistentWorkerArguments(nil)
		testutil.RequireEqualStatus(
			t,
			status.Error(codes.InvalidArgument, "Command line arguments of persistent worker actions must contain at least one argument that is not a flag file"),
			err,
		)
	})

	t.Run("NoFlagFile", func(t *testing.T) {
		_, _, err := runner.SplitPersistentWorkerArguments([]string{"tool", "--persistent_worker"})
		testutil.RequireEqualStatus(
			t,
			status.Error(codes.InvalidArgument, "Command line arguments of persistent worker actions must contain at least one \"@flagfile\" or \"--flagfile=\" argument"),
			err,
		)
	})

	t.Run("OnlyFlagFiles", func(t *testing.T) {
		_, _, err := runner.SplitPersistentWorkerArguments([]string{"@params"})
		testutil.RequireEqualStatus(
			t,
			status.Error(codes.InvalidArgument, "Command line arguments of persistent worker actions must contain at least one argument that is not a flag file"),
			err,
		)
	})
}

func TestExpandFlagFileArguments(t *testing.T) {
	inputRootPath := t.TempDir()
	require.NoError(t, os.MkdirAll(filepath.Join(inputRootPath, "bazel-out"), 0o777))
	require.NoError(t, os.WriteFile(filepath.Join(inputRootPath, "bazel-out", "hello.params"), []byte("--source\nHello.java\n"), 0o666))
	require.NoError(t, os.WriteFile(filepath.Join(inputRootPath, "nested.params"), []byte("--first\n@bazel-out/hello.params\n--last\n"), 0o666))
	require.NoError(t, os.WriteFile(filepath.Join(inputRootPath, "crlf.params"), []byte("--a\r\n--b\r\n"), 0o666))
	require.NoError(t, os.WriteFile(filepath.Join(inputRootPath, "empty.params"), nil, 0o666))
	require.NoError(t, os.WriteFile(filepath.Join(inputRootPath, "blank.params"), []byte("--a\n\n--b"), 0o666))
	require.NoError(t, os.WriteFile(filepath.Join(inputRootPath, "cyclic.params"), []byte("@cyclic.params\n"), 0o666))
	require.NoError(t, os.MkdirAll(filepath.Join(inputRootPath, "sub", "dir"), 0o777))
	require.NoError(t, os.WriteFile(filepath.Join(inputRootPath, "sub", "dir", "nested.params"), []byte("--nested\n"), 0o666))

	inputRootDirectory, err := filesystem.NewLocalDirectory(path.LocalFormat.NewParser(inputRootPath))
	require.NoError(t, err)
	defer inputRootDirectory.Close()

	t.Run("Simple", func(t *testing.T) {
		arguments, err := runner.ExpandFlagFileArguments(inputRootDirectory, nil, []string{"@bazel-out/hello.params"})
		require.NoError(t, err)
		require.Equal(t, []string{"--source", "Hello.java"}, arguments)
	})

	t.Run("Nested", func(t *testing.T) {
		arguments, err := runner.ExpandFlagFileArguments(inputRootDirectory, nil, []string{"@nested.params"})
		require.NoError(t, err)
		require.Equal(t, []string{"--first", "--source", "Hello.java", "--last"}, arguments)
	})

	t.Run("CarriageReturns", func(t *testing.T) {
		arguments, err := runner.ExpandFlagFileArguments(inputRootDirectory, nil, []string{"@crlf.params"})
		require.NoError(t, err)
		require.Equal(t, []string{"--a", "--b"}, arguments)
	})

	t.Run("Empty", func(t *testing.T) {
		arguments, err := runner.ExpandFlagFileArguments(inputRootDirectory, nil, []string{"@empty.params"})
		require.NoError(t, err)
		require.Empty(t, arguments)
	})

	t.Run("BlankLinesAndMissingTrailingNewline", func(t *testing.T) {
		arguments, err := runner.ExpandFlagFileArguments(inputRootDirectory, nil, []string{"@blank.params"})
		require.NoError(t, err)
		require.Equal(t, []string{"--a", "", "--b"}, arguments)
	})

	t.Run("NotExpanded", func(t *testing.T) {
		// Arguments that are escaped using "@@", refer to
		// targets in external repositories, or that use the
		// "--flagfile=" syntax are never expanded by Bazel.
		arguments, err := runner.ExpandFlagFileArguments(inputRootDirectory, nil, []string{
			"@@bazel-out/hello.params",
			"@rules_go//go:def.bzl",
			"--flagfile=bazel-out/hello.params",
		})
		require.NoError(t, err)
		require.Equal(t, []string{
			"@@bazel-out/hello.params",
			"@rules_go//go:def.bzl",
			"--flagfile=bazel-out/hello.params",
		}, arguments)
	})

	t.Run("NonExistent", func(t *testing.T) {
		_, err := runner.ExpandFlagFileArguments(inputRootDirectory, nil, []string{"@missing.params"})
		testutil.RequirePrefixedStatus(
			t,
			status.Error(codes.Unknown, "Failed to read flag file \"missing.params\": "),
			err,
		)
	})

	t.Run("EscapingInputRoot", func(t *testing.T) {
		_, err := runner.ExpandFlagFileArguments(inputRootDirectory, nil, []string{"@../outside.params"})
		testutil.RequirePrefixedStatus(
			t,
			status.Error(codes.InvalidArgument, "Failed to read flag file \"../outside.params\": "),
			err,
		)
	})

	t.Run("WorkingDirectory", func(t *testing.T) {
		// Command line arguments of a build action are relative
		// to its working directory, meaning flag files must be
		// resolved relative to it as well.
		arguments, err := runner.ExpandFlagFileArguments(
			inputRootDirectory,
			[]path.Component{path.MustNewComponent("sub"), path.MustNewComponent("dir")},
			[]string{"@nested.params"},
		)
		require.NoError(t, err)
		require.Equal(t, []string{"--nested"}, arguments)

		// The input root remains reachable through "..", as the
		// working directory is nested inside of it.
		arguments, err = runner.ExpandFlagFileArguments(
			inputRootDirectory,
			[]path.Component{path.MustNewComponent("sub"), path.MustNewComponent("dir")},
			[]string{"@../../bazel-out/hello.params"},
		)
		require.NoError(t, err)
		require.Equal(t, []string{"--source", "Hello.java"}, arguments)

		// Paths that escape the input root must still be
		// rejected.
		_, err = runner.ExpandFlagFileArguments(
			inputRootDirectory,
			[]path.Component{path.MustNewComponent("sub"), path.MustNewComponent("dir")},
			[]string{"@../../../outside.params"},
		)
		testutil.RequirePrefixedStatus(
			t,
			status.Error(codes.InvalidArgument, "Failed to read flag file \"../../../outside.params\": "),
			err,
		)
	})

	t.Run("Cyclic", func(t *testing.T) {
		_, err := runner.ExpandFlagFileArguments(inputRootDirectory, nil, []string{"@cyclic.params"})
		testutil.RequireEqualStatus(
			t,
			status.Error(codes.InvalidArgument, "Flag files are nested more than 32 levels deep, which likely indicates a cyclic reference"),
			err,
		)
	})
}
