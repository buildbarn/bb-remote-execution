package runner

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"syscall"
	"testing"

	"github.com/buildbarn/bb-storage/pkg/filesystem"
	"github.com/buildbarn/bb-storage/pkg/filesystem/path"
	"github.com/buildbarn/bb-storage/pkg/testutil"
	"github.com/stretchr/testify/require"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// flattenToolInputTree renders a tree as a sorted list of paths, where
// directories that are merely traversed are suffixed with a slash. This
// distinguishes them from the entries that are materialized in full.
func flattenToolInputTree(t *toolInputTree, prefix string) []string {
	var paths []string
	for name := range t.entries {
		paths = append(paths, prefix+name.String())
	}
	for name, child := range t.directories {
		paths = append(paths, prefix+name.String()+"/")
		paths = append(paths, flattenToolInputTree(child, prefix+name.String()+"/")...)
	}
	sort.Strings(paths)
	return paths
}

func TestParseToolInputPaths(t *testing.T) {
	t.Run("Empty", func(t *testing.T) {
		tree, err := parseToolInputPaths(nil, nil)
		require.NoError(t, err)
		require.True(t, tree.isEmpty())
	})

	t.Run("Nested", func(t *testing.T) {
		tree, err := parseToolInputPaths([]string{"a/b/c", "a/d", "e"}, nil)
		require.NoError(t, err)
		require.Equal(t, []string{"a/", "a/b/", "a/b/c", "a/d", "e"}, flattenToolInputTree(tree, ""))
	})

	t.Run("AncestorAlreadyMaterialized", func(t *testing.T) {
		// A path stored underneath an entry that is already
		// materialized in its entirety adds nothing.
		tree, err := parseToolInputPaths([]string{"a", "a/b/c"}, nil)
		require.NoError(t, err)
		require.Equal(t, []string{"a"}, flattenToolInputTree(tree, ""))
	})

	t.Run("DescendantSeenFirst", func(t *testing.T) {
		// The opposite order must keep descending into the
		// directory, which is the more conservative of the two.
		tree, err := parseToolInputPaths([]string{"a/b/c", "a"}, nil)
		require.NoError(t, err)
		require.Equal(t, []string{"a/", "a/b/", "a/b/c"}, flattenToolInputTree(tree, ""))
	})

	t.Run("InputRootItself", func(t *testing.T) {
		_, err := parseToolInputPaths([]string{"."}, nil)
		testutil.RequireEqualStatus(t, status.Error(codes.InvalidArgument, "Tool input path \".\" refers to the input root itself"), err)
	})

	t.Run("EscapesInputRoot", func(t *testing.T) {
		_, err := parseToolInputPaths([]string{"../a"}, nil)
		testutil.RequireEqualStatus(t, status.Error(codes.InvalidArgument, "Failed to resolve tool input path \"../a\": Path resolves to a location outside the input root directory"), err)
	})

	t.Run("WorkingDirectory", func(t *testing.T) {
		workingDirectory := []path.Component{
			path.MustNewComponent("a"),
			path.MustNewComponent("b"),
		}
		// An ancestor of the working directory may not be
		// materialized, as its contents change between actions.
		_, err := parseToolInputPaths([]string{"a"}, workingDirectory)
		testutil.RequireEqualStatus(t, status.Error(codes.InvalidArgument, "Tool input path \"a\" contains the working directory of the build action"), err)
		_, err = parseToolInputPaths([]string{"a/b"}, workingDirectory)
		testutil.RequireEqualStatus(t, status.Error(codes.InvalidArgument, "Tool input path \"a/b\" contains the working directory of the build action"), err)
		// Siblings and descendants are fine.
		_, err = parseToolInputPaths([]string{"a/c", "a/b/c"}, workingDirectory)
		require.NoError(t, err)
	})
}

// toolInputsTestDirectories creates an execution root and an input root
// on the local file system, returning handles to both.
func toolInputsTestDirectories(t *testing.T) (filesystem.Directory, string, filesystem.Directory, string) {
	execRootPath := t.TempDir()
	execRoot, err := filesystem.NewLocalDirectory(path.LocalFormat.NewParser(execRootPath))
	require.NoError(t, err)
	t.Cleanup(func() { execRoot.Close() })

	inputRootPath := t.TempDir()
	inputRoot, err := filesystem.NewLocalDirectory(path.LocalFormat.NewParser(inputRootPath))
	require.NoError(t, err)
	t.Cleanup(func() { inputRoot.Close() })

	return execRoot, execRootPath, inputRoot, inputRootPath
}

func TestMaterializeToolInputs(t *testing.T) {
	execRoot, execRootPath, inputRoot, inputRootPath := toolInputsTestDirectories(t)

	require.NoError(t, os.MkdirAll(filepath.Join(inputRootPath, "tools", "bin"), 0o777))
	require.NoError(t, os.WriteFile(filepath.Join(inputRootPath, "tools", "bin", "tool"), []byte("#!/bin/sh\n"), 0o777))
	require.NoError(t, os.WriteFile(filepath.Join(inputRootPath, "tools", "data.txt"), []byte("data"), 0o666))
	require.NoError(t, os.Symlink("data.txt", filepath.Join(inputRootPath, "tools", "link")))
	require.NoError(t, os.WriteFile(filepath.Join(inputRootPath, "other.txt"), []byte("other"), 0o666))

	tree, err := parseToolInputPaths([]string{"tools"}, nil)
	require.NoError(t, err)
	require.NoError(t, materializeToolInputs(execRoot, inputRoot, tree))

	// The tool must be a real copy, not a symbolic link, so that it
	// survives the removal of the input root.
	fileInfo, err := os.Lstat(filepath.Join(execRootPath, "tools", "bin", "tool"))
	require.NoError(t, err)
	require.True(t, fileInfo.Mode().IsRegular())
	require.NotEqual(t, os.FileMode(0), fileInfo.Mode().Perm()&0o111)

	contents, err := os.ReadFile(filepath.Join(execRootPath, "tools", "data.txt"))
	require.NoError(t, err)
	require.Equal(t, "data", string(contents))

	// Symbolic links stored inside the tool are recreated verbatim.
	target, err := os.Readlink(filepath.Join(execRootPath, "tools", "link"))
	require.NoError(t, err)
	require.Equal(t, "data.txt", target)

	// Nothing outside the tool may be materialized.
	_, err = os.Lstat(filepath.Join(execRootPath, "other.txt"))
	require.True(t, os.IsNotExist(err))

	// The materialized tool must outlive the input root.
	require.NoError(t, os.RemoveAll(inputRootPath))
	contents, err = os.ReadFile(filepath.Join(execRootPath, "tools", "bin", "tool"))
	require.NoError(t, err)
	require.Equal(t, "#!/bin/sh\n", string(contents))
}

func TestRefreshSymlinkFarmToolInputs(t *testing.T) {
	execRoot, execRootPath, inputRoot, inputRootPath := toolInputsTestDirectories(t)

	// A directory holding both a tool input and a regular input.
	require.NoError(t, os.MkdirAll(filepath.Join(inputRootPath, "tools"), 0o777))
	require.NoError(t, os.WriteFile(filepath.Join(inputRootPath, "tools", "tool"), []byte("tool"), 0o777))
	require.NoError(t, os.WriteFile(filepath.Join(inputRootPath, "tools", "input.txt"), []byte("first"), 0o666))
	require.NoError(t, os.WriteFile(filepath.Join(inputRootPath, "top.txt"), []byte("first"), 0o666))

	inputRootBuilder, scopeWalker := path.EmptyBuilder.Join(path.NewAbsoluteScopeWalker(path.VoidComponentWalker))
	require.NoError(t, path.Resolve(path.LocalFormat.NewParser(inputRootPath), scopeWalker))

	tree, err := parseToolInputPaths([]string{"tools/tool"}, nil)
	require.NoError(t, err)
	require.NoError(t, materializeToolInputs(execRoot, inputRoot, tree))
	require.NoError(t, refreshSymlinkFarm(execRoot, inputRoot, inputRootBuilder, nil, tree))

	// The tool is a real file; everything else is a symbolic link
	// that points into the input root.
	fileInfo, err := os.Lstat(filepath.Join(execRootPath, "tools", "tool"))
	require.NoError(t, err)
	require.True(t, fileInfo.Mode().IsRegular())

	fileInfo, err = os.Lstat(filepath.Join(execRootPath, "tools", "input.txt"))
	require.NoError(t, err)
	require.Equal(t, os.ModeSymlink, fileInfo.Mode()&os.ModeSymlink)

	contents, err := os.ReadFile(filepath.Join(execRootPath, "tools", "input.txt"))
	require.NoError(t, err)
	require.Equal(t, "first", string(contents))

	// Refreshing against a second input root must leave the tool
	// alone, while repointing everything else.
	execRoot2, execRootPath2, _, _ := toolInputsTestDirectories(t)
	_ = execRoot2
	_ = execRootPath2

	secondInputRootPath := t.TempDir()
	secondInputRoot, err := filesystem.NewLocalDirectory(path.LocalFormat.NewParser(secondInputRootPath))
	require.NoError(t, err)
	defer secondInputRoot.Close()
	require.NoError(t, os.MkdirAll(filepath.Join(secondInputRootPath, "tools"), 0o777))
	require.NoError(t, os.WriteFile(filepath.Join(secondInputRootPath, "tools", "input.txt"), []byte("second"), 0o666))
	require.NoError(t, os.WriteFile(filepath.Join(secondInputRootPath, "top.txt"), []byte("second"), 0o666))

	secondBuilder, scopeWalker := path.EmptyBuilder.Join(path.NewAbsoluteScopeWalker(path.VoidComponentWalker))
	require.NoError(t, path.Resolve(path.LocalFormat.NewParser(secondInputRootPath), scopeWalker))

	require.NoError(t, os.RemoveAll(inputRootPath))
	require.NoError(t, refreshSymlinkFarm(execRoot, secondInputRoot, secondBuilder, nil, tree))

	contents, err = os.ReadFile(filepath.Join(execRootPath, "tools", "tool"))
	require.NoError(t, err)
	require.Equal(t, "tool", string(contents))

	contents, err = os.ReadFile(filepath.Join(execRootPath, "tools", "input.txt"))
	require.NoError(t, err)
	require.Equal(t, "second", string(contents))

	contents, err = os.ReadFile(filepath.Join(execRootPath, "top.txt"))
	require.NoError(t, err)
	require.Equal(t, "second", string(contents))
}

// crossDeviceDirectory is a decorator for filesystem.Directory that
// makes renaming fail in the same way it does when the directory of
// persistent worker processes and the build directory are backed by
// different file systems. Production deployments place the former on
// local disk and provide the latter through a virtual file system, so
// this is the common case rather than an exotic one.
type crossDeviceDirectory struct {
	filesystem.Directory
}

func (crossDeviceDirectory) Rename(oldName path.Component, newDirectory filesystem.Directory, newName path.Component) error {
	return syscall.EXDEV
}

func TestIsCrossDevice(t *testing.T) {
	// filesystem.Directory.Rename() reports the error returned by
	// renameat() verbatim, both for a genuine cross-device rename
	// and for a rename between two different implementations of
	// filesystem.Directory.
	require.True(t, isCrossDevice(syscall.EXDEV))
	require.True(t, isCrossDevice(fmt.Errorf("renameat: %w", syscall.EXDEV)))
	require.False(t, isCrossDevice(syscall.ENOENT))
	require.False(t, isCrossDevice(nil))
}

func TestMoveIntoInputRootCrossDevice(t *testing.T) {
	run := func(t *testing.T, prepare func(execRootPath, inputRootPath string), name string) (filesystem.Directory, string, string) {
		execRoot, execRootPath, inputRoot, inputRootPath := toolInputsTestDirectories(t)
		prepare(execRootPath, inputRootPath)
		require.NoError(t, moveIntoInputRoot(crossDeviceDirectory{execRoot}, inputRoot, path.MustNewComponent(name)))
		return execRoot, execRootPath, inputRootPath
	}

	t.Run("RegularFile", func(t *testing.T) {
		_, execRootPath, inputRootPath := run(t, func(execRootPath, inputRootPath string) {
			require.NoError(t, os.WriteFile(filepath.Join(execRootPath, "output.txt"), []byte("contents"), 0o666))
		}, "output.txt")

		contents, err := os.ReadFile(filepath.Join(inputRootPath, "output.txt"))
		require.NoError(t, err)
		require.Equal(t, "contents", string(contents))

		// Moving implies that the original is gone.
		_, err = os.Lstat(filepath.Join(execRootPath, "output.txt"))
		require.True(t, os.IsNotExist(err))
	})

	t.Run("ExecutableBitPreserved", func(t *testing.T) {
		_, _, inputRootPath := run(t, func(execRootPath, inputRootPath string) {
			require.NoError(t, os.WriteFile(filepath.Join(execRootPath, "output.sh"), []byte("#!/bin/sh\n"), 0o777))
		}, "output.sh")

		fileInfo, err := os.Lstat(filepath.Join(inputRootPath, "output.sh"))
		require.NoError(t, err)
		require.NotEqual(t, os.FileMode(0), fileInfo.Mode().Perm()&0o111)
	})

	t.Run("Directory", func(t *testing.T) {
		_, _, inputRootPath := run(t, func(execRootPath, inputRootPath string) {
			require.NoError(t, os.MkdirAll(filepath.Join(execRootPath, "generated", "nested"), 0o777))
			require.NoError(t, os.WriteFile(filepath.Join(execRootPath, "generated", "nested", "output.txt"), []byte("nested"), 0o666))
			require.NoError(t, os.Symlink("nested/output.txt", filepath.Join(execRootPath, "generated", "link")))
		}, "generated")

		contents, err := os.ReadFile(filepath.Join(inputRootPath, "generated", "nested", "output.txt"))
		require.NoError(t, err)
		require.Equal(t, "nested", string(contents))

		target, err := os.Readlink(filepath.Join(inputRootPath, "generated", "link"))
		require.NoError(t, err)
		require.Equal(t, "nested/output.txt", target)
	})

	t.Run("DestinationExists", func(t *testing.T) {
		// Whatever the build action produced takes precedence
		// over what the input root already held.
		_, _, inputRootPath := run(t, func(execRootPath, inputRootPath string) {
			require.NoError(t, os.MkdirAll(filepath.Join(inputRootPath, "generated"), 0o777))
			require.NoError(t, os.WriteFile(filepath.Join(inputRootPath, "generated", "stale.txt"), []byte("stale"), 0o666))
			require.NoError(t, os.MkdirAll(filepath.Join(execRootPath, "generated"), 0o777))
			require.NoError(t, os.WriteFile(filepath.Join(execRootPath, "generated", "fresh.txt"), []byte("fresh"), 0o666))
		}, "generated")

		contents, err := os.ReadFile(filepath.Join(inputRootPath, "generated", "fresh.txt"))
		require.NoError(t, err)
		require.Equal(t, "fresh", string(contents))

		_, err = os.Lstat(filepath.Join(inputRootPath, "generated", "stale.txt"))
		require.True(t, os.IsNotExist(err))
	})
}
