package runner

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestWindowsCommandLineForCmdExe(t *testing.T) {
	for _, tc := range []struct {
		name      string
		arguments []string
		cmdLine   string
		ok        bool
	}{
		{
			// The arguments that Bazel emits for a copy
			// action. The script is a single argument, and
			// the quotes around the file names inside it
			// have to survive unescaped.
			name: "BazelCopyAction",
			arguments: []string{
				"cmd.exe",
				"/S",
				"/E:ON",
				"/V:ON",
				"/D",
				"/c",
				`copy "bazel-out\x64_windows-fastbuild\bin\libfoo.a" "bazel-out\x64_windows-fastbuild\bin\FOO.a" >NUL`,
			},
			cmdLine: `cmd.exe /S /E:ON /V:ON /D /c "copy "bazel-out\x64_windows-fastbuild\bin\libfoo.a" "bazel-out\x64_windows-fastbuild\bin\FOO.a" >NUL"`,
			ok:      true,
		},
		{
			name:      "NotCmdExe",
			arguments: []string{"powershell.exe", "/S", "/c", "echo hello"},
		},
		{
			// Only the full base name may match, as
			// unrelated executables may end with "cmd.exe".
			name:      "SuffixOfCmdExe",
			arguments: []string{"mycmd.exe", "/S", "/c", "echo hello"},
		},
		{
			name:      "NoSlashC",
			arguments: []string{"cmd.exe", "/S", "/E:ON"},
		},
		{
			// Quoting is what separates the arguments that
			// follow /c, so merging them would change how
			// the invoked program sees them.
			name:      "MultipleArgumentsAfterSlashC",
			arguments: []string{"cmd.exe", "/S", "/c", "myprog", "arg with spaces"},
		},
		{
			// Without /S, cmd.exe does not simply drop the
			// first and the last quote.
			name:      "NoSlashS",
			arguments: []string{"cmd.exe", "/E:ON", "/c", "echo hello"},
		},
		{
			name:      "AbsolutePathMixedCase",
			arguments: []string{`C:\Windows\system32\CMD.EXE`, "/s", "/C", `echo "hi"`},
			cmdLine:   `C:\Windows\system32\CMD.EXE /s /C "echo "hi""`,
			ok:        true,
		},
		{
			name:      "Empty",
			arguments: []string{},
		},
		{
			// Emitting the head verbatim is only equivalent
			// to escaping it if none of it would be quoted.
			name:      "HeadNeedsEscaping",
			arguments: []string{"cmd.exe", "/S", `/K:a b`, "/c", "echo hello"},
		},
		{
			name:      "NothingAfterSlashC",
			arguments: []string{"cmd.exe", "/S", "/c"},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cmdLine, ok := windowsCommandLineForCmdExe(tc.arguments)
			require.Equal(t, tc.ok, ok)
			require.Equal(t, tc.cmdLine, cmdLine)
		})
	}
}
