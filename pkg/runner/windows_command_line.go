package runner

import (
	"strings"
)

// isWindowsCmdExe returns whether argv[0] refers to the Windows command
// interpreter. The base name is compared in full, as a suffix match
// would also accept unrelated executables such as "mycmd.exe". The
// separators of the Windows path format are honoured explicitly instead
// of going through the filepath package, so that the outcome does not
// depend on the platform this code is compiled for.
func isWindowsCmdExe(argv0 string) bool {
	baseName := argv0[strings.LastIndexAny(argv0, `\/:`)+1:]
	return strings.EqualFold(baseName, "cmd.exe")
}

// windowsArgumentNeedsEscaping returns whether syscall.EscapeArg() would
// alter an argument. Arguments that it leaves alone may be emitted
// verbatim, which keeps this file free of Windows only dependencies.
// Backslashes do not count: EscapeArg() only doubles those in front of a
// quote, or at the end of an argument that it decided to surround with
// quotes.
func windowsArgumentNeedsEscaping(argument string) bool {
	return argument == "" || strings.ContainsAny(argument, " \t\"")
}

// windowsCommandLineForCmdExe converts the arguments of an action into a
// raw command line to be placed in SysProcAttr.CmdLine.
//
// Go turns arguments into a command line using the quoting rules of
// CommandLineToArgvW(), which is how nearly every Windows program splits
// its command line back up. That escaping is therefore right by default
// and must be left in place. cmd.exe is the documented exception: given
// /S it removes the first and the last quote of the string following /c
// and hands over everything in between untouched. A double quote inside
// that string consequently reaches cmd.exe with the backslash that Go
// inserted in front of it, which ends up in file names.
//
// Rewriting is only sound for arguments having exactly that shape, so
// the second return value reports whether the command line is usable.
// If it is not, the escaping performed by Go has to be relied upon.
func windowsCommandLineForCmdExe(arguments []string) (string, bool) {
	if len(arguments) == 0 || !isWindowsCmdExe(arguments[0]) {
		return "", false
	}
	for i, argument := range arguments {
		if !strings.EqualFold(argument, "/c") {
			continue
		}

		// Unconditional stripping of the surrounding quotes is
		// something the caller has to request with /S. Other
		// invocations are left to Go, as cmd.exe would apply a
		// different rule to the string constructed here.
		hasSlashS := false
		for _, headArgument := range arguments[1:i] {
			if strings.EqualFold(headArgument, "/S") {
				hasSlashS = true
			}
		}
		if !hasSlashS {
			return "", false
		}

		// Merging into a single quoted string may only happen if
		// there is nothing to merge. Where cmd.exe receives more
		// than one argument after /c, the boundaries between
		// them are conveyed by the very quoting that would be
		// dropped here, turning one argument of the invoked
		// program into several.
		if i+2 != len(arguments) {
			return "", false
		}

		head := arguments[:i+1]
		for _, headArgument := range head {
			if windowsArgumentNeedsEscaping(headArgument) {
				return "", false
			}
		}
		return strings.Join(head, " ") + ` "` + arguments[i+1] + `"`, true
	}
	return "", false
}
