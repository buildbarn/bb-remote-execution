package runner

import (
	"io"
	"strings"

	"github.com/buildbarn/bb-storage/pkg/filesystem"
	"github.com/buildbarn/bb-storage/pkg/filesystem/path"
	"github.com/buildbarn/bb-storage/pkg/util"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	// maximumFlagFileDepth is the number of levels of nested
	// '@flagfile' arguments that are expanded. Bazel does not impose
	// a limit, causing it to crash when a flag file refers to itself
	// directly or indirectly.
	maximumFlagFileDepth = 32

	// maximumFlagFileSizeBytes is the maximum size of a single flag
	// file. Flag files are read into memory in their entirety, so a
	// limit needs to be imposed to prevent the runner from running
	// out of memory when a build action provides a bogus one.
	maximumFlagFileSizeBytes = 64 * 1024 * 1024

	// persistentWorkerFlag is the command line argument that
	// instructs a tool to run as a persistent worker, meaning it
	// reads WorkRequest messages from its standard input instead of
	// performing the work described by its command line arguments.
	//
	// Bazel appends this argument itself when it runs a build action
	// locally, but the command line it sends to a remote execution
	// service is the one that would be used to run the action as a
	// regular process. It is therefore up to us to append it.
	persistentWorkerFlag = "--persistent_worker"
)

// flagFileArgumentPrefixes contains the prefixes of command line
// arguments that Bazel treats as flag files. Instead of being provided
// to the tool as regular command line arguments, these are provided to
// persistent workers as part of the WorkRequest message.
//
// This list corresponds to the FLAG_FILE_PATTERN regular expression in
// Bazel's WorkerParser.
var flagFileArgumentPrefixes = [...]string{"@", "-flagfile=", "--flagfile="}

func isFlagFileArgument(argument string) bool {
	for _, prefix := range flagFileArgumentPrefixes {
		if strings.HasPrefix(argument, prefix) && len(argument) > len(prefix) {
			return true
		}
	}
	return false
}

// isExternalRepositoryLabel returns whether a command line argument
// starting with '@' refers to a target in an external repository (e.g.,
// '@rules_go//go'), as opposed to a flag file. Bazel never expands these.
func isExternalRepositoryLabel(argument string) bool {
	return strings.HasPrefix(argument, "@") && strings.Contains(argument[1:], "//")
}

// SplitPersistentWorkerArguments splits the command line arguments of a
// build action into the arguments that are used to launch the
// persistent worker process, and the arguments that refer to flag
// files. The latter contain the work that the tool needs to perform,
// and are therefore provided to the tool as part of a WorkRequest
// message. The "--persistent_worker" flag is appended to the former, as
// that is what causes the tool to run as a persistent worker.
//
// This function mimics the behaviour of
// WorkerParser.splitSpawnArgsIntoWorkerArgsAndFlagFiles() in Bazel,
// with --experimental_worker_strict_flagfiles disabled. The
// --worker_extra_flag arguments that Bazel also appends are not
// supported, as those are a property of the client's command line that
// is not communicated to a remote execution service.
func SplitPersistentWorkerArguments(arguments []string) (workerArguments, flagFileArguments []string, err error) {
	for _, argument := range arguments {
		if isFlagFileArgument(argument) {
			flagFileArguments = append(flagFileArguments, argument)
		} else {
			workerArguments = append(workerArguments, argument)
		}
	}
	if len(workerArguments) == 0 {
		return nil, nil, status.Error(codes.InvalidArgument, "Command line arguments of persistent worker actions must contain at least one argument that is not a flag file")
	}
	if len(flagFileArguments) == 0 {
		return nil, nil, status.Error(codes.InvalidArgument, "Command line arguments of persistent worker actions must contain at least one \"@flagfile\" or \"--flagfile=\" argument")
	}
	return append(workerArguments, persistentWorkerFlag), flagFileArguments, nil
}

// splitFlagFileLines splits the contents of a flag file into individual
// command line arguments, using the same rules as Guava's
// CharSource.readLines(), which Bazel uses. Both LF and CRLF are
// treated as line separators, while a trailing line separator does not
// yield an additional empty argument.
func splitFlagFileLines(contents string) []string {
	if contents == "" {
		return nil
	}
	lines := strings.Split(contents, "\n")
	if lines[len(lines)-1] == "" {
		lines = lines[:len(lines)-1]
	}
	for i, line := range lines {
		lines[i] = strings.TrimSuffix(line, "\r")
	}
	return lines
}

// readFlagFile reads the full contents of a flag file. The path is
// resolved relative to the working directory of the build action, as
// that is the directory that command line arguments are relative to.
// Because resolution starts at the input root, ".." may still be used
// to refer to files stored above the working directory. Symbolic links
// and paths escaping the input root are not followed, as those may
// point to locations outside of the build action.
func readFlagFile(inputRootDirectory filesystem.Directory, workingDirectory []path.Component, filePath string) (string, error) {
	resolver := buildDirectoryPathResolver{
		stack: util.NewNonEmptyStack(filesystem.NopDirectoryCloser(inputRootDirectory)),
	}
	defer resolver.closeAll()
	for _, component := range workingDirectory {
		if _, err := resolver.OnDirectory(component); err != nil {
			return "", util.StatusWrapf(err, "Failed to enter working directory %#v", component.String())
		}
	}
	if err := path.Resolve(path.UNIXFormat.NewParser(filePath), path.NewRelativeScopeWalker(&resolver)); err != nil {
		return "", err
	}
	if resolver.TerminalName == nil {
		return "", status.Error(codes.InvalidArgument, "Path resolves to a directory")
	}

	f, err := resolver.stack.Peek().OpenRead(*resolver.TerminalName)
	if err != nil {
		return "", err
	}
	defer f.Close()

	sizeBytes, err := f.Len()
	if err != nil {
		return "", util.StatusWrap(err, "Failed to obtain file size")
	}
	if sizeBytes > maximumFlagFileSizeBytes {
		return "", status.Errorf(codes.InvalidArgument, "File is %d bytes in size, while a maximum of %d bytes is permitted", sizeBytes, maximumFlagFileSizeBytes)
	}
	contents := make([]byte, sizeBytes)
	if _, err := io.ReadFull(io.NewSectionReader(f, 0, sizeBytes), contents); err != nil {
		return "", util.StatusWrap(err, "Failed to read file contents")
	}
	return string(contents), nil
}

type flagFileExpander struct {
	inputRootDirectory filesystem.Directory
	workingDirectory   []path.Component
	arguments          []string
}

func (e *flagFileExpander) expand(argument string, depth int) error {
	if !strings.HasPrefix(argument, "@") ||
		strings.HasPrefix(argument, "@@") ||
		isExternalRepositoryLabel(argument) {
		// Not a flag file that Bazel would expand. Note that
		// '--flagfile=' arguments are deliberately left
		// unexpanded, as Bazel does the same.
		e.arguments = append(e.arguments, argument)
		return nil
	}
	if depth >= maximumFlagFileDepth {
		return status.Errorf(codes.InvalidArgument, "Flag files are nested more than %d levels deep, which likely indicates a cyclic reference", maximumFlagFileDepth)
	}

	filePath := argument[1:]
	contents, err := readFlagFile(e.inputRootDirectory, e.workingDirectory, filePath)
	if err != nil {
		return util.StatusWrapf(err, "Failed to read flag file %#v", filePath)
	}
	for _, line := range splitFlagFileLines(contents) {
		if err := e.expand(line, depth+1); err != nil {
			return err
		}
	}
	return nil
}

// ExpandFlagFileArguments converts the flag file arguments of a build
// action to the list of arguments that needs to be placed in the
// 'arguments' field of a WorkRequest message. Arguments of the shape
// '@filename' are replaced by the lines contained in the file, which
// are expanded recursively. Pathnames are resolved relative to the
// working directory of the build action, which is provided as a list of
// pathname components that are relative to the input root.
//
// This function mimics the behaviour of
// WorkerSpawnRunner.expandArgument() in Bazel.
func ExpandFlagFileArguments(inputRootDirectory filesystem.Directory, workingDirectory []path.Component, flagFileArguments []string) ([]string, error) {
	e := flagFileExpander{
		inputRootDirectory: inputRootDirectory,
		workingDirectory:   workingDirectory,
	}
	for _, argument := range flagFileArguments {
		if err := e.expand(argument, 0); err != nil {
			return nil, err
		}
	}
	return e.arguments, nil
}
