package runner_test

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/buildbarn/bb-remote-execution/internal/mock"
	"github.com/buildbarn/bb-remote-execution/pkg/proto/bazelworker"
	runner_pb "github.com/buildbarn/bb-remote-execution/pkg/proto/runner"
	"github.com/buildbarn/bb-remote-execution/pkg/runner"
	"github.com/buildbarn/bb-storage/pkg/clock"
	"github.com/buildbarn/bb-storage/pkg/filesystem"
	"github.com/buildbarn/bb-storage/pkg/filesystem/path"
	"github.com/buildbarn/bb-storage/pkg/testutil"
	"github.com/stretchr/testify/require"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protodelim"
	"google.golang.org/protobuf/encoding/protojson"

	"go.uber.org/mock/gomock"
)

// fakePersistentWorkerEnvironmentVariable is the name of the
// environment variable that causes this test executable to act as a
// persistent worker process, as opposed to running unit tests. This
// allows tests to launch a real worker process without depending on
// external tools.
const fakePersistentWorkerEnvironmentVariable = "BB_RUNNER_TEST_FAKE_PERSISTENT_WORKER"

// fakePersistentWorkerRootMarkerEnvironmentVariable holds the path of a
// second marker file that the fake persistent worker reads, in addition
// to the one stored in its working directory. Tests that use a nested
// working directory set it to a path that refers to the input root, so
// that both levels of the symlink farm are validated.
const fakePersistentWorkerRootMarkerEnvironmentVariable = "BB_RUNNER_TEST_ROOT_MARKER"

// fakePersistentWorkerOutputEnvironmentVariable causes the fake
// persistent worker to write output files, both directly in its working
// directory and in a directory that it creates itself.
const fakePersistentWorkerOutputEnvironmentVariable = "BB_RUNNER_TEST_OUTPUT_NAME"

// fakePersistentWorkerReport is emitted by the fake persistent worker
// as part of the 'output' field of every WorkResponse, so that tests
// can make assertions on the state of the worker process.
type fakePersistentWorkerReport struct {
	ProcessID        int      `json:"processID"`
	RequestCount     int      `json:"requestCount"`
	Arguments        []string `json:"arguments"`
	Inputs           []string `json:"inputs"`
	Marker           string   `json:"marker"`
	RootMarker       string   `json:"rootMarker"`
	StaleOutput      bool     `json:"staleOutput"`
	WorkingDirectory string   `json:"workingDirectory"`
}

func TestMain(m *testing.M) {
	if mode, ok := os.LookupEnv(fakePersistentWorkerEnvironmentVariable); ok {
		runFakePersistentWorker(mode)
		os.Exit(0)
	}
	os.Exit(m.Run())
}

// runFakePersistentWorker implements a minimal tool that speaks the
// Bazel persistent worker protocol. Its behaviour is controlled through
// a comma separated list of options that is provided through the
// environment.
func runFakePersistentWorker(mode string) {
	options := map[string]string{}
	for _, option := range strings.Split(mode, ",") {
		name, value, _ := strings.Cut(option, "=")
		options[name] = value
	}
	useJSON := options["protocol"] == "json"

	stdin := bufio.NewReader(os.Stdin)
	jsonDecoder := json.NewDecoder(stdin)
	requestCount := 0
	for {
		var request bazelworker.WorkRequest
		if useJSON {
			var message json.RawMessage
			if err := jsonDecoder.Decode(&message); err != nil {
				return
			}
			if err := protojson.Unmarshal(message, &request); err != nil {
				fmt.Fprintf(os.Stderr, "Failed to unmarshal work request: %s\n", err)
				os.Exit(1)
			}
		} else if err := (protodelim.UnmarshalOptions{MaxSize: -1}).UnmarshalFrom(stdin, &request); err != nil {
			return
		}
		requestCount++

		if _, ok := options["crash"]; ok {
			// Terminate without providing a response.
			os.Exit(3)
		}
		if _, ok := options["hang"]; ok {
			// Sleep instead of blocking indefinitely, as Go's
			// deadlock detector would terminate the process.
			time.Sleep(time.Hour)
		}

		// Read a file relative to the working directory, so
		// that tests can validate that the worker process
		// observes the input root of the build action that is
		// currently being executed.
		marker := readFakePersistentWorkerMarker("marker.txt")
		rootMarker := ""
		if rootMarkerPath, ok := os.LookupEnv(fakePersistentWorkerRootMarkerEnvironmentVariable); ok {
			rootMarker = readFakePersistentWorkerMarker(rootMarkerPath)
		}
		workingDirectory, _ := os.Getwd()

		// Emulate a tool that writes output files. Bazel places
		// these underneath "bazel-out", but a build action is
		// free to write them anywhere underneath its working
		// directory.
		staleOutput := false
		if outputName, ok := os.LookupEnv(fakePersistentWorkerOutputEnvironmentVariable); ok {
			// Output files of the previous build action must
			// not be visible.
			if _, err := os.Lstat(outputName); err == nil {
				staleOutput = true
			}
			if err := os.WriteFile(outputName, []byte(marker), 0o666); err != nil {
				fmt.Fprintf(os.Stderr, "Failed to write output file: %s\n", err)
				os.Exit(1)
			}
			if err := os.MkdirAll("generated", 0o777); err != nil {
				fmt.Fprintf(os.Stderr, "Failed to create output directory: %s\n", err)
				os.Exit(1)
			}
			if err := os.WriteFile(filepath.Join("generated", outputName), []byte(marker), 0o666); err != nil {
				fmt.Fprintf(os.Stderr, "Failed to write nested output file: %s\n", err)
				os.Exit(1)
			}
		}
		inputs := make([]string, 0, len(request.Inputs))
		for _, input := range request.Inputs {
			inputs = append(inputs, input.Path+"="+string(input.Digest))
		}
		output, err := json.Marshal(fakePersistentWorkerReport{
			ProcessID:        os.Getpid(),
			RequestCount:     requestCount,
			Arguments:        request.Arguments,
			Inputs:           inputs,
			Marker:           marker,
			RootMarker:       rootMarker,
			StaleOutput:      staleOutput,
			WorkingDirectory: workingDirectory,
		})
		if err != nil {
			panic(err)
		}

		exitCode := int32(0)
		if options["exitcode"] == "1" {
			exitCode = 1
		}
		response := &bazelworker.WorkResponse{
			ExitCode:  exitCode,
			Output:    string(output),
			RequestId: request.RequestId,
		}
		if useJSON {
			data, err := protojson.Marshal(response)
			if err != nil {
				panic(err)
			}
			os.Stdout.Write(data)
			os.Stdout.Write([]byte("\n"))
		} else if _, err := protodelim.MarshalTo(os.Stdout, response); err != nil {
			panic(err)
		}
	}
}

// readFakePersistentWorkerMarker reads a file from the file system,
// returning a placeholder in case it cannot be read.
func readFakePersistentWorkerMarker(markerPath string) string {
	contents, err := os.ReadFile(markerPath)
	if err != nil {
		return "<error: " + err.Error() + ">"
	}
	return string(contents)
}

// persistentWorkerTestEnvironment contains all of the state that is
// needed to invoke a Runner that supports persistent workers.
type persistentWorkerTestEnvironment struct {
	buildPath          string
	buildDirectory     filesystem.DirectoryCloser
	buildDirectoryPath *path.Builder
	pool               *runner.PersistentWorkerPool
	runner             runner_pb.RunnerServer
	baseRunner         *mock.MockRunnerServer
	nextActionID       int
}

func newLocalDirectoryAndPath(t *testing.T, directoryPath string) (filesystem.DirectoryCloser, *path.Builder) {
	directory, err := filesystem.NewLocalDirectory(path.LocalFormat.NewParser(directoryPath))
	require.NoError(t, err)
	t.Cleanup(func() { directory.Close() })

	builder, scopeWalker := path.EmptyBuilder.Join(path.NewAbsoluteScopeWalker(path.VoidComponentWalker))
	require.NoError(t, path.Resolve(path.LocalFormat.NewParser(directoryPath), scopeWalker))
	return directory, builder
}

func newPersistentWorkerTestEnvironment(t *testing.T, ctrl *gomock.Controller, c clock.Clock, maximumWorkerCount int, idleTimeout time.Duration) *persistentWorkerTestEnvironment {
	buildPath := t.TempDir()
	buildDirectory, buildDirectoryPath := newLocalDirectoryAndPath(t, buildPath)
	poolDirectory, poolDirectoryPath := newLocalDirectoryAndPath(t, t.TempDir())

	pool, err := runner.NewPersistentWorkerPool(
		poolDirectory,
		poolDirectoryPath,
		runner.NewPlainCommandCreator(&syscall.SysProcAttr{}),
		c,
		maximumWorkerCount,
		idleTimeout,
		/* setTmpdirEnvironmentVariable = */ true,
	)
	require.NoError(t, err)

	baseRunner := mock.NewMockRunnerServer(ctrl)
	return &persistentWorkerTestEnvironment{
		buildPath:          buildPath,
		buildDirectory:     buildDirectory,
		buildDirectoryPath: buildDirectoryPath,
		pool:               pool,
		runner:             runner.NewPersistentWorkerRunner(baseRunner, buildDirectory, buildDirectoryPath, pool),
		baseRunner:         baseRunner,
	}
}

// startReaper runs the background task of the pool, which terminates
// all remaining worker processes once the test completes.
func (e *persistentWorkerTestEnvironment) startReaper(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		e.pool.Run(ctx)
		close(done)
	}()
	t.Cleanup(func() {
		cancel()
		<-done
	})
}

// createAction creates the build directory of a single build action,
// laid out in the same way as bb_worker does, and returns a RunRequest
// that refers to it.
func (e *persistentWorkerTestEnvironment) createAction(t *testing.T, markerContents string, flagFileLines ...string) *runner_pb.RunRequest {
	return e.createActionWithWorkingDirectory(t, markerContents, "", flagFileLines...)
}

// createActionWithWorkingDirectory is identical to createAction, except
// that the build action is executed in a directory that is nested
// inside the input root.
//
// In addition to the "marker.txt" file that is stored in the input
// root, a file with the same name is stored in the working directory,
// so that tests can validate that both remain reachable.
func (e *persistentWorkerTestEnvironment) createActionWithWorkingDirectory(t *testing.T, markerContents, workingDirectory string, flagFileLines ...string) *runner_pb.RunRequest {
	e.nextActionID++
	name := fmt.Sprintf("%d", e.nextActionID)
	actionPath := filepath.Join(e.buildPath, name)
	inputRootPath := filepath.Join(actionPath, "root")
	require.NoError(t, os.MkdirAll(filepath.Join(inputRootPath, "bazel-out"), 0o777))
	require.NoError(t, os.WriteFile(filepath.Join(inputRootPath, "marker.txt"), []byte(markerContents), 0o666))
	require.NoError(t, os.WriteFile(
		filepath.Join(inputRootPath, "bazel-out", "hello.params"),
		[]byte(strings.Join(flagFileLines, "\n")+"\n"),
		0o666,
	))
	require.NoError(t, os.Mkdir(filepath.Join(actionPath, "tmp"), 0o777))

	// Paths provided by the build action are relative to its working
	// directory. Note that the environment must be identical for
	// every action, as it is part of the key under which worker
	// processes are pooled.
	environmentVariables := map[string]string{
		fakePersistentWorkerEnvironmentVariable: "protocol=proto",
	}
	flagFileArgument := "@bazel-out/hello.params"
	if workingDirectory != "" {
		workingDirectoryPath := filepath.Join(inputRootPath, filepath.FromSlash(workingDirectory))
		require.NoError(t, os.MkdirAll(workingDirectoryPath, 0o777))
		require.NoError(t, os.WriteFile(
			filepath.Join(workingDirectoryPath, "marker.txt"),
			[]byte(markerContents+"-nested"),
			0o666,
		))
		parentPath := strings.Repeat("../", strings.Count(workingDirectory, "/")+1)
		flagFileArgument = "@" + parentPath + "bazel-out/hello.params"
		environmentVariables[fakePersistentWorkerRootMarkerEnvironmentVariable] = parentPath + "marker.txt"
	}

	executable, err := os.Executable()
	require.NoError(t, err)
	return &runner_pb.RunRequest{
		Arguments: []string{
			executable,
			"--persistent_worker",
			flagFileArgument,
		},
		EnvironmentVariables: environmentVariables,
		WorkingDirectory:     workingDirectory,
		StdoutPath:           name + "/stdout",
		StderrPath:           name + "/stderr",
		InputRootDirectory:   name + "/root",
		TemporaryDirectory:   name + "/tmp",
		PersistentWorker: &runner_pb.PersistentWorker{
			Key: "tool-key",
		},
	}
}

func (e *persistentWorkerTestEnvironment) readReport(t *testing.T, request *runner_pb.RunRequest) fakePersistentWorkerReport {
	t.Helper()
	stdout, err := os.ReadFile(filepath.Join(e.buildPath, filepath.FromSlash(request.StdoutPath)))
	require.NoError(t, err)
	require.Empty(t, stdout)
	stderr, err := os.ReadFile(filepath.Join(e.buildPath, filepath.FromSlash(request.StderrPath)))
	require.NoError(t, err)
	var report fakePersistentWorkerReport
	require.NoError(t, json.Unmarshal(stderr, &report))
	return report
}

func TestPersistentWorkerRunnerPassthrough(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)
	e := newPersistentWorkerTestEnvironment(t, ctrl, clock.SystemClock, 4, time.Hour)

	// Build actions that don't provide persistent worker options
	// must be forwarded to the underlying Runner.
	request := &runner_pb.RunRequest{Arguments: []string{"cc"}}
	e.baseRunner.EXPECT().Run(ctx, testutil.EqProto(t, request)).
		Return(&runner_pb.RunResponse{ExitCode: 123}, nil)
	response, err := e.runner.Run(ctx, request)
	require.NoError(t, err)
	require.Equal(t, int64(123), response.ExitCode)

	// The same holds for readiness checks.
	e.baseRunner.EXPECT().CheckReadiness(ctx, gomock.Any()).Return(nil, status.Error(codes.Internal, "Not ready"))
	_, err = e.runner.CheckReadiness(ctx, &runner_pb.CheckReadinessRequest{Path: "hello"})
	testutil.RequireEqualStatus(t, status.Error(codes.Internal, "Not ready"), err)
}

func TestPersistentWorkerRunnerReuse(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("Persistent workers are not supported on Windows")
	}
	ctrl, ctx := gomock.WithContext(context.Background(), t)
	e := newPersistentWorkerTestEnvironment(t, ctrl, clock.SystemClock, 4, time.Hour)
	e.startReaper(t)

	// Execute a first build action. This causes a worker process to
	// be launched.
	request1 := e.createAction(t, "first", "--source", "Hello.java")
	request1.PersistentWorker.Inputs = []*bazelworker.Input{
		{Path: "Hello.java", Digest: []byte("d41d8cd98f00b204e9800998ecf8427e")},
	}
	response1, err := e.runner.Run(ctx, request1)
	require.NoError(t, err)
	require.Equal(t, int64(0), response1.ExitCode)
	report1 := e.readReport(t, request1)
	require.Equal(t, 1, report1.RequestCount)
	require.Equal(t, []string{"--source", "Hello.java"}, report1.Arguments)
	require.Equal(t, []string{"Hello.java=d41d8cd98f00b204e9800998ecf8427e"}, report1.Inputs)
	require.Equal(t, "first", report1.Marker)

	// Execute a second build action that has the same key, but a
	// different input root. It must be executed by the same worker
	// process, which must observe the contents of the new input
	// root.
	request2 := e.createAction(t, "second", "--source", "Goodbye.java")
	response2, err := e.runner.Run(ctx, request2)
	require.NoError(t, err)
	require.Equal(t, int64(0), response2.ExitCode)
	report2 := e.readReport(t, request2)
	require.Equal(t, report1.ProcessID, report2.ProcessID)
	require.Equal(t, report1.WorkingDirectory, report2.WorkingDirectory)
	require.Equal(t, 2, report2.RequestCount)
	require.Equal(t, []string{"--source", "Goodbye.java"}, report2.Arguments)
	require.Equal(t, "second", report2.Marker)
	require.Empty(t, report2.Inputs)

	// Build actions that use a different key must not be executed
	// by the same worker process.
	request3 := e.createAction(t, "third", "--source", "Other.java")
	request3.PersistentWorker.Key = "other-tool-key"
	_, err = e.runner.Run(ctx, request3)
	require.NoError(t, err)
	report3 := e.readReport(t, request3)
	require.NotEqual(t, report1.ProcessID, report3.ProcessID)
	require.Equal(t, 1, report3.RequestCount)
	require.Equal(t, "third", report3.Marker)
}

func TestPersistentWorkerRunnerWorkingDirectory(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("Persistent workers are not supported on Windows")
	}
	ctrl, ctx := gomock.WithContext(context.Background(), t)
	e := newPersistentWorkerTestEnvironment(t, ctrl, clock.SystemClock, 4, time.Hour)
	e.startReaper(t)

	// Build actions may declare a working directory that is nested
	// inside the input root. Because the working directory of a
	// running process cannot be changed, the directories along this
	// path must be preserved across build actions, while their
	// contents are replaced.
	request1 := e.createActionWithWorkingDirectory(t, "first", "sub/dir", "--source", "Hello.java")
	_, err := e.runner.Run(ctx, request1)
	require.NoError(t, err)
	report1 := e.readReport(t, request1)
	require.Equal(t, "first-nested", report1.Marker)
	require.Equal(t, "first", report1.RootMarker)
	require.Equal(t, []string{"--source", "Hello.java"}, report1.Arguments)

	// A second build action must be executed by the same process,
	// which must observe the contents of the new input root, both in
	// its working directory and in the input root above it.
	request2 := e.createActionWithWorkingDirectory(t, "second", "sub/dir", "--source", "Goodbye.java")
	_, err = e.runner.Run(ctx, request2)
	require.NoError(t, err)
	report2 := e.readReport(t, request2)
	require.Equal(t, report1.ProcessID, report2.ProcessID)
	require.Equal(t, report1.WorkingDirectory, report2.WorkingDirectory)
	require.Equal(t, 2, report2.RequestCount)
	require.Equal(t, "second-nested", report2.Marker)
	require.Equal(t, "second", report2.RootMarker)
	require.Equal(t, []string{"--source", "Goodbye.java"}, report2.Arguments)
}

func TestPersistentWorkerRunnerOutputFiles(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("Persistent workers are not supported on Windows")
	}

	// Files that a build action writes to its working directory need
	// to end up in the input root, as that is the directory from
	// which bb_worker collects output files. Because the working
	// directory of a persistent worker process is a symlink farm
	// rather than the input root itself, they need to be moved
	// there explicitly.
	for _, workingDirectory := range []string{"", "sub/dir"} {
		name := workingDirectory
		if name == "" {
			name = "InputRoot"
		}
		t.Run(name, func(t *testing.T) {
			ctrl, ctx := gomock.WithContext(context.Background(), t)
			e := newPersistentWorkerTestEnvironment(t, ctrl, clock.SystemClock, 4, time.Hour)
			e.startReaper(t)

			run := func(markerContents string) (*runner_pb.RunRequest, fakePersistentWorkerReport) {
				request := e.createActionWithWorkingDirectory(t, markerContents, workingDirectory, "--source", "Hello.java")
				request.EnvironmentVariables[fakePersistentWorkerOutputEnvironmentVariable] = "output.txt"
				_, err := e.runner.Run(ctx, request)
				require.NoError(t, err)
				return request, e.readReport(t, request)
			}
			outputDirectory := func(request *runner_pb.RunRequest) string {
				return filepath.Join(
					e.buildPath,
					filepath.FromSlash(request.InputRootDirectory),
					filepath.FromSlash(workingDirectory),
				)
			}

			request1, report1 := run("first")
			require.False(t, report1.StaleOutput)
			require.FileExists(t, filepath.Join(outputDirectory(request1), "output.txt"))
			contents, err := os.ReadFile(filepath.Join(outputDirectory(request1), "output.txt"))
			require.NoError(t, err)
			require.Equal(t, report1.Marker, string(contents))

			// Directories that the build action creates itself
			// need to be moved as well.
			contents, err = os.ReadFile(filepath.Join(outputDirectory(request1), "generated", "output.txt"))
			require.NoError(t, err)
			require.Equal(t, report1.Marker, string(contents))

			// A second build action must be executed by the same
			// worker process, and must not observe the output
			// files of the first one.
			request2, report2 := run("second")
			require.Equal(t, report1.ProcessID, report2.ProcessID)
			require.Equal(t, 2, report2.RequestCount)
			require.False(t, report2.StaleOutput)

			contents, err = os.ReadFile(filepath.Join(outputDirectory(request2), "output.txt"))
			require.NoError(t, err)
			require.Equal(t, report2.Marker, string(contents))
			contents, err = os.ReadFile(filepath.Join(outputDirectory(request2), "generated", "output.txt"))
			require.NoError(t, err)
			require.Equal(t, report2.Marker, string(contents))

			// Outputs of the first build action must be left
			// alone, as bb_worker may still be uploading them.
			contents, err = os.ReadFile(filepath.Join(outputDirectory(request1), "output.txt"))
			require.NoError(t, err)
			require.Equal(t, report1.Marker, string(contents))
		})
	}
}

func TestPersistentWorkerRunnerJSONProtocol(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("Persistent workers are not supported on Windows")
	}
	ctrl, ctx := gomock.WithContext(context.Background(), t)
	e := newPersistentWorkerTestEnvironment(t, ctrl, clock.SystemClock, 4, time.Hour)
	e.startReaper(t)

	request := e.createAction(t, "json", "--source", "Hello.java")
	request.EnvironmentVariables[fakePersistentWorkerEnvironmentVariable] = "protocol=json"
	request.PersistentWorker.Protocol = runner_pb.PersistentWorker_JSON
	response, err := e.runner.Run(ctx, request)
	require.NoError(t, err)
	require.Equal(t, int64(0), response.ExitCode)
	report := e.readReport(t, request)
	require.Equal(t, []string{"--source", "Hello.java"}, report.Arguments)
	require.Equal(t, "json", report.Marker)
}

func TestPersistentWorkerRunnerNonZeroExitCode(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("Persistent workers are not supported on Windows")
	}
	ctrl, ctx := gomock.WithContext(context.Background(), t)
	e := newPersistentWorkerTestEnvironment(t, ctrl, clock.SystemClock, 4, time.Hour)
	e.startReaper(t)

	request := e.createAction(t, "failure", "--source", "Broken.java")
	request.EnvironmentVariables[fakePersistentWorkerEnvironmentVariable] = "protocol=proto,exitcode=1"
	response, err := e.runner.Run(ctx, request)
	require.NoError(t, err)
	require.Equal(t, int64(1), response.ExitCode)
	require.Equal(t, "failure", e.readReport(t, request).Marker)
}

func TestPersistentWorkerRunnerCrash(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("Persistent workers are not supported on Windows")
	}
	ctrl, ctx := gomock.WithContext(context.Background(), t)
	e := newPersistentWorkerTestEnvironment(t, ctrl, clock.SystemClock, 4, time.Hour)
	e.startReaper(t)

	// A worker process that terminates without providing a work
	// response should cause the build action to fail.
	request1 := e.createAction(t, "crash", "--source", "Hello.java")
	request1.EnvironmentVariables[fakePersistentWorkerEnvironmentVariable] = "protocol=proto,crash"
	_, err := e.runner.Run(ctx, request1)
	testutil.RequirePrefixedStatus(
		t,
		status.Error(codes.Internal, "Failed to receive work response from persistent worker: "),
		err,
	)

	// The crashed worker process must not be reused. A subsequent
	// build action must be executed by a new process.
	request2 := e.createAction(t, "recovered", "--source", "Hello.java")
	_, err = e.runner.Run(ctx, request2)
	require.NoError(t, err)
	require.Equal(t, "recovered", e.readReport(t, request2).Marker)
}

func TestPersistentWorkerRunnerTimeout(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("Persistent workers are not supported on Windows")
	}
	ctrl, _ := gomock.WithContext(context.Background(), t)
	e := newPersistentWorkerTestEnvironment(t, ctrl, clock.SystemClock, 4, time.Hour)
	e.startReaper(t)

	// Build actions that exceed their execution timeout must be
	// reported as such, and must cause the worker process to be
	// terminated.
	ctxWithTimeout, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	request1 := e.createAction(t, "hang", "--source", "Hello.java")
	request1.EnvironmentVariables[fakePersistentWorkerEnvironmentVariable] = "protocol=proto,hang"
	_, err := e.runner.Run(ctxWithTimeout, request1)
	testutil.RequirePrefixedStatus(
		t,
		status.Error(codes.DeadlineExceeded, "Persistent worker did not return a work response: "),
		err,
	)

	request2 := e.createAction(t, "recovered", "--source", "Hello.java")
	_, err = e.runner.Run(context.Background(), request2)
	require.NoError(t, err)
	require.Equal(t, "recovered", e.readReport(t, request2).Marker)
}

func TestPersistentWorkerRunnerInvalidRequests(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)
	e := newPersistentWorkerTestEnvironment(t, ctrl, clock.SystemClock, 4, time.Hour)

	t.Run("NoKey", func(t *testing.T) {
		_, err := e.runner.Run(ctx, &runner_pb.RunRequest{
			Arguments:        []string{"tool", "@params"},
			PersistentWorker: &runner_pb.PersistentWorker{},
		})
		testutil.RequireEqualStatus(t, status.Error(codes.InvalidArgument, "Persistent worker options do not contain a key"), err)
	})

	t.Run("NoFlagFile", func(t *testing.T) {
		_, err := e.runner.Run(ctx, &runner_pb.RunRequest{
			Arguments: []string{"tool"},
			PersistentWorker: &runner_pb.PersistentWorker{
				Key: "tool-key",
			},
		})
		testutil.RequireEqualStatus(
			t,
			status.Error(codes.InvalidArgument, "Command line arguments of persistent worker actions must contain at least one \"@flagfile\" or \"--flagfile=\" argument"),
			err,
		)
	})

	t.Run("NonExistentInputRoot", func(t *testing.T) {
		_, err := e.runner.Run(ctx, &runner_pb.RunRequest{
			Arguments:          []string{"tool", "@params"},
			InputRootDirectory: "nonexistent/root",
			PersistentWorker: &runner_pb.PersistentWorker{
				Key: "tool-key",
			},
		})
		testutil.RequirePrefixedStatus(
			t,
			status.Error(codes.Unknown, "Failed to enter input root directory \"nonexistent/root\": "),
			err,
		)
	})
}

func TestPersistentWorkerPoolEviction(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("Persistent workers are not supported on Windows")
	}
	ctrl, ctx := gomock.WithContext(context.Background(), t)
	e := newPersistentWorkerTestEnvironment(t, ctrl, clock.SystemClock, 1, time.Hour)
	e.startReaper(t)

	// Only a single worker process may be retained. Alternating
	// between two keys must therefore cause a new worker process to
	// be launched for every build action.
	requestA1 := e.createAction(t, "a1", "--source", "A.java")
	requestA1.PersistentWorker.Key = "a"
	_, err := e.runner.Run(ctx, requestA1)
	require.NoError(t, err)
	reportA1 := e.readReport(t, requestA1)

	requestB := e.createAction(t, "b", "--source", "B.java")
	requestB.PersistentWorker.Key = "b"
	_, err = e.runner.Run(ctx, requestB)
	require.NoError(t, err)

	requestA2 := e.createAction(t, "a2", "--source", "A.java")
	requestA2.PersistentWorker.Key = "a"
	_, err = e.runner.Run(ctx, requestA2)
	require.NoError(t, err)
	reportA2 := e.readReport(t, requestA2)
	require.NotEqual(t, reportA1.ProcessID, reportA2.ProcessID)
	require.Equal(t, 1, reportA2.RequestCount)
}

func TestPersistentWorkerPoolIdleTimeout(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("Persistent workers are not supported on Windows")
	}
	ctrl, ctx := gomock.WithContext(context.Background(), t)
	clk := mock.NewMockClock(ctrl)
	baseTime := time.Unix(1000, 0)
	var currentTimeNanos atomic.Int64
	currentTimeNanos.Store(baseTime.UnixNano())
	clk.EXPECT().Now().DoAndReturn(func() time.Time {
		return time.Unix(0, currentTimeNanos.Load())
	}).AnyTimes()
	e := newPersistentWorkerTestEnvironment(t, ctrl, clk, 4, time.Minute)

	request1 := e.createAction(t, "before", "--source", "Hello.java")
	_, err := e.runner.Run(ctx, request1)
	require.NoError(t, err)
	report1 := e.readReport(t, request1)

	// Start the background task that expires idle worker processes.
	timerChannel := make(chan time.Time, 1)
	timerRearmed := make(chan struct{})
	timer := mock.NewMockTimer(ctrl)
	firstTimer := clk.EXPECT().NewTimer(time.Minute).Return(timer, timerChannel)
	clk.EXPECT().NewTimer(time.Minute).After(firstTimer).DoAndReturn(
		func(d time.Duration) (clock.Timer, <-chan time.Time) {
			close(timerRearmed)
			return timer, make(chan time.Time)
		},
	)
	timer.EXPECT().Stop().Return(true).AnyTimes()

	runnerContext, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		e.pool.Run(runnerContext)
		close(done)
	}()
	defer func() {
		cancel()
		<-done
	}()

	// Advance the clock beyond the idle timeout and let the timer
	// fire, causing the idle worker process to be terminated.
	currentTimeNanos.Store(baseTime.Add(2 * time.Minute).UnixNano())
	timerChannel <- baseTime
	<-timerRearmed

	request2 := e.createAction(t, "after", "--source", "Hello.java")
	_, err = e.runner.Run(ctx, request2)
	require.NoError(t, err)
	report2 := e.readReport(t, request2)
	require.NotEqual(t, report1.ProcessID, report2.ProcessID)
}
