package runner

import (
	"context"
	"errors"
	"io"
	"log"
	"sync"
	"time"

	"github.com/buildbarn/bb-remote-execution/pkg/cleaner"
	runner_pb "github.com/buildbarn/bb-remote-execution/pkg/proto/runner"
	"github.com/buildbarn/bb-storage/pkg/filesystem"
	"github.com/buildbarn/bb-storage/pkg/filesystem/path"
	"github.com/buildbarn/bb-storage/pkg/util"
	"github.com/google/uuid"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
)

type persistentRunnerSession struct {
	process *PersistentWorkerProcess
	logExit func()
	busy    bool
	closing bool
	done    chan struct{}
	err     error
}

// PersistentRunner implements the runner session service using retained
// compiler processes on the local system.
type PersistentRunner struct {
	localRunner                  *localRunner
	lifetimeContext              context.Context
	cancel                       context.CancelFunc
	maximumWorkResponseSizeBytes uint64
	idleInvoker                  *cleaner.IdleInvoker
	lock                         sync.Mutex
	sessions                     map[string]*persistentRunnerSession
	closed                       bool
	operations                   sync.WaitGroup
	closeOnce                    sync.Once
	closeError                   error
}

// NewPersistentRunner creates a runner that retains compiler processes
// across execution requests. The provided context controls the lifetime
// of the runner, not an individual request. Cancelling it initiates
// shutdown. Close waits for shutdown to complete.
//
// The caller retains ownership of the build directory and must close
// the runner before closing the directory. When provided, idleInvoker
// must also be used by ordinary execution on this runner. Each session
// holds a reference until its process has been reaped.
func NewPersistentRunner(ctx context.Context, buildDirectory filesystem.Directory, buildDirectoryPath *path.Builder, commandCreator CommandCreator, setTmpdirEnvironmentVariable bool, maximumWorkResponseSizeBytes uint64, idleInvoker *cleaner.IdleInvoker) *PersistentRunner {
	lifetimeContext, cancel := context.WithCancel(ctx)
	server := &PersistentRunner{
		localRunner: &localRunner{
			buildDirectory:               buildDirectory,
			buildDirectoryPath:           buildDirectoryPath,
			commandCreator:               commandCreator,
			setTmpdirEnvironmentVariable: setTmpdirEnvironmentVariable,
		},
		lifetimeContext:              lifetimeContext,
		cancel:                       cancel,
		maximumWorkResponseSizeBytes: maximumWorkResponseSizeBytes,
		idleInvoker:                  idleInvoker,
		sessions:                     map[string]*persistentRunnerSession{},
	}
	context.AfterFunc(lifetimeContext, func() { server.Close() })
	return server
}

// CheckReadiness checks that the runner is accepting sessions.
func (server *PersistentRunner) CheckReadiness(ctx context.Context, request *runner_pb.CheckReadinessRequest) (response *emptypb.Empty, returnError error) {
	if server.lifetimeContext.Err() != nil {
		return nil, status.Error(codes.Unavailable, "Persistent runner is shutting down")
	}
	if server.idleInvoker != nil {
		if err := server.idleInvoker.Acquire(ctx); err != nil {
			return nil, err
		}
		defer func() { returnError = errors.Join(returnError, server.releaseCleaner()) }()
	}
	return server.localRunner.CheckReadiness(ctx, request)
}

// CreateSession starts a compiler process and returns its session ID.
// The request context can cancel startup, but a successfully created
// session remains alive independently of that context. Confirmed startup
// failures carry a CreateSessionFailure detail once cleanup has completed.
func (server *PersistentRunner) CreateSession(ctx context.Context, request *runner_pb.CreateSessionRequest) (response *runner_pb.CreateSessionResponse, returnError error) {
	creationStopped := true
	defer func() {
		if returnError != nil && creationStopped {
			returnError = confirmedSessionCreationFailure(returnError)
		}
	}()
	if err := ctx.Err(); err != nil {
		return nil, status.FromContextError(err).Err()
	}
	server.lock.Lock()
	if server.closed {
		server.lock.Unlock()
		return nil, status.Error(codes.Unavailable, "Persistent runner is shutting down")
	}
	server.operations.Add(1)
	server.lock.Unlock()
	defer server.operations.Done()

	sessionContext, cancelSession := context.WithCancel(server.lifetimeContext)
	retained := false
	defer func() {
		if !retained {
			cancelSession()
		}
	}()
	stopCancellation := context.AfterFunc(ctx, cancelSession)
	defer stopCancellation()
	if server.idleInvoker != nil {
		if err := server.idleInvoker.Acquire(sessionContext); err != nil {
			return nil, err
		}
		defer func() {
			if !retained {
				returnError = errors.Join(returnError, server.releaseCleaner())
			}
		}()
	}
	command, err := server.localRunner.createCommand(sessionContext, &runner_pb.RunRequest{
		Arguments:            request.GetArguments(),
		EnvironmentVariables: request.GetEnvironmentVariables(),
		WorkingDirectory:     request.GetWorkingDirectory(),
		InputRootDirectory:   request.GetInputRootDirectory(),
		TemporaryDirectory:   request.GetTemporaryDirectory(),
	})
	if err != nil {
		return nil, err
	}
	stderr, err := server.localRunner.openLog(request.GetProcessStderrPath())
	if err != nil {
		return nil, util.StatusWrap(err, "Failed to open persistent worker stderr")
	}
	command.Stderr = stderr
	process, err := StartPersistentWorkerProcess(command, server.maximumWorkResponseSizeBytes)
	stderr.Close()
	if err != nil {
		return nil, util.StatusWrapWithCode(err, codes.Internal, "Failed to start persistent worker")
	}
	creationStopped = false
	defer func() {
		if !retained {
			err := process.Close()
			creationStopped = err == nil
			returnError = errors.Join(returnError, err)
			if creationStopped {
				server.logWorkerExit(process, request)
			}
		}
	}()
	if !stopCancellation() || ctx.Err() != nil {
		return nil, status.FromContextError(ctx.Err()).Err()
	}
	sessionID, err := uuid.NewRandom()
	if err != nil {
		return nil, util.StatusWrapWithCode(err, codes.Internal, "Failed to generate session ID")
	}
	server.lock.Lock()
	defer server.lock.Unlock()
	if server.closed || server.lifetimeContext.Err() != nil {
		return nil, status.Error(codes.Unavailable, "Persistent runner is shutting down")
	}
	select {
	case <-process.Done():
		return nil, status.Errorf(codes.Unavailable, "Persistent worker exited during startup: %v", process.Wait())
	default:
	}
	session := &persistentRunnerSession{
		process: process,
		logExit: sync.OnceFunc(func() { server.logWorkerExit(process, request) }),
		done:    make(chan struct{}),
	}
	server.sessions[sessionID.String()] = session
	retained = true
	server.operations.Add(1)
	go func() {
		defer server.operations.Done()
		<-process.Done()
		cancelSession()
		server.lock.Lock()
		expectedExit := session.closing || server.closed
		server.lock.Unlock()
		if !expectedExit {
			session.logExit()
		}
		session.err = server.releaseCleaner()
		server.lock.Lock()
		delete(server.sessions, sessionID.String())
		server.closeError = errors.Join(server.closeError, session.err)
		server.lock.Unlock()
		close(session.done)
	}()
	return &runner_pb.CreateSessionResponse{SessionId: sessionID.String()}, nil
}

func confirmedSessionCreationFailure(err error) error {
	creationStatus, detailError := status.Convert(err).WithDetails(&runner_pb.CreateSessionFailure{})
	if detailError != nil {
		return err
	}
	return creationStatus.Err()
}

func (server *PersistentRunner) logWorkerExit(process *PersistentWorkerProcess, request *runner_pb.CreateSessionRequest) {
	exitStatus := "exit status 0"
	if err := process.Wait(); err != nil {
		exitStatus = err.Error()
	}
	tail, err := server.readStderrTail(request.GetProcessStderrPath())
	log.Printf("Persistent worker exited: executable=%q input_root=%q exit_status=%q stderr_tail=%q stderr_read_error=%v", request.Arguments[0], request.InputRootDirectory, exitStatus, tail, err)
}

func (server *PersistentRunner) readStderrTail(logPath string) ([]byte, error) {
	resolver := buildDirectoryPathResolver{
		stack: util.NewNonEmptyStack(filesystem.NopDirectoryCloser(server.localRunner.buildDirectory)),
	}
	defer resolver.closeAll()
	if err := path.Resolve(path.UNIXFormat.NewParser(logPath), path.NewRelativeScopeWalker(&resolver)); err != nil {
		return nil, err
	}
	if resolver.TerminalName == nil {
		return nil, status.Error(codes.InvalidArgument, "Path resolves to a directory")
	}
	file, err := resolver.stack.Peek().OpenRead(*resolver.TerminalName)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	size, err := file.Len()
	if err != nil {
		return nil, err
	}
	const maximumStderrTailSizeBytes = 8 * 1024
	tail := make([]byte, min(size, maximumStderrTailSizeBytes))
	count, err := file.ReadAt(tail, size-int64(len(tail)))
	if err == io.EOF {
		err = nil
	}
	return tail[:count], err
}

// ExecuteInPersistentWorker exchanges opaque request and response
// payloads with a session's compiler. Concurrent requests to the same
// session are rejected rather than queued. Cancelling an in-flight
// exchange or encountering a framing or I/O error terminates the compiler.
func (server *PersistentRunner) ExecuteInPersistentWorker(ctx context.Context, request *runner_pb.ExecuteInPersistentWorkerRequest) (*runner_pb.ExecuteInPersistentWorkerResponse, error) {
	if err := ctx.Err(); err != nil {
		return nil, status.FromContextError(err).Err()
	}
	if request.GetSessionId() == "" {
		return nil, status.Error(codes.InvalidArgument, "Missing session ID")
	}
	server.lock.Lock()
	if server.closed {
		server.lock.Unlock()
		return nil, status.Error(codes.Unavailable, "Persistent runner is shutting down")
	}
	session := server.sessions[request.SessionId]
	if session == nil || session.closing {
		server.lock.Unlock()
		return nil, status.Error(codes.NotFound, "Persistent worker session does not exist")
	}
	if session.busy {
		server.lock.Unlock()
		return nil, status.Error(codes.FailedPrecondition, "Persistent worker session is already executing")
	}
	session.busy = true
	server.lock.Unlock()
	defer func() {
		server.lock.Lock()
		session.busy = false
		server.lock.Unlock()
	}()

	response, err := session.process.Execute(ctx, request.SerializedWorkRequest)
	if err != nil {
		if ctx.Err() != nil {
			return nil, status.FromContextError(ctx.Err()).Err()
		}
		select {
		case <-session.process.Done():
			session.logExit()
		default:
		}
		return nil, util.StatusWrapWithCode(err, codes.Unavailable, "Persistent worker exchange failed")
	}
	return &runner_pb.ExecuteInPersistentWorkerResponse{SerializedWorkResponse: response}, nil
}

// CloseSession terminates and reaps a session's compiler, interrupting
// any active exchange. Repeated closes and unknown, nonempty session
// IDs succeed.
func (server *PersistentRunner) CloseSession(ctx context.Context, request *runner_pb.SessionRequest) (*emptypb.Empty, error) {
	if request.GetSessionId() == "" {
		return nil, status.Error(codes.InvalidArgument, "Missing session ID")
	}
	server.lock.Lock()
	session := server.sessions[request.SessionId]
	if session != nil {
		session.closing = true
	}
	server.lock.Unlock()
	if session != nil {
		if err := session.process.Close(); err != nil {
			return nil, util.StatusWrapWithCode(err, codes.Internal, "Failed to close persistent worker")
		}
		select {
		case <-session.done:
			if session.err != nil {
				return nil, session.err
			}
		case <-ctx.Done():
			return nil, status.FromContextError(ctx.Err()).Err()
		}
	}
	return &emptypb.Empty{}, nil
}

// Close stops session creation, cancels startup, and terminates retained
// compilers. It waits for process cleanup and may be called multiple
// times, including concurrently.
func (server *PersistentRunner) Close() error {
	server.closeOnce.Do(func() {
		server.lock.Lock()
		server.closed = true
		server.cancel()
		sessions := make([]*persistentRunnerSession, 0, len(server.sessions))
		for _, session := range server.sessions {
			sessions = append(sessions, session)
		}
		server.lock.Unlock()
		var closeError error
		for _, session := range sessions {
			closeError = errors.Join(closeError, session.process.Close())
		}
		server.operations.Wait()
		server.lock.Lock()
		server.closeError = errors.Join(server.closeError, closeError)
		server.lock.Unlock()
	})
	return server.closeError
}

func (server *PersistentRunner) releaseCleaner() error {
	if server.idleInvoker == nil {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	return server.idleInvoker.Release(ctx)
}
