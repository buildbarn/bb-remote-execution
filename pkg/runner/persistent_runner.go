package runner

import (
	"context"
	"errors"
	"sync"

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
	busy    bool
	closing bool
}

// PersistentRunner implements the runner session service using retained
// compiler processes on the local system.
type PersistentRunner struct {
	localRunner                  *localRunner
	lifetimeContext              context.Context
	cancel                       context.CancelFunc
	maximumWorkResponseSizeBytes uint64
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
// the runner before closing the directory.
func NewPersistentRunner(ctx context.Context, buildDirectory filesystem.Directory, buildDirectoryPath *path.Builder, commandCreator CommandCreator, setTmpdirEnvironmentVariable bool, maximumWorkResponseSizeBytes uint64) *PersistentRunner {
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
		sessions:                     map[string]*persistentRunnerSession{},
	}
	context.AfterFunc(lifetimeContext, func() { server.Close() })
	return server
}

// CheckReadiness checks that the runner is accepting sessions.
func (server *PersistentRunner) CheckReadiness(ctx context.Context, request *runner_pb.CheckReadinessRequest) (*emptypb.Empty, error) {
	if server.lifetimeContext.Err() != nil {
		return nil, status.Error(codes.Unavailable, "Persistent runner is shutting down")
	}
	return server.localRunner.CheckReadiness(ctx, request)
}

// CreateSession starts a compiler process and returns its session ID.
// The request context can cancel startup, but a successfully created
// session remains alive independently of that context.
func (server *PersistentRunner) CreateSession(ctx context.Context, request *runner_pb.CreateSessionRequest) (*runner_pb.CreateSessionResponse, error) {
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
	defer func() {
		if !retained {
			process.Close()
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
		return nil, status.Error(codes.Unavailable, "Persistent worker exited during startup")
	default:
	}
	session := &persistentRunnerSession{process: process}
	server.sessions[sessionID.String()] = session
	retained = true
	server.operations.Add(1)
	go func() {
		defer server.operations.Done()
		<-process.Done()
		cancelSession()
		server.lock.Lock()
		delete(server.sessions, sessionID.String())
		server.lock.Unlock()
	}()
	return &runner_pb.CreateSessionResponse{SessionId: sessionID.String()}, nil
}

// ExecuteSession exchanges opaque request and response payloads with a
// session's compiler. Concurrent requests to the same session are
// rejected rather than queued. Cancelling an in-flight exchange or
// encountering a framing or I/O error terminates the compiler.
func (server *PersistentRunner) ExecuteSession(ctx context.Context, request *runner_pb.ExecuteSessionRequest) (*runner_pb.ExecuteSessionResponse, error) {
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
		return nil, util.StatusWrapWithCode(err, codes.Unavailable, "Persistent worker exchange failed")
	}
	return &runner_pb.ExecuteSessionResponse{SerializedWorkResponse: response}, nil
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
		for _, session := range sessions {
			server.closeError = errors.Join(server.closeError, session.process.Close())
		}
		server.operations.Wait()
	})
	return server.closeError
}
