package runner

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"sync"
)

// PersistentWorkerProcess spawns and manages a worker process.
type PersistentWorkerProcess struct {
	command       *exec.Cmd
	adapter       *PersistentWorkerAdapter
	stdin         *os.File
	stdout        *os.File
	executionLock sync.Mutex
	stopOnce      sync.Once
	stopError     error
	waitError     error
	done          chan struct{}
}

// StartPersistentWorkerProcess starts a long-lived worker using a
// preconfigured command. It takes ownership of the command and its
// stdin/stdout pipes, including waiting for process termination. The
// caller retains ownership of any configured stderr log file.
func StartPersistentWorkerProcess(command *exec.Cmd, maximumResponseSizeBytes uint64) (*PersistentWorkerProcess, error) {
	if command == nil || command.Process != nil || command.Stdin != nil || command.Stdout != nil {
		return nil, errors.New("persistent worker requires an unstarted command without stdin or stdout")
	}
	stdinReader, stdinWriter, err := os.Pipe()
	if err != nil {
		return nil, err
	}
	defer stdinReader.Close()
	stdoutReader, stdoutWriter, err := os.Pipe()
	if err != nil {
		stdinWriter.Close()
		return nil, err
	}
	defer stdoutWriter.Close()
	command.Stdin = stdinReader
	command.Stdout = stdoutWriter
	if err := command.Start(); err != nil {
		stdinWriter.Close()
		stdoutReader.Close()
		return nil, err
	}

	process := &PersistentWorkerProcess{
		command: command,
		adapter: NewPersistentWorkerAdapter(stdinWriter, stdoutReader, maximumResponseSizeBytes),
		stdin:   stdinWriter,
		stdout:  stdoutReader,
		done:    make(chan struct{}),
	}
	go func() {
		process.waitError = command.Wait()
		process.stop()
		close(process.done)
	}()
	return process, nil
}

// Execute writes an opaque request to the worker and reads its response.
// Concurrent executions are rejected. Cancelling an in-flight exchange
// or encountering a framing or I/O error closes the process; successful
// exchanges leave it available for reuse.
func (process *PersistentWorkerProcess) Execute(ctx context.Context, request []byte) ([]byte, error) {
	if !process.executionLock.TryLock() {
		return nil, errors.New("persistent worker is already executing a request")
	}
	defer process.executionLock.Unlock()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	select {
	case <-process.done:
		if process.waitError != nil {
			return nil, fmt.Errorf("persistent worker exited: %w", process.waitError)
		}
		return nil, errors.New("persistent worker has exited")
	default:
	}

	cancellationDone := make(chan struct{})
	stopCancellation := context.AfterFunc(ctx, func() {
		process.stop()
		close(cancellationDone)
	})
	var response []byte
	err := process.adapter.WriteRequest(request)
	if err == nil {
		response, err = process.adapter.ReadResponse()
	}
	if !stopCancellation() {
		<-cancellationDone
		return nil, errors.Join(ctx.Err(), process.Close())
	}
	if err != nil {
		if closeError := process.Close(); closeError != nil {
			return nil, errors.Join(err, closeError)
		}
		return nil, errors.Join(err, process.Wait())
	}
	return response, nil
}

func (process *PersistentWorkerProcess) stop() {
	process.stopOnce.Do(func() {
		process.stdin.Close()
		process.stdout.Close()
		if err := killPersistentWorkerProcess(process.command.Process); err != nil && !errors.Is(err, os.ErrProcessDone) {
			process.stopError = err
		}
	})
}

// Close closes the protocol pipes, kills the worker, and waits for it
// to be reaped. It may be called multiple times, including concurrently,
// and does not report the worker's exit status as an error.
func (process *PersistentWorkerProcess) Close() error {
	process.stop()
	if process.stopError != nil {
		return process.stopError
	}
	<-process.done
	return nil
}

// Done returns a channel that is closed after the worker has been
// reaped and its protocol pipes have been closed.
func (process *PersistentWorkerProcess) Done() <-chan struct{} {
	return process.done
}

// Wait waits for the worker to be reaped and returns the command's
// process exit error. It does not initiate process termination.
func (process *PersistentWorkerProcess) Wait() error {
	<-process.done
	return process.waitError
}
