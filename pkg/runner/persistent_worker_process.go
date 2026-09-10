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

// StartPersistentWorkerProcess spawns a long lived worker process.
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

// Execute writes a WorkRequest to the workers stdin for the worker to execute.
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
		return nil, errors.Join(err, process.Close())
	}
	return response, nil
}

func (process *PersistentWorkerProcess) stop() {
	process.stopOnce.Do(func() {
		process.stdin.Close()
		process.stdout.Close()
		if err := process.command.Process.Kill(); err != nil && !errors.Is(err, os.ErrProcessDone) {
			process.stopError = err
		}
	})
}

func (process *PersistentWorkerProcess) Close() error {
	process.stop()
	if process.stopError != nil {
		return process.stopError
	}
	<-process.done
	return nil
}

func (process *PersistentWorkerProcess) Done() <-chan struct{} {
	return process.done
}

func (process *PersistentWorkerProcess) Wait() error {
	<-process.done
	return process.waitError
}
