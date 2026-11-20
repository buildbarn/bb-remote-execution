//go:build windows
// +build windows

// This file intentionally mirrors the native Bazel Windows launcher:
// https://github.com/bazelbuild/bazel/blob/master/src/main/native/windows/process.cc
//
// The root process is assigned to a non-breakaway job as part of process
// creation. This prevents it from spawning descendants before the runner has
// contained it, which could leave those descendants alive to keep the input
// root undeletable.

package runner

import (
	"errors"
	"os"
	"os/exec"
	"sync"
	"syscall"
	"unsafe"

	"golang.org/x/sys/windows"
)

const (
	// jobObjectMsgActiveProcessZero is JOB_OBJECT_MSG_ACTIVE_PROCESS_ZERO.
	// The Windows SDK's winnt.h defines this message value as 4, but
	// x/sys/windows does not expose it. The official job completion-port docs
	// describe the message:
	// https://learn.microsoft.com/en-us/windows/win32/api/winnt/ns-winnt-jobobject_associate_completion_port
	jobObjectMsgActiveProcessZero = 4

	windowsTerminatedExitCode = 130
)

type jobObjectAssociateCompletionPort struct {
	CompletionKey  uintptr
	CompletionPort windows.Handle
}

type commandProcess struct {
	lock   sync.Mutex
	job    windows.Handle
	ioport windows.Handle
	closed bool
}

// prepareCommandForStart creates the job object and completion port before the
// process exists. It also amends cmd so Start creates the root process in the
// job and in a new process group. The returned object owns those handles until
// Close or AfterWait.
func prepareCommandForStart(cmd *exec.Cmd) (*commandProcess, error) {
	job, err := windows.CreateJobObject(nil, nil)
	if err != nil {
		return nil, err
	}
	p := &commandProcess{
		job: job,
	}
	success := false
	defer func() {
		if !success {
			p.Close()
		}
	}()

	jobInfo := windows.JOBOBJECT_EXTENDED_LIMIT_INFORMATION{}
	jobInfo.BasicLimitInformation.LimitFlags = windows.JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE
	if _, err := windows.SetInformationJobObject(
		job,
		windows.JobObjectExtendedLimitInformation,
		uintptr(unsafe.Pointer(&jobInfo)),
		uint32(unsafe.Sizeof(jobInfo)),
	); err != nil {
		return nil, err
	}

	ioport, err := windows.CreateIoCompletionPort(windows.InvalidHandle, 0, 0, 1)
	if err != nil {
		return nil, err
	}
	p.ioport = ioport

	port := jobObjectAssociateCompletionPort{
		CompletionKey:  uintptr(job),
		CompletionPort: ioport,
	}
	if _, err := windows.SetInformationJobObject(
		job,
		windows.JobObjectAssociateCompletionPortInformation,
		uintptr(unsafe.Pointer(&port)),
		uint32(unsafe.Sizeof(port)),
	); err != nil {
		return nil, err
	}

	var sysProcAttr syscall.SysProcAttr
	if cmd.SysProcAttr != nil {
		sysProcAttr = *cmd.SysProcAttr
	}
	// Clone Jobs before appending, so preparing the command does not mutate the
	// backing array of a caller-provided SysProcAttr.
	sysProcAttr.Jobs = append([]syscall.Handle(nil), sysProcAttr.Jobs...)
	sysProcAttr.Jobs = append(sysProcAttr.Jobs, syscall.Handle(job))
	sysProcAttr.CreationFlags |= windows.CREATE_NEW_PROCESS_GROUP
	cmd.SysProcAttr = &sysProcAttr
	if cmd.Cancel != nil {
		cmd.Cancel = p.Cancel
	}

	success = true
	return p, nil
}

// Cancel terminates the root process and all of its descendants through the job
// to which Start assigned the process atomically.
func (p *commandProcess) Cancel() error {
	return p.terminateJob()
}

// AfterWait runs after the root process has exited. It terminates anything that
// remains in the job, waits for the active-process-zero notification, and only
// then releases the job handles so build-directory cleanup cannot race lingering
// descendants.
func (p *commandProcess) AfterWait(cmd *exec.Cmd) error {
	defer p.Close()
	if err := p.terminateJob(); err != nil && !errors.Is(err, os.ErrProcessDone) {
		return err
	}
	return p.waitForActiveProcessZero()
}

// Close releases job resources. Once a process has been assigned to the job,
// callers should prefer AfterWait so descendants are terminated and waited for
// before these handles are closed.
func (p *commandProcess) Close() {
	p.lock.Lock()
	if p.closed {
		p.lock.Unlock()
		return
	}
	p.closed = true
	job := p.job
	ioport := p.ioport
	p.job = 0
	p.ioport = 0
	p.lock.Unlock()

	if job != 0 {
		windows.CloseHandle(job)
	}
	if ioport != 0 {
		windows.CloseHandle(ioport)
	}
}

func (p *commandProcess) terminateJob() error {
	p.lock.Lock()
	closed := p.closed
	job := p.job
	p.lock.Unlock()
	if closed || job == 0 {
		return os.ErrProcessDone
	}
	return windows.TerminateJobObject(job, windowsTerminatedExitCode)
}

func (p *commandProcess) waitForActiveProcessZero() error {
	p.lock.Lock()
	closed := p.closed
	job := p.job
	ioport := p.ioport
	p.lock.Unlock()
	if closed || job == 0 || ioport == 0 {
		return nil
	}
	for {
		var completionCode uint32
		var completionKey uintptr
		var overlapped *windows.Overlapped
		if err := windows.GetQueuedCompletionStatus(
			ioport,
			&completionCode,
			&completionKey,
			&overlapped,
			windows.INFINITE,
		); err != nil {
			return err
		}
		if windows.Handle(completionKey) == job && completionCode == jobObjectMsgActiveProcessZero {
			return nil
		}
	}
}
