//go:build windows
// +build windows

// This file intentionally mirrors the native Bazel Windows launcher:
// https://github.com/bazelbuild/bazel/blob/master/src/main/native/windows/process.cc
//
// The root process is created suspended, assigned to a non-breakaway job, and
// only then resumed. Assigning while suspended is the important race fix: an
// unsuspended process could spawn descendants before the runner has contained
// it, leaving those descendants alive to keep the input root undeletable.

package runner

import (
	"errors"
	"fmt"
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
	lock     sync.Mutex
	job      windows.Handle
	ioport   windows.Handle
	assigned bool
	closed   bool
	canceled bool
	// afterCancellationCheck is a test-only hook for pausing AfterStart while
	// it holds lock between checking canceled and resuming the process.
	afterCancellationCheck func()
}

// prepareCommandForStart creates the job object and completion port before the
// process exists. It also amends cmd so Start creates the root process suspended
// and in a new process group. The returned object owns those handles until
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
	sysProcAttr.CreationFlags |= windows.CREATE_NEW_PROCESS_GROUP | windows.CREATE_SUSPENDED
	cmd.SysProcAttr = &sysProcAttr
	if cmd.Cancel != nil {
		cmd.Cancel = p.Cancel
	}

	success = true
	return p, nil
}

// Cancel may run as soon as cmd.Context is canceled, including before AfterStart
// has assigned the suspended root process to the job. In that pre-assignment
// race, record the cancellation and let AfterStart terminate the job after the
// assignment has made termination cover the whole process tree.
func (p *commandProcess) Cancel() error {
	p.lock.Lock()
	p.canceled = true
	assigned := p.assigned
	closed := p.closed
	p.lock.Unlock()
	if closed {
		return os.ErrProcessDone
	}
	if !assigned {
		return nil
	}
	return p.terminateJob()
}

// AfterStart assigns the still-suspended root process to the job, then either
// applies a cancellation that arrived early or resumes the process. On any
// failure after Start, it kills/reaps the partially started process tree before
// returning to the caller.
func (p *commandProcess) AfterStart(cmd *exec.Cmd) error {
	processHandle, err := windows.OpenProcess(
		windows.PROCESS_SET_QUOTA|windows.PROCESS_TERMINATE,
		false,
		uint32(cmd.Process.Pid),
	)
	if err != nil {
		return p.cleanupAfterStartFailure(cmd, err, false)
	}
	err = windows.AssignProcessToJobObject(p.job, processHandle)
	windows.CloseHandle(processHandle)
	if err != nil {
		return p.cleanupAfterStartFailure(cmd, err, false)
	}

	p.lock.Lock()
	p.assigned = true
	if p.canceled {
		p.lock.Unlock()
		if err := p.terminateJob(); err != nil {
			return p.cleanupAfterStartFailure(cmd, err, true)
		}
		return nil
	}
	if p.afterCancellationCheck != nil {
		p.afterCancellationCheck()
	}
	err = resumeProcessThreads(uint32(cmd.Process.Pid))
	p.lock.Unlock()
	if err != nil {
		return p.cleanupAfterStartFailure(cmd, err, true)
	}
	return nil
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

func (p *commandProcess) cleanupAfterStartFailure(cmd *exec.Cmd, cause error, assigned bool) error {
	if assigned {
		_ = p.terminateJob()
	} else if cmd.Process != nil {
		_ = cmd.Process.Kill()
	}
	_ = cmd.Wait()
	if assigned {
		_ = p.waitForActiveProcessZero()
	}
	p.Close()
	return cause
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

// resumeProcessThreads resumes all threads belonging to the suspended root
// process. Go's os/exec path closes the primary thread handle returned by
// CreateProcess, so this code has to rediscover the thread handle by PID.
func resumeProcessThreads(pid uint32) error {
	snapshot, err := windows.CreateToolhelp32Snapshot(windows.TH32CS_SNAPTHREAD, 0)
	if err != nil {
		return err
	}
	defer windows.CloseHandle(snapshot)

	var threadHandles []windows.Handle
	defer func() {
		for _, threadHandle := range threadHandles {
			windows.CloseHandle(threadHandle)
		}
	}()

	entry := windows.ThreadEntry32{
		Size: uint32(unsafe.Sizeof(windows.ThreadEntry32{})),
	}
	for err := windows.Thread32First(snapshot, &entry); ; err = windows.Thread32Next(snapshot, &entry) {
		if err != nil {
			if errors.Is(err, windows.ERROR_NO_MORE_FILES) {
				break
			}
			return err
		}
		if entry.OwnerProcessID == pid {
			threadHandle, err := windows.OpenThread(windows.THREAD_SUSPEND_RESUME, false, entry.ThreadID)
			if err != nil {
				return err
			}
			threadHandles = append(threadHandles, threadHandle)
		}
	}
	if len(threadHandles) == 0 {
		return fmt.Errorf("process %d has no threads to resume", pid)
	}
	for _, threadHandle := range threadHandles {
		if _, err := windows.ResumeThread(threadHandle); err != nil {
			return err
		}
	}
	return nil
}
