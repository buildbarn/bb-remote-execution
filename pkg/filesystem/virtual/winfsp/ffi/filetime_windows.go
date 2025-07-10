//go:build windows
// +build windows

// This is based on https://github.com/aegistudio/go-winfsp

package ffi

import (
	"syscall"
	"time"
	"unsafe"
)

func uint64FromFiletime(filetime *syscall.Filetime) uint64 {
	return *(*uint64)(unsafe.Pointer(filetime))
}

func Timestamp(t time.Time) uint64 {
	filetime := syscall.NsecToFiletime(t.UnixNano())
	return uint64FromFiletime(&filetime)
}

func Filetime(t syscall.Filetime) uint64 {
	return uint64FromFiletime(&t)
}
