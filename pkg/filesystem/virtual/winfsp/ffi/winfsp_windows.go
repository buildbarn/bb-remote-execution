//go:build windows
// +build windows

// This is based on https://github.com/aegistudio/go-winfsp

package ffi

// This is FSP_FILE_SYSTEM_INTERFACE from WinFSP
type fspFileSystemInterface struct {
	GetVolumeInfo        uintptr
	SetVolumeLabel       uintptr
	GetSecurityByName    uintptr
	Create               uintptr
	Open                 uintptr
	Overwrite            uintptr
	Cleanup              uintptr
	Close                uintptr
	Read                 uintptr
	Write                uintptr
	Flush                uintptr
	GetFileInfo          uintptr
	SetBasicInfo         uintptr
	SetFileSize          uintptr
	CanDelete            uintptr
	Rename               uintptr
	GetSecurity          uintptr
	SetSecurity          uintptr
	ReadDirectory        uintptr
	ResolveReparsePoints uintptr
	GetReparsePoint      uintptr
	SetReparsePoint      uintptr
	DeleteReparsePoint   uintptr
	GetStreamInfo        uintptr
	GetDirInfoByName     uintptr
	Control              uintptr
	SetDelete            uintptr
	CreateEx             uintptr
	OverwriteEx          uintptr
	GetEa                uintptr
	SetEa                uintptr
	Obsolete0            uintptr
	DispatcherStopped    uintptr
	Reserved             [31]uintptr
}

type REPARSE_DATA_BUFFER_GENERIC struct {
	ReparseTag        uint32
	ReparseDataLength uint16
	Reserved          uint16
	DataBuffer        [1]byte
}

const SYMLINK_FLAG_RELATIVE = 1

type REPARSE_DATA_BUFFER_SYMBOLIC_LINK struct {
	ReparseTag           uint32
	ReparseDataLength    uint16
	Reserved             uint16
	SubstituteNameOffset uint16
	SubstituteNameLength uint16
	PrintNameOffset      uint16
	PrintNameLength      uint16
	Flags                uint32
	PathBuffer           [1]uint16
}

const FILE_NEED_EA = 0x00000080

type FILE_FULL_EA_INFORMATION struct {
	NextEntryOffset uint32
	Flags           uint8
	EaNameLength    uint8
	EaValueLength   int16
	EaName          [1]byte
}

const (
	FspCleanupDelete            = 0x01
	FspCleanupSetAllocationSize = 0x02
	FspCleanupSetArchiveBit     = 0x10
	FspCleanupSetLastAccessTime = 0x20
	FspCleanupSetLastWriteTime  = 0x40
	FspCleanupSetChangeTime     = 0x80
)

type fspFileSystem struct {
	Version     uint16
	UserContext uintptr
}
