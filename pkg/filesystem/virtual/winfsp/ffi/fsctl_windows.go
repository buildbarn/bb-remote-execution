//go:build windows
// +build windows

// This is based on https://github.com/aegistudio/go-winfsp

package ffi

const (
	SIZEOF_WCHAR = 2
)

const (
	FSP_FSCTL_VOLUME_NAME_SIZE    = 64 * SIZEOF_WCHAR
	FSP_FSCTL_VOLUME_PREFIX_SIZE  = 192 * SIZEOF_WCHAR
	FSP_FSCTL_VOLUME_FSNAME_SIZE  = 16 * SIZEOF_WCHAR
	FSP_FSCTL_VOLUME_NAME_SIZEMAX = FSP_FSCTL_VOLUME_NAME_SIZE + FSP_FSCTL_VOLUME_PREFIX_SIZE
)

type FSP_FSCTL_VOLUME_INFO struct {
	TotalSize         uint64
	FreeSize          uint64
	VolumeLabelLength uint16
	VolumeLabel       [32]uint16
}

const (
	// basic filesystem attributes
	FspFSAttributeCaseSensitive = 1 << iota
	FspFSAttributeCasePreservedNames
	FspFSAttributeUnicodeOnDisk
	FspFSAttributePersistentAcls
	FspFSAttributeReparsePoints
	FspFSAttributeReparsePointsAccessCheck
	FspFSAttributeNamedStreams
	FspFSAttributeHardLinks
	FspFSAttributeExtendedAttributes
	FspFSAttributeReadOnlyVolume

	// kernel mode flags
	FspFSAttributePostCleanupWhenModifiedOnly
	FspFSAttributePassQueryDirectoryPattern
	FspFSAttributeAlwaysUseDoubleBuffering
	FspFSAttributePassQueryDirectoryFileName
	FspFSAttributeFlushAndPurgeOnCleanup
	FspFSAttributeDeviceControl

	// user mode flags
	FspFSAttributeUmFileContextIsUserContext2
	FspFSAttributeUmFileContextIsFullContext
	FspFSAttributeUmNoReparsePointsDirCheck
	FspFSAttributeUmReservedFlags0
	FspFSAttributeUmReservedFlags1
	FspFSAttributeUmReservedFlags2
	FspFSAttributeUmReservedFlags3
	FspFSAttributeUmReservedFlags4

	// additional kernel mode flags
	FspFSAttributeAllowOpenInKernelMode
	FspFSAttributeCasePreservedExtendedAttributes
	FspFSAttributeWslFeatures
	FspFSAttributeDirectoryMarkerAsNextOffset
	FspFSAttributeRejectIrpPriorToTransact0
	FspFSAttributeSupportsPosixUnlinkRename
	FspFSAttributePostDispositionWhenNecessaryOnly
	FspFSAttributeKmReservedFlags0
)

// FSP_FSCTL_VOLUME_PARAMS_V1
type fspFSCTLVolumeParamsV1 struct {
	SizeOfVolumeParamsV1     uint16
	SectorSize               uint16
	SectorsPerAllocationUnit uint16
	MaxComponentLength       uint16
	VolumeCreationTime       uint64
	VolumeSerialNumber       uint32
	TransactTimeout          uint32
	IrpTimeout               uint32
	IrpCapacity              uint32
	FileInfoTimeout          uint32
	FileSystemAttribute      uint32
	Prefix                   [FSP_FSCTL_VOLUME_PREFIX_SIZE / SIZEOF_WCHAR]uint16
	FileSystemName           [FSP_FSCTL_VOLUME_FSNAME_SIZE / SIZEOF_WCHAR]uint16
	FileSystemAttribute2     uint32
	VolumeInfoTimeout        uint32
	DirInfoTimeout           uint32
	SecurityTimeout          uint32
	StreamInfoTimeout        uint32
	EaTimeout                uint32
	FsextControlCode         uint32
	Reserved32               [1]uint32
	Reserved64               [2]uint64
	// 504 bytes
}

type FSP_FSCTL_FILE_INFO struct {
	FileAttributes uint32
	ReparseTag     uint32
	AllocationSize uint64
	FileSize       uint64
	CreationTime   uint64
	LastAccessTime uint64
	LastWriteTime  uint64
	ChangeTime     uint64
	IndexNumber    uint64
	HardLinks      uint32 // unimplemented: set to 0
	EaSize         uint32
}

type FSP_FSCTL_DIR_INFO struct {
	Size       uint16
	FileInfo   FSP_FSCTL_FILE_INFO
	NextOffset uint64
	Padding0   uint64
	Padding1   uint64
}

type FSP_FSCTL_NOTIFY_INFO struct {
	Size        uint16
	Filter      uint32
	Action      uint32
	FileNameBuf *uint16
}
