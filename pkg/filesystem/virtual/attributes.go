package virtual

import (
	"time"

	"github.com/buildbarn/bb-storage/pkg/filesystem"
)

// AttributesMask is a bitmask of status attributes that need to be
// requested through Node.VirtualGetAttributes().
type AttributesMask uint32

const (
	// AttributesMaskChangeID requests the change ID, which clients
	// can use to determine if file data, directory contents, or
	// attributes of the node have changed.
	AttributesMaskChangeID AttributesMask = 1 << iota
	// AttributesMaskDeviceNumber requests the raw device number
	// (st_rdev).
	AttributesMaskDeviceNumber
	// AttributesMaskFileHandle requests an identifier of the file
	// that contains sufficient information to be able to resolve it
	// at a later point in time.
	AttributesMaskFileHandle
	// AttributesMaskFileType requests the file type (upper 4 bits
	// of st_mode).
	AttributesMaskFileType
	// AttributesMaskInodeNumber requests the inode number (st_ino).
	AttributesMaskInodeNumber
	// AttributesMaskLastDataModificationTime requests the last data
	// modification time (st_mtim).
	AttributesMaskLastDataModificationTime
	// AttributesMaskLinkCount requests the link count (st_nlink).
	AttributesMaskLinkCount
	// AttributesMaskPermissions requests the permissions (lowest 12
	// bits of set_mode).
	AttributesMaskPermissions
	// AttributesMaskSizeBytes requests the file size (st_size).
	AttributesMaskSizeBytes
)

// Attributes of a file, normally requested through stat() or readdir().
// A bitmask is used to track which attributes are set.
type Attributes struct {
	fieldsPresent AttributesMask

	changeID                 uint64
	deviceNumber             filesystem.DeviceNumber
	fileHandle               []byte
	fileType                 filesystem.FileType
	inodeNumber              uint64
	lastDataModificationTime time.Time
	linkCount                uint32
	permissions              Permissions
	sizeBytes                uint64
}

// GetChangeID returns the change ID, which clients can use to determine
// if file data, directory contents, or attributes of the node have
// changed.
func (a *Attributes) GetChangeID() uint64 {
	if a.fieldsPresent&AttributesMaskChangeID == 0 {
		panic("The change ID attribute is mandatory, meaning it should be set when requested")
	}
	return a.changeID
}

// SetChangeID sets the change ID, which clients can use to determine if
// file data, directory contents, or attributes of the node have
// changed.
func (a *Attributes) SetChangeID(changeID uint64) *Attributes {
	a.changeID = changeID
	a.fieldsPresent |= AttributesMaskChangeID
	return a
}

// GetDeviceNumber returns the raw device number (st_rdev).
func (a *Attributes) GetDeviceNumber() (filesystem.DeviceNumber, bool) {
	return a.deviceNumber, a.fieldsPresent&AttributesMaskDeviceNumber != 0
}

// SetDeviceNumber sets the raw device number (st_rdev).
func (a *Attributes) SetDeviceNumber(deviceNumber filesystem.DeviceNumber) *Attributes {
	a.deviceNumber = deviceNumber
	a.fieldsPresent |= AttributesMaskDeviceNumber
	return a
}

// GetFileHandle returns an identifier of the file that contains
// sufficient information to be able to resolve it at a later point in
// time.
func (a *Attributes) GetFileHandle() []byte {
	if a.fieldsPresent&AttributesMaskFileHandle == 0 {
		panic("The file handle attribute is mandatory, meaning it should be set when requested")
	}
	return a.fileHandle
}

// SetFileHandle sets an identifier of the file that contains
// sufficient information to be able to resolve it at a later point in
// time.
func (a *Attributes) SetFileHandle(fileHandle []byte) *Attributes {
	a.fileHandle = fileHandle
	a.fieldsPresent |= AttributesMaskFileHandle
	return a
}

// GetFileType returns the file type (upper 4 bits of st_mode).
func (a *Attributes) GetFileType() filesystem.FileType {
	if a.fieldsPresent&AttributesMaskFileType == 0 {
		panic("The file type attribute is mandatory, meaning it should be set when requested")
	}
	return a.fileType
}

// SetFileType sets the file type (upper 4 bits of st_mode).
func (a *Attributes) SetFileType(fileType filesystem.FileType) *Attributes {
	a.fileType = fileType
	a.fieldsPresent |= AttributesMaskFileType
	return a
}

// GetInodeNumber returns the inode number (st_ino).
func (a *Attributes) GetInodeNumber() uint64 {
	if a.fieldsPresent&AttributesMaskInodeNumber == 0 {
		panic("The inode number attribute is mandatory, meaning it should be set when requested")
	}
	return a.inodeNumber
}

// SetInodeNumber sets the inode number (st_ino).
func (a *Attributes) SetInodeNumber(inodeNumber uint64) *Attributes {
	a.inodeNumber = inodeNumber
	a.fieldsPresent |= AttributesMaskInodeNumber
	return a
}

// GetLastDataModificationTime returns the last data modification time
// (st_mtim).
func (a *Attributes) GetLastDataModificationTime() (time.Time, bool) {
	return a.lastDataModificationTime, a.fieldsPresent&AttributesMaskLastDataModificationTime != 0
}

// SetLastDataModificationTime sets the last data modification time
// (st_mtim).
func (a *Attributes) SetLastDataModificationTime(lastDataModificationTime time.Time) *Attributes {
	a.lastDataModificationTime = lastDataModificationTime
	a.fieldsPresent |= AttributesMaskLastDataModificationTime
	return a
}

// GetLinkCount returns the link count (st_nlink).
func (a *Attributes) GetLinkCount() uint32 {
	if a.fieldsPresent&AttributesMaskLinkCount == 0 {
		panic("The link count attribute is mandatory, meaning it should be set when requested")
	}
	return a.linkCount
}

// SetLinkCount sets the link count (st_nlink).
func (a *Attributes) SetLinkCount(linkCount uint32) *Attributes {
	a.linkCount = linkCount
	a.fieldsPresent |= AttributesMaskLinkCount
	return a
}

// GetPermissions returns the mode (lowest 12 bits of st_mode).
func (a *Attributes) GetPermissions() (Permissions, bool) {
	return a.permissions, a.fieldsPresent&AttributesMaskPermissions != 0
}

// SetPermissions sets the mode (lowest 12 bits of st_mode).
func (a *Attributes) SetPermissions(permissions Permissions) *Attributes {
	a.permissions = permissions
	a.fieldsPresent |= AttributesMaskPermissions
	return a
}

// GetSizeBytes returns the file size (st_size).
func (a *Attributes) GetSizeBytes() (uint64, bool) {
	return a.sizeBytes, a.fieldsPresent&AttributesMaskSizeBytes != 0
}

// SetSizeBytes sets the file size (st_size).
func (a *Attributes) SetSizeBytes(sizeBytes uint64) *Attributes {
	a.sizeBytes = sizeBytes
	a.fieldsPresent |= AttributesMaskSizeBytes
	return a
}
