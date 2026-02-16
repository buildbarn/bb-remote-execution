//go:build windows
// +build windows

package winfsp

import (
	"context"
	"strings"
	"sync"
	"time"
	"unsafe"

	"github.com/buildbarn/bb-remote-execution/pkg/filesystem/virtual"
	"github.com/buildbarn/bb-storage/pkg/filesystem"
	path "github.com/buildbarn/bb-storage/pkg/filesystem/path"
	"github.com/buildbarn/bb-storage/pkg/filesystem/windowsext"
	"github.com/buildbarn/bb-storage/pkg/util"
	ffi "github.com/winfsp/go-winfsp"
	"github.com/winfsp/go-winfsp/filetime"

	"golang.org/x/sys/windows"
)

// Some notes on locking.
//
// We're using WinFSP's default concurrency mode, which is
// FSP_FILE_SYSTEM_OPERATION_GUARD_STRATEGY_FINE. This means that WinFSP will
// effectively use a single sync.RWMutex. Operations acquire this as follows:
//   - Writers: SetVolumeLabel, Flush(Volume), Create, Cleanup(Delete),
//     SetInformation(Rename).
//   - Readers: GetVolumeInfo, Open, SetInformation(Disposition),
//     ReadDirectory.
// All other operations do not acquire any locks. However, WinFSP still applies
// a sync.RWLock lock per individual file (inside the WinFSP driver itself), so
// concurrent reads/writes/etc. are still possible, but not in a conflicting
// way on the same file.
//
// On Windows there are quite a few different operations that might conflict.
// For example, you need to prevent deletion of files if they're open, and also
// enforce sharing modes when opening files. Thankfully, WinFSP does all of
// this for us.
//
// In quite a few places in this implementation, we have to perform several
// operations sequentially that really ought to be done atomically. For
// example, in the implementation of Write, if we should write at the end
// we really ought to get the size of the file and perform the write
// atomically, but the VFS does not provide enough locking to do this. In
// general we rely on the fact that WinFSP does RW locking above us, and we
// assume that the VFS implementation will not be trying to concurrently
// write to the same file. That will be true if this is bb_worker and it's
// trying to fetch inputs to a build action, but wouldn't if the VFS was
// being used for something more elaborate.

type FileSystem struct {
	caseSensitive bool
	rootDirectory virtual.Directory
	openFiles     openedNodesPool

	// This does not need to be locked; WinFSP enforces a read/write lock.
	label []uint16
}

func NewFileSystem(rootDirectory virtual.Directory, caseSensitive bool) *FileSystem {
	return &FileSystem{
		caseSensitive: caseSensitive,
		rootDirectory: rootDirectory,
		openFiles:     newOpenedNodesPool(),
	}
}

func (fs *FileSystem) Mount(fsName, mountpoint string) (*ffi.FileSystem, error) {
	// debugHandle, err := syscall.GetStdHandle(syscall.STD_ERROR_HANDLE)
	// if err != nil {
	// 	return nil, err
	// }
	// err = ffi.DebugLogSetHandle(debugHandle)
	// if err != nil {
	// 	return nil, err
	// }

	var attributes uint32 = ffi.FspFSAttributeUmNoReparsePointsDirCheck
	attributes |= ffi.FspFSAttributeCasePreservedNames

	fileSystem, err := ffi.Mount(
		fs,
		mountpoint,
		ffi.Attributes(attributes),
		ffi.CaseSensitive(fs.caseSensitive),
		// ffi.Debug(true),
		ffi.FileSystemName(fsName),
		ffi.SectorSize(512, 1),
	)
	if err != nil {
		return nil, util.StatusWrap(err, "Failed to mount WinFSP file system")
	}
	return fileSystem, nil
}

// AttributesMaskForWinFSPAttr is the attributes mask to use for
// VirtualGetAttributes() to populate all relevant fields of
// FSP_FSCTL_FILE_INFO.
const AttributesMaskForWinFSPAttr = virtual.AttributesMaskDeviceNumber |
	virtual.AttributesMaskFileType |
	virtual.AttributesMaskInodeNumber |
	virtual.AttributesMaskLastDataModificationTime |
	virtual.AttributesMaskLinkCount |
	virtual.AttributesMaskOwnerGroupID |
	virtual.AttributesMaskOwnerUserID |
	virtual.AttributesMaskPermissions |
	virtual.AttributesMaskSizeBytes

// unsupportedCreateOptions are the options that are not supported.
const unsupportedCreateOptions = windows.FILE_CREATE_TREE_CONNECTION |
	windows.FILE_NO_EA_KNOWLEDGE |
	windows.FILE_OPEN_BY_FILE_ID |
	windows.FILE_RESERVE_OPFILTER |
	windows.FILE_OPEN_REQUIRING_OPLOCK |
	windows.FILE_COMPLETE_IF_OPLOCKED

const fileAndDirectoryFlag = windows.FILE_DIRECTORY_FILE | windows.FILE_NON_DIRECTORY_FILE

// Attributes we support being set via SetBasicInfo.
const setBasicInfoSupportedAttributes = windows.FILE_ATTRIBUTE_DIRECTORY |
	windows.FILE_ATTRIBUTE_NORMAL |
	windows.FILE_ATTRIBUTE_READONLY

// The Unix Epoch as a Windows FILETIME.
const unixEpochAsFiletime = 116444736000000000

var (
	// Stores the current processes' uid and gid, calculated once.
	currentUserID  *windows.SID
	currentGroupID *windows.SID
	userInfoError  error
	userInfoOnce   sync.Once

	// Stores the everyone SID, calculated once.
	everyoneSid      *windows.SID
	everyoneSidError error
	everyoneSidOnce  sync.Once
)

func calculateCurrentUserAndGroup() (uid, gid *windows.SID, err error) {
	token := windows.GetCurrentProcessToken()

	user, err := token.GetTokenUser()
	if err != nil {
		return nil, nil, err
	}

	group, err := token.GetTokenPrimaryGroup()
	if err != nil {
		return nil, nil, err
	}

	return user.User.Sid, group.PrimaryGroup, nil
}

func getCurrentProcessUserAndGroup() (uid, gid *windows.SID, err error) {
	userInfoOnce.Do(func() {
		currentUserID, currentGroupID, userInfoError = calculateCurrentUserAndGroup()
	})
	return currentUserID, currentGroupID, userInfoError
}

func getEveryoneSid() (*windows.SID, error) {
	everyoneSidOnce.Do(func() {
		everyoneSid, everyoneSidError = windows.CreateWellKnownSid(windows.WinWorldSid)
	})
	return everyoneSid, everyoneSidError
}

func toNTStatus(status virtual.Status) windows.NTStatus {
	switch status {
	case virtual.StatusOK:
		return windows.STATUS_SUCCESS
	case virtual.StatusErrAccess:
		return windows.STATUS_ACCESS_DENIED
	case virtual.StatusErrBadHandle:
		return windows.STATUS_INVALID_HANDLE
	case virtual.StatusErrExist:
		return windows.STATUS_OBJECT_NAME_COLLISION
	case virtual.StatusErrInval:
		return windows.STATUS_INVALID_PARAMETER
	case virtual.StatusErrIO:
		return windows.STATUS_UNEXPECTED_IO_ERROR
	case virtual.StatusErrIsDir:
		return windows.STATUS_FILE_IS_A_DIRECTORY
	case virtual.StatusErrNoEnt:
		return windows.STATUS_OBJECT_NAME_NOT_FOUND
	case virtual.StatusErrNotDir:
		return windows.STATUS_NOT_A_DIRECTORY
	case virtual.StatusErrNotEmpty:
		return windows.STATUS_DIRECTORY_NOT_EMPTY
	case virtual.StatusErrNXIO:
		return windows.STATUS_NO_SUCH_DEVICE
	case virtual.StatusErrPerm:
		return windows.STATUS_ACCESS_DENIED
	case virtual.StatusErrROFS:
		return windows.STATUS_MEDIA_WRITE_PROTECTED
	case virtual.StatusErrStale:
		return windows.STATUS_OBJECT_NAME_NOT_FOUND
	case virtual.StatusErrSymlink:
		return windows.STATUS_REPARSE
	case virtual.StatusErrWrongType:
		return windows.STATUS_OBJECT_TYPE_MISMATCH
	case virtual.StatusErrXDev:
		return windows.STATUS_NOT_SUPPORTED
	default:
		// For any other status, we return a generic error.
		return windows.STATUS_UNSUCCESSFUL
	}
}

func toShareMask(grantedAccess uint32) virtual.ShareMask {
	var shareMask virtual.ShareMask
	if grantedAccess&windows.FILE_READ_DATA != 0 {
		shareMask |= virtual.ShareMaskRead
	}
	if grantedAccess&(windows.FILE_WRITE_DATA|windows.FILE_APPEND_DATA) != 0 {
		shareMask |= virtual.ShareMaskWrite
	}
	return shareMask
}

// Convert FILETIME to a time.Time object.
func filetimeToTime(ft uint64) time.Time {
	nanoseconds := int64(ft-unixEpochAsFiletime) * 100
	result := time.Unix(0, nanoseconds).UTC()
	return result
}

func toVirtualAttributes(leaf virtual.DirectoryChild, attribute uint32, newAttributes *virtual.Attributes, attributesMask *virtual.AttributesMask) error {
	if attribute != windows.INVALID_FILE_ATTRIBUTES {
		if attribute&^setBasicInfoSupportedAttributes != 0 {
			return windows.STATUS_INVALID_PARAMETER
		}
		if attribute&windows.FILE_ATTRIBUTE_READONLY != 0 {
			permissions := virtual.PermissionsRead
			_, file := leaf.GetPair()
			if file != nil {
				permissions |= virtual.PermissionsExecute
			}
			newAttributes.SetPermissions(permissions)
			*attributesMask |= virtual.AttributesMaskPermissions
		}
		if attribute&windows.FILE_ATTRIBUTE_DIRECTORY != 0 {
			dir, _ := leaf.GetPair()
			if dir == nil {
				return windows.STATUS_NOT_A_DIRECTORY
			}
		}
	}
	return nil
}

func toWinFSPFileAttributes(attributes *virtual.Attributes) uint32 {
	var fileAttributes uint32
	switch attributes.GetFileType() {
	case filesystem.FileTypeRegularFile:
		fileAttributes |= windows.FILE_ATTRIBUTE_NORMAL
	case filesystem.FileTypeDirectory:
		fileAttributes |= windows.FILE_ATTRIBUTE_DIRECTORY
	case filesystem.FileTypeSymlink:
		fileAttributes |= windows.FILE_ATTRIBUTE_REPARSE_POINT
	default:
	}
	if permissions, ok := attributes.GetPermissions(); ok && permissions&virtual.PermissionsWrite == 0 {
		fileAttributes |= windows.FILE_ATTRIBUTE_READONLY
	}
	return fileAttributes
}

func toWinFSPFileInfo(attributes *virtual.Attributes, info *ffi.FSP_FSCTL_FILE_INFO) {
	info.EaSize = 0
	info.HardLinks = attributes.GetLinkCount()
	info.IndexNumber = attributes.GetInodeNumber()
	info.FileAttributes = toWinFSPFileAttributes(attributes)
	info.ReparseTag = 0

	if attributes.GetFileType() == filesystem.FileTypeSymlink {
		info.ReparseTag = windows.IO_REPARSE_TAG_SYMLINK
	}

	if sizeBytes, ok := attributes.GetSizeBytes(); ok {
		info.FileSize = sizeBytes
		// Set allocation size (rounded up to 4KB boundaries like in go-winfsp)
		info.AllocationSize = ((sizeBytes + 4095) / 4096) * 4096
	}

	info.ChangeTime = filetime.Timestamp(filesystem.DeterministicFileModificationTimestamp)
	info.CreationTime = filetime.Timestamp(filesystem.DeterministicFileModificationTimestamp)
	info.LastAccessTime = filetime.Timestamp(filesystem.DeterministicFileModificationTimestamp)
	lastWriteTime, present := attributes.GetLastDataModificationTime()
	if !present {
		lastWriteTime = filesystem.DeterministicFileModificationTimestamp
	}
	info.LastWriteTime = filetime.Timestamp(lastWriteTime)
}

const (
	fileAddFile         windows.ACCESS_MASK = 0x02
	fileAddSubdirectory windows.ACCESS_MASK = 0x04
	fileDeleteChild     windows.ACCESS_MASK = 0x40

	ownerDefaultPermissions windows.ACCESS_MASK = windows.DELETE |
		windows.WRITE_DAC |
		windows.WRITE_OWNER |
		windows.FILE_WRITE_ATTRIBUTES |
		windows.FILE_WRITE_EA
)

func mapPermissionToAccessMask(mode virtual.Permissions, fileType filesystem.FileType) windows.ACCESS_MASK {
	var result windows.ACCESS_MASK = windows.FILE_READ_ATTRIBUTES |
		windows.FILE_READ_EA |
		windows.READ_CONTROL |
		windows.SYNCHRONIZE
	if mode&virtual.PermissionsRead != 0 {
		result |= windows.FILE_LIST_DIRECTORY |
			windows.FILE_READ_DATA |
			windows.FILE_TRAVERSE
	}
	if mode&virtual.PermissionsWrite != 0 {
		result |= windows.FILE_APPEND_DATA |
			windows.FILE_WRITE_ATTRIBUTES |
			windows.FILE_WRITE_DATA |
			windows.FILE_WRITE_EA
		if fileType == filesystem.FileTypeDirectory {
			result |= fileAddFile |
				fileAddSubdirectory |
				fileDeleteChild
		}
	}
	if mode&virtual.PermissionsExecute != 0 {
		result |= windows.FILE_EXECUTE |
			windows.FILE_TRAVERSE
	}
	return result
}

// Calculates a security descriptor for the attributes by looking at the
// permissions and owner/group IDs in the attributes.
func toSecurityDescriptor(attributes *virtual.Attributes) (*windows.SECURITY_DESCRIPTOR, error) {
	var err error

	// Compute the owner, falling back to the current processes' user.
	var ownerSid *windows.SID
	if uid, ok := attributes.GetOwnerUserID(); ok {
		ownerSid, err = ffi.PosixMapUidToSid(uid)
		if err != nil {
			return nil, err
		}
	} else {
		ownerSid, _, err = getCurrentProcessUserAndGroup()
		if err != nil {
			return nil, err
		}
	}

	// Get group SID.
	var groupSid *windows.SID
	if gid, ok := attributes.GetOwnerGroupID(); ok {
		groupSid, err = ffi.PosixMapUidToSid(gid)
		if err != nil {
			return nil, err
		}
	} else {
		_, groupSid, err = getCurrentProcessUserAndGroup()
		if err != nil {
			return nil, err
		}
	}

	mode, ok := attributes.GetPermissions()
	if !ok {
		panic("Attributes do not contain mandatory permissions attribute")
	}

	everyoneSid, err := getEveryoneSid()
	if err != nil {
		return nil, err
	}

	permissions := mapPermissionToAccessMask(mode, attributes.GetFileType())
	aclEntries := [2]windows.EXPLICIT_ACCESS{
		// The owner has additional permissions to, for example, modify the
		// permissions
		{
			AccessPermissions: permissions | ownerDefaultPermissions,
			AccessMode:        windows.GRANT_ACCESS,
			Inheritance:       windows.NO_INHERITANCE,
			Trustee: windows.TRUSTEE{
				TrusteeForm:  windows.TRUSTEE_IS_SID,
				TrusteeValue: windows.TrusteeValueFromSID(ownerSid),
			},
		},
		// Since the VFS has the same permissions for user/group/everyone,
		// we can group most permissions under one entry.
		{
			AccessPermissions: permissions,
			AccessMode:        windows.GRANT_ACCESS,
			Inheritance:       windows.NO_INHERITANCE,
			Trustee: windows.TRUSTEE{
				TrusteeForm:  windows.TRUSTEE_IS_SID,
				TrusteeValue: windows.TrusteeValueFromSID(everyoneSid),
			},
		},
	}

	// Build the security descriptor.
	sd, err := windows.NewSecurityDescriptor()
	if err != nil {
		return nil, err
	}
	// Don't allow inheritance of security descriptors from parents.
	if err := sd.SetControl(windows.SE_DACL_PROTECTED, windows.SE_DACL_PROTECTED); err != nil {
		return nil, err
	}
	if err := sd.SetOwner(ownerSid, false); err != nil {
		return nil, err
	}
	if err := sd.SetGroup(groupSid, false); err != nil {
		return nil, err
	}
	dacl, err := windows.ACLFromEntries(aclEntries[:], nil)
	if err != nil {
		return nil, err
	}
	if err := sd.SetDACL(dacl, true, false); err != nil {
		return nil, err
	}

	return sd.ToSelfRelative()
}

// Sets the attributes of node, and stores the updated attributes in info.
func setAndGetAttributes(ctx context.Context, newAttributesMask virtual.AttributesMask, node virtual.Node, newAttributes virtual.Attributes, info *ffi.FSP_FSCTL_FILE_INFO) error {
	var attributesOut virtual.Attributes
	if newAttributesMask != 0 {
		status := node.VirtualSetAttributes(ctx, &newAttributes, AttributesMaskForWinFSPAttr, &attributesOut)
		if status != virtual.StatusOK {
			return toNTStatus(status)
		}
	} else {
		node.VirtualGetAttributes(ctx, AttributesMaskForWinFSPAttr, &attributesOut)
	}
	toWinFSPFileInfo(&attributesOut, info)
	return nil
}

type directoryResolver struct {
	ctx     context.Context
	current virtual.Directory
}

func (d *directoryResolver) OnDirectory(component path.Component) (path.GotDirectoryOrSymlink, error) {
	var attributes virtual.Attributes
	child, status := d.current.VirtualLookup(d.ctx, component, virtual.AttributesMaskFileType, &attributes)
	if status != virtual.StatusOK {
		return nil, toNTStatus(status)
	}
	d.current, _ = child.GetPair()
	if d.current == nil {
		return nil, windows.STATUS_NOT_A_DIRECTORY
	}
	return path.GotDirectory{
		Child:        d,
		IsReversible: false,
	}, nil
}

func (d *directoryResolver) OnTerminal(name path.Component) (*path.GotSymlink, error) {
	return path.OnTerminalViaOnDirectory(d, name)
}

func (directoryResolver) OnUp() (path.ComponentWalker, error) {
	return nil, windows.STATUS_OBJECT_NAME_NOT_FOUND
}

func (d *directoryResolver) OnAbsolute() (path.ComponentWalker, error) {
	return d, nil
}

func (directoryResolver) OnDriveLetter(drive rune) (path.ComponentWalker, error) {
	return nil, windows.STATUS_OBJECT_NAME_NOT_FOUND
}

func (directoryResolver) OnRelative() (path.ComponentWalker, error) {
	return nil, windows.STATUS_OBJECT_NAME_NOT_FOUND
}

func (directoryResolver) OnShare(server, share string) (path.ComponentWalker, error) {
	return nil, windows.STATUS_OBJECT_NAME_NOT_FOUND
}

// resolveDirectory resolves a path from WinFSP into a virtual.Directory. It
// is only designed to cope with directory paths from WinFSP, which are guaranteed
// to start with \ and not contain any .. components.
func (fs *FileSystem) resolveDirectory(ctx context.Context, name string) (virtual.Directory, error) {
	w := directoryResolver{
		current: fs.rootDirectory,
		ctx:     ctx,
	}
	if err := path.Resolve(path.LocalFormat.NewParser(name), &w); err != nil {
		return nil, err
	}
	return w.current, nil
}

func (FileSystem) openOrCreateDir(ctx context.Context, parent virtual.Directory, leafName path.Component, disposition uint32, attributes *virtual.Attributes) (virtual.DirectoryChild, error) {
	switch disposition {
	case windows.FILE_OPEN, windows.FILE_OPEN_IF:
		// Ty and open an existing directory
		child, status := parent.VirtualLookup(ctx, leafName, AttributesMaskForWinFSPAttr, attributes)
		if status == virtual.StatusOK {
			if directory, _ := child.GetPair(); directory == nil {
				if attributes.GetFileType() == filesystem.FileTypeSymlink {
					// If it's a symlink then we have no way of checking
					// whether the target is a directory or not, since the
					// symlink might be to a file that is outside of the VFS.
					// We therefore do not enforce that the file is a
					// directory. This is like WinFSP's fuse implementation.
					return child, nil
				}
				return virtual.DirectoryChild{}, windows.STATUS_NOT_A_DIRECTORY
			}
			return child, nil
		}
		// Fallthrough to creation.

	case windows.FILE_CREATE:
		// Fallthrough to creation.

	default:
		return virtual.DirectoryChild{}, windows.STATUS_INVALID_PARAMETER
	}

	// Create new directory.
	childDir, _, status := parent.VirtualMkdir(leafName, AttributesMaskForWinFSPAttr, attributes)
	if status != virtual.StatusOK {
		return virtual.DirectoryChild{}, toNTStatus(status)
	}
	return virtual.DirectoryChild{}.FromDirectory(childDir), nil
}

// openOrCreateFileOrDir opens an existing file or directory, or creates a new file.
func (FileSystem) openOrCreateFileOrDir(ctx context.Context, parent virtual.Directory, leafName path.Component, disposition uint32, shareMask virtual.ShareMask, createPermissions virtual.Permissions, attributes *virtual.Attributes) (virtual.DirectoryChild, virtual.ShareMask, error) {
	var createAttributes *virtual.Attributes
	var existingOptions *virtual.OpenExistingOptions

	switch disposition {
	case windows.FILE_OPEN:
		existingOptions = &virtual.OpenExistingOptions{}
	case windows.FILE_CREATE:
		createAttributes = &virtual.Attributes{}
	case windows.FILE_OPEN_IF:
		createAttributes = &virtual.Attributes{}
		existingOptions = &virtual.OpenExistingOptions{}
	case windows.FILE_OVERWRITE:
		existingOptions = &virtual.OpenExistingOptions{Truncate: true}
	case windows.FILE_OVERWRITE_IF:
		createAttributes = &virtual.Attributes{}
		existingOptions = &virtual.OpenExistingOptions{Truncate: true}
	case windows.FILE_SUPERSEDE:
		// We treat this like a standard truncate, which is not strictly
		// speaking correct: Supersede really means "delete and recreate",
		// which would mean throwing away some attributes. However, it's not
		// uncommon to treat it like O_TRUNC; some SMB implementations do
		// for example.
		createAttributes = &virtual.Attributes{}
		existingOptions = &virtual.OpenExistingOptions{Truncate: true}
	default:
		return virtual.DirectoryChild{}, 0, windows.STATUS_INVALID_PARAMETER
	}

	if createAttributes != nil && createPermissions != 0 {
		createAttributes.SetPermissions(createPermissions)
	}

	var child virtual.DirectoryChild
	leaf, _, _, status := parent.VirtualOpenChild(ctx, leafName, shareMask, createAttributes, existingOptions, AttributesMaskForWinFSPAttr, attributes)
	switch status {
	case virtual.StatusOK:
		child = virtual.DirectoryChild{}.FromLeaf(leaf)
	case virtual.StatusErrNoEnt, virtual.StatusErrIsDir, virtual.StatusErrSymlink:
		// We were asked to open a file, but the target is a directory or
		// symlink so use VirtualLookup instead.
		//
		// We also handle ErrNoEnt in the same way: some VFS implementations
		// (e.g. bonanza) return ErrNoEnt when VirtualOpenChild is called even
		// if a directory of the same name exists.
		child, status = parent.VirtualLookup(ctx, leafName, AttributesMaskForWinFSPAttr, attributes)
		// In this case, the file is not actually open so reset the shareMask.
		shareMask = 0
	}
	if status != virtual.StatusOK {
		return virtual.DirectoryChild{}, 0, toNTStatus(status)
	}
	return child, shareMask, nil
}

func (fs *FileSystem) createHandle(ctx context.Context, name string, createOptions, grantedAccess uint32, createPermissions virtual.Permissions, info *ffi.FSP_FSCTL_FILE_INFO) (uintptr, error) {
	if createOptions&unsupportedCreateOptions != 0 {
		return 0, windows.STATUS_INVALID_PARAMETER
	}
	if createOptions&fileAndDirectoryFlag == fileAndDirectoryFlag {
		// Can't be both
		return 0, windows.STATUS_INVALID_PARAMETER
	}
	disposition := (createOptions >> 24) & 0x0ff

	var attributes virtual.Attributes
	var parent virtual.Directory
	var component path.Component
	var node virtual.DirectoryChild
	var shareMask virtual.ShareMask
	parentName, leafName, err := parsePath(name)
	if err != nil {
		return 0, err
	}
	if leafName == nil {
		// This is the root: this is a bit special
		if createOptions&windows.FILE_NON_DIRECTORY_FILE != 0 {
			return 0, windows.STATUS_FILE_IS_A_DIRECTORY
		}

		switch disposition {
		case windows.FILE_OPEN, windows.FILE_OPEN_IF:
			node = virtual.DirectoryChild{}.FromDirectory(fs.rootDirectory)
			node.GetNode().VirtualGetAttributes(ctx, AttributesMaskForWinFSPAttr, &attributes)

		case windows.FILE_CREATE:
			return 0, windows.STATUS_OBJECT_NAME_COLLISION
		default:
			return 0, windows.STATUS_INVALID_PARAMETER
		}
	} else {
		parent, err = fs.resolveDirectory(ctx, parentName)
		if err != nil {
			return 0, err
		}

		// Handle dispositions through the appropriate function based on whether it's a directory or file
		if createOptions&windows.FILE_DIRECTORY_FILE != 0 {
			node, err = fs.openOrCreateDir(ctx, parent, *leafName, disposition, &attributes)
		} else {
			node, shareMask, err = fs.openOrCreateFileOrDir(ctx, parent, *leafName, disposition, toShareMask(grantedAccess), createPermissions, &attributes)
		}
		if err != nil {
			return 0, err
		}
		component = *leafName
	}

	handle := fs.openFiles.createHandle(parent, component, node, shareMask)
	toWinFSPFileInfo(&attributes, info)
	return handle, nil
}

func (FileSystem) createContext() (context.Context, error) {
	// Currently we just return a default context, but this could be extended
	// in the future to perform some authentication of requests.
	return context.Background(), nil
}

func (fs *FileSystem) Create(ref *ffi.FileSystemRef, name string, createOptions, grantedAccess, fileAttributes uint32, securityDescriptor *windows.SECURITY_DESCRIPTOR, allocationSize uint64, info *ffi.FSP_FSCTL_FILE_INFO) (uintptr, error) {
	ctx, err := fs.createContext()
	if err != nil {
		return 0, err
	}
	// We need to set the default permissions for this file. We don't have
	// much to set this based on as Windows doesn't have an equivalent of
	// umask; instead in Windows files normally inherit their permissions
	// from the parent directory. However this doesn't work well with the VFS
	// as it stores permissions per node and nodes always have permissions
	// attached.
	createPermissions := virtual.PermissionsExecute | virtual.PermissionsRead
	if fileAttributes&windows.FILE_ATTRIBUTE_READONLY == 0 {
		createPermissions |= virtual.PermissionsWrite
	}
	return fs.createHandle(ctx, name, createOptions, grantedAccess, createPermissions, info)
}

func (fs *FileSystem) Open(ref *ffi.FileSystemRef, name string, createOptions, grantedAccess uint32, info *ffi.FSP_FSCTL_FILE_INFO) (uintptr, error) {
	ctx, err := fs.createContext()
	if err != nil {
		return 0, err
	}
	return fs.createHandle(ctx, name, createOptions, grantedAccess, 0, info)
}

func (fs *FileSystem) Close(ref *ffi.FileSystemRef, handle uintptr) {
	node, err := fs.openFiles.removeHandle(handle)
	if err != nil {
		return
	}
	_, leaf := node.node.GetPair()
	if leaf != nil && node.shareMask != 0 {
		leaf.VirtualClose(node.shareMask)
	}
}

// Writes directory entries into a buffer in the WinFSP format.
type dirBufferWriter struct {
	buffer       []byte
	cookieOffset uint64
	exhausted    bool
	writtenBytes int
}

// Adds a directory to the buffer, returning false if the buffer has been
// exhausted.
func (dc *dirBufferWriter) append(name string, nextCookie uint64, info *ffi.FSP_FSCTL_FILE_INFO) bool {
	written := ffi.FileSystemAddDirInfo(name, nextCookie, info, dc.buffer[dc.writtenBytes:])
	if written == 0 {
		// The buffer is too small.
		dc.exhausted = true
		return false
	}
	dc.writtenBytes += written
	return true
}

func (dc *dirBufferWriter) ReportEntry(nextCookie uint64, name path.Component, child virtual.DirectoryChild, attributes *virtual.Attributes) bool {
	var info ffi.FSP_FSCTL_FILE_INFO
	toWinFSPFileInfo(attributes, &info)
	return dc.append(name.String(), nextCookie+dc.cookieOffset, &info)
}

func (fs *FileSystem) ReadDirectoryOffset(ref *ffi.FileSystemRef, handle uintptr, pattern *uint16, offset uint64, buffer []byte) (int, error) {
	ctx, err := fs.createContext()
	if err != nil {
		return 0, err
	}

	directory, err := fs.openFiles.nodeForDirectoryHandle(handle)
	if err != nil {
		return 0, err
	}

	dc := dirBufferWriter{
		buffer: buffer,
	}

	var readDirCookie uint64
	if directory == fs.rootDirectory {
		readDirCookie = offset
	} else {
		// For everything aside from the root we have to include "." and ".."
		// entries. Thus we offset the VFS cookies by 2.
		dc.cookieOffset = 2
		if offset >= dc.cookieOffset {
			readDirCookie = offset - dc.cookieOffset
		}

		if offset <= 0 {
			if !dc.append(".", 1, &ffi.FSP_FSCTL_FILE_INFO{
				FileAttributes: windows.FILE_ATTRIBUTE_DIRECTORY,
			}) {
				return dc.writtenBytes, nil
			}
		}
		if offset <= 1 {
			if !dc.append("..", 2, &ffi.FSP_FSCTL_FILE_INFO{
				FileAttributes: windows.FILE_ATTRIBUTE_DIRECTORY,
			}) {
				return dc.writtenBytes, nil
			}
		}
	}

	status := directory.VirtualReadDir(ctx, readDirCookie, AttributesMaskForWinFSPAttr, &dc)
	if status != virtual.StatusOK {
		return 0, toNTStatus(status)
	}
	if !dc.exhausted {
		// Must have reached the end: add the null entry.
		dc.append("", 0, nil)
	}
	return dc.writtenBytes, nil
}

func (fs *FileSystem) GetFileInfo(ref *ffi.FileSystemRef, handle uintptr, info *ffi.FSP_FSCTL_FILE_INFO) error {
	ctx, err := fs.createContext()
	if err != nil {
		return err
	}
	node, err := fs.openFiles.nodeForHandle(handle)
	if err != nil {
		return err
	}
	var attributes virtual.Attributes
	node.GetNode().VirtualGetAttributes(ctx, AttributesMaskForWinFSPAttr, &attributes)
	toWinFSPFileInfo(&attributes, info)
	return nil
}

func (fs *FileSystem) GetVolumeInfo(ref *ffi.FileSystemRef, info *ffi.FSP_FSCTL_VOLUME_INFO) error {
	// Sizes are 0 as the VFS does not maintain them.
	info.FreeSize = 0
	info.TotalSize = 0
	info.VolumeLabelLength = 2 * uint16(copy(info.VolumeLabel[:], fs.label[:]))
	return nil
}

func (fs *FileSystem) Overwrite(ref *ffi.FileSystemRef, handle uintptr, winfspAttributes uint32, replaceAttributes bool, allocationSize uint64, info *ffi.FSP_FSCTL_FILE_INFO) error {
	ctx, err := fs.createContext()
	if err != nil {
		return err
	}

	openNode, err := fs.openFiles.nodeForHandle(handle)
	if err != nil {
		return err
	}
	_, file := openNode.GetPair()
	if file == nil {
		return windows.STATUS_FILE_IS_A_DIRECTORY
	}

	var newAttributes virtual.Attributes
	var newAttributesMask virtual.AttributesMask
	if !replaceAttributes {
		// Then initialise based on the current attributes.
		file.VirtualGetAttributes(ctx, AttributesMaskForWinFSPAttr, &newAttributes)
	}

	// Add additional attributes
	toVirtualAttributes(openNode, winfspAttributes, &newAttributes, &newAttributesMask)
	newAttributesMask |= virtual.AttributesMaskSizeBytes
	newAttributes.SetSizeBytes(0)

	return setAndGetAttributes(ctx, newAttributesMask, file, newAttributes, info)
}

func (fs *FileSystem) Read(ref *ffi.FileSystemRef, handle uintptr, buffer []byte, offset uint64) (int, error) {
	_, err := fs.createContext()
	if err != nil {
		return 0, err
	}

	file, err := fs.openFiles.nodeForLeafHandle(handle)
	if err != nil {
		return 0, err
	}
	read, eof, status := file.VirtualRead(buffer, offset)
	if status != virtual.StatusOK {
		return read, toNTStatus(status)
	}
	if read == 0 && eof {
		// Must have tried to read at the end of the file
		return 0, windows.STATUS_END_OF_FILE
	}
	return read, nil
}

func (fs *FileSystem) Write(ref *ffi.FileSystemRef, handle uintptr, buf []byte, offset uint64, writeToEndOfFile, constrainedIo bool, info *ffi.FSP_FSCTL_FILE_INFO) (int, error) {
	ctx, err := fs.createContext()
	if err != nil {
		return 0, err
	}

	file, err := fs.openFiles.nodeForLeafHandle(handle)
	if err != nil {
		return 0, err
	}

	if constrainedIo || writeToEndOfFile {
		var attributes virtual.Attributes
		file.VirtualGetAttributes(ctx, virtual.AttributesMaskSizeBytes, &attributes)
		sizeBytes, ok := attributes.GetSizeBytes()
		if !ok {
			return 0, windows.STATUS_INVALID_PARAMETER
		}

		if constrainedIo {
			// Then constrain the buffer to the file size.
			if offset >= sizeBytes {
				return 0, nil
			}
			if offset+uint64(len(buf)) > sizeBytes {
				buf = buf[:sizeBytes-offset]
			}
		} else if writeToEndOfFile {
			offset = sizeBytes
		}
	}

	written, status := file.VirtualWrite(buf, offset)
	if status != virtual.StatusOK {
		return written, toNTStatus(status)
	}

	// Update file info with new attributes after write
	var attributes virtual.Attributes
	file.VirtualGetAttributes(ctx, AttributesMaskForWinFSPAttr, &attributes)
	toWinFSPFileInfo(&attributes, info)

	return written, nil
}

func (fs *FileSystem) Flush(ref *ffi.FileSystemRef, handle uintptr, info *ffi.FSP_FSCTL_FILE_INFO) error {
	// Like the other VFS frontends (e.g. FUSE) we don't actually implement flush.
	ctx, err := fs.createContext()
	if err != nil {
		return err
	}

	// Check if it's a volume flush (file handle is 0)
	if handle == 0 {
		return nil
	}

	openNode, err := fs.openFiles.nodeForHandle(handle)
	if err != nil {
		return err
	}
	var attributes virtual.Attributes
	openNode.GetNode().VirtualGetAttributes(ctx, AttributesMaskForWinFSPAttr, &attributes)
	toWinFSPFileInfo(&attributes, info)
	return nil
}

func (fs *FileSystem) SetBasicInfo(ref *ffi.FileSystemRef, handle uintptr, flags ffi.SetBasicInfoFlags, attribute uint32, creationTime, lastAccessTime, lastWriteTime, changeTime uint64, info *ffi.FSP_FSCTL_FILE_INFO) error {
	ctx, err := fs.createContext()
	if err != nil {
		return err
	}

	openNode, err := fs.openFiles.nodeForHandle(handle)
	if err != nil {
		return err
	}
	node := openNode.GetNode()

	var newAttributes virtual.Attributes
	attributesMask := virtual.AttributesMask(0)

	toVirtualAttributes(openNode, attribute, &newAttributes, &attributesMask)

	if flags&ffi.SetBasicInfoLastWriteTime != 0 {
		attributesMask |= virtual.AttributesMaskLastDataModificationTime
		newAttributes.SetLastDataModificationTime(filetimeToTime(lastWriteTime))
	}

	return setAndGetAttributes(ctx, attributesMask, node, newAttributes, info)
}

func (fs *FileSystem) SetVolumeLabel(ref *ffi.FileSystemRef, volumeLabel string, info *ffi.FSP_FSCTL_VOLUME_INFO) error {
	utf16, err := windows.UTF16FromString(volumeLabel)
	if err != nil {
		return err
	}
	fs.label = utf16
	return nil
}

func (fs *FileSystem) SetFileSize(ref *ffi.FileSystemRef, handle uintptr, newSize uint64, setAllocationSize bool, info *ffi.FSP_FSCTL_FILE_INFO) error {
	ctx, err := fs.createContext()
	if err != nil {
		return err
	}

	file, err := fs.openFiles.nodeForLeafHandle(handle)
	if err != nil {
		return err
	}

	var setSize bool
	if setAllocationSize {
		// We need to ensure that file size is less than the allocation size,
		// so might still need to do truncation here even though we don't
		// maintain allocation size in the VFS.
		var attributes virtual.Attributes
		file.VirtualGetAttributes(ctx, virtual.AttributesMaskSizeBytes, &attributes)
		if sizeBytes, ok := attributes.GetSizeBytes(); ok && sizeBytes > newSize {
			// Truncate the file to the new size
			setSize = true
		}
	} else {
		setSize = true
	}

	var newAttributes virtual.Attributes
	var attributesMask virtual.AttributesMask
	if setSize {
		newAttributes.SetSizeBytes(newSize)
		attributesMask |= virtual.AttributesMaskSizeBytes
	}
	return setAndGetAttributes(ctx, attributesMask, file, newAttributes, info)
}

func (fs *FileSystem) CanDelete(ref *ffi.FileSystemRef, handle uintptr, name string) error {
	ctx, err := fs.createContext()
	if err != nil {
		return err
	}

	openNode, err := fs.openFiles.nodeForHandle(handle)
	if err != nil {
		return err
	}

	directory, _ := openNode.GetPair()
	if directory == nil {
		// Leaves can always be deleted.
		return nil
	}

	// For directories, check if they are empty.
	isEmpty := true
	status := directory.VirtualReadDir(ctx, 0, 0, &emptyDirectoryChecker{&isEmpty})
	if status != virtual.StatusOK {
		return toNTStatus(status)
	}
	if !isEmpty {
		return windows.STATUS_DIRECTORY_NOT_EMPTY
	}
	return nil
}

type emptyDirectoryChecker struct {
	isEmpty *bool
}

func (e *emptyDirectoryChecker) ReportEntry(nextCookie uint64, name path.Component, child virtual.DirectoryChild, attributes *virtual.Attributes) bool {
	*e.isEmpty = false
	// Stop iterating.
	return false
}

func (fs *FileSystem) Cleanup(ref *ffi.FileSystemRef, handle uintptr, name string, cleanupFlags uint32) {
	ctx, err := fs.createContext()
	if err != nil {
		return
	}

	openNode, err := fs.openFiles.trackedNodeForHandle(handle)
	if err != nil {
		return
	}

	var attributes virtual.Attributes
	var attributesMask virtual.AttributesMask

	if cleanupFlags&ffi.FspCleanupSetLastWriteTime != 0 {
		attributes.SetLastDataModificationTime(time.Now())
		attributesMask |= virtual.AttributesMaskLastDataModificationTime
	}

	if attributesMask != 0 {
		var outAttributes virtual.Attributes
		openNode.node.GetNode().VirtualSetAttributes(ctx, &attributes, attributesMask, &outAttributes)
	}

	// Check if we should delete the file.
	if cleanupFlags&ffi.FspCleanupDelete != 0 && openNode.parent != nil {
		directory, _ := openNode.node.GetPair()
		isDirectory := directory != nil
		_, fileName, err := parsePath(name)
		if err == nil && fileName != nil {
			openNode.parent.VirtualRemove(*fileName, isDirectory, !isDirectory)
		}
	}
}

func (fs *FileSystem) GetDirInfoByName(ref *ffi.FileSystemRef, parentHandle uintptr, name string, dirInfo *ffi.FSP_FSCTL_DIR_INFO) error {
	ctx, err := fs.createContext()
	if err != nil {
		return err
	}

	// Get the parent directory
	parentDirectory, err := fs.openFiles.nodeForDirectoryHandle(parentHandle)
	if err != nil {
		return err
	}

	var attributes virtual.Attributes
	_, status := parentDirectory.VirtualLookup(ctx, path.MustNewComponent(name), AttributesMaskForWinFSPAttr, &attributes)
	if status != virtual.StatusOK {
		return toNTStatus(status)
	}

	toWinFSPFileInfo(&attributes, &dirInfo.FileInfo)

	return nil
}

func (fs *FileSystem) GetSecurity(ref *ffi.FileSystemRef, handle uintptr) (*windows.SECURITY_DESCRIPTOR, error) {
	ctx, err := fs.createContext()
	if err != nil {
		return nil, err
	}
	node, err := fs.openFiles.nodeForHandle(handle)
	if err != nil {
		return nil, err
	}
	var attributes virtual.Attributes
	node.GetNode().VirtualGetAttributes(ctx, AttributesMaskForWinFSPAttr, &attributes)
	return toSecurityDescriptor(&attributes)
}

func (FileSystem) containsReparsePoint(ref *ffi.FileSystemRef, fileName string) bool {
	found, _, err := ffi.FileSystemFindReparsePoint(ref, fileName)
	if found && err == nil {
		return found
	}
	return false
}

func (fs *FileSystem) GetSecurityByName(ref *ffi.FileSystemRef, name string, flags ffi.GetSecurityByNameFlags) (uint32, *windows.SECURITY_DESCRIPTOR, error) {
	ctx, err := fs.createContext()
	if err != nil {
		return 0, nil, err
	}

	var attributes virtual.Attributes
	parentName, leafName, err := parsePath(name)
	if err != nil {
		return 0, nil, err
	}
	if leafName == nil {
		fs.rootDirectory.VirtualGetAttributes(ctx, AttributesMaskForWinFSPAttr, &attributes)
	} else {
		parent, err := fs.resolveDirectory(ctx, parentName)
		if err != nil {
			if fs.containsReparsePoint(ref, name) {
				return 0, nil, windows.STATUS_REPARSE
			}
			return 0, nil, err
		}
		_, status := parent.VirtualLookup(ctx, *leafName, AttributesMaskForWinFSPAttr, &attributes)
		if status != virtual.StatusOK {
			if fs.containsReparsePoint(ref, name) {
				return 0, nil, windows.STATUS_REPARSE
			}
			return 0, nil, toNTStatus(status)
		}
	}

	if flags == ffi.GetExistenceOnly {
		return 0, nil, nil
	}

	var sd *windows.SECURITY_DESCRIPTOR
	if (flags & ffi.GetSecurityByName) != 0 {
		sd, err = toSecurityDescriptor(&attributes)
		if err != nil {
			return 0, nil, err
		}
	}
	return toWinFSPFileAttributes(&attributes), sd, nil
}

// Splits the last component from the rest of the path.
func parsePath(name string) (parentName string, leafName *path.Component, err error) {
	if name == "\\" {
		return "\\", nil, nil
	}
	// Windows allows both forward and backward slashes:
	// https://learn.microsoft.com/en-us/dotnet/standard/io/file-path-formats
	// Most of the time these are normalised to \ before reaching us, but
	// it seems this is not true when we are handling reparse points.
	lastSeparator := strings.LastIndexAny(name, "\\/")
	if lastSeparator < 0 {
		return "", nil, windows.STATUS_INVALID_PARAMETER
	}
	component, ok := path.NewComponent(name[lastSeparator+1:])
	if !ok {
		return "", nil, windows.STATUS_INVALID_PARAMETER
	}
	return name[:lastSeparator+1], &component, nil
}

func (fs *FileSystem) Rename(ref *ffi.FileSystemRef, handle uintptr, source, target string, replaceIfExist bool) error {
	ctx, err := fs.createContext()
	if err != nil {
		return err
	}

	openNode, err := fs.openFiles.trackedNodeForHandle(handle)
	if err != nil {
		return err
	}
	if openNode.parent == nil {
		return windows.STATUS_INVALID_PARAMETER
	}

	_, sourceLeafName, err := parsePath(source)
	if err != nil {
		return err
	}
	if sourceLeafName == nil {
		return windows.STATUS_INVALID_PARAMETER
	}
	targetParentName, targetLeafName, err := parsePath(target)
	if err != nil {
		return err
	}
	if targetLeafName == nil {
		return windows.STATUS_INVALID_PARAMETER
	}
	targetParent, err := fs.resolveDirectory(ctx, targetParentName)
	if err != nil {
		return err
	}

	// Check if target exists and handle replaceIfExist.
	var attributes virtual.Attributes
	if existingNode, status := targetParent.VirtualLookup(ctx, *targetLeafName, virtual.AttributesMaskFileType, &attributes); status == virtual.StatusOK {
		if existingNode == openNode.node {
			// Ok
		} else {
			if attributes.GetFileType() == filesystem.FileTypeDirectory {
				// Windows never allows renames over an existing directory.
				return windows.STATUS_ACCESS_DENIED
			}
			if !replaceIfExist {
				return windows.STATUS_OBJECT_NAME_COLLISION
			}
		}
	}

	// Perform the rename operation
	if _, _, status := openNode.parent.VirtualRename(*sourceLeafName, targetParent, *targetLeafName); status != virtual.StatusOK {
		return toNTStatus(status)
	}
	return nil
}

func (fs *FileSystem) SetSecurity(ref *ffi.FileSystemRef, handle uintptr, info windows.SECURITY_INFORMATION, modificationDesc *windows.SECURITY_DESCRIPTOR) error {
	ctx, err := fs.createContext()
	if err != nil {
		return err
	}

	openNode, err := fs.openFiles.nodeForHandle(handle)
	if err != nil {
		return err
	}

	var attributes virtual.Attributes
	openNode.GetNode().VirtualGetAttributes(ctx, AttributesMaskForWinFSPAttr, &attributes)

	currentSd, err := toSecurityDescriptor(&attributes)
	if err != nil {
		return err
	}
	newSd, err := ffi.SetSecurityDescriptor(currentSd, info, modificationDesc)
	if err != nil {
		return err
	}
	defer ffi.DeleteSecurityDescriptor(newSd)

	uid, gid, mode, err := ffi.PosixMapSecurityDescriptorToPermissions(newSd)
	if err != nil {
		return err
	}

	attributes.SetOwnerGroupID(gid)
	attributes.SetOwnerUserID(uid)
	attributes.SetPermissions(virtual.NewPermissionsFromMode(mode))
	var outAttributes virtual.Attributes
	openNode.GetNode().VirtualSetAttributes(ctx,
		&attributes,
		virtual.AttributesMaskOwnerGroupID|virtual.AttributesMaskOwnerUserID|virtual.AttributesMaskPermissions,
		&outAttributes)

	return nil
}

func (FileSystem) DeleteReparsePoint(ref *ffi.FileSystemRef, file uintptr, name string, buffer []byte) error {
	// We can't support this: we can only support deleting the file, not
	// deleting the symlink and thus converting it into a regular file.
	// Thus we do exactly what the WinFSP fuse plugin does.
	return windows.STATUS_ACCESS_DENIED
}

// FillSymlinkReparseBuffer populates buffer with a
// windowsext.REPARSE_DATA_BUFFER for a symlink that points to target with
// the passed flags. It returns the number of bytes written to the buffer.
func FillSymlinkReparseBuffer(target string, flags uint32, buffer []byte) (int, error) {
	targetUTF16, err := windows.UTF16FromString(string(target))
	if err != nil {
		return 0, err
	}

	// utf-16 encoded so 2 bytes per character; no null terminator needed.
	targetUTF16Len := len(targetUTF16) - 1
	targetUTF16Bytes := targetUTF16Len * 2
	symbolicLinkReparseSize := int(unsafe.Sizeof(windowsext.SymbolicLinkReparseBuffer{})) -
		// Exclude the PathBuffer member.
		2 +
		// Two copies of the target path.
		targetUTF16Bytes*2
	requiredSize := int(unsafe.Sizeof(windowsext.REPARSE_DATA_BUFFER_HEADER{})) + symbolicLinkReparseSize
	if len(buffer) < requiredSize {
		return 0, windows.STATUS_BUFFER_TOO_SMALL
	}

	rdb := (*windowsext.REPARSE_DATA_BUFFER)(unsafe.Pointer(&buffer[0]))
	rdb.ReparseTag = windows.IO_REPARSE_TAG_SYMLINK
	rdb.ReparseDataLength = uint16(symbolicLinkReparseSize)
	rdb.Reserved = 0

	slrb := (*windowsext.SymbolicLinkReparseBuffer)(unsafe.Pointer(&rdb.DUMMYUNIONNAME))
	slrb.Flags = flags
	slrb.SubstituteNameOffset = 0
	slrb.SubstituteNameLength = uint16(targetUTF16Bytes)
	slrb.PrintNameOffset = uint16(targetUTF16Bytes)
	slrb.PrintNameLength = uint16(targetUTF16Bytes)

	pathBuffer := unsafe.Slice(&slrb.PathBuffer[0], 2*targetUTF16Bytes)
	copy(pathBuffer[0:targetUTF16Len], targetUTF16)
	copy(pathBuffer[targetUTF16Len:targetUTF16Len*2], targetUTF16)

	return requiredSize, nil
}

func getReparsePointForLeaf(ctx context.Context, leaf virtual.Leaf, buffer []byte) (int, error) {
	target, status := leaf.VirtualReadlink(ctx)
	if status != virtual.StatusOK {
		return 0, toNTStatus(status)
	}
	if buffer == nil {
		// Then we were just interested in knowing if this was a reparse point.
		return 0, nil
	}

	// Normalize to backslashes. After reparse resolution the
	// I/O manager re-issues the request with the substituted
	// path. FspFileSystemFindReparsePoint splits on backslashes
	// only, so forward slashes cause it to misidentify the
	// reparse point's depth, breaking chained symlinks.
	targetParser := path.LocalFormat.NewParser(string(target))
	cleanPathBuilder, scopeWalker := path.EmptyBuilder.Join(path.VoidScopeWalker)
	if err := path.Resolve(targetParser, scopeWalker); err != nil {
		return 0, err
	}
	var flags uint32
	switch cleanPathBuilder.WindowsPathKind() {
	case path.WindowsPathKindRelative, path.WindowsPathKindDriveRelative:
		flags = uint32(windowsext.SYMLINK_FLAG_RELATIVE)
	case path.WindowsPathKindAbsolute:
	}
	targetStr, err := cleanPathBuilder.GetWindowsString(path.WindowsPathFormatStandard)
	if err != nil {
		return 0, err
	}
	return FillSymlinkReparseBuffer(targetStr, flags, buffer)
}

func (fs *FileSystem) GetReparsePoint(ref *ffi.FileSystemRef, file uintptr, name string, buffer []byte) (int, error) {
	ctx, err := fs.createContext()
	if err != nil {
		return 0, err
	}
	leaf, err := fs.openFiles.nodeForLeafHandle(file)
	if err != nil {
		return 0, err
	}
	return getReparsePointForLeaf(ctx, leaf, buffer)
}

func (fs *FileSystem) GetReparsePointByName(ref *ffi.FileSystemRef, name string, isDirectory bool, buffer []byte) (int, error) {
	ctx, err := fs.createContext()
	if err != nil {
		return 0, err
	}
	parentName, leafName, err := parsePath(name)
	if err != nil {
		return 0, err
	}
	if leafName == nil {
		return 0, windows.STATUS_NOT_A_REPARSE_POINT
	}
	parent, err := fs.resolveDirectory(ctx, parentName)
	if err != nil {
		return 0, err
	}
	var attributes virtual.Attributes
	node, status := parent.VirtualLookup(ctx, *leafName, AttributesMaskForWinFSPAttr, &attributes)
	if status != virtual.StatusOK {
		return 0, toNTStatus(status)
	}
	if attributes.GetFileType() != filesystem.FileTypeSymlink {
		return 0, windows.STATUS_NOT_A_REPARSE_POINT
	}
	_, leaf := node.GetPair()
	if leaf == nil {
		return 0, windows.STATUS_NOT_A_REPARSE_POINT
	}
	return getReparsePointForLeaf(ctx, leaf, buffer)
}

func (fs *FileSystem) SetReparsePoint(ref *ffi.FileSystemRef, handle uintptr, name string, buffer []byte) error {
	ctx, err := fs.createContext()
	if err != nil {
		return err
	}

	node, err := fs.openFiles.trackedNodeForHandle(handle)
	if err != nil {
		return windows.STATUS_INVALID_HANDLE
	}
	if node.parent == nil {
		// This is the root folder. We can't set a reparse point for this.
		return windows.STATUS_INVALID_PARAMETER
	}

	if len(buffer) < int(unsafe.Sizeof(windowsext.REPARSE_DATA_BUFFER{})) {
		return windows.STATUS_INVALID_PARAMETER
	}
	reparseBuffer := (*windowsext.REPARSE_DATA_BUFFER)(unsafe.Pointer(&buffer[0]))

	switch reparseBuffer.ReparseTag {
	case windows.IO_REPARSE_TAG_SYMLINK:
		// Handle symbolic link reparse point
		if reparseBuffer.ReparseDataLength < uint16(unsafe.Sizeof(windowsext.SymbolicLinkReparseBuffer{})) {
			return windows.STATUS_INVALID_PARAMETER
		}
		symbolicLinkBuffer := (*windowsext.SymbolicLinkReparseBuffer)(unsafe.Pointer(&reparseBuffer.DUMMYUNIONNAME))
		targetPath := symbolicLinkBuffer.Path()
		if _, s := node.parent.VirtualRemove(node.name, true, true); s != virtual.StatusOK {
			return toNTStatus(s)
		}
		var outAttributes virtual.Attributes
		if _, _, s := node.parent.VirtualSymlink(ctx, []byte(targetPath), node.name, virtual.AttributesMaskFileType, &outAttributes); s != virtual.StatusOK {
			return toNTStatus(s)
		}

		return nil

	default:
		return windows.STATUS_IO_REPARSE_TAG_MISMATCH
	}
}

// Check we implement the relevant interfaces as go-winfsp uses which
// interfaces we implement to determine capabilities.
var (
	_ ffi.BehaviourBase                  = (*FileSystem)(nil)
	_ ffi.BehaviourCanDelete             = (*FileSystem)(nil)
	_ ffi.BehaviourCleanup               = (*FileSystem)(nil)
	_ ffi.BehaviourCreate                = (*FileSystem)(nil)
	_ ffi.BehaviourDeleteReparsePoint    = (*FileSystem)(nil)
	_ ffi.BehaviourFlush                 = (*FileSystem)(nil)
	_ ffi.BehaviourGetDirInfoByName      = (*FileSystem)(nil)
	_ ffi.BehaviourGetFileInfo           = (*FileSystem)(nil)
	_ ffi.BehaviourGetReparsePoint       = (*FileSystem)(nil)
	_ ffi.BehaviourGetReparsePointByName = (*FileSystem)(nil)
	_ ffi.BehaviourGetSecurity           = (*FileSystem)(nil)
	_ ffi.BehaviourGetSecurityByName     = (*FileSystem)(nil)
	_ ffi.BehaviourGetVolumeInfo         = (*FileSystem)(nil)
	_ ffi.BehaviourOverwrite             = (*FileSystem)(nil)
	_ ffi.BehaviourRead                  = (*FileSystem)(nil)
	_ ffi.BehaviourReadDirectoryOffset   = (*FileSystem)(nil)
	_ ffi.BehaviourRename                = (*FileSystem)(nil)
	_ ffi.BehaviourSetBasicInfo          = (*FileSystem)(nil)
	_ ffi.BehaviourSetFileSize           = (*FileSystem)(nil)
	_ ffi.BehaviourSetReparsePoint       = (*FileSystem)(nil)
	_ ffi.BehaviourSetSecurity           = (*FileSystem)(nil)
	_ ffi.BehaviourSetVolumeLabel        = (*FileSystem)(nil)
	_ ffi.BehaviourWrite                 = (*FileSystem)(nil)
)
