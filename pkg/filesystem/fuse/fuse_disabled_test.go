// +build freebsd windows

package fuse_test

import (
	"testing"
)

// List of tests that should be disabled on platforms that don't support
// FUSE. We should list these here, because rules_go generates a
// testmain.go file that lists all tests, regardless of whether they are
// enabled the platform in question.

func TestContentAddressableStorageFileGetXAttr(t *testing.T)                     {}
func TestDefaultAttributesInjectingRawFileSystem(t *testing.T)                   {}
func TestInMemoryDirectoryEnterBadName(t *testing.T)                             {}
func TestInMemoryDirectoryEnterFile(t *testing.T)                                {}
func TestInMemoryDirectoryEnterNonExistent(t *testing.T)                         {}
func TestInMemoryDirectoryEnterSuccess(t *testing.T)                             {}
func TestInMemoryDirectoryFUSECreateAllocationFailure(t *testing.T)              {}
func TestInMemoryDirectoryFUSECreateDirectoryExists(t *testing.T)                {}
func TestInMemoryDirectoryFUSECreateFileExists(t *testing.T)                     {}
func TestInMemoryDirectoryFUSECreateInRemovedDirectory(t *testing.T)             {}
func TestInMemoryDirectoryFUSECreateOpenFailure(t *testing.T)                    {}
func TestInMemoryDirectoryFUSECreateSuccess(t *testing.T)                        {}
func TestInMemoryDirectoryFUSEGetAttr(t *testing.T)                              {}
func TestInMemoryDirectoryFUSELinkExists(t *testing.T)                           {}
func TestInMemoryDirectoryFUSELinkInRemovedDirectory(t *testing.T)               {}
func TestInMemoryDirectoryFUSELinkSuccess(t *testing.T)                          {}
func TestInMemoryDirectoryFUSELookup(t *testing.T)                               {}
func TestInMemoryDirectoryFUSEMknodExists(t *testing.T)                          {}
func TestInMemoryDirectoryFUSEMknodPermissionDenied(t *testing.T)                {}
func TestInMemoryDirectoryFUSEMknodSuccess(t *testing.T)                         {}
func TestInMemoryDirectoryFUSEReadDir(t *testing.T)                              {}
func TestInMemoryDirectoryFUSEReadDirPlus(t *testing.T)                          {}
func TestInMemoryDirectoryFUSERenameCrossDevice1(t *testing.T)                   {}
func TestInMemoryDirectoryFUSERenameCrossDevice2(t *testing.T)                   {}
func TestInMemoryDirectoryFUSERenameDirectoryInRemovedDirectory(t *testing.T)    {}
func TestInMemoryDirectoryFUSERenameDirectoryTwice(t *testing.T)                 {}
func TestInMemoryDirectoryFUSERenameFileInRemovedDirectory(t *testing.T)         {}
func TestInMemoryDirectoryFUSERenameSelfDirectory(t *testing.T)                  {}
func TestInMemoryDirectoryFUSERenameSelfFile(t *testing.T)                       {}
func TestInMemoryDirectoryInstallHooks(t *testing.T)                             {}
func TestInMemoryDirectoryLstatBadName(t *testing.T)                             {}
func TestInMemoryDirectoryLstatDirectory(t *testing.T)                           {}
func TestInMemoryDirectoryLstatFile(t *testing.T)                                {}
func TestInMemoryDirectoryLstatNonExistent(t *testing.T)                         {}
func TestInMemoryDirectoryMergeDirectoryContentsInRemovedDirectory(t *testing.T) {}
func TestInMemoryDirectoryMergeDirectoryContentsSuccess(t *testing.T)            {}
func TestInMemoryDirectoryMkdirBadName(t *testing.T)                             {}
func TestInMemoryDirectoryMkdirExisting(t *testing.T)                            {}
func TestInMemoryDirectoryMkdirInRemovedDirectory(t *testing.T)                  {}
func TestInMemoryDirectoryMkdirSuccess(t *testing.T)                             {}
func TestInMemoryDirectoryMknodBadName(t *testing.T)                             {}
func TestInMemoryDirectoryMknodExisting(t *testing.T)                            {}
func TestInMemoryDirectoryMknodSuccessCharacterDevice(t *testing.T)              {}
func TestInMemoryDirectoryReadDir(t *testing.T)                                  {}
func TestInMemoryDirectoryReadlinkBadName(t *testing.T)                          {}
func TestInMemoryDirectoryReadlinkDirectory(t *testing.T)                        {}
func TestInMemoryDirectoryReadlinkFile(t *testing.T)                             {}
func TestInMemoryDirectoryReadlinkNonExistent(t *testing.T)                      {}
func TestInMemoryDirectoryReadlinkSuccess(t *testing.T)                          {}
func TestInMemoryDirectoryRemoveBadName(t *testing.T)                            {}
func TestInMemoryDirectoryRemoveDirectory(t *testing.T)                          {}
func TestInMemoryDirectoryRemoveDirectoryNotEmpty(t *testing.T)                  {}
func TestInMemoryDirectoryRemoveFile(t *testing.T)                               {}
func TestInMemoryDirectoryRemoveNonExistent(t *testing.T)                        {}
func TestInMemoryDirectoryUploadFile(t *testing.T)                               {}
func TestPoolBackedFileAllocatorFUSEOpenStaleAfterClose(t *testing.T)            {}
func TestPoolBackedFileAllocatorFUSEOpenStaleAfterUnlink(t *testing.T)           {}
func TestPoolBackedFileAllocatorFUSEReadEOF(t *testing.T)                        {}
func TestPoolBackedFileAllocatorFUSEReadFailure(t *testing.T)                    {}
func TestPoolBackedFileAllocatorFUSETruncateFailure(t *testing.T)                {}
func TestPoolBackedFileAllocatorFUSEUploadFile(t *testing.T)                     {}
func TestPoolBackedFileAllocatorFUSEWriteFailure(t *testing.T)                   {}
func TestSimpleRawFileSystemForget(t *testing.T)                                 {}
func TestSimpleRawFileSystemGetAttr(t *testing.T)                                {}
func TestSimpleRawFileSystemInit(t *testing.T)                                   {}
func TestSimpleRawFileSystemLookup(t *testing.T)                                 {}
func TestSimpleRawFileSystemMkdir(t *testing.T)                                  {}
func TestSimpleRawFileSystemMknod(t *testing.T)                                  {}
func TestSimpleRawFileSystemReadDir(t *testing.T)                                {}
func TestSimpleRawFileSystemReadDirPlus(t *testing.T)                            {}
func TestSimpleRawFileSystemSetAttr(t *testing.T)                                {}
func TestSimpleRawFileSystemStatFs(t *testing.T)                                 {}
