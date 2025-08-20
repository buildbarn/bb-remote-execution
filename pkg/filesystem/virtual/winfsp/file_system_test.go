//go:build windows
// +build windows

package winfsp_test

import (
	"context"
	"fmt"
	"testing"
	"time"
	"unsafe"

	ffi "github.com/aegistudio/go-winfsp"
	"github.com/aegistudio/go-winfsp/filetime"
	"github.com/buildbarn/bb-remote-execution/internal/mock"
	"github.com/buildbarn/bb-remote-execution/pkg/filesystem/virtual"
	"github.com/buildbarn/bb-remote-execution/pkg/filesystem/virtual/winfsp"
	"github.com/buildbarn/bb-storage/pkg/filesystem"
	"github.com/buildbarn/bb-storage/pkg/filesystem/path"
	"github.com/buildbarn/bb-storage/pkg/filesystem/windowsext"
	"github.com/stretchr/testify/require"

	"golang.org/x/sys/windows"

	"go.uber.org/mock/gomock"
)

// Test suite for winfspFileSystem that doesn't require WinFSP to be installed
// These tests verify the core filesystem logic by directly testing the
// winfspFileSystem struct methods using mock objects.

type directoryEntry struct {
	name       string
	fileInfo   ffi.FSP_FSCTL_FILE_INFO
	nextOffset uint64
}

// parseDirectoryBuffer parses a buffer containing FSP_FSCTL_DIR_INFO entries
// formatted as WinFSP expects them.
func parseDirectoryBuffer(buffer []byte) ([]directoryEntry, bool, error) {
	var entries []directoryEntry
	offset := 0
	for {
		if offset == len(buffer) {
			// Then we've exhausted the buffer but not found the end.
			return entries, false, nil
		}
		if offset == len(buffer)-2 && buffer[offset] == 0 && buffer[offset+1] == 0 {
			// Then we've found the end marker.
			return entries, true, nil
		}

		// Read the next entry.
		if offset+int(unsafe.Sizeof(ffi.FSP_FSCTL_DIR_INFO{})) > len(buffer) {
			return nil, false, fmt.Errorf("buffer was not properly terminated")
		}
		dirInfo := (*ffi.FSP_FSCTL_DIR_INFO)(unsafe.Pointer(&buffer[offset]))
		dirInfoSize := int(unsafe.Sizeof(ffi.FSP_FSCTL_DIR_INFO{}))
		nameOffset := offset + dirInfoSize
		nameLength := int(dirInfo.Size) - dirInfoSize

		// Extract the name.
		if nameOffset+nameLength > len(buffer) {
			return nil, false, fmt.Errorf("name overflows the end of the buffer")
		}
		var name string
		if nameLength > 0 {
			name = windows.UTF16ToString((*[1 << 30]uint16)(unsafe.Pointer(&buffer[nameOffset]))[:nameLength/2])
		}

		entries = append(entries, directoryEntry{
			name:       name,
			fileInfo:   dirInfo.FileInfo,
			nextOffset: dirInfo.NextOffset,
		})

		const dirInfoAlignment uint16 = uint16(unsafe.Alignof(ffi.FSP_FSCTL_DIR_INFO{}))
		alignedSize := (dirInfo.Size + dirInfoAlignment - 1) & ^(dirInfoAlignment - 1)
		offset += int(alignedSize)
	}
}

func extractSymlinkReparseTarget(buffer []byte) (string, uint32, error) {
	if len(buffer) < int(unsafe.Sizeof(windowsext.REPARSE_DATA_BUFFER_HEADER{})) {
		return "", 0, fmt.Errorf("buffer too small")
	}
	// Parse the reparse data buffer
	reparseData := (*windowsext.REPARSE_DATA_BUFFER)(unsafe.Pointer(&buffer[0]))
	if reparseData.ReparseTag != uint32(windows.IO_REPARSE_TAG_SYMLINK) {
		return "", 0, fmt.Errorf("expected symlink reparse tag, got %d", windows.IO_REPARSE_TAG_SYMLINK, reparseData.ReparseTag)
	}

	symlinkData := (*windowsext.SymbolicLinkReparseBuffer)(unsafe.Pointer(&reparseData.DUMMYUNIONNAME))
	return symlinkData.Path(), symlinkData.Flags, nil
}

func dispositionToOptions(disposition uint32) uint32 {
	return disposition << 24
}

func getAclEntries(sd *windows.SECURITY_DESCRIPTOR) ([]*windows.ACCESS_ALLOWED_ACE, error) {
	dacl, _, err := sd.DACL()
	if err != nil {
		return nil, err
	}
	entries := make([]*windows.ACCESS_ALLOWED_ACE, dacl.AceCount)
	for i := uint16(0); i < dacl.AceCount; i++ {
		err := windows.GetAce(dacl, uint32(i), &entries[i])
		if err != nil {
			return nil, err
		}
	}
	return entries, nil
}

func TestWinFSPFileSystemCreate(t *testing.T) {
	ctrl := gomock.NewController(t)
	rootDirectory := mock.NewMockVirtualDirectory(ctrl)
	file := mock.NewMockVirtualLeaf(ctrl)
	subdir := mock.NewMockVirtualDirectory(ctrl)

	fs := winfsp.NewFileSystem(rootDirectory, false)
	ref := &ffi.FileSystemRef{}

	t.Run("CreateFileExists", func(t *testing.T) {
		// Check the normal WinFSP sequence of Create -> Cleanup -> Close.
		rootDirectory.EXPECT().VirtualOpenChild(
			gomock.Any(),
			path.MustNewComponent("test.txt"),
			virtual.ShareMaskWrite,
			(&virtual.Attributes{}).SetPermissions(virtual.PermissionsExecute|virtual.PermissionsRead|virtual.PermissionsWrite),
			&virtual.OpenExistingOptions{Truncate: true},
			winfsp.AttributesMaskForWinFSPAttr,
			gomock.Any(),
		).DoAndReturn(func(ctx context.Context, name path.Component, shareAccess virtual.ShareMask, createAttributes *virtual.Attributes, existingOptions *virtual.OpenExistingOptions, requested virtual.AttributesMask, out *virtual.Attributes) (virtual.Leaf, virtual.AttributesMask, virtual.ChangeInfo, virtual.Status) {
			out.SetFileType(filesystem.FileTypeRegularFile)
			out.SetInodeNumber(123)
			out.SetSizeBytes(0)
			out.SetPermissions(virtual.PermissionsExecute | virtual.PermissionsRead | virtual.PermissionsWrite)
			out.SetLinkCount(1)
			out.SetLastDataModificationTime(time.Unix(1000, 0))
			return file, 0, virtual.ChangeInfo{}, virtual.StatusOK
		})

		var info ffi.FSP_FSCTL_FILE_INFO
		handle, err := fs.Create(
			ref,
			"\\test.txt",
			windows.FILE_NON_DIRECTORY_FILE,
			windows.FILE_WRITE_DATA,
			0,
			nil,
			0,
			&info,
		)

		require.NoError(t, err)
		require.NotZero(t, handle)
		require.Equal(t, uint32(windows.FILE_ATTRIBUTE_NORMAL), info.FileAttributes)
		require.Equal(t, uint64(123), info.IndexNumber)
		require.Equal(t, uint64(0), info.FileSize)

		// Cleanup
		file.EXPECT().VirtualSetAttributes(
			gomock.Any(),
			gomock.Any(),
			virtual.AttributesMaskLastDataModificationTime,
			gomock.Any(),
		).Return(virtual.StatusOK)
		fs.Cleanup(
			ref,
			handle,
			"\\test.txt",
			ffi.FspCleanupSetLastWriteTime,
		)

		// Close
		file.EXPECT().VirtualClose(virtual.ShareMaskWrite)
		fs.Close(ref, handle)
	})

	t.Run("OpenFileDoesNotExist", func(t *testing.T) {
		// Simulate trying to open a file that does not exist, with
		// disposition that does not allow creation (FILE_OPEN).
		rootDirectory.EXPECT().VirtualOpenChild(
			gomock.Any(),
			path.MustNewComponent("does_not_exist.txt"),
			virtual.ShareMaskWrite,
			nil,
			&virtual.OpenExistingOptions{Truncate: false},
			winfsp.AttributesMaskForWinFSPAttr,
			gomock.Any(),
		).DoAndReturn(func(ctx context.Context, name path.Component, shareAccess virtual.ShareMask, createAttributes *virtual.Attributes, existingOptions *virtual.OpenExistingOptions, requested virtual.AttributesMask, out *virtual.Attributes) (virtual.Leaf, virtual.AttributesMask, virtual.ChangeInfo, virtual.Status) {
			return nil, 0, virtual.ChangeInfo{}, virtual.StatusErrNoEnt
		})
		rootDirectory.EXPECT().VirtualLookup(
			gomock.Any(),
			path.MustNewComponent("does_not_exist.txt"),
			winfsp.AttributesMaskForWinFSPAttr,
			gomock.Any(),
		).DoAndReturn(func(ctx context.Context, name path.Component, requested virtual.AttributesMask, out *virtual.Attributes) (virtual.DirectoryChild, virtual.Status) {
			return virtual.DirectoryChild{}, virtual.StatusErrNoEnt
		})

		var info ffi.FSP_FSCTL_FILE_INFO
		handle, err := fs.Create(
			ref,
			"\\does_not_exist.txt",
			dispositionToOptions(windows.FILE_OPEN),
			windows.FILE_WRITE_DATA,
			0,
			nil,
			0,
			&info,
		)

		require.Error(t, err)
		require.Zero(t, handle)
	})

	t.Run("CreateFileDoesNotExist", func(t *testing.T) {
		// Simulate trying to open a file that does not exist, with
		// disposition that does allow creation (FILE_OPEN).
		rootDirectory.EXPECT().VirtualOpenChild(
			gomock.Any(),
			path.MustNewComponent("does_not_exist.txt"),
			virtual.ShareMaskWrite,
			(&virtual.Attributes{}).SetPermissions(virtual.PermissionsExecute|virtual.PermissionsRead|virtual.PermissionsWrite),
			nil,
			winfsp.AttributesMaskForWinFSPAttr,
			gomock.Any(),
		).DoAndReturn(func(ctx context.Context, name path.Component, shareAccess virtual.ShareMask, createAttributes *virtual.Attributes, existingOptions *virtual.OpenExistingOptions, requested virtual.AttributesMask, out *virtual.Attributes) (virtual.Leaf, virtual.AttributesMask, virtual.ChangeInfo, virtual.Status) {
			out.SetFileType(filesystem.FileTypeRegularFile)
			out.SetInodeNumber(123)
			out.SetSizeBytes(0)
			out.SetPermissions(virtual.PermissionsExecute | virtual.PermissionsRead | virtual.PermissionsWrite)
			out.SetLinkCount(1)
			out.SetLastDataModificationTime(time.Unix(1000, 0))
			return file, 0, virtual.ChangeInfo{}, virtual.StatusOK
		})

		var info ffi.FSP_FSCTL_FILE_INFO
		handle, err := fs.Create(
			ref,
			"\\does_not_exist.txt",
			dispositionToOptions(windows.FILE_CREATE),
			windows.FILE_WRITE_DATA,
			0,
			nil,
			0,
			&info,
		)

		require.NoError(t, err)
		require.NotZero(t, handle)
		require.Equal(t, uint32(windows.FILE_ATTRIBUTE_NORMAL), info.FileAttributes)
		require.Equal(t, uint64(123), info.IndexNumber)
		require.Equal(t, uint64(0), info.FileSize)
	})

	t.Run("CreateDirectoryLookupOnOpenFail", func(t *testing.T) {
		// Check that if calling VirtualOpen fails because the item is a
		// directory, we use VirtualLookup instead.
		//
		// This simulates a failure found when testing with bonanza.
		rootDirectory.EXPECT().VirtualOpenChild(
			gomock.Any(),
			path.MustNewComponent("subdir"),
			virtual.ShareMaskWrite,
			nil,
			&virtual.OpenExistingOptions{},
			winfsp.AttributesMaskForWinFSPAttr,
			gomock.Any(),
		).DoAndReturn(func(ctx context.Context, name path.Component, shareAccess virtual.ShareMask, createAttributes *virtual.Attributes, existingOptions *virtual.OpenExistingOptions, requested virtual.AttributesMask, out *virtual.Attributes) (virtual.Leaf, virtual.AttributesMask, virtual.ChangeInfo, virtual.Status) {
			// This is how bonanza implements this function.
			return virtual.ReadOnlyDirectoryOpenChildDoesntExist(createAttributes)
		})
		rootDirectory.EXPECT().VirtualLookup(
			gomock.Any(),
			path.MustNewComponent("subdir"),
			winfsp.AttributesMaskForWinFSPAttr,
			gomock.Any(),
		).DoAndReturn(func(ctx context.Context, name path.Component, requested virtual.AttributesMask, out *virtual.Attributes) (virtual.DirectoryChild, virtual.Status) {
			out.SetFileType(filesystem.FileTypeDirectory)
			out.SetInodeNumber(123)
			out.SetPermissions(virtual.PermissionsExecute | virtual.PermissionsRead | virtual.PermissionsWrite)
			out.SetLinkCount(1)
			out.SetLastDataModificationTime(time.Unix(1000, 0))
			return virtual.DirectoryChild{}.FromDirectory(subdir), virtual.StatusOK
		})

		var info ffi.FSP_FSCTL_FILE_INFO
		handle, err := fs.Create(
			ref,
			"\\subdir",
			dispositionToOptions(windows.FILE_OPEN),
			windows.FILE_WRITE_DATA,
			0,
			nil,
			0,
			&info,
		)
		require.NoError(t, err)
		require.NotZero(t, handle)
		require.Equal(t, uint32(windows.FILE_ATTRIBUTE_DIRECTORY), info.FileAttributes)

		// We don't expect a close.
		fs.Close(ref, handle)
	})
}

func TestWinFSPFileSystemOpen(t *testing.T) {
	ctrl := gomock.NewController(t)
	rootDirectory := mock.NewMockVirtualDirectory(ctrl)
	file := mock.NewMockVirtualLeaf(ctrl)

	fs := winfsp.NewFileSystem(rootDirectory, false)
	ref := &ffi.FileSystemRef{}

	t.Run("OpenExistingFile", func(t *testing.T) {
		// Open a file that does exist.
		rootDirectory.EXPECT().VirtualOpenChild(
			gomock.Any(),
			path.MustNewComponent("existing.txt"),
			virtual.ShareMaskRead,
			nil,
			&virtual.OpenExistingOptions{},
			winfsp.AttributesMaskForWinFSPAttr,
			gomock.Any(),
		).DoAndReturn(func(ctx context.Context, name path.Component, shareAccess virtual.ShareMask, createAttributes *virtual.Attributes, existingOptions *virtual.OpenExistingOptions, requested virtual.AttributesMask, out *virtual.Attributes) (virtual.Leaf, virtual.AttributesMask, virtual.ChangeInfo, virtual.Status) {
			// Return attributes for an existing file
			out.SetFileType(filesystem.FileTypeRegularFile)
			out.SetInodeNumber(456)
			out.SetSizeBytes(100)
			out.SetPermissions(virtual.PermissionsRead)
			out.SetLinkCount(1)
			out.SetLastDataModificationTime(time.Unix(2000, 0))
			return file, 0, virtual.ChangeInfo{}, virtual.StatusOK
		})

		var info ffi.FSP_FSCTL_FILE_INFO
		handle, err := fs.Open(
			ref,
			"\\existing.txt",
			dispositionToOptions(windows.FILE_OPEN),
			windows.FILE_READ_DATA,
			&info,
		)

		require.NoError(t, err)
		require.NotZero(t, handle)
		require.Equal(t, uint32(windows.FILE_ATTRIBUTE_NORMAL|windows.FILE_ATTRIBUTE_READONLY), info.FileAttributes)
		require.Equal(t, uint64(456), info.IndexNumber)
		require.Equal(t, uint64(100), info.FileSize)
	})

	t.Run("OpenNonExistentFile", func(t *testing.T) {
		// Open a file that does not exist.
		rootDirectory.EXPECT().VirtualOpenChild(
			gomock.Any(),
			path.MustNewComponent("does_not_exist.txt"),
			virtual.ShareMaskRead,
			nil,
			&virtual.OpenExistingOptions{},
			winfsp.AttributesMaskForWinFSPAttr,
			gomock.Any(),
		).DoAndReturn(func(ctx context.Context, name path.Component, shareAccess virtual.ShareMask, createAttributes *virtual.Attributes, existingOptions *virtual.OpenExistingOptions, requested virtual.AttributesMask, out *virtual.Attributes) (virtual.Leaf, virtual.AttributesMask, virtual.ChangeInfo, virtual.Status) {
			return nil, 0, virtual.ChangeInfo{}, virtual.StatusErrNoEnt
		})
		rootDirectory.EXPECT().VirtualLookup(
			gomock.Any(),
			path.MustNewComponent("does_not_exist.txt"),
			winfsp.AttributesMaskForWinFSPAttr,
			gomock.Any(),
		).DoAndReturn(func(ctx context.Context, name path.Component, requested virtual.AttributesMask, out *virtual.Attributes) (virtual.DirectoryChild, virtual.Status) {
			return virtual.DirectoryChild{}, virtual.StatusErrNoEnt
		})

		var info ffi.FSP_FSCTL_FILE_INFO
		handle, err := fs.Open(
			ref,
			"\\does_not_exist.txt",
			dispositionToOptions(windows.FILE_OPEN),
			windows.FILE_READ_DATA,
			&info,
		)

		require.Error(t, err)
		require.Zero(t, handle)
	})
}

func TestWinFSPFileSystemRead(t *testing.T) {
	ctrl := gomock.NewController(t)
	rootDirectory := mock.NewMockVirtualDirectory(ctrl)
	file := mock.NewMockVirtualLeaf(ctrl)

	fs := winfsp.NewFileSystem(rootDirectory, false)
	ref := &ffi.FileSystemRef{}

	fileName := "\\read_test.txt"
	expectedContent := []byte("Test file content")

	t.Run("ReadFromFile", func(t *testing.T) {
		// Create the file
		rootDirectory.EXPECT().VirtualOpenChild(
			gomock.Any(),
			path.MustNewComponent("read_test.txt"),
			virtual.ShareMaskRead,
			(&virtual.Attributes{}).SetPermissions(virtual.PermissionsExecute|virtual.PermissionsRead|virtual.PermissionsWrite),
			&virtual.OpenExistingOptions{Truncate: true},
			winfsp.AttributesMaskForWinFSPAttr,
			gomock.Any(),
		).DoAndReturn(func(ctx context.Context, name path.Component, shareAccess virtual.ShareMask, createAttributes *virtual.Attributes, existingOptions *virtual.OpenExistingOptions, requested virtual.AttributesMask, out *virtual.Attributes) (virtual.Leaf, virtual.AttributesMask, virtual.ChangeInfo, virtual.Status) {
			out.SetFileType(filesystem.FileTypeRegularFile)
			out.SetInodeNumber(789)
			out.SetSizeBytes(uint64(len(expectedContent)))
			out.SetPermissions(virtual.PermissionsRead)
			out.SetLinkCount(1)
			return file, 0, virtual.ChangeInfo{}, virtual.StatusOK
		})

		var info ffi.FSP_FSCTL_FILE_INFO
		handle, err := fs.Create(
			ref,
			fileName,
			0,
			windows.FILE_READ_DATA,
			0,
			nil,
			0,
			&info,
		)
		require.NoError(t, err)

		// Read and validate the data
		file.EXPECT().VirtualRead(
			gomock.Any(),
			uint64(0),
		).DoAndReturn(func(buffer []byte, offset uint64) (int, bool, virtual.Status) {
			n := copy(buffer, expectedContent)
			return n, false, virtual.StatusOK
		})

		buffer := make([]byte, 100)
		bytesRead, err := fs.Read(ref, handle, buffer, 0)

		require.NoError(t, err)
		require.Equal(t, len(expectedContent), bytesRead)
		require.Equal(t, expectedContent, buffer[:bytesRead])
	})
}

func TestWinFSPFileSystemWrite(t *testing.T) {
	ctrl := gomock.NewController(t)
	rootDirectory := mock.NewMockVirtualDirectory(ctrl)
	file := mock.NewMockVirtualLeaf(ctrl)

	fs := winfsp.NewFileSystem(rootDirectory, false)
	ref := &ffi.FileSystemRef{}

	t.Run("WriteToFile", func(t *testing.T) {
		// Create a file
		rootDirectory.EXPECT().VirtualOpenChild(
			gomock.Any(),
			path.MustNewComponent("test.txt"),
			virtual.ShareMaskWrite,
			(&virtual.Attributes{}).SetPermissions(virtual.PermissionsExecute|virtual.PermissionsRead|virtual.PermissionsWrite),
			&virtual.OpenExistingOptions{Truncate: true},
			winfsp.AttributesMaskForWinFSPAttr,
			gomock.Any(),
		).DoAndReturn(func(ctx context.Context, name path.Component, shareAccess virtual.ShareMask, createAttributes *virtual.Attributes, existingOptions *virtual.OpenExistingOptions, requested virtual.AttributesMask, out *virtual.Attributes) (virtual.Leaf, virtual.AttributesMask, virtual.ChangeInfo, virtual.Status) {
			out.SetFileType(filesystem.FileTypeRegularFile)
			out.SetInodeNumber(123)
			out.SetSizeBytes(0)
			out.SetPermissions(virtual.PermissionsRead | virtual.PermissionsWrite)
			out.SetLinkCount(1)
			out.SetLastDataModificationTime(time.Unix(1000, 0))
			return file, 0, virtual.ChangeInfo{}, virtual.StatusOK
		})

		var info ffi.FSP_FSCTL_FILE_INFO
		handle, err := fs.Create(
			ref,
			"\\test.txt",
			windows.FILE_NON_DIRECTORY_FILE,
			windows.FILE_WRITE_DATA,
			0,
			nil,
			0,
			&info,
		)
		require.NoError(t, err)

		// Write to the file
		fileContent := []byte("Hello, World!")
		file.EXPECT().VirtualWrite(
			fileContent,
			uint64(0), // offset
		).Return(len(fileContent), virtual.StatusOK)

		// Expect the file attributes to be queried after write
		file.EXPECT().VirtualGetAttributes(
			gomock.Any(),
			winfsp.AttributesMaskForWinFSPAttr,
			gomock.Any(),
		).Do(func(ctx context.Context, requested virtual.AttributesMask, attributes *virtual.Attributes) {
			attributes.SetFileType(filesystem.FileTypeRegularFile)
			attributes.SetInodeNumber(123)
			attributes.SetSizeBytes(uint64(len(fileContent)))
			attributes.SetPermissions(virtual.PermissionsRead | virtual.PermissionsWrite)
			attributes.SetLinkCount(1)
			attributes.SetLastDataModificationTime(time.Unix(1001, 0))
		})

		var writeInfo ffi.FSP_FSCTL_FILE_INFO
		written, err := fs.Write(
			ref,
			handle,
			fileContent,
			0,
			false,
			false,
			&writeInfo,
		)

		require.NoError(t, err)
		require.Equal(t, len(fileContent), written)
		require.Equal(t, uint64(len(fileContent)), writeInfo.FileSize)
	})
}

func TestWinFSPFileSystemReadDirectoryOffset(t *testing.T) {
	ctrl := gomock.NewController(t)
	rootDirectory := mock.NewMockVirtualDirectory(ctrl)
	subDirectory := mock.NewMockVirtualDirectory(ctrl)

	fs := winfsp.NewFileSystem(rootDirectory, false)
	ref := &ffi.FileSystemRef{}

	t.Run("ReadRootDirectory", func(t *testing.T) {
		rootDirectory.EXPECT().VirtualGetAttributes(
			gomock.Any(),
			winfsp.AttributesMaskForWinFSPAttr,
			gomock.Any(),
		).Do(func(ctx context.Context, requested virtual.AttributesMask, attributes *virtual.Attributes) {
			attributes.SetFileType(filesystem.FileTypeDirectory)
			attributes.SetInodeNumber(1)
			attributes.SetSizeBytes(0)
			attributes.SetPermissions(virtual.PermissionsRead | virtual.PermissionsWrite)
			attributes.SetLinkCount(1)
		})

		var info ffi.FSP_FSCTL_FILE_INFO
		handle, err := fs.Create(
			ref,
			"\\",
			dispositionToOptions(windows.FILE_OPEN)|windows.FILE_DIRECTORY_FILE,
			windows.FILE_LIST_DIRECTORY,
			0,
			nil,
			0,
			&info,
		)
		require.NoError(t, err)

		// Mock directory reading
		rootDirectory.EXPECT().VirtualReadDir(
			gomock.Any(),
			uint64(0),
			winfsp.AttributesMaskForWinFSPAttr,
			gomock.Any(),
		).DoAndReturn(func(ctx context.Context, firstCookie uint64, attributesMask virtual.AttributesMask, reporter virtual.DirectoryEntryReporter) virtual.Status {
			// Report a file entry
			attrs := virtual.Attributes{}
			attrs.SetFileType(filesystem.FileTypeRegularFile)
			attrs.SetInodeNumber(100)
			attrs.SetSizeBytes(42)
			attrs.SetPermissions(virtual.PermissionsRead)
			attrs.SetLinkCount(1)

			child := virtual.DirectoryChild{}.FromLeaf(mock.NewMockVirtualLeaf(ctrl))
			require.True(t, reporter.ReportEntry(1, path.MustNewComponent("file1.txt"), child, &attrs))
			return virtual.StatusOK
		})

		buffer := make([]byte, 1024)
		bytesWritten, err := fs.ReadDirectoryOffset(ref, handle, nil, 0, buffer)
		require.NoError(t, err)
		require.LessOrEqual(t, bytesWritten, len(buffer))

		// Validate the entries.
		entries, finished, err := parseDirectoryBuffer(buffer[:bytesWritten])
		require.NoError(t, err)
		require.True(t, finished)
		require.Len(t, entries, 1)

		require.Equal(t, "file1.txt", entries[0].name)
		require.Equal(t, uint64(100), entries[0].fileInfo.IndexNumber)
		require.Equal(t, uint64(42), entries[0].fileInfo.FileSize)
		require.Equal(t, uint32(windows.FILE_ATTRIBUTE_NORMAL|windows.FILE_ATTRIBUTE_READONLY), entries[0].fileInfo.FileAttributes)
	})

	t.Run("ReadSubDirectory", func(t *testing.T) {
		// Check for sub directories we get . and .. entries.

		rootDirectory.EXPECT().VirtualLookup(
			gomock.Any(),
			path.MustNewComponent("subdir"),
			winfsp.AttributesMaskForWinFSPAttr,
			gomock.Any(),
		).DoAndReturn(func(ctx context.Context, name path.Component, requested virtual.AttributesMask, out *virtual.Attributes) (virtual.DirectoryChild, virtual.Status) {
			out.SetFileType(filesystem.FileTypeDirectory)
			out.SetInodeNumber(200)
			out.SetPermissions(virtual.PermissionsRead | virtual.PermissionsExecute)
			out.SetLinkCount(1)
			return virtual.DirectoryChild{}.FromDirectory(subDirectory), virtual.StatusOK
		})

		var info ffi.FSP_FSCTL_FILE_INFO
		handle, err := fs.Create(
			ref,
			"\\subdir",
			dispositionToOptions(windows.FILE_OPEN)|windows.FILE_DIRECTORY_FILE,
			windows.FILE_LIST_DIRECTORY,
			0,
			nil,
			0,
			&info,
		)
		require.NoError(t, err)

		subDirectory.EXPECT().VirtualReadDir(
			gomock.Any(),
			uint64(0),
			winfsp.AttributesMaskForWinFSPAttr,
			gomock.Any(),
		).DoAndReturn(func(ctx context.Context, firstCookie uint64, attributesMask virtual.AttributesMask, reporter virtual.DirectoryEntryReporter) virtual.Status {
			// Report one entry
			attrs := virtual.Attributes{}
			attrs.SetFileType(filesystem.FileTypeRegularFile)
			attrs.SetInodeNumber(300)
			attrs.SetSizeBytes(0)
			attrs.SetPermissions(virtual.PermissionsRead)
			attrs.SetLinkCount(1)

			child := virtual.DirectoryChild{}.FromLeaf(mock.NewMockVirtualLeaf(ctrl))
			require.True(t, reporter.ReportEntry(1, path.MustNewComponent("nested.txt"), child, &attrs))
			return virtual.StatusOK
		})

		buffer := make([]byte, 1024)
		bytesWritten, err := fs.ReadDirectoryOffset(ref, handle, nil, 0, buffer)
		require.NoError(t, err)
		require.LessOrEqual(t, bytesWritten, len(buffer))

		// Validate the entries.
		entries, finished, err := parseDirectoryBuffer(buffer[:bytesWritten])
		require.NoError(t, err)
		require.True(t, finished)
		require.Len(t, entries, 3)

		require.Equal(t, ".", entries[0].name)
		require.Equal(t, uint32(windows.FILE_ATTRIBUTE_DIRECTORY), entries[0].fileInfo.FileAttributes)

		require.Equal(t, "..", entries[1].name)
		require.Equal(t, uint32(windows.FILE_ATTRIBUTE_DIRECTORY), entries[1].fileInfo.FileAttributes)

		require.Equal(t, "nested.txt", entries[2].name)
		require.Equal(t, uint64(300), entries[2].fileInfo.IndexNumber)
		require.Equal(t, uint64(0), entries[2].fileInfo.FileSize)
		require.Equal(t, uint32(windows.FILE_ATTRIBUTE_NORMAL|windows.FILE_ATTRIBUTE_READONLY), entries[2].fileInfo.FileAttributes)
	})

	t.Run("ReadSubDirectoryWithPagination", func(t *testing.T) {
		// Test reading a subdirectory where the buffer is too small for
		// all entries, requiring multiple calls with different offsets.

		// Create mock objects
		attrs1 := virtual.Attributes{}
		attrs1.SetFileType(filesystem.FileTypeRegularFile)
		attrs1.SetInodeNumber(400)
		attrs1.SetSizeBytes(10)
		attrs1.SetPermissions(virtual.PermissionsRead)
		attrs1.SetLinkCount(1)
		child1 := virtual.DirectoryChild{}.FromLeaf(mock.NewMockVirtualLeaf(ctrl))

		attrs2 := virtual.Attributes{}
		attrs2.SetFileType(filesystem.FileTypeRegularFile)
		attrs2.SetInodeNumber(401)
		attrs2.SetSizeBytes(20)
		attrs2.SetPermissions(virtual.PermissionsRead)
		attrs2.SetLinkCount(1)
		child2 := virtual.DirectoryChild{}.FromLeaf(mock.NewMockVirtualLeaf(ctrl))

		attrs3 := virtual.Attributes{}
		attrs3.SetFileType(filesystem.FileTypeRegularFile)
		attrs3.SetInodeNumber(402)
		attrs3.SetSizeBytes(30)
		attrs3.SetPermissions(virtual.PermissionsRead)
		attrs3.SetLinkCount(1)

		child3 := virtual.DirectoryChild{}.FromLeaf(mock.NewMockVirtualLeaf(ctrl))

		rootDirectory.EXPECT().VirtualLookup(
			gomock.Any(),
			path.MustNewComponent("bigdir"),
			winfsp.AttributesMaskForWinFSPAttr,
			gomock.Any(),
		).DoAndReturn(func(ctx context.Context, name path.Component, requested virtual.AttributesMask, out *virtual.Attributes) (virtual.DirectoryChild, virtual.Status) {
			out.SetFileType(filesystem.FileTypeDirectory)
			out.SetInodeNumber(300)
			out.SetPermissions(virtual.PermissionsRead | virtual.PermissionsExecute)
			out.SetLinkCount(1)
			return virtual.DirectoryChild{}.FromDirectory(subDirectory), virtual.StatusOK
		})

		var info ffi.FSP_FSCTL_FILE_INFO
		handle, err := fs.Create(
			ref,
			"\\bigdir",
			dispositionToOptions(windows.FILE_OPEN)|windows.FILE_DIRECTORY_FILE,
			windows.FILE_LIST_DIRECTORY,
			0,
			nil,
			0,
			&info,
		)
		require.NoError(t, err)

		// First call: VirtualReadDir with cookie 0, returns first 2 entries.
		subDirectory.EXPECT().VirtualReadDir(
			gomock.Any(),
			uint64(0),
			winfsp.AttributesMaskForWinFSPAttr,
			gomock.Any(),
		).DoAndReturn(func(ctx context.Context, firstCookie uint64, attributesMask virtual.AttributesMask, reporter virtual.DirectoryEntryReporter) virtual.Status {
			require.True(t, reporter.ReportEntry(1, path.MustNewComponent("file1.txt"), child1, &attrs1))
			// Should be exhausted.
			require.False(t, reporter.ReportEntry(2, path.MustNewComponent("file2.txt"), child2, &attrs2))
			return virtual.StatusOK
		})

		// Use a small buffer that can only fit "." + ".." + one file entry.
		buffer1 := make([]byte, 400)
		bytesWritten1, err := fs.ReadDirectoryOffset(ref, handle, nil, 0, buffer1)
		require.NoError(t, err)
		require.Greater(t, bytesWritten1, 0)

		// Check the first batch.
		entries1, finished1, err := parseDirectoryBuffer(buffer1[:bytesWritten1])
		require.NoError(t, err)
		require.False(t, finished1)
		require.Len(t, entries1, 3)
		require.Equal(t, ".", entries1[0].name)
		require.Equal(t, "..", entries1[1].name)
		require.Equal(t, "file1.txt", entries1[2].name)

		// Second call: VirtualReadDir with cookie 1 (offset by 2 for "." and "..").
		subDirectory.EXPECT().VirtualReadDir(
			gomock.Any(),
			uint64(1),
			winfsp.AttributesMaskForWinFSPAttr,
			gomock.Any(),
		).DoAndReturn(func(ctx context.Context, firstCookie uint64, attributesMask virtual.AttributesMask, reporter virtual.DirectoryEntryReporter) virtual.Status {
			require.True(t, reporter.ReportEntry(2, path.MustNewComponent("file2.txt"), child2, &attrs2))
			require.True(t, reporter.ReportEntry(3, path.MustNewComponent("file3.txt"), child3, &attrs3))
			return virtual.StatusOK
		})

		buffer2 := make([]byte, 1024)
		bytesWritten2, err := fs.ReadDirectoryOffset(ref, handle, nil, entries1[2].nextOffset, buffer2)
		require.NoError(t, err)

		// Parse the second batch.
		entries2, finished2, err := parseDirectoryBuffer(buffer2[:bytesWritten2])
		require.NoError(t, err)
		require.True(t, finished2)
		require.Len(t, entries2, 2)

		require.Equal(t, "file2.txt", entries2[0].name)
		require.Equal(t, "file3.txt", entries2[1].name)
	})
}

func TestWinFSPFileSystemGetFileInfo(t *testing.T) {
	ctrl := gomock.NewController(t)
	rootDirectory := mock.NewMockVirtualDirectory(ctrl)
	file := mock.NewMockVirtualLeaf(ctrl)

	fs := winfsp.NewFileSystem(rootDirectory, false)
	ref := &ffi.FileSystemRef{}

	t.Run("GetFileInfoSuccess", func(t *testing.T) {
		rootDirectory.EXPECT().VirtualOpenChild(
			gomock.Any(),
			path.MustNewComponent("info_test.txt"),
			virtual.ShareMaskRead,
			(&virtual.Attributes{}).SetPermissions(virtual.PermissionsExecute|virtual.PermissionsRead|virtual.PermissionsWrite),
			&virtual.OpenExistingOptions{Truncate: true},
			winfsp.AttributesMaskForWinFSPAttr,
			gomock.Any(),
		).DoAndReturn(func(ctx context.Context, name path.Component, shareAccess virtual.ShareMask, createAttributes *virtual.Attributes, existingOptions *virtual.OpenExistingOptions, requested virtual.AttributesMask, out *virtual.Attributes) (virtual.Leaf, virtual.AttributesMask, virtual.ChangeInfo, virtual.Status) {
			out.SetFileType(filesystem.FileTypeRegularFile)
			out.SetInodeNumber(500)
			out.SetSizeBytes(256)
			out.SetPermissions(virtual.PermissionsRead | virtual.PermissionsWrite)
			out.SetLinkCount(1)
			out.SetLastDataModificationTime(time.Unix(3000, 0))
			return file, 0, virtual.ChangeInfo{}, virtual.StatusOK
		})

		var createInfo ffi.FSP_FSCTL_FILE_INFO
		handle, err := fs.Create(
			ref,
			"\\info_test.txt",
			0,
			windows.FILE_READ_DATA,
			0,
			nil,
			0,
			&createInfo,
		)
		require.NoError(t, err)

		file.EXPECT().VirtualGetAttributes(
			gomock.Any(),
			winfsp.AttributesMaskForWinFSPAttr,
			gomock.Any(),
		).Do(func(ctx context.Context, requested virtual.AttributesMask, attributes *virtual.Attributes) {
			attributes.SetFileType(filesystem.FileTypeRegularFile)
			attributes.SetInodeNumber(500)
			attributes.SetSizeBytes(256)
			attributes.SetPermissions(virtual.PermissionsRead | virtual.PermissionsWrite)
			attributes.SetLinkCount(1)
			attributes.SetLastDataModificationTime(time.Unix(3000, 0))
		})

		var info ffi.FSP_FSCTL_FILE_INFO
		err = fs.GetFileInfo(ref, handle, &info)

		require.NoError(t, err)
		require.Equal(t, uint32(windows.FILE_ATTRIBUTE_NORMAL), info.FileAttributes)
		require.Equal(t, uint64(500), info.IndexNumber)
		require.Equal(t, uint64(256), info.FileSize)
	})
}

func TestWinFSPFileSystemGetVolumeInfo(t *testing.T) {
	ctrl := gomock.NewController(t)
	rootDirectory := mock.NewMockVirtualDirectory(ctrl)

	fs := winfsp.NewFileSystem(rootDirectory, false)
	ref := &ffi.FileSystemRef{}

	t.Run("GetVolumeInfoDefault", func(t *testing.T) {
		var info ffi.FSP_FSCTL_VOLUME_INFO
		err := fs.GetVolumeInfo(ref, &info)

		require.NoError(t, err)
		require.Equal(t, uint64(0), info.FreeSize)
		require.Equal(t, uint64(0), info.TotalSize)
		require.Equal(t, uint16(0), info.VolumeLabelLength)
	})

	t.Run("SetVolumeLabel", func(t *testing.T) {
		var setInfo ffi.FSP_FSCTL_VOLUME_INFO
		err := fs.SetVolumeLabel(ref, "TestVolume", &setInfo)
		require.NoError(t, err)

		var getInfo ffi.FSP_FSCTL_VOLUME_INFO
		err = fs.GetVolumeInfo(ref, &getInfo)
		require.NoError(t, err)
		require.Equal(t, uint64(0), getInfo.FreeSize)
		require.Equal(t, uint64(0), getInfo.TotalSize)
		require.Equal(t, "TestVolume", windows.UTF16ToString(getInfo.VolumeLabel[:]))
	})
}

func TestWinFSPFileSystemSetFileSize(t *testing.T) {
	ctrl := gomock.NewController(t)
	rootDirectory := mock.NewMockVirtualDirectory(ctrl)
	file := mock.NewMockVirtualLeaf(ctrl)

	fs := winfsp.NewFileSystem(rootDirectory, false)
	ref := &ffi.FileSystemRef{}

	t.Run("SetFileSizeSuccess", func(t *testing.T) {
		rootDirectory.EXPECT().VirtualOpenChild(
			gomock.Any(),
			path.MustNewComponent("resize_test.txt"),
			virtual.ShareMaskWrite,
			(&virtual.Attributes{}).SetPermissions(virtual.PermissionsExecute|virtual.PermissionsRead|virtual.PermissionsWrite),
			&virtual.OpenExistingOptions{Truncate: true},
			winfsp.AttributesMaskForWinFSPAttr,
			gomock.Any(),
		).DoAndReturn(func(ctx context.Context, name path.Component, shareAccess virtual.ShareMask, createAttributes *virtual.Attributes, existingOptions *virtual.OpenExistingOptions, requested virtual.AttributesMask, out *virtual.Attributes) (virtual.Leaf, virtual.AttributesMask, virtual.ChangeInfo, virtual.Status) {
			out.SetFileType(filesystem.FileTypeRegularFile)
			out.SetInodeNumber(600)
			out.SetSizeBytes(100)
			out.SetPermissions(virtual.PermissionsRead | virtual.PermissionsWrite)
			out.SetLinkCount(1)
			return file, 0, virtual.ChangeInfo{}, virtual.StatusOK
		})

		var createInfo ffi.FSP_FSCTL_FILE_INFO
		handle, err := fs.Create(
			ref,
			"\\resize_test.txt",
			0,
			windows.FILE_WRITE_DATA,
			0,
			nil,
			0,
			&createInfo,
		)
		require.NoError(t, err)

		// Set new file size
		newSize := uint64(200)
		file.EXPECT().VirtualSetAttributes(
			gomock.Any(),
			gomock.Any(),
			winfsp.AttributesMaskForWinFSPAttr,
			gomock.Any(),
		).DoAndReturn(func(ctx context.Context, in *virtual.Attributes, attributesMask virtual.AttributesMask, out *virtual.Attributes) virtual.Status {
			size, _ := in.GetSizeBytes()
			require.Equal(t, newSize, size)

			out.SetFileType(filesystem.FileTypeRegularFile)
			out.SetInodeNumber(600)
			out.SetSizeBytes(newSize)
			out.SetPermissions(virtual.PermissionsRead | virtual.PermissionsWrite)
			out.SetLinkCount(1)
			return virtual.StatusOK
		})

		var info ffi.FSP_FSCTL_FILE_INFO
		err = fs.SetFileSize(ref, handle, newSize, false, &info)

		require.NoError(t, err)
		require.Equal(t, newSize, info.FileSize)
	})
}

func TestWinFSPFileSystemCanDelete(t *testing.T) {
	ctrl := gomock.NewController(t)
	rootDirectory := mock.NewMockVirtualDirectory(ctrl)
	file := mock.NewMockVirtualLeaf(ctrl)
	emptyDir := mock.NewMockVirtualDirectory(ctrl)
	nonEmptyDir := mock.NewMockVirtualDirectory(ctrl)

	fs := winfsp.NewFileSystem(rootDirectory, false)
	ref := &ffi.FileSystemRef{}

	t.Run("CanDeleteFile", func(t *testing.T) {
		rootDirectory.EXPECT().VirtualOpenChild(
			gomock.Any(),
			path.MustNewComponent("deletable.txt"),
			virtual.ShareMaskRead,
			(&virtual.Attributes{}).SetPermissions(virtual.PermissionsExecute|virtual.PermissionsRead|virtual.PermissionsWrite),
			&virtual.OpenExistingOptions{Truncate: true},
			winfsp.AttributesMaskForWinFSPAttr,
			gomock.Any(),
		).DoAndReturn(func(ctx context.Context, name path.Component, shareAccess virtual.ShareMask, createAttributes *virtual.Attributes, existingOptions *virtual.OpenExistingOptions, requested virtual.AttributesMask, out *virtual.Attributes) (virtual.Leaf, virtual.AttributesMask, virtual.ChangeInfo, virtual.Status) {
			out.SetFileType(filesystem.FileTypeRegularFile)
			out.SetInodeNumber(600)
			out.SetSizeBytes(100)
			out.SetPermissions(virtual.PermissionsRead | virtual.PermissionsWrite)
			out.SetLinkCount(1)
			return file, 0, virtual.ChangeInfo{}, virtual.StatusOK
		})

		var info ffi.FSP_FSCTL_FILE_INFO
		handle, err := fs.Create(ref, "\\deletable.txt", 0, windows.FILE_READ_DATA, 0, nil, 0, &info)
		require.NoError(t, err)

		err = fs.CanDelete(ref, handle, "\\deletable.txt")
		require.NoError(t, err)
	})

	t.Run("CanDeleteEmptyDirectory", func(t *testing.T) {
		rootDirectory.EXPECT().VirtualLookup(
			gomock.Any(),
			path.MustNewComponent("empty_dir"),
			winfsp.AttributesMaskForWinFSPAttr,
			gomock.Any(),
		).DoAndReturn(func(ctx context.Context, name path.Component, requested virtual.AttributesMask, out *virtual.Attributes) (virtual.DirectoryChild, virtual.Status) {
			out.SetFileType(filesystem.FileTypeDirectory)
			out.SetInodeNumber(600)
			out.SetSizeBytes(100)
			out.SetPermissions(virtual.PermissionsRead | virtual.PermissionsWrite)
			out.SetLinkCount(1)
			return virtual.DirectoryChild{}.FromDirectory(emptyDir), virtual.StatusOK
		})

		var info ffi.FSP_FSCTL_FILE_INFO
		handle, err := fs.Create(ref, "\\empty_dir", dispositionToOptions(windows.FILE_OPEN)|windows.FILE_DIRECTORY_FILE, windows.FILE_READ_DATA, 0, nil, 0, &info)
		require.NoError(t, err)

		// Mock empty directory check.
		emptyDir.EXPECT().VirtualReadDir(
			gomock.Any(),
			uint64(0),
			virtual.AttributesMask(0),
			gomock.Any(),
		).DoAndReturn(func(ctx context.Context, firstCookie uint64, attributesMask virtual.AttributesMask, reporter virtual.DirectoryEntryReporter) virtual.Status {
			return virtual.StatusOK
		})

		err = fs.CanDelete(ref, handle, "\\empty_dir")
		require.NoError(t, err)
	})

	t.Run("CannotDeleteNonEmptyDirectory", func(t *testing.T) {
		rootDirectory.EXPECT().VirtualLookup(
			gomock.Any(),
			path.MustNewComponent("nonempty_dir"),
			winfsp.AttributesMaskForWinFSPAttr,
			gomock.Any(),
		).DoAndReturn(func(ctx context.Context, name path.Component, requested virtual.AttributesMask, out *virtual.Attributes) (virtual.DirectoryChild, virtual.Status) {
			out.SetFileType(filesystem.FileTypeDirectory)
			out.SetInodeNumber(600)
			out.SetSizeBytes(100)
			out.SetPermissions(virtual.PermissionsRead | virtual.PermissionsWrite)
			out.SetLinkCount(1)
			return virtual.DirectoryChild{}.FromDirectory(nonEmptyDir), virtual.StatusOK
		})

		var info ffi.FSP_FSCTL_FILE_INFO
		handle, err := fs.Create(ref, "\\nonempty_dir", dispositionToOptions(windows.FILE_OPEN)|windows.FILE_DIRECTORY_FILE, windows.FILE_READ_DATA, 0, nil, 0, &info)
		require.NoError(t, err)

		// Mock non-empty directory check.
		nonEmptyDir.EXPECT().VirtualReadDir(
			gomock.Any(),
			uint64(0),
			virtual.AttributesMask(0),
			gomock.Any(),
		).DoAndReturn(func(ctx context.Context, firstCookie uint64, attributesMask virtual.AttributesMask, reporter virtual.DirectoryEntryReporter) virtual.Status {
			attrs := virtual.Attributes{}
			child := virtual.DirectoryChild{}.FromLeaf(mock.NewMockVirtualLeaf(ctrl))
			reporter.ReportEntry(1, path.MustNewComponent("some_file.txt"), child, &attrs)
			return virtual.StatusOK
		})

		err = fs.CanDelete(ref, handle, "\\nonempty_dir")
		require.Error(t, err)
		require.Equal(t, windows.STATUS_DIRECTORY_NOT_EMPTY, err)
	})
}

func TestWinFSPFileSystemRename(t *testing.T) {
	ctrl := gomock.NewController(t)
	rootDirectory := mock.NewMockVirtualDirectory(ctrl)
	file := mock.NewMockVirtualLeaf(ctrl)

	fs := winfsp.NewFileSystem(rootDirectory, false)
	ref := &ffi.FileSystemRef{}

	t.Run("RenameFileSuccess", func(t *testing.T) {
		rootDirectory.EXPECT().VirtualOpenChild(
			gomock.Any(),
			path.MustNewComponent("oldname.txt"),
			virtual.ShareMaskWrite,
			(&virtual.Attributes{}).SetPermissions(virtual.PermissionsExecute|virtual.PermissionsRead|virtual.PermissionsWrite),
			&virtual.OpenExistingOptions{Truncate: true},
			winfsp.AttributesMaskForWinFSPAttr,
			gomock.Any(),
		).DoAndReturn(func(ctx context.Context, name path.Component, shareAccess virtual.ShareMask, createAttributes *virtual.Attributes, existingOptions *virtual.OpenExistingOptions, requested virtual.AttributesMask, out *virtual.Attributes) (virtual.Leaf, virtual.AttributesMask, virtual.ChangeInfo, virtual.Status) {
			out.SetFileType(filesystem.FileTypeRegularFile)
			out.SetInodeNumber(600)
			out.SetSizeBytes(100)
			out.SetPermissions(virtual.PermissionsRead | virtual.PermissionsWrite)
			out.SetLinkCount(1)
			return file, 0, virtual.ChangeInfo{}, virtual.StatusOK
		})

		var info ffi.FSP_FSCTL_FILE_INFO
		handle, err := fs.Create(ref, "\\oldname.txt", 0, windows.FILE_WRITE_DATA, 0, nil, 0, &info)
		require.NoError(t, err)

		rootDirectory.EXPECT().VirtualLookup(
			gomock.Any(),
			path.MustNewComponent("newname.txt"),
			virtual.AttributesMaskFileType,
			gomock.Any(),
		).Return(virtual.DirectoryChild{}, virtual.StatusErrNoEnt)

		rootDirectory.EXPECT().VirtualRename(
			path.MustNewComponent("oldname.txt"),
			rootDirectory,
			path.MustNewComponent("newname.txt"),
		).Return(virtual.ChangeInfo{}, virtual.ChangeInfo{}, virtual.StatusOK)

		err = fs.Rename(ref, handle, "\\oldname.txt", "\\newname.txt", false)
		require.NoError(t, err)
	})

	t.Run("RenameFileTargetExists", func(t *testing.T) {
		rootDirectory.EXPECT().VirtualOpenChild(
			gomock.Any(),
			path.MustNewComponent("source.txt"),
			virtual.ShareMaskWrite,
			(&virtual.Attributes{}).SetPermissions(virtual.PermissionsExecute|virtual.PermissionsRead|virtual.PermissionsWrite),
			&virtual.OpenExistingOptions{Truncate: true},
			winfsp.AttributesMaskForWinFSPAttr,
			gomock.Any(),
		).DoAndReturn(func(ctx context.Context, name path.Component, shareAccess virtual.ShareMask, createAttributes *virtual.Attributes, existingOptions *virtual.OpenExistingOptions, requested virtual.AttributesMask, out *virtual.Attributes) (virtual.Leaf, virtual.AttributesMask, virtual.ChangeInfo, virtual.Status) {
			out.SetFileType(filesystem.FileTypeRegularFile)
			out.SetInodeNumber(600)
			out.SetSizeBytes(100)
			out.SetPermissions(virtual.PermissionsRead | virtual.PermissionsWrite)
			out.SetLinkCount(1)
			return file, 0, virtual.ChangeInfo{}, virtual.StatusOK
		})

		var info ffi.FSP_FSCTL_FILE_INFO
		handle, err := fs.Create(ref, "\\source.txt", 0, windows.FILE_WRITE_DATA, 0, nil, 0, &info)
		require.NoError(t, err)

		targetFile := mock.NewMockVirtualLeaf(ctrl)
		rootDirectory.EXPECT().VirtualLookup(
			gomock.Any(),
			path.MustNewComponent("target.txt"),
			virtual.AttributesMaskFileType,
			gomock.Any(),
		).DoAndReturn(func(ctx context.Context, name path.Component, requested virtual.AttributesMask, out *virtual.Attributes) (virtual.DirectoryChild, virtual.Status) {
			out.SetFileType(filesystem.FileTypeRegularFile)
			out.SetInodeNumber(601)
			out.SetSizeBytes(100)
			out.SetPermissions(virtual.PermissionsRead | virtual.PermissionsWrite)
			out.SetLinkCount(1)
			return virtual.DirectoryChild{}.FromLeaf(targetFile), virtual.StatusOK
		})

		err = fs.Rename(ref, handle, "\\source.txt", "\\target.txt", false)
		require.Error(t, err)
		require.Equal(t, windows.STATUS_OBJECT_NAME_COLLISION, err)
	})

	t.Run("RenameFileReplaceExisting", func(t *testing.T) {
		rootDirectory.EXPECT().VirtualOpenChild(
			gomock.Any(),
			path.MustNewComponent("source.txt"),
			virtual.ShareMaskWrite,
			(&virtual.Attributes{}).SetPermissions(virtual.PermissionsExecute|virtual.PermissionsRead|virtual.PermissionsWrite),
			&virtual.OpenExistingOptions{Truncate: true},
			winfsp.AttributesMaskForWinFSPAttr,
			gomock.Any(),
		).DoAndReturn(func(ctx context.Context, name path.Component, shareAccess virtual.ShareMask, createAttributes *virtual.Attributes, existingOptions *virtual.OpenExistingOptions, requested virtual.AttributesMask, out *virtual.Attributes) (virtual.Leaf, virtual.AttributesMask, virtual.ChangeInfo, virtual.Status) {
			out.SetFileType(filesystem.FileTypeRegularFile)
			out.SetInodeNumber(600)
			out.SetSizeBytes(100)
			out.SetPermissions(virtual.PermissionsRead | virtual.PermissionsWrite)
			out.SetLinkCount(1)
			return file, 0, virtual.ChangeInfo{}, virtual.StatusOK
		})

		var info ffi.FSP_FSCTL_FILE_INFO
		handle, err := fs.Create(ref, "\\source.txt", 0, windows.FILE_WRITE_DATA, 0, nil, 0, &info)
		require.NoError(t, err)

		targetFile := mock.NewMockVirtualLeaf(ctrl)
		rootDirectory.EXPECT().VirtualLookup(
			gomock.Any(),
			path.MustNewComponent("target.txt"),
			virtual.AttributesMaskFileType,
			gomock.Any(),
		).DoAndReturn(func(ctx context.Context, name path.Component, requested virtual.AttributesMask, out *virtual.Attributes) (virtual.DirectoryChild, virtual.Status) {
			out.SetFileType(filesystem.FileTypeRegularFile)
			out.SetInodeNumber(601)
			out.SetSizeBytes(100)
			out.SetPermissions(virtual.PermissionsRead | virtual.PermissionsWrite)
			out.SetLinkCount(1)
			return virtual.DirectoryChild{}.FromLeaf(targetFile), virtual.StatusOK
		})

		rootDirectory.EXPECT().VirtualRename(
			path.MustNewComponent("source.txt"),
			rootDirectory,
			path.MustNewComponent("target.txt"),
		).Return(virtual.ChangeInfo{}, virtual.ChangeInfo{}, virtual.StatusOK)

		err = fs.Rename(ref, handle, "\\source.txt", "\\target.txt", true)
		require.NoError(t, err)
	})

	t.Run("RenameFileToSameFile", func(t *testing.T) {
		rootDirectory.EXPECT().VirtualOpenChild(
			gomock.Any(),
			path.MustNewComponent("samefile.txt"),
			virtual.ShareMaskWrite,
			(&virtual.Attributes{}).SetPermissions(virtual.PermissionsExecute|virtual.PermissionsRead|virtual.PermissionsWrite),
			&virtual.OpenExistingOptions{Truncate: true},
			winfsp.AttributesMaskForWinFSPAttr,
			gomock.Any(),
		).DoAndReturn(func(ctx context.Context, name path.Component, shareAccess virtual.ShareMask, createAttributes *virtual.Attributes, existingOptions *virtual.OpenExistingOptions, requested virtual.AttributesMask, out *virtual.Attributes) (virtual.Leaf, virtual.AttributesMask, virtual.ChangeInfo, virtual.Status) {
			out.SetFileType(filesystem.FileTypeRegularFile)
			out.SetInodeNumber(600)
			out.SetSizeBytes(100)
			out.SetPermissions(virtual.PermissionsRead | virtual.PermissionsWrite)
			out.SetLinkCount(1)
			return file, 0, virtual.ChangeInfo{}, virtual.StatusOK
		})

		var info ffi.FSP_FSCTL_FILE_INFO
		handle, err := fs.Create(ref, "\\samefile.txt", 0, windows.FILE_WRITE_DATA, 0, nil, 0, &info)
		require.NoError(t, err)

		rootDirectory.EXPECT().VirtualLookup(
			gomock.Any(),
			path.MustNewComponent("samefile.txt"),
			virtual.AttributesMaskFileType,
			gomock.Any(),
		).DoAndReturn(func(ctx context.Context, name path.Component, requested virtual.AttributesMask, out *virtual.Attributes) (virtual.DirectoryChild, virtual.Status) {
			out.SetFileType(filesystem.FileTypeRegularFile)
			out.SetInodeNumber(600)
			out.SetSizeBytes(100)
			out.SetPermissions(virtual.PermissionsRead | virtual.PermissionsWrite)
			out.SetLinkCount(1)
			return virtual.DirectoryChild{}.FromLeaf(file), virtual.StatusOK
		})

		rootDirectory.EXPECT().VirtualRename(
			path.MustNewComponent("samefile.txt"),
			rootDirectory,
			path.MustNewComponent("samefile.txt"),
		).Return(virtual.ChangeInfo{}, virtual.ChangeInfo{}, virtual.StatusOK)

		err = fs.Rename(ref, handle, "\\samefile.txt", "\\samefile.txt", false)
		require.NoError(t, err)
	})
}

func TestWinFSPFileSystemOverwrite(t *testing.T) {
	ctrl := gomock.NewController(t)
	rootDirectory := mock.NewMockVirtualDirectory(ctrl)
	file := mock.NewMockVirtualLeaf(ctrl)

	fs := winfsp.NewFileSystem(rootDirectory, false)
	ref := &ffi.FileSystemRef{}

	t.Run("OverwriteFile", func(t *testing.T) {
		// Create a file
		rootDirectory.EXPECT().VirtualOpenChild(
			gomock.Any(),
			path.MustNewComponent("overwrite_test.txt"),
			virtual.ShareMaskWrite,
			(&virtual.Attributes{}).SetPermissions(virtual.PermissionsExecute|virtual.PermissionsRead|virtual.PermissionsWrite),
			&virtual.OpenExistingOptions{Truncate: true},
			winfsp.AttributesMaskForWinFSPAttr,
			gomock.Any(),
		).DoAndReturn(func(ctx context.Context, name path.Component, shareAccess virtual.ShareMask, createAttributes *virtual.Attributes, existingOptions *virtual.OpenExistingOptions, requested virtual.AttributesMask, out *virtual.Attributes) (virtual.Leaf, virtual.AttributesMask, virtual.ChangeInfo, virtual.Status) {
			out.SetFileType(filesystem.FileTypeRegularFile)
			out.SetInodeNumber(700)
			out.SetSizeBytes(100)
			out.SetPermissions(virtual.PermissionsRead | virtual.PermissionsWrite)
			out.SetLinkCount(1)
			return file, 0, virtual.ChangeInfo{}, virtual.StatusOK
		})

		var createInfo ffi.FSP_FSCTL_FILE_INFO
		handle, err := fs.Create(ref, "\\overwrite_test.txt", 0, windows.FILE_WRITE_DATA, 0, nil, 0, &createInfo)
		require.NoError(t, err)

		// Mock overwrite operation
		file.EXPECT().VirtualSetAttributes(
			gomock.Any(),
			gomock.Any(),
			winfsp.AttributesMaskForWinFSPAttr,
			gomock.Any(),
		).DoAndReturn(func(ctx context.Context, in *virtual.Attributes, attributesMask virtual.AttributesMask, out *virtual.Attributes) virtual.Status {
			size, present := in.GetSizeBytes()
			require.True(t, present)
			require.Equal(t, uint64(0), size)

			out.SetFileType(filesystem.FileTypeRegularFile)
			out.SetInodeNumber(700)
			out.SetSizeBytes(0)
			out.SetPermissions(virtual.PermissionsRead | virtual.PermissionsWrite)
			out.SetLinkCount(1)
			return virtual.StatusOK
		})

		var info ffi.FSP_FSCTL_FILE_INFO
		err = fs.Overwrite(ref, handle, 0, true, 0, &info)

		require.NoError(t, err)
		require.Equal(t, uint64(0), info.FileSize)
	})
}

func TestWinFSPFileSystemGetReparsePointByName(t *testing.T) {
	ctrl := gomock.NewController(t)
	rootDirectory := mock.NewMockVirtualDirectory(ctrl)
	symlink := mock.NewMockVirtualLeaf(ctrl)

	fs := winfsp.NewFileSystem(rootDirectory, false)
	ref := &ffi.FileSystemRef{}

	t.Run("GetSymlinkReparsePoint", func(t *testing.T) {
		rootDirectory.EXPECT().VirtualLookup(
			gomock.Any(),
			path.MustNewComponent("symlink.txt"),
			winfsp.AttributesMaskForWinFSPAttr,
			gomock.Any(),
		).DoAndReturn(func(ctx context.Context, name path.Component, requested virtual.AttributesMask, out *virtual.Attributes) (virtual.DirectoryChild, virtual.Status) {
			out.SetFileType(filesystem.FileTypeSymlink)
			out.SetInodeNumber(800)
			out.SetPermissions(virtual.PermissionsRead)
			out.SetLinkCount(1)
			return virtual.DirectoryChild{}.FromLeaf(symlink), virtual.StatusOK
		})

		// Mock readlink
		target := []byte("target.txt")
		symlink.EXPECT().VirtualReadlink(gomock.Any()).Return(target, virtual.StatusOK)

		buffer := make([]byte, 1024)
		bytesWritten, err := fs.GetReparsePointByName(ref, "\\symlink.txt", false, buffer)
		require.NoError(t, err)
		actualTarget, flags, err := extractSymlinkReparseTarget(buffer[:bytesWritten])
		require.NoError(t, err)
		require.Equal(t, "target.txt", string(actualTarget))
		require.Equal(t, uint32(windowsext.SYMLINK_FLAG_RELATIVE), flags)
	})

	t.Run("GetReparsePointForRegularFile", func(t *testing.T) {
		rootDirectory.EXPECT().VirtualLookup(
			gomock.Any(),
			path.MustNewComponent("regular.txt"),
			winfsp.AttributesMaskForWinFSPAttr,
			gomock.Any(),
		).DoAndReturn(func(ctx context.Context, name path.Component, requested virtual.AttributesMask, out *virtual.Attributes) (virtual.DirectoryChild, virtual.Status) {
			out.SetFileType(filesystem.FileTypeRegularFile)
			return virtual.DirectoryChild{}.FromLeaf(mock.NewMockVirtualLeaf(ctrl)), virtual.StatusOK
		})

		buffer := make([]byte, 1024)
		_, err := fs.GetReparsePointByName(ref, "\\regular.txt", false, buffer)
		require.Error(t, err)
		require.Equal(t, windows.STATUS_NOT_A_REPARSE_POINT, err)
	})
}

func TestWinFSPFileSystemGetReparsePoint(t *testing.T) {
	ctrl := gomock.NewController(t)
	rootDirectory := mock.NewMockVirtualDirectory(ctrl)
	symlink := mock.NewMockVirtualLeaf(ctrl)
	regularFile := mock.NewMockVirtualLeaf(ctrl)

	fs := winfsp.NewFileSystem(rootDirectory, false)
	ref := &ffi.FileSystemRef{}

	t.Run("GetReparsePointForSymlinkRelative", func(t *testing.T) {
		rootDirectory.EXPECT().VirtualOpenChild(
			gomock.Any(),
			path.MustNewComponent("symlink.txt"),
			virtual.ShareMaskRead,
			(&virtual.Attributes{}),
			&virtual.OpenExistingOptions{},
			winfsp.AttributesMaskForWinFSPAttr,
			gomock.Any(),
		).DoAndReturn(func(ctx context.Context, name path.Component, shareAccess virtual.ShareMask, createAttributes *virtual.Attributes, existingOptions *virtual.OpenExistingOptions, requested virtual.AttributesMask, out *virtual.Attributes) (virtual.Leaf, virtual.AttributesMask, virtual.ChangeInfo, virtual.Status) {
			out.SetFileType(filesystem.FileTypeSymlink)
			out.SetInodeNumber(800)
			out.SetPermissions(virtual.PermissionsRead)
			out.SetLinkCount(1)
			return symlink, 0, virtual.ChangeInfo{}, virtual.StatusOK
		})

		var info ffi.FSP_FSCTL_FILE_INFO
		handle, err := fs.Open(ref, "\\symlink.txt", dispositionToOptions(windows.FILE_OPEN_IF), windows.FILE_READ_DATA, &info)
		require.NoError(t, err)

		target := []byte("relative_target.txt")
		symlink.EXPECT().VirtualReadlink(gomock.Any()).Return(target, virtual.StatusOK)

		buffer := make([]byte, 1024)
		bytesWritten, err := fs.GetReparsePoint(ref, handle, "\\symlink.txt", buffer)
		require.NoError(t, err)
		require.Greater(t, bytesWritten, 0)

		actualTarget, flags, err := extractSymlinkReparseTarget(buffer[:bytesWritten])
		require.NoError(t, err)
		require.Equal(t, "relative_target.txt", string(actualTarget))
		require.Equal(t, uint32(windowsext.SYMLINK_FLAG_RELATIVE), flags)
	})

	t.Run("GetReparsePointForSymlinkAbsolute", func(t *testing.T) {
		rootDirectory.EXPECT().VirtualOpenChild(
			gomock.Any(),
			path.MustNewComponent("abs_symlink.txt"),
			virtual.ShareMaskRead,
			(&virtual.Attributes{}),
			&virtual.OpenExistingOptions{},
			winfsp.AttributesMaskForWinFSPAttr,
			gomock.Any(),
		).DoAndReturn(func(ctx context.Context, name path.Component, shareAccess virtual.ShareMask, createAttributes *virtual.Attributes, existingOptions *virtual.OpenExistingOptions, requested virtual.AttributesMask, out *virtual.Attributes) (virtual.Leaf, virtual.AttributesMask, virtual.ChangeInfo, virtual.Status) {
			out.SetFileType(filesystem.FileTypeSymlink)
			out.SetInodeNumber(801)
			out.SetPermissions(virtual.PermissionsRead)
			out.SetLinkCount(1)
			return symlink, 0, virtual.ChangeInfo{}, virtual.StatusOK
		})

		var info ffi.FSP_FSCTL_FILE_INFO
		handle, err := fs.Open(ref, "\\abs_symlink.txt", dispositionToOptions(windows.FILE_OPEN_IF), windows.FILE_READ_DATA, &info)
		require.NoError(t, err)

		target := []byte("C:\\absolute\\target.txt")
		symlink.EXPECT().VirtualReadlink(gomock.Any()).Return(target, virtual.StatusOK)

		buffer := make([]byte, 1024)
		bytesWritten, err := fs.GetReparsePoint(ref, handle, "\\abs_symlink.txt", buffer)
		require.NoError(t, err)
		require.Greater(t, bytesWritten, 0)

		actualTarget, flags, err := extractSymlinkReparseTarget(buffer[:bytesWritten])
		require.NoError(t, err)
		require.Equal(t, "C:\\absolute\\target.txt", string(actualTarget))
		require.Equal(t, uint32(0), flags)
	})

	t.Run("GetReparsePointForRegularFile", func(t *testing.T) {
		rootDirectory.EXPECT().VirtualOpenChild(
			gomock.Any(),
			path.MustNewComponent("regular.txt"),
			virtual.ShareMaskRead,
			(&virtual.Attributes{}),
			&virtual.OpenExistingOptions{},
			winfsp.AttributesMaskForWinFSPAttr,
			gomock.Any(),
		).DoAndReturn(func(ctx context.Context, name path.Component, shareAccess virtual.ShareMask, createAttributes *virtual.Attributes, existingOptions *virtual.OpenExistingOptions, requested virtual.AttributesMask, out *virtual.Attributes) (virtual.Leaf, virtual.AttributesMask, virtual.ChangeInfo, virtual.Status) {
			out.SetFileType(filesystem.FileTypeRegularFile)
			out.SetInodeNumber(802)
			out.SetPermissions(virtual.PermissionsRead | virtual.PermissionsWrite)
			out.SetLinkCount(1)
			return regularFile, 0, virtual.ChangeInfo{}, virtual.StatusOK
		})

		var info ffi.FSP_FSCTL_FILE_INFO
		handle, err := fs.Open(ref, "\\regular.txt", dispositionToOptions(windows.FILE_OPEN_IF), windows.FILE_READ_DATA, &info)
		require.NoError(t, err)

		regularFile.EXPECT().VirtualReadlink(gomock.Any()).Return(nil, virtual.StatusErrSymlink)

		buffer := make([]byte, 1024)
		_, err = fs.GetReparsePoint(ref, handle, "\\regular.txt", buffer)
		require.Error(t, err)
		require.Equal(t, windows.STATUS_REPARSE, err)
	})
}

func TestWinFSPFileSystemDirectoryCreation(t *testing.T) {
	ctrl := gomock.NewController(t)
	rootDirectory := mock.NewMockVirtualDirectory(ctrl)
	newDirectory := mock.NewMockVirtualDirectory(ctrl)

	fs := winfsp.NewFileSystem(rootDirectory, false)
	ref := &ffi.FileSystemRef{}

	t.Run("CreateNewDirectory", func(t *testing.T) {
		// Simulate creating a new directory
		rootDirectory.EXPECT().VirtualMkdir(
			path.MustNewComponent("newdir"),
			winfsp.AttributesMaskForWinFSPAttr,
			gomock.Any(),
		).DoAndReturn(func(name path.Component, requested virtual.AttributesMask, out *virtual.Attributes) (virtual.Directory, virtual.ChangeInfo, virtual.Status) {
			out.SetFileType(filesystem.FileTypeDirectory)
			out.SetInodeNumber(900)
			out.SetPermissions(virtual.PermissionsExecute | virtual.PermissionsRead | virtual.PermissionsWrite)
			out.SetLinkCount(2)
			out.SetLastDataModificationTime(time.Unix(4000, 0))
			return newDirectory, virtual.ChangeInfo{}, virtual.StatusOK
		})

		var info ffi.FSP_FSCTL_FILE_INFO
		handle, err := fs.Create(
			ref,
			"\\newdir",
			dispositionToOptions(windows.FILE_CREATE)|windows.FILE_DIRECTORY_FILE,
			windows.FILE_LIST_DIRECTORY,
			0,
			nil,
			0,
			&info,
		)

		require.NoError(t, err)
		require.NotZero(t, handle)
		require.Equal(t, uint32(windows.FILE_ATTRIBUTE_DIRECTORY), info.FileAttributes)
		require.Equal(t, uint64(900), info.IndexNumber)
	})

	t.Run("CreateDirectoryAlreadyExists", func(t *testing.T) {
		// Try to create a directory that already exists
		rootDirectory.EXPECT().VirtualMkdir(
			path.MustNewComponent("existingdir"),
			winfsp.AttributesMaskForWinFSPAttr,
			gomock.Any(),
		).DoAndReturn(func(name path.Component, requested virtual.AttributesMask, out *virtual.Attributes) (virtual.Directory, virtual.ChangeInfo, virtual.Status) {
			return nil, virtual.ChangeInfo{}, virtual.StatusErrExist
		})

		var info ffi.FSP_FSCTL_FILE_INFO
		handle, err := fs.Create(
			ref,
			"\\existingdir",
			dispositionToOptions(windows.FILE_CREATE)|windows.FILE_DIRECTORY_FILE,
			windows.FILE_LIST_DIRECTORY,
			0,
			nil,
			0,
			&info,
		)

		require.Error(t, err)
		require.Zero(t, handle)
	})
}

func TestWinFSPFileSystemDirectoryRemoval(t *testing.T) {
	ctrl := gomock.NewController(t)
	rootDirectory := mock.NewMockVirtualDirectory(ctrl)
	emptyDir := mock.NewMockVirtualDirectory(ctrl)

	fs := winfsp.NewFileSystem(rootDirectory, false)
	ref := &ffi.FileSystemRef{}

	t.Run("RemoveEmptyDirectory", func(t *testing.T) {
		// Setup directory lookup
		rootDirectory.EXPECT().VirtualLookup(
			gomock.Any(),
			path.MustNewComponent("emptydir"),
			winfsp.AttributesMaskForWinFSPAttr,
			gomock.Any(),
		).DoAndReturn(func(ctx context.Context, name path.Component, requested virtual.AttributesMask, out *virtual.Attributes) (virtual.DirectoryChild, virtual.Status) {
			out.SetFileType(filesystem.FileTypeDirectory)
			out.SetInodeNumber(901)
			out.SetPermissions(virtual.PermissionsRead | virtual.PermissionsWrite | virtual.PermissionsExecute)
			out.SetLinkCount(2)
			return virtual.DirectoryChild{}.FromDirectory(emptyDir), virtual.StatusOK
		})

		var info ffi.FSP_FSCTL_FILE_INFO
		handle, err := fs.Create(
			ref,
			"\\emptydir",
			dispositionToOptions(windows.FILE_OPEN)|windows.FILE_DIRECTORY_FILE,
			windows.FILE_LIST_DIRECTORY,
			0,
			nil,
			0,
			&info,
		)
		require.NoError(t, err)

		// Check if directory is empty (should pass)
		emptyDir.EXPECT().VirtualReadDir(
			gomock.Any(),
			uint64(0),
			virtual.AttributesMask(0),
			gomock.Any(),
		).DoAndReturn(func(ctx context.Context, firstCookie uint64, attributesMask virtual.AttributesMask, reporter virtual.DirectoryEntryReporter) virtual.Status {
			return virtual.StatusOK // No entries reported = empty directory
		})

		err = fs.CanDelete(ref, handle, "\\emptydir")
		require.NoError(t, err)

		rootDirectory.EXPECT().VirtualRemove(
			path.MustNewComponent("emptydir"),
			true,
			false,
		).Return(virtual.ChangeInfo{}, virtual.StatusOK)

		// Cleanup
		fs.Cleanup(
			ref,
			handle,
			"\\emptydir",
			ffi.FspCleanupDelete,
		)
		fs.Close(ref, handle)
	})
}

func TestWinFSPFileSystemFileRemoval(t *testing.T) {
	ctrl := gomock.NewController(t)
	rootDirectory := mock.NewMockVirtualDirectory(ctrl)
	file := mock.NewMockVirtualLeaf(ctrl)

	fs := winfsp.NewFileSystem(rootDirectory, false)
	ref := &ffi.FileSystemRef{}

	t.Run("RemoveRegularFile", func(t *testing.T) {
		// Create/open a file for deletion
		rootDirectory.EXPECT().VirtualOpenChild(
			gomock.Any(),
			path.MustNewComponent("deleteme.txt"),
			virtual.ShareMaskRead,
			nil,
			&virtual.OpenExistingOptions{Truncate: false},
			winfsp.AttributesMaskForWinFSPAttr,
			gomock.Any(),
		).DoAndReturn(func(ctx context.Context, name path.Component, shareAccess virtual.ShareMask, createAttributes *virtual.Attributes, existingOptions *virtual.OpenExistingOptions, requested virtual.AttributesMask, out *virtual.Attributes) (virtual.Leaf, virtual.AttributesMask, virtual.ChangeInfo, virtual.Status) {
			out.SetFileType(filesystem.FileTypeRegularFile)
			out.SetInodeNumber(1000)
			out.SetSizeBytes(100)
			out.SetPermissions(virtual.PermissionsRead | virtual.PermissionsWrite)
			out.SetLinkCount(1)
			return file, 0, virtual.ChangeInfo{}, virtual.StatusOK
		})

		var info ffi.FSP_FSCTL_FILE_INFO
		handle, err := fs.Create(
			ref,
			"\\deleteme.txt",
			dispositionToOptions(windows.FILE_OPEN)|windows.FILE_NON_DIRECTORY_FILE,
			windows.FILE_READ_DATA,
			0,
			nil,
			0,
			&info,
		)
		require.NoError(t, err)

		err = fs.CanDelete(ref, handle, "\\deleteme.txt")
		require.NoError(t, err)

		rootDirectory.EXPECT().VirtualRemove(
			path.MustNewComponent("deleteme.txt"),
			false,
			true,
		).Return(virtual.ChangeInfo{}, virtual.StatusOK)
		fs.Cleanup(
			ref,
			handle,
			"\\deleteme.txt",
			ffi.FspCleanupDelete,
		)

		file.EXPECT().VirtualClose(virtual.ShareMaskRead)
		fs.Close(ref, handle)
	})
}

func TestWinFSPFileSystemGetDirInfoByName(t *testing.T) {
	ctrl := gomock.NewController(t)
	rootDirectory := mock.NewMockVirtualDirectory(ctrl)
	file := mock.NewMockVirtualLeaf(ctrl)

	fs := winfsp.NewFileSystem(rootDirectory, false)
	ref := &ffi.FileSystemRef{}

	t.Run("GetDirInfoByNameExistingFile", func(t *testing.T) {
		// First, get a handle to the root directory
		rootDirectory.EXPECT().VirtualGetAttributes(
			gomock.Any(),
			winfsp.AttributesMaskForWinFSPAttr,
			gomock.Any(),
		).Do(func(ctx context.Context, requested virtual.AttributesMask, attributes *virtual.Attributes) {
			attributes.SetFileType(filesystem.FileTypeDirectory)
			attributes.SetInodeNumber(1)
			attributes.SetPermissions(virtual.PermissionsRead | virtual.PermissionsWrite | virtual.PermissionsExecute)
			attributes.SetLinkCount(1)
		})

		var info ffi.FSP_FSCTL_FILE_INFO
		handle, err := fs.Create(
			ref,
			"\\",
			dispositionToOptions(windows.FILE_OPEN)|windows.FILE_DIRECTORY_FILE,
			windows.FILE_LIST_DIRECTORY,
			0,
			nil,
			0,
			&info,
		)
		require.NoError(t, err)

		// Mock the VirtualLookup call for the target file
		rootDirectory.EXPECT().VirtualLookup(
			gomock.Any(),
			path.MustNewComponent("testfile.txt"),
			winfsp.AttributesMaskForWinFSPAttr,
			gomock.Any(),
		).DoAndReturn(func(ctx context.Context, name path.Component, requested virtual.AttributesMask, out *virtual.Attributes) (virtual.DirectoryChild, virtual.Status) {
			out.SetFileType(filesystem.FileTypeRegularFile)
			out.SetInodeNumber(42)
			out.SetSizeBytes(100)
			out.SetPermissions(virtual.PermissionsRead | virtual.PermissionsWrite)
			out.SetLinkCount(1)
			out.SetLastDataModificationTime(time.Unix(1000, 0))
			return virtual.DirectoryChild{}.FromLeaf(file), virtual.StatusOK
		})

		var dirInfo ffi.FSP_FSCTL_DIR_INFO
		err = fs.GetDirInfoByName(ref, handle, "testfile.txt", &dirInfo)

		require.NoError(t, err)
		require.Equal(t, uint64(42), dirInfo.FileInfo.IndexNumber)
		require.Equal(t, uint64(100), dirInfo.FileInfo.FileSize)
		require.Equal(t, uint32(windows.FILE_ATTRIBUTE_NORMAL), dirInfo.FileInfo.FileAttributes)
	})

	t.Run("GetDirInfoByNameNonExistentFile", func(t *testing.T) {
		// Get a handle to the root directory
		rootDirectory.EXPECT().VirtualGetAttributes(
			gomock.Any(),
			winfsp.AttributesMaskForWinFSPAttr,
			gomock.Any(),
		).Do(func(ctx context.Context, requested virtual.AttributesMask, attributes *virtual.Attributes) {
			attributes.SetFileType(filesystem.FileTypeDirectory)
			attributes.SetInodeNumber(1)
			attributes.SetPermissions(virtual.PermissionsRead | virtual.PermissionsWrite | virtual.PermissionsExecute)
			attributes.SetLinkCount(1)
		})

		var info ffi.FSP_FSCTL_FILE_INFO
		handle, err := fs.Create(
			ref,
			"\\",
			dispositionToOptions(windows.FILE_OPEN)|windows.FILE_DIRECTORY_FILE,
			windows.FILE_LIST_DIRECTORY,
			0,
			nil,
			0,
			&info,
		)
		require.NoError(t, err)

		// Mock the VirtualLookup call to return StatusErrNoEnt
		rootDirectory.EXPECT().VirtualLookup(
			gomock.Any(),
			path.MustNewComponent("nonexistent.txt"),
			winfsp.AttributesMaskForWinFSPAttr,
			gomock.Any(),
		).Return(virtual.DirectoryChild{}, virtual.StatusErrNoEnt)

		var dirInfo ffi.FSP_FSCTL_DIR_INFO
		err = fs.GetDirInfoByName(ref, handle, "nonexistent.txt", &dirInfo)

		require.Error(t, err)
		require.Equal(t, windows.STATUS_OBJECT_NAME_NOT_FOUND, err)
	})
}

func TestWinFSPFileSystemSetBasicInfo(t *testing.T) {
	ctrl := gomock.NewController(t)
	rootDirectory := mock.NewMockVirtualDirectory(ctrl)
	file := mock.NewMockVirtualLeaf(ctrl)

	fs := winfsp.NewFileSystem(rootDirectory, false)
	ref := &ffi.FileSystemRef{}

	t.Run("SetBasicInfoAttributes", func(t *testing.T) {
		rootDirectory.EXPECT().VirtualOpenChild(
			gomock.Any(),
			path.MustNewComponent("testfile.txt"),
			virtual.ShareMaskWrite,
			(&virtual.Attributes{}).SetPermissions(virtual.PermissionsExecute|virtual.PermissionsRead|virtual.PermissionsWrite),
			&virtual.OpenExistingOptions{Truncate: true},
			winfsp.AttributesMaskForWinFSPAttr,
			gomock.Any(),
		).DoAndReturn(func(ctx context.Context, name path.Component, shareAccess virtual.ShareMask, createAttributes *virtual.Attributes, existingOptions *virtual.OpenExistingOptions, requested virtual.AttributesMask, out *virtual.Attributes) (virtual.Leaf, virtual.AttributesMask, virtual.ChangeInfo, virtual.Status) {
			out.SetFileType(filesystem.FileTypeRegularFile)
			out.SetInodeNumber(100)
			out.SetSizeBytes(50)
			out.SetPermissions(virtual.PermissionsRead | virtual.PermissionsWrite)
			out.SetLinkCount(1)
			out.SetLastDataModificationTime(time.Unix(1000, 0))
			return file, 0, virtual.ChangeInfo{}, virtual.StatusOK
		})

		var createInfo ffi.FSP_FSCTL_FILE_INFO
		handle, err := fs.Create(
			ref,
			"\\testfile.txt",
			0,
			windows.FILE_WRITE_DATA,
			0,
			nil,
			0,
			&createInfo,
		)
		require.NoError(t, err)

		// Mock setting read-only attribute
		file.EXPECT().VirtualSetAttributes(
			gomock.Any(),
			gomock.Any(),
			winfsp.AttributesMaskForWinFSPAttr,
			gomock.Any(),
		).DoAndReturn(func(ctx context.Context, in *virtual.Attributes, attributesMask virtual.AttributesMask, out *virtual.Attributes) virtual.Status {
			permissions, hasPermissions := in.GetPermissions()
			require.True(t, hasPermissions)
			require.Equal(t, virtual.PermissionsRead|virtual.PermissionsExecute, permissions)

			out.SetFileType(filesystem.FileTypeRegularFile)
			out.SetInodeNumber(100)
			out.SetSizeBytes(50)
			out.SetPermissions(permissions)
			out.SetLinkCount(1)
			out.SetLastDataModificationTime(time.Unix(1000, 0))
			return virtual.StatusOK
		})

		var info ffi.FSP_FSCTL_FILE_INFO
		err = fs.SetBasicInfo(
			ref,
			handle,
			ffi.SetBasicInfoAttributes,
			windows.FILE_ATTRIBUTE_READONLY,
			0, 0, 0, 0,
			&info,
		)

		require.NoError(t, err)
		require.Equal(t, uint32(windows.FILE_ATTRIBUTE_NORMAL|windows.FILE_ATTRIBUTE_READONLY), info.FileAttributes)
	})

	t.Run("SetBasicInfoLastWriteTime", func(t *testing.T) {
		rootDirectory.EXPECT().VirtualOpenChild(
			gomock.Any(),
			path.MustNewComponent("timefile.txt"),
			virtual.ShareMaskWrite,
			(&virtual.Attributes{}).SetPermissions(virtual.PermissionsExecute|virtual.PermissionsRead|virtual.PermissionsWrite),
			&virtual.OpenExistingOptions{Truncate: true},
			winfsp.AttributesMaskForWinFSPAttr,
			gomock.Any(),
		).DoAndReturn(func(ctx context.Context, name path.Component, shareAccess virtual.ShareMask, createAttributes *virtual.Attributes, existingOptions *virtual.OpenExistingOptions, requested virtual.AttributesMask, out *virtual.Attributes) (virtual.Leaf, virtual.AttributesMask, virtual.ChangeInfo, virtual.Status) {
			out.SetFileType(filesystem.FileTypeRegularFile)
			out.SetInodeNumber(200)
			out.SetSizeBytes(25)
			out.SetPermissions(virtual.PermissionsRead | virtual.PermissionsWrite)
			out.SetLinkCount(1)
			out.SetLastDataModificationTime(time.Unix(1000, 0))
			return file, 0, virtual.ChangeInfo{}, virtual.StatusOK
		})

		var createInfo ffi.FSP_FSCTL_FILE_INFO
		handle, err := fs.Create(
			ref,
			"\\timefile.txt",
			0,
			windows.FILE_WRITE_DATA,
			0,
			nil,
			0,
			&createInfo,
		)
		require.NoError(t, err)

		// Mock setting last write time
		newTime := time.Unix(2000, 0)

		file.EXPECT().VirtualSetAttributes(
			gomock.Any(),
			gomock.Any(),
			winfsp.AttributesMaskForWinFSPAttr,
			gomock.Any(),
		).DoAndReturn(func(ctx context.Context, in *virtual.Attributes, attributesMask virtual.AttributesMask, out *virtual.Attributes) virtual.Status {
			require.True(t, attributesMask&virtual.AttributesMaskLastDataModificationTime != 0)

			modTime, hasModTime := in.GetLastDataModificationTime()
			require.True(t, hasModTime)
			require.Equal(t, newTime.Unix(), modTime.Unix())

			out.SetFileType(filesystem.FileTypeRegularFile)
			out.SetInodeNumber(200)
			out.SetSizeBytes(25)
			out.SetPermissions(virtual.PermissionsRead | virtual.PermissionsWrite)
			out.SetLinkCount(1)
			out.SetLastDataModificationTime(modTime)
			return virtual.StatusOK
		})

		var info ffi.FSP_FSCTL_FILE_INFO
		err = fs.SetBasicInfo(
			ref,
			handle,
			ffi.SetBasicInfoLastWriteTime,
			0,
			0, 0, filetime.Timestamp(newTime), 0,
			&info,
		)

		require.NoError(t, err)
		require.Equal(t, filetime.Timestamp(newTime), info.LastWriteTime)
	})
}

func TestWinFSPFileSystemSymlinkCreation(t *testing.T) {
	ctrl := gomock.NewController(t)
	rootDirectory := mock.NewMockVirtualDirectory(ctrl)
	file := mock.NewMockVirtualLeaf(ctrl)
	symlink := mock.NewMockVirtualLeaf(ctrl)

	fs := winfsp.NewFileSystem(rootDirectory, false)
	ref := &ffi.FileSystemRef{}

	// Helper function to create a symlink reparse buffer.
	createSymlinkReparseBuffer := func(target string, flags uint32) []byte {
		buffer := make([]byte, 1024)
		used, err := winfsp.FillSymlinkReparseBuffer(target, flags, buffer)
		require.NoError(t, err)
		return buffer[:used]
	}

	// Symlinks are a bit odd: WinFSP creates symlinks by creating regular
	// files and then calling SetReparsePoint.

	t.Run("CreateSymlinkUsingSetReparsePoint", func(t *testing.T) {
		rootDirectory.EXPECT().VirtualOpenChild(
			gomock.Any(),
			path.MustNewComponent("symlink.txt"),
			virtual.ShareMaskWrite,
			(&virtual.Attributes{}).SetPermissions(virtual.PermissionsExecute|virtual.PermissionsRead|virtual.PermissionsWrite),
			&virtual.OpenExistingOptions{Truncate: true},
			winfsp.AttributesMaskForWinFSPAttr,
			gomock.Any(),
		).DoAndReturn(func(ctx context.Context, name path.Component, shareAccess virtual.ShareMask, createAttributes *virtual.Attributes, existingOptions *virtual.OpenExistingOptions, requested virtual.AttributesMask, out *virtual.Attributes) (virtual.Leaf, virtual.AttributesMask, virtual.ChangeInfo, virtual.Status) {
			out.SetFileType(filesystem.FileTypeRegularFile)
			out.SetInodeNumber(1100)
			out.SetSizeBytes(0)
			out.SetPermissions(virtual.PermissionsExecute | virtual.PermissionsRead | virtual.PermissionsWrite)
			out.SetLinkCount(1)
			return file, 0, virtual.ChangeInfo{}, virtual.StatusOK
		})

		var info ffi.FSP_FSCTL_FILE_INFO
		handle, err := fs.Create(
			ref,
			"\\symlink.txt",
			0,
			windows.FILE_WRITE_DATA,
			0,
			nil,
			0,
			&info,
		)
		require.NoError(t, err)
		require.NotZero(t, handle)

		targetPath := "target.txt"
		reparseBuffer := createSymlinkReparseBuffer(targetPath, windowsext.SYMLINK_FLAG_RELATIVE)

		rootDirectory.EXPECT().VirtualRemove(
			path.MustNewComponent("symlink.txt"),
			true,
			true,
		).Return(virtual.ChangeInfo{}, virtual.StatusOK)

		rootDirectory.EXPECT().VirtualSymlink(
			gomock.Any(),
			[]byte(targetPath),
			path.MustNewComponent("symlink.txt"),
			virtual.AttributesMaskFileType,
			gomock.Any(),
		).DoAndReturn(func(ctx context.Context, target []byte, name path.Component, requested virtual.AttributesMask, out *virtual.Attributes) (virtual.Leaf, virtual.ChangeInfo, virtual.Status) {
			out.SetFileType(filesystem.FileTypeSymlink)
			out.SetInodeNumber(1101)
			out.SetPermissions(virtual.PermissionsRead)
			out.SetLinkCount(1)
			return symlink, virtual.ChangeInfo{}, virtual.StatusOK
		})

		err = fs.SetReparsePoint(ref, handle, "\\symlink.txt", reparseBuffer)
		require.NoError(t, err)
	})

	t.Run("CreateSymlinkWithAbsolutePath", func(t *testing.T) {
		rootDirectory.EXPECT().VirtualOpenChild(
			gomock.Any(),
			path.MustNewComponent("abs_symlink.txt"),
			virtual.ShareMaskWrite,
			(&virtual.Attributes{}).SetPermissions(virtual.PermissionsExecute|virtual.PermissionsRead|virtual.PermissionsWrite),
			&virtual.OpenExistingOptions{Truncate: true},
			winfsp.AttributesMaskForWinFSPAttr,
			gomock.Any(),
		).DoAndReturn(func(ctx context.Context, name path.Component, shareAccess virtual.ShareMask, createAttributes *virtual.Attributes, existingOptions *virtual.OpenExistingOptions, requested virtual.AttributesMask, out *virtual.Attributes) (virtual.Leaf, virtual.AttributesMask, virtual.ChangeInfo, virtual.Status) {
			out.SetFileType(filesystem.FileTypeRegularFile)
			out.SetInodeNumber(1102)
			out.SetSizeBytes(0)
			out.SetPermissions(virtual.PermissionsExecute | virtual.PermissionsRead | virtual.PermissionsWrite)
			out.SetLinkCount(1)
			return file, 0, virtual.ChangeInfo{}, virtual.StatusOK
		})

		var info ffi.FSP_FSCTL_FILE_INFO
		handle, err := fs.Create(
			ref,
			"\\abs_symlink.txt",
			0,
			windows.FILE_WRITE_DATA,
			0,
			nil,
			0,
			&info,
		)
		require.NoError(t, err)

		targetPath := "C:\\absolute\\target.txt"
		reparseBuffer := createSymlinkReparseBuffer(targetPath, 0)

		rootDirectory.EXPECT().VirtualRemove(
			path.MustNewComponent("abs_symlink.txt"),
			true,
			true,
		).Return(virtual.ChangeInfo{}, virtual.StatusOK)

		rootDirectory.EXPECT().VirtualSymlink(
			gomock.Any(),
			[]byte(targetPath),
			path.MustNewComponent("abs_symlink.txt"),
			virtual.AttributesMaskFileType,
			gomock.Any(),
		).DoAndReturn(func(ctx context.Context, target []byte, name path.Component, requested virtual.AttributesMask, out *virtual.Attributes) (virtual.Leaf, virtual.ChangeInfo, virtual.Status) {
			out.SetFileType(filesystem.FileTypeSymlink)
			out.SetInodeNumber(1103)
			out.SetPermissions(virtual.PermissionsRead)
			out.SetLinkCount(1)
			return symlink, virtual.ChangeInfo{}, virtual.StatusOK
		})

		err = fs.SetReparsePoint(ref, handle, "\\abs_symlink.txt", reparseBuffer)
		require.NoError(t, err)
	})

	t.Run("SetReparsePointInvalidBuffer", func(t *testing.T) {
		// Test with an buffer that's too small.
		rootDirectory.EXPECT().VirtualOpenChild(
			gomock.Any(),
			path.MustNewComponent("invalid_symlink.txt"),
			virtual.ShareMaskWrite,
			(&virtual.Attributes{}).SetPermissions(virtual.PermissionsExecute|virtual.PermissionsRead|virtual.PermissionsWrite),
			&virtual.OpenExistingOptions{Truncate: true},
			winfsp.AttributesMaskForWinFSPAttr,
			gomock.Any(),
		).DoAndReturn(func(ctx context.Context, name path.Component, shareAccess virtual.ShareMask, createAttributes *virtual.Attributes, existingOptions *virtual.OpenExistingOptions, requested virtual.AttributesMask, out *virtual.Attributes) (virtual.Leaf, virtual.AttributesMask, virtual.ChangeInfo, virtual.Status) {
			out.SetFileType(filesystem.FileTypeRegularFile)
			out.SetInodeNumber(1104)
			out.SetSizeBytes(0)
			out.SetPermissions(virtual.PermissionsExecute | virtual.PermissionsRead | virtual.PermissionsWrite)
			out.SetLinkCount(1)
			return file, 0, virtual.ChangeInfo{}, virtual.StatusOK
		})

		var info ffi.FSP_FSCTL_FILE_INFO
		handle, err := fs.Create(
			ref,
			"\\invalid_symlink.txt",
			0,
			windows.FILE_WRITE_DATA,
			0,
			nil,
			0,
			&info,
		)
		require.NoError(t, err)

		smallBuffer := make([]byte, 4)

		err = fs.SetReparsePoint(ref, handle, "\\invalid_symlink.txt", smallBuffer)
		require.Error(t, err)
		require.Equal(t, windows.STATUS_INVALID_PARAMETER, err)
	})

	t.Run("SetReparsePointUnsupportedTag", func(t *testing.T) {
		// Test with an unsupported reparse tag.
		rootDirectory.EXPECT().VirtualOpenChild(
			gomock.Any(),
			path.MustNewComponent("unsupported_reparse.txt"),
			virtual.ShareMaskWrite,
			(&virtual.Attributes{}).SetPermissions(virtual.PermissionsExecute|virtual.PermissionsRead|virtual.PermissionsWrite),
			&virtual.OpenExistingOptions{Truncate: true},
			winfsp.AttributesMaskForWinFSPAttr,
			gomock.Any(),
		).DoAndReturn(func(ctx context.Context, name path.Component, shareAccess virtual.ShareMask, createAttributes *virtual.Attributes, existingOptions *virtual.OpenExistingOptions, requested virtual.AttributesMask, out *virtual.Attributes) (virtual.Leaf, virtual.AttributesMask, virtual.ChangeInfo, virtual.Status) {
			out.SetFileType(filesystem.FileTypeRegularFile)
			out.SetInodeNumber(1105)
			out.SetSizeBytes(0)
			out.SetPermissions(virtual.PermissionsExecute | virtual.PermissionsRead | virtual.PermissionsWrite)
			out.SetLinkCount(1)
			return file, 0, virtual.ChangeInfo{}, virtual.StatusOK
		})

		var info ffi.FSP_FSCTL_FILE_INFO
		handle, err := fs.Create(
			ref,
			"\\unsupported_reparse.txt",
			0,
			windows.FILE_WRITE_DATA,
			0,
			nil,
			0,
			&info,
		)
		require.NoError(t, err)

		buffer := make([]byte, int(unsafe.Sizeof(windowsext.REPARSE_DATA_BUFFER{})))
		rdb := (*windowsext.REPARSE_DATA_BUFFER)(unsafe.Pointer(&buffer[0]))
		rdb.ReparseTag = 0x12345678
		rdb.ReparseDataLength = 0
		rdb.Reserved = 0

		err = fs.SetReparsePoint(ref, handle, "\\unsupported_reparse.txt", buffer)
		require.Error(t, err)
		require.Equal(t, windows.STATUS_IO_REPARSE_TAG_MISMATCH, err)
	})
}

func TestWinFSPFileSystemGetSecurity(t *testing.T) {
	ctrl := gomock.NewController(t)
	rootDirectory := mock.NewMockVirtualDirectory(ctrl)
	file := mock.NewMockVirtualLeaf(ctrl)

	fs := winfsp.NewFileSystem(rootDirectory, false)
	ref := &ffi.FileSystemRef{}

	t.Run("GetSecurityForFile", func(t *testing.T) {
		rootDirectory.EXPECT().VirtualOpenChild(
			gomock.Any(),
			path.MustNewComponent("security_test.txt"),
			virtual.ShareMaskRead,
			(&virtual.Attributes{}).SetPermissions(virtual.PermissionsExecute|virtual.PermissionsRead|virtual.PermissionsWrite),
			&virtual.OpenExistingOptions{Truncate: true},
			winfsp.AttributesMaskForWinFSPAttr,
			gomock.Any(),
		).DoAndReturn(func(ctx context.Context, name path.Component, shareAccess virtual.ShareMask, createAttributes *virtual.Attributes, existingOptions *virtual.OpenExistingOptions, requested virtual.AttributesMask, out *virtual.Attributes) (virtual.Leaf, virtual.AttributesMask, virtual.ChangeInfo, virtual.Status) {
			out.SetFileType(filesystem.FileTypeRegularFile)
			out.SetInodeNumber(1200)
			out.SetSizeBytes(100)
			out.SetPermissions(virtual.PermissionsRead | virtual.PermissionsWrite)
			out.SetLinkCount(1)
			return file, 0, virtual.ChangeInfo{}, virtual.StatusOK
		})

		var createInfo ffi.FSP_FSCTL_FILE_INFO
		handle, err := fs.Create(
			ref,
			"\\security_test.txt",
			0,
			windows.FILE_READ_DATA,
			0,
			nil,
			0,
			&createInfo,
		)
		require.NoError(t, err)

		file.EXPECT().VirtualGetAttributes(
			gomock.Any(),
			winfsp.AttributesMaskForWinFSPAttr,
			gomock.Any(),
		).Do(func(ctx context.Context, requested virtual.AttributesMask, attributes *virtual.Attributes) {
			attributes.SetFileType(filesystem.FileTypeRegularFile)
			attributes.SetInodeNumber(1200)
			attributes.SetSizeBytes(100)
			attributes.SetPermissions(virtual.PermissionsRead | virtual.PermissionsWrite)
			attributes.SetLinkCount(1)
		})

		sd, err := fs.GetSecurity(ref, handle)
		require.NoError(t, err)
		require.NotNil(t, sd)
		dacl, err := getAclEntries(sd)
		require.NoError(t, err)
		require.Len(t, dacl, 2)
		require.Equal(t, uint8(windows.ACCESS_ALLOWED_ACE_TYPE), dacl[0].Header.AceType)
		require.Equal(t, windows.ACCESS_MASK(0x1f01bf), dacl[0].Mask)
		require.Equal(t, uint8(windows.ACCESS_ALLOWED_ACE_TYPE), dacl[1].Header.AceType)
		require.Equal(t, windows.ACCESS_MASK(0x1201bf), dacl[1].Mask)
	})

	t.Run("GetSecurityForDirectory", func(t *testing.T) {
		rootDirectory.EXPECT().VirtualGetAttributes(
			gomock.Any(),
			winfsp.AttributesMaskForWinFSPAttr,
			gomock.Any(),
		).Do(func(ctx context.Context, requested virtual.AttributesMask, attributes *virtual.Attributes) {
			attributes.SetFileType(filesystem.FileTypeDirectory)
			attributes.SetInodeNumber(1)
			attributes.SetPermissions(virtual.PermissionsRead | virtual.PermissionsWrite | virtual.PermissionsExecute)
			attributes.SetLinkCount(2)
		})

		var info ffi.FSP_FSCTL_FILE_INFO
		handle, err := fs.Create(
			ref,
			"\\",
			dispositionToOptions(windows.FILE_OPEN)|windows.FILE_DIRECTORY_FILE,
			windows.FILE_LIST_DIRECTORY,
			0,
			nil,
			0,
			&info,
		)
		require.NoError(t, err)

		rootDirectory.EXPECT().VirtualGetAttributes(
			gomock.Any(),
			winfsp.AttributesMaskForWinFSPAttr,
			gomock.Any(),
		).Do(func(ctx context.Context, requested virtual.AttributesMask, attributes *virtual.Attributes) {
			attributes.SetFileType(filesystem.FileTypeDirectory)
			attributes.SetInodeNumber(1)
			attributes.SetPermissions(virtual.PermissionsRead | virtual.PermissionsWrite | virtual.PermissionsExecute)
			attributes.SetLinkCount(2)
		})

		sd, err := fs.GetSecurity(ref, handle)
		require.NoError(t, err)
		dacl, err := getAclEntries(sd)
		require.NoError(t, err)
		require.Len(t, dacl, 2)
		require.Equal(t, uint8(windows.ACCESS_ALLOWED_ACE_TYPE), dacl[0].Header.AceType)
		require.Equal(t, windows.ACCESS_MASK(0x1f01ff), dacl[0].Mask)
		require.Equal(t, uint8(windows.ACCESS_ALLOWED_ACE_TYPE), dacl[1].Header.AceType)
		require.Equal(t, windows.ACCESS_MASK(0x1201ff), dacl[1].Mask)
	})

	t.Run("GetSecurityForReadOnlyFile", func(t *testing.T) {
		rootDirectory.EXPECT().VirtualOpenChild(
			gomock.Any(),
			path.MustNewComponent("readonly.txt"),
			virtual.ShareMaskRead,
			(&virtual.Attributes{}).SetPermissions(virtual.PermissionsExecute|virtual.PermissionsRead|virtual.PermissionsWrite),
			&virtual.OpenExistingOptions{Truncate: true},
			winfsp.AttributesMaskForWinFSPAttr,
			gomock.Any(),
		).DoAndReturn(func(ctx context.Context, name path.Component, shareAccess virtual.ShareMask, createAttributes *virtual.Attributes, existingOptions *virtual.OpenExistingOptions, requested virtual.AttributesMask, out *virtual.Attributes) (virtual.Leaf, virtual.AttributesMask, virtual.ChangeInfo, virtual.Status) {
			out.SetFileType(filesystem.FileTypeRegularFile)
			out.SetInodeNumber(1201)
			out.SetSizeBytes(50)
			out.SetPermissions(virtual.PermissionsRead)
			out.SetLinkCount(1)
			return file, 0, virtual.ChangeInfo{}, virtual.StatusOK
		})

		var createInfo ffi.FSP_FSCTL_FILE_INFO
		handle, err := fs.Create(
			ref,
			"\\readonly.txt",
			0,
			windows.FILE_READ_DATA,
			0,
			nil,
			0,
			&createInfo,
		)
		require.NoError(t, err)

		file.EXPECT().VirtualGetAttributes(
			gomock.Any(),
			winfsp.AttributesMaskForWinFSPAttr,
			gomock.Any(),
		).Do(func(ctx context.Context, requested virtual.AttributesMask, attributes *virtual.Attributes) {
			attributes.SetFileType(filesystem.FileTypeRegularFile)
			attributes.SetInodeNumber(1201)
			attributes.SetSizeBytes(50)
			attributes.SetPermissions(virtual.PermissionsRead)
			attributes.SetLinkCount(1)
		})

		sd, err := fs.GetSecurity(ref, handle)
		require.NoError(t, err)
		dacl, err := getAclEntries(sd)
		require.NoError(t, err)
		require.Len(t, dacl, 2)
		require.Equal(t, uint8(windows.ACCESS_ALLOWED_ACE_TYPE), dacl[0].Header.AceType)
		require.Equal(t, windows.ACCESS_MASK(0x1f01b9), dacl[0].Mask)
		require.Equal(t, uint8(windows.ACCESS_ALLOWED_ACE_TYPE), dacl[1].Header.AceType)
		require.Equal(t, windows.ACCESS_MASK(0x1200a9), dacl[1].Mask)
	})
}

func TestWinFSPFileSystemGetSecurityByName(t *testing.T) {
	ctrl := gomock.NewController(t)
	rootDirectory := mock.NewMockVirtualDirectory(ctrl)
	file := mock.NewMockVirtualLeaf(ctrl)

	fs := winfsp.NewFileSystem(rootDirectory, false)
	ref := &ffi.FileSystemRef{}

	t.Run("GetSecurityByNameForFile", func(t *testing.T) {
		rootDirectory.EXPECT().VirtualLookup(
			gomock.Any(),
			path.MustNewComponent("security_by_name.txt"),
			winfsp.AttributesMaskForWinFSPAttr,
			gomock.Any(),
		).DoAndReturn(func(ctx context.Context, name path.Component, requested virtual.AttributesMask, out *virtual.Attributes) (virtual.DirectoryChild, virtual.Status) {
			out.SetFileType(filesystem.FileTypeRegularFile)
			out.SetInodeNumber(1300)
			out.SetSizeBytes(200)
			out.SetPermissions(virtual.PermissionsRead | virtual.PermissionsWrite | virtual.PermissionsExecute)
			out.SetLinkCount(1)
			return virtual.DirectoryChild{}.FromLeaf(file), virtual.StatusOK
		})

		fa, sd, err := fs.GetSecurityByName(
			ref,
			"\\security_by_name.txt",
			ffi.GetSecurityByName,
		)
		require.NoError(t, err)
		require.Equal(t, uint32(windows.FILE_ATTRIBUTE_NORMAL), fa)
		require.NotNil(t, sd)
		dacl, err := getAclEntries(sd)
		require.NoError(t, err)
		require.Len(t, dacl, 2)
		require.Equal(t, uint8(windows.ACCESS_ALLOWED_ACE_TYPE), dacl[0].Header.AceType)
		require.Equal(t, windows.ACCESS_MASK(0x1f01bf), dacl[0].Mask)
		require.Equal(t, uint8(windows.ACCESS_ALLOWED_ACE_TYPE), dacl[1].Header.AceType)
		require.Equal(t, windows.ACCESS_MASK(0x1201bf), dacl[1].Mask)
	})

	t.Run("GetSecurityByNameForRootDirectory", func(t *testing.T) {
		rootDirectory.EXPECT().VirtualGetAttributes(
			gomock.Any(),
			winfsp.AttributesMaskForWinFSPAttr,
			gomock.Any(),
		).Do(func(ctx context.Context, requested virtual.AttributesMask, attributes *virtual.Attributes) {
			attributes.SetFileType(filesystem.FileTypeDirectory)
			attributes.SetInodeNumber(1)
			attributes.SetPermissions(virtual.PermissionsRead | virtual.PermissionsWrite | virtual.PermissionsExecute)
			attributes.SetLinkCount(2)
		})

		fa, sd, err := fs.GetSecurityByName(
			ref,
			"\\",
			ffi.GetSecurityByName,
		)

		require.NoError(t, err)
		require.Equal(t, uint32(windows.FILE_ATTRIBUTE_DIRECTORY), fa)
		require.NotNil(t, sd)
		dacl, err := getAclEntries(sd)
		require.NoError(t, err)
		require.Len(t, dacl, 2)
		require.Equal(t, uint8(windows.ACCESS_ALLOWED_ACE_TYPE), dacl[0].Header.AceType)
		require.Equal(t, windows.ACCESS_MASK(0x1f01ff), dacl[0].Mask)
		require.Equal(t, uint8(windows.ACCESS_ALLOWED_ACE_TYPE), dacl[1].Header.AceType)
		require.Equal(t, windows.ACCESS_MASK(0x1201ff), dacl[1].Mask)
	})

	t.Run("GetSecurityByNameExistenceOnly", func(t *testing.T) {
		rootDirectory.EXPECT().VirtualLookup(
			gomock.Any(),
			path.MustNewComponent("exists.txt"),
			winfsp.AttributesMaskForWinFSPAttr,
			gomock.Any(),
		).DoAndReturn(func(ctx context.Context, name path.Component, requested virtual.AttributesMask, out *virtual.Attributes) (virtual.DirectoryChild, virtual.Status) {
			out.SetFileType(filesystem.FileTypeRegularFile)
			out.SetInodeNumber(1301)
			out.SetSizeBytes(150)
			out.SetPermissions(virtual.PermissionsRead)
			out.SetLinkCount(1)
			return virtual.DirectoryChild{}.FromLeaf(file), virtual.StatusOK
		})

		fileAttributes, securityDescriptor, err := fs.GetSecurityByName(
			ref,
			"\\exists.txt",
			ffi.GetExistenceOnly,
		)

		require.NoError(t, err)
		require.Equal(t, uint32(0), fileAttributes)
		require.Nil(t, securityDescriptor)
	})

	t.Run("GetSecurityByNameForReadOnlyFile", func(t *testing.T) {
		rootDirectory.EXPECT().VirtualLookup(
			gomock.Any(),
			path.MustNewComponent("readonly_by_name.txt"),
			winfsp.AttributesMaskForWinFSPAttr,
			gomock.Any(),
		).DoAndReturn(func(ctx context.Context, name path.Component, requested virtual.AttributesMask, out *virtual.Attributes) (virtual.DirectoryChild, virtual.Status) {
			out.SetFileType(filesystem.FileTypeRegularFile)
			out.SetInodeNumber(1302)
			out.SetSizeBytes(75)
			out.SetPermissions(virtual.PermissionsRead)
			out.SetLinkCount(1)
			return virtual.DirectoryChild{}.FromLeaf(file), virtual.StatusOK
		})

		fileAttributes, securityDescriptor, err := fs.GetSecurityByName(
			ref,
			"\\readonly_by_name.txt",
			ffi.GetSecurityByName,
		)

		require.NoError(t, err)
		require.Equal(t, uint32(windows.FILE_ATTRIBUTE_NORMAL|windows.FILE_ATTRIBUTE_READONLY), fileAttributes)
		require.NotNil(t, securityDescriptor)
	})

	t.Run("GetSecurityByNameForSymlink", func(t *testing.T) {
		// Test for a symlink
		rootDirectory.EXPECT().VirtualLookup(
			gomock.Any(),
			path.MustNewComponent("symlink_by_name.txt"),
			winfsp.AttributesMaskForWinFSPAttr,
			gomock.Any(),
		).DoAndReturn(func(ctx context.Context, name path.Component, requested virtual.AttributesMask, out *virtual.Attributes) (virtual.DirectoryChild, virtual.Status) {
			out.SetFileType(filesystem.FileTypeSymlink)
			out.SetInodeNumber(1303)
			out.SetPermissions(virtual.PermissionsRead)
			out.SetLinkCount(1)
			return virtual.DirectoryChild{}.FromLeaf(file), virtual.StatusOK
		})

		fileAttributes, securityDescriptor, err := fs.GetSecurityByName(
			ref,
			"\\symlink_by_name.txt",
			ffi.GetSecurityByName,
		)

		require.NoError(t, err)
		require.Equal(t, uint32(windows.FILE_ATTRIBUTE_REPARSE_POINT|windows.FILE_ATTRIBUTE_READONLY), fileAttributes)
		require.NotNil(t, securityDescriptor)
	})
}

// There are no unit tests for SetSecurity as that requires WinFSP to be
// installed for several functions such as
// PosixMapSecurityDescriptorToPermissions. We test this in the
// integration tests instead.
