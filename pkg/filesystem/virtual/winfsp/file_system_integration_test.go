//go:build windows
// +build windows

package winfsp_test

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/bazelbuild/rules_go/go/runfiles"
	"github.com/buildbarn/bb-remote-execution/pkg/filesystem/pool"
	"github.com/buildbarn/bb-remote-execution/pkg/filesystem/virtual"
	virtual_configuration "github.com/buildbarn/bb-remote-execution/pkg/filesystem/virtual/configuration"
	virtual_pb "github.com/buildbarn/bb-remote-execution/pkg/proto/configuration/filesystem/virtual"
	"github.com/buildbarn/bb-storage/pkg/blockdevice"
	"github.com/buildbarn/bb-storage/pkg/clock"
	"github.com/buildbarn/bb-storage/pkg/program"
	"github.com/buildbarn/bb-storage/pkg/util"
	"github.com/stretchr/testify/require"
	ffi "github.com/winfsp/go-winfsp"

	"golang.org/x/sys/windows"
)

func findFreeDriveLetter() (string, error) {
	for letter := 'Z'; letter >= 'A'; letter-- {
		if _, err := os.Stat(fmt.Sprintf("%c:\\", letter)); err != nil {
			// Drive appears to be free.
			return string(letter), nil
		}
	}
	return "", fmt.Errorf("no free drive letters available")
}

func createWinFSPMountForTest(t *testing.T, terminationGroup program.Group, caseSensitive bool) (string, blockdevice.BlockDevice) {
	// We can't run winfsp-tests at a directory path due to
	// https://github.com/winfsp/winfsp/issues/279. Instead find a free drive
	// letter and run it there instead.
	drive, err := findFreeDriveLetter()
	require.NoError(t, err, "Failed to find free drive letter")
	vfsPath := fmt.Sprintf(`%s:`, drive)

	// Create a WinFSP mount.
	mount, handleAllocator, err := virtual_configuration.NewMountFromConfiguration(
		&virtual_pb.MountConfiguration{
			MountPath: vfsPath,
			Backend:   &virtual_pb.MountConfiguration_Winfsp{},
		},
		"winfsp_integration_test",
		/* rootDirectory = */ virtual_configuration.LongAttributeCaching,
		/* childDirectories = */ virtual_configuration.LongAttributeCaching,
		/* leaves = */ virtual_configuration.NoAttributeCaching,
		caseSensitive)
	require.NoError(t, err, "Failed to create WinFSP mount")

	// Create a block device to store new files.
	bd, sectorSizeBytes, sectorCount, err := blockdevice.NewBlockDeviceFromFile(
		path.Join(t.TempDir(), "block_device"),
		10*1024*1024, // 10 MiB
		false,
	)
	require.NoError(t, err, "Failed to create block device")

	normalizer := virtual.CaseInsensitiveComponentNormalizer
	if caseSensitive {
		normalizer = virtual.CaseSensitiveComponentNormalizer
	}

	// Create a virtual directory to hold new files.
	defaultAttributesSetter := func(requested virtual.AttributesMask, attributes *virtual.Attributes) {}
	err = mount.Expose(
		terminationGroup,
		virtual.NewInMemoryPrepopulatedDirectory(
			virtual.NewHandleAllocatingFileAllocator(
				virtual.NewPoolBackedFileAllocator(
					pool.NewBlockDeviceBackedFilePool(
						bd,
						pool.NewBitmapSectorAllocator(uint32(sectorCount)),
						sectorSizeBytes,
					),
					util.DefaultErrorLogger,
					defaultAttributesSetter,
					virtual.NoNamedAttributesFactory,
				),
				handleAllocator,
			),
			virtual.NewHandleAllocatingSymlinkFactory(
				virtual.NewBaseSymlinkFactory(defaultAttributesSetter),
				handleAllocator.New(),
			),
			util.DefaultErrorLogger,
			handleAllocator,
			sort.Sort,
			func(s string) bool { return false },
			clock.SystemClock,
			normalizer,
			defaultAttributesSetter,
			virtual.NoNamedAttributesFactory,
		),
	)
	require.NoError(t, err, "Failed to expose mount point")

	return vfsPath, bd
}

func runWinFSPTests(t *testing.T, terminationGroup program.Group, caseSensitive bool, testArgs []string) {
	vfsPath, bd := createWinFSPMountForTest(t, terminationGroup, caseSensitive)
	defer bd.Close()

	winfspTestBinary, err := runfiles.Rlocation("com_github_winfsp_winfsp_tests/winfsp-tests-x64.exe")
	if err != nil {
		require.NoError(t, err, "Failed to locate WinFSP test binary in runfiles")
	}

	// Execute WinFSP tests
	cmd := exec.Command(
		winfspTestBinary,
		append(
			testArgs,
			"--external",
			"--resilient",
			// The archive attribute is unsupported.
			"-create_fileattr_test",
			// PoolBackedFileAllocator doesn't support read-only mode.
			"-create_readonlydir_test",
			// We don't support setting file's allocation sizes.
			"-create_allocation_test",
			// Unsupported reparse point kinds.
			"-reparse_guid_test",
			"-reparse_nfs_test",
			// Requires tracking creation time.
			"-getfileinfo_test",
			// Requires stream features.
			"-stream*",
			// Require more fine grained permissions
			"-create_notraverse_test",
			"-create_backup_test",
			"-create_restore_test",
			"-getfileattr_test",
			"-setfileinfo_test",
			"-delete_access_test",
			"-setsecurity_test",
		)...,
	)
	cmd.Dir = vfsPath

	// Update PATH to include WinFSP install directory so that winfsp-tests
	// can find the dll.
	winfspInstallDir, err := ffi.BinPath()
	require.NoError(t, err, "Could not locate WinFSP install directory")

	env := os.Environ()
	pathUpdated := false
	for i, envVar := range env {
		if strings.HasPrefix(strings.ToUpper(envVar), "PATH=") {
			env[i] = envVar + ";" + winfspInstallDir
			pathUpdated = true
			break
		}
	}
	if !pathUpdated {
		env = append(env, "PATH="+winfspInstallDir)
	}
	cmd.Env = env

	output, err := cmd.CombinedOutput()
	t.Logf("WinFSP test output:\n%s", string(output))
	require.NoError(t, err, "WinFSP test binary failed")
}

func TestWinFSPIntegration(t *testing.T) {
	t.Run("CaseInsensitive", func(t *testing.T) {
		program.RunLocal(context.Background(), func(ctx context.Context, siblingsGroup, dependenciesGroup program.Group) error {
			runWinFSPTests(t, dependenciesGroup, false, []string{
				"--case-insensitive",
				"--case-insensitive-cmp",
				// Requires a case insensitive but case preserving FS.
				"-getfileinfo_name_test",
			})
			return nil
		})
	})

	t.Run("CaseSensitive", func(t *testing.T) {
		program.RunLocal(context.Background(), func(ctx context.Context, siblingsGroup, dependenciesGroup program.Group) error {
			runWinFSPTests(t, dependenciesGroup, true, []string{
				// Requires a case insensitive FS.
				"-getfileinfo_name_test",
			})
			return nil
		})
	})
}

func TestWinFSPFileSystemSetSecurity(t *testing.T) {
	program.RunLocal(context.Background(), func(ctx context.Context, siblingsGroup, dependenciesGroup program.Group) error {
		vfsPath, bd := createWinFSPMountForTest(t, dependenciesGroup, false)
		defer bd.Close()

		testDir := filepath.Join(vfsPath, "security_test_dir")
		err := os.Mkdir(testDir, 0o755)
		require.NoError(t, err)

		testFile := filepath.Join(vfsPath, "security_test_file.txt")
		f, err := os.Create(testFile)
		require.NoError(t, err)
		_, err = f.WriteString("test content")
		require.NoError(t, err)
		f.Close()

		testOwnerFile := filepath.Join(vfsPath, "security_test_owner_file.txt")
		f, err = os.Create(testOwnerFile)
		require.NoError(t, err)
		f.Close()

		t.Run("SetSecurityOnFile", func(t *testing.T) {
			testFileUtf16, err := windows.UTF16FromString(testFile)
			require.NoError(t, err)

			h, err := windows.CreateFile(
				&testFileUtf16[0],
				windows.READ_CONTROL|windows.WRITE_DAC|windows.WRITE_OWNER,
				windows.FILE_SHARE_READ|windows.FILE_SHARE_WRITE,
				nil,
				windows.OPEN_EXISTING,
				0,
				0,
			)
			require.NoError(t, err, "Failed to open test file")
			defer windows.Close(h)

			sd, err := windows.GetSecurityInfo(
				h,
				windows.SE_FILE_OBJECT,
				windows.DACL_SECURITY_INFORMATION,
			)
			require.NoError(t, err)

			dacl, _, err := sd.DACL()
			require.NoError(t, err)
			err = windows.SetSecurityInfo(
				h,
				windows.SE_FILE_OBJECT,
				windows.DACL_SECURITY_INFORMATION,
				nil,
				nil,
				dacl,
				nil,
			)
			require.NoError(t, err)
		})

		t.Run("SetSecurityOnDirectory", func(t *testing.T) {
			testDirUtf16, err := windows.UTF16FromString(testDir)
			require.NoError(t, err)

			h, err := windows.CreateFile(
				&testDirUtf16[0],
				windows.READ_CONTROL|windows.WRITE_DAC|windows.WRITE_OWNER,
				windows.FILE_SHARE_READ|windows.FILE_SHARE_WRITE,
				nil,
				windows.OPEN_EXISTING,
				windows.FILE_FLAG_BACKUP_SEMANTICS,
				0,
			)
			require.NoError(t, err)
			defer windows.Close(h)

			sd, err := windows.GetSecurityInfo(
				h,
				windows.SE_FILE_OBJECT,
				windows.DACL_SECURITY_INFORMATION,
			)
			require.NoError(t, err)

			dacl, _, err := sd.DACL()
			require.NoError(t, err)

			err = windows.SetSecurityInfo(
				h,
				windows.SE_FILE_OBJECT,
				windows.DACL_SECURITY_INFORMATION,
				nil,
				nil,
				dacl,
				nil,
			)
			require.NoError(t, err)
		})

		t.Run("SetSecurityWithSimpleDACL", func(t *testing.T) {
			testFileUtf16, err := windows.UTF16FromString(testFile)
			require.NoError(t, err)

			h, err := windows.CreateFile(
				&testFileUtf16[0],
				windows.READ_CONTROL|windows.WRITE_DAC|windows.WRITE_OWNER,
				windows.FILE_SHARE_READ|windows.FILE_SHARE_WRITE,
				nil,
				windows.OPEN_EXISTING,
				0,
				0,
			)
			require.NoError(t, err, "Failed to open test file")
			defer windows.Close(h)

			// Create a simple security descriptor with everyone having
			// read access.
			everyoneSid, err := windows.CreateWellKnownSid(windows.WinWorldSid)
			require.NoError(t, err, "Failed to create Everyone SID")

			aclEntries := []windows.EXPLICIT_ACCESS{
				{
					AccessPermissions: windows.FILE_READ_DATA | windows.FILE_READ_ATTRIBUTES,
					AccessMode:        windows.GRANT_ACCESS,
					Inheritance:       windows.NO_INHERITANCE,
					Trustee: windows.TRUSTEE{
						TrusteeForm:  windows.TRUSTEE_IS_SID,
						TrusteeValue: windows.TrusteeValueFromSID(everyoneSid),
					},
				},
			}

			dacl, err := windows.ACLFromEntries(aclEntries, nil)
			require.NoError(t, err)

			err = windows.SetSecurityInfo(
				h,
				windows.SE_FILE_OBJECT,
				windows.DACL_SECURITY_INFORMATION,
				nil,
				nil,
				dacl,
				nil,
			)
			require.NoError(t, err)

			newSd, err := windows.GetSecurityInfo(
				h,
				windows.SE_FILE_OBJECT,
				windows.DACL_SECURITY_INFORMATION,
			)
			require.NoError(t, err)

			newDacl, _, err := newSd.DACL()
			require.NoError(t, err)
			require.NotNil(t, newDacl)
		})

		return nil
	})
}

func TestWinFSPFileSystemGetSecurityByNameIntegration(t *testing.T) {
	program.RunLocal(context.Background(), func(ctx context.Context, siblingsGroup, dependenciesGroup program.Group) error {
		vfsPath, bd := createWinFSPMountForTest(t, dependenciesGroup, false)
		defer bd.Close()

		testFile := filepath.Join(vfsPath, "security_test_file.txt")
		f, err := os.Create(testFile)
		require.NoError(t, err)
		f.Close()

		testDir := filepath.Join(vfsPath, "security_test_dir")
		err = os.Mkdir(testDir, 0o755)
		require.NoError(t, err)

		symlinkTarget := filepath.Join(vfsPath, "symlink_target.txt")
		f, err = os.Create(symlinkTarget)
		require.NoError(t, err)
		f.Close()

		testSymlink := filepath.Join(vfsPath, "test_symlink.txt")
		err = os.Symlink(symlinkTarget, testSymlink)
		require.NoError(t, err)

		t.Run("GetSecurityByNameOnFile", func(t *testing.T) {
			sd, err := windows.GetNamedSecurityInfo(
				testFile,
				windows.SE_FILE_OBJECT,
				windows.DACL_SECURITY_INFORMATION|windows.OWNER_SECURITY_INFORMATION|windows.GROUP_SECURITY_INFORMATION,
			)
			require.NoError(t, err)
			require.NotNil(t, sd)
		})

		t.Run("GetSecurityByNameOnDirectory", func(t *testing.T) {
			// Get security info using Windows API
			sd, err := windows.GetNamedSecurityInfo(
				testDir,
				windows.SE_FILE_OBJECT,
				windows.DACL_SECURITY_INFORMATION|windows.OWNER_SECURITY_INFORMATION|windows.GROUP_SECURITY_INFORMATION,
			)
			require.NoError(t, err)
			require.NotNil(t, sd)
		})

		t.Run("GetSecurityByNameOnSymlink", func(t *testing.T) {
			// Get security info for the symlink
			sd, err := windows.GetNamedSecurityInfo(
				testSymlink,
				windows.SE_FILE_OBJECT,
				windows.DACL_SECURITY_INFORMATION|windows.OWNER_SECURITY_INFORMATION|windows.GROUP_SECURITY_INFORMATION,
			)
			require.NoError(t, err)
			require.NotNil(t, sd)
		})

		return nil
	})
}

func TestWinFSPFileSystemCasePreserving(t *testing.T) {
	program.RunLocal(context.Background(), func(ctx context.Context, siblingsGroup, dependenciesGroup program.Group) error {
		vfsPath, bd := createWinFSPMountForTest(t, dependenciesGroup, false)
		defer bd.Close()

		t.Run("FileCreationAndAccess", func(t *testing.T) {
			originalFile := filepath.Join(vfsPath, "TestFile.txt")
			f, err := os.Create(originalFile)
			require.NoError(t, err)
			f.Close()

			for _, testPath := range []string{
				filepath.Join(vfsPath, "testfile.txt"),
				filepath.Join(vfsPath, "TESTFILE.TXT"),
				filepath.Join(vfsPath, "TestFile.txt"),
				filepath.Join(vfsPath, "testFILE.txt"),
			} {
				_, err := os.ReadFile(testPath)
				require.NoError(t, err, "Failed to read file using path: %s", testPath)
			}
		})

		t.Run("CaseInsensitiveCreateFail", func(t *testing.T) {
			originalFile := filepath.Join(vfsPath, "CaseTestFile.txt")
			f, err := os.OpenFile(originalFile, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o644)
			require.NoError(t, err)
			f.Close()

			duplicateFile := filepath.Join(vfsPath, "casetestfile.txt")
			_, err = os.OpenFile(duplicateFile, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o644)
			require.Error(t, err)

			err = os.Remove(filepath.Join(vfsPath, "CASETESTFILE.TXT"))
			require.NoError(t, err)

			_, err = os.Stat(originalFile)
			require.Error(t, err, "File should be deleted")
		})

		t.Run("CasePreservationInListing", func(t *testing.T) {
			items := []struct {
				name  string
				isDir bool
			}{
				{"MixedCaseFile.txt", false},
				{"UPPERCASE.TXT", false},
				{"lowercase.txt", false},
				{"MixedCaseDir", true},
				{"UPPERCASEDIR", true},
				{"lowercasedir", true},
			}

			for _, item := range items {
				fullPath := filepath.Join(vfsPath, item.name)
				if item.isDir {
					err := os.Mkdir(fullPath, 0o755)
					require.NoError(t, err)
				} else {
					f, err := os.Create(fullPath)
					require.NoError(t, err)
					f.Close()
				}
			}

			entries, err := os.ReadDir(vfsPath)
			require.NoError(t, err)

			foundNames := make([]string, 0, len(items))
			for _, entry := range entries {
				foundNames = append(foundNames, entry.Name())
			}
			for _, item := range items {
				require.Contains(t, foundNames, item.name)
			}
		})

		return nil
	})
}
