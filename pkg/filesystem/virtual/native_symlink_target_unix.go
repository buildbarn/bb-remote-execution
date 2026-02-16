//go:build !windows

package virtual

// nativeSymlinkTarget converts a REv2 symlink target (UNIX format)
// to the native platform format. On UNIX this is a no-op, as REv2
// already uses UNIX path separators.
func nativeSymlinkTarget(target string) ([]byte, error) {
	return []byte(target), nil
}
