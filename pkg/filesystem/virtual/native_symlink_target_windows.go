//go:build windows

package virtual

import "github.com/buildbarn/bb-storage/pkg/filesystem/path"

// nativeSymlinkTarget converts a REv2 symlink target (UNIX format)
// to the native platform format. On Windows this converts forward
// slashes to backslashes and strips trailing directory separators
// that would cause issues in reparse point substitute names.
func nativeSymlinkTarget(target string) ([]byte, error) {
	targetParser := path.UNIXFormat.NewParser(target)
	targetPath, scopeWalker := path.EmptyBuilder.Join(path.VoidScopeWalker)
	if err := path.Resolve(targetParser, scopeWalker); err != nil {
		return nil, err
	}
	s, err := targetPath.GetWindowsString(path.WindowsPathFormatStandard)
	if err != nil {
		return nil, err
	}
	return []byte(s), nil
}
