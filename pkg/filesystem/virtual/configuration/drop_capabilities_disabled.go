//go:build !linux
// +build !linux

package configuration

// dropMountCapabilities removes capabilities that are needed to mount a FUSE filesystem,
// but not needed after initialization.
//
// This is a placeholder implementation for operating systems other than
// Linux.
func dropMountCapabilities() error {
	return nil
}
