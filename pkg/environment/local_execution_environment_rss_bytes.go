// +build darwin

package environment

const (
	// On macOS, the getrusage(2) man page documents that the
	// resident set size is returned in bytes.
	maximumResidentSetSizeUnit = 1
)
