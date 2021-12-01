//go:build freebsd || linux
// +build freebsd linux

package runner

const (
	// On Linux and FreeBSD, the getrusage(2) man pages document
	// that the resident set size is returned in kilobytes, though
	// kernel sources indicate kibibytes are used.
	maximumResidentSetSizeUnit = 1024
)
