package runner

import (
	"syscall"
)

func init() {
	// Running Mach-O binaries for the wrong architecture returns
	// EBADARCH instead of ENOEXEC.
	invalidArgumentErrs = append(invalidArgumentErrs, syscall.EBADARCH)
}
