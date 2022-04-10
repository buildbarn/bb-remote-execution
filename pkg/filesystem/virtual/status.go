package virtual

// Status response of operations applied against Node objects.
type Status int

const (
	// StatusOK indicates that the operation succeeded.
	StatusOK Status = iota
	// StatusErrAccess indicates that the operation failed due to
	// permission being denied.
	StatusErrAccess
	// StatusErrBadHandle indicates that the provided file handle
	// failed internal consistency checks.
	StatusErrBadHandle
	// StatusErrExist indicates that a file system object of the
	// specified target name (when creating, renaming or linking)
	// already exists.
	StatusErrExist
	// StatusErrInval indicates that the arguments for this
	// operation are not valid.
	StatusErrInval
	// StatusErrIO indicates that the operation failed due to an I/O
	// error.
	StatusErrIO
	// StatusErrIsDir indicates that a request is made against a
	// directory when the current operation does not allow a
	// directory as a target.
	StatusErrIsDir
	// StatusErrNoEnt indicate sthat the operation failed due to a
	// file not existing.
	StatusErrNoEnt
	// StatusErrNotDir indicates that a request is made against a
	// leaf when the current operation does not allow a leaf as a
	// target.
	StatusErrNotDir
	// StatusErrNotEmpty indicates that attempt was made to remove a
	// directory that was not empty.
	StatusErrNotEmpty
	// StatusErrNXIO indicates that a request is made beyond the
	// limits of the file or device.
	StatusErrNXIO
	// StatusErrPerm indicates that the operation was not allowed
	// because the caller is neither a privileged user (root) nor
	// the owner of the target of the operation.
	StatusErrPerm
	// StatusErrROFS indicates that a modifying operation was
	// attempted on a read-only file system.
	StatusErrROFS
	// StatusErrStale indicates that the file system object referred
	// to by the file handle no longer exists, or access to it has
	// been revoked.
	StatusErrStale
	// StatusErrSymlink indicates that a request is made against a
	// symbolic link when the current operation does not allow a
	// symbolic link as a target.
	StatusErrSymlink
	// StatusErrWrongType that a request is made against an object
	// that is of an invalid type for the current operation, and
	// there is no more specific error (such as StatusErrIsDir or
	// StatusErrSymlink) that applies.
	StatusErrWrongType
	// StatusErrXDev indicates an attempt to do an operation, such
	// as linking, that inappropriately crosses a boundary.
	StatusErrXDev
)
