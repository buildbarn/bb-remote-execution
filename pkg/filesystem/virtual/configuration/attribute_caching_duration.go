package configuration

import (
	"time"
)

// AttributeCachingDuration specifies the amount of time attributes of
// files may be cached by an NFSv4 file system that accesses a virtual
// file system.
//
// Unlike FUSE, NFSv4.0 provides no facilities for letting the server
// invalidate information on files cached by the client. To work around
// this, most NFS clients provide mount options such as 'acregmin',
// 'acregmax', 'acdirmin' and 'acdirmax' that permit specifying the
// amount of time attributes may be cached.
type AttributeCachingDuration struct {
	minimum time.Duration
	maximum time.Duration
}

// Min returns the lowest attribute cache duration. This can be used to
// combine multiple attribute caching durations into a single value, in
// case the NFS client makes no distinction between individual values.
func (a AttributeCachingDuration) Min(b AttributeCachingDuration) AttributeCachingDuration {
	if a.minimum > b.minimum {
		a.minimum = b.minimum
	}
	if a.maximum > b.maximum {
		a.maximum = b.maximum
	}
	return a
}

// NoAttributeCaching indicates that the NFS client should not cache any
// file attributes, such as size, modification time, permissions and
// symlink target.
//
// This is a good policy for bb_virtual_tmp's symbolic link, whose
// target may not be cached.
var NoAttributeCaching = AttributeCachingDuration{
	minimum: 0,
	maximum: 0,
}

// ShortAttributeCaching indicates that the NFS client should only cache
// file attributes for a short amount of time.
//
// This is a good policy for bb_worker's root directory. The contents of
// this directory change regularly, but there is a very low probability
// of immediate reuse of build action subdirectories having the same
// name. It is therefore desirable to have a limited amount of caching.
var ShortAttributeCaching = AttributeCachingDuration{
	minimum: time.Second,
	maximum: time.Second,
}

// LongAttributeCaching indicates that the NFS client may cache file
// attributes for a long amount of time.
//
// This is a good policy for files or directories that are either fully
// immutable, or are only mutated through operations performed by the
// client through the mount point.
var LongAttributeCaching = AttributeCachingDuration{
	minimum: time.Minute,
	maximum: 5 * time.Minute,
}
