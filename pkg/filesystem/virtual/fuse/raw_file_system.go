//go:build darwin || freebsd || linux
// +build darwin freebsd linux

package fuse

import (
	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
)

// ReadDirEntryList can be called into by implementations of
// RawFileSystem.ReadDir() to append new directory entries to the
// results.
type ReadDirEntryList interface {
	AddDirEntry(e fuse.DirEntry) bool
}

// ReadDirPlusEntryList can be called into by implementations of
// RawFileSystem.ReadDirPlus() to append new directory entries to the
// results.
type ReadDirPlusEntryList interface {
	AddDirLookupEntry(e fuse.DirEntry) *fuse.EntryOut
}

// RawFileSystem is identical to go-fuse's own RawFileSystem interface,
// except that it fixes some of the function signatures to use
// interfaces instead of concrete types. This is done to ease testing
// and permit decorating.
type RawFileSystem interface {
	Access(cancel <-chan struct{}, input *fuse.AccessIn) fuse.Status
	CopyFileRange(cancel <-chan struct{}, input *fuse.CopyFileRangeIn) (uint32, fuse.Status)
	Create(cancel <-chan struct{}, input *fuse.CreateIn, name string, out *fuse.CreateOut) fuse.Status
	Fallocate(cancel <-chan struct{}, input *fuse.FallocateIn) fuse.Status
	Flush(cancel <-chan struct{}, input *fuse.FlushIn) fuse.Status
	Forget(nodeid, nlookup uint64)
	Fsync(cancel <-chan struct{}, input *fuse.FsyncIn) fuse.Status
	FsyncDir(cancel <-chan struct{}, input *fuse.FsyncIn) fuse.Status
	GetAttr(cancel <-chan struct{}, input *fuse.GetAttrIn, out *fuse.AttrOut) fuse.Status
	GetLk(cancel <-chan struct{}, input *fuse.LkIn, out *fuse.LkOut) fuse.Status
	GetXAttr(cancel <-chan struct{}, header *fuse.InHeader, attr string, dest []byte) (uint32, fuse.Status)
	Init(fs.ServerCallbacks)
	Ioctl(cancel <-chan struct{}, input *fuse.IoctlIn, inbuf []byte, output *fuse.IoctlOut, outbuf []byte) fuse.Status
	Link(cancel <-chan struct{}, input *fuse.LinkIn, filename string, out *fuse.EntryOut) fuse.Status
	ListXAttr(cancel <-chan struct{}, header *fuse.InHeader, dest []byte) (uint32, fuse.Status)
	Lookup(cancel <-chan struct{}, header *fuse.InHeader, name string, out *fuse.EntryOut) fuse.Status
	Lseek(cancel <-chan struct{}, in *fuse.LseekIn, out *fuse.LseekOut) fuse.Status
	Mkdir(cancel <-chan struct{}, input *fuse.MkdirIn, name string, out *fuse.EntryOut) fuse.Status
	Mknod(cancel <-chan struct{}, input *fuse.MknodIn, name string, out *fuse.EntryOut) fuse.Status
	OnUnmount()
	Open(cancel <-chan struct{}, input *fuse.OpenIn, out *fuse.OpenOut) fuse.Status
	OpenDir(cancel <-chan struct{}, input *fuse.OpenIn, out *fuse.OpenOut) fuse.Status
	Read(cancel <-chan struct{}, input *fuse.ReadIn, buf []byte) (fuse.ReadResult, fuse.Status)
	ReadDir(cancel <-chan struct{}, input *fuse.ReadIn, out ReadDirEntryList) fuse.Status
	ReadDirPlus(cancel <-chan struct{}, input *fuse.ReadIn, out ReadDirPlusEntryList) fuse.Status
	Readlink(cancel <-chan struct{}, header *fuse.InHeader) ([]byte, fuse.Status)
	Release(cancel <-chan struct{}, input *fuse.ReleaseIn)
	ReleaseDir(input *fuse.ReleaseIn)
	RemoveXAttr(cancel <-chan struct{}, header *fuse.InHeader, attr string) fuse.Status
	Rename(cancel <-chan struct{}, input *fuse.RenameIn, oldName, newName string) fuse.Status
	Rmdir(cancel <-chan struct{}, header *fuse.InHeader, name string) fuse.Status
	SetAttr(cancel <-chan struct{}, input *fuse.SetAttrIn, out *fuse.AttrOut) fuse.Status
	SetDebug(debug bool)
	SetLk(cancel <-chan struct{}, input *fuse.LkIn) fuse.Status
	SetLkw(cancel <-chan struct{}, input *fuse.LkIn) fuse.Status
	SetXAttr(cancel <-chan struct{}, input *fuse.SetXAttrIn, attr string, data []byte) fuse.Status
	StatFs(cancel <-chan struct{}, input *fuse.InHeader, out *fuse.StatfsOut) fuse.Status
	Statx(cancel <-chan struct{}, input *fuse.StatxIn, out *fuse.StatxOut) fuse.Status
	String() string
	Symlink(cancel <-chan struct{}, header *fuse.InHeader, pointedTo, linkName string, out *fuse.EntryOut) fuse.Status
	Unlink(cancel <-chan struct{}, header *fuse.InHeader, name string) fuse.Status
	Write(cancel <-chan struct{}, input *fuse.WriteIn, data []byte) (uint32, fuse.Status)
}

type concreteRawFileSystem struct {
	RawFileSystem
}

// NewConcreteRawFileSystem creates a decorator for RawFileSystem that
// lets methods like Init(), ReadDir(), and ReadDirPlus() accept
// concrete types instead of interfaces. This allows it to be passed to
// go-fuse.
func NewConcreteRawFileSystem(base RawFileSystem) fuse.RawFileSystem {
	return &concreteRawFileSystem{
		RawFileSystem: base,
	}
}

func (rfs *concreteRawFileSystem) Init(server *fuse.Server) {
	rfs.RawFileSystem.Init(server)
}

func (rfs *concreteRawFileSystem) ReadDir(cancel <-chan struct{}, input *fuse.ReadIn, out *fuse.DirEntryList) fuse.Status {
	return rfs.RawFileSystem.ReadDir(cancel, input, out)
}

func (rfs *concreteRawFileSystem) ReadDirPlus(cancel <-chan struct{}, input *fuse.ReadIn, out *fuse.DirEntryList) fuse.Status {
	return rfs.RawFileSystem.ReadDirPlus(cancel, input, out)
}
