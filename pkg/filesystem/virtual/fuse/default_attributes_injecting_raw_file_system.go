//go:build darwin || linux
// +build darwin linux

package fuse

import (
	"time"

	"github.com/hanwen/go-fuse/v2/fuse"
)

type defaultAttributesInjectingRawFileSystem struct {
	fuse.RawFileSystem

	attrOut  fuse.AttrOut
	entryOut fuse.EntryOut
}

// NewDefaultAttributesInjectingRawFileSystem creates a decorator for
// RawFileSystem that places default values into AttrOut and EntryOut
// structures before they are passed on to FUSE operations. This means
// their values are only retained if the underlying implementation
// doesn't fill in values explicitly.
//
// Use cases of this decorator include filling in default
// entry/attribute validity durations, file modification times, file
// ownership, etc.
func NewDefaultAttributesInjectingRawFileSystem(base fuse.RawFileSystem, entryValid, attrValid time.Duration, attr *fuse.Attr) fuse.RawFileSystem {
	entryValidNsec := entryValid.Nanoseconds()
	attrValidNsec := attrValid.Nanoseconds()
	return &defaultAttributesInjectingRawFileSystem{
		RawFileSystem: base,

		attrOut: fuse.AttrOut{
			AttrValid:     uint64(attrValidNsec / 1e9),
			AttrValidNsec: uint32(attrValidNsec % 1e9),
			Attr:          *attr,
		},
		entryOut: fuse.EntryOut{
			EntryValid:     uint64(entryValidNsec / 1e9),
			EntryValidNsec: uint32(entryValidNsec % 1e9),
			AttrValid:      uint64(attrValidNsec / 1e9),
			AttrValidNsec:  uint32(attrValidNsec % 1e9),
			Attr:           *attr,
		},
	}
}

func (rfs *defaultAttributesInjectingRawFileSystem) Lookup(cancel <-chan struct{}, header *fuse.InHeader, name string, out *fuse.EntryOut) fuse.Status {
	*out = rfs.entryOut
	return rfs.RawFileSystem.Lookup(cancel, header, name, out)
}

func (rfs *defaultAttributesInjectingRawFileSystem) GetAttr(cancel <-chan struct{}, input *fuse.GetAttrIn, out *fuse.AttrOut) fuse.Status {
	*out = rfs.attrOut
	return rfs.RawFileSystem.GetAttr(cancel, input, out)
}

func (rfs *defaultAttributesInjectingRawFileSystem) SetAttr(cancel <-chan struct{}, input *fuse.SetAttrIn, out *fuse.AttrOut) fuse.Status {
	*out = rfs.attrOut
	return rfs.RawFileSystem.SetAttr(cancel, input, out)
}

func (rfs *defaultAttributesInjectingRawFileSystem) Mknod(cancel <-chan struct{}, input *fuse.MknodIn, name string, out *fuse.EntryOut) fuse.Status {
	*out = rfs.entryOut
	return rfs.RawFileSystem.Mknod(cancel, input, name, out)
}

func (rfs *defaultAttributesInjectingRawFileSystem) Mkdir(cancel <-chan struct{}, input *fuse.MkdirIn, name string, out *fuse.EntryOut) fuse.Status {
	*out = rfs.entryOut
	return rfs.RawFileSystem.Mkdir(cancel, input, name, out)
}

func (rfs *defaultAttributesInjectingRawFileSystem) Link(cancel <-chan struct{}, input *fuse.LinkIn, filename string, out *fuse.EntryOut) fuse.Status {
	*out = rfs.entryOut
	return rfs.RawFileSystem.Link(cancel, input, filename, out)
}

func (rfs *defaultAttributesInjectingRawFileSystem) Symlink(cancel <-chan struct{}, header *fuse.InHeader, pointedTo, linkName string, out *fuse.EntryOut) fuse.Status {
	*out = rfs.entryOut
	return rfs.RawFileSystem.Symlink(cancel, header, pointedTo, linkName, out)
}

func (rfs *defaultAttributesInjectingRawFileSystem) Create(cancel <-chan struct{}, input *fuse.CreateIn, name string, out *fuse.CreateOut) fuse.Status {
	out.EntryOut = rfs.entryOut
	return rfs.RawFileSystem.Create(cancel, input, name, out)
}

func (rfs *defaultAttributesInjectingRawFileSystem) ReadDirPlus(cancel <-chan struct{}, input *fuse.ReadIn, out fuse.ReadDirPlusEntryList) fuse.Status {
	return rfs.RawFileSystem.ReadDirPlus(cancel, input, &defaultAttributesInjectingReadDirPlusEntryList{
		ReadDirPlusEntryList: out,
		entryOut:             &rfs.entryOut,
	})
}

type defaultAttributesInjectingReadDirPlusEntryList struct {
	fuse.ReadDirPlusEntryList

	entryOut *fuse.EntryOut
}

func (el *defaultAttributesInjectingReadDirPlusEntryList) AddDirLookupEntry(e fuse.DirEntry, off uint64) *fuse.EntryOut {
	if entryOut := el.ReadDirPlusEntryList.AddDirLookupEntry(e, off); entryOut != nil {
		*entryOut = *el.entryOut
		return entryOut
	}
	return nil
}
