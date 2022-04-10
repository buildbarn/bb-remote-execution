package virtual

import (
	"bytes"
	"encoding/binary"
	"io"
	"math"

	"github.com/buildbarn/bb-storage/pkg/filesystem"
)

// CharacterDeviceFactory is a factory type for character devices.
// Character devices are immutable files; it is not possible to change
// the device after it has been created.
type CharacterDeviceFactory interface {
	LookupCharacterDevice(deviceNumber filesystem.DeviceNumber) NativeLeaf
}

type baseCharacterDeviceFactory struct{}

func (baseCharacterDeviceFactory) LookupCharacterDevice(deviceNumber filesystem.DeviceNumber) NativeLeaf {
	return NewSpecialFile(filesystem.FileTypeCharacterDevice, &deviceNumber)
}

// BaseCharacterDeviceFactory can be used to create simple immutable
// character device nodes.
var BaseCharacterDeviceFactory CharacterDeviceFactory = baseCharacterDeviceFactory{}

type handleAllocatingCharacterDeviceFactory struct {
	base      CharacterDeviceFactory
	allocator ResolvableHandleAllocator
}

// NewHandleAllocatingCharacterDeviceFactory creates a decorator for
// CharacterDeviceFactory that creates character devices that have a
// handle associated with them.
//
// Because device numbers are small, this implementation uses a
// resolvable handle allocator, meaning that the major and minor number
// of the device are stored in the file handle.
func NewHandleAllocatingCharacterDeviceFactory(base CharacterDeviceFactory, allocation ResolvableHandleAllocation) CharacterDeviceFactory {
	cdf := &handleAllocatingCharacterDeviceFactory{
		base: base,
	}
	cdf.allocator = allocation.AsResolvableAllocator(cdf.resolve)
	return cdf
}

func (cdf *handleAllocatingCharacterDeviceFactory) LookupCharacterDevice(deviceNumber filesystem.DeviceNumber) NativeLeaf {
	// Convert the device number to a binary identifier.
	major, minor := deviceNumber.ToMajorMinor()
	var identifier [binary.MaxVarintLen32 * 2]byte
	length := binary.PutUvarint(identifier[:], uint64(major))
	length += binary.PutUvarint(identifier[length:], uint64(minor))

	return cdf.allocator.
		New(bytes.NewBuffer(identifier[:length])).
		AsNativeLeaf(cdf.base.LookupCharacterDevice(deviceNumber))
}

func (cdf *handleAllocatingCharacterDeviceFactory) resolve(r io.ByteReader) (Directory, Leaf, Status) {
	// Convert the binary identifier to a device number.
	major, err := binary.ReadUvarint(r)
	if err != nil || major > math.MaxUint32 {
		return nil, nil, StatusErrBadHandle
	}
	minor, err := binary.ReadUvarint(r)
	if err != nil || minor > math.MaxUint32 {
		return nil, nil, StatusErrBadHandle
	}
	deviceNumber := filesystem.NewDeviceNumberFromMajorMinor(uint32(major), uint32(minor))

	return nil, cdf.LookupCharacterDevice(deviceNumber), StatusOK
}
