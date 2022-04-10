package virtual

import (
	"context"
	"unicode/utf8"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-remote-execution/pkg/proto/outputpathpersistency"
	"github.com/buildbarn/bb-remote-execution/pkg/proto/remoteoutputservice"
	"github.com/buildbarn/bb-storage/pkg/blobstore"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/filesystem"
	"github.com/buildbarn/bb-storage/pkg/filesystem/path"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type symlinkFactory struct{}

func (symlinkFactory) LookupSymlink(target []byte) NativeLeaf {
	return symlink{target: target}
}

// BaseSymlinkFactory can be used to create simple immutable symlink nodes.
var BaseSymlinkFactory SymlinkFactory = symlinkFactory{}

type symlink struct {
	target []byte
}

func (f symlink) Link() Status {
	return StatusOK
}

func (f symlink) Readlink() (string, error) {
	if !utf8.Valid(f.target) {
		return "", status.Error(codes.InvalidArgument, "Symbolic link contents are not valid UTF-8")
	}
	return string(f.target), nil
}

func (f symlink) Unlink() {
}

func (f symlink) UploadFile(ctx context.Context, contentAddressableStorage blobstore.BlobAccess, digestFunction digest.Function) (digest.Digest, error) {
	return digest.BadDigest, status.Error(codes.InvalidArgument, "This file cannot be uploaded, as it is a symbolic link")
}

func (f symlink) GetContainingDigests() digest.Set {
	return digest.EmptySet
}

func (f symlink) GetOutputServiceFileStatus(digestFunction *digest.Function) (*remoteoutputservice.FileStatus, error) {
	target, err := f.Readlink()
	if err != nil {
		return nil, err
	}
	return &remoteoutputservice.FileStatus{
		FileType: &remoteoutputservice.FileStatus_Symlink_{
			Symlink: &remoteoutputservice.FileStatus_Symlink{
				Target: target,
			},
		},
	}, nil
}

func (f symlink) AppendOutputPathPersistencyDirectoryNode(directory *outputpathpersistency.Directory, name path.Component) {
	if target, err := f.Readlink(); err == nil {
		directory.Symlinks = append(directory.Symlinks, &remoteexecution.SymlinkNode{
			Name:   name.String(),
			Target: target,
		})
	}
}

func (f symlink) VirtualAllocate(off, size uint64) Status {
	return StatusErrWrongType
}

func (f symlink) VirtualGetAttributes(requested AttributesMask, attributes *Attributes) {
	attributes.SetChangeID(0)
	attributes.SetFileType(filesystem.FileTypeSymlink)
	attributes.SetPermissions(PermissionsRead | PermissionsWrite | PermissionsExecute)
	attributes.SetSizeBytes(uint64(len(f.target)))
}

func (f symlink) VirtualSeek(offset uint64, regionType filesystem.RegionType) (*uint64, Status) {
	panic("Request to seek on symbolic link should have been intercepted")
}

func (f symlink) VirtualOpenSelf(shareAccess ShareMask, options *OpenExistingOptions, requested AttributesMask, attributes *Attributes) Status {
	return StatusErrSymlink
}

func (f symlink) VirtualRead(buf []byte, offset uint64) (int, bool, Status) {
	panic("Request to read from symbolic link should have been intercepted")
}

func (f symlink) VirtualReadlink() ([]byte, Status) {
	return f.target, StatusOK
}

func (f symlink) VirtualClose() {}

func (f symlink) VirtualSetAttributes(in *Attributes, requested AttributesMask, out *Attributes) Status {
	if _, ok := in.GetSizeBytes(); ok {
		return StatusErrInval
	}
	f.VirtualGetAttributes(requested, out)
	return StatusOK
}

func (f symlink) VirtualWrite(buf []byte, off uint64) (int, Status) {
	panic("Request to write to symbolic link should have been intercepted")
}
