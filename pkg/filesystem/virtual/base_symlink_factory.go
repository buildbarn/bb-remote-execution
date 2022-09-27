package virtual

import (
	"context"
	"unicode/utf8"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-remote-execution/pkg/proto/outputpathpersistency"
	"github.com/buildbarn/bb-remote-execution/pkg/proto/remoteoutputservice"
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
	placeholderFile

	target []byte
}

func (f symlink) Readlink() (string, error) {
	if !utf8.Valid(f.target) {
		return "", status.Error(codes.InvalidArgument, "Symbolic link contents are not valid UTF-8")
	}
	return string(f.target), nil
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

func (f symlink) VirtualGetAttributes(ctx context.Context, requested AttributesMask, attributes *Attributes) {
	attributes.SetChangeID(0)
	attributes.SetFileType(filesystem.FileTypeSymlink)
	attributes.SetPermissions(PermissionsRead | PermissionsWrite | PermissionsExecute)
	attributes.SetSizeBytes(uint64(len(f.target)))
}

func (f symlink) VirtualReadlink(ctx context.Context) ([]byte, Status) {
	return f.target, StatusOK
}

func (f symlink) VirtualSetAttributes(ctx context.Context, in *Attributes, requested AttributesMask, out *Attributes) Status {
	if _, ok := in.GetSizeBytes(); ok {
		return StatusErrInval
	}
	f.VirtualGetAttributes(ctx, requested, out)
	return StatusOK
}
