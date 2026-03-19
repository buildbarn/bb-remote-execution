package virtual

import (
	"context"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-remote-execution/pkg/proto/bazeloutputservice"
	"github.com/buildbarn/bb-storage/pkg/filesystem"
	"github.com/buildbarn/bb-storage/pkg/filesystem/path"
)

type symlinkFactory struct{}

func (symlinkFactory) LookupSymlink(target path.Parser) (LinkableLeaf, error) {
	return symlink{target: target}, nil
}

// BaseSymlinkFactory can be used to create simple immutable symlink nodes.
var BaseSymlinkFactory SymlinkFactory = symlinkFactory{}

type symlink struct {
	placeholderFile

	target path.Parser
}

func (f symlink) readlinkString() (string, error) {
	targetPath, scopeWalker := path.EmptyBuilder.Join(path.VoidScopeWalker)
	if err := path.Resolve(f.target, scopeWalker); err != nil {
		return "", err
	}
	return targetPath.GetUNIXString(), nil
}

func (f symlink) VirtualGetAttributes(ctx context.Context, requested AttributesMask, attributes *Attributes) {
	attributes.SetChangeID(0)
	attributes.SetFileType(filesystem.FileTypeSymlink)
	attributes.SetHasNamedAttributes(false)
	attributes.SetPermissions(PermissionsRead | PermissionsWrite | PermissionsExecute)
	attributes.SetSymlinkTarget(f.target)
}

func (f symlink) VirtualSetAttributes(ctx context.Context, in *Attributes, requested AttributesMask, out *Attributes) Status {
	if _, ok := in.GetSizeBytes(); ok {
		return StatusErrInval
	}
	f.VirtualGetAttributes(ctx, requested, out)
	return StatusOK
}

func (f symlink) VirtualApply(data any) bool {
	switch p := data.(type) {
	case *ApplyGetBazelOutputServiceStat:
		if target, err := f.readlinkString(); err == nil {
			p.Stat = &bazeloutputservice.BatchStatResponse_Stat{
				Type: &bazeloutputservice.BatchStatResponse_Stat_Symlink_{
					Symlink: &bazeloutputservice.BatchStatResponse_Stat_Symlink{
						Target: target,
					},
				},
			}
		} else {
			p.Err = err
		}
	case *ApplyAppendOutputPathPersistencyDirectoryNode:
		if target, err := f.readlinkString(); err == nil {
			p.Directory.Symlinks = append(p.Directory.Symlinks, &remoteexecution.SymlinkNode{
				Name:   p.Name.String(),
				Target: target,
			})
		}
	default:
		return f.placeholderFile.VirtualApply(data)
	}
	return true
}
