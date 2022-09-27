package virtual

import (
	"context"
	"sync"

	"github.com/buildbarn/bb-remote-execution/pkg/proto/outputpathpersistency"
	"github.com/buildbarn/bb-remote-execution/pkg/proto/remoteoutputservice"
	"github.com/buildbarn/bb-remote-execution/pkg/proto/tmp_installer"
	"github.com/buildbarn/bb-storage/pkg/auth"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/filesystem"
	"github.com/buildbarn/bb-storage/pkg/filesystem/path"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/emptypb"
)

// UserSettableSymlink is an implementation of a symbolic link, whose
// target can be modified using the TemporaryDirectoryInstaller gRPC
// API.
//
// Instead of just pointing to a single target, this type is capable of
// storing one target per user. Both when reading and adjusting the
// symbolic link's target, the public authentication metadata is used to
// identify the user.
type UserSettableSymlink struct {
	placeholderFile

	buildDirectory *path.Builder

	lock     sync.Mutex
	targets  map[string][]byte
	changeID uint64
}

var (
	_ NativeLeaf                                      = (*UserSettableSymlink)(nil)
	_ tmp_installer.TemporaryDirectoryInstallerServer = (*UserSettableSymlink)(nil)
)

// NewUserSettableSymlink creates a UserSettableSymlink that doesn't
// have any targets configured.
func NewUserSettableSymlink(buildDirectory *path.Builder) *UserSettableSymlink {
	return &UserSettableSymlink{
		buildDirectory: buildDirectory,
		targets:        map[string][]byte{},
	}
}

// CheckReadiness returns whether the target of the symbolic link is
// capable of being mutated.
func (f *UserSettableSymlink) CheckReadiness(ctx context.Context, request *emptypb.Empty) (*emptypb.Empty, error) {
	return &emptypb.Empty{}, nil
}

// InstallTemporaryDirectory sets the target of the symbolic link for
// the user stored in the authentication metadata.
func (f *UserSettableSymlink) InstallTemporaryDirectory(ctx context.Context, request *tmp_installer.InstallTemporaryDirectoryRequest) (*emptypb.Empty, error) {
	publicAuthenticationMetadata, _ := auth.AuthenticationMetadataFromContext(ctx).GetPublicProto()
	key := protojson.Format(publicAuthenticationMetadata)

	temporaryDirectory, scopeWalker := f.buildDirectory.Join(path.NewRelativeScopeWalker(path.VoidComponentWalker))
	if err := path.Resolve(request.TemporaryDirectory, scopeWalker); err != nil {
		return nil, err
	}
	target := []byte(temporaryDirectory.String())

	f.lock.Lock()
	f.targets[key] = target
	f.lock.Unlock()
	return &emptypb.Empty{}, nil
}

// Readlink returns the target of the symbolic link. This method always
// fails, as it's called in places where no Context is available.
func (f *UserSettableSymlink) Readlink() (string, error) {
	return "", status.Error(codes.InvalidArgument, "Target of user settable symlinks can only be obtained through the virtual file system")
}

// GetOutputServiceFileStatus returns the status of the symbolic link,
// so that it may be reported through the Remote Output Service. This
// method is a no-op, as this type is not used in combination with the
// Remote Output Service.
func (f *UserSettableSymlink) GetOutputServiceFileStatus(digestFunction *digest.Function) (*remoteoutputservice.FileStatus, error) {
	return &remoteoutputservice.FileStatus{}, nil
}

// AppendOutputPathPersistencyDirectoryNode returns the status of the
// symbolic link, so that it may be persisted on disk. This method is a
// no-op, as this type is not used as part of build output paths.
func (f *UserSettableSymlink) AppendOutputPathPersistencyDirectoryNode(directory *outputpathpersistency.Directory, name path.Component) {
}

// VirtualGetAttributes returns the file system attributes of the
// symbolic link.
func (f *UserSettableSymlink) VirtualGetAttributes(ctx context.Context, requested AttributesMask, attributes *Attributes) {
	attributes.SetFileType(filesystem.FileTypeSymlink)
	attributes.SetPermissions(PermissionsRead | PermissionsWrite | PermissionsExecute)

	if requested&(AttributesMaskChangeID|AttributesMaskSizeBytes) != 0 {
		var key string
		if requested&AttributesMaskSizeBytes != 0 {
			publicAuthenticationMetadata, _ := auth.AuthenticationMetadataFromContext(ctx).GetPublicProto()
			key = protojson.Format(publicAuthenticationMetadata)
		}

		f.lock.Lock()
		if requested&AttributesMaskChangeID != 0 {
			// Clients may use the change ID to determine
			// whether the target of the symbolic link
			// changes. Ensure no caching is performed by
			// incrementing the change ID when requested.
			attributes.SetChangeID(f.changeID)
			f.changeID++
		}
		if requested&AttributesMaskSizeBytes != 0 {
			attributes.SetSizeBytes(uint64(len(f.targets[key])))
		}
		f.lock.Unlock()
	}
}

// VirtualReadlink returns the target of the symbolic link for the
// calling user.
func (f *UserSettableSymlink) VirtualReadlink(ctx context.Context) ([]byte, Status) {
	publicAuthenticationMetadata, _ := auth.AuthenticationMetadataFromContext(ctx).GetPublicProto()
	key := protojson.Format(publicAuthenticationMetadata)

	f.lock.Lock()
	defer f.lock.Unlock()

	if target, ok := f.targets[key]; ok {
		return target, StatusOK
	}
	return nil, StatusErrNoEnt
}

// VirtualSetAttributes adjusts the attributes of the symbolic link.
func (f *UserSettableSymlink) VirtualSetAttributes(ctx context.Context, in *Attributes, requested AttributesMask, out *Attributes) Status {
	if _, ok := in.GetSizeBytes(); ok {
		return StatusErrInval
	}
	f.VirtualGetAttributes(ctx, requested, out)
	return StatusOK
}
