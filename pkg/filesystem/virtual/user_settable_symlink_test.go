package virtual_test

import (
	"context"
	"testing"

	"github.com/buildbarn/bb-remote-execution/pkg/filesystem/virtual"
	"github.com/buildbarn/bb-remote-execution/pkg/proto/tmp_installer"
	"github.com/buildbarn/bb-storage/pkg/auth"
	"github.com/buildbarn/bb-storage/pkg/filesystem"
	"github.com/buildbarn/bb-storage/pkg/filesystem/path"
	auth_pb "github.com/buildbarn/bb-storage/pkg/proto/auth"
	"github.com/buildbarn/bb-storage/pkg/testutil"
	"github.com/stretchr/testify/require"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/structpb"
)

func TestUserSettableSymlink(t *testing.T) {
	buildDirectory, scopeWalker := path.EmptyBuilder.Join(path.VoidScopeWalker)
	require.NoError(t, path.Resolve("/var/build", scopeWalker))
	symlink := virtual.NewUserSettableSymlink(buildDirectory)

	ctx1 := auth.NewContextWithAuthenticationMetadata(
		context.Background(),
		auth.MustNewAuthenticationMetadataFromProto(&auth_pb.AuthenticationMetadata{
			Public: structpb.NewStringValue("user1"),
		}))
	ctx2 := auth.NewContextWithAuthenticationMetadata(
		context.Background(),
		auth.MustNewAuthenticationMetadataFromProto(&auth_pb.AuthenticationMetadata{
			Public: structpb.NewStringValue("user2"),
		}))

	t.Run("InstallTemporaryDirectory", func(t *testing.T) {
		// Attempt to set some symlink target paths for the
		// tests that follow.

		t.Run("InvalidPath", func(t *testing.T) {
			_, err := symlink.InstallTemporaryDirectory(context.Background(), &tmp_installer.InstallTemporaryDirectoryRequest{
				TemporaryDirectory: "/foo",
			})
			testutil.RequireEqualStatus(t, status.Error(codes.InvalidArgument, "Path is absolute, while a relative path was expected"), err)
		})

		t.Run("Success", func(t *testing.T) {
			_, err := symlink.InstallTemporaryDirectory(ctx1, &tmp_installer.InstallTemporaryDirectoryRequest{
				TemporaryDirectory: "125/tmp",
			})
			require.NoError(t, err)

			_, err = symlink.InstallTemporaryDirectory(ctx2, &tmp_installer.InstallTemporaryDirectoryRequest{
				TemporaryDirectory: "4857/tmp",
			})
			require.NoError(t, err)
		})
	})

	t.Run("VirtualReadlink", func(t *testing.T) {
		// The target returned by the symbolic link depends on
		// the authentication metadata that is provided.

		t.Run("UnknownUser", func(t *testing.T) {
			_, s := symlink.VirtualReadlink(context.Background())
			require.Equal(t, virtual.StatusErrNoEnt, s)
		})

		t.Run("Success", func(t *testing.T) {
			target1, s := symlink.VirtualReadlink(ctx1)
			require.Equal(t, virtual.StatusOK, s)
			require.Equal(t, []byte("/var/build/125/tmp"), target1)

			target2, s := symlink.VirtualReadlink(ctx2)
			require.Equal(t, virtual.StatusOK, s)
			require.Equal(t, []byte("/var/build/4857/tmp"), target2)
		})
	})

	t.Run("VirtualGetAttributes", func(t *testing.T) {
		// The size of the symbolic link depends on the user
		// that requests it. As VirtualGetAttributes() can't
		// fail, we return zero in case an unknown user requests
		// it. The change ID should be incremented every time it
		// is requested.

		requestedAttributes := virtual.AttributesMaskChangeID |
			virtual.AttributesMaskFileType |
			virtual.AttributesMaskPermissions |
			virtual.AttributesMaskSizeBytes

		t.Run("UnknownUser", func(t *testing.T) {
			var attributes virtual.Attributes
			symlink.VirtualGetAttributes(context.Background(), requestedAttributes, &attributes)
			require.Equal(t, *(&virtual.Attributes{}).
				SetChangeID(0).
				SetFileType(filesystem.FileTypeSymlink).
				SetPermissions(virtual.PermissionsRead | virtual.PermissionsWrite | virtual.PermissionsExecute).
				SetSizeBytes(0),
				attributes)
		})

		t.Run("Success", func(t *testing.T) {
			var attributes1 virtual.Attributes
			symlink.VirtualGetAttributes(ctx1, requestedAttributes, &attributes1)
			require.Equal(t, *(&virtual.Attributes{}).
				SetChangeID(1).
				SetFileType(filesystem.FileTypeSymlink).
				SetPermissions(virtual.PermissionsRead | virtual.PermissionsWrite | virtual.PermissionsExecute).
				SetSizeBytes(18),
				attributes1)

			var attributes2 virtual.Attributes
			symlink.VirtualGetAttributes(ctx2, requestedAttributes, &attributes2)
			require.Equal(t, *(&virtual.Attributes{}).
				SetChangeID(2).
				SetFileType(filesystem.FileTypeSymlink).
				SetPermissions(virtual.PermissionsRead | virtual.PermissionsWrite | virtual.PermissionsExecute).
				SetSizeBytes(19),
				attributes2)
		})
	})
}
