package nfsv4_test

import (
	"context"
	"testing"

	"github.com/buildbarn/bb-remote-execution/pkg/filesystem/virtual/nfsv4"
	"github.com/buildbarn/bb-storage/pkg/auth"
	"github.com/buildbarn/bb-storage/pkg/eviction"
	auth_pb "github.com/buildbarn/bb-storage/pkg/proto/auth"
	"github.com/buildbarn/bb-storage/pkg/testutil"
	"github.com/buildbarn/go-xdr/pkg/protocols/rpcv2"
	"github.com/jmespath/go-jmespath"
	"github.com/stretchr/testify/require"

	"google.golang.org/protobuf/types/known/structpb"
)

func TestSystemAuthenticator(t *testing.T) {
	ctx := context.Background()

	evictionSet := eviction.NewLRUSet[nfsv4.SystemAuthenticatorCacheKey]()
	authenticator := nfsv4.NewSystemAuthenticator(jmespath.MustCompile("{\"public\": @}"), 10, evictionSet)

	t.Run("InvalidFlavor", func(t *testing.T) {
		// Flavor of requests must be AUTH_SHORT or AUTH_SYS.
		_, _, s := authenticator.Authenticate(
			ctx,
			&rpcv2.OpaqueAuth{Flavor: rpcv2.AUTH_NONE},
			&rpcv2.OpaqueAuth{Flavor: rpcv2.AUTH_NONE})
		require.Equal(t, rpcv2.AUTH_BADCRED, s)
	})

	t.Run("AuthSysInvalidBody", func(t *testing.T) {
		// Body of AUTH_SYS credentials can't be empty.
		_, _, s := authenticator.Authenticate(
			ctx,
			&rpcv2.OpaqueAuth{Flavor: rpcv2.AUTH_SYS},
			&rpcv2.OpaqueAuth{Flavor: rpcv2.AUTH_NONE})
		require.Equal(t, rpcv2.AUTH_BADCRED, s)
	})

	t.Run("AuthSysTrailingGarbage", func(t *testing.T) {
		// AUTH_SYS credentials are malformed, because they have
		// some trailing garbage.
		_, _, s := authenticator.Authenticate(
			ctx,
			&rpcv2.OpaqueAuth{
				Flavor: rpcv2.AUTH_SYS,
				Body: []byte{
					// stamp.
					0x7b, 0xfe, 0x88, 0xfc,
					// machinename.
					0x00, 0x00, 0x00, 0x09,
					0x6c, 0x6f, 0x63, 0x61,
					0x6c, 0x68, 0x6f, 0x73,
					0x74, 0x00, 0x00, 0x00,
					// uid.
					0x00, 0x00, 0x03, 0xe8,
					// gid.
					0x00, 0x00, 0x00, 0x64,
					// gids.
					0x00, 0x00, 0x00, 0x02,
					0x00, 0x00, 0x00, 0x0c,
					0x00, 0x00, 0x00, 0x14,
					// Trailing garbage.
					0xf2, 0x61, 0x9e, 0xca,
				},
			},
			&rpcv2.OpaqueAuth{Flavor: rpcv2.AUTH_NONE})
		require.Equal(t, rpcv2.AUTH_BADCRED, s)
	})

	t.Run("AuthShortInvalidBodyLength", func(t *testing.T) {
		// This implementation requires that AUTH_SHORT
		// credentials have a given length.
		_, _, s := authenticator.Authenticate(
			ctx,
			&rpcv2.OpaqueAuth{
				Flavor: rpcv2.AUTH_SHORT,
				Body:   []byte{1, 2, 3},
			},
			&rpcv2.OpaqueAuth{Flavor: rpcv2.AUTH_NONE})
		require.Equal(t, rpcv2.AUTH_BADCRED, s)
	})

	t.Run("AuthShortUnknownCredentials", func(t *testing.T) {
		// If we receive AUTH_SHORT credentials using a key that
		// is not part of the cache, we should return
		// AUTH_REJECTEDCRED, so that the client can retry the
		// request using the full AUTH_SYS credentials.
		_, _, s := authenticator.Authenticate(
			ctx,
			&rpcv2.OpaqueAuth{
				Flavor: rpcv2.AUTH_SHORT,
				Body: []byte{
					0x7d, 0x23, 0x54, 0xf5, 0x3c, 0xdf, 0x0d, 0x29,
					0x0a, 0xb1, 0x7c, 0xe6, 0xa8, 0xcc, 0xf2, 0x66,
					0x52, 0x30, 0x12, 0xb1, 0x17, 0x8c, 0xe5, 0xe0,
					0xbb, 0xe1, 0xd6, 0x1f, 0x37, 0x42, 0xb2, 0x42,
				},
			},
			&rpcv2.OpaqueAuth{Flavor: rpcv2.AUTH_NONE})
		require.Equal(t, rpcv2.AUTH_REJECTEDCRED, s)
	})

	t.Run("Success", func(t *testing.T) {
		// Credentials are well formed, meaning that they will
		// be attached to the Context object. A short
		// authentication key will be returned, which may be
		// used to refer to the credentials later.
		ctx1, verifier1, s := authenticator.Authenticate(
			ctx,
			&rpcv2.OpaqueAuth{
				Flavor: rpcv2.AUTH_SYS,
				Body: []byte{
					// stamp.
					0x7b, 0xfe, 0x88, 0xfc,
					// machinename.
					0x00, 0x00, 0x00, 0x09,
					0x6c, 0x6f, 0x63, 0x61,
					0x6c, 0x68, 0x6f, 0x73,
					0x74, 0x00, 0x00, 0x00,
					// uid.
					0x00, 0x00, 0x03, 0xe8,
					// gid.
					0x00, 0x00, 0x00, 0x64,
					// gids.
					0x00, 0x00, 0x00, 0x02,
					0x00, 0x00, 0x00, 0x0c,
					0x00, 0x00, 0x00, 0x14,
				},
			},
			&rpcv2.OpaqueAuth{Flavor: rpcv2.AUTH_NONE})
		require.Equal(t, rpcv2.AUTH_OK, s)
		metadata1, _ := auth.AuthenticationMetadataFromContext(ctx1).GetPublicProto()
		testutil.RequireEqualProto(t, &auth_pb.AuthenticationMetadata{
			Public: structpb.NewStructValue(&structpb.Struct{
				Fields: map[string]*structpb.Value{
					"stamp":       structpb.NewNumberValue(0x7bfe88fc),
					"machinename": structpb.NewStringValue("localhost"),
					"uid":         structpb.NewNumberValue(1000),
					"gid":         structpb.NewNumberValue(100),
					"gids": structpb.NewListValue(&structpb.ListValue{
						Values: []*structpb.Value{
							structpb.NewNumberValue(12),
							structpb.NewNumberValue(20),
						},
					}),
				},
			}),
		}, metadata1)
		require.Equal(t, rpcv2.OpaqueAuth{
			Flavor: rpcv2.AUTH_SHORT,
			Body: []byte{
				0x68, 0xe7, 0xce, 0xa7, 0x9b, 0x51, 0x58, 0x83,
				0x16, 0x5f, 0x74, 0xbc, 0x46, 0x44, 0x4e, 0x50,
				0x12, 0x26, 0xc1, 0xb4, 0xa0, 0xd4, 0xbf, 0x51,
				0xb5, 0xc3, 0xdc, 0xe2, 0x4d, 0x7b, 0x07, 0x69,
			},
		}, verifier1)

		// During subsequent requests we may use the short
		// authentication key to refer to the same credentials.
		ctx2, verifier2, s := authenticator.Authenticate(
			ctx,
			&rpcv2.OpaqueAuth{
				Flavor: rpcv2.AUTH_SHORT,
				Body: []byte{
					0x68, 0xe7, 0xce, 0xa7, 0x9b, 0x51, 0x58, 0x83,
					0x16, 0x5f, 0x74, 0xbc, 0x46, 0x44, 0x4e, 0x50,
					0x12, 0x26, 0xc1, 0xb4, 0xa0, 0xd4, 0xbf, 0x51,
					0xb5, 0xc3, 0xdc, 0xe2, 0x4d, 0x7b, 0x07, 0x69,
				},
			},
			&rpcv2.OpaqueAuth{Flavor: rpcv2.AUTH_NONE})
		require.Equal(t, rpcv2.AUTH_OK, s)
		metadata2, _ := auth.AuthenticationMetadataFromContext(ctx2).GetPublicProto()
		testutil.RequireEqualProto(t, &auth_pb.AuthenticationMetadata{
			Public: structpb.NewStructValue(&structpb.Struct{
				Fields: map[string]*structpb.Value{
					"stamp":       structpb.NewNumberValue(0x7bfe88fc),
					"machinename": structpb.NewStringValue("localhost"),
					"uid":         structpb.NewNumberValue(1000),
					"gid":         structpb.NewNumberValue(100),
					"gids": structpb.NewListValue(&structpb.ListValue{
						Values: []*structpb.Value{
							structpb.NewNumberValue(12),
							structpb.NewNumberValue(20),
						},
					}),
				},
			}),
		}, metadata2)
		require.Equal(t, rpcv2.OpaqueAuth{Flavor: rpcv2.AUTH_NONE}, verifier2)
	})
}
