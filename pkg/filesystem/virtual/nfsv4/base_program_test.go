package nfsv4_test

import (
	"bytes"
	"context"
	"io"
	"testing"
	"time"

	"github.com/buildbarn/bb-remote-execution/internal/mock"
	"github.com/buildbarn/bb-remote-execution/pkg/filesystem/virtual"
	"github.com/buildbarn/bb-remote-execution/pkg/filesystem/virtual/nfsv4"
	"github.com/buildbarn/bb-storage/pkg/filesystem"
	"github.com/buildbarn/bb-storage/pkg/filesystem/path"
	nfsv4_xdr "github.com/buildbarn/go-xdr/pkg/protocols/nfsv4"
	"github.com/buildbarn/go-xdr/pkg/protocols/rpcv2"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
)

func handleResolverExpectCall(t *testing.T, handleResolver *mock.MockHandleResolver, expectedID []byte, child virtual.DirectoryChild, status virtual.Status) {
	handleResolver.EXPECT().Call(gomock.Any()).
		DoAndReturn(func(id io.WriterTo) (virtual.DirectoryChild, virtual.Status) {
			idBuf := bytes.NewBuffer(nil)
			n, err := id.WriteTo(idBuf)
			require.NoError(t, err)
			require.Equal(t, int64(len(expectedID)), n)
			require.Equal(t, expectedID, idBuf.Bytes())
			return child, status
		})
}

func randomNumberGeneratorExpectRead(randomNumberGenerator *mock.MockSingleThreadedGenerator, data []byte) {
	randomNumberGenerator.EXPECT().Read(gomock.Len(len(data))).
		DoAndReturn(func(p []byte) (int, error) {
			return copy(p, data), nil
		})
}

func setClientIDForTesting(ctx context.Context, t *testing.T, randomNumberGenerator *mock.MockSingleThreadedGenerator, program nfsv4_xdr.Nfs4Program, shortClientID nfsv4_xdr.Clientid4) {
	randomNumberGenerator.EXPECT().Uint64().Return(uint64(shortClientID))
	randomNumberGeneratorExpectRead(randomNumberGenerator, []byte{0xf8, 0x6e, 0x57, 0x12, 0x9c, 0x7a, 0x62, 0x8a})

	res, err := program.NfsV4Nfsproc4Compound(ctx, &nfsv4_xdr.Compound4args{
		Tag: "setclientid",
		Argarray: []nfsv4_xdr.NfsArgop4{
			&nfsv4_xdr.NfsArgop4_OP_SETCLIENTID{
				Opsetclientid: nfsv4_xdr.Setclientid4args{
					Client: nfsv4_xdr.NfsClientId4{
						Verifier: nfsv4_xdr.Verifier4{0x95, 0x38, 0xc4, 0xfc, 0x81, 0x3e, 0x92, 0x2a},
						Id:       []byte{0xa6, 0x9d, 0x64, 0x34, 0xdb, 0xcb, 0x09, 0x53},
					},
					Callback: nfsv4_xdr.CbClient4{
						CbProgram: 0x8554a7c7,
						CbLocation: nfsv4_xdr.Clientaddr4{
							RNetid: "tcp",
							RAddr:  "127.0.0.1.196.95",
						},
					},
					CallbackIdent: 0xa2bef9ca,
				},
			},
		},
	})
	require.NoError(t, err)
	require.Equal(t, &nfsv4_xdr.Compound4res{
		Tag: "setclientid",
		Resarray: []nfsv4_xdr.NfsResop4{
			&nfsv4_xdr.NfsResop4_OP_SETCLIENTID{
				Opsetclientid: &nfsv4_xdr.Setclientid4res_NFS4_OK{
					Resok4: nfsv4_xdr.Setclientid4resok{
						Clientid:           shortClientID,
						SetclientidConfirm: nfsv4_xdr.Verifier4{0xf8, 0x6e, 0x57, 0x12, 0x9c, 0x7a, 0x62, 0x8a},
					},
				},
			},
		},
		Status: nfsv4_xdr.NFS4_OK,
	}, res)

	res, err = program.NfsV4Nfsproc4Compound(ctx, &nfsv4_xdr.Compound4args{
		Tag: "setclientid",
		Argarray: []nfsv4_xdr.NfsArgop4{
			&nfsv4_xdr.NfsArgop4_OP_SETCLIENTID_CONFIRM{
				OpsetclientidConfirm: nfsv4_xdr.SetclientidConfirm4args{
					Clientid:           shortClientID,
					SetclientidConfirm: nfsv4_xdr.Verifier4{0xf8, 0x6e, 0x57, 0x12, 0x9c, 0x7a, 0x62, 0x8a},
				},
			},
		},
	})
	require.NoError(t, err)
	require.Equal(t, &nfsv4_xdr.Compound4res{
		Tag: "setclientid",
		Resarray: []nfsv4_xdr.NfsResop4{
			&nfsv4_xdr.NfsResop4_OP_SETCLIENTID_CONFIRM{
				OpsetclientidConfirm: nfsv4_xdr.SetclientidConfirm4res{
					Status: nfsv4_xdr.NFS4_OK,
				},
			},
		},
		Status: nfsv4_xdr.NFS4_OK,
	}, res)
}

func openUnconfirmedFileForTesting(ctx context.Context, t *testing.T, randomNumberGenerator *mock.MockSingleThreadedGenerator, program nfsv4_xdr.Nfs4Program, rootDirectory *mock.MockVirtualDirectory, leaf *mock.MockVirtualLeaf, fileHandle nfsv4_xdr.NfsFh4, shortClientID nfsv4_xdr.Clientid4, seqID nfsv4_xdr.Seqid4, stateIDOther [nfsv4_xdr.NFS4_OTHER_SIZE]byte) {
	rootDirectory.EXPECT().VirtualOpenChild(
		ctx,
		path.MustNewComponent("Hello"),
		virtual.ShareMaskRead,
		nil,
		&virtual.OpenExistingOptions{},
		virtual.AttributesMaskFileHandle,
		gomock.Any(),
	).DoAndReturn(func(ctx context.Context, name path.Component, shareAccess virtual.ShareMask, createAttributes *virtual.Attributes, existingOptions *virtual.OpenExistingOptions, requested virtual.AttributesMask, openedFileAttributes *virtual.Attributes) (virtual.Leaf, virtual.AttributesMask, virtual.ChangeInfo, virtual.Status) {
		openedFileAttributes.SetFileHandle(fileHandle)
		return leaf, 0, virtual.ChangeInfo{
			Before: 0x29291f1b07caf9ea,
			After:  0x360e671892329978,
		}, virtual.StatusOK
	})
	randomNumberGeneratorExpectRead(randomNumberGenerator, stateIDOther[4:])

	res, err := program.NfsV4Nfsproc4Compound(ctx, &nfsv4_xdr.Compound4args{
		Tag: "open",
		Argarray: []nfsv4_xdr.NfsArgop4{
			&nfsv4_xdr.NfsArgop4_OP_PUTROOTFH{},
			&nfsv4_xdr.NfsArgop4_OP_OPEN{
				Opopen: nfsv4_xdr.Open4args{
					Seqid:       seqID,
					ShareAccess: nfsv4_xdr.OPEN4_SHARE_ACCESS_READ,
					ShareDeny:   nfsv4_xdr.OPEN4_SHARE_DENY_NONE,
					Owner: nfsv4_xdr.OpenOwner4{
						Clientid: shortClientID,
						Owner:    []byte{0xc4, 0x85, 0x50, 0x6b, 0xa5, 0xec, 0x8e, 0x2c},
					},
					Openhow: &nfsv4_xdr.Openflag4_default{},
					Claim: &nfsv4_xdr.OpenClaim4_CLAIM_NULL{
						File: "Hello",
					},
				},
			},
			&nfsv4_xdr.NfsArgop4_OP_GETFH{},
		},
	})
	require.NoError(t, err)
	require.Equal(t, &nfsv4_xdr.Compound4res{
		Tag: "open",
		Resarray: []nfsv4_xdr.NfsResop4{
			&nfsv4_xdr.NfsResop4_OP_PUTROOTFH{
				Opputrootfh: nfsv4_xdr.Putrootfh4res{
					Status: nfsv4_xdr.NFS4_OK,
				},
			},
			&nfsv4_xdr.NfsResop4_OP_OPEN{
				Opopen: &nfsv4_xdr.Open4res_NFS4_OK{
					Resok4: nfsv4_xdr.Open4resok{
						Stateid: nfsv4_xdr.Stateid4{
							Seqid: 1,
							Other: stateIDOther,
						},
						Cinfo: nfsv4_xdr.ChangeInfo4{
							Atomic: true,
							Before: 0x29291f1b07caf9ea,
							After:  0x360e671892329978,
						},
						Rflags:     nfsv4_xdr.OPEN4_RESULT_CONFIRM | nfsv4_xdr.OPEN4_RESULT_LOCKTYPE_POSIX,
						Attrset:    nfsv4_xdr.Bitmap4{},
						Delegation: &nfsv4_xdr.OpenDelegation4_OPEN_DELEGATE_NONE{},
					},
				},
			},
			&nfsv4_xdr.NfsResop4_OP_GETFH{
				Opgetfh: &nfsv4_xdr.Getfh4res_NFS4_OK{
					Resok4: nfsv4_xdr.Getfh4resok{
						Object: fileHandle,
					},
				},
			},
		},
		Status: nfsv4_xdr.NFS4_OK,
	}, res)
}

func openConfirmForTesting(ctx context.Context, t *testing.T, randomNumberGenerator *mock.MockSingleThreadedGenerator, program nfsv4_xdr.Nfs4Program, fileHandle nfsv4_xdr.NfsFh4, seqID nfsv4_xdr.Seqid4, stateIDOther [nfsv4_xdr.NFS4_OTHER_SIZE]byte) {
	res, err := program.NfsV4Nfsproc4Compound(ctx, &nfsv4_xdr.Compound4args{
		Tag: "open_confirm",
		Argarray: []nfsv4_xdr.NfsArgop4{
			&nfsv4_xdr.NfsArgop4_OP_PUTFH{
				Opputfh: nfsv4_xdr.Putfh4args{
					Object: fileHandle,
				},
			},
			&nfsv4_xdr.NfsArgop4_OP_OPEN_CONFIRM{
				OpopenConfirm: nfsv4_xdr.OpenConfirm4args{
					OpenStateid: nfsv4_xdr.Stateid4{
						Seqid: 1,
						Other: stateIDOther,
					},
					Seqid: seqID,
				},
			},
		},
	})
	require.NoError(t, err)
	require.Equal(t, &nfsv4_xdr.Compound4res{
		Tag: "open_confirm",
		Resarray: []nfsv4_xdr.NfsResop4{
			&nfsv4_xdr.NfsResop4_OP_PUTFH{
				Opputfh: nfsv4_xdr.Putfh4res{
					Status: nfsv4_xdr.NFS4_OK,
				},
			},
			&nfsv4_xdr.NfsResop4_OP_OPEN_CONFIRM{
				OpopenConfirm: &nfsv4_xdr.OpenConfirm4res_NFS4_OK{
					Resok4: nfsv4_xdr.OpenConfirm4resok{
						OpenStateid: nfsv4_xdr.Stateid4{
							Seqid: 2,
							Other: stateIDOther,
						},
					},
				},
			},
		},
		Status: nfsv4_xdr.NFS4_OK,
	}, res)
}

func TestBaseProgramCompound_OP_ACCESS(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	rootDirectory := mock.NewMockVirtualDirectory(ctrl)
	rootDirectory.EXPECT().VirtualGetAttributes(gomock.Any(), virtual.AttributesMaskFileHandle, gomock.Any()).
		Do(func(ctx context.Context, requested virtual.AttributesMask, attributes *virtual.Attributes) {
			attributes.SetFileHandle([]byte{0x63, 0x40, 0xb6, 0x51, 0x6d, 0xa1, 0x7f, 0xcb})
		})
	handleResolver := mock.NewMockHandleResolver(ctrl)
	randomNumberGenerator := mock.NewMockSingleThreadedGenerator(ctrl)
	rebootVerifier := nfsv4_xdr.Verifier4{0x96, 0x63, 0x54, 0xf1, 0xa2, 0x6b, 0x8c, 0x61}
	stateIDOtherPrefix := [...]byte{0x68, 0x78, 0x20, 0xb7}
	clock := mock.NewMockClock(ctrl)
	program := nfsv4.NewBaseProgram(rootDirectory, handleResolver.Call, randomNumberGenerator, rebootVerifier, stateIDOtherPrefix, clock, 2*time.Minute, time.Minute)

	t.Run("NoFileHandle", func(t *testing.T) {
		// Calling ACCESS without a file handle should fail.
		res, err := program.NfsV4Nfsproc4Compound(ctx, &nfsv4_xdr.Compound4args{
			Tag: "access",
			Argarray: []nfsv4_xdr.NfsArgop4{
				&nfsv4_xdr.NfsArgop4_OP_ACCESS{
					Opaccess: nfsv4_xdr.Access4args{
						Access: nfsv4_xdr.ACCESS4_READ,
					},
				},
			},
		})
		require.NoError(t, err)
		require.Equal(t, &nfsv4_xdr.Compound4res{
			Tag: "access",
			Resarray: []nfsv4_xdr.NfsResop4{
				&nfsv4_xdr.NfsResop4_OP_ACCESS{
					Opaccess: &nfsv4_xdr.Access4res_default{
						Status: nfsv4_xdr.NFS4ERR_NOFILEHANDLE,
					},
				},
			},
			Status: nfsv4_xdr.NFS4ERR_NOFILEHANDLE,
		}, res)
	})

	t.Run("Directory", func(t *testing.T) {
		// Access checks against a directory.
		rootDirectory.EXPECT().VirtualGetAttributes(ctx, virtual.AttributesMaskPermissions, gomock.Any()).
			Do(func(ctx context.Context, requested virtual.AttributesMask, attributes *virtual.Attributes) {
				attributes.SetPermissions(virtual.PermissionsExecute)
			})

		res, err := program.NfsV4Nfsproc4Compound(ctx, &nfsv4_xdr.Compound4args{
			Tag: "access",
			Argarray: []nfsv4_xdr.NfsArgop4{
				&nfsv4_xdr.NfsArgop4_OP_PUTROOTFH{},
				&nfsv4_xdr.NfsArgop4_OP_ACCESS{
					Opaccess: nfsv4_xdr.Access4args{
						Access: nfsv4_xdr.ACCESS4_EXECUTE | nfsv4_xdr.ACCESS4_LOOKUP | nfsv4_xdr.ACCESS4_READ,
					},
				},
			},
		})
		require.NoError(t, err)
		require.Equal(t, &nfsv4_xdr.Compound4res{
			Tag: "access",
			Resarray: []nfsv4_xdr.NfsResop4{
				&nfsv4_xdr.NfsResop4_OP_PUTROOTFH{
					Opputrootfh: nfsv4_xdr.Putrootfh4res{
						Status: nfsv4_xdr.NFS4_OK,
					},
				},
				&nfsv4_xdr.NfsResop4_OP_ACCESS{
					Opaccess: &nfsv4_xdr.Access4res_NFS4_OK{
						Resok4: nfsv4_xdr.Access4resok{
							Supported: nfsv4_xdr.ACCESS4_LOOKUP | nfsv4_xdr.ACCESS4_READ,
							Access:    nfsv4_xdr.ACCESS4_LOOKUP,
						},
					},
				},
			},
			Status: nfsv4_xdr.NFS4_OK,
		}, res)
	})

	t.Run("File", func(t *testing.T) {
		// Access checks against a file.
		leaf := mock.NewMockVirtualLeaf(ctrl)
		clock.EXPECT().Now().Return(time.Unix(1000, 0))
		handleResolverExpectCall(t, handleResolver, []byte{1, 2, 3}, virtual.DirectoryChild{}.FromLeaf(leaf), virtual.StatusOK)
		leaf.EXPECT().VirtualGetAttributes(ctx, virtual.AttributesMaskPermissions, gomock.Any()).
			Do(func(ctx context.Context, requested virtual.AttributesMask, attributes *virtual.Attributes) {
				attributes.SetPermissions(virtual.PermissionsRead | virtual.PermissionsWrite)
			})

		res, err := program.NfsV4Nfsproc4Compound(ctx, &nfsv4_xdr.Compound4args{
			Tag: "access",
			Argarray: []nfsv4_xdr.NfsArgop4{
				&nfsv4_xdr.NfsArgop4_OP_PUTFH{
					Opputfh: nfsv4_xdr.Putfh4args{
						Object: nfsv4_xdr.NfsFh4{1, 2, 3},
					},
				},
				&nfsv4_xdr.NfsArgop4_OP_ACCESS{
					Opaccess: nfsv4_xdr.Access4args{
						Access: nfsv4_xdr.ACCESS4_EXECUTE | nfsv4_xdr.ACCESS4_LOOKUP | nfsv4_xdr.ACCESS4_READ,
					},
				},
			},
		})
		require.NoError(t, err)
		require.Equal(t, &nfsv4_xdr.Compound4res{
			Tag: "access",
			Resarray: []nfsv4_xdr.NfsResop4{
				&nfsv4_xdr.NfsResop4_OP_PUTFH{
					Opputfh: nfsv4_xdr.Putfh4res{
						Status: nfsv4_xdr.NFS4_OK,
					},
				},
				&nfsv4_xdr.NfsResop4_OP_ACCESS{
					Opaccess: &nfsv4_xdr.Access4res_NFS4_OK{
						Resok4: nfsv4_xdr.Access4resok{
							Supported: nfsv4_xdr.ACCESS4_EXECUTE | nfsv4_xdr.ACCESS4_READ,
							Access:    nfsv4_xdr.ACCESS4_READ,
						},
					},
				},
			},
			Status: nfsv4_xdr.NFS4_OK,
		}, res)
	})
}

func TestBaseProgramCompound_OP_CLOSE(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	rootDirectory := mock.NewMockVirtualDirectory(ctrl)
	rootDirectory.EXPECT().VirtualGetAttributes(gomock.Any(), virtual.AttributesMaskFileHandle, gomock.Any()).
		Do(func(ctx context.Context, requested virtual.AttributesMask, attributes *virtual.Attributes) {
			attributes.SetFileHandle([]byte{0x14, 0x55, 0xb5, 0x51, 0x02, 0x31, 0xd6, 0x75})
		})
	handleResolver := mock.NewMockHandleResolver(ctrl)
	randomNumberGenerator := mock.NewMockSingleThreadedGenerator(ctrl)
	rebootVerifier := nfsv4_xdr.Verifier4{0x9f, 0xa8, 0x23, 0x40, 0x68, 0x9f, 0x3e, 0xac}
	stateIDOtherPrefix := [...]byte{0xf5, 0x47, 0xa8, 0x88}
	clock := mock.NewMockClock(ctrl)
	program := nfsv4.NewBaseProgram(rootDirectory, handleResolver.Call, randomNumberGenerator, rebootVerifier, stateIDOtherPrefix, clock, 2*time.Minute, time.Minute)

	t.Run("AnonymousStateID", func(t *testing.T) {
		// Calling CLOSE against the anonymous state ID is of
		// course not permitted. This operation only works when
		// called against regular state IDs.
		leaf := mock.NewMockVirtualLeaf(ctrl)
		clock.EXPECT().Now().Return(time.Unix(1000, 0))
		handleResolverExpectCall(t, handleResolver, []byte{1, 2, 3}, virtual.DirectoryChild{}.FromLeaf(leaf), virtual.StatusOK)

		res, err := program.NfsV4Nfsproc4Compound(ctx, &nfsv4_xdr.Compound4args{
			Tag: "close",
			Argarray: []nfsv4_xdr.NfsArgop4{
				&nfsv4_xdr.NfsArgop4_OP_PUTFH{
					Opputfh: nfsv4_xdr.Putfh4args{
						Object: nfsv4_xdr.NfsFh4{1, 2, 3},
					},
				},
				&nfsv4_xdr.NfsArgop4_OP_CLOSE{
					Opclose: nfsv4_xdr.Close4args{
						Seqid: 0x33cfa3a9,
					},
				},
			},
		})
		require.NoError(t, err)
		require.Equal(t, &nfsv4_xdr.Compound4res{
			Tag: "close",
			Resarray: []nfsv4_xdr.NfsResop4{
				&nfsv4_xdr.NfsResop4_OP_PUTFH{
					Opputfh: nfsv4_xdr.Putfh4res{
						Status: nfsv4_xdr.NFS4_OK,
					},
				},
				&nfsv4_xdr.NfsResop4_OP_CLOSE{
					Opclose: &nfsv4_xdr.Close4res_default{
						Status: nfsv4_xdr.NFS4ERR_BAD_STATEID,
					},
				},
			},
			Status: nfsv4_xdr.NFS4ERR_BAD_STATEID,
		}, res)
	})

	t.Run("StaleStateID", func(t *testing.T) {
		// Providing an arbitrary state ID that does not start
		// with a known prefix should return
		// NFS4ERR_STALE_STATEID, as it's likely from before a
		// restart.
		leaf := mock.NewMockVirtualLeaf(ctrl)
		clock.EXPECT().Now().Return(time.Unix(1001, 0))
		handleResolverExpectCall(t, handleResolver, []byte{1, 2, 3}, virtual.DirectoryChild{}.FromLeaf(leaf), virtual.StatusOK)

		res, err := program.NfsV4Nfsproc4Compound(ctx, &nfsv4_xdr.Compound4args{
			Tag: "close",
			Argarray: []nfsv4_xdr.NfsArgop4{
				&nfsv4_xdr.NfsArgop4_OP_PUTFH{
					Opputfh: nfsv4_xdr.Putfh4args{
						Object: nfsv4_xdr.NfsFh4{1, 2, 3},
					},
				},
				&nfsv4_xdr.NfsArgop4_OP_CLOSE{
					Opclose: nfsv4_xdr.Close4args{
						Seqid: 0x299f061e,
						OpenStateid: nfsv4_xdr.Stateid4{
							Seqid: 0x7746b4d2,
							Other: [...]byte{
								0x36, 0xeb, 0x77, 0x13,
								0x42, 0xfa, 0x7f, 0xbc,
								0xe2, 0x36, 0x20, 0x1b,
							},
						},
					},
				},
			},
		})
		require.NoError(t, err)
		require.Equal(t, &nfsv4_xdr.Compound4res{
			Tag: "close",
			Resarray: []nfsv4_xdr.NfsResop4{
				&nfsv4_xdr.NfsResop4_OP_PUTFH{
					Opputfh: nfsv4_xdr.Putfh4res{
						Status: nfsv4_xdr.NFS4_OK,
					},
				},
				&nfsv4_xdr.NfsResop4_OP_CLOSE{
					Opclose: &nfsv4_xdr.Close4res_default{
						Status: nfsv4_xdr.NFS4ERR_STALE_STATEID,
					},
				},
			},
			Status: nfsv4_xdr.NFS4ERR_STALE_STATEID,
		}, res)
	})

	t.Run("BadStateID", func(t *testing.T) {
		// Providing an arbitrary state ID that does not start with
		// the known prefix should return NFS4ERR_BAD_STATEID.
		leaf := mock.NewMockVirtualLeaf(ctrl)
		clock.EXPECT().Now().Return(time.Unix(1002, 0))
		clock.EXPECT().Now().Return(time.Unix(1003, 0))
		handleResolverExpectCall(t, handleResolver, []byte{1, 2, 3}, virtual.DirectoryChild{}.FromLeaf(leaf), virtual.StatusOK)

		res, err := program.NfsV4Nfsproc4Compound(ctx, &nfsv4_xdr.Compound4args{
			Tag: "close",
			Argarray: []nfsv4_xdr.NfsArgop4{
				&nfsv4_xdr.NfsArgop4_OP_PUTFH{
					Opputfh: nfsv4_xdr.Putfh4args{
						Object: nfsv4_xdr.NfsFh4{1, 2, 3},
					},
				},
				&nfsv4_xdr.NfsArgop4_OP_CLOSE{
					Opclose: nfsv4_xdr.Close4args{
						Seqid: 0xf4cf976e,
						OpenStateid: nfsv4_xdr.Stateid4{
							Seqid: 0x444b408c,
							Other: [...]byte{
								0xf5, 0x47, 0xa8, 0x88,
								0x91, 0x2a, 0x94, 0x35,
								0x7f, 0xc9, 0x06, 0x70,
							},
						},
					},
				},
			},
		})
		require.NoError(t, err)
		require.Equal(t, &nfsv4_xdr.Compound4res{
			Tag: "close",
			Resarray: []nfsv4_xdr.NfsResop4{
				&nfsv4_xdr.NfsResop4_OP_PUTFH{
					Opputfh: nfsv4_xdr.Putfh4res{
						Status: nfsv4_xdr.NFS4_OK,
					},
				},
				&nfsv4_xdr.NfsResop4_OP_CLOSE{
					Opclose: &nfsv4_xdr.Close4res_default{
						Status: nfsv4_xdr.NFS4ERR_BAD_STATEID,
					},
				},
			},
			Status: nfsv4_xdr.NFS4ERR_BAD_STATEID,
		}, res)
	})

	// The remainder of the test assumes the availability of a client ID.
	clock.EXPECT().Now().Return(time.Unix(1004, 0))
	clock.EXPECT().Now().Return(time.Unix(1005, 0))
	setClientIDForTesting(ctx, t, randomNumberGenerator, program, 0xc4cf32ab1168aabc)

	// Open a file for reading, but don't confirm it yet.
	leaf := mock.NewMockVirtualLeaf(ctrl)
	clock.EXPECT().Now().Return(time.Unix(1006, 0))
	clock.EXPECT().Now().Return(time.Unix(1007, 0))
	openUnconfirmedFileForTesting(
		ctx,
		t,
		randomNumberGenerator,
		program,
		rootDirectory,
		leaf,
		nfsv4_xdr.NfsFh4{0x1f, 0x5b, 0x1f, 0x0e, 0x8c, 0xf4, 0xf5, 0x40},
		/* shortClientID = */ 0xc4cf32ab1168aabc,
		/* seqID = */ 241,
		/* stateIDOther = */ [...]byte{
			0xf5, 0x47, 0xa8, 0x88,
			0x74, 0x62, 0xab, 0x46,
			0x26, 0x1d, 0x14, 0x7f,
		})

	t.Run("UnconfirmedStateID", func(t *testing.T) {
		// CLOSE can't be called against an open-owner that
		// hasn't been confirmed yet.
		clock.EXPECT().Now().Return(time.Unix(1008, 0))
		clock.EXPECT().Now().Return(time.Unix(1009, 0))

		res, err := program.NfsV4Nfsproc4Compound(ctx, &nfsv4_xdr.Compound4args{
			Tag: "close",
			Argarray: []nfsv4_xdr.NfsArgop4{
				&nfsv4_xdr.NfsArgop4_OP_PUTFH{
					Opputfh: nfsv4_xdr.Putfh4args{
						Object: nfsv4_xdr.NfsFh4{0x1f, 0x5b, 0x1f, 0x0e, 0x8c, 0xf4, 0xf5, 0x40},
					},
				},
				&nfsv4_xdr.NfsArgop4_OP_CLOSE{
					Opclose: nfsv4_xdr.Close4args{
						Seqid: 242,
						OpenStateid: nfsv4_xdr.Stateid4{
							Seqid: 1,
							Other: [...]byte{
								0xf5, 0x47, 0xa8, 0x88,
								0x74, 0x62, 0xab, 0x46,
								0x26, 0x1d, 0x14, 0x7f,
							},
						},
					},
				},
			},
		})
		require.NoError(t, err)
		require.Equal(t, &nfsv4_xdr.Compound4res{
			Tag: "close",
			Resarray: []nfsv4_xdr.NfsResop4{
				&nfsv4_xdr.NfsResop4_OP_PUTFH{
					Opputfh: nfsv4_xdr.Putfh4res{
						Status: nfsv4_xdr.NFS4_OK,
					},
				},
				&nfsv4_xdr.NfsResop4_OP_CLOSE{
					Opclose: &nfsv4_xdr.Close4res_default{
						Status: nfsv4_xdr.NFS4ERR_BAD_SEQID,
					},
				},
			},
			Status: nfsv4_xdr.NFS4ERR_BAD_SEQID,
		}, res)
	})

	// Confirm the open-owner for the remainder of the test.
	clock.EXPECT().Now().Return(time.Unix(1010, 0))
	clock.EXPECT().Now().Return(time.Unix(1011, 0))
	openConfirmForTesting(
		ctx,
		t,
		randomNumberGenerator,
		program,
		nfsv4_xdr.NfsFh4{0x1f, 0x5b, 0x1f, 0x0e, 0x8c, 0xf4, 0xf5, 0x40},
		/* seqID = */ 242,
		/* stateIDOther = */ [...]byte{
			0xf5, 0x47, 0xa8, 0x88,
			0x74, 0x62, 0xab, 0x46,
			0x26, 0x1d, 0x14, 0x7f,
		})

	t.Run("OldStateID", func(t *testing.T) {
		// Can't call CLOSE on a state ID from the past.
		clock.EXPECT().Now().Return(time.Unix(1012, 0))
		clock.EXPECT().Now().Return(time.Unix(1013, 0))

		res, err := program.NfsV4Nfsproc4Compound(ctx, &nfsv4_xdr.Compound4args{
			Tag: "close",
			Argarray: []nfsv4_xdr.NfsArgop4{
				&nfsv4_xdr.NfsArgop4_OP_PUTFH{
					Opputfh: nfsv4_xdr.Putfh4args{
						Object: nfsv4_xdr.NfsFh4{0x1f, 0x5b, 0x1f, 0x0e, 0x8c, 0xf4, 0xf5, 0x40},
					},
				},
				&nfsv4_xdr.NfsArgop4_OP_CLOSE{
					Opclose: nfsv4_xdr.Close4args{
						Seqid: 243,
						OpenStateid: nfsv4_xdr.Stateid4{
							Seqid: 1,
							Other: [...]byte{
								0xf5, 0x47, 0xa8, 0x88,
								0x74, 0x62, 0xab, 0x46,
								0x26, 0x1d, 0x14, 0x7f,
							},
						},
					},
				},
			},
		})
		require.NoError(t, err)
		require.Equal(t, &nfsv4_xdr.Compound4res{
			Tag: "close",
			Resarray: []nfsv4_xdr.NfsResop4{
				&nfsv4_xdr.NfsResop4_OP_PUTFH{
					Opputfh: nfsv4_xdr.Putfh4res{
						Status: nfsv4_xdr.NFS4_OK,
					},
				},
				&nfsv4_xdr.NfsResop4_OP_CLOSE{
					Opclose: &nfsv4_xdr.Close4res_default{
						Status: nfsv4_xdr.NFS4ERR_OLD_STATEID,
					},
				},
			},
			Status: nfsv4_xdr.NFS4ERR_OLD_STATEID,
		}, res)
	})

	t.Run("FuturisticStateID", func(t *testing.T) {
		// Can't call CLOSE on a state ID from the future.
		clock.EXPECT().Now().Return(time.Unix(1014, 0))
		clock.EXPECT().Now().Return(time.Unix(1015, 0))

		res, err := program.NfsV4Nfsproc4Compound(ctx, &nfsv4_xdr.Compound4args{
			Tag: "close",
			Argarray: []nfsv4_xdr.NfsArgop4{
				&nfsv4_xdr.NfsArgop4_OP_PUTFH{
					Opputfh: nfsv4_xdr.Putfh4args{
						Object: nfsv4_xdr.NfsFh4{0x1f, 0x5b, 0x1f, 0x0e, 0x8c, 0xf4, 0xf5, 0x40},
					},
				},
				&nfsv4_xdr.NfsArgop4_OP_CLOSE{
					Opclose: nfsv4_xdr.Close4args{
						Seqid: 244,
						OpenStateid: nfsv4_xdr.Stateid4{
							Seqid: 3,
							Other: [...]byte{
								0xf5, 0x47, 0xa8, 0x88,
								0x74, 0x62, 0xab, 0x46,
								0x26, 0x1d, 0x14, 0x7f,
							},
						},
					},
				},
			},
		})
		require.NoError(t, err)
		require.Equal(t, &nfsv4_xdr.Compound4res{
			Tag: "close",
			Resarray: []nfsv4_xdr.NfsResop4{
				&nfsv4_xdr.NfsResop4_OP_PUTFH{
					Opputfh: nfsv4_xdr.Putfh4res{
						Status: nfsv4_xdr.NFS4_OK,
					},
				},
				&nfsv4_xdr.NfsResop4_OP_CLOSE{
					Opclose: &nfsv4_xdr.Close4res_default{
						Status: nfsv4_xdr.NFS4ERR_BAD_STATEID,
					},
				},
			},
			Status: nfsv4_xdr.NFS4ERR_BAD_STATEID,
		}, res)
	})

	t.Run("NoFileHandle", func(t *testing.T) {
		// Calling CLOSE without a file handle should fail.
		clock.EXPECT().Now().Return(time.Unix(1016, 0))

		res, err := program.NfsV4Nfsproc4Compound(ctx, &nfsv4_xdr.Compound4args{
			Tag: "close",
			Argarray: []nfsv4_xdr.NfsArgop4{
				&nfsv4_xdr.NfsArgop4_OP_CLOSE{
					Opclose: nfsv4_xdr.Close4args{
						Seqid: 244,
						OpenStateid: nfsv4_xdr.Stateid4{
							Seqid: 2,
							Other: [...]byte{
								0xf5, 0x47, 0xa8, 0x88,
								0x74, 0x62, 0xab, 0x46,
								0x26, 0x1d, 0x14, 0x7f,
							},
						},
					},
				},
			},
		})
		require.NoError(t, err)
		require.Equal(t, &nfsv4_xdr.Compound4res{
			Tag: "close",
			Resarray: []nfsv4_xdr.NfsResop4{
				&nfsv4_xdr.NfsResop4_OP_CLOSE{
					Opclose: &nfsv4_xdr.Close4res_default{
						Status: nfsv4_xdr.NFS4ERR_NOFILEHANDLE,
					},
				},
			},
			Status: nfsv4_xdr.NFS4ERR_NOFILEHANDLE,
		}, res)
	})

	t.Run("Success", func(t *testing.T) {
		// Actually close the file. It should be safe to call
		// this multiple times, as it should just return a
		// cached response.
		for i := int64(0); i < 2*10; i++ {
			clock.EXPECT().Now().Return(time.Unix(1017+i, 0))
		}
		leaf.EXPECT().VirtualClose()

		for i := uint32(0); i < 10; i++ {
			res, err := program.NfsV4Nfsproc4Compound(ctx, &nfsv4_xdr.Compound4args{
				Tag: "close",
				Argarray: []nfsv4_xdr.NfsArgop4{
					&nfsv4_xdr.NfsArgop4_OP_PUTFH{
						Opputfh: nfsv4_xdr.Putfh4args{
							Object: nfsv4_xdr.NfsFh4{0x1f, 0x5b, 0x1f, 0x0e, 0x8c, 0xf4, 0xf5, 0x40},
						},
					},
					&nfsv4_xdr.NfsArgop4_OP_CLOSE{
						Opclose: nfsv4_xdr.Close4args{
							Seqid: 244,
							OpenStateid: nfsv4_xdr.Stateid4{
								Seqid: 2,
								Other: [...]byte{
									0xf5, 0x47, 0xa8, 0x88,
									0x74, 0x62, 0xab, 0x46,
									0x26, 0x1d, 0x14, 0x7f,
								},
							},
						},
					},
				},
			})
			require.NoError(t, err)
			require.Equal(t, &nfsv4_xdr.Compound4res{
				Tag: "close",
				Resarray: []nfsv4_xdr.NfsResop4{
					&nfsv4_xdr.NfsResop4_OP_PUTFH{
						Opputfh: nfsv4_xdr.Putfh4res{
							Status: nfsv4_xdr.NFS4_OK,
						},
					},
					&nfsv4_xdr.NfsResop4_OP_CLOSE{
						Opclose: &nfsv4_xdr.Close4res_NFS4_OK{
							OpenStateid: nfsv4_xdr.Stateid4{
								Seqid: 3,
								Other: [...]byte{
									0xf5, 0x47, 0xa8, 0x88,
									0x74, 0x62, 0xab, 0x46,
									0x26, 0x1d, 0x14, 0x7f,
								},
							},
						},
					},
				},
				Status: nfsv4_xdr.NFS4_OK,
			}, res)
		}
	})

	t.Run("RetransmissionWithMismatchingStateID", func(t *testing.T) {
		// At a minimum, the standard states that when returning
		// a cached response it is sufficient to compare the
		// original operation type and sequence ID. Let's be a
		// bit more strict and actually check whether the
		// provided state ID matches the one that was provided
		// as part of the original request.
		//
		// More details: RFC 7530, section 9.1.9, bullet point 3.
		clock.EXPECT().Now().Return(time.Unix(1037, 0))
		clock.EXPECT().Now().Return(time.Unix(1038, 0))

		res, err := program.NfsV4Nfsproc4Compound(ctx, &nfsv4_xdr.Compound4args{
			Tag: "close",
			Argarray: []nfsv4_xdr.NfsArgop4{
				&nfsv4_xdr.NfsArgop4_OP_PUTFH{
					Opputfh: nfsv4_xdr.Putfh4args{
						Object: nfsv4_xdr.NfsFh4{0x1f, 0x5b, 0x1f, 0x0e, 0x8c, 0xf4, 0xf5, 0x40},
					},
				},
				&nfsv4_xdr.NfsArgop4_OP_CLOSE{
					Opclose: nfsv4_xdr.Close4args{
						Seqid: 244,
						OpenStateid: nfsv4_xdr.Stateid4{
							Seqid: 3,
							Other: [...]byte{
								0xf5, 0x47, 0xa8, 0x88,
								0x74, 0x62, 0xab, 0x46,
								0x26, 0x1d, 0x14, 0x7f,
							},
						},
					},
				},
			},
		})
		require.NoError(t, err)
		require.Equal(t, &nfsv4_xdr.Compound4res{
			Tag: "close",
			Resarray: []nfsv4_xdr.NfsResop4{
				&nfsv4_xdr.NfsResop4_OP_PUTFH{
					Opputfh: nfsv4_xdr.Putfh4res{
						Status: nfsv4_xdr.NFS4_OK,
					},
				},
				&nfsv4_xdr.NfsResop4_OP_CLOSE{
					Opclose: &nfsv4_xdr.Close4res_default{
						Status: nfsv4_xdr.NFS4ERR_BAD_SEQID,
					},
				},
			},
			Status: nfsv4_xdr.NFS4ERR_BAD_SEQID,
		}, res)
	})

	t.Run("CloseAfterClosed", func(t *testing.T) {
		// We should no longer be able to interact with the
		// state ID after closing it. Attempting to close a file
		// that has already been closed should just return
		// NFS4ERR_BAD_STATEID.
		clock.EXPECT().Now().Return(time.Unix(1039, 0))
		clock.EXPECT().Now().Return(time.Unix(1040, 0))

		res, err := program.NfsV4Nfsproc4Compound(ctx, &nfsv4_xdr.Compound4args{
			Tag: "close",
			Argarray: []nfsv4_xdr.NfsArgop4{
				&nfsv4_xdr.NfsArgop4_OP_PUTFH{
					Opputfh: nfsv4_xdr.Putfh4args{
						Object: nfsv4_xdr.NfsFh4{0x1f, 0x5b, 0x1f, 0x0e, 0x8c, 0xf4, 0xf5, 0x40},
					},
				},
				&nfsv4_xdr.NfsArgop4_OP_CLOSE{
					Opclose: nfsv4_xdr.Close4args{
						Seqid: 245,
						OpenStateid: nfsv4_xdr.Stateid4{
							Seqid: 3,
							Other: [...]byte{
								0xf5, 0x47, 0xa8, 0x88,
								0x74, 0x62, 0xab, 0x46,
								0x26, 0x1d, 0x14, 0x7f,
							},
						},
					},
				},
			},
		})
		require.NoError(t, err)
		require.Equal(t, &nfsv4_xdr.Compound4res{
			Tag: "close",
			Resarray: []nfsv4_xdr.NfsResop4{
				&nfsv4_xdr.NfsResop4_OP_PUTFH{
					Opputfh: nfsv4_xdr.Putfh4res{
						Status: nfsv4_xdr.NFS4_OK,
					},
				},
				&nfsv4_xdr.NfsResop4_OP_CLOSE{
					Opclose: &nfsv4_xdr.Close4res_default{
						Status: nfsv4_xdr.NFS4ERR_BAD_STATEID,
					},
				},
			},
			Status: nfsv4_xdr.NFS4ERR_BAD_STATEID,
		}, res)
	})
}

func TestBaseProgramCompound_OP_COMMIT(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	rootDirectory := mock.NewMockVirtualDirectory(ctrl)
	rootDirectory.EXPECT().VirtualGetAttributes(gomock.Any(), virtual.AttributesMaskFileHandle, gomock.Any()).
		Do(func(ctx context.Context, requested virtual.AttributesMask, attributes *virtual.Attributes) {
			attributes.SetFileHandle([]byte{0x5e, 0x1e, 0xca, 0x70, 0xcc, 0x9d, 0x5e, 0xd5})
		})
	handleResolver := mock.NewMockHandleResolver(ctrl)
	randomNumberGenerator := mock.NewMockSingleThreadedGenerator(ctrl)
	rebootVerifier := nfsv4_xdr.Verifier4{0x1a, 0xa6, 0x7e, 0x3b, 0xf7, 0x29, 0xa4, 0x7b}
	stateIDOtherPrefix := [...]byte{0x24, 0xa7, 0x48, 0xbc}
	clock := mock.NewMockClock(ctrl)
	program := nfsv4.NewBaseProgram(rootDirectory, handleResolver.Call, randomNumberGenerator, rebootVerifier, stateIDOtherPrefix, clock, 2*time.Minute, time.Minute)

	t.Run("NoFileHandle", func(t *testing.T) {
		// Calling COMMIT without a file handle should fail.
		res, err := program.NfsV4Nfsproc4Compound(ctx, &nfsv4_xdr.Compound4args{
			Tag: "fsync",
			Argarray: []nfsv4_xdr.NfsArgop4{
				&nfsv4_xdr.NfsArgop4_OP_COMMIT{
					Opcommit: nfsv4_xdr.Commit4args{
						Offset: 10,
						Count:  20,
					},
				},
			},
		})
		require.NoError(t, err)
		require.Equal(t, &nfsv4_xdr.Compound4res{
			Tag: "fsync",
			Resarray: []nfsv4_xdr.NfsResop4{
				&nfsv4_xdr.NfsResop4_OP_COMMIT{
					Opcommit: &nfsv4_xdr.Commit4res_default{
						Status: nfsv4_xdr.NFS4ERR_NOFILEHANDLE,
					},
				},
			},
			Status: nfsv4_xdr.NFS4ERR_NOFILEHANDLE,
		}, res)
	})

	t.Run("NotFile", func(t *testing.T) {
		res, err := program.NfsV4Nfsproc4Compound(ctx, &nfsv4_xdr.Compound4args{
			Tag: "fsync",
			Argarray: []nfsv4_xdr.NfsArgop4{
				&nfsv4_xdr.NfsArgop4_OP_PUTROOTFH{},
				&nfsv4_xdr.NfsArgop4_OP_COMMIT{
					Opcommit: nfsv4_xdr.Commit4args{
						Offset: 10,
						Count:  20,
					},
				},
			},
		})
		require.NoError(t, err)
		require.Equal(t, &nfsv4_xdr.Compound4res{
			Tag: "fsync",
			Resarray: []nfsv4_xdr.NfsResop4{
				&nfsv4_xdr.NfsResop4_OP_PUTROOTFH{
					Opputrootfh: nfsv4_xdr.Putrootfh4res{
						Status: nfsv4_xdr.NFS4_OK,
					},
				},
				&nfsv4_xdr.NfsResop4_OP_COMMIT{
					Opcommit: &nfsv4_xdr.Commit4res_default{
						Status: nfsv4_xdr.NFS4ERR_ISDIR,
					},
				},
			},
			Status: nfsv4_xdr.NFS4ERR_ISDIR,
		}, res)
	})

	t.Run("Success", func(t *testing.T) {
		leaf := mock.NewMockVirtualLeaf(ctrl)
		clock.EXPECT().Now().Return(time.Unix(1000, 0))
		handleResolverExpectCall(t, handleResolver, []byte{1, 2, 3}, virtual.DirectoryChild{}.FromLeaf(leaf), virtual.StatusOK)

		res, err := program.NfsV4Nfsproc4Compound(ctx, &nfsv4_xdr.Compound4args{
			Tag: "fsync",
			Argarray: []nfsv4_xdr.NfsArgop4{
				&nfsv4_xdr.NfsArgop4_OP_PUTFH{
					Opputfh: nfsv4_xdr.Putfh4args{
						Object: nfsv4_xdr.NfsFh4{1, 2, 3},
					},
				},
				&nfsv4_xdr.NfsArgop4_OP_COMMIT{
					Opcommit: nfsv4_xdr.Commit4args{
						Offset: 10,
						Count:  20,
					},
				},
			},
		})
		require.NoError(t, err)
		require.Equal(t, &nfsv4_xdr.Compound4res{
			Tag: "fsync",
			Resarray: []nfsv4_xdr.NfsResop4{
				&nfsv4_xdr.NfsResop4_OP_PUTFH{
					Opputfh: nfsv4_xdr.Putfh4res{
						Status: nfsv4_xdr.NFS4_OK,
					},
				},
				&nfsv4_xdr.NfsResop4_OP_COMMIT{
					Opcommit: &nfsv4_xdr.Commit4res_NFS4_OK{
						Resok4: nfsv4_xdr.Commit4resok{
							Writeverf: rebootVerifier,
						},
					},
				},
			},
			Status: nfsv4_xdr.NFS4_OK,
		}, res)
	})
}

func TestBaseProgramCompound_OP_CREATE(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	rootDirectory := mock.NewMockVirtualDirectory(ctrl)
	rootDirectory.EXPECT().VirtualGetAttributes(gomock.Any(), virtual.AttributesMaskFileHandle, gomock.Any()).
		Do(func(ctx context.Context, requested virtual.AttributesMask, attributes *virtual.Attributes) {
			attributes.SetFileHandle([]byte{0x9b, 0xe9, 0x83, 0x67, 0x8d, 0x92, 0x5e, 0x62})
		})
	handleResolver := mock.NewMockHandleResolver(ctrl)
	randomNumberGenerator := mock.NewMockSingleThreadedGenerator(ctrl)
	rebootVerifier := nfsv4_xdr.Verifier4{0x8d, 0x3d, 0xe8, 0x2e, 0xee, 0x3b, 0xca, 0x60}
	stateIDOtherPrefix := [...]byte{0x60, 0xf5, 0x56, 0x97}
	clock := mock.NewMockClock(ctrl)
	program := nfsv4.NewBaseProgram(rootDirectory, handleResolver.Call, randomNumberGenerator, rebootVerifier, stateIDOtherPrefix, clock, 2*time.Minute, time.Minute)

	t.Run("NoFileHandle", func(t *testing.T) {
		// Calling CREATE without a file handle should fail.
		res, err := program.NfsV4Nfsproc4Compound(ctx, &nfsv4_xdr.Compound4args{
			Tag: "create",
			Argarray: []nfsv4_xdr.NfsArgop4{
				&nfsv4_xdr.NfsArgop4_OP_CREATE{
					Opcreate: nfsv4_xdr.Create4args{
						Objtype: &nfsv4_xdr.Createtype4_NF4DIR{},
						Objname: "Hello",
					},
				},
			},
		})
		require.NoError(t, err)
		require.Equal(t, &nfsv4_xdr.Compound4res{
			Tag: "create",
			Resarray: []nfsv4_xdr.NfsResop4{
				&nfsv4_xdr.NfsResop4_OP_CREATE{
					Opcreate: &nfsv4_xdr.Create4res_default{
						Status: nfsv4_xdr.NFS4ERR_NOFILEHANDLE,
					},
				},
			},
			Status: nfsv4_xdr.NFS4ERR_NOFILEHANDLE,
		}, res)
	})

	t.Run("NotDirectory", func(t *testing.T) {
		leaf := mock.NewMockVirtualLeaf(ctrl)
		clock.EXPECT().Now().Return(time.Unix(1000, 0))
		handleResolverExpectCall(t, handleResolver, []byte{1, 2, 3}, virtual.DirectoryChild{}.FromLeaf(leaf), virtual.StatusOK)

		res, err := program.NfsV4Nfsproc4Compound(ctx, &nfsv4_xdr.Compound4args{
			Tag: "lookup",
			Argarray: []nfsv4_xdr.NfsArgop4{
				&nfsv4_xdr.NfsArgop4_OP_PUTFH{
					Opputfh: nfsv4_xdr.Putfh4args{
						Object: nfsv4_xdr.NfsFh4{1, 2, 3},
					},
				},
				&nfsv4_xdr.NfsArgop4_OP_CREATE{
					Opcreate: nfsv4_xdr.Create4args{
						Objtype: &nfsv4_xdr.Createtype4_NF4DIR{},
						Objname: "Hello",
					},
				},
			},
		})
		require.NoError(t, err)
		require.Equal(t, &nfsv4_xdr.Compound4res{
			Tag: "lookup",
			Resarray: []nfsv4_xdr.NfsResop4{
				&nfsv4_xdr.NfsResop4_OP_PUTFH{
					Opputfh: nfsv4_xdr.Putfh4res{
						Status: nfsv4_xdr.NFS4_OK,
					},
				},
				&nfsv4_xdr.NfsResop4_OP_CREATE{
					Opcreate: &nfsv4_xdr.Create4res_default{
						Status: nfsv4_xdr.NFS4ERR_NOTDIR,
					},
				},
			},
			Status: nfsv4_xdr.NFS4ERR_NOTDIR,
		}, res)
	})

	t.Run("BadName", func(t *testing.T) {
		res, err := program.NfsV4Nfsproc4Compound(ctx, &nfsv4_xdr.Compound4args{
			Tag: "create",
			Argarray: []nfsv4_xdr.NfsArgop4{
				&nfsv4_xdr.NfsArgop4_OP_PUTROOTFH{},
				&nfsv4_xdr.NfsArgop4_OP_CREATE{
					Opcreate: nfsv4_xdr.Create4args{
						Objtype: &nfsv4_xdr.Createtype4_NF4DIR{},
						Objname: "..",
					},
				},
			},
		})
		require.NoError(t, err)
		require.Equal(t, &nfsv4_xdr.Compound4res{
			Tag: "create",
			Resarray: []nfsv4_xdr.NfsResop4{
				&nfsv4_xdr.NfsResop4_OP_PUTROOTFH{
					Opputrootfh: nfsv4_xdr.Putrootfh4res{
						Status: nfsv4_xdr.NFS4_OK,
					},
				},
				&nfsv4_xdr.NfsResop4_OP_CREATE{
					Opcreate: &nfsv4_xdr.Create4res_default{
						Status: nfsv4_xdr.NFS4ERR_BADNAME,
					},
				},
			},
			Status: nfsv4_xdr.NFS4ERR_BADNAME,
		}, res)
	})

	t.Run("MissingName", func(t *testing.T) {
		res, err := program.NfsV4Nfsproc4Compound(ctx, &nfsv4_xdr.Compound4args{
			Tag: "create",
			Argarray: []nfsv4_xdr.NfsArgop4{
				&nfsv4_xdr.NfsArgop4_OP_PUTROOTFH{},
				&nfsv4_xdr.NfsArgop4_OP_CREATE{
					Opcreate: nfsv4_xdr.Create4args{
						Objtype: &nfsv4_xdr.Createtype4_NF4DIR{},
					},
				},
			},
		})
		require.NoError(t, err)
		require.Equal(t, &nfsv4_xdr.Compound4res{
			Tag: "create",
			Resarray: []nfsv4_xdr.NfsResop4{
				&nfsv4_xdr.NfsResop4_OP_PUTROOTFH{
					Opputrootfh: nfsv4_xdr.Putrootfh4res{
						Status: nfsv4_xdr.NFS4_OK,
					},
				},
				&nfsv4_xdr.NfsResop4_OP_CREATE{
					Opcreate: &nfsv4_xdr.Create4res_default{
						Status: nfsv4_xdr.NFS4ERR_INVAL,
					},
				},
			},
			Status: nfsv4_xdr.NFS4ERR_INVAL,
		}, res)
	})

	t.Run("BadType", func(t *testing.T) {
		res, err := program.NfsV4Nfsproc4Compound(ctx, &nfsv4_xdr.Compound4args{
			Tag: "create",
			Argarray: []nfsv4_xdr.NfsArgop4{
				&nfsv4_xdr.NfsArgop4_OP_PUTROOTFH{},
				&nfsv4_xdr.NfsArgop4_OP_CREATE{
					Opcreate: nfsv4_xdr.Create4args{
						Objtype: &nfsv4_xdr.Createtype4_default{
							Type: nfsv4_xdr.NF4REG,
						},
						Objname: "file",
					},
				},
			},
		})
		require.NoError(t, err)
		require.Equal(t, &nfsv4_xdr.Compound4res{
			Tag: "create",
			Resarray: []nfsv4_xdr.NfsResop4{
				&nfsv4_xdr.NfsResop4_OP_PUTROOTFH{
					Opputrootfh: nfsv4_xdr.Putrootfh4res{
						Status: nfsv4_xdr.NFS4_OK,
					},
				},
				&nfsv4_xdr.NfsResop4_OP_CREATE{
					Opcreate: &nfsv4_xdr.Create4res_default{
						Status: nfsv4_xdr.NFS4ERR_BADTYPE,
					},
				},
			},
			Status: nfsv4_xdr.NFS4ERR_BADTYPE,
		}, res)
	})

	t.Run("SymlinkFailure", func(t *testing.T) {
		rootDirectory.EXPECT().VirtualSymlink(
			ctx,
			[]byte("target"),
			path.MustNewComponent("symlink"),
			virtual.AttributesMaskFileHandle,
			gomock.Any(),
		).Return(nil, virtual.ChangeInfo{}, virtual.StatusErrAccess)

		res, err := program.NfsV4Nfsproc4Compound(ctx, &nfsv4_xdr.Compound4args{
			Tag: "create",
			Argarray: []nfsv4_xdr.NfsArgop4{
				&nfsv4_xdr.NfsArgop4_OP_PUTROOTFH{},
				&nfsv4_xdr.NfsArgop4_OP_CREATE{
					Opcreate: nfsv4_xdr.Create4args{
						Objtype: &nfsv4_xdr.Createtype4_NF4LNK{
							Linkdata: nfsv4_xdr.Linktext4("target"),
						},
						Objname: "symlink",
					},
				},
			},
		})
		require.NoError(t, err)
		require.Equal(t, &nfsv4_xdr.Compound4res{
			Tag: "create",
			Resarray: []nfsv4_xdr.NfsResop4{
				&nfsv4_xdr.NfsResop4_OP_PUTROOTFH{
					Opputrootfh: nfsv4_xdr.Putrootfh4res{
						Status: nfsv4_xdr.NFS4_OK,
					},
				},
				&nfsv4_xdr.NfsResop4_OP_CREATE{
					Opcreate: &nfsv4_xdr.Create4res_default{
						Status: nfsv4_xdr.NFS4ERR_ACCESS,
					},
				},
			},
			Status: nfsv4_xdr.NFS4ERR_ACCESS,
		}, res)
	})

	t.Run("SymlinkSuccess", func(t *testing.T) {
		leaf := mock.NewMockVirtualLeaf(ctrl)
		rootDirectory.EXPECT().VirtualSymlink(
			ctx,
			[]byte("target"),
			path.MustNewComponent("symlink"),
			virtual.AttributesMaskFileHandle,
			gomock.Any(),
		).DoAndReturn(func(ctx context.Context, target []byte, name path.Component, requested virtual.AttributesMask, attributes *virtual.Attributes) (virtual.Leaf, virtual.ChangeInfo, virtual.Status) {
			attributes.SetFileHandle([]byte{0xbe, 0xb7, 0xe9, 0xb1, 0xbb, 0x21, 0x9a, 0xa8})
			return leaf, virtual.ChangeInfo{
				Before: 0x803325cc21deffd8,
				After:  0xa1b8abe75e185bb5,
			}, virtual.StatusOK
		})

		res, err := program.NfsV4Nfsproc4Compound(ctx, &nfsv4_xdr.Compound4args{
			Tag: "create",
			Argarray: []nfsv4_xdr.NfsArgop4{
				&nfsv4_xdr.NfsArgop4_OP_PUTROOTFH{},
				&nfsv4_xdr.NfsArgop4_OP_CREATE{
					Opcreate: nfsv4_xdr.Create4args{
						Objtype: &nfsv4_xdr.Createtype4_NF4LNK{
							Linkdata: nfsv4_xdr.Linktext4("target"),
						},
						Objname: "symlink",
					},
				},
				&nfsv4_xdr.NfsArgop4_OP_GETFH{},
			},
		})
		require.NoError(t, err)
		require.Equal(t, &nfsv4_xdr.Compound4res{
			Tag: "create",
			Resarray: []nfsv4_xdr.NfsResop4{
				&nfsv4_xdr.NfsResop4_OP_PUTROOTFH{
					Opputrootfh: nfsv4_xdr.Putrootfh4res{
						Status: nfsv4_xdr.NFS4_OK,
					},
				},
				&nfsv4_xdr.NfsResop4_OP_CREATE{
					Opcreate: &nfsv4_xdr.Create4res_NFS4_OK{
						Resok4: nfsv4_xdr.Create4resok{
							Cinfo: nfsv4_xdr.ChangeInfo4{
								Atomic: true,
								Before: 0x803325cc21deffd8,
								After:  0xa1b8abe75e185bb5,
							},
						},
					},
				},
				&nfsv4_xdr.NfsResop4_OP_GETFH{
					Opgetfh: &nfsv4_xdr.Getfh4res_NFS4_OK{
						Resok4: nfsv4_xdr.Getfh4resok{
							Object: nfsv4_xdr.NfsFh4{
								0xbe, 0xb7, 0xe9, 0xb1, 0xbb, 0x21, 0x9a, 0xa8,
							},
						},
					},
				},
			},
			Status: nfsv4_xdr.NFS4_OK,
		}, res)
	})

	t.Run("BlockDeviceFailure", func(t *testing.T) {
		// Disallow the creation of block devices. There is no
		// need for build actions to do that.
		res, err := program.NfsV4Nfsproc4Compound(ctx, &nfsv4_xdr.Compound4args{
			Tag: "create",
			Argarray: []nfsv4_xdr.NfsArgop4{
				&nfsv4_xdr.NfsArgop4_OP_PUTROOTFH{},
				&nfsv4_xdr.NfsArgop4_OP_CREATE{
					Opcreate: nfsv4_xdr.Create4args{
						Objtype: &nfsv4_xdr.Createtype4_NF4BLK{
							Devdata: nfsv4_xdr.Specdata4{
								Specdata1: 8,
								Specdata2: 0,
							},
						},
						Objname: "sda",
					},
				},
			},
		})
		require.NoError(t, err)
		require.Equal(t, &nfsv4_xdr.Compound4res{
			Tag: "create",
			Resarray: []nfsv4_xdr.NfsResop4{
				&nfsv4_xdr.NfsResop4_OP_PUTROOTFH{
					Opputrootfh: nfsv4_xdr.Putrootfh4res{
						Status: nfsv4_xdr.NFS4_OK,
					},
				},
				&nfsv4_xdr.NfsResop4_OP_CREATE{
					Opcreate: &nfsv4_xdr.Create4res_default{
						Status: nfsv4_xdr.NFS4ERR_PERM,
					},
				},
			},
			Status: nfsv4_xdr.NFS4ERR_PERM,
		}, res)
	})

	t.Run("CharacterDeviceFailure", func(t *testing.T) {
		// Disallow the creation of character devices. There is no
		// need for build actions to do that.
		res, err := program.NfsV4Nfsproc4Compound(ctx, &nfsv4_xdr.Compound4args{
			Tag: "create",
			Argarray: []nfsv4_xdr.NfsArgop4{
				&nfsv4_xdr.NfsArgop4_OP_PUTROOTFH{},
				&nfsv4_xdr.NfsArgop4_OP_CREATE{
					Opcreate: nfsv4_xdr.Create4args{
						Objtype: &nfsv4_xdr.Createtype4_NF4CHR{
							Devdata: nfsv4_xdr.Specdata4{
								Specdata1: 1,
								Specdata2: 3,
							},
						},
						Objname: "null",
					},
				},
			},
		})
		require.NoError(t, err)
		require.Equal(t, &nfsv4_xdr.Compound4res{
			Tag: "create",
			Resarray: []nfsv4_xdr.NfsResop4{
				&nfsv4_xdr.NfsResop4_OP_PUTROOTFH{
					Opputrootfh: nfsv4_xdr.Putrootfh4res{
						Status: nfsv4_xdr.NFS4_OK,
					},
				},
				&nfsv4_xdr.NfsResop4_OP_CREATE{
					Opcreate: &nfsv4_xdr.Create4res_default{
						Status: nfsv4_xdr.NFS4ERR_PERM,
					},
				},
			},
			Status: nfsv4_xdr.NFS4ERR_PERM,
		}, res)
	})

	t.Run("SocketSuccess", func(t *testing.T) {
		leaf := mock.NewMockVirtualLeaf(ctrl)
		rootDirectory.EXPECT().VirtualMknod(
			ctx,
			path.MustNewComponent("socket"),
			filesystem.FileTypeSocket,
			virtual.AttributesMaskFileHandle,
			gomock.Any(),
		).DoAndReturn(func(ctx context.Context, name path.Component, fileType filesystem.FileType, requested virtual.AttributesMask, attributes *virtual.Attributes) (virtual.Leaf, virtual.ChangeInfo, virtual.Status) {
			attributes.SetFileHandle([]byte{0xe0, 0x45, 0x9a, 0xca, 0x4f, 0x67, 0x7c, 0xaa})
			return leaf, virtual.ChangeInfo{
				Before: 0xf46dd045aaf43210,
				After:  0xc687134057752dbb,
			}, virtual.StatusOK
		})

		res, err := program.NfsV4Nfsproc4Compound(ctx, &nfsv4_xdr.Compound4args{
			Tag: "create",
			Argarray: []nfsv4_xdr.NfsArgop4{
				&nfsv4_xdr.NfsArgop4_OP_PUTROOTFH{},
				&nfsv4_xdr.NfsArgop4_OP_CREATE{
					Opcreate: nfsv4_xdr.Create4args{
						Objtype: &nfsv4_xdr.Createtype4_NF4SOCK{},
						Objname: "socket",
					},
				},
				&nfsv4_xdr.NfsArgop4_OP_GETFH{},
			},
		})
		require.NoError(t, err)
		require.Equal(t, &nfsv4_xdr.Compound4res{
			Tag: "create",
			Resarray: []nfsv4_xdr.NfsResop4{
				&nfsv4_xdr.NfsResop4_OP_PUTROOTFH{
					Opputrootfh: nfsv4_xdr.Putrootfh4res{
						Status: nfsv4_xdr.NFS4_OK,
					},
				},
				&nfsv4_xdr.NfsResop4_OP_CREATE{
					Opcreate: &nfsv4_xdr.Create4res_NFS4_OK{
						Resok4: nfsv4_xdr.Create4resok{
							Cinfo: nfsv4_xdr.ChangeInfo4{
								Atomic: true,
								Before: 0xf46dd045aaf43210,
								After:  0xc687134057752dbb,
							},
						},
					},
				},
				&nfsv4_xdr.NfsResop4_OP_GETFH{
					Opgetfh: &nfsv4_xdr.Getfh4res_NFS4_OK{
						Resok4: nfsv4_xdr.Getfh4resok{
							Object: nfsv4_xdr.NfsFh4{
								0xe0, 0x45, 0x9a, 0xca, 0x4f, 0x67, 0x7c, 0xaa,
							},
						},
					},
				},
			},
			Status: nfsv4_xdr.NFS4_OK,
		}, res)
	})

	t.Run("FIFOSuccess", func(t *testing.T) {
		leaf := mock.NewMockVirtualLeaf(ctrl)
		rootDirectory.EXPECT().VirtualMknod(
			ctx,
			path.MustNewComponent("fifo"),
			filesystem.FileTypeFIFO,
			virtual.AttributesMaskFileHandle,
			gomock.Any(),
		).DoAndReturn(func(ctx context.Context, name path.Component, fileType filesystem.FileType, requested virtual.AttributesMask, attributes *virtual.Attributes) (virtual.Leaf, virtual.ChangeInfo, virtual.Status) {
			attributes.SetFileHandle([]byte{0x73, 0x9c, 0x31, 0x40, 0x63, 0x49, 0xbb, 0x09})
			return leaf, virtual.ChangeInfo{
				Before: 0x1e80315f7745fc50,
				After:  0xe280a823543ce5ac,
			}, virtual.StatusOK
		})

		res, err := program.NfsV4Nfsproc4Compound(ctx, &nfsv4_xdr.Compound4args{
			Tag: "create",
			Argarray: []nfsv4_xdr.NfsArgop4{
				&nfsv4_xdr.NfsArgop4_OP_PUTROOTFH{},
				&nfsv4_xdr.NfsArgop4_OP_CREATE{
					Opcreate: nfsv4_xdr.Create4args{
						Objtype: &nfsv4_xdr.Createtype4_NF4FIFO{},
						Objname: "fifo",
					},
				},
				&nfsv4_xdr.NfsArgop4_OP_GETFH{},
			},
		})
		require.NoError(t, err)
		require.Equal(t, &nfsv4_xdr.Compound4res{
			Tag: "create",
			Resarray: []nfsv4_xdr.NfsResop4{
				&nfsv4_xdr.NfsResop4_OP_PUTROOTFH{
					Opputrootfh: nfsv4_xdr.Putrootfh4res{
						Status: nfsv4_xdr.NFS4_OK,
					},
				},
				&nfsv4_xdr.NfsResop4_OP_CREATE{
					Opcreate: &nfsv4_xdr.Create4res_NFS4_OK{
						Resok4: nfsv4_xdr.Create4resok{
							Cinfo: nfsv4_xdr.ChangeInfo4{
								Atomic: true,
								Before: 0x1e80315f7745fc50,
								After:  0xe280a823543ce5ac,
							},
						},
					},
				},
				&nfsv4_xdr.NfsResop4_OP_GETFH{
					Opgetfh: &nfsv4_xdr.Getfh4res_NFS4_OK{
						Resok4: nfsv4_xdr.Getfh4resok{
							Object: nfsv4_xdr.NfsFh4{
								0x73, 0x9c, 0x31, 0x40, 0x63, 0x49, 0xbb, 0x09,
							},
						},
					},
				},
			},
			Status: nfsv4_xdr.NFS4_OK,
		}, res)
	})

	t.Run("DirectorySuccess", func(t *testing.T) {
		directory := mock.NewMockVirtualDirectory(ctrl)
		rootDirectory.EXPECT().VirtualMkdir(
			path.MustNewComponent("dir"),
			virtual.AttributesMaskFileHandle,
			gomock.Any(),
		).DoAndReturn(func(name path.Component, requested virtual.AttributesMask, attributes *virtual.Attributes) (virtual.Directory, virtual.ChangeInfo, virtual.Status) {
			attributes.SetFileHandle([]byte{0x19, 0xe5, 0x26, 0x1b, 0xee, 0x25, 0x4a, 0x76})
			return directory, virtual.ChangeInfo{
				Before: 0x60a4a64a5af2116f,
				After:  0x58e160960c2d0339,
			}, virtual.StatusOK
		})

		res, err := program.NfsV4Nfsproc4Compound(ctx, &nfsv4_xdr.Compound4args{
			Tag: "create",
			Argarray: []nfsv4_xdr.NfsArgop4{
				&nfsv4_xdr.NfsArgop4_OP_PUTROOTFH{},
				&nfsv4_xdr.NfsArgop4_OP_CREATE{
					Opcreate: nfsv4_xdr.Create4args{
						Objtype: &nfsv4_xdr.Createtype4_NF4DIR{},
						Objname: "dir",
					},
				},
				&nfsv4_xdr.NfsArgop4_OP_GETFH{},
			},
		})
		require.NoError(t, err)
		require.Equal(t, &nfsv4_xdr.Compound4res{
			Tag: "create",
			Resarray: []nfsv4_xdr.NfsResop4{
				&nfsv4_xdr.NfsResop4_OP_PUTROOTFH{
					Opputrootfh: nfsv4_xdr.Putrootfh4res{
						Status: nfsv4_xdr.NFS4_OK,
					},
				},
				&nfsv4_xdr.NfsResop4_OP_CREATE{
					Opcreate: &nfsv4_xdr.Create4res_NFS4_OK{
						Resok4: nfsv4_xdr.Create4resok{
							Cinfo: nfsv4_xdr.ChangeInfo4{
								Atomic: true,
								Before: 0x60a4a64a5af2116f,
								After:  0x58e160960c2d0339,
							},
						},
					},
				},
				&nfsv4_xdr.NfsResop4_OP_GETFH{
					Opgetfh: &nfsv4_xdr.Getfh4res_NFS4_OK{
						Resok4: nfsv4_xdr.Getfh4resok{
							Object: nfsv4_xdr.NfsFh4{
								0x19, 0xe5, 0x26, 0x1b, 0xee, 0x25, 0x4a, 0x76,
							},
						},
					},
				},
			},
			Status: nfsv4_xdr.NFS4_OK,
		}, res)
	})
}

func TestBaseProgramCompound_OP_DELEGPURGE(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	rootDirectory := mock.NewMockVirtualDirectory(ctrl)
	rootDirectory.EXPECT().VirtualGetAttributes(gomock.Any(), virtual.AttributesMaskFileHandle, gomock.Any()).
		Do(func(ctx context.Context, requested virtual.AttributesMask, attributes *virtual.Attributes) {
			attributes.SetFileHandle([]byte{0x45, 0x22, 0xbb, 0xf6, 0xf0, 0x61, 0x71, 0x6d})
		})
	handleResolver := mock.NewMockHandleResolver(ctrl)
	randomNumberGenerator := mock.NewMockSingleThreadedGenerator(ctrl)
	rebootVerifier := nfsv4_xdr.Verifier4{0x0b, 0xb3, 0x0d, 0xa3, 0x50, 0x11, 0x6b, 0x38}
	stateIDOtherPrefix := [...]byte{0x17, 0x18, 0x71, 0xc6}
	clock := mock.NewMockClock(ctrl)
	program := nfsv4.NewBaseProgram(rootDirectory, handleResolver.Call, randomNumberGenerator, rebootVerifier, stateIDOtherPrefix, clock, 2*time.Minute, time.Minute)

	t.Run("NotSupported", func(t *testing.T) {
		// As we don't support CLAIM_DELEGATE_PREV, this method
		// is required to return NFS4ERR_NOTSUPP.
		res, err := program.NfsV4Nfsproc4Compound(ctx, &nfsv4_xdr.Compound4args{
			Tag: "stat",
			Argarray: []nfsv4_xdr.NfsArgop4{
				&nfsv4_xdr.NfsArgop4_OP_DELEGPURGE{
					Opdelegpurge: nfsv4_xdr.Delegpurge4args{
						Clientid: 0xc08f7e033702ee2c,
					},
				},
			},
		})
		require.NoError(t, err)
		require.Equal(t, &nfsv4_xdr.Compound4res{
			Tag: "stat",
			Resarray: []nfsv4_xdr.NfsResop4{
				&nfsv4_xdr.NfsResop4_OP_DELEGPURGE{
					Opdelegpurge: nfsv4_xdr.Delegpurge4res{
						Status: nfsv4_xdr.NFS4ERR_NOTSUPP,
					},
				},
			},
			Status: nfsv4_xdr.NFS4ERR_NOTSUPP,
		}, res)
	})
}

// TODO: DELEGRETURN

func TestBaseProgramCompound_OP_GETATTR(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	rootDirectory := mock.NewMockVirtualDirectory(ctrl)
	rootDirectory.EXPECT().VirtualGetAttributes(gomock.Any(), virtual.AttributesMaskFileHandle, gomock.Any()).
		Do(func(ctx context.Context, requested virtual.AttributesMask, attributes *virtual.Attributes) {
			attributes.SetFileHandle([]byte{0x9b, 0x51, 0x40, 0x9b, 0x8c, 0x7a, 0x54, 0x47})
		})
	handleResolver := mock.NewMockHandleResolver(ctrl)
	randomNumberGenerator := mock.NewMockSingleThreadedGenerator(ctrl)
	rebootVerifier := nfsv4_xdr.Verifier4{0x5e, 0x5f, 0xfe, 0x34, 0x05, 0x98, 0x9d, 0xf1}
	stateIDOtherPrefix := [...]byte{0x3d, 0xc0, 0x5d, 0xd2}
	clock := mock.NewMockClock(ctrl)
	program := nfsv4.NewBaseProgram(rootDirectory, handleResolver.Call, randomNumberGenerator, rebootVerifier, stateIDOtherPrefix, clock, 2*time.Minute, time.Minute)

	t.Run("NoFileHandle", func(t *testing.T) {
		// Calling GETATTR without a file handle should fail.
		res, err := program.NfsV4Nfsproc4Compound(ctx, &nfsv4_xdr.Compound4args{
			Tag: "stat",
			Argarray: []nfsv4_xdr.NfsArgop4{
				&nfsv4_xdr.NfsArgop4_OP_GETATTR{
					Opgetattr: nfsv4_xdr.Getattr4args{
						AttrRequest: nfsv4_xdr.Bitmap4{
							(1 << nfsv4_xdr.FATTR4_TYPE) |
								(1 << nfsv4_xdr.FATTR4_FILEID),
						},
					},
				},
			},
		})
		require.NoError(t, err)
		require.Equal(t, &nfsv4_xdr.Compound4res{
			Tag: "stat",
			Resarray: []nfsv4_xdr.NfsResop4{
				&nfsv4_xdr.NfsResop4_OP_GETATTR{
					Opgetattr: &nfsv4_xdr.Getattr4res_default{
						Status: nfsv4_xdr.NFS4ERR_NOFILEHANDLE,
					},
				},
			},
			Status: nfsv4_xdr.NFS4ERR_NOFILEHANDLE,
		}, res)
	})

	t.Run("NoAttributes", func(t *testing.T) {
		// Request absolutely no attributes.
		rootDirectory.EXPECT().VirtualGetAttributes(ctx, virtual.AttributesMask(0), gomock.Any())

		res, err := program.NfsV4Nfsproc4Compound(ctx, &nfsv4_xdr.Compound4args{
			Tag: "stat",
			Argarray: []nfsv4_xdr.NfsArgop4{
				&nfsv4_xdr.NfsArgop4_OP_PUTROOTFH{},
				&nfsv4_xdr.NfsArgop4_OP_GETATTR{},
			},
		})
		require.NoError(t, err)
		require.Equal(t, &nfsv4_xdr.Compound4res{
			Tag: "stat",
			Resarray: []nfsv4_xdr.NfsResop4{
				&nfsv4_xdr.NfsResop4_OP_PUTROOTFH{
					Opputrootfh: nfsv4_xdr.Putrootfh4res{
						Status: nfsv4_xdr.NFS4_OK,
					},
				},
				&nfsv4_xdr.NfsResop4_OP_GETATTR{
					Opgetattr: &nfsv4_xdr.Getattr4res_NFS4_OK{
						Resok4: nfsv4_xdr.Getattr4resok{
							ObjAttributes: nfsv4_xdr.Fattr4{
								Attrmask: nfsv4_xdr.Bitmap4{},
							},
						},
					},
				},
			},
			Status: nfsv4_xdr.NFS4_OK,
		}, res)
	})

	t.Run("AllAttributes", func(t *testing.T) {
		// Request all supported attributes.
		rootDirectory.EXPECT().VirtualGetAttributes(
			ctx,
			virtual.AttributesMaskChangeID|virtual.AttributesMaskFileHandle|virtual.AttributesMaskFileType|virtual.AttributesMaskInodeNumber|virtual.AttributesMaskLastDataModificationTime|virtual.AttributesMaskLinkCount|virtual.AttributesMaskPermissions|virtual.AttributesMaskSizeBytes,
			gomock.Any(),
		).Do(func(ctx context.Context, requested virtual.AttributesMask, attributes *virtual.Attributes) {
			attributes.SetChangeID(0xeaab7253dad16ee5)
			attributes.SetFileHandle([]byte{0xcd, 0xe9, 0xc7, 0x4c, 0x8b, 0x8d, 0x58, 0xef, 0xd9, 0x9f})
			attributes.SetFileType(filesystem.FileTypeDirectory)
			attributes.SetInodeNumber(0xfcadd45521cb1db2)
			attributes.SetLastDataModificationTime(time.Unix(1654791566, 4839067173))
			attributes.SetLinkCount(12)
			attributes.SetPermissions(virtual.PermissionsRead | virtual.PermissionsExecute)
			attributes.SetSizeBytes(8192)
		})

		res, err := program.NfsV4Nfsproc4Compound(ctx, &nfsv4_xdr.Compound4args{
			Tag: "stat",
			Argarray: []nfsv4_xdr.NfsArgop4{
				&nfsv4_xdr.NfsArgop4_OP_PUTROOTFH{},
				&nfsv4_xdr.NfsArgop4_OP_GETATTR{
					Opgetattr: nfsv4_xdr.Getattr4args{
						AttrRequest: nfsv4_xdr.Bitmap4{
							(1 << nfsv4_xdr.FATTR4_SUPPORTED_ATTRS) |
								(1 << nfsv4_xdr.FATTR4_TYPE) |
								(1 << nfsv4_xdr.FATTR4_FH_EXPIRE_TYPE) |
								(1 << nfsv4_xdr.FATTR4_CHANGE) |
								(1 << nfsv4_xdr.FATTR4_SIZE) |
								(1 << nfsv4_xdr.FATTR4_LINK_SUPPORT) |
								(1 << nfsv4_xdr.FATTR4_SYMLINK_SUPPORT) |
								(1 << nfsv4_xdr.FATTR4_NAMED_ATTR) |
								(1 << nfsv4_xdr.FATTR4_FSID) |
								(1 << nfsv4_xdr.FATTR4_UNIQUE_HANDLES) |
								(1 << nfsv4_xdr.FATTR4_LEASE_TIME) |
								(1 << nfsv4_xdr.FATTR4_FILEHANDLE) |
								(1 << nfsv4_xdr.FATTR4_FILEID),
							(1 << (nfsv4_xdr.FATTR4_MODE - 32)) |
								(1 << (nfsv4_xdr.FATTR4_NUMLINKS - 32)) |
								(1 << (nfsv4_xdr.FATTR4_TIME_ACCESS - 32)) |
								(1 << (nfsv4_xdr.FATTR4_TIME_METADATA - 32)) |
								(1 << (nfsv4_xdr.FATTR4_TIME_MODIFY - 32)),
						},
					},
				},
			},
		})
		require.NoError(t, err)
		require.Equal(t, &nfsv4_xdr.Compound4res{
			Tag: "stat",
			Resarray: []nfsv4_xdr.NfsResop4{
				&nfsv4_xdr.NfsResop4_OP_PUTROOTFH{
					Opputrootfh: nfsv4_xdr.Putrootfh4res{
						Status: nfsv4_xdr.NFS4_OK,
					},
				},
				&nfsv4_xdr.NfsResop4_OP_GETATTR{
					Opgetattr: &nfsv4_xdr.Getattr4res_NFS4_OK{
						Resok4: nfsv4_xdr.Getattr4resok{
							ObjAttributes: nfsv4_xdr.Fattr4{
								Attrmask: nfsv4_xdr.Bitmap4{
									(1 << nfsv4_xdr.FATTR4_SUPPORTED_ATTRS) |
										(1 << nfsv4_xdr.FATTR4_TYPE) |
										(1 << nfsv4_xdr.FATTR4_FH_EXPIRE_TYPE) |
										(1 << nfsv4_xdr.FATTR4_CHANGE) |
										(1 << nfsv4_xdr.FATTR4_SIZE) |
										(1 << nfsv4_xdr.FATTR4_LINK_SUPPORT) |
										(1 << nfsv4_xdr.FATTR4_SYMLINK_SUPPORT) |
										(1 << nfsv4_xdr.FATTR4_NAMED_ATTR) |
										(1 << nfsv4_xdr.FATTR4_FSID) |
										(1 << nfsv4_xdr.FATTR4_UNIQUE_HANDLES) |
										(1 << nfsv4_xdr.FATTR4_LEASE_TIME) |
										(1 << nfsv4_xdr.FATTR4_FILEHANDLE) |
										(1 << nfsv4_xdr.FATTR4_FILEID),
									(1 << (nfsv4_xdr.FATTR4_MODE - 32)) |
										(1 << (nfsv4_xdr.FATTR4_NUMLINKS - 32)) |
										(1 << (nfsv4_xdr.FATTR4_TIME_ACCESS - 32)) |
										(1 << (nfsv4_xdr.FATTR4_TIME_METADATA - 32)) |
										(1 << (nfsv4_xdr.FATTR4_TIME_MODIFY - 32)),
								},
								AttrVals: nfsv4_xdr.Attrlist4{
									// FATTR4_SUPPORTED_ATTRS.
									0x00, 0x00, 0x00, 0x02,
									0x00, 0x18, 0x0f, 0xff,
									0x00, 0x30, 0x80, 0x0a,
									// FATTR4_TYPE == NF4DIR.
									0x00, 0x00, 0x00, 0x02,
									// FATTR4_FH_EXPIRE_TYPE == FH4_PERSISTENT.
									0x00, 0x00, 0x00, 0x00,
									// FATTR4_CHANGE.
									0xea, 0xab, 0x72, 0x53, 0xda, 0xd1, 0x6e, 0xe5,
									// FATTR4_SIZE.
									0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x20, 0x00,
									// FATTR4_LINK_SUPPORT == TRUE.
									0x00, 0x00, 0x00, 0x01,
									// FATTR4_SYMLINK_SUPPORT == TRUE.
									0x00, 0x00, 0x00, 0x01,
									// FATTR4_NAMED_ATTR == FALSE.
									0x00, 0x00, 0x00, 0x00,
									// FATTR4_FSID.
									0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01,
									0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01,
									// FATTR4_UNIQUE_HANDLES == TRUE.
									0x00, 0x00, 0x00, 0x01,
									// FATTR4_LEASE_TIME == 60 seconds.
									0x00, 0x00, 0x00, 0x3c,
									// FATTR4_FILEHANDLE.
									0x00, 0x00, 0x00, 0x0a,
									0xcd, 0xe9, 0xc7, 0x4c, 0x8b, 0x8d, 0x58, 0xef, 0xd9, 0x9f, 0x00, 0x00,
									// FATTR4_FILEID.
									0xfc, 0xad, 0xd4, 0x55, 0x21, 0xcb, 0x1d, 0xb2,
									// FATTR4_MODE.
									0x00, 0x00, 0x01, 0x6d,
									// FATTR4_NUMLINKS.
									0x00, 0x00, 0x00, 0x0c,
									// FATTR4_TIME_ACCESS == 2000-01-01T00:00:00Z.
									0x00, 0x00, 0x00, 0x00, 0x38, 0x6d, 0x43, 0x80,
									0x00, 0x00, 0x00, 0x00,
									// FATTR4_TIME_METADATA == 2000-01-01T00:00:00Z.
									0x00, 0x00, 0x00, 0x00, 0x38, 0x6d, 0x43, 0x80,
									0x00, 0x00, 0x00, 0x00,
									// FATTR4_TIME_MODIFY == 2022-06-09T16:19:26.4839067173Z.
									0x00, 0x00, 0x00, 0x00, 0x62, 0xa2, 0x1d, 0x92,
									0x32, 0x03, 0x26, 0x25,
								},
							},
						},
					},
				},
			},
			Status: nfsv4_xdr.NFS4_OK,
		}, res)
	})
}

func TestBaseProgramCompound_OP_GETFH(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	rootDirectory := mock.NewMockVirtualDirectory(ctrl)
	rootDirectory.EXPECT().VirtualGetAttributes(gomock.Any(), virtual.AttributesMaskFileHandle, gomock.Any()).
		Do(func(ctx context.Context, requested virtual.AttributesMask, attributes *virtual.Attributes) {
			attributes.SetFileHandle([]byte{0x85, 0xc5, 0x54, 0x77, 0x90, 0x7c, 0xf1, 0xf9})
		})
	handleResolver := mock.NewMockHandleResolver(ctrl)
	randomNumberGenerator := mock.NewMockSingleThreadedGenerator(ctrl)
	rebootVerifier := nfsv4_xdr.Verifier4{0x3c, 0x79, 0xba, 0xfe, 0xd6, 0x87, 0x1e, 0x32}
	stateIDOtherPrefix := [...]byte{0x95, 0xce, 0xb4, 0x96}
	clock := mock.NewMockClock(ctrl)
	program := nfsv4.NewBaseProgram(rootDirectory, handleResolver.Call, randomNumberGenerator, rebootVerifier, stateIDOtherPrefix, clock, 2*time.Minute, time.Minute)

	t.Run("NoFileHandle", func(t *testing.T) {
		// Calling GETFH without a file handle should fail.
		res, err := program.NfsV4Nfsproc4Compound(ctx, &nfsv4_xdr.Compound4args{
			Tag: "getfh",
			Argarray: []nfsv4_xdr.NfsArgop4{
				&nfsv4_xdr.NfsArgop4_OP_GETFH{},
			},
		})
		require.NoError(t, err)
		require.Equal(t, &nfsv4_xdr.Compound4res{
			Tag: "getfh",
			Resarray: []nfsv4_xdr.NfsResop4{
				&nfsv4_xdr.NfsResop4_OP_GETFH{
					Opgetfh: &nfsv4_xdr.Getfh4res_default{
						Status: nfsv4_xdr.NFS4ERR_NOFILEHANDLE,
					},
				},
			},
			Status: nfsv4_xdr.NFS4ERR_NOFILEHANDLE,
		}, res)
	})

	t.Run("Success", func(t *testing.T) {
		res, err := program.NfsV4Nfsproc4Compound(ctx, &nfsv4_xdr.Compound4args{
			Tag: "getfh",
			Argarray: []nfsv4_xdr.NfsArgop4{
				&nfsv4_xdr.NfsArgop4_OP_PUTROOTFH{},
				&nfsv4_xdr.NfsArgop4_OP_GETFH{},
			},
		})
		require.NoError(t, err)
		require.Equal(t, &nfsv4_xdr.Compound4res{
			Tag: "getfh",
			Resarray: []nfsv4_xdr.NfsResop4{
				&nfsv4_xdr.NfsResop4_OP_PUTROOTFH{
					Opputrootfh: nfsv4_xdr.Putrootfh4res{
						Status: nfsv4_xdr.NFS4_OK,
					},
				},
				&nfsv4_xdr.NfsResop4_OP_GETFH{
					Opgetfh: &nfsv4_xdr.Getfh4res_NFS4_OK{
						Resok4: nfsv4_xdr.Getfh4resok{
							Object: nfsv4_xdr.NfsFh4{
								0x85, 0xc5, 0x54, 0x77, 0x90, 0x7c, 0xf1, 0xf9,
							},
						},
					},
				},
			},
			Status: nfsv4_xdr.NFS4_OK,
		}, res)
	})
}

func TestBaseProgramCompound_OP_ILLEGAL(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	rootDirectory := mock.NewMockVirtualDirectory(ctrl)
	rootDirectory.EXPECT().VirtualGetAttributes(gomock.Any(), virtual.AttributesMaskFileHandle, gomock.Any()).
		Do(func(ctx context.Context, requested virtual.AttributesMask, attributes *virtual.Attributes) {
			attributes.SetFileHandle([]byte{0x0e, 0xad, 0xf1, 0x83, 0xb1, 0xc0, 0xfc, 0x6f})
		})
	handleResolver := mock.NewMockHandleResolver(ctrl)
	randomNumberGenerator := mock.NewMockSingleThreadedGenerator(ctrl)
	rebootVerifier := nfsv4_xdr.Verifier4{0x42, 0x51, 0x65, 0x8b, 0xd2, 0x27, 0xc4, 0x13}
	stateIDOtherPrefix := [...]byte{0x01, 0x22, 0xe2, 0xaa}
	clock := mock.NewMockClock(ctrl)
	program := nfsv4.NewBaseProgram(rootDirectory, handleResolver.Call, randomNumberGenerator, rebootVerifier, stateIDOtherPrefix, clock, 2*time.Minute, time.Minute)

	t.Run("Failure", func(t *testing.T) {
		res, err := program.NfsV4Nfsproc4Compound(ctx, &nfsv4_xdr.Compound4args{
			Tag: "illegal",
			Argarray: []nfsv4_xdr.NfsArgop4{
				&nfsv4_xdr.NfsArgop4_OP_ILLEGAL{},
			},
		})
		require.NoError(t, err)
		require.Equal(t, &nfsv4_xdr.Compound4res{
			Tag: "illegal",
			Resarray: []nfsv4_xdr.NfsResop4{
				&nfsv4_xdr.NfsResop4_OP_ILLEGAL{
					Opillegal: nfsv4_xdr.Illegal4res{
						Status: nfsv4_xdr.NFS4ERR_OP_ILLEGAL,
					},
				},
			},
			Status: nfsv4_xdr.NFS4ERR_OP_ILLEGAL,
		}, res)
	})
}

func TestBaseProgramCompound_OP_LINK(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	rootDirectory := mock.NewMockVirtualDirectory(ctrl)
	rootDirectory.EXPECT().VirtualGetAttributes(gomock.Any(), virtual.AttributesMaskFileHandle, gomock.Any()).
		Do(func(ctx context.Context, requested virtual.AttributesMask, attributes *virtual.Attributes) {
			attributes.SetFileHandle([]byte{0x27, 0xec, 0x12, 0x85, 0xcb, 0x2d, 0x57, 0xe2})
		})
	handleResolver := mock.NewMockHandleResolver(ctrl)
	randomNumberGenerator := mock.NewMockSingleThreadedGenerator(ctrl)
	rebootVerifier := nfsv4_xdr.Verifier4{0x8d, 0x94, 0x96, 0x9c, 0xe9, 0x4b, 0xcf, 0xf5}
	stateIDOtherPrefix := [...]byte{0xdf, 0xdb, 0x0d, 0x38}
	clock := mock.NewMockClock(ctrl)
	program := nfsv4.NewBaseProgram(rootDirectory, handleResolver.Call, randomNumberGenerator, rebootVerifier, stateIDOtherPrefix, clock, 2*time.Minute, time.Minute)

	t.Run("NoFileHandle1", func(t *testing.T) {
		// Calling LINK without any file handles should fail.
		res, err := program.NfsV4Nfsproc4Compound(ctx, &nfsv4_xdr.Compound4args{
			Tag: "link",
			Argarray: []nfsv4_xdr.NfsArgop4{
				&nfsv4_xdr.NfsArgop4_OP_LINK{
					Oplink: nfsv4_xdr.Link4args{
						Newname: "Hello",
					},
				},
			},
		})
		require.NoError(t, err)
		require.Equal(t, &nfsv4_xdr.Compound4res{
			Tag: "link",
			Resarray: []nfsv4_xdr.NfsResop4{
				&nfsv4_xdr.NfsResop4_OP_LINK{
					Oplink: &nfsv4_xdr.Link4res_default{
						Status: nfsv4_xdr.NFS4ERR_NOFILEHANDLE,
					},
				},
			},
			Status: nfsv4_xdr.NFS4ERR_NOFILEHANDLE,
		}, res)
	})

	t.Run("NoFileHandle2", func(t *testing.T) {
		// Calling LINK without a saved file handle should fail.
		res, err := program.NfsV4Nfsproc4Compound(ctx, &nfsv4_xdr.Compound4args{
			Tag: "link",
			Argarray: []nfsv4_xdr.NfsArgop4{
				&nfsv4_xdr.NfsArgop4_OP_PUTROOTFH{},
				&nfsv4_xdr.NfsArgop4_OP_LINK{
					Oplink: nfsv4_xdr.Link4args{
						Newname: "Hello",
					},
				},
			},
		})
		require.NoError(t, err)
		require.Equal(t, &nfsv4_xdr.Compound4res{
			Tag: "link",
			Resarray: []nfsv4_xdr.NfsResop4{
				&nfsv4_xdr.NfsResop4_OP_PUTROOTFH{
					Opputrootfh: nfsv4_xdr.Putrootfh4res{
						Status: nfsv4_xdr.NFS4_OK,
					},
				},
				&nfsv4_xdr.NfsResop4_OP_LINK{
					Oplink: &nfsv4_xdr.Link4res_default{
						Status: nfsv4_xdr.NFS4ERR_NOFILEHANDLE,
					},
				},
			},
			Status: nfsv4_xdr.NFS4ERR_NOFILEHANDLE,
		}, res)
	})

	t.Run("BadName", func(t *testing.T) {
		// Calling LINK with a bad filename should fail.
		leaf := mock.NewMockVirtualLeaf(ctrl)
		clock.EXPECT().Now().Return(time.Unix(1000, 0))
		handleResolverExpectCall(t, handleResolver, []byte{0x62, 0xfc, 0x0c, 0x8c, 0x94, 0x86, 0x8d, 0xc7}, virtual.DirectoryChild{}.FromLeaf(leaf), virtual.StatusOK)

		res, err := program.NfsV4Nfsproc4Compound(ctx, &nfsv4_xdr.Compound4args{
			Tag: "link",
			Argarray: []nfsv4_xdr.NfsArgop4{
				&nfsv4_xdr.NfsArgop4_OP_PUTFH{
					Opputfh: nfsv4_xdr.Putfh4args{
						Object: nfsv4_xdr.NfsFh4{
							0x62, 0xfc, 0x0c, 0x8c, 0x94, 0x86, 0x8d, 0xc7,
						},
					},
				},
				&nfsv4_xdr.NfsArgop4_OP_SAVEFH{},
				&nfsv4_xdr.NfsArgop4_OP_PUTROOTFH{},
				&nfsv4_xdr.NfsArgop4_OP_LINK{
					Oplink: nfsv4_xdr.Link4args{
						Newname: "..",
					},
				},
			},
		})
		require.NoError(t, err)
		require.Equal(t, &nfsv4_xdr.Compound4res{
			Tag: "link",
			Resarray: []nfsv4_xdr.NfsResop4{
				&nfsv4_xdr.NfsResop4_OP_PUTFH{
					Opputfh: nfsv4_xdr.Putfh4res{
						Status: nfsv4_xdr.NFS4_OK,
					},
				},
				&nfsv4_xdr.NfsResop4_OP_SAVEFH{
					Opsavefh: nfsv4_xdr.Savefh4res{
						Status: nfsv4_xdr.NFS4_OK,
					},
				},
				&nfsv4_xdr.NfsResop4_OP_PUTROOTFH{
					Opputrootfh: nfsv4_xdr.Putrootfh4res{
						Status: nfsv4_xdr.NFS4_OK,
					},
				},
				&nfsv4_xdr.NfsResop4_OP_LINK{
					Oplink: &nfsv4_xdr.Link4res_default{
						Status: nfsv4_xdr.NFS4ERR_BADNAME,
					},
				},
			},
			Status: nfsv4_xdr.NFS4ERR_BADNAME,
		}, res)
	})

	t.Run("MissingName", func(t *testing.T) {
		// Calling LINK with a name of length zero should fail
		// with NFS4ERR_INVAL.
		leaf := mock.NewMockVirtualLeaf(ctrl)
		clock.EXPECT().Now().Return(time.Unix(1001, 0))
		handleResolverExpectCall(t, handleResolver, []byte{0x62, 0xfc, 0x0c, 0x8c, 0x94, 0x86, 0x8d, 0xc7}, virtual.DirectoryChild{}.FromLeaf(leaf), virtual.StatusOK)

		res, err := program.NfsV4Nfsproc4Compound(ctx, &nfsv4_xdr.Compound4args{
			Tag: "link",
			Argarray: []nfsv4_xdr.NfsArgop4{
				&nfsv4_xdr.NfsArgop4_OP_PUTFH{
					Opputfh: nfsv4_xdr.Putfh4args{
						Object: nfsv4_xdr.NfsFh4{
							0x62, 0xfc, 0x0c, 0x8c, 0x94, 0x86, 0x8d, 0xc7,
						},
					},
				},
				&nfsv4_xdr.NfsArgop4_OP_SAVEFH{},
				&nfsv4_xdr.NfsArgop4_OP_PUTROOTFH{},
				&nfsv4_xdr.NfsArgop4_OP_LINK{},
			},
		})
		require.NoError(t, err)
		require.Equal(t, &nfsv4_xdr.Compound4res{
			Tag: "link",
			Resarray: []nfsv4_xdr.NfsResop4{
				&nfsv4_xdr.NfsResop4_OP_PUTFH{
					Opputfh: nfsv4_xdr.Putfh4res{
						Status: nfsv4_xdr.NFS4_OK,
					},
				},
				&nfsv4_xdr.NfsResop4_OP_SAVEFH{
					Opsavefh: nfsv4_xdr.Savefh4res{
						Status: nfsv4_xdr.NFS4_OK,
					},
				},
				&nfsv4_xdr.NfsResop4_OP_PUTROOTFH{
					Opputrootfh: nfsv4_xdr.Putrootfh4res{
						Status: nfsv4_xdr.NFS4_OK,
					},
				},
				&nfsv4_xdr.NfsResop4_OP_LINK{
					Oplink: &nfsv4_xdr.Link4res_default{
						Status: nfsv4_xdr.NFS4ERR_INVAL,
					},
				},
			},
			Status: nfsv4_xdr.NFS4ERR_INVAL,
		}, res)
	})

	t.Run("SourceIsDirectory", func(t *testing.T) {
		// Calling LINK with a directory as a source object should fail.
		directory := mock.NewMockVirtualDirectory(ctrl)
		clock.EXPECT().Now().Return(time.Unix(1002, 0))
		handleResolverExpectCall(t, handleResolver, []byte{0x92, 0xcc, 0xd9, 0x59, 0xef, 0xf3, 0xef, 0x0a}, virtual.DirectoryChild{}.FromDirectory(directory), virtual.StatusOK)

		res, err := program.NfsV4Nfsproc4Compound(ctx, &nfsv4_xdr.Compound4args{
			Tag: "link",
			Argarray: []nfsv4_xdr.NfsArgop4{
				&nfsv4_xdr.NfsArgop4_OP_PUTFH{
					Opputfh: nfsv4_xdr.Putfh4args{
						Object: nfsv4_xdr.NfsFh4{
							0x92, 0xcc, 0xd9, 0x59, 0xef, 0xf3, 0xef, 0x0a,
						},
					},
				},
				&nfsv4_xdr.NfsArgop4_OP_SAVEFH{},
				&nfsv4_xdr.NfsArgop4_OP_PUTROOTFH{},
				&nfsv4_xdr.NfsArgop4_OP_LINK{
					Oplink: nfsv4_xdr.Link4args{
						Newname: "Hello",
					},
				},
			},
		})
		require.NoError(t, err)
		require.Equal(t, &nfsv4_xdr.Compound4res{
			Tag: "link",
			Resarray: []nfsv4_xdr.NfsResop4{
				&nfsv4_xdr.NfsResop4_OP_PUTFH{
					Opputfh: nfsv4_xdr.Putfh4res{
						Status: nfsv4_xdr.NFS4_OK,
					},
				},
				&nfsv4_xdr.NfsResop4_OP_SAVEFH{
					Opsavefh: nfsv4_xdr.Savefh4res{
						Status: nfsv4_xdr.NFS4_OK,
					},
				},
				&nfsv4_xdr.NfsResop4_OP_PUTROOTFH{
					Opputrootfh: nfsv4_xdr.Putrootfh4res{
						Status: nfsv4_xdr.NFS4_OK,
					},
				},
				&nfsv4_xdr.NfsResop4_OP_LINK{
					Oplink: &nfsv4_xdr.Link4res_default{
						Status: nfsv4_xdr.NFS4ERR_ISDIR,
					},
				},
			},
			Status: nfsv4_xdr.NFS4ERR_ISDIR,
		}, res)
	})

	t.Run("LinkCreationFailure", func(t *testing.T) {
		// All arguments are correct, but the underlying
		// directory does not allow the link to be created.
		leaf := mock.NewMockVirtualLeaf(ctrl)
		clock.EXPECT().Now().Return(time.Unix(1003, 0))
		handleResolverExpectCall(t, handleResolver, []byte{0x98, 0x55, 0x2f, 0xf4, 0x06, 0xa1, 0xea, 0xbd}, virtual.DirectoryChild{}.FromLeaf(leaf), virtual.StatusOK)
		rootDirectory.EXPECT().VirtualLink(
			ctx,
			path.MustNewComponent("Hello"),
			leaf,
			virtual.AttributesMask(0),
			gomock.Any(),
		).Return(virtual.ChangeInfo{}, virtual.StatusErrXDev)

		res, err := program.NfsV4Nfsproc4Compound(ctx, &nfsv4_xdr.Compound4args{
			Tag: "link",
			Argarray: []nfsv4_xdr.NfsArgop4{
				&nfsv4_xdr.NfsArgop4_OP_PUTFH{
					Opputfh: nfsv4_xdr.Putfh4args{
						Object: nfsv4_xdr.NfsFh4{
							0x98, 0x55, 0x2f, 0xf4, 0x06, 0xa1, 0xea, 0xbd,
						},
					},
				},
				&nfsv4_xdr.NfsArgop4_OP_SAVEFH{},
				&nfsv4_xdr.NfsArgop4_OP_PUTROOTFH{},
				&nfsv4_xdr.NfsArgop4_OP_LINK{
					Oplink: nfsv4_xdr.Link4args{
						Newname: "Hello",
					},
				},
			},
		})
		require.NoError(t, err)
		require.Equal(t, &nfsv4_xdr.Compound4res{
			Tag: "link",
			Resarray: []nfsv4_xdr.NfsResop4{
				&nfsv4_xdr.NfsResop4_OP_PUTFH{
					Opputfh: nfsv4_xdr.Putfh4res{
						Status: nfsv4_xdr.NFS4_OK,
					},
				},
				&nfsv4_xdr.NfsResop4_OP_SAVEFH{
					Opsavefh: nfsv4_xdr.Savefh4res{
						Status: nfsv4_xdr.NFS4_OK,
					},
				},
				&nfsv4_xdr.NfsResop4_OP_PUTROOTFH{
					Opputrootfh: nfsv4_xdr.Putrootfh4res{
						Status: nfsv4_xdr.NFS4_OK,
					},
				},
				&nfsv4_xdr.NfsResop4_OP_LINK{
					Oplink: &nfsv4_xdr.Link4res_default{
						Status: nfsv4_xdr.NFS4ERR_XDEV,
					},
				},
			},
			Status: nfsv4_xdr.NFS4ERR_XDEV,
		}, res)
	})

	t.Run("Success", func(t *testing.T) {
		leaf := mock.NewMockVirtualLeaf(ctrl)
		clock.EXPECT().Now().Return(time.Unix(1004, 0))
		handleResolverExpectCall(t, handleResolver, []byte{0x98, 0x55, 0x2f, 0xf4, 0x06, 0xa1, 0xea, 0xbd}, virtual.DirectoryChild{}.FromLeaf(leaf), virtual.StatusOK)
		rootDirectory.EXPECT().VirtualLink(
			ctx,
			path.MustNewComponent("Hello"),
			leaf,
			virtual.AttributesMask(0),
			gomock.Any(),
		).Return(virtual.ChangeInfo{
			Before: 0x6eee6c2bf6db7101,
			After:  0x5d2447d9e6bec4b8,
		}, virtual.StatusOK)

		res, err := program.NfsV4Nfsproc4Compound(ctx, &nfsv4_xdr.Compound4args{
			Tag: "link",
			Argarray: []nfsv4_xdr.NfsArgop4{
				&nfsv4_xdr.NfsArgop4_OP_PUTFH{
					Opputfh: nfsv4_xdr.Putfh4args{
						Object: nfsv4_xdr.NfsFh4{
							0x98, 0x55, 0x2f, 0xf4, 0x06, 0xa1, 0xea, 0xbd,
						},
					},
				},
				&nfsv4_xdr.NfsArgop4_OP_SAVEFH{},
				&nfsv4_xdr.NfsArgop4_OP_PUTROOTFH{},
				&nfsv4_xdr.NfsArgop4_OP_LINK{
					Oplink: nfsv4_xdr.Link4args{
						Newname: "Hello",
					},
				},
			},
		})
		require.NoError(t, err)
		require.Equal(t, &nfsv4_xdr.Compound4res{
			Tag: "link",
			Resarray: []nfsv4_xdr.NfsResop4{
				&nfsv4_xdr.NfsResop4_OP_PUTFH{
					Opputfh: nfsv4_xdr.Putfh4res{
						Status: nfsv4_xdr.NFS4_OK,
					},
				},
				&nfsv4_xdr.NfsResop4_OP_SAVEFH{
					Opsavefh: nfsv4_xdr.Savefh4res{
						Status: nfsv4_xdr.NFS4_OK,
					},
				},
				&nfsv4_xdr.NfsResop4_OP_PUTROOTFH{
					Opputrootfh: nfsv4_xdr.Putrootfh4res{
						Status: nfsv4_xdr.NFS4_OK,
					},
				},
				&nfsv4_xdr.NfsResop4_OP_LINK{
					Oplink: &nfsv4_xdr.Link4res_NFS4_OK{
						Resok4: nfsv4_xdr.Link4resok{
							Cinfo: nfsv4_xdr.ChangeInfo4{
								Atomic: true,
								Before: 0x6eee6c2bf6db7101,
								After:  0x5d2447d9e6bec4b8,
							},
						},
					},
				},
			},
			Status: nfsv4_xdr.NFS4_OK,
		}, res)
	})
}

func TestBaseProgramCompound_OP_LOOKUP(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	rootDirectory := mock.NewMockVirtualDirectory(ctrl)
	rootDirectory.EXPECT().VirtualGetAttributes(gomock.Any(), virtual.AttributesMaskFileHandle, gomock.Any()).
		Do(func(ctx context.Context, requested virtual.AttributesMask, attributes *virtual.Attributes) {
			attributes.SetFileHandle([]byte{0x5a, 0x8a, 0xf7, 0x7b, 0x6f, 0x5e, 0xbc, 0xff})
		})
	handleResolver := mock.NewMockHandleResolver(ctrl)
	randomNumberGenerator := mock.NewMockSingleThreadedGenerator(ctrl)
	rebootVerifier := nfsv4_xdr.Verifier4{0xf5, 0x66, 0xea, 0xae, 0x76, 0x70, 0xd1, 0x5b}
	stateIDOtherPrefix := [...]byte{0x2d, 0x48, 0xd3, 0x9b}
	clock := mock.NewMockClock(ctrl)
	program := nfsv4.NewBaseProgram(rootDirectory, handleResolver.Call, randomNumberGenerator, rebootVerifier, stateIDOtherPrefix, clock, 2*time.Minute, time.Minute)

	t.Run("NoFileHandle", func(t *testing.T) {
		// Calling LOOKUP without a file handle should fail.
		res, err := program.NfsV4Nfsproc4Compound(ctx, &nfsv4_xdr.Compound4args{
			Tag: "lookup",
			Argarray: []nfsv4_xdr.NfsArgop4{
				&nfsv4_xdr.NfsArgop4_OP_LOOKUP{
					Oplookup: nfsv4_xdr.Lookup4args{
						Objname: "Hello",
					},
				},
			},
		})
		require.NoError(t, err)
		require.Equal(t, &nfsv4_xdr.Compound4res{
			Tag: "lookup",
			Resarray: []nfsv4_xdr.NfsResop4{
				&nfsv4_xdr.NfsResop4_OP_LOOKUP{
					Oplookup: nfsv4_xdr.Lookup4res{
						Status: nfsv4_xdr.NFS4ERR_NOFILEHANDLE,
					},
				},
			},
			Status: nfsv4_xdr.NFS4ERR_NOFILEHANDLE,
		}, res)
	})

	t.Run("BadName", func(t *testing.T) {
		res, err := program.NfsV4Nfsproc4Compound(ctx, &nfsv4_xdr.Compound4args{
			Tag: "lookup",
			Argarray: []nfsv4_xdr.NfsArgop4{
				&nfsv4_xdr.NfsArgop4_OP_PUTROOTFH{},
				&nfsv4_xdr.NfsArgop4_OP_LOOKUP{
					Oplookup: nfsv4_xdr.Lookup4args{
						Objname: "..",
					},
				},
			},
		})
		require.NoError(t, err)
		require.Equal(t, &nfsv4_xdr.Compound4res{
			Tag: "lookup",
			Resarray: []nfsv4_xdr.NfsResop4{
				&nfsv4_xdr.NfsResop4_OP_PUTROOTFH{
					Opputrootfh: nfsv4_xdr.Putrootfh4res{
						Status: nfsv4_xdr.NFS4_OK,
					},
				},
				&nfsv4_xdr.NfsResop4_OP_LOOKUP{
					Oplookup: nfsv4_xdr.Lookup4res{
						Status: nfsv4_xdr.NFS4ERR_BADNAME,
					},
				},
			},
			Status: nfsv4_xdr.NFS4ERR_BADNAME,
		}, res)
	})

	t.Run("MissingName", func(t *testing.T) {
		res, err := program.NfsV4Nfsproc4Compound(ctx, &nfsv4_xdr.Compound4args{
			Tag: "lookup",
			Argarray: []nfsv4_xdr.NfsArgop4{
				&nfsv4_xdr.NfsArgop4_OP_PUTROOTFH{},
				&nfsv4_xdr.NfsArgop4_OP_LOOKUP{},
			},
		})
		require.NoError(t, err)
		require.Equal(t, &nfsv4_xdr.Compound4res{
			Tag: "lookup",
			Resarray: []nfsv4_xdr.NfsResop4{
				&nfsv4_xdr.NfsResop4_OP_PUTROOTFH{
					Opputrootfh: nfsv4_xdr.Putrootfh4res{
						Status: nfsv4_xdr.NFS4_OK,
					},
				},
				&nfsv4_xdr.NfsResop4_OP_LOOKUP{
					Oplookup: nfsv4_xdr.Lookup4res{
						Status: nfsv4_xdr.NFS4ERR_INVAL,
					},
				},
			},
			Status: nfsv4_xdr.NFS4ERR_INVAL,
		}, res)
	})

	t.Run("NotDirectory", func(t *testing.T) {
		// When called against files other than symbolic links,
		// LOOKUP should return NFS4ERR_NOTDIR.
		leaf := mock.NewMockVirtualLeaf(ctrl)
		clock.EXPECT().Now().Return(time.Unix(1000, 0))
		handleResolverExpectCall(t, handleResolver, []byte{1, 2, 3}, virtual.DirectoryChild{}.FromLeaf(leaf), virtual.StatusOK)
		leaf.EXPECT().VirtualGetAttributes(ctx, virtual.AttributesMaskFileType, gomock.Any()).
			Do(func(ctx context.Context, requested virtual.AttributesMask, attributes *virtual.Attributes) {
				attributes.SetFileType(filesystem.FileTypeRegularFile)
			})

		res, err := program.NfsV4Nfsproc4Compound(ctx, &nfsv4_xdr.Compound4args{
			Tag: "lookup",
			Argarray: []nfsv4_xdr.NfsArgop4{
				&nfsv4_xdr.NfsArgop4_OP_PUTFH{
					Opputfh: nfsv4_xdr.Putfh4args{
						Object: nfsv4_xdr.NfsFh4{1, 2, 3},
					},
				},
				&nfsv4_xdr.NfsArgop4_OP_LOOKUP{
					Oplookup: nfsv4_xdr.Lookup4args{
						Objname: "Hello",
					},
				},
			},
		})
		require.NoError(t, err)
		require.Equal(t, &nfsv4_xdr.Compound4res{
			Tag: "lookup",
			Resarray: []nfsv4_xdr.NfsResop4{
				&nfsv4_xdr.NfsResop4_OP_PUTFH{
					Opputfh: nfsv4_xdr.Putfh4res{
						Status: nfsv4_xdr.NFS4_OK,
					},
				},
				&nfsv4_xdr.NfsResop4_OP_LOOKUP{
					Oplookup: nfsv4_xdr.Lookup4res{
						Status: nfsv4_xdr.NFS4ERR_NOTDIR,
					},
				},
			},
			Status: nfsv4_xdr.NFS4ERR_NOTDIR,
		}, res)
	})

	t.Run("Symlink", func(t *testing.T) {
		// When called against symbolic links, LOOKUP should
		// return NFS4ERR_SYMLINK. That way the client knows it
		// may need to do symlink expansion.
		leaf := mock.NewMockVirtualLeaf(ctrl)
		clock.EXPECT().Now().Return(time.Unix(1001, 0))
		handleResolverExpectCall(t, handleResolver, []byte{4, 5, 6}, virtual.DirectoryChild{}.FromLeaf(leaf), virtual.StatusOK)
		leaf.EXPECT().VirtualGetAttributes(ctx, virtual.AttributesMaskFileType, gomock.Any()).
			Do(func(ctx context.Context, requested virtual.AttributesMask, attributes *virtual.Attributes) {
				attributes.SetFileType(filesystem.FileTypeSymlink)
			})

		res, err := program.NfsV4Nfsproc4Compound(ctx, &nfsv4_xdr.Compound4args{
			Tag: "lookup",
			Argarray: []nfsv4_xdr.NfsArgop4{
				&nfsv4_xdr.NfsArgop4_OP_PUTFH{
					Opputfh: nfsv4_xdr.Putfh4args{
						Object: nfsv4_xdr.NfsFh4{4, 5, 6},
					},
				},
				&nfsv4_xdr.NfsArgop4_OP_LOOKUP{
					Oplookup: nfsv4_xdr.Lookup4args{
						Objname: "Hello",
					},
				},
			},
		})
		require.NoError(t, err)
		require.Equal(t, &nfsv4_xdr.Compound4res{
			Tag: "lookup",
			Resarray: []nfsv4_xdr.NfsResop4{
				&nfsv4_xdr.NfsResop4_OP_PUTFH{
					Opputfh: nfsv4_xdr.Putfh4res{
						Status: nfsv4_xdr.NFS4_OK,
					},
				},
				&nfsv4_xdr.NfsResop4_OP_LOOKUP{
					Oplookup: nfsv4_xdr.Lookup4res{
						Status: nfsv4_xdr.NFS4ERR_SYMLINK,
					},
				},
			},
			Status: nfsv4_xdr.NFS4ERR_SYMLINK,
		}, res)
	})

	t.Run("NotFound", func(t *testing.T) {
		rootDirectory.EXPECT().VirtualLookup(
			gomock.Any(),
			path.MustNewComponent("Hello"),
			virtual.AttributesMaskFileHandle,
			gomock.Any(),
		).Return(virtual.DirectoryChild{}, virtual.StatusErrNoEnt)

		res, err := program.NfsV4Nfsproc4Compound(ctx, &nfsv4_xdr.Compound4args{
			Tag: "lookup",
			Argarray: []nfsv4_xdr.NfsArgop4{
				&nfsv4_xdr.NfsArgop4_OP_PUTROOTFH{},
				&nfsv4_xdr.NfsArgop4_OP_LOOKUP{
					Oplookup: nfsv4_xdr.Lookup4args{
						Objname: "Hello",
					},
				},
			},
		})
		require.NoError(t, err)
		require.Equal(t, &nfsv4_xdr.Compound4res{
			Tag: "lookup",
			Resarray: []nfsv4_xdr.NfsResop4{
				&nfsv4_xdr.NfsResop4_OP_PUTROOTFH{
					Opputrootfh: nfsv4_xdr.Putrootfh4res{
						Status: nfsv4_xdr.NFS4_OK,
					},
				},
				&nfsv4_xdr.NfsResop4_OP_LOOKUP{
					Oplookup: nfsv4_xdr.Lookup4res{
						Status: nfsv4_xdr.NFS4ERR_NOENT,
					},
				},
			},
			Status: nfsv4_xdr.NFS4ERR_NOENT,
		}, res)
	})

	t.Run("Success", func(t *testing.T) {
		leaf := mock.NewMockVirtualLeaf(ctrl)
		rootDirectory.EXPECT().VirtualLookup(
			ctx,
			path.MustNewComponent("Hello"),
			virtual.AttributesMaskFileHandle,
			gomock.Any(),
		).DoAndReturn(func(ctx context.Context, name path.Component, requested virtual.AttributesMask, attributes *virtual.Attributes) (virtual.DirectoryChild, virtual.Status) {
			attributes.SetFileHandle([]byte{0x98, 0xb2, 0xdc, 0x6e, 0x34, 0xa2, 0xcf, 0xa5})
			return virtual.DirectoryChild{}.FromLeaf(leaf), virtual.StatusOK
		})

		res, err := program.NfsV4Nfsproc4Compound(ctx, &nfsv4_xdr.Compound4args{
			Tag: "lookup",
			Argarray: []nfsv4_xdr.NfsArgop4{
				&nfsv4_xdr.NfsArgop4_OP_PUTROOTFH{},
				&nfsv4_xdr.NfsArgop4_OP_LOOKUP{
					Oplookup: nfsv4_xdr.Lookup4args{
						Objname: "Hello",
					},
				},
				&nfsv4_xdr.NfsArgop4_OP_GETFH{},
			},
		})
		require.NoError(t, err)
		require.Equal(t, &nfsv4_xdr.Compound4res{
			Tag: "lookup",
			Resarray: []nfsv4_xdr.NfsResop4{
				&nfsv4_xdr.NfsResop4_OP_PUTROOTFH{
					Opputrootfh: nfsv4_xdr.Putrootfh4res{
						Status: nfsv4_xdr.NFS4_OK,
					},
				},
				&nfsv4_xdr.NfsResop4_OP_LOOKUP{
					Oplookup: nfsv4_xdr.Lookup4res{
						Status: nfsv4_xdr.NFS4_OK,
					},
				},
				&nfsv4_xdr.NfsResop4_OP_GETFH{
					Opgetfh: &nfsv4_xdr.Getfh4res_NFS4_OK{
						Resok4: nfsv4_xdr.Getfh4resok{
							Object: nfsv4_xdr.NfsFh4{
								0x98, 0xb2, 0xdc, 0x6e, 0x34, 0xa2, 0xcf, 0xa5,
							},
						},
					},
				},
			},
			Status: nfsv4_xdr.NFS4_OK,
		}, res)
	})
}

// TODO: LOOKUPP

func TestBaseProgramCompound_OP_NVERIFY(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	rootDirectory := mock.NewMockVirtualDirectory(ctrl)
	rootDirectory.EXPECT().VirtualGetAttributes(gomock.Any(), virtual.AttributesMaskFileHandle, gomock.Any()).
		Do(func(ctx context.Context, requested virtual.AttributesMask, attributes *virtual.Attributes) {
			attributes.SetFileHandle([]byte{0xe0, 0x7a, 0x5b, 0x53, 0x03, 0x7a, 0x0a, 0x6f})
		})
	handleResolver := mock.NewMockHandleResolver(ctrl)
	randomNumberGenerator := mock.NewMockSingleThreadedGenerator(ctrl)
	rebootVerifier := nfsv4_xdr.Verifier4{0xab, 0x23, 0xe8, 0x04, 0x79, 0x23, 0x0a, 0x27}
	stateIDOtherPrefix := [...]byte{0x41, 0x40, 0x91, 0x69}
	clock := mock.NewMockClock(ctrl)
	program := nfsv4.NewBaseProgram(rootDirectory, handleResolver.Call, randomNumberGenerator, rebootVerifier, stateIDOtherPrefix, clock, 2*time.Minute, time.Minute)

	// Only basic testing coverage for NVERIFY is provided, as it is
	// assumed most of the logic is shared with VERIFY.

	t.Run("Match", func(t *testing.T) {
		rootDirectory.EXPECT().VirtualGetAttributes(ctx, virtual.AttributesMaskFileType|virtual.AttributesMaskInodeNumber, gomock.Any()).
			Do(func(ctx context.Context, requested virtual.AttributesMask, attributes *virtual.Attributes) {
				attributes.SetFileType(filesystem.FileTypeDirectory)
				attributes.SetInodeNumber(0x676b7bcb66d92ed6)
			})

		res, err := program.NfsV4Nfsproc4Compound(ctx, &nfsv4_xdr.Compound4args{
			Tag: "nverify",
			Argarray: []nfsv4_xdr.NfsArgop4{
				&nfsv4_xdr.NfsArgop4_OP_PUTROOTFH{},
				&nfsv4_xdr.NfsArgop4_OP_NVERIFY{
					Opnverify: nfsv4_xdr.Nverify4args{
						ObjAttributes: nfsv4_xdr.Fattr4{
							Attrmask: nfsv4_xdr.Bitmap4{
								(1 << nfsv4_xdr.FATTR4_TYPE) |
									(1 << nfsv4_xdr.FATTR4_FILEID),
							},
							AttrVals: nfsv4_xdr.Attrlist4{
								// FATTR4_TYPE == NF4DIR.
								0x00, 0x00, 0x00, 0x02,
								// FATTR4_FILEID.
								0x67, 0x6b, 0x7b, 0xcb, 0x66, 0xd9, 0x2e, 0xd6,
							},
						},
					},
				},
			},
		})
		require.NoError(t, err)
		require.Equal(t, &nfsv4_xdr.Compound4res{
			Tag: "nverify",
			Resarray: []nfsv4_xdr.NfsResop4{
				&nfsv4_xdr.NfsResop4_OP_PUTROOTFH{
					Opputrootfh: nfsv4_xdr.Putrootfh4res{
						Status: nfsv4_xdr.NFS4_OK,
					},
				},
				&nfsv4_xdr.NfsResop4_OP_NVERIFY{
					Opnverify: nfsv4_xdr.Nverify4res{
						Status: nfsv4_xdr.NFS4ERR_SAME,
					},
				},
			},
			Status: nfsv4_xdr.NFS4ERR_SAME,
		}, res)
	})

	t.Run("Mismatch", func(t *testing.T) {
		rootDirectory.EXPECT().VirtualGetAttributes(ctx, virtual.AttributesMaskFileType, gomock.Any()).
			Do(func(ctx context.Context, requested virtual.AttributesMask, attributes *virtual.Attributes) {
				attributes.SetFileType(filesystem.FileTypeDirectory)
			})

		res, err := program.NfsV4Nfsproc4Compound(ctx, &nfsv4_xdr.Compound4args{
			Tag: "nverify",
			Argarray: []nfsv4_xdr.NfsArgop4{
				&nfsv4_xdr.NfsArgop4_OP_PUTROOTFH{},
				&nfsv4_xdr.NfsArgop4_OP_NVERIFY{
					Opnverify: nfsv4_xdr.Nverify4args{
						ObjAttributes: nfsv4_xdr.Fattr4{
							Attrmask: nfsv4_xdr.Bitmap4{
								1 << nfsv4_xdr.FATTR4_TYPE,
							},
							AttrVals: nfsv4_xdr.Attrlist4{
								// FATTR4_TYPE == NF4BLK.
								0x00, 0x00, 0x00, 0x03,
							},
						},
					},
				},
			},
		})
		require.NoError(t, err)
		require.Equal(t, &nfsv4_xdr.Compound4res{
			Tag: "nverify",
			Resarray: []nfsv4_xdr.NfsResop4{
				&nfsv4_xdr.NfsResop4_OP_PUTROOTFH{
					Opputrootfh: nfsv4_xdr.Putrootfh4res{
						Status: nfsv4_xdr.NFS4_OK,
					},
				},
				&nfsv4_xdr.NfsResop4_OP_NVERIFY{
					Opnverify: nfsv4_xdr.Nverify4res{
						Status: nfsv4_xdr.NFS4_OK,
					},
				},
			},
			Status: nfsv4_xdr.NFS4_OK,
		}, res)
	})
}

// TODO: OPEN

func TestBaseProgramCompound_OP_OPENATTR(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	rootDirectory := mock.NewMockVirtualDirectory(ctrl)
	rootDirectory.EXPECT().VirtualGetAttributes(gomock.Any(), virtual.AttributesMaskFileHandle, gomock.Any()).
		Do(func(ctx context.Context, requested virtual.AttributesMask, attributes *virtual.Attributes) {
			attributes.SetFileHandle([]byte{0x03, 0x86, 0xd4, 0xcb, 0x44, 0x7c, 0x7e, 0x77})
		})
	handleResolver := mock.NewMockHandleResolver(ctrl)
	randomNumberGenerator := mock.NewMockSingleThreadedGenerator(ctrl)
	rebootVerifier := nfsv4_xdr.Verifier4{0xe6, 0x7e, 0xb7, 0xdb, 0x52, 0x9c, 0x7c, 0x86}
	stateIDOtherPrefix := [...]byte{0x06, 0x00, 0x7c, 0x9d}
	clock := mock.NewMockClock(ctrl)
	program := nfsv4.NewBaseProgram(rootDirectory, handleResolver.Call, randomNumberGenerator, rebootVerifier, stateIDOtherPrefix, clock, 2*time.Minute, time.Minute)

	t.Run("NoFileHandle", func(t *testing.T) {
		// Calling OPENATTR without a file handle should fail.
		res, err := program.NfsV4Nfsproc4Compound(ctx, &nfsv4_xdr.Compound4args{
			Tag: "openattr",
			Argarray: []nfsv4_xdr.NfsArgop4{
				&nfsv4_xdr.NfsArgop4_OP_OPENATTR{
					Opopenattr: nfsv4_xdr.Openattr4args{
						Createdir: true,
					},
				},
			},
		})
		require.NoError(t, err)
		require.Equal(t, &nfsv4_xdr.Compound4res{
			Tag: "openattr",
			Resarray: []nfsv4_xdr.NfsResop4{
				&nfsv4_xdr.NfsResop4_OP_OPENATTR{
					Opopenattr: nfsv4_xdr.Openattr4res{
						Status: nfsv4_xdr.NFS4ERR_NOFILEHANDLE,
					},
				},
			},
			Status: nfsv4_xdr.NFS4ERR_NOFILEHANDLE,
		}, res)
	})

	t.Run("NotSupported", func(t *testing.T) {
		// This implementation does not support named attributes.
		res, err := program.NfsV4Nfsproc4Compound(ctx, &nfsv4_xdr.Compound4args{
			Tag: "openattr",
			Argarray: []nfsv4_xdr.NfsArgop4{
				&nfsv4_xdr.NfsArgop4_OP_PUTROOTFH{},
				&nfsv4_xdr.NfsArgop4_OP_OPENATTR{
					Opopenattr: nfsv4_xdr.Openattr4args{
						Createdir: true,
					},
				},
			},
		})
		require.NoError(t, err)
		require.Equal(t, &nfsv4_xdr.Compound4res{
			Tag: "openattr",
			Resarray: []nfsv4_xdr.NfsResop4{
				&nfsv4_xdr.NfsResop4_OP_PUTROOTFH{
					Opputrootfh: nfsv4_xdr.Putrootfh4res{
						Status: nfsv4_xdr.NFS4_OK,
					},
				},
				&nfsv4_xdr.NfsResop4_OP_OPENATTR{
					Opopenattr: nfsv4_xdr.Openattr4res{
						Status: nfsv4_xdr.NFS4ERR_NOTSUPP,
					},
				},
			},
			Status: nfsv4_xdr.NFS4ERR_NOTSUPP,
		}, res)
	})
}

// TODO: OPEN_CONFIRM
// TODO: OPEN_DOWNGRADE
// TODO: PUTFH
// TODO: PUTPUBFH
// TODO: PUTROOTFH

func TestBaseProgramCompound_OP_READ(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	rootDirectory := mock.NewMockVirtualDirectory(ctrl)
	rootDirectory.EXPECT().VirtualGetAttributes(gomock.Any(), virtual.AttributesMaskFileHandle, gomock.Any()).
		Do(func(ctx context.Context, requested virtual.AttributesMask, attributes *virtual.Attributes) {
			attributes.SetFileHandle([]byte{0x37, 0xfd, 0xd0, 0xfc, 0x45, 0x2b, 0x79, 0x32})
		})
	handleResolver := mock.NewMockHandleResolver(ctrl)
	randomNumberGenerator := mock.NewMockSingleThreadedGenerator(ctrl)
	rebootVerifier := nfsv4_xdr.Verifier4{0x58, 0x61, 0xb4, 0xff, 0x82, 0x40, 0x8f, 0x1a}
	stateIDOtherPrefix := [...]byte{0x55, 0xc7, 0xc6, 0xa0}
	clock := mock.NewMockClock(ctrl)
	program := nfsv4.NewBaseProgram(rootDirectory, handleResolver.Call, randomNumberGenerator, rebootVerifier, stateIDOtherPrefix, clock, 2*time.Minute, time.Minute)

	t.Run("StaleStateID", func(t *testing.T) {
		// Providing a state ID that uses an unknown prefix
		// should cause READ to fail with NFS4ERR_STALE_STATEID,
		// as it likely refers to a state ID from before a
		// restart.
		leaf := mock.NewMockVirtualLeaf(ctrl)
		clock.EXPECT().Now().Return(time.Unix(1000, 0))
		handleResolverExpectCall(t, handleResolver, []byte{1, 2, 3}, virtual.DirectoryChild{}.FromLeaf(leaf), virtual.StatusOK)

		res, err := program.NfsV4Nfsproc4Compound(ctx, &nfsv4_xdr.Compound4args{
			Tag: "read",
			Argarray: []nfsv4_xdr.NfsArgop4{
				&nfsv4_xdr.NfsArgop4_OP_PUTFH{
					Opputfh: nfsv4_xdr.Putfh4args{
						Object: nfsv4_xdr.NfsFh4{1, 2, 3},
					},
				},
				&nfsv4_xdr.NfsArgop4_OP_READ{
					Opread: nfsv4_xdr.Read4args{
						Stateid: nfsv4_xdr.Stateid4{
							Seqid: 0xce56be4e,
							Other: [...]byte{
								0x88, 0xa8, 0x5a, 0x60,
								0x01, 0xa8, 0x3e, 0xff,
								0x36, 0xe4, 0xcf, 0xd8,
							},
						},
						Offset: 1000,
						Count:  100,
					},
				},
			},
		})
		require.NoError(t, err)
		require.Equal(t, &nfsv4_xdr.Compound4res{
			Tag: "read",
			Resarray: []nfsv4_xdr.NfsResop4{
				&nfsv4_xdr.NfsResop4_OP_PUTFH{
					Opputfh: nfsv4_xdr.Putfh4res{
						Status: nfsv4_xdr.NFS4_OK,
					},
				},
				&nfsv4_xdr.NfsResop4_OP_READ{
					Opread: &nfsv4_xdr.Read4res_default{
						Status: nfsv4_xdr.NFS4ERR_STALE_STATEID,
					},
				},
			},
			Status: nfsv4_xdr.NFS4ERR_STALE_STATEID,
		}, res)
	})

	t.Run("BadStateID", func(t *testing.T) {
		// The prefix of the state ID matches, but it does not
		// correspond to a known value.
		leaf := mock.NewMockVirtualLeaf(ctrl)
		clock.EXPECT().Now().Return(time.Unix(1001, 0))
		clock.EXPECT().Now().Return(time.Unix(1002, 0))
		handleResolverExpectCall(t, handleResolver, []byte{1, 2, 3}, virtual.DirectoryChild{}.FromLeaf(leaf), virtual.StatusOK)

		res, err := program.NfsV4Nfsproc4Compound(ctx, &nfsv4_xdr.Compound4args{
			Tag: "read",
			Argarray: []nfsv4_xdr.NfsArgop4{
				&nfsv4_xdr.NfsArgop4_OP_PUTFH{
					Opputfh: nfsv4_xdr.Putfh4args{
						Object: nfsv4_xdr.NfsFh4{1, 2, 3},
					},
				},
				&nfsv4_xdr.NfsArgop4_OP_READ{
					Opread: nfsv4_xdr.Read4args{
						Stateid: nfsv4_xdr.Stateid4{
							Seqid: 0xce56be4e,
							Other: [...]byte{
								0x55, 0xc7, 0xc6, 0xa0,
								0xdf, 0xa1, 0xb4, 0x3b,
								0xb2, 0x4c, 0x2b, 0x5f,
							},
						},
						Offset: 1000,
						Count:  100,
					},
				},
			},
		})
		require.NoError(t, err)
		require.Equal(t, &nfsv4_xdr.Compound4res{
			Tag: "read",
			Resarray: []nfsv4_xdr.NfsResop4{
				&nfsv4_xdr.NfsResop4_OP_PUTFH{
					Opputfh: nfsv4_xdr.Putfh4res{
						Status: nfsv4_xdr.NFS4_OK,
					},
				},
				&nfsv4_xdr.NfsResop4_OP_READ{
					Opread: &nfsv4_xdr.Read4res_default{
						Status: nfsv4_xdr.NFS4ERR_BAD_STATEID,
					},
				},
			},
			Status: nfsv4_xdr.NFS4ERR_BAD_STATEID,
		}, res)
	})

	t.Run("BadReadBypassStateID", func(t *testing.T) {
		// The standard requires that if the "other" field in
		// the state ID is all zeroes or all ones, the "seqid"
		// field must match.
		leaf := mock.NewMockVirtualLeaf(ctrl)
		clock.EXPECT().Now().Return(time.Unix(1003, 0))
		handleResolverExpectCall(t, handleResolver, []byte{1, 2, 3}, virtual.DirectoryChild{}.FromLeaf(leaf), virtual.StatusOK)

		res, err := program.NfsV4Nfsproc4Compound(ctx, &nfsv4_xdr.Compound4args{
			Tag: "read",
			Argarray: []nfsv4_xdr.NfsArgop4{
				&nfsv4_xdr.NfsArgop4_OP_PUTFH{
					Opputfh: nfsv4_xdr.Putfh4args{
						Object: nfsv4_xdr.NfsFh4{1, 2, 3},
					},
				},
				&nfsv4_xdr.NfsArgop4_OP_READ{
					Opread: nfsv4_xdr.Read4args{
						Stateid: nfsv4_xdr.Stateid4{
							Seqid: 123,
							Other: [...]byte{
								0xff, 0xff, 0xff, 0xff,
								0xff, 0xff, 0xff, 0xff,
								0xff, 0xff, 0xff, 0xff,
							},
						},
						Offset: 1000,
						Count:  100,
					},
				},
			},
		})
		require.NoError(t, err)
		require.Equal(t, &nfsv4_xdr.Compound4res{
			Tag: "read",
			Resarray: []nfsv4_xdr.NfsResop4{
				&nfsv4_xdr.NfsResop4_OP_PUTFH{
					Opputfh: nfsv4_xdr.Putfh4res{
						Status: nfsv4_xdr.NFS4_OK,
					},
				},
				&nfsv4_xdr.NfsResop4_OP_READ{
					Opread: &nfsv4_xdr.Read4res_default{
						Status: nfsv4_xdr.NFS4ERR_BAD_STATEID,
					},
				},
			},
			Status: nfsv4_xdr.NFS4ERR_BAD_STATEID,
		}, res)
	})

	t.Run("AnonymousStateIDNoFileHandle", func(t *testing.T) {
		res, err := program.NfsV4Nfsproc4Compound(ctx, &nfsv4_xdr.Compound4args{
			Tag: "read",
			Argarray: []nfsv4_xdr.NfsArgop4{
				&nfsv4_xdr.NfsArgop4_OP_READ{
					Opread: nfsv4_xdr.Read4args{
						Offset: 1000,
						Count:  100,
					},
				},
			},
		})
		require.NoError(t, err)
		require.Equal(t, &nfsv4_xdr.Compound4res{
			Tag: "read",
			Resarray: []nfsv4_xdr.NfsResop4{
				&nfsv4_xdr.NfsResop4_OP_READ{
					Opread: &nfsv4_xdr.Read4res_default{
						Status: nfsv4_xdr.NFS4ERR_NOFILEHANDLE,
					},
				},
			},
			Status: nfsv4_xdr.NFS4ERR_NOFILEHANDLE,
		}, res)
	})

	t.Run("AnonymousStateIDOpenFailure", func(t *testing.T) {
		// A state ID consisting exclusively of zero bits is
		// referred to as the anonymous state ID. It should
		// cause the underlying file to be opened temporarily.
		// Failures when doing so should propagate.
		leaf := mock.NewMockVirtualLeaf(ctrl)
		clock.EXPECT().Now().Return(time.Unix(1004, 0))
		handleResolverExpectCall(t, handleResolver, []byte{4, 5, 6}, virtual.DirectoryChild{}.FromLeaf(leaf), virtual.StatusOK)
		leaf.EXPECT().VirtualOpenSelf(ctx, virtual.ShareMaskRead, &virtual.OpenExistingOptions{}, virtual.AttributesMask(0), gomock.Any()).Return(virtual.StatusErrIO)

		res, err := program.NfsV4Nfsproc4Compound(ctx, &nfsv4_xdr.Compound4args{
			Tag: "read",
			Argarray: []nfsv4_xdr.NfsArgop4{
				&nfsv4_xdr.NfsArgop4_OP_PUTFH{
					Opputfh: nfsv4_xdr.Putfh4args{
						Object: nfsv4_xdr.NfsFh4{4, 5, 6},
					},
				},
				&nfsv4_xdr.NfsArgop4_OP_READ{
					Opread: nfsv4_xdr.Read4args{
						Offset: 1000,
						Count:  100,
					},
				},
			},
		})
		require.NoError(t, err)
		require.Equal(t, &nfsv4_xdr.Compound4res{
			Tag: "read",
			Resarray: []nfsv4_xdr.NfsResop4{
				&nfsv4_xdr.NfsResop4_OP_PUTFH{
					Opputfh: nfsv4_xdr.Putfh4res{
						Status: nfsv4_xdr.NFS4_OK,
					},
				},
				&nfsv4_xdr.NfsResop4_OP_READ{
					Opread: &nfsv4_xdr.Read4res_default{
						Status: nfsv4_xdr.NFS4ERR_IO,
					},
				},
			},
			Status: nfsv4_xdr.NFS4ERR_IO,
		}, res)
	})

	t.Run("AnonymousStateIDReadFailure", func(t *testing.T) {
		leaf := mock.NewMockVirtualLeaf(ctrl)
		clock.EXPECT().Now().Return(time.Unix(1005, 0))
		handleResolverExpectCall(t, handleResolver, []byte{4, 5, 6}, virtual.DirectoryChild{}.FromLeaf(leaf), virtual.StatusOK)
		gomock.InOrder(
			leaf.EXPECT().VirtualOpenSelf(ctx, virtual.ShareMaskRead, &virtual.OpenExistingOptions{}, virtual.AttributesMask(0), gomock.Any()),
			leaf.EXPECT().VirtualRead(gomock.Len(100), uint64(1000)).Return(0, false, virtual.StatusErrIO),
			leaf.EXPECT().VirtualClose())

		res, err := program.NfsV4Nfsproc4Compound(ctx, &nfsv4_xdr.Compound4args{
			Tag: "read",
			Argarray: []nfsv4_xdr.NfsArgop4{
				&nfsv4_xdr.NfsArgop4_OP_PUTFH{
					Opputfh: nfsv4_xdr.Putfh4args{
						Object: nfsv4_xdr.NfsFh4{4, 5, 6},
					},
				},
				&nfsv4_xdr.NfsArgop4_OP_READ{
					Opread: nfsv4_xdr.Read4args{
						Offset: 1000,
						Count:  100,
					},
				},
			},
		})
		require.NoError(t, err)
		require.Equal(t, &nfsv4_xdr.Compound4res{
			Tag: "read",
			Resarray: []nfsv4_xdr.NfsResop4{
				&nfsv4_xdr.NfsResop4_OP_PUTFH{
					Opputfh: nfsv4_xdr.Putfh4res{
						Status: nfsv4_xdr.NFS4_OK,
					},
				},
				&nfsv4_xdr.NfsResop4_OP_READ{
					Opread: &nfsv4_xdr.Read4res_default{
						Status: nfsv4_xdr.NFS4ERR_IO,
					},
				},
			},
			Status: nfsv4_xdr.NFS4ERR_IO,
		}, res)
	})

	t.Run("AnonymousStateIDSuccess", func(t *testing.T) {
		leaf := mock.NewMockVirtualLeaf(ctrl)
		clock.EXPECT().Now().Return(time.Unix(1006, 0))
		handleResolverExpectCall(t, handleResolver, []byte{4, 5, 6}, virtual.DirectoryChild{}.FromLeaf(leaf), virtual.StatusOK)
		gomock.InOrder(
			leaf.EXPECT().VirtualOpenSelf(ctx, virtual.ShareMaskRead, &virtual.OpenExistingOptions{}, virtual.AttributesMask(0), gomock.Any()),
			leaf.EXPECT().VirtualRead(gomock.Len(100), uint64(1000)).
				DoAndReturn(func(buf []byte, offset uint64) (int, bool, virtual.Status) {
					return copy(buf, "Hello"), true, virtual.StatusOK
				}),
			leaf.EXPECT().VirtualClose())

		res, err := program.NfsV4Nfsproc4Compound(ctx, &nfsv4_xdr.Compound4args{
			Tag: "read",
			Argarray: []nfsv4_xdr.NfsArgop4{
				&nfsv4_xdr.NfsArgop4_OP_PUTFH{
					Opputfh: nfsv4_xdr.Putfh4args{
						Object: nfsv4_xdr.NfsFh4{4, 5, 6},
					},
				},
				&nfsv4_xdr.NfsArgop4_OP_READ{
					Opread: nfsv4_xdr.Read4args{
						Offset: 1000,
						Count:  100,
					},
				},
			},
		})
		require.NoError(t, err)
		require.Equal(t, &nfsv4_xdr.Compound4res{
			Tag: "read",
			Resarray: []nfsv4_xdr.NfsResop4{
				&nfsv4_xdr.NfsResop4_OP_PUTFH{
					Opputfh: nfsv4_xdr.Putfh4res{
						Status: nfsv4_xdr.NFS4_OK,
					},
				},
				&nfsv4_xdr.NfsResop4_OP_READ{
					Opread: &nfsv4_xdr.Read4res_NFS4_OK{
						Resok4: nfsv4_xdr.Read4resok{
							Eof:  true,
							Data: []byte("Hello"),
						},
					},
				},
			},
			Status: nfsv4_xdr.NFS4_OK,
		}, res)
	})

	// The remainder of the test assumes the availability of a client ID.
	clock.EXPECT().Now().Return(time.Unix(1007, 0))
	clock.EXPECT().Now().Return(time.Unix(1008, 0))
	setClientIDForTesting(ctx, t, randomNumberGenerator, program, 0xf86e57129c7a628a)

	// Open a file for reading, but don't confirm it yet.
	leaf := mock.NewMockVirtualLeaf(ctrl)
	clock.EXPECT().Now().Return(time.Unix(1009, 0))
	clock.EXPECT().Now().Return(time.Unix(1010, 0))
	openUnconfirmedFileForTesting(
		ctx,
		t,
		randomNumberGenerator,
		program,
		rootDirectory,
		leaf,
		nfsv4_xdr.NfsFh4{0xd8, 0x47, 0x07, 0x55, 0x44, 0x96, 0x88, 0x8d},
		/* shortClientID = */ 0xf86e57129c7a628a,
		/* seqID = */ 7010,
		/* stateIDOther = */ [...]byte{
			0x55, 0xc7, 0xc6, 0xa0,
			0xe0, 0x17, 0x83, 0x9c,
			0x17, 0x7d, 0xa2, 0x16,
		})

	t.Run("UnconfirmedStateID", func(t *testing.T) {
		// The state ID belongs to an open-owner that has not
		// been confirmed using OPEN_CONFIRM yet. The READ
		// operation should not be permitted.
		clock.EXPECT().Now().Return(time.Unix(1011, 0))
		clock.EXPECT().Now().Return(time.Unix(1012, 0))

		res, err := program.NfsV4Nfsproc4Compound(ctx, &nfsv4_xdr.Compound4args{
			Tag: "read",
			Argarray: []nfsv4_xdr.NfsArgop4{
				&nfsv4_xdr.NfsArgop4_OP_PUTFH{
					Opputfh: nfsv4_xdr.Putfh4args{
						Object: nfsv4_xdr.NfsFh4{0xd8, 0x47, 0x07, 0x55, 0x44, 0x96, 0x88, 0x8d},
					},
				},
				&nfsv4_xdr.NfsArgop4_OP_READ{
					Opread: nfsv4_xdr.Read4args{
						Stateid: nfsv4_xdr.Stateid4{
							Seqid: 1,
							Other: [...]byte{
								0x55, 0xc7, 0xc6, 0xa0,
								0xe0, 0x17, 0x83, 0x9c,
								0x17, 0x7d, 0xa2, 0x16,
							},
						},
						Offset: 1000,
						Count:  100,
					},
				},
			},
		})
		require.NoError(t, err)
		require.Equal(t, &nfsv4_xdr.Compound4res{
			Tag: "read",
			Resarray: []nfsv4_xdr.NfsResop4{
				&nfsv4_xdr.NfsResop4_OP_PUTFH{
					Opputfh: nfsv4_xdr.Putfh4res{
						Status: nfsv4_xdr.NFS4_OK,
					},
				},
				&nfsv4_xdr.NfsResop4_OP_READ{
					Opread: &nfsv4_xdr.Read4res_default{
						Status: nfsv4_xdr.NFS4ERR_BAD_STATEID,
					},
				},
			},
			Status: nfsv4_xdr.NFS4ERR_BAD_STATEID,
		}, res)
	})

	// Confirm the open-owner for the remainder of the test.
	clock.EXPECT().Now().Return(time.Unix(1013, 0))
	clock.EXPECT().Now().Return(time.Unix(1014, 0))
	openConfirmForTesting(
		ctx,
		t,
		randomNumberGenerator,
		program,
		nfsv4_xdr.NfsFh4{0xd8, 0x47, 0x07, 0x55, 0x44, 0x96, 0x88, 0x8d},
		/* seqID = */ 7011,
		/* stateIDOther = */ [...]byte{
			0x55, 0xc7, 0xc6, 0xa0,
			0xe0, 0x17, 0x83, 0x9c,
			0x17, 0x7d, 0xa2, 0x16,
		})

	t.Run("OldStateID", func(t *testing.T) {
		// The OPEN_CONFIRM call above increased the sequence ID
		// of the state ID to 2. Calling READ with a lower value
		// should cause us to return NFS4ERR_OLD_STATEID.
		clock.EXPECT().Now().Return(time.Unix(1015, 0))
		clock.EXPECT().Now().Return(time.Unix(1016, 0))

		res, err := program.NfsV4Nfsproc4Compound(ctx, &nfsv4_xdr.Compound4args{
			Tag: "read",
			Argarray: []nfsv4_xdr.NfsArgop4{
				&nfsv4_xdr.NfsArgop4_OP_PUTFH{
					Opputfh: nfsv4_xdr.Putfh4args{
						Object: nfsv4_xdr.NfsFh4{0xd8, 0x47, 0x07, 0x55, 0x44, 0x96, 0x88, 0x8d},
					},
				},
				&nfsv4_xdr.NfsArgop4_OP_READ{
					Opread: nfsv4_xdr.Read4args{
						Stateid: nfsv4_xdr.Stateid4{
							Seqid: 1,
							Other: [...]byte{
								0x55, 0xc7, 0xc6, 0xa0,
								0xe0, 0x17, 0x83, 0x9c,
								0x17, 0x7d, 0xa2, 0x16,
							},
						},
						Offset: 1000,
						Count:  100,
					},
				},
			},
		})
		require.NoError(t, err)
		require.Equal(t, &nfsv4_xdr.Compound4res{
			Tag: "read",
			Resarray: []nfsv4_xdr.NfsResop4{
				&nfsv4_xdr.NfsResop4_OP_PUTFH{
					Opputfh: nfsv4_xdr.Putfh4res{
						Status: nfsv4_xdr.NFS4_OK,
					},
				},
				&nfsv4_xdr.NfsResop4_OP_READ{
					Opread: &nfsv4_xdr.Read4res_default{
						Status: nfsv4_xdr.NFS4ERR_OLD_STATEID,
					},
				},
			},
			Status: nfsv4_xdr.NFS4ERR_OLD_STATEID,
		}, res)
	})

	t.Run("FuturisticStateID", func(t *testing.T) {
		// Similarly, using sequence ID 3 is too new, as it's
		// never been handed out by the server.
		clock.EXPECT().Now().Return(time.Unix(1017, 0))
		clock.EXPECT().Now().Return(time.Unix(1018, 0))

		res, err := program.NfsV4Nfsproc4Compound(ctx, &nfsv4_xdr.Compound4args{
			Tag: "read",
			Argarray: []nfsv4_xdr.NfsArgop4{
				&nfsv4_xdr.NfsArgop4_OP_PUTFH{
					Opputfh: nfsv4_xdr.Putfh4args{
						Object: nfsv4_xdr.NfsFh4{0xd8, 0x47, 0x07, 0x55, 0x44, 0x96, 0x88, 0x8d},
					},
				},
				&nfsv4_xdr.NfsArgop4_OP_READ{
					Opread: nfsv4_xdr.Read4args{
						Stateid: nfsv4_xdr.Stateid4{
							Seqid: 3,
							Other: [...]byte{
								0x55, 0xc7, 0xc6, 0xa0,
								0xe0, 0x17, 0x83, 0x9c,
								0x17, 0x7d, 0xa2, 0x16,
							},
						},
						Offset: 1000,
						Count:  100,
					},
				},
			},
		})
		require.NoError(t, err)
		require.Equal(t, &nfsv4_xdr.Compound4res{
			Tag: "read",
			Resarray: []nfsv4_xdr.NfsResop4{
				&nfsv4_xdr.NfsResop4_OP_PUTFH{
					Opputfh: nfsv4_xdr.Putfh4res{
						Status: nfsv4_xdr.NFS4_OK,
					},
				},
				&nfsv4_xdr.NfsResop4_OP_READ{
					Opread: &nfsv4_xdr.Read4res_default{
						Status: nfsv4_xdr.NFS4ERR_BAD_STATEID,
					},
				},
			},
			Status: nfsv4_xdr.NFS4ERR_BAD_STATEID,
		}, res)
	})

	t.Run("OpenStateIDSuccess", func(t *testing.T) {
		clock.EXPECT().Now().Return(time.Unix(1019, 0))
		clock.EXPECT().Now().Return(time.Unix(1020, 0))
		clock.EXPECT().Now().Return(time.Unix(1021, 0))
		leaf.EXPECT().VirtualRead(gomock.Len(100), uint64(1000)).
			DoAndReturn(func(buf []byte, offset uint64) (int, bool, virtual.Status) {
				return copy(buf, "Hello"), true, virtual.StatusOK
			})

		res, err := program.NfsV4Nfsproc4Compound(ctx, &nfsv4_xdr.Compound4args{
			Tag: "read",
			Argarray: []nfsv4_xdr.NfsArgop4{
				&nfsv4_xdr.NfsArgop4_OP_PUTFH{
					Opputfh: nfsv4_xdr.Putfh4args{
						Object: nfsv4_xdr.NfsFh4{0xd8, 0x47, 0x07, 0x55, 0x44, 0x96, 0x88, 0x8d},
					},
				},
				&nfsv4_xdr.NfsArgop4_OP_READ{
					Opread: nfsv4_xdr.Read4args{
						Stateid: nfsv4_xdr.Stateid4{
							Seqid: 2,
							Other: [...]byte{
								0x55, 0xc7, 0xc6, 0xa0,
								0xe0, 0x17, 0x83, 0x9c,
								0x17, 0x7d, 0xa2, 0x16,
							},
						},
						Offset: 1000,
						Count:  100,
					},
				},
			},
		})
		require.NoError(t, err)
		require.Equal(t, &nfsv4_xdr.Compound4res{
			Tag: "read",
			Resarray: []nfsv4_xdr.NfsResop4{
				&nfsv4_xdr.NfsResop4_OP_PUTFH{
					Opputfh: nfsv4_xdr.Putfh4res{
						Status: nfsv4_xdr.NFS4_OK,
					},
				},
				&nfsv4_xdr.NfsResop4_OP_READ{
					Opread: &nfsv4_xdr.Read4res_NFS4_OK{
						Resok4: nfsv4_xdr.Read4resok{
							Eof:  true,
							Data: []byte("Hello"),
						},
					},
				},
			},
			Status: nfsv4_xdr.NFS4_OK,
		}, res)
	})

	// Acquire a lock on the file to test the behaviour of READ when
	// called with a lock state ID.
	clock.EXPECT().Now().Return(time.Unix(1022, 0))
	clock.EXPECT().Now().Return(time.Unix(1023, 0))
	randomNumberGeneratorExpectRead(randomNumberGenerator, []byte{0xe8, 0xf2, 0xf2, 0x43, 0xc1, 0x91, 0x76, 0x91})

	res, err := program.NfsV4Nfsproc4Compound(ctx, &nfsv4_xdr.Compound4args{
		Tag: "lock",
		Argarray: []nfsv4_xdr.NfsArgop4{
			&nfsv4_xdr.NfsArgop4_OP_PUTFH{
				Opputfh: nfsv4_xdr.Putfh4args{
					Object: nfsv4_xdr.NfsFh4{0xd8, 0x47, 0x07, 0x55, 0x44, 0x96, 0x88, 0x8d},
				},
			},
			&nfsv4_xdr.NfsArgop4_OP_LOCK{
				Oplock: nfsv4_xdr.Lock4args{
					Locktype: nfsv4_xdr.WRITE_LT,
					Reclaim:  false,
					Offset:   100,
					Length:   100,
					Locker: &nfsv4_xdr.Locker4_TRUE{
						OpenOwner: nfsv4_xdr.OpenToLockOwner4{
							OpenSeqid: 7012,
							OpenStateid: nfsv4_xdr.Stateid4{
								Seqid: 2,
								Other: [...]byte{
									0x55, 0xc7, 0xc6, 0xa0,
									0xe0, 0x17, 0x83, 0x9c,
									0x17, 0x7d, 0xa2, 0x16,
								},
							},
							LockSeqid: 9640,
							LockOwner: nfsv4_xdr.LockOwner4{
								Clientid: 0xf86e57129c7a628a,
								Owner:    []byte{0x58, 0xa8, 0x53, 0x4c, 0xf8, 0xe8, 0xaa, 0xf3},
							},
						},
					},
				},
			},
		},
	})
	require.NoError(t, err)
	require.Equal(t, &nfsv4_xdr.Compound4res{
		Tag: "lock",
		Resarray: []nfsv4_xdr.NfsResop4{
			&nfsv4_xdr.NfsResop4_OP_PUTFH{
				Opputfh: nfsv4_xdr.Putfh4res{
					Status: nfsv4_xdr.NFS4_OK,
				},
			},
			&nfsv4_xdr.NfsResop4_OP_LOCK{
				Oplock: &nfsv4_xdr.Lock4res_NFS4_OK{
					Resok4: nfsv4_xdr.Lock4resok{
						LockStateid: nfsv4_xdr.Stateid4{
							Seqid: 1,
							Other: [...]byte{
								0x55, 0xc7, 0xc6, 0xa0,
								0xe8, 0xf2, 0xf2, 0x43,
								0xc1, 0x91, 0x76, 0x91,
							},
						},
					},
				},
			},
		},
		Status: nfsv4_xdr.NFS4_OK,
	}, res)

	t.Run("LockStateIDSuccess", func(t *testing.T) {
		// It's also permitted to call READ using a lock state ID.
		clock.EXPECT().Now().Return(time.Unix(1024, 0))
		clock.EXPECT().Now().Return(time.Unix(1025, 0))
		clock.EXPECT().Now().Return(time.Unix(1026, 0))
		leaf.EXPECT().VirtualRead(gomock.Len(100), uint64(1000)).
			DoAndReturn(func(buf []byte, offset uint64) (int, bool, virtual.Status) {
				return copy(buf, "Hello"), true, virtual.StatusOK
			})

		res, err := program.NfsV4Nfsproc4Compound(ctx, &nfsv4_xdr.Compound4args{
			Tag: "read",
			Argarray: []nfsv4_xdr.NfsArgop4{
				&nfsv4_xdr.NfsArgop4_OP_PUTFH{
					Opputfh: nfsv4_xdr.Putfh4args{
						Object: nfsv4_xdr.NfsFh4{0xd8, 0x47, 0x07, 0x55, 0x44, 0x96, 0x88, 0x8d},
					},
				},
				&nfsv4_xdr.NfsArgop4_OP_READ{
					Opread: nfsv4_xdr.Read4args{
						Stateid: nfsv4_xdr.Stateid4{
							Seqid: 1,
							Other: [...]byte{
								0x55, 0xc7, 0xc6, 0xa0,
								0xe8, 0xf2, 0xf2, 0x43,
								0xc1, 0x91, 0x76, 0x91,
							},
						},
						Offset: 1000,
						Count:  100,
					},
				},
			},
		})
		require.NoError(t, err)
		require.Equal(t, &nfsv4_xdr.Compound4res{
			Tag: "read",
			Resarray: []nfsv4_xdr.NfsResop4{
				&nfsv4_xdr.NfsResop4_OP_PUTFH{
					Opputfh: nfsv4_xdr.Putfh4res{
						Status: nfsv4_xdr.NFS4_OK,
					},
				},
				&nfsv4_xdr.NfsResop4_OP_READ{
					Opread: &nfsv4_xdr.Read4res_NFS4_OK{
						Resok4: nfsv4_xdr.Read4resok{
							Eof:  true,
							Data: []byte("Hello"),
						},
					},
				},
			},
			Status: nfsv4_xdr.NFS4_OK,
		}, res)
	})

	// Close the file for the remainder of the test.
	clock.EXPECT().Now().Return(time.Unix(1027, 0))
	clock.EXPECT().Now().Return(time.Unix(1028, 0))
	leaf.EXPECT().VirtualClose()

	res, err = program.NfsV4Nfsproc4Compound(ctx, &nfsv4_xdr.Compound4args{
		Tag: "close",
		Argarray: []nfsv4_xdr.NfsArgop4{
			&nfsv4_xdr.NfsArgop4_OP_PUTFH{
				Opputfh: nfsv4_xdr.Putfh4args{
					Object: nfsv4_xdr.NfsFh4{0xd8, 0x47, 0x07, 0x55, 0x44, 0x96, 0x88, 0x8d},
				},
			},
			&nfsv4_xdr.NfsArgop4_OP_CLOSE{
				Opclose: nfsv4_xdr.Close4args{
					Seqid: 7013,
					OpenStateid: nfsv4_xdr.Stateid4{
						Seqid: 2,
						Other: [...]byte{
							0x55, 0xc7, 0xc6, 0xa0,
							0xe0, 0x17, 0x83, 0x9c,
							0x17, 0x7d, 0xa2, 0x16,
						},
					},
				},
			},
		},
	})
	require.NoError(t, err)
	require.Equal(t, &nfsv4_xdr.Compound4res{
		Tag: "close",
		Resarray: []nfsv4_xdr.NfsResop4{
			&nfsv4_xdr.NfsResop4_OP_PUTFH{
				Opputfh: nfsv4_xdr.Putfh4res{
					Status: nfsv4_xdr.NFS4_OK,
				},
			},
			&nfsv4_xdr.NfsResop4_OP_CLOSE{
				Opclose: &nfsv4_xdr.Close4res_NFS4_OK{
					OpenStateid: nfsv4_xdr.Stateid4{
						Seqid: 3,
						Other: [...]byte{
							0x55, 0xc7, 0xc6, 0xa0,
							0xe0, 0x17, 0x83, 0x9c,
							0x17, 0x7d, 0xa2, 0x16,
						},
					},
				},
			},
		},
		Status: nfsv4_xdr.NFS4_OK,
	}, res)

	t.Run("ClosedStateIDBefore", func(t *testing.T) {
		// Normally a subsequent operation on a state ID with a
		// sequence ID that's too low should return
		// NFS4ERR_OLD_STATEID. Because the state ID has been
		// closed altogether, we should see NFS4ERR_BAD_STATEID
		// instead.
		clock.EXPECT().Now().Return(time.Unix(1029, 0))
		clock.EXPECT().Now().Return(time.Unix(1030, 0))

		res, err := program.NfsV4Nfsproc4Compound(ctx, &nfsv4_xdr.Compound4args{
			Tag: "read",
			Argarray: []nfsv4_xdr.NfsArgop4{
				&nfsv4_xdr.NfsArgop4_OP_PUTFH{
					Opputfh: nfsv4_xdr.Putfh4args{
						Object: nfsv4_xdr.NfsFh4{0xd8, 0x47, 0x07, 0x55, 0x44, 0x96, 0x88, 0x8d},
					},
				},
				&nfsv4_xdr.NfsArgop4_OP_READ{
					Opread: nfsv4_xdr.Read4args{
						Stateid: nfsv4_xdr.Stateid4{
							Seqid: 2,
							Other: [...]byte{
								0x55, 0xc7, 0xc6, 0xa0,
								0xe0, 0x17, 0x83, 0x9c,
								0x17, 0x7d, 0xa2, 0x16,
							},
						},
						Offset: 1000,
						Count:  100,
					},
				},
			},
		})
		require.NoError(t, err)
		require.Equal(t, &nfsv4_xdr.Compound4res{
			Tag: "read",
			Resarray: []nfsv4_xdr.NfsResop4{
				&nfsv4_xdr.NfsResop4_OP_PUTFH{
					Opputfh: nfsv4_xdr.Putfh4res{
						Status: nfsv4_xdr.NFS4_OK,
					},
				},
				&nfsv4_xdr.NfsResop4_OP_READ{
					Opread: &nfsv4_xdr.Read4res_default{
						Status: nfsv4_xdr.NFS4ERR_BAD_STATEID,
					},
				},
			},
			Status: nfsv4_xdr.NFS4ERR_BAD_STATEID,
		}, res)
	})

	t.Run("ClosedStateIDAfter", func(t *testing.T) {
		// Similar to the above, using the file with the exact
		// state ID should also return NFS4ERR_BAD_STATEID.
		clock.EXPECT().Now().Return(time.Unix(1030, 0))
		clock.EXPECT().Now().Return(time.Unix(1031, 0))

		res, err := program.NfsV4Nfsproc4Compound(ctx, &nfsv4_xdr.Compound4args{
			Tag: "read",
			Argarray: []nfsv4_xdr.NfsArgop4{
				&nfsv4_xdr.NfsArgop4_OP_PUTFH{
					Opputfh: nfsv4_xdr.Putfh4args{
						Object: nfsv4_xdr.NfsFh4{0xd8, 0x47, 0x07, 0x55, 0x44, 0x96, 0x88, 0x8d},
					},
				},
				&nfsv4_xdr.NfsArgop4_OP_READ{
					Opread: nfsv4_xdr.Read4args{
						Stateid: nfsv4_xdr.Stateid4{
							Seqid: 3,
							Other: [...]byte{
								0x55, 0xc7, 0xc6, 0xa0,
								0xe0, 0x17, 0x83, 0x9c,
								0x17, 0x7d, 0xa2, 0x16,
							},
						},
						Offset: 1000,
						Count:  100,
					},
				},
			},
		})
		require.NoError(t, err)
		require.Equal(t, &nfsv4_xdr.Compound4res{
			Tag: "read",
			Resarray: []nfsv4_xdr.NfsResop4{
				&nfsv4_xdr.NfsResop4_OP_PUTFH{
					Opputfh: nfsv4_xdr.Putfh4res{
						Status: nfsv4_xdr.NFS4_OK,
					},
				},
				&nfsv4_xdr.NfsResop4_OP_READ{
					Opread: &nfsv4_xdr.Read4res_default{
						Status: nfsv4_xdr.NFS4ERR_BAD_STATEID,
					},
				},
			},
			Status: nfsv4_xdr.NFS4ERR_BAD_STATEID,
		}, res)
	})
}

func TestBaseProgramCompound_OP_READDIR(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	rootDirectory := mock.NewMockVirtualDirectory(ctrl)
	rootDirectory.EXPECT().VirtualGetAttributes(gomock.Any(), virtual.AttributesMaskFileHandle, gomock.Any()).
		Do(func(ctx context.Context, requested virtual.AttributesMask, attributes *virtual.Attributes) {
			attributes.SetFileHandle([]byte{0x52, 0x5e, 0x17, 0x6e, 0xad, 0x2f, 0xc3, 0xf9})
		})
	handleResolver := mock.NewMockHandleResolver(ctrl)
	randomNumberGenerator := mock.NewMockSingleThreadedGenerator(ctrl)
	rebootVerifier := nfsv4_xdr.Verifier4{0x80, 0x29, 0x6e, 0xe3, 0x1a, 0xf1, 0xec, 0x41}
	stateIDOtherPrefix := [...]byte{0xce, 0x11, 0x76, 0xe8}
	clock := mock.NewMockClock(ctrl)
	program := nfsv4.NewBaseProgram(rootDirectory, handleResolver.Call, randomNumberGenerator, rebootVerifier, stateIDOtherPrefix, clock, 2*time.Minute, time.Minute)

	t.Run("NoFileHandle", func(t *testing.T) {
		// Calling READDIR without a file handle should fail.
		res, err := program.NfsV4Nfsproc4Compound(ctx, &nfsv4_xdr.Compound4args{
			Tag: "readdir",
			Argarray: []nfsv4_xdr.NfsArgop4{
				&nfsv4_xdr.NfsArgop4_OP_READDIR{
					Opreaddir: nfsv4_xdr.Readdir4args{
						Maxcount: 1000,
						AttrRequest: nfsv4_xdr.Bitmap4{
							(1 << nfsv4_xdr.FATTR4_TYPE) |
								(1 << nfsv4_xdr.FATTR4_FILEID),
						},
					},
				},
			},
		})
		require.NoError(t, err)
		require.Equal(t, &nfsv4_xdr.Compound4res{
			Tag: "readdir",
			Resarray: []nfsv4_xdr.NfsResop4{
				&nfsv4_xdr.NfsResop4_OP_READDIR{
					Opreaddir: &nfsv4_xdr.Readdir4res_default{
						Status: nfsv4_xdr.NFS4ERR_NOFILEHANDLE,
					},
				},
			},
			Status: nfsv4_xdr.NFS4ERR_NOFILEHANDLE,
		}, res)
	})

	t.Run("NotDir", func(t *testing.T) {
		// Calling READDIR with a non-directory file handle
		// should fail.
		leaf := mock.NewMockVirtualLeaf(ctrl)
		rootDirectory.EXPECT().VirtualLookup(
			ctx,
			path.MustNewComponent("file"),
			virtual.AttributesMaskFileHandle,
			gomock.Any(),
		).DoAndReturn(func(ctx context.Context, name path.Component, requested virtual.AttributesMask, attributes *virtual.Attributes) (virtual.DirectoryChild, virtual.Status) {
			attributes.SetFileHandle([]byte{0x1c, 0xae, 0xab, 0x22, 0xdf, 0xf4, 0x9e, 0x93})
			return virtual.DirectoryChild{}.FromLeaf(leaf), virtual.StatusOK
		})

		res, err := program.NfsV4Nfsproc4Compound(ctx, &nfsv4_xdr.Compound4args{
			Tag: "readdir",
			Argarray: []nfsv4_xdr.NfsArgop4{
				&nfsv4_xdr.NfsArgop4_OP_PUTROOTFH{},
				&nfsv4_xdr.NfsArgop4_OP_LOOKUP{
					Oplookup: nfsv4_xdr.Lookup4args{
						Objname: "file",
					},
				},
				&nfsv4_xdr.NfsArgop4_OP_READDIR{
					Opreaddir: nfsv4_xdr.Readdir4args{
						Maxcount: 1000,
						AttrRequest: nfsv4_xdr.Bitmap4{
							(1 << nfsv4_xdr.FATTR4_TYPE) |
								(1 << nfsv4_xdr.FATTR4_FILEID),
						},
					},
				},
			},
		})
		require.NoError(t, err)
		require.Equal(t, &nfsv4_xdr.Compound4res{
			Tag: "readdir",
			Resarray: []nfsv4_xdr.NfsResop4{
				&nfsv4_xdr.NfsResop4_OP_PUTROOTFH{
					Opputrootfh: nfsv4_xdr.Putrootfh4res{
						Status: nfsv4_xdr.NFS4_OK,
					},
				},
				&nfsv4_xdr.NfsResop4_OP_LOOKUP{
					Oplookup: nfsv4_xdr.Lookup4res{
						Status: nfsv4_xdr.NFS4_OK,
					},
				},
				&nfsv4_xdr.NfsResop4_OP_READDIR{
					Opreaddir: &nfsv4_xdr.Readdir4res_default{
						Status: nfsv4_xdr.NFS4ERR_NOTDIR,
					},
				},
			},
			Status: nfsv4_xdr.NFS4ERR_NOTDIR,
		}, res)
	})

	t.Run("EmptyDirectory", func(t *testing.T) {
		// Returning no results should cause EOF to be set.
		rootDirectory.EXPECT().VirtualReadDir(
			ctx,
			uint64(0),
			virtual.AttributesMaskFileType|virtual.AttributesMaskInodeNumber,
			gomock.Any(),
		).Return(virtual.StatusOK)

		res, err := program.NfsV4Nfsproc4Compound(ctx, &nfsv4_xdr.Compound4args{
			Tag: "readdir",
			Argarray: []nfsv4_xdr.NfsArgop4{
				&nfsv4_xdr.NfsArgop4_OP_PUTROOTFH{},
				&nfsv4_xdr.NfsArgop4_OP_READDIR{
					Opreaddir: nfsv4_xdr.Readdir4args{
						Maxcount: 1000,
						AttrRequest: nfsv4_xdr.Bitmap4{
							(1 << nfsv4_xdr.FATTR4_TYPE) |
								(1 << nfsv4_xdr.FATTR4_FILEID),
						},
					},
				},
			},
		})
		require.NoError(t, err)
		require.Equal(t, &nfsv4_xdr.Compound4res{
			Tag: "readdir",
			Resarray: []nfsv4_xdr.NfsResop4{
				&nfsv4_xdr.NfsResop4_OP_PUTROOTFH{
					Opputrootfh: nfsv4_xdr.Putrootfh4res{
						Status: nfsv4_xdr.NFS4_OK,
					},
				},
				&nfsv4_xdr.NfsResop4_OP_READDIR{
					Opreaddir: &nfsv4_xdr.Readdir4res_NFS4_OK{
						Resok4: nfsv4_xdr.Readdir4resok{
							Cookieverf: rebootVerifier,
							Reply: nfsv4_xdr.Dirlist4{
								Eof: true,
							},
						},
					},
				},
			},
			Status: nfsv4_xdr.NFS4_OK,
		}, res)
	})

	t.Run("TooSmall", func(t *testing.T) {
		// The requested entry is going to be 56 bytes in size.
		// If READDIR is called with maxcount set to 59, the
		// request should fail with NFS4ERR_TOOSMALL.
		rootDirectory.EXPECT().VirtualReadDir(
			ctx,
			uint64(0),
			virtual.AttributesMaskFileType|virtual.AttributesMaskInodeNumber,
			gomock.Any(),
		).DoAndReturn(func(ctx context.Context, firstCookie uint64, attributesMask virtual.AttributesMask, reporter virtual.DirectoryEntryReporter) virtual.Status {
			leaf := mock.NewMockVirtualLeaf(ctrl)
			require.False(t, reporter.ReportEntry(
				uint64(1),
				path.MustNewComponent("file"),
				virtual.DirectoryChild{}.FromLeaf(leaf),
				(&virtual.Attributes{}).
					SetFileType(filesystem.FileTypeRegularFile).
					SetInodeNumber(123)))
			return virtual.StatusOK
		})

		res, err := program.NfsV4Nfsproc4Compound(ctx, &nfsv4_xdr.Compound4args{
			Tag: "readdir",
			Argarray: []nfsv4_xdr.NfsArgop4{
				&nfsv4_xdr.NfsArgop4_OP_PUTROOTFH{},
				&nfsv4_xdr.NfsArgop4_OP_READDIR{
					Opreaddir: nfsv4_xdr.Readdir4args{
						Maxcount: 59,
						AttrRequest: nfsv4_xdr.Bitmap4{
							(1 << nfsv4_xdr.FATTR4_TYPE) |
								(1 << nfsv4_xdr.FATTR4_FILEID),
						},
					},
				},
			},
		})
		require.NoError(t, err)
		require.Equal(t, &nfsv4_xdr.Compound4res{
			Tag: "readdir",
			Resarray: []nfsv4_xdr.NfsResop4{
				&nfsv4_xdr.NfsResop4_OP_PUTROOTFH{
					Opputrootfh: nfsv4_xdr.Putrootfh4res{
						Status: nfsv4_xdr.NFS4_OK,
					},
				},
				&nfsv4_xdr.NfsResop4_OP_READDIR{
					Opreaddir: &nfsv4_xdr.Readdir4res_default{
						Status: nfsv4_xdr.NFS4ERR_TOOSMALL,
					},
				},
			},
			Status: nfsv4_xdr.NFS4ERR_TOOSMALL,
		}, res)
	})

	t.Run("JustBigEnough", func(t *testing.T) {
		// The same test as the one above, but with a maxcount
		// of 60 bytes. This should make the call succeed.
		rootDirectory.EXPECT().VirtualReadDir(
			ctx,
			uint64(0),
			virtual.AttributesMaskFileType|virtual.AttributesMaskInodeNumber,
			gomock.Any(),
		).DoAndReturn(func(ctx context.Context, firstCookie uint64, attributesMask virtual.AttributesMask, reporter virtual.DirectoryEntryReporter) virtual.Status {
			leaf := mock.NewMockVirtualLeaf(ctrl)
			require.True(t, reporter.ReportEntry(
				uint64(1),
				path.MustNewComponent("file"),
				virtual.DirectoryChild{}.FromLeaf(leaf),
				(&virtual.Attributes{}).
					SetFileType(filesystem.FileTypeRegularFile).
					SetInodeNumber(123)))
			return virtual.StatusOK
		})

		res, err := program.NfsV4Nfsproc4Compound(ctx, &nfsv4_xdr.Compound4args{
			Tag: "readdir",
			Argarray: []nfsv4_xdr.NfsArgop4{
				&nfsv4_xdr.NfsArgop4_OP_PUTROOTFH{},
				&nfsv4_xdr.NfsArgop4_OP_READDIR{
					Opreaddir: nfsv4_xdr.Readdir4args{
						Maxcount: 60,
						AttrRequest: nfsv4_xdr.Bitmap4{
							(1 << nfsv4_xdr.FATTR4_TYPE) |
								(1 << nfsv4_xdr.FATTR4_FILEID),
						},
					},
				},
			},
		})
		require.NoError(t, err)
		require.Equal(t, &nfsv4_xdr.Compound4res{
			Tag: "readdir",
			Resarray: []nfsv4_xdr.NfsResop4{
				&nfsv4_xdr.NfsResop4_OP_PUTROOTFH{
					Opputrootfh: nfsv4_xdr.Putrootfh4res{
						Status: nfsv4_xdr.NFS4_OK,
					},
				},
				&nfsv4_xdr.NfsResop4_OP_READDIR{
					Opreaddir: &nfsv4_xdr.Readdir4res_NFS4_OK{
						Resok4: nfsv4_xdr.Readdir4resok{
							Cookieverf: rebootVerifier,
							Reply: nfsv4_xdr.Dirlist4{
								Eof: true,
								Entries: &nfsv4_xdr.Entry4{
									Cookie: 3,
									Name:   "file",
									Attrs: nfsv4_xdr.Fattr4{
										Attrmask: nfsv4_xdr.Bitmap4{
											(1 << nfsv4_xdr.FATTR4_TYPE) |
												(1 << nfsv4_xdr.FATTR4_FILEID),
										},
										AttrVals: nfsv4_xdr.Attrlist4{
											// FATTR4_TYPE == NF4REG.
											0x00, 0x00, 0x00, 0x01,
											// FATTR4_FILEID == 123.
											0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x7b,
										},
									},
								},
							},
						},
					},
				},
			},
			Status: nfsv4_xdr.NFS4_OK,
		}, res)
	})

	t.Run("IncorrectVerifier", func(t *testing.T) {
		// Passing in a cookie rebootVerifier that doesn't match with
		// what was handed out previously should cause an
		// NFS4ERR_NOT_SAME error.
		res, err := program.NfsV4Nfsproc4Compound(ctx, &nfsv4_xdr.Compound4args{
			Tag: "readdir",
			Argarray: []nfsv4_xdr.NfsArgop4{
				&nfsv4_xdr.NfsArgop4_OP_PUTROOTFH{},
				&nfsv4_xdr.NfsArgop4_OP_READDIR{
					Opreaddir: nfsv4_xdr.Readdir4args{
						Cookie: 72,
						Cookieverf: nfsv4_xdr.Verifier4{
							0xb, 0xa, 0xd, 0xc, 0x00, 0xc, 0x1, 0xe,
						},
						Maxcount: 1000,
						AttrRequest: nfsv4_xdr.Bitmap4{
							(1 << nfsv4_xdr.FATTR4_TYPE) |
								(1 << nfsv4_xdr.FATTR4_FILEID),
						},
					},
				},
			},
		})
		require.NoError(t, err)
		require.Equal(t, &nfsv4_xdr.Compound4res{
			Tag: "readdir",
			Resarray: []nfsv4_xdr.NfsResop4{
				&nfsv4_xdr.NfsResop4_OP_PUTROOTFH{
					Opputrootfh: nfsv4_xdr.Putrootfh4res{
						Status: nfsv4_xdr.NFS4_OK,
					},
				},
				&nfsv4_xdr.NfsResop4_OP_READDIR{
					Opreaddir: &nfsv4_xdr.Readdir4res_default{
						Status: nfsv4_xdr.NFS4ERR_NOT_SAME,
					},
				},
			},
			Status: nfsv4_xdr.NFS4ERR_NOT_SAME,
		}, res)
	})
}

func TestBaseProgramCompound_OP_READLINK(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	rootDirectory := mock.NewMockVirtualDirectory(ctrl)
	rootDirectory.EXPECT().VirtualGetAttributes(gomock.Any(), virtual.AttributesMaskFileHandle, gomock.Any()).
		Do(func(ctx context.Context, requested virtual.AttributesMask, attributes *virtual.Attributes) {
			attributes.SetFileHandle([]byte{0xe7, 0x09, 0xea, 0x64, 0xd4, 0x5a, 0xf2, 0x87})
		})
	handleResolver := mock.NewMockHandleResolver(ctrl)
	randomNumberGenerator := mock.NewMockSingleThreadedGenerator(ctrl)
	rebootVerifier := nfsv4_xdr.Verifier4{0xa8, 0x90, 0x8c, 0x43, 0xb7, 0xd6, 0x0f, 0x74}
	stateIDOtherPrefix := [...]byte{0x46, 0x64, 0x44, 0x31}
	clock := mock.NewMockClock(ctrl)
	program := nfsv4.NewBaseProgram(rootDirectory, handleResolver.Call, randomNumberGenerator, rebootVerifier, stateIDOtherPrefix, clock, 2*time.Minute, time.Minute)

	t.Run("NoFileHandle", func(t *testing.T) {
		// Calling READLINK without a file handle should fail.
		res, err := program.NfsV4Nfsproc4Compound(ctx, &nfsv4_xdr.Compound4args{
			Tag: "readlink",
			Argarray: []nfsv4_xdr.NfsArgop4{
				&nfsv4_xdr.NfsArgop4_OP_READLINK{},
			},
		})
		require.NoError(t, err)
		require.Equal(t, &nfsv4_xdr.Compound4res{
			Tag: "readlink",
			Resarray: []nfsv4_xdr.NfsResop4{
				&nfsv4_xdr.NfsResop4_OP_READLINK{
					Opreadlink: &nfsv4_xdr.Readlink4res_default{
						Status: nfsv4_xdr.NFS4ERR_NOFILEHANDLE,
					},
				},
			},
			Status: nfsv4_xdr.NFS4ERR_NOFILEHANDLE,
		}, res)
	})

	t.Run("Directory", func(t *testing.T) {
		// Even though most file operations will return
		// NFS4ERR_ISDIR when called against a directory,
		// READLINK is required to return NFS4ERR_INVAL.
		res, err := program.NfsV4Nfsproc4Compound(ctx, &nfsv4_xdr.Compound4args{
			Tag: "readlink",
			Argarray: []nfsv4_xdr.NfsArgop4{
				&nfsv4_xdr.NfsArgop4_OP_PUTROOTFH{},
				&nfsv4_xdr.NfsArgop4_OP_READLINK{},
			},
		})
		require.NoError(t, err)
		require.Equal(t, &nfsv4_xdr.Compound4res{
			Tag: "readlink",
			Resarray: []nfsv4_xdr.NfsResop4{
				&nfsv4_xdr.NfsResop4_OP_PUTROOTFH{
					Opputrootfh: nfsv4_xdr.Putrootfh4res{
						Status: nfsv4_xdr.NFS4_OK,
					},
				},
				&nfsv4_xdr.NfsResop4_OP_READLINK{
					Opreadlink: &nfsv4_xdr.Readlink4res_default{
						Status: nfsv4_xdr.NFS4ERR_INVAL,
					},
				},
			},
			Status: nfsv4_xdr.NFS4ERR_INVAL,
		}, res)
	})

	t.Run("Failure", func(t *testing.T) {
		leaf := mock.NewMockVirtualLeaf(ctrl)
		clock.EXPECT().Now().Return(time.Unix(1000, 0))
		handleResolverExpectCall(t, handleResolver, []byte{1, 2, 3}, virtual.DirectoryChild{}.FromLeaf(leaf), virtual.StatusOK)
		leaf.EXPECT().VirtualReadlink(ctx).Return(nil, virtual.StatusErrIO)

		res, err := program.NfsV4Nfsproc4Compound(ctx, &nfsv4_xdr.Compound4args{
			Tag: "readlink",
			Argarray: []nfsv4_xdr.NfsArgop4{
				&nfsv4_xdr.NfsArgop4_OP_PUTFH{
					Opputfh: nfsv4_xdr.Putfh4args{
						Object: nfsv4_xdr.NfsFh4{1, 2, 3},
					},
				},
				&nfsv4_xdr.NfsArgop4_OP_READLINK{},
			},
		})
		require.NoError(t, err)
		require.Equal(t, &nfsv4_xdr.Compound4res{
			Tag: "readlink",
			Resarray: []nfsv4_xdr.NfsResop4{
				&nfsv4_xdr.NfsResop4_OP_PUTFH{
					Opputfh: nfsv4_xdr.Putfh4res{
						Status: nfsv4_xdr.NFS4_OK,
					},
				},
				&nfsv4_xdr.NfsResop4_OP_READLINK{
					Opreadlink: &nfsv4_xdr.Readlink4res_default{
						Status: nfsv4_xdr.NFS4ERR_IO,
					},
				},
			},
			Status: nfsv4_xdr.NFS4ERR_IO,
		}, res)
	})

	t.Run("Success", func(t *testing.T) {
		leaf := mock.NewMockVirtualLeaf(ctrl)
		clock.EXPECT().Now().Return(time.Unix(1001, 0))
		handleResolverExpectCall(t, handleResolver, []byte{4, 5, 6}, virtual.DirectoryChild{}.FromLeaf(leaf), virtual.StatusOK)
		leaf.EXPECT().VirtualReadlink(ctx).Return([]byte("target"), virtual.StatusOK)

		res, err := program.NfsV4Nfsproc4Compound(ctx, &nfsv4_xdr.Compound4args{
			Tag: "readlink",
			Argarray: []nfsv4_xdr.NfsArgop4{
				&nfsv4_xdr.NfsArgop4_OP_PUTFH{
					Opputfh: nfsv4_xdr.Putfh4args{
						Object: nfsv4_xdr.NfsFh4{4, 5, 6},
					},
				},
				&nfsv4_xdr.NfsArgop4_OP_READLINK{},
			},
		})
		require.NoError(t, err)
		require.Equal(t, &nfsv4_xdr.Compound4res{
			Tag: "readlink",
			Resarray: []nfsv4_xdr.NfsResop4{
				&nfsv4_xdr.NfsResop4_OP_PUTFH{
					Opputfh: nfsv4_xdr.Putfh4res{
						Status: nfsv4_xdr.NFS4_OK,
					},
				},
				&nfsv4_xdr.NfsResop4_OP_READLINK{
					Opreadlink: &nfsv4_xdr.Readlink4res_NFS4_OK{
						Resok4: nfsv4_xdr.Readlink4resok{
							Link: nfsv4_xdr.Linktext4("target"),
						},
					},
				},
			},
			Status: nfsv4_xdr.NFS4_OK,
		}, res)
	})
}

func TestBaseProgramCompound_OP_RELEASE_LOCKOWNER(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	rootDirectory := mock.NewMockVirtualDirectory(ctrl)
	rootDirectory.EXPECT().VirtualGetAttributes(gomock.Any(), virtual.AttributesMaskFileHandle, gomock.Any()).
		Do(func(ctx context.Context, requested virtual.AttributesMask, attributes *virtual.Attributes) {
			attributes.SetFileHandle([]byte{0x8e, 0x16, 0xec, 0x1a, 0x60, 0x6a, 0x9d, 0x3d})
		})
	handleResolver := mock.NewMockHandleResolver(ctrl)
	randomNumberGenerator := mock.NewMockSingleThreadedGenerator(ctrl)
	rebootVerifier := nfsv4_xdr.Verifier4{0x27, 0xe1, 0xcd, 0x6a, 0x3f, 0xf8, 0xb7, 0xb2}
	stateIDOtherPrefix := [...]byte{0xab, 0x4f, 0xf6, 0x1c}
	clock := mock.NewMockClock(ctrl)
	program := nfsv4.NewBaseProgram(rootDirectory, handleResolver.Call, randomNumberGenerator, rebootVerifier, stateIDOtherPrefix, clock, 2*time.Minute, time.Minute)

	t.Run("StaleClientID", func(t *testing.T) {
		// Calling RELEASE_LOCKOWNER against a non-existent
		// short client ID should result in failure.
		clock.EXPECT().Now().Return(time.Unix(1000, 0))

		res, err := program.NfsV4Nfsproc4Compound(ctx, &nfsv4_xdr.Compound4args{
			Tag: "release_lockowner",
			Argarray: []nfsv4_xdr.NfsArgop4{
				&nfsv4_xdr.NfsArgop4_OP_RELEASE_LOCKOWNER{
					OpreleaseLockowner: nfsv4_xdr.ReleaseLockowner4args{
						LockOwner: nfsv4_xdr.LockOwner4{
							Clientid: 0xf7fdfdc38f805b08,
							Owner:    []byte{0xac, 0xce, 0x68, 0x2f, 0x60, 0x36, 0x4f, 0xbf},
						},
					},
				},
			},
		})
		require.NoError(t, err)
		require.Equal(t, &nfsv4_xdr.Compound4res{
			Tag: "release_lockowner",
			Resarray: []nfsv4_xdr.NfsResop4{
				&nfsv4_xdr.NfsResop4_OP_RELEASE_LOCKOWNER{
					OpreleaseLockowner: nfsv4_xdr.ReleaseLockowner4res{
						Status: nfsv4_xdr.NFS4ERR_STALE_CLIENTID,
					},
				},
			},
			Status: nfsv4_xdr.NFS4ERR_STALE_CLIENTID,
		}, res)
	})

	// The remainder of the test assumes the availability of a client ID.
	clock.EXPECT().Now().Return(time.Unix(1001, 0))
	clock.EXPECT().Now().Return(time.Unix(1002, 0))
	setClientIDForTesting(ctx, t, randomNumberGenerator, program, 0xf7fdfdc38f805b08)

	t.Run("SuccessNoOp", func(t *testing.T) {
		// Now that a client ID has been allocated, the
		// RELEASE_LOCKOWNER call should succeed. Because we
		// haven't acquired any locks yet, it should still be a
		// no-op.
		clock.EXPECT().Now().Return(time.Unix(1003, 0))

		res, err := program.NfsV4Nfsproc4Compound(ctx, &nfsv4_xdr.Compound4args{
			Tag: "release_lockowner",
			Argarray: []nfsv4_xdr.NfsArgop4{
				&nfsv4_xdr.NfsArgop4_OP_RELEASE_LOCKOWNER{
					OpreleaseLockowner: nfsv4_xdr.ReleaseLockowner4args{
						LockOwner: nfsv4_xdr.LockOwner4{
							Clientid: 0xf7fdfdc38f805b08,
							Owner:    []byte{0xad, 0x75, 0x31, 0x9f, 0xe7, 0xef, 0x5a, 0x00},
						},
					},
				},
			},
		})
		require.NoError(t, err)
		require.Equal(t, &nfsv4_xdr.Compound4res{
			Tag: "release_lockowner",
			Resarray: []nfsv4_xdr.NfsResop4{
				&nfsv4_xdr.NfsResop4_OP_RELEASE_LOCKOWNER{
					OpreleaseLockowner: nfsv4_xdr.ReleaseLockowner4res{
						Status: nfsv4_xdr.NFS4_OK,
					},
				},
			},
			Status: nfsv4_xdr.NFS4_OK,
		}, res)
	})

	// Open a file and acquire a lock on it for the remainder of
	// this test.
	leaf := mock.NewMockVirtualLeaf(ctrl)
	clock.EXPECT().Now().Return(time.Unix(1004, 0))
	clock.EXPECT().Now().Return(time.Unix(1005, 0))
	openUnconfirmedFileForTesting(
		ctx,
		t,
		randomNumberGenerator,
		program,
		rootDirectory,
		leaf,
		nfsv4_xdr.NfsFh4{0xa7, 0x7b, 0xdf, 0xee, 0x60, 0xed, 0x37, 0x9d},
		/* shortClientID = */ 0xf7fdfdc38f805b08,
		/* seqID = */ 30430,
		/* stateIDOther = */ [...]byte{
			0xab, 0x4f, 0xf6, 0x1c,
			0x1e, 0x72, 0x2a, 0xe1,
			0x85, 0x8e, 0x31, 0x01,
		})
	clock.EXPECT().Now().Return(time.Unix(1006, 0))
	clock.EXPECT().Now().Return(time.Unix(1007, 0))
	openConfirmForTesting(
		ctx,
		t,
		randomNumberGenerator,
		program,
		nfsv4_xdr.NfsFh4{0xa7, 0x7b, 0xdf, 0xee, 0x60, 0xed, 0x37, 0x9d},
		/* seqID = */ 30431,
		/* stateIDOther = */ [...]byte{
			0xab, 0x4f, 0xf6, 0x1c,
			0x1e, 0x72, 0x2a, 0xe1,
			0x85, 0x8e, 0x31, 0x01,
		})

	clock.EXPECT().Now().Return(time.Unix(1008, 0))
	clock.EXPECT().Now().Return(time.Unix(1009, 0))
	randomNumberGeneratorExpectRead(randomNumberGenerator, []byte{0xe8, 0xef, 0xf4, 0x3d, 0x9b, 0x99, 0x0e, 0xf1})

	res, err := program.NfsV4Nfsproc4Compound(ctx, &nfsv4_xdr.Compound4args{
		Tag: "lock",
		Argarray: []nfsv4_xdr.NfsArgop4{
			&nfsv4_xdr.NfsArgop4_OP_PUTFH{
				Opputfh: nfsv4_xdr.Putfh4args{
					Object: nfsv4_xdr.NfsFh4{0xa7, 0x7b, 0xdf, 0xee, 0x60, 0xed, 0x37, 0x9d},
				},
			},
			&nfsv4_xdr.NfsArgop4_OP_LOCK{
				Oplock: nfsv4_xdr.Lock4args{
					Locktype: nfsv4_xdr.WRITE_LT,
					Reclaim:  false,
					Offset:   100,
					Length:   100,
					Locker: &nfsv4_xdr.Locker4_TRUE{
						OpenOwner: nfsv4_xdr.OpenToLockOwner4{
							OpenSeqid: 30432,
							OpenStateid: nfsv4_xdr.Stateid4{
								Seqid: 2,
								Other: [...]byte{
									0xab, 0x4f, 0xf6, 0x1c,
									0x1e, 0x72, 0x2a, 0xe1,
									0x85, 0x8e, 0x31, 0x01,
								},
							},
							LockSeqid: 16946,
							LockOwner: nfsv4_xdr.LockOwner4{
								Clientid: 0xf7fdfdc38f805b08,
								Owner:    []byte{0xad, 0x75, 0x31, 0x9f, 0xe7, 0xef, 0x5a, 0x00},
							},
						},
					},
				},
			},
		},
	})
	require.NoError(t, err)
	require.Equal(t, &nfsv4_xdr.Compound4res{
		Tag: "lock",
		Resarray: []nfsv4_xdr.NfsResop4{
			&nfsv4_xdr.NfsResop4_OP_PUTFH{
				Opputfh: nfsv4_xdr.Putfh4res{
					Status: nfsv4_xdr.NFS4_OK,
				},
			},
			&nfsv4_xdr.NfsResop4_OP_LOCK{
				Oplock: &nfsv4_xdr.Lock4res_NFS4_OK{
					Resok4: nfsv4_xdr.Lock4resok{
						LockStateid: nfsv4_xdr.Stateid4{
							Seqid: 1,
							Other: [...]byte{
								0xab, 0x4f, 0xf6, 0x1c,
								0xe8, 0xef, 0xf4, 0x3d,
								0x9b, 0x99, 0x0e, 0xf1,
							},
						},
					},
				},
			},
		},
		Status: nfsv4_xdr.NFS4_OK,
	}, res)

	t.Run("LocksHeld", func(t *testing.T) {
		// Now that this lock-owner holds one or more locks,
		// RELEASE_LOCKOWNER can no longer be called.
		clock.EXPECT().Now().Return(time.Unix(1010, 0))

		res, err := program.NfsV4Nfsproc4Compound(ctx, &nfsv4_xdr.Compound4args{
			Tag: "release_lockowner",
			Argarray: []nfsv4_xdr.NfsArgop4{
				&nfsv4_xdr.NfsArgop4_OP_RELEASE_LOCKOWNER{
					OpreleaseLockowner: nfsv4_xdr.ReleaseLockowner4args{
						LockOwner: nfsv4_xdr.LockOwner4{
							Clientid: 0xf7fdfdc38f805b08,
							Owner:    []byte{0xad, 0x75, 0x31, 0x9f, 0xe7, 0xef, 0x5a, 0x00},
						},
					},
				},
			},
		})
		require.NoError(t, err)
		require.Equal(t, &nfsv4_xdr.Compound4res{
			Tag: "release_lockowner",
			Resarray: []nfsv4_xdr.NfsResop4{
				&nfsv4_xdr.NfsResop4_OP_RELEASE_LOCKOWNER{
					OpreleaseLockowner: nfsv4_xdr.ReleaseLockowner4res{
						Status: nfsv4_xdr.NFS4ERR_LOCKS_HELD,
					},
				},
			},
			Status: nfsv4_xdr.NFS4ERR_LOCKS_HELD,
		}, res)
	})

	// Drop the lock.
	clock.EXPECT().Now().Return(time.Unix(1011, 0))
	clock.EXPECT().Now().Return(time.Unix(1012, 0))

	res, err = program.NfsV4Nfsproc4Compound(ctx, &nfsv4_xdr.Compound4args{
		Tag: "unlock",
		Argarray: []nfsv4_xdr.NfsArgop4{
			&nfsv4_xdr.NfsArgop4_OP_PUTFH{
				Opputfh: nfsv4_xdr.Putfh4args{
					Object: nfsv4_xdr.NfsFh4{0xa7, 0x7b, 0xdf, 0xee, 0x60, 0xed, 0x37, 0x9d},
				},
			},
			&nfsv4_xdr.NfsArgop4_OP_LOCKU{
				Oplocku: nfsv4_xdr.Locku4args{
					Locktype: nfsv4_xdr.WRITE_LT,
					Seqid:    16947,
					LockStateid: nfsv4_xdr.Stateid4{
						Seqid: 1,
						Other: [...]byte{
							0xab, 0x4f, 0xf6, 0x1c,
							0xe8, 0xef, 0xf4, 0x3d,
							0x9b, 0x99, 0x0e, 0xf1,
						},
					},
					Offset: 50,
					Length: 200,
				},
			},
		},
	})
	require.NoError(t, err)
	require.Equal(t, &nfsv4_xdr.Compound4res{
		Tag: "unlock",
		Resarray: []nfsv4_xdr.NfsResop4{
			&nfsv4_xdr.NfsResop4_OP_PUTFH{
				Opputfh: nfsv4_xdr.Putfh4res{
					Status: nfsv4_xdr.NFS4_OK,
				},
			},
			&nfsv4_xdr.NfsResop4_OP_LOCKU{
				Oplocku: &nfsv4_xdr.Locku4res_NFS4_OK{
					LockStateid: nfsv4_xdr.Stateid4{
						Seqid: 2,
						Other: [...]byte{
							0xab, 0x4f, 0xf6, 0x1c,
							0xe8, 0xef, 0xf4, 0x3d,
							0x9b, 0x99, 0x0e, 0xf1,
						},
					},
				},
			},
		},
		Status: nfsv4_xdr.NFS4_OK,
	}, res)

	t.Run("SuccessAfterUnlock", func(t *testing.T) {
		// Now that the file has been unlocked, it should be
		// possible to call RELEASE_LOCKOWNER once again.
		clock.EXPECT().Now().Return(time.Unix(1013, 0))

		res, err := program.NfsV4Nfsproc4Compound(ctx, &nfsv4_xdr.Compound4args{
			Tag: "release_lockowner",
			Argarray: []nfsv4_xdr.NfsArgop4{
				&nfsv4_xdr.NfsArgop4_OP_RELEASE_LOCKOWNER{
					OpreleaseLockowner: nfsv4_xdr.ReleaseLockowner4args{
						LockOwner: nfsv4_xdr.LockOwner4{
							Clientid: 0xf7fdfdc38f805b08,
							Owner:    []byte{0xad, 0x75, 0x31, 0x9f, 0xe7, 0xef, 0x5a, 0x00},
						},
					},
				},
			},
		})
		require.NoError(t, err)
		require.Equal(t, &nfsv4_xdr.Compound4res{
			Tag: "release_lockowner",
			Resarray: []nfsv4_xdr.NfsResop4{
				&nfsv4_xdr.NfsResop4_OP_RELEASE_LOCKOWNER{
					OpreleaseLockowner: nfsv4_xdr.ReleaseLockowner4res{
						Status: nfsv4_xdr.NFS4_OK,
					},
				},
			},
			Status: nfsv4_xdr.NFS4_OK,
		}, res)

		// As a consequence, future LOCK operations for the
		// lock-owner should fail, as long as no
		// open_to_lock_owner4 is provided.
		clock.EXPECT().Now().Return(time.Unix(1014, 0))
		clock.EXPECT().Now().Return(time.Unix(1015, 0))

		res, err = program.NfsV4Nfsproc4Compound(ctx, &nfsv4_xdr.Compound4args{
			Tag: "lock",
			Argarray: []nfsv4_xdr.NfsArgop4{
				&nfsv4_xdr.NfsArgop4_OP_PUTFH{
					Opputfh: nfsv4_xdr.Putfh4args{
						Object: nfsv4_xdr.NfsFh4{0xa7, 0x7b, 0xdf, 0xee, 0x60, 0xed, 0x37, 0x9d},
					},
				},
				&nfsv4_xdr.NfsArgop4_OP_LOCK{
					Oplock: nfsv4_xdr.Lock4args{
						Locktype: nfsv4_xdr.WRITE_LT,
						Reclaim:  false,
						Offset:   100,
						Length:   100,
						Locker: &nfsv4_xdr.Locker4_FALSE{
							LockOwner: nfsv4_xdr.ExistLockOwner4{
								LockSeqid: 16947,
								LockStateid: nfsv4_xdr.Stateid4{
									Seqid: 2,
									Other: [...]byte{
										0xab, 0x4f, 0xf6, 0x1c,
										0xe8, 0xef, 0xf4, 0x3d,
										0x9b, 0x99, 0x0e, 0xf1,
									},
								},
							},
						},
					},
				},
			},
		})
		require.NoError(t, err)
		require.Equal(t, &nfsv4_xdr.Compound4res{
			Tag: "lock",
			Resarray: []nfsv4_xdr.NfsResop4{
				&nfsv4_xdr.NfsResop4_OP_PUTFH{
					Opputfh: nfsv4_xdr.Putfh4res{
						Status: nfsv4_xdr.NFS4_OK,
					},
				},
				&nfsv4_xdr.NfsResop4_OP_LOCK{
					Oplock: &nfsv4_xdr.Lock4res_default{
						Status: nfsv4_xdr.NFS4ERR_BAD_STATEID,
					},
				},
			},
			Status: nfsv4_xdr.NFS4ERR_BAD_STATEID,
		}, res)
	})
}

func TestBaseProgramCompound_OP_REMOVE(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	rootDirectory := mock.NewMockVirtualDirectory(ctrl)
	rootDirectory.EXPECT().VirtualGetAttributes(gomock.Any(), virtual.AttributesMaskFileHandle, gomock.Any()).
		Do(func(ctx context.Context, requested virtual.AttributesMask, attributes *virtual.Attributes) {
			attributes.SetFileHandle([]byte{0xe3, 0x85, 0x4a, 0x60, 0x0d, 0xaf, 0x14, 0x20})
		})
	handleResolver := mock.NewMockHandleResolver(ctrl)
	randomNumberGenerator := mock.NewMockSingleThreadedGenerator(ctrl)
	rebootVerifier := nfsv4_xdr.Verifier4{0xe7, 0x77, 0x33, 0xf4, 0x21, 0xad, 0x7a, 0x1b}
	stateIDOtherPrefix := [...]byte{0x4b, 0x46, 0x62, 0x3c}
	clock := mock.NewMockClock(ctrl)
	program := nfsv4.NewBaseProgram(rootDirectory, handleResolver.Call, randomNumberGenerator, rebootVerifier, stateIDOtherPrefix, clock, 2*time.Minute, time.Minute)

	t.Run("NoFileHandle", func(t *testing.T) {
		// Calling REMOVE without a file handle should fail.
		res, err := program.NfsV4Nfsproc4Compound(ctx, &nfsv4_xdr.Compound4args{
			Tag: "unlink",
			Argarray: []nfsv4_xdr.NfsArgop4{
				&nfsv4_xdr.NfsArgop4_OP_REMOVE{
					Opremove: nfsv4_xdr.Remove4args{
						Target: "file",
					},
				},
			},
		})
		require.NoError(t, err)
		require.Equal(t, &nfsv4_xdr.Compound4res{
			Tag: "unlink",
			Resarray: []nfsv4_xdr.NfsResop4{
				&nfsv4_xdr.NfsResop4_OP_REMOVE{
					Opremove: &nfsv4_xdr.Remove4res_default{
						Status: nfsv4_xdr.NFS4ERR_NOFILEHANDLE,
					},
				},
			},
			Status: nfsv4_xdr.NFS4ERR_NOFILEHANDLE,
		}, res)
	})

	t.Run("NotDirectory", func(t *testing.T) {
		leaf := mock.NewMockVirtualLeaf(ctrl)
		clock.EXPECT().Now().Return(time.Unix(1000, 0))
		handleResolverExpectCall(t, handleResolver, []byte{1, 2, 3}, virtual.DirectoryChild{}.FromLeaf(leaf), virtual.StatusOK)

		res, err := program.NfsV4Nfsproc4Compound(ctx, &nfsv4_xdr.Compound4args{
			Tag: "unlink",
			Argarray: []nfsv4_xdr.NfsArgop4{
				&nfsv4_xdr.NfsArgop4_OP_PUTFH{
					Opputfh: nfsv4_xdr.Putfh4args{
						Object: nfsv4_xdr.NfsFh4{1, 2, 3},
					},
				},
				&nfsv4_xdr.NfsArgop4_OP_REMOVE{
					Opremove: nfsv4_xdr.Remove4args{
						Target: "file",
					},
				},
			},
		})
		require.NoError(t, err)
		require.Equal(t, &nfsv4_xdr.Compound4res{
			Tag: "unlink",
			Resarray: []nfsv4_xdr.NfsResop4{
				&nfsv4_xdr.NfsResop4_OP_PUTFH{
					Opputfh: nfsv4_xdr.Putfh4res{
						Status: nfsv4_xdr.NFS4_OK,
					},
				},
				&nfsv4_xdr.NfsResop4_OP_REMOVE{
					Opremove: &nfsv4_xdr.Remove4res_default{
						Status: nfsv4_xdr.NFS4ERR_NOTDIR,
					},
				},
			},
			Status: nfsv4_xdr.NFS4ERR_NOTDIR,
		}, res)
	})

	t.Run("BadName", func(t *testing.T) {
		res, err := program.NfsV4Nfsproc4Compound(ctx, &nfsv4_xdr.Compound4args{
			Tag: "unlink",
			Argarray: []nfsv4_xdr.NfsArgop4{
				&nfsv4_xdr.NfsArgop4_OP_PUTROOTFH{},
				&nfsv4_xdr.NfsArgop4_OP_REMOVE{
					Opremove: nfsv4_xdr.Remove4args{
						Target: "..",
					},
				},
			},
		})
		require.NoError(t, err)
		require.Equal(t, &nfsv4_xdr.Compound4res{
			Tag: "unlink",
			Resarray: []nfsv4_xdr.NfsResop4{
				&nfsv4_xdr.NfsResop4_OP_PUTROOTFH{
					Opputrootfh: nfsv4_xdr.Putrootfh4res{
						Status: nfsv4_xdr.NFS4_OK,
					},
				},
				&nfsv4_xdr.NfsResop4_OP_REMOVE{
					Opremove: &nfsv4_xdr.Remove4res_default{
						Status: nfsv4_xdr.NFS4ERR_BADNAME,
					},
				},
			},
			Status: nfsv4_xdr.NFS4ERR_BADNAME,
		}, res)
	})

	t.Run("MissingName", func(t *testing.T) {
		res, err := program.NfsV4Nfsproc4Compound(ctx, &nfsv4_xdr.Compound4args{
			Tag: "unlink",
			Argarray: []nfsv4_xdr.NfsArgop4{
				&nfsv4_xdr.NfsArgop4_OP_PUTROOTFH{},
				&nfsv4_xdr.NfsArgop4_OP_REMOVE{},
			},
		})
		require.NoError(t, err)
		require.Equal(t, &nfsv4_xdr.Compound4res{
			Tag: "unlink",
			Resarray: []nfsv4_xdr.NfsResop4{
				&nfsv4_xdr.NfsResop4_OP_PUTROOTFH{
					Opputrootfh: nfsv4_xdr.Putrootfh4res{
						Status: nfsv4_xdr.NFS4_OK,
					},
				},
				&nfsv4_xdr.NfsResop4_OP_REMOVE{
					Opremove: &nfsv4_xdr.Remove4res_default{
						Status: nfsv4_xdr.NFS4ERR_INVAL,
					},
				},
			},
			Status: nfsv4_xdr.NFS4ERR_INVAL,
		}, res)
	})

	t.Run("Failure", func(t *testing.T) {
		rootDirectory.EXPECT().VirtualRemove(
			path.MustNewComponent("file"),
			true,
			true,
		).Return(virtual.ChangeInfo{}, virtual.StatusErrAccess)

		res, err := program.NfsV4Nfsproc4Compound(ctx, &nfsv4_xdr.Compound4args{
			Tag: "unlink",
			Argarray: []nfsv4_xdr.NfsArgop4{
				&nfsv4_xdr.NfsArgop4_OP_PUTROOTFH{},
				&nfsv4_xdr.NfsArgop4_OP_REMOVE{
					Opremove: nfsv4_xdr.Remove4args{
						Target: "file",
					},
				},
			},
		})
		require.NoError(t, err)
		require.Equal(t, &nfsv4_xdr.Compound4res{
			Tag: "unlink",
			Resarray: []nfsv4_xdr.NfsResop4{
				&nfsv4_xdr.NfsResop4_OP_PUTROOTFH{
					Opputrootfh: nfsv4_xdr.Putrootfh4res{
						Status: nfsv4_xdr.NFS4_OK,
					},
				},
				&nfsv4_xdr.NfsResop4_OP_REMOVE{
					Opremove: &nfsv4_xdr.Remove4res_default{
						Status: nfsv4_xdr.NFS4ERR_ACCESS,
					},
				},
			},
			Status: nfsv4_xdr.NFS4ERR_ACCESS,
		}, res)
	})

	t.Run("Success", func(t *testing.T) {
		rootDirectory.EXPECT().VirtualRemove(
			path.MustNewComponent("file"),
			true,
			true,
		).Return(virtual.ChangeInfo{
			Before: 0x65821b4665becdc0,
			After:  0x9c6360fa70cc3aea,
		}, virtual.StatusOK)

		res, err := program.NfsV4Nfsproc4Compound(ctx, &nfsv4_xdr.Compound4args{
			Tag: "unlink",
			Argarray: []nfsv4_xdr.NfsArgop4{
				&nfsv4_xdr.NfsArgop4_OP_PUTROOTFH{},
				&nfsv4_xdr.NfsArgop4_OP_REMOVE{
					Opremove: nfsv4_xdr.Remove4args{
						Target: "file",
					},
				},
			},
		})
		require.NoError(t, err)
		require.Equal(t, &nfsv4_xdr.Compound4res{
			Tag: "unlink",
			Resarray: []nfsv4_xdr.NfsResop4{
				&nfsv4_xdr.NfsResop4_OP_PUTROOTFH{
					Opputrootfh: nfsv4_xdr.Putrootfh4res{
						Status: nfsv4_xdr.NFS4_OK,
					},
				},
				&nfsv4_xdr.NfsResop4_OP_REMOVE{
					Opremove: &nfsv4_xdr.Remove4res_NFS4_OK{
						Resok4: nfsv4_xdr.Remove4resok{
							Cinfo: nfsv4_xdr.ChangeInfo4{
								Atomic: true,
								Before: 0x65821b4665becdc0,
								After:  0x9c6360fa70cc3aea,
							},
						},
					},
				},
			},
			Status: nfsv4_xdr.NFS4_OK,
		}, res)
	})
}

// TODO: RENAME
// TODO: RENEW

func TestBaseProgramCompound_OP_RESTOREFH(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	rootDirectory := mock.NewMockVirtualDirectory(ctrl)
	rootDirectory.EXPECT().VirtualGetAttributes(gomock.Any(), virtual.AttributesMaskFileHandle, gomock.Any()).
		Do(func(ctx context.Context, requested virtual.AttributesMask, attributes *virtual.Attributes) {
			attributes.SetFileHandle([]byte{0x16, 0xb9, 0x45, 0x1d, 0x06, 0x85, 0xc4, 0xbb})
		})
	handleResolver := mock.NewMockHandleResolver(ctrl)
	randomNumberGenerator := mock.NewMockSingleThreadedGenerator(ctrl)
	rebootVerifier := nfsv4_xdr.Verifier4{0x5f, 0x98, 0x5c, 0xdf, 0x8a, 0xac, 0x4d, 0x97}
	stateIDOtherPrefix := [...]byte{0xd4, 0x7c, 0xd1, 0x8f}
	clock := mock.NewMockClock(ctrl)
	program := nfsv4.NewBaseProgram(rootDirectory, handleResolver.Call, randomNumberGenerator, rebootVerifier, stateIDOtherPrefix, clock, 2*time.Minute, time.Minute)

	t.Run("NoSavedFileHandle", func(t *testing.T) {
		// Calling RESTOREFH without a saved file handle should fail.
		res, err := program.NfsV4Nfsproc4Compound(ctx, &nfsv4_xdr.Compound4args{
			Tag: "restorefh",
			Argarray: []nfsv4_xdr.NfsArgop4{
				&nfsv4_xdr.NfsArgop4_OP_RESTOREFH{},
			},
		})
		require.NoError(t, err)
		require.Equal(t, &nfsv4_xdr.Compound4res{
			Tag: "restorefh",
			Resarray: []nfsv4_xdr.NfsResop4{
				&nfsv4_xdr.NfsResop4_OP_RESTOREFH{
					Oprestorefh: nfsv4_xdr.Restorefh4res{
						Status: nfsv4_xdr.NFS4ERR_RESTOREFH,
					},
				},
			},
			Status: nfsv4_xdr.NFS4ERR_RESTOREFH,
		}, res)
	})

	t.Run("Success", func(t *testing.T) {
		// RESTOREFH should restore the file that was saved
		// previously. The current file handle for successive
		// operations should apply to that file instead.
		leaf := mock.NewMockVirtualLeaf(ctrl)
		clock.EXPECT().Now().Return(time.Unix(1000, 0))
		handleResolverExpectCall(t, handleResolver, []byte{1, 2, 3}, virtual.DirectoryChild{}.FromLeaf(leaf), virtual.StatusOK)

		res, err := program.NfsV4Nfsproc4Compound(ctx, &nfsv4_xdr.Compound4args{
			Tag: "restorefh",
			Argarray: []nfsv4_xdr.NfsArgop4{
				&nfsv4_xdr.NfsArgop4_OP_PUTFH{
					Opputfh: nfsv4_xdr.Putfh4args{
						Object: nfsv4_xdr.NfsFh4{1, 2, 3},
					},
				},
				&nfsv4_xdr.NfsArgop4_OP_SAVEFH{},
				&nfsv4_xdr.NfsArgop4_OP_GETFH{},
				&nfsv4_xdr.NfsArgop4_OP_PUTROOTFH{},
				&nfsv4_xdr.NfsArgop4_OP_RESTOREFH{},
				&nfsv4_xdr.NfsArgop4_OP_GETFH{},
			},
		})
		require.NoError(t, err)
		require.Equal(t, &nfsv4_xdr.Compound4res{
			Tag: "restorefh",
			Resarray: []nfsv4_xdr.NfsResop4{
				&nfsv4_xdr.NfsResop4_OP_PUTFH{
					Opputfh: nfsv4_xdr.Putfh4res{
						Status: nfsv4_xdr.NFS4_OK,
					},
				},
				&nfsv4_xdr.NfsResop4_OP_SAVEFH{
					Opsavefh: nfsv4_xdr.Savefh4res{
						Status: nfsv4_xdr.NFS4_OK,
					},
				},
				&nfsv4_xdr.NfsResop4_OP_GETFH{
					Opgetfh: &nfsv4_xdr.Getfh4res_NFS4_OK{
						Resok4: nfsv4_xdr.Getfh4resok{
							Object: nfsv4_xdr.NfsFh4{1, 2, 3},
						},
					},
				},
				&nfsv4_xdr.NfsResop4_OP_PUTROOTFH{
					Opputrootfh: nfsv4_xdr.Putrootfh4res{
						Status: nfsv4_xdr.NFS4_OK,
					},
				},
				&nfsv4_xdr.NfsResop4_OP_RESTOREFH{
					Oprestorefh: nfsv4_xdr.Restorefh4res{
						Status: nfsv4_xdr.NFS4_OK,
					},
				},
				&nfsv4_xdr.NfsResop4_OP_GETFH{
					Opgetfh: &nfsv4_xdr.Getfh4res_NFS4_OK{
						Resok4: nfsv4_xdr.Getfh4resok{
							Object: nfsv4_xdr.NfsFh4{1, 2, 3},
						},
					},
				},
			},
			Status: nfsv4_xdr.NFS4_OK,
		}, res)
	})
}

func TestBaseProgramCompound_OP_SAVEFH(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	rootDirectory := mock.NewMockVirtualDirectory(ctrl)
	rootDirectory.EXPECT().VirtualGetAttributes(gomock.Any(), virtual.AttributesMaskFileHandle, gomock.Any()).
		Do(func(ctx context.Context, requested virtual.AttributesMask, attributes *virtual.Attributes) {
			attributes.SetFileHandle([]byte{0xc4, 0x2b, 0x0e, 0x04, 0xde, 0x15, 0x66, 0x77})
		})
	handleResolver := mock.NewMockHandleResolver(ctrl)
	randomNumberGenerator := mock.NewMockSingleThreadedGenerator(ctrl)
	rebootVerifier := nfsv4_xdr.Verifier4{0xe9, 0xf5, 0x40, 0xa0, 0x20, 0xd9, 0x2c, 0x52}
	stateIDOtherPrefix := [...]byte{0xf1, 0xd0, 0x0e, 0xa0}
	clock := mock.NewMockClock(ctrl)
	program := nfsv4.NewBaseProgram(rootDirectory, handleResolver.Call, randomNumberGenerator, rebootVerifier, stateIDOtherPrefix, clock, 2*time.Minute, time.Minute)

	t.Run("NoFileHandle", func(t *testing.T) {
		// Calling SAVEFH without a file handle should fail.
		res, err := program.NfsV4Nfsproc4Compound(ctx, &nfsv4_xdr.Compound4args{
			Tag: "savefh",
			Argarray: []nfsv4_xdr.NfsArgop4{
				&nfsv4_xdr.NfsArgop4_OP_SAVEFH{},
			},
		})
		require.NoError(t, err)
		require.Equal(t, &nfsv4_xdr.Compound4res{
			Tag: "savefh",
			Resarray: []nfsv4_xdr.NfsResop4{
				&nfsv4_xdr.NfsResop4_OP_SAVEFH{
					Opsavefh: nfsv4_xdr.Savefh4res{
						Status: nfsv4_xdr.NFS4ERR_NOFILEHANDLE,
					},
				},
			},
			Status: nfsv4_xdr.NFS4ERR_NOFILEHANDLE,
		}, res)
	})

	// The success case is tested as part of OP_RESTOREFH.
}

func TestBaseProgramCompound_OP_SECINFO(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	rootDirectory := mock.NewMockVirtualDirectory(ctrl)
	rootDirectory.EXPECT().VirtualGetAttributes(gomock.Any(), virtual.AttributesMaskFileHandle, gomock.Any()).
		Do(func(ctx context.Context, requested virtual.AttributesMask, attributes *virtual.Attributes) {
			attributes.SetFileHandle([]byte{0x0a, 0xa2, 0x92, 0x2f, 0x06, 0x66, 0xd8, 0x80})
		})
	handleResolver := mock.NewMockHandleResolver(ctrl)
	randomNumberGenerator := mock.NewMockSingleThreadedGenerator(ctrl)
	rebootVerifier := nfsv4_xdr.Verifier4{0x70, 0x34, 0xc6, 0x7a, 0x25, 0x6e, 0x08, 0xc0}
	stateIDOtherPrefix := [...]byte{0xf9, 0x44, 0xa6, 0x25}
	clock := mock.NewMockClock(ctrl)
	program := nfsv4.NewBaseProgram(rootDirectory, handleResolver.Call, randomNumberGenerator, rebootVerifier, stateIDOtherPrefix, clock, 2*time.Minute, time.Minute)

	t.Run("NoFileHandle", func(t *testing.T) {
		// Calling SECINFO without a file handle should fail.
		res, err := program.NfsV4Nfsproc4Compound(ctx, &nfsv4_xdr.Compound4args{
			Tag: "secinfo",
			Argarray: []nfsv4_xdr.NfsArgop4{
				&nfsv4_xdr.NfsArgop4_OP_SECINFO{
					Opsecinfo: nfsv4_xdr.Secinfo4args{
						Name: "Hello",
					},
				},
			},
		})
		require.NoError(t, err)
		require.Equal(t, &nfsv4_xdr.Compound4res{
			Tag: "secinfo",
			Resarray: []nfsv4_xdr.NfsResop4{
				&nfsv4_xdr.NfsResop4_OP_SECINFO{
					Opsecinfo: &nfsv4_xdr.Secinfo4res_default{
						Status: nfsv4_xdr.NFS4ERR_NOFILEHANDLE,
					},
				},
			},
			Status: nfsv4_xdr.NFS4ERR_NOFILEHANDLE,
		}, res)
	})

	t.Run("NotDirectory", func(t *testing.T) {
		// Even though LOOKUP may return NFS4ERR_SYMLINK when
		// called against a symbolic link, SECINFO has no such
		// requirement. It should always return NFS4ERR_NOTDIR.
		leaf := mock.NewMockVirtualLeaf(ctrl)
		clock.EXPECT().Now().Return(time.Unix(1000, 0))
		handleResolverExpectCall(t, handleResolver, []byte{1, 2, 3}, virtual.DirectoryChild{}.FromLeaf(leaf), virtual.StatusOK)

		res, err := program.NfsV4Nfsproc4Compound(ctx, &nfsv4_xdr.Compound4args{
			Tag: "secinfo",
			Argarray: []nfsv4_xdr.NfsArgop4{
				&nfsv4_xdr.NfsArgop4_OP_PUTFH{
					Opputfh: nfsv4_xdr.Putfh4args{
						Object: nfsv4_xdr.NfsFh4{1, 2, 3},
					},
				},
				&nfsv4_xdr.NfsArgop4_OP_SECINFO{
					Opsecinfo: nfsv4_xdr.Secinfo4args{
						Name: "Hello",
					},
				},
			},
		})
		require.NoError(t, err)
		require.Equal(t, &nfsv4_xdr.Compound4res{
			Tag: "secinfo",
			Resarray: []nfsv4_xdr.NfsResop4{
				&nfsv4_xdr.NfsResop4_OP_PUTFH{
					Opputfh: nfsv4_xdr.Putfh4res{
						Status: nfsv4_xdr.NFS4_OK,
					},
				},
				&nfsv4_xdr.NfsResop4_OP_SECINFO{
					Opsecinfo: &nfsv4_xdr.Secinfo4res_default{
						Status: nfsv4_xdr.NFS4ERR_NOTDIR,
					},
				},
			},
			Status: nfsv4_xdr.NFS4ERR_NOTDIR,
		}, res)
	})

	t.Run("BadName", func(t *testing.T) {
		res, err := program.NfsV4Nfsproc4Compound(ctx, &nfsv4_xdr.Compound4args{
			Tag: "secinfo",
			Argarray: []nfsv4_xdr.NfsArgop4{
				&nfsv4_xdr.NfsArgop4_OP_PUTROOTFH{},
				&nfsv4_xdr.NfsArgop4_OP_SECINFO{
					Opsecinfo: nfsv4_xdr.Secinfo4args{
						Name: "..",
					},
				},
			},
		})
		require.NoError(t, err)
		require.Equal(t, &nfsv4_xdr.Compound4res{
			Tag: "secinfo",
			Resarray: []nfsv4_xdr.NfsResop4{
				&nfsv4_xdr.NfsResop4_OP_PUTROOTFH{
					Opputrootfh: nfsv4_xdr.Putrootfh4res{
						Status: nfsv4_xdr.NFS4_OK,
					},
				},
				&nfsv4_xdr.NfsResop4_OP_SECINFO{
					Opsecinfo: &nfsv4_xdr.Secinfo4res_default{
						Status: nfsv4_xdr.NFS4ERR_BADNAME,
					},
				},
			},
			Status: nfsv4_xdr.NFS4ERR_BADNAME,
		}, res)
	})

	t.Run("MissingName", func(t *testing.T) {
		res, err := program.NfsV4Nfsproc4Compound(ctx, &nfsv4_xdr.Compound4args{
			Tag: "secinfo",
			Argarray: []nfsv4_xdr.NfsArgop4{
				&nfsv4_xdr.NfsArgop4_OP_PUTROOTFH{},
				&nfsv4_xdr.NfsArgop4_OP_SECINFO{},
			},
		})
		require.NoError(t, err)
		require.Equal(t, &nfsv4_xdr.Compound4res{
			Tag: "secinfo",
			Resarray: []nfsv4_xdr.NfsResop4{
				&nfsv4_xdr.NfsResop4_OP_PUTROOTFH{
					Opputrootfh: nfsv4_xdr.Putrootfh4res{
						Status: nfsv4_xdr.NFS4_OK,
					},
				},
				&nfsv4_xdr.NfsResop4_OP_SECINFO{
					Opsecinfo: &nfsv4_xdr.Secinfo4res_default{
						Status: nfsv4_xdr.NFS4ERR_INVAL,
					},
				},
			},
			Status: nfsv4_xdr.NFS4ERR_INVAL,
		}, res)
	})

	t.Run("NotFound", func(t *testing.T) {
		rootDirectory.EXPECT().VirtualLookup(
			gomock.Any(),
			path.MustNewComponent("Hello"),
			virtual.AttributesMask(0),
			gomock.Any(),
		).Return(virtual.DirectoryChild{}, virtual.StatusErrNoEnt)

		res, err := program.NfsV4Nfsproc4Compound(ctx, &nfsv4_xdr.Compound4args{
			Tag: "secinfo",
			Argarray: []nfsv4_xdr.NfsArgop4{
				&nfsv4_xdr.NfsArgop4_OP_PUTROOTFH{},
				&nfsv4_xdr.NfsArgop4_OP_SECINFO{
					Opsecinfo: nfsv4_xdr.Secinfo4args{
						Name: "Hello",
					},
				},
			},
		})
		require.NoError(t, err)
		require.Equal(t, &nfsv4_xdr.Compound4res{
			Tag: "secinfo",
			Resarray: []nfsv4_xdr.NfsResop4{
				&nfsv4_xdr.NfsResop4_OP_PUTROOTFH{
					Opputrootfh: nfsv4_xdr.Putrootfh4res{
						Status: nfsv4_xdr.NFS4_OK,
					},
				},
				&nfsv4_xdr.NfsResop4_OP_SECINFO{
					Opsecinfo: &nfsv4_xdr.Secinfo4res_default{
						Status: nfsv4_xdr.NFS4ERR_NOENT,
					},
				},
			},
			Status: nfsv4_xdr.NFS4ERR_NOENT,
		}, res)
	})

	t.Run("Success", func(t *testing.T) {
		leaf := mock.NewMockNativeLeaf(ctrl)
		rootDirectory.EXPECT().VirtualLookup(
			gomock.Any(),
			path.MustNewComponent("Hello"),
			virtual.AttributesMask(0),
			gomock.Any(),
		).Return(virtual.DirectoryChild{}.FromLeaf(leaf), virtual.StatusOK)

		res, err := program.NfsV4Nfsproc4Compound(ctx, &nfsv4_xdr.Compound4args{
			Tag: "secinfo",
			Argarray: []nfsv4_xdr.NfsArgop4{
				&nfsv4_xdr.NfsArgop4_OP_PUTROOTFH{},
				&nfsv4_xdr.NfsArgop4_OP_SECINFO{
					Opsecinfo: nfsv4_xdr.Secinfo4args{
						Name: "Hello",
					},
				},
			},
		})
		require.NoError(t, err)
		require.Equal(t, &nfsv4_xdr.Compound4res{
			Tag: "secinfo",
			Resarray: []nfsv4_xdr.NfsResop4{
				&nfsv4_xdr.NfsResop4_OP_PUTROOTFH{
					Opputrootfh: nfsv4_xdr.Putrootfh4res{
						Status: nfsv4_xdr.NFS4_OK,
					},
				},
				&nfsv4_xdr.NfsResop4_OP_SECINFO{
					Opsecinfo: &nfsv4_xdr.Secinfo4res_NFS4_OK{
						Resok4: []nfsv4_xdr.Secinfo4{
							&nfsv4_xdr.Secinfo4_default{
								Flavor: rpcv2.AUTH_NONE,
							},
						},
					},
				},
			},
			Status: nfsv4_xdr.NFS4_OK,
		}, res)
	})
}

// TODO: SETATTR
// TODO: SETCLIENTID

func TestBaseProgramCompound_OP_SETCLIENTID_CONFIRM(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	rootDirectory := mock.NewMockVirtualDirectory(ctrl)
	rootDirectory.EXPECT().VirtualGetAttributes(gomock.Any(), virtual.AttributesMaskFileHandle, gomock.Any()).
		Do(func(ctx context.Context, requested virtual.AttributesMask, attributes *virtual.Attributes) {
			attributes.SetFileHandle([]byte{0x3d, 0x01, 0x56, 0xaf, 0xab, 0x16, 0xe9, 0x23})
		})
	handleResolver := mock.NewMockHandleResolver(ctrl)
	randomNumberGenerator := mock.NewMockSingleThreadedGenerator(ctrl)
	rebootVerifier := nfsv4_xdr.Verifier4{0x73, 0xaf, 0xeb, 0xd6, 0x5b, 0x96, 0x74, 0xde}
	stateIDOtherPrefix := [...]byte{0xdb, 0xd3, 0xb5, 0x41}
	clock := mock.NewMockClock(ctrl)
	program := nfsv4.NewBaseProgram(rootDirectory, handleResolver.Call, randomNumberGenerator, rebootVerifier, stateIDOtherPrefix, clock, 2*time.Minute, time.Minute)

	t.Run("NoKnownClientID", func(t *testing.T) {
		// Calling SETCLIENTID_CONFIRM without calling
		// SETCLIENTID first doesn't work.
		clock.EXPECT().Now().Return(time.Unix(1000, 0))

		res, err := program.NfsV4Nfsproc4Compound(ctx, &nfsv4_xdr.Compound4args{
			Tag: "setclientid_confirm",
			Argarray: []nfsv4_xdr.NfsArgop4{
				&nfsv4_xdr.NfsArgop4_OP_SETCLIENTID_CONFIRM{
					OpsetclientidConfirm: nfsv4_xdr.SetclientidConfirm4args{
						Clientid:           0x90fee2857d7b5f5b,
						SetclientidConfirm: nfsv4_xdr.Verifier4{0xa1, 0x30, 0xf6, 0x1a, 0xc0, 0xac, 0x1f, 0x36},
					},
				},
			},
		})
		require.NoError(t, err)
		require.Equal(t, &nfsv4_xdr.Compound4res{
			Tag: "setclientid_confirm",
			Resarray: []nfsv4_xdr.NfsResop4{
				&nfsv4_xdr.NfsResop4_OP_SETCLIENTID_CONFIRM{
					OpsetclientidConfirm: nfsv4_xdr.SetclientidConfirm4res{
						Status: nfsv4_xdr.NFS4ERR_STALE_CLIENTID,
					},
				},
			},
			Status: nfsv4_xdr.NFS4ERR_STALE_CLIENTID,
		}, res)
	})

	t.Run("TooSlow", func(t *testing.T) {
		// As the server was created with a maximum lease time
		// of 120 seconds, we should see SETCLIENTID_CONFIRM
		// fail if there are 121 seconds in between.
		clock.EXPECT().Now().Return(time.Unix(1100, 0))
		randomNumberGenerator.EXPECT().Uint64().Return(uint64(0xabd34c548970a69b))
		randomNumberGeneratorExpectRead(randomNumberGenerator, []byte{0xbd, 0x89, 0xa7, 0x95, 0xc4, 0x18, 0xd0, 0xd0})

		res, err := program.NfsV4Nfsproc4Compound(ctx, &nfsv4_xdr.Compound4args{
			Tag: "setclientid",
			Argarray: []nfsv4_xdr.NfsArgop4{
				&nfsv4_xdr.NfsArgop4_OP_SETCLIENTID{
					Opsetclientid: nfsv4_xdr.Setclientid4args{
						Client: nfsv4_xdr.NfsClientId4{
							Verifier: nfsv4_xdr.Verifier4{0xcd, 0xef, 0x89, 0x02, 0x4c, 0x39, 0x2d, 0xeb},
							Id:       []byte{0x06, 0x3f, 0xfe, 0x38, 0x30, 0xc5, 0xa8, 0xbc},
						},
						Callback: nfsv4_xdr.CbClient4{
							CbProgram: 0x7b3f75b9,
							CbLocation: nfsv4_xdr.Clientaddr4{
								RNetid: "tcp",
								RAddr:  "127.0.0.1.200.123",
							},
						},
						CallbackIdent: 0x1d004919,
					},
				},
			},
		})
		require.NoError(t, err)
		require.Equal(t, &nfsv4_xdr.Compound4res{
			Tag: "setclientid",
			Resarray: []nfsv4_xdr.NfsResop4{
				&nfsv4_xdr.NfsResop4_OP_SETCLIENTID{
					Opsetclientid: &nfsv4_xdr.Setclientid4res_NFS4_OK{
						Resok4: nfsv4_xdr.Setclientid4resok{
							Clientid:           0xabd34c548970a69b,
							SetclientidConfirm: nfsv4_xdr.Verifier4{0xbd, 0x89, 0xa7, 0x95, 0xc4, 0x18, 0xd0, 0xd0},
						},
					},
				},
			},
			Status: nfsv4_xdr.NFS4_OK,
		}, res)

		clock.EXPECT().Now().Return(time.Unix(1221, 0))

		res, err = program.NfsV4Nfsproc4Compound(ctx, &nfsv4_xdr.Compound4args{
			Tag: "setclientid_confirm",
			Argarray: []nfsv4_xdr.NfsArgop4{
				&nfsv4_xdr.NfsArgop4_OP_SETCLIENTID_CONFIRM{
					OpsetclientidConfirm: nfsv4_xdr.SetclientidConfirm4args{
						Clientid:           0xabd34c548970a69b,
						SetclientidConfirm: nfsv4_xdr.Verifier4{0xbd, 0x89, 0xa7, 0x95, 0xc4, 0x18, 0xd0, 0xd0},
					},
				},
			},
		})
		require.NoError(t, err)
		require.Equal(t, &nfsv4_xdr.Compound4res{
			Tag: "setclientid_confirm",
			Resarray: []nfsv4_xdr.NfsResop4{
				&nfsv4_xdr.NfsResop4_OP_SETCLIENTID_CONFIRM{
					OpsetclientidConfirm: nfsv4_xdr.SetclientidConfirm4res{
						Status: nfsv4_xdr.NFS4ERR_STALE_CLIENTID,
					},
				},
			},
			Status: nfsv4_xdr.NFS4ERR_STALE_CLIENTID,
		}, res)
	})

	t.Run("Success", func(t *testing.T) {
		// Successfully confirm a client.
		clock.EXPECT().Now().Return(time.Unix(1300, 0))
		randomNumberGenerator.EXPECT().Uint64().Return(uint64(0x23078b2a3f2e1856))
		randomNumberGeneratorExpectRead(randomNumberGenerator, []byte{0xd8, 0x4d, 0xc4, 0x51, 0xcb, 0xe9, 0xec, 0xb9})

		res, err := program.NfsV4Nfsproc4Compound(ctx, &nfsv4_xdr.Compound4args{
			Tag: "setclientid",
			Argarray: []nfsv4_xdr.NfsArgop4{
				&nfsv4_xdr.NfsArgop4_OP_SETCLIENTID{
					Opsetclientid: nfsv4_xdr.Setclientid4args{
						Client: nfsv4_xdr.NfsClientId4{
							Verifier: nfsv4_xdr.Verifier4{0xd4, 0x3e, 0x5a, 0x75, 0x93, 0x4f, 0x01, 0x7c},
							Id:       []byte{0x75, 0x89, 0x89, 0xbf, 0x89, 0x10, 0x20, 0xd3},
						},
						Callback: nfsv4_xdr.CbClient4{
							CbProgram: 0xc32f5c62,
							CbLocation: nfsv4_xdr.Clientaddr4{
								RNetid: "tcp",
								RAddr:  "127.0.0.1.200.472",
							},
						},
						CallbackIdent: 0xf5dc603e,
					},
				},
			},
		})
		require.NoError(t, err)
		require.Equal(t, &nfsv4_xdr.Compound4res{
			Tag: "setclientid",
			Resarray: []nfsv4_xdr.NfsResop4{
				&nfsv4_xdr.NfsResop4_OP_SETCLIENTID{
					Opsetclientid: &nfsv4_xdr.Setclientid4res_NFS4_OK{
						Resok4: nfsv4_xdr.Setclientid4resok{
							Clientid:           0x23078b2a3f2e1856,
							SetclientidConfirm: nfsv4_xdr.Verifier4{0xd8, 0x4d, 0xc4, 0x51, 0xcb, 0xe9, 0xec, 0xb9},
						},
					},
				},
			},
			Status: nfsv4_xdr.NFS4_OK,
		}, res)

		clock.EXPECT().Now().Return(time.Unix(1301, 0))

		res, err = program.NfsV4Nfsproc4Compound(ctx, &nfsv4_xdr.Compound4args{
			Tag: "setclientid_confirm",
			Argarray: []nfsv4_xdr.NfsArgop4{
				&nfsv4_xdr.NfsArgop4_OP_SETCLIENTID_CONFIRM{
					OpsetclientidConfirm: nfsv4_xdr.SetclientidConfirm4args{
						Clientid:           0x23078b2a3f2e1856,
						SetclientidConfirm: nfsv4_xdr.Verifier4{0xd8, 0x4d, 0xc4, 0x51, 0xcb, 0xe9, 0xec, 0xb9},
					},
				},
			},
		})
		require.NoError(t, err)
		require.Equal(t, &nfsv4_xdr.Compound4res{
			Tag: "setclientid_confirm",
			Resarray: []nfsv4_xdr.NfsResop4{
				&nfsv4_xdr.NfsResop4_OP_SETCLIENTID_CONFIRM{
					OpsetclientidConfirm: nfsv4_xdr.SetclientidConfirm4res{
						Status: nfsv4_xdr.NFS4_OK,
					},
				},
			},
			Status: nfsv4_xdr.NFS4_OK,
		}, res)
	})

	// Open a file for the remainder of this test.
	leaf := mock.NewMockVirtualLeaf(ctrl)
	clock.EXPECT().Now().Return(time.Unix(1302, 0))
	clock.EXPECT().Now().Return(time.Unix(1303, 0))
	openUnconfirmedFileForTesting(
		ctx,
		t,
		randomNumberGenerator,
		program,
		rootDirectory,
		leaf,
		nfsv4_xdr.NfsFh4{0xc0, 0xa3, 0xb8, 0x99, 0x08, 0x03, 0xe8, 0x45},
		/* shortClientID = */ 0x23078b2a3f2e1856,
		/* seqID = */ 3726,
		/* stateIDOther = */ [...]byte{
			0xdb, 0xd3, 0xb5, 0x41,
			0xc3, 0x2f, 0x5c, 0x62,
			0xf5, 0xdc, 0x60, 0x3e,
		})

	t.Run("Idempotence", func(t *testing.T) {
		// Sending the same confirmation as before should cause
		// no meaningful change. We should see the same response
		// as before.
		clock.EXPECT().Now().Return(time.Unix(1304, 0))

		res, err := program.NfsV4Nfsproc4Compound(ctx, &nfsv4_xdr.Compound4args{
			Tag: "setclientid_confirm",
			Argarray: []nfsv4_xdr.NfsArgop4{
				&nfsv4_xdr.NfsArgop4_OP_SETCLIENTID_CONFIRM{
					OpsetclientidConfirm: nfsv4_xdr.SetclientidConfirm4args{
						Clientid:           0x23078b2a3f2e1856,
						SetclientidConfirm: nfsv4_xdr.Verifier4{0xd8, 0x4d, 0xc4, 0x51, 0xcb, 0xe9, 0xec, 0xb9},
					},
				},
			},
		})
		require.NoError(t, err)
		require.Equal(t, &nfsv4_xdr.Compound4res{
			Tag: "setclientid_confirm",
			Resarray: []nfsv4_xdr.NfsResop4{
				&nfsv4_xdr.NfsResop4_OP_SETCLIENTID_CONFIRM{
					OpsetclientidConfirm: nfsv4_xdr.SetclientidConfirm4res{
						Status: nfsv4_xdr.NFS4_OK,
					},
				},
			},
			Status: nfsv4_xdr.NFS4_OK,
		}, res)
	})

	t.Run("DifferentVerifier", func(t *testing.T) {
		// Sending a new SETCLIENTID request with the same ID,
		// but a different verifier should cause the server to
		// return a new verifier as well.
		clock.EXPECT().Now().Return(time.Unix(1305, 0))
		randomNumberGenerator.EXPECT().Uint64().Return(uint64(0x23078b2a3f2e1856))
		randomNumberGeneratorExpectRead(randomNumberGenerator, []byte{0x76, 0x23, 0x20, 0xcb, 0xb5, 0x5d, 0xed, 0x61})

		res, err := program.NfsV4Nfsproc4Compound(ctx, &nfsv4_xdr.Compound4args{
			Tag: "setclientid",
			Argarray: []nfsv4_xdr.NfsArgop4{
				&nfsv4_xdr.NfsArgop4_OP_SETCLIENTID{
					Opsetclientid: nfsv4_xdr.Setclientid4args{
						Client: nfsv4_xdr.NfsClientId4{
							Verifier: nfsv4_xdr.Verifier4{0x2c, 0xf9, 0x38, 0xc4, 0xc6, 0xea, 0x03, 0x72},
							Id:       []byte{0x75, 0x89, 0x89, 0xbf, 0x89, 0x10, 0x20, 0xd3},
						},
						Callback: nfsv4_xdr.CbClient4{
							CbProgram: 0xc32f5c62,
							CbLocation: nfsv4_xdr.Clientaddr4{
								RNetid: "tcp",
								RAddr:  "127.0.0.1.200.472",
							},
						},
						CallbackIdent: 0xf5dc603e,
					},
				},
			},
		})
		require.NoError(t, err)
		require.Equal(t, &nfsv4_xdr.Compound4res{
			Tag: "setclientid",
			Resarray: []nfsv4_xdr.NfsResop4{
				&nfsv4_xdr.NfsResop4_OP_SETCLIENTID{
					Opsetclientid: &nfsv4_xdr.Setclientid4res_NFS4_OK{
						Resok4: nfsv4_xdr.Setclientid4resok{
							Clientid:           0x23078b2a3f2e1856,
							SetclientidConfirm: nfsv4_xdr.Verifier4{0x76, 0x23, 0x20, 0xcb, 0xb5, 0x5d, 0xed, 0x61},
						},
					},
				},
			},
			Status: nfsv4_xdr.NFS4_OK,
		}, res)

		// Confirming the new verifier should cause the server
		// to discard all state associated with the previous
		// one, as it indicated the client rebooted.
		clock.EXPECT().Now().Return(time.Unix(1306, 0))
		leaf.EXPECT().VirtualClose()

		res, err = program.NfsV4Nfsproc4Compound(ctx, &nfsv4_xdr.Compound4args{
			Tag: "setclientid_confirm",
			Argarray: []nfsv4_xdr.NfsArgop4{
				&nfsv4_xdr.NfsArgop4_OP_SETCLIENTID_CONFIRM{
					OpsetclientidConfirm: nfsv4_xdr.SetclientidConfirm4args{
						Clientid:           0x23078b2a3f2e1856,
						SetclientidConfirm: nfsv4_xdr.Verifier4{0x76, 0x23, 0x20, 0xcb, 0xb5, 0x5d, 0xed, 0x61},
					},
				},
			},
		})
		require.NoError(t, err)
		require.Equal(t, &nfsv4_xdr.Compound4res{
			Tag: "setclientid_confirm",
			Resarray: []nfsv4_xdr.NfsResop4{
				&nfsv4_xdr.NfsResop4_OP_SETCLIENTID_CONFIRM{
					OpsetclientidConfirm: nfsv4_xdr.SetclientidConfirm4res{
						Status: nfsv4_xdr.NFS4_OK,
					},
				},
			},
			Status: nfsv4_xdr.NFS4_OK,
		}, res)
	})
}

func TestBaseProgramCompound_OP_VERIFY(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	rootDirectory := mock.NewMockVirtualDirectory(ctrl)
	rootDirectory.EXPECT().VirtualGetAttributes(gomock.Any(), virtual.AttributesMaskFileHandle, gomock.Any()).
		Do(func(ctx context.Context, requested virtual.AttributesMask, attributes *virtual.Attributes) {
			attributes.SetFileHandle([]byte{0xe1, 0x79, 0xc1, 0x39, 0x2a, 0xef, 0xbb, 0xde})
		})
	handleResolver := mock.NewMockHandleResolver(ctrl)
	randomNumberGenerator := mock.NewMockSingleThreadedGenerator(ctrl)
	rebootVerifier := nfsv4_xdr.Verifier4{0x71, 0x69, 0x6c, 0x7c, 0x90, 0x79, 0x3b, 0x13}
	stateIDOtherPrefix := [...]byte{0x19, 0xed, 0x93, 0x5f}
	clock := mock.NewMockClock(ctrl)
	program := nfsv4.NewBaseProgram(rootDirectory, handleResolver.Call, randomNumberGenerator, rebootVerifier, stateIDOtherPrefix, clock, 2*time.Minute, time.Minute)

	t.Run("NoFileHandle", func(t *testing.T) {
		// Calling VERIFY without a file handle should fail.
		res, err := program.NfsV4Nfsproc4Compound(ctx, &nfsv4_xdr.Compound4args{
			Tag: "verify",
			Argarray: []nfsv4_xdr.NfsArgop4{
				&nfsv4_xdr.NfsArgop4_OP_VERIFY{
					Opverify: nfsv4_xdr.Verify4args{
						ObjAttributes: nfsv4_xdr.Fattr4{
							Attrmask: nfsv4_xdr.Bitmap4{
								1 << nfsv4_xdr.FATTR4_TYPE,
							},
							AttrVals: nfsv4_xdr.Attrlist4{
								// FATTR4_TYPE == NF4DIR.
								0x00, 0x00, 0x00, 0x02,
							},
						},
					},
				},
			},
		})
		require.NoError(t, err)
		require.Equal(t, &nfsv4_xdr.Compound4res{
			Tag: "verify",
			Resarray: []nfsv4_xdr.NfsResop4{
				&nfsv4_xdr.NfsResop4_OP_VERIFY{
					Opverify: nfsv4_xdr.Verify4res{
						Status: nfsv4_xdr.NFS4ERR_NOFILEHANDLE,
					},
				},
			},
			Status: nfsv4_xdr.NFS4ERR_NOFILEHANDLE,
		}, res)
	})

	t.Run("BadXDR1", func(t *testing.T) {
		// If the client provides attributes that are an exact
		// prefix of what we compute ourselves, then the data
		// provided by the client must be corrupted. XDR would
		// never allow that.
		rootDirectory.EXPECT().VirtualGetAttributes(ctx, virtual.AttributesMaskFileType, gomock.Any()).
			Do(func(ctx context.Context, requested virtual.AttributesMask, attributes *virtual.Attributes) {
				attributes.SetFileType(filesystem.FileTypeDirectory)
			})

		res, err := program.NfsV4Nfsproc4Compound(ctx, &nfsv4_xdr.Compound4args{
			Tag: "verify",
			Argarray: []nfsv4_xdr.NfsArgop4{
				&nfsv4_xdr.NfsArgop4_OP_PUTROOTFH{},
				&nfsv4_xdr.NfsArgop4_OP_VERIFY{
					Opverify: nfsv4_xdr.Verify4args{
						ObjAttributes: nfsv4_xdr.Fattr4{
							Attrmask: nfsv4_xdr.Bitmap4{
								1 << nfsv4_xdr.FATTR4_TYPE,
							},
							AttrVals: nfsv4_xdr.Attrlist4{
								// FATTR4_TYPE, truncated.
								0x00, 0x00, 0x00,
							},
						},
					},
				},
			},
		})
		require.NoError(t, err)
		require.Equal(t, &nfsv4_xdr.Compound4res{
			Tag: "verify",
			Resarray: []nfsv4_xdr.NfsResop4{
				&nfsv4_xdr.NfsResop4_OP_PUTROOTFH{
					Opputrootfh: nfsv4_xdr.Putrootfh4res{
						Status: nfsv4_xdr.NFS4_OK,
					},
				},
				&nfsv4_xdr.NfsResop4_OP_VERIFY{
					Opverify: nfsv4_xdr.Verify4res{
						Status: nfsv4_xdr.NFS4ERR_BADXDR,
					},
				},
			},
			Status: nfsv4_xdr.NFS4ERR_BADXDR,
		}, res)
	})

	t.Run("BadXDR2", func(t *testing.T) {
		// The same holds for when the client provides more data
		// than we generate.
		rootDirectory.EXPECT().VirtualGetAttributes(ctx, virtual.AttributesMaskFileType, gomock.Any()).
			Do(func(ctx context.Context, requested virtual.AttributesMask, attributes *virtual.Attributes) {
				attributes.SetFileType(filesystem.FileTypeDirectory)
			})

		res, err := program.NfsV4Nfsproc4Compound(ctx, &nfsv4_xdr.Compound4args{
			Tag: "verify",
			Argarray: []nfsv4_xdr.NfsArgop4{
				&nfsv4_xdr.NfsArgop4_OP_PUTROOTFH{},
				&nfsv4_xdr.NfsArgop4_OP_VERIFY{
					Opverify: nfsv4_xdr.Verify4args{
						ObjAttributes: nfsv4_xdr.Fattr4{
							Attrmask: nfsv4_xdr.Bitmap4{
								1 << nfsv4_xdr.FATTR4_TYPE,
							},
							AttrVals: nfsv4_xdr.Attrlist4{
								// FATTR4_TYPE == NF4DIR.
								0x00, 0x00, 0x00, 0x02,
								// Trailing garbage.
								0xde, 0xad, 0xc0, 0xde,
							},
						},
					},
				},
			},
		})
		require.NoError(t, err)
		require.Equal(t, &nfsv4_xdr.Compound4res{
			Tag: "verify",
			Resarray: []nfsv4_xdr.NfsResop4{
				&nfsv4_xdr.NfsResop4_OP_PUTROOTFH{
					Opputrootfh: nfsv4_xdr.Putrootfh4res{
						Status: nfsv4_xdr.NFS4_OK,
					},
				},
				&nfsv4_xdr.NfsResop4_OP_VERIFY{
					Opverify: nfsv4_xdr.Verify4res{
						Status: nfsv4_xdr.NFS4ERR_BADXDR,
					},
				},
			},
			Status: nfsv4_xdr.NFS4ERR_BADXDR,
		}, res)
	})

	t.Run("UnsupportedAttribute", func(t *testing.T) {
		// We don't support the 'system' attribute. Providing it
		// as part of VERIFY should cause us to return
		// NFS4ERR_ATTRNOTSUPP.
		rootDirectory.EXPECT().VirtualGetAttributes(ctx, virtual.AttributesMaskFileType, gomock.Any()).
			Do(func(ctx context.Context, requested virtual.AttributesMask, attributes *virtual.Attributes) {
				attributes.SetFileType(filesystem.FileTypeDirectory)
			})

		res, err := program.NfsV4Nfsproc4Compound(ctx, &nfsv4_xdr.Compound4args{
			Tag: "verify",
			Argarray: []nfsv4_xdr.NfsArgop4{
				&nfsv4_xdr.NfsArgop4_OP_PUTROOTFH{},
				&nfsv4_xdr.NfsArgop4_OP_VERIFY{
					Opverify: nfsv4_xdr.Verify4args{
						ObjAttributes: nfsv4_xdr.Fattr4{
							Attrmask: nfsv4_xdr.Bitmap4{
								1 << nfsv4_xdr.FATTR4_TYPE,
								1 << (nfsv4_xdr.FATTR4_SYSTEM - 32),
							},
							AttrVals: nfsv4_xdr.Attrlist4{
								// FATTR4_TYPE == NF4DIR.
								0x00, 0x00, 0x00, 0x02,
								// FATTR4_SYSTEM == TRUE.
								0x00, 0x00, 0x00, 0x01,
							},
						},
					},
				},
			},
		})
		require.NoError(t, err)
		require.Equal(t, &nfsv4_xdr.Compound4res{
			Tag: "verify",
			Resarray: []nfsv4_xdr.NfsResop4{
				&nfsv4_xdr.NfsResop4_OP_PUTROOTFH{
					Opputrootfh: nfsv4_xdr.Putrootfh4res{
						Status: nfsv4_xdr.NFS4_OK,
					},
				},
				&nfsv4_xdr.NfsResop4_OP_VERIFY{
					Opverify: nfsv4_xdr.Verify4res{
						Status: nfsv4_xdr.NFS4ERR_ATTRNOTSUPP,
					},
				},
			},
			Status: nfsv4_xdr.NFS4ERR_ATTRNOTSUPP,
		}, res)
	})

	t.Run("InvalidAttribute", func(t *testing.T) {
		// The 'rdattr_error' attribute is only returned as part
		// of READDIR. It cannot be provided to VERIFY.
		rootDirectory.EXPECT().VirtualGetAttributes(ctx, virtual.AttributesMaskFileType, gomock.Any()).
			Do(func(ctx context.Context, requested virtual.AttributesMask, attributes *virtual.Attributes) {
				attributes.SetFileType(filesystem.FileTypeDirectory)
			})

		res, err := program.NfsV4Nfsproc4Compound(ctx, &nfsv4_xdr.Compound4args{
			Tag: "verify",
			Argarray: []nfsv4_xdr.NfsArgop4{
				&nfsv4_xdr.NfsArgop4_OP_PUTROOTFH{},
				&nfsv4_xdr.NfsArgop4_OP_VERIFY{
					Opverify: nfsv4_xdr.Verify4args{
						ObjAttributes: nfsv4_xdr.Fattr4{
							Attrmask: nfsv4_xdr.Bitmap4{
								(1 << nfsv4_xdr.FATTR4_TYPE) |
									(1 << nfsv4_xdr.FATTR4_RDATTR_ERROR),
							},
							AttrVals: nfsv4_xdr.Attrlist4{
								// FATTR4_TYPE == NF4DIR.
								0x00, 0x00, 0x00, 0x02,
								// FATTR4_RDATTR_ERROR == NFS4ERR_IO.
								0x00, 0x00, 0x00, 0x05,
							},
						},
					},
				},
			},
		})
		require.NoError(t, err)
		require.Equal(t, &nfsv4_xdr.Compound4res{
			Tag: "verify",
			Resarray: []nfsv4_xdr.NfsResop4{
				&nfsv4_xdr.NfsResop4_OP_PUTROOTFH{
					Opputrootfh: nfsv4_xdr.Putrootfh4res{
						Status: nfsv4_xdr.NFS4_OK,
					},
				},
				&nfsv4_xdr.NfsResop4_OP_VERIFY{
					Opverify: nfsv4_xdr.Verify4res{
						Status: nfsv4_xdr.NFS4ERR_INVAL,
					},
				},
			},
			Status: nfsv4_xdr.NFS4ERR_INVAL,
		}, res)
	})

	t.Run("Mismatch", func(t *testing.T) {
		rootDirectory.EXPECT().VirtualGetAttributes(ctx, virtual.AttributesMaskFileType, gomock.Any()).
			Do(func(ctx context.Context, requested virtual.AttributesMask, attributes *virtual.Attributes) {
				attributes.SetFileType(filesystem.FileTypeDirectory)
			})

		res, err := program.NfsV4Nfsproc4Compound(ctx, &nfsv4_xdr.Compound4args{
			Tag: "verify",
			Argarray: []nfsv4_xdr.NfsArgop4{
				&nfsv4_xdr.NfsArgop4_OP_PUTROOTFH{},
				&nfsv4_xdr.NfsArgop4_OP_VERIFY{
					Opverify: nfsv4_xdr.Verify4args{
						ObjAttributes: nfsv4_xdr.Fattr4{
							Attrmask: nfsv4_xdr.Bitmap4{
								1 << nfsv4_xdr.FATTR4_TYPE,
							},
							AttrVals: nfsv4_xdr.Attrlist4{
								// FATTR4_TYPE == NF4BLK.
								0x00, 0x00, 0x00, 0x03,
							},
						},
					},
				},
			},
		})
		require.NoError(t, err)
		require.Equal(t, &nfsv4_xdr.Compound4res{
			Tag: "verify",
			Resarray: []nfsv4_xdr.NfsResop4{
				&nfsv4_xdr.NfsResop4_OP_PUTROOTFH{
					Opputrootfh: nfsv4_xdr.Putrootfh4res{
						Status: nfsv4_xdr.NFS4_OK,
					},
				},
				&nfsv4_xdr.NfsResop4_OP_VERIFY{
					Opverify: nfsv4_xdr.Verify4res{
						Status: nfsv4_xdr.NFS4ERR_NOT_SAME,
					},
				},
			},
			Status: nfsv4_xdr.NFS4ERR_NOT_SAME,
		}, res)
	})

	t.Run("Match", func(t *testing.T) {
		rootDirectory.EXPECT().VirtualGetAttributes(ctx, virtual.AttributesMaskFileType|virtual.AttributesMaskInodeNumber, gomock.Any()).
			Do(func(ctx context.Context, requested virtual.AttributesMask, attributes *virtual.Attributes) {
				attributes.SetFileType(filesystem.FileTypeDirectory)
				attributes.SetInodeNumber(0x676b7bcb66d92ed6)
			})

		res, err := program.NfsV4Nfsproc4Compound(ctx, &nfsv4_xdr.Compound4args{
			Tag: "verify",
			Argarray: []nfsv4_xdr.NfsArgop4{
				&nfsv4_xdr.NfsArgop4_OP_PUTROOTFH{},
				&nfsv4_xdr.NfsArgop4_OP_VERIFY{
					Opverify: nfsv4_xdr.Verify4args{
						ObjAttributes: nfsv4_xdr.Fattr4{
							Attrmask: nfsv4_xdr.Bitmap4{
								(1 << nfsv4_xdr.FATTR4_TYPE) |
									(1 << nfsv4_xdr.FATTR4_FILEID),
							},
							AttrVals: nfsv4_xdr.Attrlist4{
								// FATTR4_TYPE == NF4DIR.
								0x00, 0x00, 0x00, 0x02,
								// FATTR4_FILEID.
								0x67, 0x6b, 0x7b, 0xcb, 0x66, 0xd9, 0x2e, 0xd6,
							},
						},
					},
				},
			},
		})
		require.NoError(t, err)
		require.Equal(t, &nfsv4_xdr.Compound4res{
			Tag: "verify",
			Resarray: []nfsv4_xdr.NfsResop4{
				&nfsv4_xdr.NfsResop4_OP_PUTROOTFH{
					Opputrootfh: nfsv4_xdr.Putrootfh4res{
						Status: nfsv4_xdr.NFS4_OK,
					},
				},
				&nfsv4_xdr.NfsResop4_OP_VERIFY{
					Opverify: nfsv4_xdr.Verify4res{
						Status: nfsv4_xdr.NFS4_OK,
					},
				},
			},
			Status: nfsv4_xdr.NFS4_OK,
		}, res)
	})
}

// TODO: WRITE
