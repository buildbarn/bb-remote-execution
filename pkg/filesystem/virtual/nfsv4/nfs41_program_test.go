package nfsv4_test

import (
	"context"
	"testing"
	"time"

	"github.com/buildbarn/bb-remote-execution/internal/mock"
	"github.com/buildbarn/bb-remote-execution/pkg/filesystem/virtual"
	"github.com/buildbarn/bb-remote-execution/pkg/filesystem/virtual/nfsv4"
	"github.com/buildbarn/bb-storage/pkg/filesystem/path"
	nfsv4_xdr "github.com/buildbarn/go-xdr/pkg/protocols/nfsv4"
	"github.com/stretchr/testify/require"

	"go.uber.org/mock/gomock"
)

func exchangeIDAndCreateSessionForTesting(ctx context.Context, t *testing.T, serverOwner nfsv4_xdr.ServerOwner4, serverScope []byte, randomNumberGenerator *mock.MockSingleThreadedGenerator, program nfsv4_xdr.Nfs4Program, sessionID nfsv4_xdr.Sessionid4) {
	randomNumberGenerator.EXPECT().Uint64().Return(uint64(0xa103edeb4dd51d2a))
	randomNumberGenerator.EXPECT().Uint32().Return(uint32(0x501590ad))

	res, err := program.NfsV4Nfsproc4Compound(ctx, &nfsv4_xdr.Compound4args{
		Tag:          "exchange_id",
		Minorversion: 1,
		Argarray: []nfsv4_xdr.NfsArgop4{
			&nfsv4_xdr.NfsArgop4_OP_EXCHANGE_ID{
				OpexchangeId: nfsv4_xdr.ExchangeId4args{
					EiaClientowner: nfsv4_xdr.ClientOwner4{
						CoVerifier: nfsv4_xdr.Verifier4{0x34, 0xe0, 0x62, 0xbb, 0x0c, 0x77, 0x75, 0x97},
						CoOwnerid:  []byte{0x53, 0x21, 0x05, 0x3f, 0xd5, 0xdd, 0x60, 0xc3},
					},
					EiaFlags:        0,
					EiaStateProtect: &nfsv4_xdr.StateProtect4A_SP4_NONE{},
				},
			},
		},
	})
	require.NoError(t, err)
	require.Equal(t, &nfsv4_xdr.Compound4res{
		Tag: "exchange_id",
		Resarray: []nfsv4_xdr.NfsResop4{
			&nfsv4_xdr.NfsResop4_OP_EXCHANGE_ID{
				OpexchangeId: &nfsv4_xdr.ExchangeId4res_NFS4_OK{
					EirResok4: nfsv4_xdr.ExchangeId4resok{
						EirClientid:     0xa103edeb4dd51d2a,
						EirSequenceid:   0x501590ae,
						EirFlags:        nfsv4_xdr.EXCHGID4_FLAG_USE_NON_PNFS,
						EirStateProtect: &nfsv4_xdr.StateProtect4R_SP4_NONE{},
						EirServerOwner:  serverOwner,
						EirServerScope:  serverScope,
						EirServerImplId: []nfsv4_xdr.NfsImplId4{{
							NiiDomain: "buildbarn.github.io",
						}},
					},
				},
			},
		},
		Status: nfsv4_xdr.NFS4_OK,
	}, res)

	randomNumberGenerator.EXPECT().Read(gomock.Len(nfsv4_xdr.NFS4_SESSIONID_SIZE)).
		DoAndReturn(func(p []byte) (int, error) {
			return copy(p, sessionID[:]), nil
		})

	res, err = program.NfsV4Nfsproc4Compound(ctx, &nfsv4_xdr.Compound4args{
		Tag:          "create_session",
		Minorversion: 1,
		Argarray: []nfsv4_xdr.NfsArgop4{
			&nfsv4_xdr.NfsArgop4_OP_CREATE_SESSION{
				OpcreateSession: nfsv4_xdr.CreateSession4args{
					CsaClientid: 0xa103edeb4dd51d2a,
					CsaSequence: 0x501590ae,
					CsaFlags:    0,
					CsaForeChanAttrs: nfsv4_xdr.ChannelAttrs4{
						CaHeaderpadsize:         512,
						CaMaxrequestsize:        4 * 1024 * 1024,
						CaMaxresponsesize:       6 * 1024 * 1024,
						CaMaxresponsesizeCached: 8 * 1024 * 1024,
						CaMaxoperations:         100000,
						CaMaxrequests:           1000000,
					},
				},
			},
		},
	})
	require.NoError(t, err)
	require.Equal(t, &nfsv4_xdr.Compound4res{
		Tag: "create_session",
		Resarray: []nfsv4_xdr.NfsResop4{
			&nfsv4_xdr.NfsResop4_OP_CREATE_SESSION{
				OpcreateSession: &nfsv4_xdr.CreateSession4res_NFS4_OK{
					CsrResok4: nfsv4_xdr.CreateSession4resok{
						CsrSessionid: sessionID,
						CsrSequence:  0x501590ae,
						CsrFlags:     0,
						CsrForeChanAttrs: nfsv4_xdr.ChannelAttrs4{
							CaHeaderpadsize:         0,
							CaMaxrequestsize:        1 * 1024 * 1024,
							CaMaxresponsesize:       2 * 1024 * 1024,
							CaMaxresponsesizeCached: 64 * 1024,
							CaMaxoperations:         100,
							CaMaxrequests:           100,
						},
					},
				},
			},
		},
		Status: nfsv4_xdr.NFS4_OK,
	}, res)
}

func TestNFS41ProgramCompound_OP_CLOSE(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	rootDirectory := mock.NewMockVirtualDirectory(ctrl)
	rootDirectory.EXPECT().VirtualGetAttributes(gomock.Any(), virtual.AttributesMaskFileHandle, gomock.Any()).
		Do(func(ctx context.Context, requested virtual.AttributesMask, attributes *virtual.Attributes) {
			attributes.SetFileHandle([]byte{0x36, 0x00, 0x34, 0x9d, 0xad, 0x05, 0x2e, 0xc9})
		})
	handleResolver := mock.NewMockHandleResolver(ctrl)
	serverOwner := nfsv4_xdr.ServerOwner4{
		SoMinorId: 0x9845dcbaf41ca916,
		SoMajorId: []byte{0xae, 0x56, 0x12, 0x3b, 0x7c, 0x2c, 0xee, 0xc8},
	}
	serverScope := []byte{0x4a, 0x92, 0xbc, 0x46, 0xf9, 0xde, 0x21, 0xc1}
	randomNumberGenerator := mock.NewMockSingleThreadedGenerator(ctrl)
	rebootVerifier := nfsv4_xdr.Verifier4{0x27, 0xa3, 0x1b, 0x72, 0xab, 0x10, 0xb4, 0xcd}
	clock := mock.NewMockClock(ctrl)
	program := nfsv4.NewNFS41Program(
		rootDirectory,
		nfsv4.NewOpenedFilesPool(handleResolver.Call),
		serverOwner,
		serverScope,
		&nfsv4_xdr.ChannelAttrs4{
			CaHeaderpadsize:         0,
			CaMaxrequestsize:        1 * 1024 * 1024,
			CaMaxresponsesize:       2 * 1024 * 1024,
			CaMaxresponsesizeCached: 64 * 1024,
			CaMaxoperations:         100,
			CaMaxrequests:           100,
		},
		randomNumberGenerator,
		rebootVerifier,
		clock,
		/* enforcedLeaseTime = */ 2*time.Minute,
		/* announcedLeaseTime = */ time.Minute,
	)

	clock.EXPECT().Now().Return(time.Unix(1000, 0)).Times(2)
	exchangeIDAndCreateSessionForTesting(
		ctx,
		t,
		serverOwner,
		serverScope,
		randomNumberGenerator,
		program,
		/* sessionID = */ [...]byte{
			0x07, 0xcc, 0xb4, 0x52, 0x2d, 0x64, 0xa1, 0xf0,
			0x9f, 0xaa, 0x61, 0x6b, 0x7a, 0x63, 0xf7, 0xaa,
		},
	)

	t.Run("AnonymousStateID", func(t *testing.T) {
		// Calling CLOSE against the anonymous state ID is of
		// course not permitted. This operation only works when
		// called against regular state IDs.
		clock.EXPECT().Now().Return(time.Unix(1001, 0)).Times(2)
		leaf := mock.NewMockVirtualLeaf(ctrl)
		handleResolverExpectCall(t, handleResolver, []byte{0xc1, 0xe2, 0x37, 0xfb, 0xd0, 0xab, 0xe8, 0x7d}, virtual.DirectoryChild{}.FromLeaf(leaf), virtual.StatusOK)

		res, err := program.NfsV4Nfsproc4Compound(ctx, &nfsv4_xdr.Compound4args{
			Tag:          "close",
			Minorversion: 1,
			Argarray: []nfsv4_xdr.NfsArgop4{
				&nfsv4_xdr.NfsArgop4_OP_SEQUENCE{
					Opsequence: nfsv4_xdr.Sequence4args{
						SaSessionid: [...]byte{
							0x07, 0xcc, 0xb4, 0x52, 0x2d, 0x64, 0xa1, 0xf0,
							0x9f, 0xaa, 0x61, 0x6b, 0x7a, 0x63, 0xf7, 0xaa,
						},
						SaSequenceid:    1,
						SaSlotid:        0,
						SaHighestSlotid: 0,
					},
				},
				&nfsv4_xdr.NfsArgop4_OP_PUTFH{
					Opputfh: nfsv4_xdr.Putfh4args{
						Object: nfsv4_xdr.NfsFh4{0xc1, 0xe2, 0x37, 0xfb, 0xd0, 0xab, 0xe8, 0x7d},
					},
				},
				&nfsv4_xdr.NfsArgop4_OP_CLOSE{
					Opclose: nfsv4_xdr.Close4args{
						OpenStateid: nfsv4_xdr.Stateid4{
							Seqid: 0,
							Other: [...]byte{
								0x00, 0x00, 0x00, 0x00,
								0x00, 0x00, 0x00, 0x00,
								0x00, 0x00, 0x00, 0x00,
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
				&nfsv4_xdr.NfsResop4_OP_SEQUENCE{
					Opsequence: &nfsv4_xdr.Sequence4res_NFS4_OK{
						SrResok4: nfsv4_xdr.Sequence4resok{
							SrSessionid: [...]byte{
								0x07, 0xcc, 0xb4, 0x52, 0x2d, 0x64, 0xa1, 0xf0,
								0x9f, 0xaa, 0x61, 0x6b, 0x7a, 0x63, 0xf7, 0xaa,
							},
							SrSequenceid:          1,
							SrSlotid:              0,
							SrHighestSlotid:       99,
							SrTargetHighestSlotid: 99,
							SrStatusFlags:         0,
						},
					},
				},
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

	// The remainder of this test assumes that a file has been
	// opened, having a seqid greater than 1.
	leaf := mock.NewMockVirtualLeaf(ctrl)
	handleResolverExpectCall(t, handleResolver, []byte{0xeb, 0x31, 0xdc, 0x69, 0x20, 0x87, 0x71, 0x25}, virtual.DirectoryChild{}.FromLeaf(leaf), virtual.StatusOK)

	for i := 0; i < 10; i++ {
		leaf.EXPECT().VirtualOpenSelf(
			ctx,
			virtual.ShareMaskRead|virtual.ShareMaskWrite,
			&virtual.OpenExistingOptions{},
			virtual.AttributesMask(0),
			gomock.Any(),
		)
		if i > 0 {
			leaf.EXPECT().VirtualClose(virtual.ShareMaskRead | virtual.ShareMaskWrite)
		}

		clock.EXPECT().Now().Return(time.Unix(1002, 0)).Times(2)
		res, err := program.NfsV4Nfsproc4Compound(ctx, &nfsv4_xdr.Compound4args{
			Tag:          "open",
			Minorversion: 1,
			Argarray: []nfsv4_xdr.NfsArgop4{
				&nfsv4_xdr.NfsArgop4_OP_SEQUENCE{
					Opsequence: nfsv4_xdr.Sequence4args{
						SaSessionid: [...]byte{
							0x07, 0xcc, 0xb4, 0x52, 0x2d, 0x64, 0xa1, 0xf0,
							0x9f, 0xaa, 0x61, 0x6b, 0x7a, 0x63, 0xf7, 0xaa,
						},
						SaSequenceid:    nfsv4_xdr.Sequenceid4(i + 2),
						SaSlotid:        0,
						SaHighestSlotid: 0,
					},
				},
				&nfsv4_xdr.NfsArgop4_OP_PUTFH{
					Opputfh: nfsv4_xdr.Putfh4args{
						Object: nfsv4_xdr.NfsFh4{0xeb, 0x31, 0xdc, 0x69, 0x20, 0x87, 0x71, 0x25},
					},
				},
				&nfsv4_xdr.NfsArgop4_OP_OPEN{
					Opopen: nfsv4_xdr.Open4args{
						ShareAccess: nfsv4_xdr.OPEN4_SHARE_ACCESS_BOTH,
						ShareDeny:   nfsv4_xdr.OPEN4_SHARE_DENY_NONE,
						Owner: nfsv4_xdr.OpenOwner4{
							Owner: []byte{0xc5, 0x0a, 0x91, 0x89, 0xec, 0x49, 0xbf, 0x1f},
						},
						Openhow: &nfsv4_xdr.Openflag4_default{
							Opentype: nfsv4_xdr.OPEN4_NOCREATE,
						},
						Claim: &nfsv4_xdr.OpenClaim4_CLAIM_FH{},
					},
				},
			},
		})
		require.NoError(t, err)
		require.Equal(t, &nfsv4_xdr.Compound4res{
			Tag: "open",
			Resarray: []nfsv4_xdr.NfsResop4{
				&nfsv4_xdr.NfsResop4_OP_SEQUENCE{
					Opsequence: &nfsv4_xdr.Sequence4res_NFS4_OK{
						SrResok4: nfsv4_xdr.Sequence4resok{
							SrSessionid: [...]byte{
								0x07, 0xcc, 0xb4, 0x52, 0x2d, 0x64, 0xa1, 0xf0,
								0x9f, 0xaa, 0x61, 0x6b, 0x7a, 0x63, 0xf7, 0xaa,
							},
							SrSequenceid:          nfsv4_xdr.Sequenceid4(i + 2),
							SrSlotid:              0,
							SrHighestSlotid:       99,
							SrTargetHighestSlotid: 99,
							SrStatusFlags:         0,
						},
					},
				},
				&nfsv4_xdr.NfsResop4_OP_PUTFH{
					Opputfh: nfsv4_xdr.Putfh4res{
						Status: nfsv4_xdr.NFS4_OK,
					},
				},
				&nfsv4_xdr.NfsResop4_OP_OPEN{
					Opopen: &nfsv4_xdr.Open4res_NFS4_OK{
						Resok4: nfsv4_xdr.Open4resok{
							Stateid: nfsv4_xdr.Stateid4{
								Seqid: uint32(i + 1),
								Other: [...]byte{
									0x01, 0x00, 0x00, 0x00,
									0x00, 0x00, 0x00, 0x00,
									0x00, 0x00, 0x00, 0x00,
								},
							},
							Cinfo:      nfsv4_xdr.ChangeInfo4{Atomic: true},
							Rflags:     nfsv4_xdr.OPEN4_RESULT_LOCKTYPE_POSIX | nfsv4_xdr.OPEN4_RESULT_PRESERVE_UNLINKED,
							Attrset:    nfsv4_xdr.Bitmap4{},
							Delegation: &nfsv4_xdr.OpenDelegation4_OPEN_DELEGATE_NONE{},
						},
					},
				},
			},
			Status: nfsv4_xdr.NFS4_OK,
		}, res)
	}

	t.Run("NoFileHandle", func(t *testing.T) {
		// Calling CLOSE without a file handle should fail.
		clock.EXPECT().Now().Return(time.Unix(1003, 0)).Times(2)

		res, err := program.NfsV4Nfsproc4Compound(ctx, &nfsv4_xdr.Compound4args{
			Tag:          "close",
			Minorversion: 1,
			Argarray: []nfsv4_xdr.NfsArgop4{
				&nfsv4_xdr.NfsArgop4_OP_SEQUENCE{
					Opsequence: nfsv4_xdr.Sequence4args{
						SaSessionid: [...]byte{
							0x07, 0xcc, 0xb4, 0x52, 0x2d, 0x64, 0xa1, 0xf0,
							0x9f, 0xaa, 0x61, 0x6b, 0x7a, 0x63, 0xf7, 0xaa,
						},
						SaSequenceid:    12,
						SaSlotid:        0,
						SaHighestSlotid: 0,
					},
				},
				&nfsv4_xdr.NfsArgop4_OP_CLOSE{
					Opclose: nfsv4_xdr.Close4args{
						OpenStateid: nfsv4_xdr.Stateid4{
							Seqid: 10,
							Other: [...]byte{
								0x01, 0x00, 0x00, 0x00,
								0x00, 0x00, 0x00, 0x00,
								0x00, 0x00, 0x00, 0x00,
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
				&nfsv4_xdr.NfsResop4_OP_SEQUENCE{
					Opsequence: &nfsv4_xdr.Sequence4res_NFS4_OK{
						SrResok4: nfsv4_xdr.Sequence4resok{
							SrSessionid: [...]byte{
								0x07, 0xcc, 0xb4, 0x52, 0x2d, 0x64, 0xa1, 0xf0,
								0x9f, 0xaa, 0x61, 0x6b, 0x7a, 0x63, 0xf7, 0xaa,
							},
							SrSequenceid:          12,
							SrSlotid:              0,
							SrHighestSlotid:       99,
							SrTargetHighestSlotid: 99,
							SrStatusFlags:         0,
						},
					},
				},
				&nfsv4_xdr.NfsResop4_OP_CLOSE{
					Opclose: &nfsv4_xdr.Close4res_default{
						Status: nfsv4_xdr.NFS4ERR_NOFILEHANDLE,
					},
				},
			},
			Status: nfsv4_xdr.NFS4ERR_NOFILEHANDLE,
		}, res)
	})

	t.Run("OldStateID", func(t *testing.T) {
		// Can't call CLOSE on a state ID from the past.
		clock.EXPECT().Now().Return(time.Unix(1004, 0)).Times(2)

		res, err := program.NfsV4Nfsproc4Compound(ctx, &nfsv4_xdr.Compound4args{
			Tag:          "close",
			Minorversion: 1,
			Argarray: []nfsv4_xdr.NfsArgop4{
				&nfsv4_xdr.NfsArgop4_OP_SEQUENCE{
					Opsequence: nfsv4_xdr.Sequence4args{
						SaSessionid: [...]byte{
							0x07, 0xcc, 0xb4, 0x52, 0x2d, 0x64, 0xa1, 0xf0,
							0x9f, 0xaa, 0x61, 0x6b, 0x7a, 0x63, 0xf7, 0xaa,
						},
						SaSequenceid:    13,
						SaSlotid:        0,
						SaHighestSlotid: 0,
					},
				},
				&nfsv4_xdr.NfsArgop4_OP_PUTFH{
					Opputfh: nfsv4_xdr.Putfh4args{
						Object: nfsv4_xdr.NfsFh4{0xeb, 0x31, 0xdc, 0x69, 0x20, 0x87, 0x71, 0x25},
					},
				},
				&nfsv4_xdr.NfsArgop4_OP_CLOSE{
					Opclose: nfsv4_xdr.Close4args{
						OpenStateid: nfsv4_xdr.Stateid4{
							Seqid: 9,
							Other: [...]byte{
								0x01, 0x00, 0x00, 0x00,
								0x00, 0x00, 0x00, 0x00,
								0x00, 0x00, 0x00, 0x00,
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
				&nfsv4_xdr.NfsResop4_OP_SEQUENCE{
					Opsequence: &nfsv4_xdr.Sequence4res_NFS4_OK{
						SrResok4: nfsv4_xdr.Sequence4resok{
							SrSessionid: [...]byte{
								0x07, 0xcc, 0xb4, 0x52, 0x2d, 0x64, 0xa1, 0xf0,
								0x9f, 0xaa, 0x61, 0x6b, 0x7a, 0x63, 0xf7, 0xaa,
							},
							SrSequenceid:          13,
							SrSlotid:              0,
							SrHighestSlotid:       99,
							SrTargetHighestSlotid: 99,
							SrStatusFlags:         0,
						},
					},
				},
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

	t.Run("Success", func(t *testing.T) {
		// Providing the right seqid should cause the file to be closed.
		clock.EXPECT().Now().Return(time.Unix(1005, 0)).Times(2)
		leaf.EXPECT().VirtualClose(virtual.ShareMaskRead | virtual.ShareMaskWrite)

		res, err := program.NfsV4Nfsproc4Compound(ctx, &nfsv4_xdr.Compound4args{
			Tag:          "close",
			Minorversion: 1,
			Argarray: []nfsv4_xdr.NfsArgop4{
				&nfsv4_xdr.NfsArgop4_OP_SEQUENCE{
					Opsequence: nfsv4_xdr.Sequence4args{
						SaSessionid: [...]byte{
							0x07, 0xcc, 0xb4, 0x52, 0x2d, 0x64, 0xa1, 0xf0,
							0x9f, 0xaa, 0x61, 0x6b, 0x7a, 0x63, 0xf7, 0xaa,
						},
						SaSequenceid:    14,
						SaSlotid:        0,
						SaHighestSlotid: 0,
					},
				},
				&nfsv4_xdr.NfsArgop4_OP_PUTFH{
					Opputfh: nfsv4_xdr.Putfh4args{
						Object: nfsv4_xdr.NfsFh4{0xeb, 0x31, 0xdc, 0x69, 0x20, 0x87, 0x71, 0x25},
					},
				},
				&nfsv4_xdr.NfsArgop4_OP_CLOSE{
					Opclose: nfsv4_xdr.Close4args{
						OpenStateid: nfsv4_xdr.Stateid4{
							Seqid: 10,
							Other: [...]byte{
								0x01, 0x00, 0x00, 0x00,
								0x00, 0x00, 0x00, 0x00,
								0x00, 0x00, 0x00, 0x00,
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
				&nfsv4_xdr.NfsResop4_OP_SEQUENCE{
					Opsequence: &nfsv4_xdr.Sequence4res_NFS4_OK{
						SrResok4: nfsv4_xdr.Sequence4resok{
							SrSessionid: [...]byte{
								0x07, 0xcc, 0xb4, 0x52, 0x2d, 0x64, 0xa1, 0xf0,
								0x9f, 0xaa, 0x61, 0x6b, 0x7a, 0x63, 0xf7, 0xaa,
							},
							SrSequenceid:          14,
							SrSlotid:              0,
							SrHighestSlotid:       99,
							SrTargetHighestSlotid: 99,
							SrStatusFlags:         0,
						},
					},
				},
				&nfsv4_xdr.NfsResop4_OP_PUTFH{
					Opputfh: nfsv4_xdr.Putfh4res{
						Status: nfsv4_xdr.NFS4_OK,
					},
				},
				&nfsv4_xdr.NfsResop4_OP_CLOSE{
					Opclose: &nfsv4_xdr.Close4res_NFS4_OK{
						OpenStateid: nfsv4_xdr.Stateid4{
							Seqid: nfsv4_xdr.NFS4_UINT32_MAX,
							Other: [...]byte{
								0x00, 0x00, 0x00, 0x00,
								0x00, 0x00, 0x00, 0x00,
								0x00, 0x00, 0x00, 0x00,
							},
						},
					},
				},
			},
			Status: nfsv4_xdr.NFS4_OK,
		}, res)
	})

	t.Run("EnforcesCurrentStateIDSeqID", func(t *testing.T) {
		// For calls to CLOSE and OPEN_DOWNGRADE, the sequence
		// ID of the current state ID should be validated. This
		// means that the initial attempt to CLOSE should fail,
		// because the restored state ID still uses a sequence
		// ID value of 1.
		clock.EXPECT().Now().Return(time.Unix(1006, 0)).Times(2)
		leaf := mock.NewMockVirtualLeaf(ctrl)
		rootDirectory.EXPECT().VirtualOpenChild(
			ctx,
			path.MustNewComponent("a"),
			virtual.ShareMaskRead,
			nil,
			&virtual.OpenExistingOptions{},
			virtual.AttributesMaskFileHandle,
			gomock.Any(),
		).DoAndReturn(func(ctx context.Context, name path.Component, shareAccess virtual.ShareMask, createAttributes *virtual.Attributes, existingOptions *virtual.OpenExistingOptions, requested virtual.AttributesMask, openedFileAttributes *virtual.Attributes) (virtual.Leaf, virtual.AttributesMask, virtual.ChangeInfo, virtual.Status) {
			openedFileAttributes.SetFileHandle([]byte{0x4c, 0xa3, 0xde, 0xe3, 0x55, 0x2b, 0x1b, 0x05})
			return leaf, 0, virtual.ChangeInfo{
				Before: 0xf830be6e457fe45c,
				After:  0x4f86da3294696fb7,
			}, virtual.StatusOK
		})
		rootDirectory.EXPECT().VirtualOpenChild(
			ctx,
			path.MustNewComponent("b"),
			virtual.ShareMaskRead,
			nil,
			&virtual.OpenExistingOptions{},
			virtual.AttributesMaskFileHandle,
			gomock.Any(),
		).DoAndReturn(func(ctx context.Context, name path.Component, shareAccess virtual.ShareMask, createAttributes *virtual.Attributes, existingOptions *virtual.OpenExistingOptions, requested virtual.AttributesMask, openedFileAttributes *virtual.Attributes) (virtual.Leaf, virtual.AttributesMask, virtual.ChangeInfo, virtual.Status) {
			openedFileAttributes.SetFileHandle([]byte{0x4c, 0xa3, 0xde, 0xe3, 0x55, 0x2b, 0x1b, 0x05})
			return leaf, 0, virtual.ChangeInfo{
				Before: 0xe14a85736f0883da,
				After:  0x4ca3dee3552b1b05,
			}, virtual.StatusOK
		})
		leaf.EXPECT().VirtualClose(virtual.ShareMaskRead)

		res, err := program.NfsV4Nfsproc4Compound(ctx, &nfsv4_xdr.Compound4args{
			Tag:          "openopenclose",
			Minorversion: 1,
			Argarray: []nfsv4_xdr.NfsArgop4{
				&nfsv4_xdr.NfsArgop4_OP_SEQUENCE{
					Opsequence: nfsv4_xdr.Sequence4args{
						SaSessionid: [...]byte{
							0x07, 0xcc, 0xb4, 0x52, 0x2d, 0x64, 0xa1, 0xf0,
							0x9f, 0xaa, 0x61, 0x6b, 0x7a, 0x63, 0xf7, 0xaa,
						},
						SaSequenceid:    15,
						SaSlotid:        0,
						SaHighestSlotid: 0,
					},
				},
				&nfsv4_xdr.NfsArgop4_OP_PUTROOTFH{},
				&nfsv4_xdr.NfsArgop4_OP_OPEN{
					Opopen: nfsv4_xdr.Open4args{
						ShareAccess: nfsv4_xdr.OPEN4_SHARE_ACCESS_READ,
						ShareDeny:   nfsv4_xdr.OPEN4_SHARE_DENY_NONE,
						Owner: nfsv4_xdr.OpenOwner4{
							Owner: []byte{0x2d, 0x51, 0xd4, 0x81, 0xae, 0x8f, 0x66, 0xae},
						},
						Openhow: &nfsv4_xdr.Openflag4_default{
							Opentype: nfsv4_xdr.OPEN4_NOCREATE,
						},
						Claim: &nfsv4_xdr.OpenClaim4_CLAIM_NULL{
							File: "a",
						},
					},
				},
				&nfsv4_xdr.NfsArgop4_OP_GETFH{},
				&nfsv4_xdr.NfsArgop4_OP_SAVEFH{},
				&nfsv4_xdr.NfsArgop4_OP_PUTROOTFH{},
				&nfsv4_xdr.NfsArgop4_OP_OPEN{
					Opopen: nfsv4_xdr.Open4args{
						ShareAccess: nfsv4_xdr.OPEN4_SHARE_ACCESS_READ,
						ShareDeny:   nfsv4_xdr.OPEN4_SHARE_DENY_NONE,
						Owner: nfsv4_xdr.OpenOwner4{
							Owner: []byte{0x2d, 0x51, 0xd4, 0x81, 0xae, 0x8f, 0x66, 0xae},
						},
						Openhow: &nfsv4_xdr.Openflag4_default{
							Opentype: nfsv4_xdr.OPEN4_NOCREATE,
						},
						Claim: &nfsv4_xdr.OpenClaim4_CLAIM_NULL{
							File: "b",
						},
					},
				},
				&nfsv4_xdr.NfsArgop4_OP_GETFH{},
				&nfsv4_xdr.NfsArgop4_OP_RESTOREFH{},
				&nfsv4_xdr.NfsArgop4_OP_CLOSE{
					Opclose: nfsv4_xdr.Close4args{
						OpenStateid: nfsv4_xdr.Stateid4{
							Seqid: 1,
							Other: [...]byte{
								0x00, 0x00, 0x00, 0x00,
								0x00, 0x00, 0x00, 0x00,
								0x00, 0x00, 0x00, 0x00,
							},
						},
					},
				},
			},
		})
		require.NoError(t, err)
		require.Equal(t, &nfsv4_xdr.Compound4res{
			Tag: "openopenclose",
			Resarray: []nfsv4_xdr.NfsResop4{
				&nfsv4_xdr.NfsResop4_OP_SEQUENCE{
					Opsequence: &nfsv4_xdr.Sequence4res_NFS4_OK{
						SrResok4: nfsv4_xdr.Sequence4resok{
							SrSessionid: [...]byte{
								0x07, 0xcc, 0xb4, 0x52, 0x2d, 0x64, 0xa1, 0xf0,
								0x9f, 0xaa, 0x61, 0x6b, 0x7a, 0x63, 0xf7, 0xaa,
							},
							SrSequenceid:          15,
							SrSlotid:              0,
							SrHighestSlotid:       99,
							SrTargetHighestSlotid: 99,
							SrStatusFlags:         0,
						},
					},
				},
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
								Other: [...]byte{
									0x02, 0x00, 0x00, 0x00,
									0x00, 0x00, 0x00, 0x00,
									0x00, 0x00, 0x00, 0x00,
								},
							},
							Cinfo: nfsv4_xdr.ChangeInfo4{
								Atomic: true,
								Before: 0xf830be6e457fe45c,
								After:  0x4f86da3294696fb7,
							},
							Rflags:     nfsv4_xdr.OPEN4_RESULT_LOCKTYPE_POSIX | nfsv4_xdr.OPEN4_RESULT_PRESERVE_UNLINKED,
							Attrset:    nfsv4_xdr.Bitmap4{},
							Delegation: &nfsv4_xdr.OpenDelegation4_OPEN_DELEGATE_NONE{},
						},
					},
				},
				&nfsv4_xdr.NfsResop4_OP_GETFH{
					Opgetfh: &nfsv4_xdr.Getfh4res_NFS4_OK{
						Resok4: nfsv4_xdr.Getfh4resok{
							Object: nfsv4_xdr.NfsFh4{0x4c, 0xa3, 0xde, 0xe3, 0x55, 0x2b, 0x1b, 0x05},
						},
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
				&nfsv4_xdr.NfsResop4_OP_OPEN{
					Opopen: &nfsv4_xdr.Open4res_NFS4_OK{
						Resok4: nfsv4_xdr.Open4resok{
							Stateid: nfsv4_xdr.Stateid4{
								Seqid: 2,
								Other: [...]byte{
									0x02, 0x00, 0x00, 0x00,
									0x00, 0x00, 0x00, 0x00,
									0x00, 0x00, 0x00, 0x00,
								},
							},
							Cinfo: nfsv4_xdr.ChangeInfo4{
								Atomic: true,
								Before: 0xe14a85736f0883da,
								After:  0x4ca3dee3552b1b05,
							},
							Rflags:     nfsv4_xdr.OPEN4_RESULT_LOCKTYPE_POSIX | nfsv4_xdr.OPEN4_RESULT_PRESERVE_UNLINKED,
							Attrset:    nfsv4_xdr.Bitmap4{},
							Delegation: &nfsv4_xdr.OpenDelegation4_OPEN_DELEGATE_NONE{},
						},
					},
				},
				&nfsv4_xdr.NfsResop4_OP_GETFH{
					Opgetfh: &nfsv4_xdr.Getfh4res_NFS4_OK{
						Resok4: nfsv4_xdr.Getfh4resok{
							Object: nfsv4_xdr.NfsFh4{0x4c, 0xa3, 0xde, 0xe3, 0x55, 0x2b, 0x1b, 0x05},
						},
					},
				},
				&nfsv4_xdr.NfsResop4_OP_RESTOREFH{
					Oprestorefh: nfsv4_xdr.Restorefh4res{
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

		clock.EXPECT().Now().Return(time.Unix(1007, 0)).Times(2)
		leaf.EXPECT().VirtualClose(virtual.ShareMaskRead)

		res, err = program.NfsV4Nfsproc4Compound(ctx, &nfsv4_xdr.Compound4args{
			Tag:          "close",
			Minorversion: 1,
			Argarray: []nfsv4_xdr.NfsArgop4{
				&nfsv4_xdr.NfsArgop4_OP_SEQUENCE{
					Opsequence: nfsv4_xdr.Sequence4args{
						SaSessionid: [...]byte{
							0x07, 0xcc, 0xb4, 0x52, 0x2d, 0x64, 0xa1, 0xf0,
							0x9f, 0xaa, 0x61, 0x6b, 0x7a, 0x63, 0xf7, 0xaa,
						},
						SaSequenceid:    16,
						SaSlotid:        0,
						SaHighestSlotid: 0,
					},
				},
				&nfsv4_xdr.NfsArgop4_OP_PUTFH{
					Opputfh: nfsv4_xdr.Putfh4args{
						Object: nfsv4_xdr.NfsFh4{0x4c, 0xa3, 0xde, 0xe3, 0x55, 0x2b, 0x1b, 0x05},
					},
				},
				&nfsv4_xdr.NfsArgop4_OP_CLOSE{
					Opclose: nfsv4_xdr.Close4args{
						OpenStateid: nfsv4_xdr.Stateid4{
							Seqid: 2,
							Other: [...]byte{
								0x02, 0x00, 0x00, 0x00,
								0x00, 0x00, 0x00, 0x00,
								0x00, 0x00, 0x00, 0x00,
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
				&nfsv4_xdr.NfsResop4_OP_SEQUENCE{
					Opsequence: &nfsv4_xdr.Sequence4res_NFS4_OK{
						SrResok4: nfsv4_xdr.Sequence4resok{
							SrSessionid: [...]byte{
								0x07, 0xcc, 0xb4, 0x52, 0x2d, 0x64, 0xa1, 0xf0,
								0x9f, 0xaa, 0x61, 0x6b, 0x7a, 0x63, 0xf7, 0xaa,
							},
							SrSequenceid:          16,
							SrSlotid:              0,
							SrHighestSlotid:       99,
							SrTargetHighestSlotid: 99,
							SrStatusFlags:         0,
						},
					},
				},
				&nfsv4_xdr.NfsResop4_OP_PUTFH{
					Opputfh: nfsv4_xdr.Putfh4res{
						Status: nfsv4_xdr.NFS4_OK,
					},
				},
				&nfsv4_xdr.NfsResop4_OP_CLOSE{
					Opclose: &nfsv4_xdr.Close4res_NFS4_OK{
						OpenStateid: nfsv4_xdr.Stateid4{
							Seqid: nfsv4_xdr.NFS4_UINT32_MAX,
							Other: [...]byte{
								0x00, 0x00, 0x00, 0x00,
								0x00, 0x00, 0x00, 0x00,
								0x00, 0x00, 0x00, 0x00,
							},
						},
					},
				},
			},
			Status: nfsv4_xdr.NFS4_OK,
		}, res)
	})

	// Invoke SEQUENCE at a time far in the future. This should
	// cause nothing to happen, because the file opened above has
	// already been closed.
	clock.EXPECT().Now().Return(time.Unix(10000, 0))

	res, err := program.NfsV4Nfsproc4Compound(ctx, &nfsv4_xdr.Compound4args{
		Tag:          "sequence",
		Minorversion: 1,
		Argarray: []nfsv4_xdr.NfsArgop4{
			&nfsv4_xdr.NfsArgop4_OP_SEQUENCE{
				Opsequence: nfsv4_xdr.Sequence4args{
					SaSessionid: [...]byte{
						0x07, 0xcc, 0xb4, 0x52, 0x2d, 0x64, 0xa1, 0xf0,
						0x9f, 0xaa, 0x61, 0x6b, 0x7a, 0x63, 0xf7, 0xaa,
					},
					SaSequenceid:    15,
					SaSlotid:        0,
					SaHighestSlotid: 0,
				},
			},
		},
	})
	require.NoError(t, err)
	require.Equal(t, &nfsv4_xdr.Compound4res{
		Tag: "sequence",
		Resarray: []nfsv4_xdr.NfsResop4{
			&nfsv4_xdr.NfsResop4_OP_SEQUENCE{
				Opsequence: &nfsv4_xdr.Sequence4res_default{
					SrStatus: nfsv4_xdr.NFS4ERR_BADSESSION,
				},
			},
		},
		Status: nfsv4_xdr.NFS4ERR_BADSESSION,
	}, res)
}

func TestNFS41ProgramCompound_OP_CREATE_SESSION(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	rootDirectory := mock.NewMockVirtualDirectory(ctrl)
	rootDirectory.EXPECT().VirtualGetAttributes(gomock.Any(), virtual.AttributesMaskFileHandle, gomock.Any()).
		Do(func(ctx context.Context, requested virtual.AttributesMask, attributes *virtual.Attributes) {
			attributes.SetFileHandle([]byte{0xa9, 0x95, 0x05, 0xae, 0xa8, 0xbe, 0xe7, 0x95})
		})
	handleResolver := mock.NewMockHandleResolver(ctrl)
	serverOwner := nfsv4_xdr.ServerOwner4{
		SoMinorId: 0x9f661ac6ffcbbbf8,
		SoMajorId: []byte{0x64, 0xdb, 0xc0, 0x7d, 0xe7, 0xa4, 0x96, 0x61},
	}
	serverScope := []byte{0x0c, 0xae, 0x58, 0x9d, 0xde, 0x5c, 0x0c, 0x79}
	randomNumberGenerator := mock.NewMockSingleThreadedGenerator(ctrl)
	rebootVerifier := nfsv4_xdr.Verifier4{0x62, 0x32, 0xde, 0x8b, 0x43, 0x94, 0x86, 0xc9}
	clock := mock.NewMockClock(ctrl)
	program := nfsv4.NewNFS41Program(
		rootDirectory,
		nfsv4.NewOpenedFilesPool(handleResolver.Call),
		serverOwner,
		serverScope,
		&nfsv4_xdr.ChannelAttrs4{
			CaHeaderpadsize:         0,
			CaMaxrequestsize:        1 * 1024 * 1024,
			CaMaxresponsesize:       2 * 1024 * 1024,
			CaMaxresponsesizeCached: 64 * 1024,
			CaMaxoperations:         1000,
			CaMaxrequests:           10000,
		},
		randomNumberGenerator,
		rebootVerifier,
		clock,
		/* enforcedLeaseTime = */ 2*time.Minute,
		/* announcedLeaseTime = */ time.Minute,
	)

	t.Run("NotOnlyOperation", func(t *testing.T) {
		// If CREATE_SESSION is sent without a preceding
		// SEQUENCE, then it MUST be the only operation in the
		// COMPOUND procedure's request. If it is not, the
		// server MUST return NFS4ERR_NOT_ONLY_OP.
		res, err := program.NfsV4Nfsproc4Compound(ctx, &nfsv4_xdr.Compound4args{
			Tag:          "create_session",
			Minorversion: 1,
			Argarray: []nfsv4_xdr.NfsArgop4{
				&nfsv4_xdr.NfsArgop4_OP_CREATE_SESSION{
					OpcreateSession: nfsv4_xdr.CreateSession4args{
						CsaClientid: 0xafc2db47eaa7358b,
						CsaSequence: 0x821049b1,
						CsaFlags:    nfsv4_xdr.CREATE_SESSION4_FLAG_PERSIST,
						CsaForeChanAttrs: nfsv4_xdr.ChannelAttrs4{
							CaHeaderpadsize:         512,
							CaMaxrequestsize:        4 * 1024 * 1024,
							CaMaxresponsesize:       6 * 1024 * 1024,
							CaMaxresponsesizeCached: 8 * 1024 * 1024,
							CaMaxoperations:         100000,
							CaMaxrequests:           1000000,
						},
						CsaBackChanAttrs: nfsv4_xdr.ChannelAttrs4{
							CaHeaderpadsize:         1024,
							CaMaxrequestsize:        8 * 1024 * 1024,
							CaMaxresponsesize:       12 * 1024 * 1024,
							CaMaxresponsesizeCached: 16 * 1024 * 1024,
							CaMaxoperations:         100000,
							CaMaxrequests:           1000000,
						},
					},
				},
				&nfsv4_xdr.NfsArgop4_OP_PUTROOTFH{},
			},
		})
		require.NoError(t, err)
		require.Equal(t, &nfsv4_xdr.Compound4res{
			Tag: "create_session",
			Resarray: []nfsv4_xdr.NfsResop4{
				&nfsv4_xdr.NfsResop4_OP_CREATE_SESSION{
					OpcreateSession: &nfsv4_xdr.CreateSession4res_default{
						CsrStatus: nfsv4_xdr.NFS4ERR_NOT_ONLY_OP,
					},
				},
			},
			Status: nfsv4_xdr.NFS4ERR_NOT_ONLY_OP,
		}, res)
	})

	t.Run("StaleClientID", func(t *testing.T) {
		// Clients must first call EXCHANGE_ID to obtain a
		// client ID. Calling CREATE_SESSION with an arbitrary
		// client ID shouldn't work.
		clock.EXPECT().Now().Return(time.Unix(1000, 0))

		res, err := program.NfsV4Nfsproc4Compound(ctx, &nfsv4_xdr.Compound4args{
			Tag:          "create_session",
			Minorversion: 1,
			Argarray: []nfsv4_xdr.NfsArgop4{
				&nfsv4_xdr.NfsArgop4_OP_CREATE_SESSION{
					OpcreateSession: nfsv4_xdr.CreateSession4args{
						CsaClientid: 0xc370efda2a0232c6,
						CsaSequence: 0x9fdad1d4,
						CsaFlags:    nfsv4_xdr.CREATE_SESSION4_FLAG_PERSIST,
						CsaForeChanAttrs: nfsv4_xdr.ChannelAttrs4{
							CaHeaderpadsize:         512,
							CaMaxrequestsize:        4 * 1024 * 1024,
							CaMaxresponsesize:       6 * 1024 * 1024,
							CaMaxresponsesizeCached: 8 * 1024 * 1024,
							CaMaxoperations:         100000,
							CaMaxrequests:           1000000,
						},
						CsaBackChanAttrs: nfsv4_xdr.ChannelAttrs4{
							CaHeaderpadsize:         1024,
							CaMaxrequestsize:        8 * 1024 * 1024,
							CaMaxresponsesize:       12 * 1024 * 1024,
							CaMaxresponsesizeCached: 16 * 1024 * 1024,
							CaMaxoperations:         100000,
							CaMaxrequests:           1000000,
						},
					},
				},
			},
		})
		require.NoError(t, err)
		require.Equal(t, &nfsv4_xdr.Compound4res{
			Tag: "create_session",
			Resarray: []nfsv4_xdr.NfsResop4{
				&nfsv4_xdr.NfsResop4_OP_CREATE_SESSION{
					OpcreateSession: &nfsv4_xdr.CreateSession4res_default{
						CsrStatus: nfsv4_xdr.NFS4ERR_STALE_CLIENTID,
					},
				},
			},
			Status: nfsv4_xdr.NFS4ERR_STALE_CLIENTID,
		}, res)
	})

	// The remainder of this test assumes we have a valid client ID.
	clock.EXPECT().Now().Return(time.Unix(1001, 0))
	randomNumberGenerator.EXPECT().Uint64().Return(uint64(0x3d07dee448db2e68))
	randomNumberGenerator.EXPECT().Uint32().Return(uint32(0x1e0423a9))

	res, err := program.NfsV4Nfsproc4Compound(ctx, &nfsv4_xdr.Compound4args{
		Tag:          "exchange_id",
		Minorversion: 1,
		Argarray: []nfsv4_xdr.NfsArgop4{
			&nfsv4_xdr.NfsArgop4_OP_EXCHANGE_ID{
				OpexchangeId: nfsv4_xdr.ExchangeId4args{
					EiaClientowner: nfsv4_xdr.ClientOwner4{
						CoVerifier: nfsv4_xdr.Verifier4{0x4e, 0x65, 0x5a, 0x19, 0x44, 0x87, 0xdc, 0xa7},
						CoOwnerid:  []byte{0x23, 0x19, 0x19, 0x4c, 0x52, 0xfa, 0x5a, 0xaa},
					},
					EiaFlags:        0,
					EiaStateProtect: &nfsv4_xdr.StateProtect4A_SP4_NONE{},
				},
			},
		},
	})
	require.NoError(t, err)
	require.Equal(t, &nfsv4_xdr.Compound4res{
		Tag: "exchange_id",
		Resarray: []nfsv4_xdr.NfsResop4{
			&nfsv4_xdr.NfsResop4_OP_EXCHANGE_ID{
				OpexchangeId: &nfsv4_xdr.ExchangeId4res_NFS4_OK{
					EirResok4: nfsv4_xdr.ExchangeId4resok{
						EirClientid:     0x3d07dee448db2e68,
						EirSequenceid:   0x1e0423aa,
						EirFlags:        nfsv4_xdr.EXCHGID4_FLAG_USE_NON_PNFS,
						EirStateProtect: &nfsv4_xdr.StateProtect4R_SP4_NONE{},
						EirServerOwner:  serverOwner,
						EirServerScope:  serverScope,
						EirServerImplId: []nfsv4_xdr.NfsImplId4{{
							NiiDomain: "buildbarn.github.io",
						}},
					},
				},
			},
		},
		Status: nfsv4_xdr.NFS4_OK,
	}, res)

	t.Run("MisorderedSequence", func(t *testing.T) {
		// If the sequence ID is any different from the value
		// returned by EXCHANGE_ID, the initial call to
		// CREATE_SESSION should fail with
		// NFS4ERR_SEQ_MISORDERED.
		for sequenceID := nfsv4_xdr.Sequenceid4(0x1e04239a); sequenceID < 0x1e0423ba; sequenceID++ {
			if sequenceID != 0x1e0423aa {
				clock.EXPECT().Now().Return(time.Unix(1002, 0))

				res, err := program.NfsV4Nfsproc4Compound(ctx, &nfsv4_xdr.Compound4args{
					Tag:          "create_session",
					Minorversion: 1,
					Argarray: []nfsv4_xdr.NfsArgop4{
						&nfsv4_xdr.NfsArgop4_OP_CREATE_SESSION{
							OpcreateSession: nfsv4_xdr.CreateSession4args{
								CsaClientid: 0x3d07dee448db2e68,
								CsaSequence: sequenceID,
								CsaFlags:    0,
								CsaForeChanAttrs: nfsv4_xdr.ChannelAttrs4{
									CaHeaderpadsize:         512,
									CaMaxrequestsize:        4 * 1024 * 1024,
									CaMaxresponsesize:       6 * 1024 * 1024,
									CaMaxresponsesizeCached: 8 * 1024 * 1024,
									CaMaxoperations:         100000,
									CaMaxrequests:           1000000,
								},
								CsaBackChanAttrs: nfsv4_xdr.ChannelAttrs4{
									CaHeaderpadsize:         1024,
									CaMaxrequestsize:        8 * 1024 * 1024,
									CaMaxresponsesize:       12 * 1024 * 1024,
									CaMaxresponsesizeCached: 16 * 1024 * 1024,
									CaMaxoperations:         100000,
									CaMaxrequests:           1000000,
								},
							},
						},
					},
				})
				require.NoError(t, err)
				require.Equal(t, &nfsv4_xdr.Compound4res{
					Tag: "create_session",
					Resarray: []nfsv4_xdr.NfsResop4{
						&nfsv4_xdr.NfsResop4_OP_CREATE_SESSION{
							OpcreateSession: &nfsv4_xdr.CreateSession4res_default{
								CsrStatus: nfsv4_xdr.NFS4ERR_SEQ_MISORDERED,
							},
						},
					},
					Status: nfsv4_xdr.NFS4ERR_SEQ_MISORDERED,
				}, res)
			}
		}
	})

	t.Run("Success", func(t *testing.T) {
		// Use the client ID to create ten sessions. Also call
		// SESSION_CREATE for each session multiple times, to
		// make sure it's idempotent.
		for i := 0; i < 10; i++ {
			randomNumberGenerator.EXPECT().Read(gomock.Len(nfsv4_xdr.NFS4_SESSIONID_SIZE)).
				DoAndReturn(func(p []byte) (int, error) {
					p[0] = byte(i)
					return len(p), nil
				})

			for j := 0; j < 10; j++ {
				clock.EXPECT().Now().Return(time.Unix(1003, 0))

				res, err := program.NfsV4Nfsproc4Compound(ctx, &nfsv4_xdr.Compound4args{
					Tag:          "create_session",
					Minorversion: 1,
					Argarray: []nfsv4_xdr.NfsArgop4{
						&nfsv4_xdr.NfsArgop4_OP_CREATE_SESSION{
							OpcreateSession: nfsv4_xdr.CreateSession4args{
								CsaClientid: 0x3d07dee448db2e68,
								CsaSequence: nfsv4_xdr.Sequenceid4(i) + 0x1e0423aa,
								CsaFlags:    0,
								CsaForeChanAttrs: nfsv4_xdr.ChannelAttrs4{
									CaHeaderpadsize:         512,
									CaMaxrequestsize:        4 * 1024 * 1024,
									CaMaxresponsesize:       6 * 1024 * 1024,
									CaMaxresponsesizeCached: 8 * 1024 * 1024,
									CaMaxoperations:         100000,
									CaMaxrequests:           1000000,
								},
								CsaBackChanAttrs: nfsv4_xdr.ChannelAttrs4{
									CaHeaderpadsize:         1024,
									CaMaxrequestsize:        8 * 1024 * 1024,
									CaMaxresponsesize:       12 * 1024 * 1024,
									CaMaxresponsesizeCached: 16 * 1024 * 1024,
									CaMaxoperations:         100000,
									CaMaxrequests:           1000000,
								},
							},
						},
					},
				})
				require.NoError(t, err)
				require.Equal(t, &nfsv4_xdr.Compound4res{
					Tag: "create_session",
					Resarray: []nfsv4_xdr.NfsResop4{
						&nfsv4_xdr.NfsResop4_OP_CREATE_SESSION{
							OpcreateSession: &nfsv4_xdr.CreateSession4res_NFS4_OK{
								CsrResok4: nfsv4_xdr.CreateSession4resok{
									CsrSessionid: [nfsv4_xdr.NFS4_SESSIONID_SIZE]byte{byte(i)},
									CsrSequence:  nfsv4_xdr.Sequenceid4(i) + 0x1e0423aa,
									CsrFlags:     0,
									CsrForeChanAttrs: nfsv4_xdr.ChannelAttrs4{
										CaHeaderpadsize:         0,
										CaMaxrequestsize:        1 * 1024 * 1024,
										CaMaxresponsesize:       2 * 1024 * 1024,
										CaMaxresponsesizeCached: 64 * 1024,
										CaMaxoperations:         1000,
										CaMaxrequests:           10000,
									},
									CsrBackChanAttrs: nfsv4_xdr.ChannelAttrs4{
										CaHeaderpadsize:         1024,
										CaMaxrequestsize:        8 * 1024 * 1024,
										CaMaxresponsesize:       12 * 1024 * 1024,
										CaMaxresponsesizeCached: 16 * 1024 * 1024,
										CaMaxoperations:         100000,
										CaMaxrequests:           1000000,
									},
								},
							},
						},
					},
					Status: nfsv4_xdr.NFS4_OK,
				}, res)
			}
		}
	})

	t.Run("ClientExpired", func(t *testing.T) {
		// After a sufficient amount of time has passed, the
		// client ID will no longer be valid.
		clock.EXPECT().Now().Return(time.Unix(1124, 0))

		res, err := program.NfsV4Nfsproc4Compound(ctx, &nfsv4_xdr.Compound4args{
			Tag:          "create_session",
			Minorversion: 1,
			Argarray: []nfsv4_xdr.NfsArgop4{
				&nfsv4_xdr.NfsArgop4_OP_CREATE_SESSION{
					OpcreateSession: nfsv4_xdr.CreateSession4args{
						CsaClientid: 0x3d07dee448db2e68,
						CsaSequence: 0x1e0423b4,
						CsaFlags:    0,
						CsaForeChanAttrs: nfsv4_xdr.ChannelAttrs4{
							CaHeaderpadsize:         512,
							CaMaxrequestsize:        4 * 1024 * 1024,
							CaMaxresponsesize:       6 * 1024 * 1024,
							CaMaxresponsesizeCached: 8 * 1024 * 1024,
							CaMaxoperations:         100000,
							CaMaxrequests:           1000000,
						},
						CsaBackChanAttrs: nfsv4_xdr.ChannelAttrs4{
							CaHeaderpadsize:         1024,
							CaMaxrequestsize:        8 * 1024 * 1024,
							CaMaxresponsesize:       12 * 1024 * 1024,
							CaMaxresponsesizeCached: 16 * 1024 * 1024,
							CaMaxoperations:         100000,
							CaMaxrequests:           1000000,
						},
					},
				},
			},
		})
		require.NoError(t, err)
		require.Equal(t, &nfsv4_xdr.Compound4res{
			Tag: "create_session",
			Resarray: []nfsv4_xdr.NfsResop4{
				&nfsv4_xdr.NfsResop4_OP_CREATE_SESSION{
					OpcreateSession: &nfsv4_xdr.CreateSession4res_default{
						CsrStatus: nfsv4_xdr.NFS4ERR_STALE_CLIENTID,
					},
				},
			},
			Status: nfsv4_xdr.NFS4ERR_STALE_CLIENTID,
		}, res)
	})
}

func TestNFS41ProgramCompound_OP_DESTROY_CLIENTID(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	rootDirectory := mock.NewMockVirtualDirectory(ctrl)
	rootDirectory.EXPECT().VirtualGetAttributes(gomock.Any(), virtual.AttributesMaskFileHandle, gomock.Any()).
		Do(func(ctx context.Context, requested virtual.AttributesMask, attributes *virtual.Attributes) {
			attributes.SetFileHandle([]byte{0x3a, 0x76, 0x6c, 0x6c, 0x27, 0x72, 0x19, 0x62})
		})
	handleResolver := mock.NewMockHandleResolver(ctrl)
	serverOwner := nfsv4_xdr.ServerOwner4{
		SoMinorId: 0xaa948cec30357b88,
		SoMajorId: []byte{0x4c, 0xf9, 0x86, 0x0b, 0x5c, 0x08, 0x52, 0x13},
	}
	serverScope := []byte{0xb1, 0x2c, 0xcd, 0xf9, 0x70, 0x3e, 0x37, 0x59}
	randomNumberGenerator := mock.NewMockSingleThreadedGenerator(ctrl)
	rebootVerifier := nfsv4_xdr.Verifier4{0xfa, 0xd6, 0x5c, 0xd6, 0xb2, 0x4b, 0x0b, 0x51}
	clock := mock.NewMockClock(ctrl)
	program := nfsv4.NewNFS41Program(
		rootDirectory,
		nfsv4.NewOpenedFilesPool(handleResolver.Call),
		serverOwner,
		serverScope,
		&nfsv4_xdr.ChannelAttrs4{
			CaHeaderpadsize:         0,
			CaMaxrequestsize:        1 * 1024 * 1024,
			CaMaxresponsesize:       2 * 1024 * 1024,
			CaMaxresponsesizeCached: 64 * 1024,
			CaMaxoperations:         100,
			CaMaxrequests:           100,
		},
		randomNumberGenerator,
		rebootVerifier,
		clock,
		/* enforcedLeaseTime = */ 2*time.Minute,
		/* announcedLeaseTime = */ time.Minute,
	)

	t.Run("StaleClientID", func(t *testing.T) {
		clock.EXPECT().Now().Return(time.Unix(1000, 0))

		res, err := program.NfsV4Nfsproc4Compound(ctx, &nfsv4_xdr.Compound4args{
			Tag:          "destroy_clientid",
			Minorversion: 1,
			Argarray: []nfsv4_xdr.NfsArgop4{
				&nfsv4_xdr.NfsArgop4_OP_DESTROY_CLIENTID{
					OpdestroyClientid: nfsv4_xdr.DestroyClientid4args{
						DcaClientid: 0xd2653b56bc82b37a,
					},
				},
			},
		})
		require.NoError(t, err)
		require.Equal(t, &nfsv4_xdr.Compound4res{
			Tag: "destroy_clientid",
			Resarray: []nfsv4_xdr.NfsResop4{
				&nfsv4_xdr.NfsResop4_OP_DESTROY_CLIENTID{
					OpdestroyClientid: nfsv4_xdr.DestroyClientid4res{
						DcrStatus: nfsv4_xdr.NFS4ERR_STALE_CLIENTID,
					},
				},
			},
			Status: nfsv4_xdr.NFS4ERR_STALE_CLIENTID,
		}, res)
	})

	t.Run("DestroyUnconfirmed", func(t *testing.T) {
		clock.EXPECT().Now().Return(time.Unix(1001, 0))
		randomNumberGenerator.EXPECT().Uint64().Return(uint64(0x1c985fa22cd237d3))
		randomNumberGenerator.EXPECT().Uint32().Return(uint32(0x909bf041))

		res, err := program.NfsV4Nfsproc4Compound(ctx, &nfsv4_xdr.Compound4args{
			Tag:          "exchange_id",
			Minorversion: 1,
			Argarray: []nfsv4_xdr.NfsArgop4{
				&nfsv4_xdr.NfsArgop4_OP_EXCHANGE_ID{
					OpexchangeId: nfsv4_xdr.ExchangeId4args{
						EiaClientowner: nfsv4_xdr.ClientOwner4{
							CoVerifier: nfsv4_xdr.Verifier4{0x1f, 0xad, 0xe3, 0x67, 0x5d, 0xb6, 0xeb, 0xd8},
							CoOwnerid:  []byte{0x61, 0x42, 0xa0, 0x99, 0xc0, 0xf6, 0xf3, 0x2a},
						},
						EiaStateProtect: &nfsv4_xdr.StateProtect4A_SP4_NONE{},
					},
				},
			},
		})
		require.NoError(t, err)
		require.Equal(t, &nfsv4_xdr.Compound4res{
			Tag: "exchange_id",
			Resarray: []nfsv4_xdr.NfsResop4{
				&nfsv4_xdr.NfsResop4_OP_EXCHANGE_ID{
					OpexchangeId: &nfsv4_xdr.ExchangeId4res_NFS4_OK{
						EirResok4: nfsv4_xdr.ExchangeId4resok{
							EirClientid:     0x1c985fa22cd237d3,
							EirSequenceid:   0x909bf042,
							EirFlags:        nfsv4_xdr.EXCHGID4_FLAG_USE_NON_PNFS,
							EirStateProtect: &nfsv4_xdr.StateProtect4R_SP4_NONE{},
							EirServerOwner:  serverOwner,
							EirServerScope:  serverScope,
							EirServerImplId: []nfsv4_xdr.NfsImplId4{{
								NiiDomain: "buildbarn.github.io",
							}},
						},
					},
				},
			},
			Status: nfsv4_xdr.NFS4_OK,
		}, res)

		clock.EXPECT().Now().Return(time.Unix(1002, 0))

		res, err = program.NfsV4Nfsproc4Compound(ctx, &nfsv4_xdr.Compound4args{
			Tag:          "destroy_clientid",
			Minorversion: 1,
			Argarray: []nfsv4_xdr.NfsArgop4{
				&nfsv4_xdr.NfsArgop4_OP_DESTROY_CLIENTID{
					OpdestroyClientid: nfsv4_xdr.DestroyClientid4args{
						DcaClientid: 0x1c985fa22cd237d3,
					},
				},
			},
		})
		require.NoError(t, err)
		require.Equal(t, &nfsv4_xdr.Compound4res{
			Tag: "destroy_clientid",
			Resarray: []nfsv4_xdr.NfsResop4{
				&nfsv4_xdr.NfsResop4_OP_DESTROY_CLIENTID{
					OpdestroyClientid: nfsv4_xdr.DestroyClientid4res{
						DcrStatus: nfsv4_xdr.NFS4_OK,
					},
				},
			},
			Status: nfsv4_xdr.NFS4_OK,
		}, res)
	})
}

func TestNFS41ProgramCompound_OP_DESTROY_SESSION(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	rootDirectory := mock.NewMockVirtualDirectory(ctrl)
	rootDirectory.EXPECT().VirtualGetAttributes(gomock.Any(), virtual.AttributesMaskFileHandle, gomock.Any()).
		Do(func(ctx context.Context, requested virtual.AttributesMask, attributes *virtual.Attributes) {
			attributes.SetFileHandle([]byte{0x76, 0x12, 0xb7, 0xa6, 0x9f, 0x54, 0x21, 0x7d})
		})
	handleResolver := mock.NewMockHandleResolver(ctrl)
	serverOwner := nfsv4_xdr.ServerOwner4{
		SoMinorId: 0x9e2612357330543f,
		SoMajorId: []byte{0x4c, 0xf9, 0x86, 0x0b, 0x5c, 0x08, 0x52, 0x13},
	}
	serverScope := []byte{0xf9, 0x06, 0x76, 0x70, 0x91, 0x92, 0xf0, 0x92}
	randomNumberGenerator := mock.NewMockSingleThreadedGenerator(ctrl)
	rebootVerifier := nfsv4_xdr.Verifier4{0xfa, 0xd6, 0x5c, 0xd6, 0xb2, 0x4b, 0x0b, 0x51}
	clock := mock.NewMockClock(ctrl)
	program := nfsv4.NewNFS41Program(
		rootDirectory,
		nfsv4.NewOpenedFilesPool(handleResolver.Call),
		serverOwner,
		serverScope,
		&nfsv4_xdr.ChannelAttrs4{
			CaHeaderpadsize:         0,
			CaMaxrequestsize:        1 * 1024 * 1024,
			CaMaxresponsesize:       2 * 1024 * 1024,
			CaMaxresponsesizeCached: 64 * 1024,
			CaMaxoperations:         100,
			CaMaxrequests:           100,
		},
		randomNumberGenerator,
		rebootVerifier,
		clock,
		/* enforcedLeaseTime = */ 2*time.Minute,
		/* announcedLeaseTime = */ time.Minute,
	)

	t.Run("BadSession", func(t *testing.T) {
		// Attempt to call DESTROY_SESSION against a nonexistent
		// session.
		clock.EXPECT().Now().Return(time.Unix(1000, 0))

		res, err := program.NfsV4Nfsproc4Compound(ctx, &nfsv4_xdr.Compound4args{
			Tag:          "destroy_session",
			Minorversion: 1,
			Argarray: []nfsv4_xdr.NfsArgop4{
				&nfsv4_xdr.NfsArgop4_OP_DESTROY_SESSION{
					OpdestroySession: nfsv4_xdr.DestroySession4args{
						DsaSessionid: [...]byte{
							0x38, 0x24, 0xcb, 0xea, 0xc6, 0x1f, 0xf5, 0xe3,
							0xb4, 0x1d, 0x88, 0x0d, 0x4f, 0xdc, 0x82, 0x57,
						},
					},
				},
			},
		})
		require.NoError(t, err)
		require.Equal(t, &nfsv4_xdr.Compound4res{
			Tag: "destroy_session",
			Resarray: []nfsv4_xdr.NfsResop4{
				&nfsv4_xdr.NfsResop4_OP_DESTROY_SESSION{
					OpdestroySession: nfsv4_xdr.DestroySession4res{
						DsrStatus: nfsv4_xdr.NFS4ERR_BADSESSION,
					},
				},
			},
			Status: nfsv4_xdr.NFS4ERR_BADSESSION,
		}, res)
	})

	clock.EXPECT().Now().Return(time.Unix(1001, 0)).Times(2)
	exchangeIDAndCreateSessionForTesting(
		ctx,
		t,
		serverOwner,
		serverScope,
		randomNumberGenerator,
		program,
		/* sessionID = */ [...]byte{
			0xa7, 0x87, 0xbb, 0xbf, 0xdd, 0x68, 0xb3, 0xe1,
			0xa5, 0x50, 0x65, 0xd9, 0x4b, 0x42, 0x46, 0x67,
		},
	)

	t.Run("NotOnlyOperation", func(t *testing.T) {
		// When not part of a sequence, DESTROY_SESSION needs to
		// be the only operation.
		res, err := program.NfsV4Nfsproc4Compound(ctx, &nfsv4_xdr.Compound4args{
			Tag:          "destroy_session",
			Minorversion: 1,
			Argarray: []nfsv4_xdr.NfsArgop4{
				&nfsv4_xdr.NfsArgop4_OP_DESTROY_SESSION{
					OpdestroySession: nfsv4_xdr.DestroySession4args{
						DsaSessionid: [...]byte{
							0xa7, 0x87, 0xbb, 0xbf, 0xdd, 0x68, 0xb3, 0xe1,
							0xa5, 0x50, 0x65, 0xd9, 0x4b, 0x42, 0x46, 0x67,
						},
					},
				},
				&nfsv4_xdr.NfsArgop4_OP_DESTROY_SESSION{
					OpdestroySession: nfsv4_xdr.DestroySession4args{
						DsaSessionid: [...]byte{
							0xa7, 0x87, 0xbb, 0xbf, 0xdd, 0x68, 0xb3, 0xe1,
							0xa5, 0x50, 0x65, 0xd9, 0x4b, 0x42, 0x46, 0x67,
						},
					},
				},
			},
		})
		require.NoError(t, err)
		require.Equal(t, &nfsv4_xdr.Compound4res{
			Tag: "destroy_session",
			Resarray: []nfsv4_xdr.NfsResop4{
				&nfsv4_xdr.NfsResop4_OP_DESTROY_SESSION{
					OpdestroySession: nfsv4_xdr.DestroySession4res{
						DsrStatus: nfsv4_xdr.NFS4ERR_NOT_ONLY_OP,
					},
				},
			},
			Status: nfsv4_xdr.NFS4ERR_NOT_ONLY_OP,
		}, res)
	})

	t.Run("Standalone", func(t *testing.T) {
		// DESTROY_SESSION can be invoked standalone, outside of
		// a sequence.
		clock.EXPECT().Now().Return(time.Unix(1002, 0))

		res, err := program.NfsV4Nfsproc4Compound(ctx, &nfsv4_xdr.Compound4args{
			Tag:          "destroy_session",
			Minorversion: 1,
			Argarray: []nfsv4_xdr.NfsArgop4{
				&nfsv4_xdr.NfsArgop4_OP_DESTROY_SESSION{
					OpdestroySession: nfsv4_xdr.DestroySession4args{
						DsaSessionid: [...]byte{
							0xa7, 0x87, 0xbb, 0xbf, 0xdd, 0x68, 0xb3, 0xe1,
							0xa5, 0x50, 0x65, 0xd9, 0x4b, 0x42, 0x46, 0x67,
						},
					},
				},
			},
		})
		require.NoError(t, err)
		require.Equal(t, &nfsv4_xdr.Compound4res{
			Tag: "destroy_session",
			Resarray: []nfsv4_xdr.NfsResop4{
				&nfsv4_xdr.NfsResop4_OP_DESTROY_SESSION{
					OpdestroySession: nfsv4_xdr.DestroySession4res{
						DsrStatus: nfsv4_xdr.NFS4_OK,
					},
				},
			},
			Status: nfsv4_xdr.NFS4_OK,
		}, res)
	})

	clock.EXPECT().Now().Return(time.Unix(2000, 0)).Times(2)
	exchangeIDAndCreateSessionForTesting(
		ctx,
		t,
		serverOwner,
		serverScope,
		randomNumberGenerator,
		program,
		/* sessionID = */ [...]byte{
			0xef, 0x9f, 0xf1, 0x6e, 0x0c, 0x6a, 0xd0, 0x75,
			0x2c, 0xa5, 0xf7, 0x96, 0xfa, 0x62, 0xc9, 0x87,
		},
	)

	t.Run("AtEndOfSequence", func(t *testing.T) {
		// DESTROY_SESSION can also be invoked as part of a
		// sequence. If it refers to the same session, it needs
		// to be placed at the end.
		clock.EXPECT().Now().Return(time.Unix(2001, 0)).Times(3)

		res, err := program.NfsV4Nfsproc4Compound(ctx, &nfsv4_xdr.Compound4args{
			Tag:          "destroy_session",
			Minorversion: 1,
			Argarray: []nfsv4_xdr.NfsArgop4{
				&nfsv4_xdr.NfsArgop4_OP_SEQUENCE{
					Opsequence: nfsv4_xdr.Sequence4args{
						SaSessionid: [...]byte{
							0xef, 0x9f, 0xf1, 0x6e, 0x0c, 0x6a, 0xd0, 0x75,
							0x2c, 0xa5, 0xf7, 0x96, 0xfa, 0x62, 0xc9, 0x87,
						},
						SaSequenceid:    1,
						SaSlotid:        0,
						SaHighestSlotid: 0,
					},
				},
				&nfsv4_xdr.NfsArgop4_OP_PUTROOTFH{},
				&nfsv4_xdr.NfsArgop4_OP_DESTROY_SESSION{
					OpdestroySession: nfsv4_xdr.DestroySession4args{
						DsaSessionid: [...]byte{
							0xef, 0x9f, 0xf1, 0x6e, 0x0c, 0x6a, 0xd0, 0x75,
							0x2c, 0xa5, 0xf7, 0x96, 0xfa, 0x62, 0xc9, 0x87,
						},
					},
				},
			},
		})
		require.NoError(t, err)
		require.Equal(t, &nfsv4_xdr.Compound4res{
			Tag: "destroy_session",
			Resarray: []nfsv4_xdr.NfsResop4{
				&nfsv4_xdr.NfsResop4_OP_SEQUENCE{
					Opsequence: &nfsv4_xdr.Sequence4res_NFS4_OK{
						SrResok4: nfsv4_xdr.Sequence4resok{
							SrSessionid: [...]byte{
								0xef, 0x9f, 0xf1, 0x6e, 0x0c, 0x6a, 0xd0, 0x75,
								0x2c, 0xa5, 0xf7, 0x96, 0xfa, 0x62, 0xc9, 0x87,
							},
							SrSequenceid:          1,
							SrSlotid:              0,
							SrHighestSlotid:       99,
							SrTargetHighestSlotid: 99,
							SrStatusFlags:         0,
						},
					},
				},
				&nfsv4_xdr.NfsResop4_OP_PUTROOTFH{
					Opputrootfh: nfsv4_xdr.Putrootfh4res{
						Status: nfsv4_xdr.NFS4_OK,
					},
				},
				&nfsv4_xdr.NfsResop4_OP_DESTROY_SESSION{
					OpdestroySession: nfsv4_xdr.DestroySession4res{
						DsrStatus: nfsv4_xdr.NFS4_OK,
					},
				},
			},
			Status: nfsv4_xdr.NFS4_OK,
		}, res)
	})
}

func TestNFS41ProgramCompound_OP_EXCHANGE_ID(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	rootDirectory := mock.NewMockVirtualDirectory(ctrl)
	rootDirectory.EXPECT().VirtualGetAttributes(gomock.Any(), virtual.AttributesMaskFileHandle, gomock.Any()).
		Do(func(ctx context.Context, requested virtual.AttributesMask, attributes *virtual.Attributes) {
			attributes.SetFileHandle([]byte{0x53, 0x58, 0x67, 0x6d, 0x87, 0xa7, 0x8c, 0x8d})
		})
	handleResolver := mock.NewMockHandleResolver(ctrl)
	serverOwner := nfsv4_xdr.ServerOwner4{
		SoMinorId: 0xdfa5a23a7dd7c485,
		SoMajorId: []byte{0x78, 0xbd, 0xe6, 0x9d, 0x75, 0x5e, 0xa4, 0xfc},
	}
	serverScope := []byte{0xa2, 0x6a, 0x18, 0xfc, 0x2b, 0x40, 0x90, 0x88}
	randomNumberGenerator := mock.NewMockSingleThreadedGenerator(ctrl)
	rebootVerifier := nfsv4_xdr.Verifier4{0x5f, 0xb9, 0x4e, 0x19, 0xaa, 0x9a, 0xd2, 0x2e}
	clock := mock.NewMockClock(ctrl)
	program := nfsv4.NewNFS41Program(
		rootDirectory,
		nfsv4.NewOpenedFilesPool(handleResolver.Call),
		serverOwner,
		serverScope,
		&nfsv4_xdr.ChannelAttrs4{
			CaHeaderpadsize:         0,
			CaMaxrequestsize:        1 * 1024 * 1024,
			CaMaxresponsesize:       2 * 1024 * 1024,
			CaMaxresponsesizeCached: 64 * 1024,
			CaMaxoperations:         1000,
			CaMaxrequests:           10000,
		},
		randomNumberGenerator,
		rebootVerifier,
		clock,
		/* enforcedLeaseTime = */ 2*time.Minute,
		/* announcedLeaseTime = */ time.Minute,
	)

	t.Run("NotOnlyOperation", func(t *testing.T) {
		// If EXCHANGE_ID is sent without a preceding SEQUENCE,
		// then it MUST be the only operation in the COMPOUND
		// procedure's request. If it is not, the server MUST
		// return NFS4ERR_NOT_ONLY_OP.
		res, err := program.NfsV4Nfsproc4Compound(ctx, &nfsv4_xdr.Compound4args{
			Tag:          "exchange_id",
			Minorversion: 1,
			Argarray: []nfsv4_xdr.NfsArgop4{
				&nfsv4_xdr.NfsArgop4_OP_EXCHANGE_ID{
					OpexchangeId: nfsv4_xdr.ExchangeId4args{
						EiaClientowner: nfsv4_xdr.ClientOwner4{
							CoVerifier: nfsv4_xdr.Verifier4{0x8a, 0x45, 0xaa, 0x44, 0xbd, 0x2a, 0x07, 0x41},
							CoOwnerid:  []byte{0xd5, 0x00, 0x59, 0x8e, 0x93, 0xb4, 0x14, 0x62},
						},
						EiaFlags:        nfsv4_xdr.EXCHGID4_FLAG_SUPP_MOVED_REFER,
						EiaStateProtect: &nfsv4_xdr.StateProtect4A_SP4_NONE{},
						EiaClientImplId: []nfsv4_xdr.NfsImplId4{{
							NiiDomain: "freebsd.org",
							NiiName:   "14.1-RELEASE",
							NiiDate:   nfsv4_xdr.Nfstime4{Seconds: 1293840000},
						}},
					},
				},
				&nfsv4_xdr.NfsArgop4_OP_PUTROOTFH{},
			},
		})
		require.NoError(t, err)
		require.Equal(t, &nfsv4_xdr.Compound4res{
			Tag: "exchange_id",
			Resarray: []nfsv4_xdr.NfsResop4{
				&nfsv4_xdr.NfsResop4_OP_EXCHANGE_ID{
					OpexchangeId: &nfsv4_xdr.ExchangeId4res_default{
						EirStatus: nfsv4_xdr.NFS4ERR_NOT_ONLY_OP,
					},
				},
			},
			Status: nfsv4_xdr.NFS4ERR_NOT_ONLY_OP,
		}, res)
	})

	t.Run("SuccessIdempotent", func(t *testing.T) {
		// Calling EXCHANGE_ID with the same client owner
		// repeatedly should cause the server to return the same
		// client ID and sequence ID.
		randomNumberGenerator.EXPECT().Uint64().Return(uint64(0x346a1abf4a4d9caa))
		randomNumberGenerator.EXPECT().Uint32().Return(uint32(0x041fd847))

		for i := 0; i < 10; i++ {
			clock.EXPECT().Now().Return(time.Unix(1000, 0))

			res, err := program.NfsV4Nfsproc4Compound(ctx, &nfsv4_xdr.Compound4args{
				Tag:          "exchange_id",
				Minorversion: 1,
				Argarray: []nfsv4_xdr.NfsArgop4{
					&nfsv4_xdr.NfsArgop4_OP_EXCHANGE_ID{
						OpexchangeId: nfsv4_xdr.ExchangeId4args{
							EiaClientowner: nfsv4_xdr.ClientOwner4{
								CoVerifier: nfsv4_xdr.Verifier4{0x8a, 0x45, 0xaa, 0x44, 0xbd, 0x2a, 0x07, 0x41},
								CoOwnerid:  []byte{0xd5, 0x00, 0x59, 0x8e, 0x93, 0xb4, 0x14, 0x62},
							},
							EiaFlags:        nfsv4_xdr.EXCHGID4_FLAG_USE_NON_PNFS,
							EiaStateProtect: &nfsv4_xdr.StateProtect4A_SP4_NONE{},
						},
					},
				},
			})
			require.NoError(t, err)
			require.Equal(t, &nfsv4_xdr.Compound4res{
				Tag: "exchange_id",
				Resarray: []nfsv4_xdr.NfsResop4{
					&nfsv4_xdr.NfsResop4_OP_EXCHANGE_ID{
						OpexchangeId: &nfsv4_xdr.ExchangeId4res_NFS4_OK{
							EirResok4: nfsv4_xdr.ExchangeId4resok{
								EirClientid:     0x346a1abf4a4d9caa,
								EirSequenceid:   0x041fd848,
								EirFlags:        nfsv4_xdr.EXCHGID4_FLAG_USE_NON_PNFS,
								EirStateProtect: &nfsv4_xdr.StateProtect4R_SP4_NONE{},
								EirServerOwner:  serverOwner,
								EirServerScope:  serverScope,
								EirServerImplId: []nfsv4_xdr.NfsImplId4{{
									NiiDomain: "buildbarn.github.io",
								}},
							},
						},
					},
				},
				Status: nfsv4_xdr.NFS4_OK,
			}, res)
		}
	})

	t.Run("FlagConfirmed", func(t *testing.T) {
		// If a client managed to establish an initial sequence
		// value by calling CREATE_SESSION, subsequent calls to
		// EXCHANGE_ID should set flag EXCHGID4_FLAG_CONFIRMED_R.
		// In that case it's no longer necessary to set
		// eir_sequenceid as part of the response.
		clock.EXPECT().Now().Return(time.Unix(1001, 0))
		randomNumberGenerator.EXPECT().Uint64().Return(uint64(0x353974981fdd3329))
		randomNumberGenerator.EXPECT().Uint32().Return(uint32(0x1e260002))

		res, err := program.NfsV4Nfsproc4Compound(ctx, &nfsv4_xdr.Compound4args{
			Tag:          "exchange_id",
			Minorversion: 1,
			Argarray: []nfsv4_xdr.NfsArgop4{
				&nfsv4_xdr.NfsArgop4_OP_EXCHANGE_ID{
					OpexchangeId: nfsv4_xdr.ExchangeId4args{
						EiaClientowner: nfsv4_xdr.ClientOwner4{
							CoVerifier: nfsv4_xdr.Verifier4{0x46, 0x12, 0xcd, 0xd1, 0x91, 0xda, 0x5f, 0xa4},
							CoOwnerid:  []byte{0xb5, 0x12, 0x21, 0xcd, 0x10, 0x22, 0x9b, 0x20},
						},
						EiaStateProtect: &nfsv4_xdr.StateProtect4A_SP4_NONE{},
					},
				},
			},
		})
		require.NoError(t, err)
		require.Equal(t, &nfsv4_xdr.Compound4res{
			Tag: "exchange_id",
			Resarray: []nfsv4_xdr.NfsResop4{
				&nfsv4_xdr.NfsResop4_OP_EXCHANGE_ID{
					OpexchangeId: &nfsv4_xdr.ExchangeId4res_NFS4_OK{
						EirResok4: nfsv4_xdr.ExchangeId4resok{
							EirClientid:     0x353974981fdd3329,
							EirSequenceid:   0x1e260003,
							EirFlags:        nfsv4_xdr.EXCHGID4_FLAG_USE_NON_PNFS,
							EirStateProtect: &nfsv4_xdr.StateProtect4R_SP4_NONE{},
							EirServerOwner:  serverOwner,
							EirServerScope:  serverScope,
							EirServerImplId: []nfsv4_xdr.NfsImplId4{{
								NiiDomain: "buildbarn.github.io",
							}},
						},
					},
				},
			},
			Status: nfsv4_xdr.NFS4_OK,
		}, res)

		clock.EXPECT().Now().Return(time.Unix(1002, 0))
		randomNumberGenerator.EXPECT().Read(gomock.Len(nfsv4_xdr.NFS4_SESSIONID_SIZE)).
			DoAndReturn(func(p []byte) (int, error) {
				return copy(p, []byte{
					0xd0, 0x6a, 0x80, 0x4e, 0x65, 0xef, 0x33, 0xb0,
					0x7c, 0xde, 0xa0, 0xf6, 0x3c, 0x78, 0xc9, 0xc1,
				}), nil
			})

		res, err = program.NfsV4Nfsproc4Compound(ctx, &nfsv4_xdr.Compound4args{
			Tag:          "create_session",
			Minorversion: 1,
			Argarray: []nfsv4_xdr.NfsArgop4{
				&nfsv4_xdr.NfsArgop4_OP_CREATE_SESSION{
					OpcreateSession: nfsv4_xdr.CreateSession4args{
						CsaClientid: 0x353974981fdd3329,
						CsaSequence: 0x1e260003,
						CsaFlags:    nfsv4_xdr.CREATE_SESSION4_FLAG_PERSIST,
						CsaForeChanAttrs: nfsv4_xdr.ChannelAttrs4{
							CaHeaderpadsize:         512,
							CaMaxrequestsize:        4 * 1024 * 1024,
							CaMaxresponsesize:       6 * 1024 * 1024,
							CaMaxresponsesizeCached: 8 * 1024 * 1024,
							CaMaxoperations:         100000,
							CaMaxrequests:           1000000,
						},
						CsaBackChanAttrs: nfsv4_xdr.ChannelAttrs4{
							CaHeaderpadsize:         1024,
							CaMaxrequestsize:        8 * 1024 * 1024,
							CaMaxresponsesize:       12 * 1024 * 1024,
							CaMaxresponsesizeCached: 16 * 1024 * 1024,
							CaMaxoperations:         100000,
							CaMaxrequests:           1000000,
						},
					},
				},
			},
		})
		require.NoError(t, err)
		require.Equal(t, &nfsv4_xdr.Compound4res{
			Tag: "create_session",
			Resarray: []nfsv4_xdr.NfsResop4{
				&nfsv4_xdr.NfsResop4_OP_CREATE_SESSION{
					OpcreateSession: &nfsv4_xdr.CreateSession4res_NFS4_OK{
						CsrResok4: nfsv4_xdr.CreateSession4resok{
							CsrSessionid: [...]byte{
								0xd0, 0x6a, 0x80, 0x4e, 0x65, 0xef, 0x33, 0xb0,
								0x7c, 0xde, 0xa0, 0xf6, 0x3c, 0x78, 0xc9, 0xc1,
							},
							CsrSequence: 0x1e260003,
							CsrFlags:    nfsv4_xdr.CREATE_SESSION4_FLAG_PERSIST,
							CsrForeChanAttrs: nfsv4_xdr.ChannelAttrs4{
								CaHeaderpadsize:         0,
								CaMaxrequestsize:        1 * 1024 * 1024,
								CaMaxresponsesize:       2 * 1024 * 1024,
								CaMaxresponsesizeCached: 64 * 1024,
								CaMaxoperations:         1000,
								CaMaxrequests:           10000,
							},
							CsrBackChanAttrs: nfsv4_xdr.ChannelAttrs4{
								CaHeaderpadsize:         1024,
								CaMaxrequestsize:        8 * 1024 * 1024,
								CaMaxresponsesize:       12 * 1024 * 1024,
								CaMaxresponsesizeCached: 16 * 1024 * 1024,
								CaMaxoperations:         100000,
								CaMaxrequests:           1000000,
							},
						},
					},
				},
			},
			Status: nfsv4_xdr.NFS4_OK,
		}, res)

		for i := 0; i < 10; i++ {
			clock.EXPECT().Now().Return(time.Unix(1003, 0))

			res, err := program.NfsV4Nfsproc4Compound(ctx, &nfsv4_xdr.Compound4args{
				Tag:          "exchange_id",
				Minorversion: 1,
				Argarray: []nfsv4_xdr.NfsArgop4{
					&nfsv4_xdr.NfsArgop4_OP_EXCHANGE_ID{
						OpexchangeId: nfsv4_xdr.ExchangeId4args{
							EiaClientowner: nfsv4_xdr.ClientOwner4{
								CoVerifier: nfsv4_xdr.Verifier4{0x46, 0x12, 0xcd, 0xd1, 0x91, 0xda, 0x5f, 0xa4},
								CoOwnerid:  []byte{0xb5, 0x12, 0x21, 0xcd, 0x10, 0x22, 0x9b, 0x20},
							},
							EiaStateProtect: &nfsv4_xdr.StateProtect4A_SP4_NONE{},
						},
					},
				},
			})
			require.NoError(t, err)
			require.Equal(t, &nfsv4_xdr.Compound4res{
				Tag: "exchange_id",
				Resarray: []nfsv4_xdr.NfsResop4{
					&nfsv4_xdr.NfsResop4_OP_EXCHANGE_ID{
						OpexchangeId: &nfsv4_xdr.ExchangeId4res_NFS4_OK{
							EirResok4: nfsv4_xdr.ExchangeId4resok{
								EirClientid:     0x353974981fdd3329,
								EirFlags:        nfsv4_xdr.EXCHGID4_FLAG_USE_NON_PNFS | nfsv4_xdr.EXCHGID4_FLAG_CONFIRMED_R,
								EirStateProtect: &nfsv4_xdr.StateProtect4R_SP4_NONE{},
								EirServerOwner:  serverOwner,
								EirServerScope:  serverScope,
								EirServerImplId: []nfsv4_xdr.NfsImplId4{{
									NiiDomain: "buildbarn.github.io",
								}},
							},
						},
					},
				},
				Status: nfsv4_xdr.NFS4_OK,
			}, res)
		}
	})
}

func TestNFS41ProgramCompound_OP_OPEN(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	rootDirectory := mock.NewMockVirtualDirectory(ctrl)
	rootDirectory.EXPECT().VirtualGetAttributes(gomock.Any(), virtual.AttributesMaskFileHandle, gomock.Any()).
		Do(func(ctx context.Context, requested virtual.AttributesMask, attributes *virtual.Attributes) {
			attributes.SetFileHandle([]byte{0xce, 0xb3, 0xe0, 0x45, 0xd1, 0xf4, 0x9d, 0x58})
		})
	handleResolver := mock.NewMockHandleResolver(ctrl)
	serverOwner := nfsv4_xdr.ServerOwner4{
		SoMinorId: 0x7ce2ca8e8dba70ef,
		SoMajorId: []byte{0xb8, 0x6d, 0x25, 0x82, 0xb1, 0x77, 0xe1, 0xf0},
	}
	serverScope := []byte{0x4a, 0x92, 0xbc, 0x46, 0xf9, 0xde, 0x21, 0xc1}
	randomNumberGenerator := mock.NewMockSingleThreadedGenerator(ctrl)
	rebootVerifier := nfsv4_xdr.Verifier4{0xe7, 0x04, 0x52, 0x70, 0xc4, 0x8a, 0x5b, 0xc3}
	clock := mock.NewMockClock(ctrl)
	program := nfsv4.NewNFS41Program(
		rootDirectory,
		nfsv4.NewOpenedFilesPool(handleResolver.Call),
		serverOwner,
		serverScope,
		&nfsv4_xdr.ChannelAttrs4{
			CaHeaderpadsize:         0,
			CaMaxrequestsize:        1 * 1024 * 1024,
			CaMaxresponsesize:       2 * 1024 * 1024,
			CaMaxresponsesizeCached: 64 * 1024,
			CaMaxoperations:         100,
			CaMaxrequests:           100,
		},
		randomNumberGenerator,
		rebootVerifier,
		clock,
		/* enforcedLeaseTime = */ 2*time.Minute,
		/* announcedLeaseTime = */ time.Minute,
	)

	clock.EXPECT().Now().Return(time.Unix(1000, 0)).Times(2)
	exchangeIDAndCreateSessionForTesting(
		ctx,
		t,
		serverOwner,
		serverScope,
		randomNumberGenerator,
		program,
		/* sessionID = */ [...]byte{
			0x32, 0xfe, 0xf3, 0x62, 0xa8, 0x17, 0x31, 0xdd,
			0xe0, 0xd7, 0x10, 0x1e, 0xb2, 0x69, 0x1f, 0x8e,
		},
	)

	t.Run("ShareDeny", func(t *testing.T) {
		// We only care about supporting UNIX-style workloads.
		// There is no support for providing "share_deny" flags.
		clock.EXPECT().Now().Return(time.Unix(1003, 0)).Times(2)

		res, err := program.NfsV4Nfsproc4Compound(ctx, &nfsv4_xdr.Compound4args{
			Tag:          "open",
			Minorversion: 1,
			Argarray: []nfsv4_xdr.NfsArgop4{
				&nfsv4_xdr.NfsArgop4_OP_SEQUENCE{
					Opsequence: nfsv4_xdr.Sequence4args{
						SaSessionid: [...]byte{
							0x32, 0xfe, 0xf3, 0x62, 0xa8, 0x17, 0x31, 0xdd,
							0xe0, 0xd7, 0x10, 0x1e, 0xb2, 0x69, 0x1f, 0x8e,
						},
						SaSequenceid:    1,
						SaSlotid:        0,
						SaHighestSlotid: 0,
					},
				},
				&nfsv4_xdr.NfsArgop4_OP_PUTROOTFH{},
				&nfsv4_xdr.NfsArgop4_OP_OPEN{
					Opopen: nfsv4_xdr.Open4args{
						ShareAccess: nfsv4_xdr.OPEN4_SHARE_ACCESS_READ,
						ShareDeny:   nfsv4_xdr.OPEN4_SHARE_DENY_BOTH,
						Owner: nfsv4_xdr.OpenOwner4{
							Owner: []byte{0x4d, 0x6e, 0xba, 0x60, 0x71, 0xdd, 0xb5, 0x00},
						},
						Openhow: &nfsv4_xdr.Openflag4_default{
							Opentype: nfsv4_xdr.OPEN4_NOCREATE,
						},
						Claim: &nfsv4_xdr.OpenClaim4_CLAIM_NULL{
							File: "Hello",
						},
					},
				},
			},
		})
		require.NoError(t, err)
		require.Equal(t, &nfsv4_xdr.Compound4res{
			Tag: "open",
			Resarray: []nfsv4_xdr.NfsResop4{
				&nfsv4_xdr.NfsResop4_OP_SEQUENCE{
					Opsequence: &nfsv4_xdr.Sequence4res_NFS4_OK{
						SrResok4: nfsv4_xdr.Sequence4resok{
							SrSessionid: [...]byte{
								0x32, 0xfe, 0xf3, 0x62, 0xa8, 0x17, 0x31, 0xdd,
								0xe0, 0xd7, 0x10, 0x1e, 0xb2, 0x69, 0x1f, 0x8e,
							},
							SrSequenceid:          1,
							SrSlotid:              0,
							SrHighestSlotid:       99,
							SrTargetHighestSlotid: 99,
							SrStatusFlags:         0,
						},
					},
				},
				&nfsv4_xdr.NfsResop4_OP_PUTROOTFH{
					Opputrootfh: nfsv4_xdr.Putrootfh4res{
						Status: nfsv4_xdr.NFS4_OK,
					},
				},
				&nfsv4_xdr.NfsResop4_OP_OPEN{
					Opopen: &nfsv4_xdr.Open4res_default{
						Status: nfsv4_xdr.NFS4ERR_SHARE_DENIED,
					},
				},
			},
			Status: nfsv4_xdr.NFS4ERR_SHARE_DENIED,
		}, res)
	})

	t.Run("NoFileHandle", func(t *testing.T) {
		// When using CLAIM_NULL, CURRENT_FH needs to
		// refer to a valid filehandle.
		clock.EXPECT().Now().Return(time.Unix(1004, 0)).Times(2)

		res, err := program.NfsV4Nfsproc4Compound(ctx, &nfsv4_xdr.Compound4args{
			Tag:          "open",
			Minorversion: 1,
			Argarray: []nfsv4_xdr.NfsArgop4{
				&nfsv4_xdr.NfsArgop4_OP_SEQUENCE{
					Opsequence: nfsv4_xdr.Sequence4args{
						SaSessionid: [...]byte{
							0x32, 0xfe, 0xf3, 0x62, 0xa8, 0x17, 0x31, 0xdd,
							0xe0, 0xd7, 0x10, 0x1e, 0xb2, 0x69, 0x1f, 0x8e,
						},
						SaSequenceid:    2,
						SaSlotid:        0,
						SaHighestSlotid: 0,
					},
				},
				&nfsv4_xdr.NfsArgop4_OP_OPEN{
					Opopen: nfsv4_xdr.Open4args{
						ShareAccess: nfsv4_xdr.OPEN4_SHARE_ACCESS_READ,
						ShareDeny:   nfsv4_xdr.OPEN4_SHARE_DENY_NONE,
						Owner: nfsv4_xdr.OpenOwner4{
							Owner: []byte{0xe5, 0x0b, 0x1e, 0xd1, 0x6f, 0x2f, 0x05, 0x08},
						},
						Openhow: &nfsv4_xdr.Openflag4_default{
							Opentype: nfsv4_xdr.OPEN4_NOCREATE,
						},
						Claim: &nfsv4_xdr.OpenClaim4_CLAIM_NULL{
							File: "Hello",
						},
					},
				},
			},
		})
		require.NoError(t, err)
		require.Equal(t, &nfsv4_xdr.Compound4res{
			Tag: "open",
			Resarray: []nfsv4_xdr.NfsResop4{
				&nfsv4_xdr.NfsResop4_OP_SEQUENCE{
					Opsequence: &nfsv4_xdr.Sequence4res_NFS4_OK{
						SrResok4: nfsv4_xdr.Sequence4resok{
							SrSessionid: [...]byte{
								0x32, 0xfe, 0xf3, 0x62, 0xa8, 0x17, 0x31, 0xdd,
								0xe0, 0xd7, 0x10, 0x1e, 0xb2, 0x69, 0x1f, 0x8e,
							},
							SrSequenceid:          2,
							SrSlotid:              0,
							SrHighestSlotid:       99,
							SrTargetHighestSlotid: 99,
							SrStatusFlags:         0,
						},
					},
				},
				&nfsv4_xdr.NfsResop4_OP_OPEN{
					Opopen: &nfsv4_xdr.Open4res_default{
						Status: nfsv4_xdr.NFS4ERR_NOFILEHANDLE,
					},
				},
			},
			Status: nfsv4_xdr.NFS4ERR_NOFILEHANDLE,
		}, res)
	})

	t.Run("RegularFile", func(t *testing.T) {
		// When using CLAIM_NULL, CURRENT_FH cannot be a
		// regular file.
		clock.EXPECT().Now().Return(time.Unix(1005, 0)).Times(2)
		leaf := mock.NewMockVirtualLeaf(ctrl)
		handleResolverExpectCall(t, handleResolver, []byte{0xc4, 0xfc, 0xae, 0xdc, 0xc8, 0x64, 0x87, 0x78}, virtual.DirectoryChild{}.FromLeaf(leaf), virtual.StatusOK)

		res, err := program.NfsV4Nfsproc4Compound(ctx, &nfsv4_xdr.Compound4args{
			Tag:          "open",
			Minorversion: 1,
			Argarray: []nfsv4_xdr.NfsArgop4{
				&nfsv4_xdr.NfsArgop4_OP_SEQUENCE{
					Opsequence: nfsv4_xdr.Sequence4args{
						SaSessionid: [...]byte{
							0x32, 0xfe, 0xf3, 0x62, 0xa8, 0x17, 0x31, 0xdd,
							0xe0, 0xd7, 0x10, 0x1e, 0xb2, 0x69, 0x1f, 0x8e,
						},
						SaSequenceid:    3,
						SaSlotid:        0,
						SaHighestSlotid: 0,
					},
				},
				&nfsv4_xdr.NfsArgop4_OP_PUTFH{
					Opputfh: nfsv4_xdr.Putfh4args{
						Object: nfsv4_xdr.NfsFh4{0xc4, 0xfc, 0xae, 0xdc, 0xc8, 0x64, 0x87, 0x78},
					},
				},
				&nfsv4_xdr.NfsArgop4_OP_OPEN{
					Opopen: nfsv4_xdr.Open4args{
						ShareAccess: nfsv4_xdr.OPEN4_SHARE_ACCESS_READ,
						ShareDeny:   nfsv4_xdr.OPEN4_SHARE_DENY_NONE,
						Owner: nfsv4_xdr.OpenOwner4{
							Owner: []byte{0x2f, 0xff, 0x8b, 0xa5, 0x59, 0xc9, 0xda, 0xe8},
						},
						Openhow: &nfsv4_xdr.Openflag4_default{
							Opentype: nfsv4_xdr.OPEN4_NOCREATE,
						},
						Claim: &nfsv4_xdr.OpenClaim4_CLAIM_NULL{
							File: "Hello",
						},
					},
				},
			},
		})
		require.NoError(t, err)
		require.Equal(t, &nfsv4_xdr.Compound4res{
			Tag: "open",
			Resarray: []nfsv4_xdr.NfsResop4{
				&nfsv4_xdr.NfsResop4_OP_SEQUENCE{
					Opsequence: &nfsv4_xdr.Sequence4res_NFS4_OK{
						SrResok4: nfsv4_xdr.Sequence4resok{
							SrSessionid: [...]byte{
								0x32, 0xfe, 0xf3, 0x62, 0xa8, 0x17, 0x31, 0xdd,
								0xe0, 0xd7, 0x10, 0x1e, 0xb2, 0x69, 0x1f, 0x8e,
							},
							SrSequenceid:          3,
							SrSlotid:              0,
							SrHighestSlotid:       99,
							SrTargetHighestSlotid: 99,
							SrStatusFlags:         0,
						},
					},
				},
				&nfsv4_xdr.NfsResop4_OP_PUTFH{
					Opputfh: nfsv4_xdr.Putfh4res{
						Status: nfsv4_xdr.NFS4_OK,
					},
				},
				&nfsv4_xdr.NfsResop4_OP_OPEN{
					Opopen: &nfsv4_xdr.Open4res_default{
						Status: nfsv4_xdr.NFS4ERR_NOTDIR,
					},
				},
			},
			Status: nfsv4_xdr.NFS4ERR_NOTDIR,
		}, res)
	})

	t.Run("NoSuchFileOrDirectory", func(t *testing.T) {
		// Opening the file in the underlying file system fails.
		clock.EXPECT().Now().Return(time.Unix(1006, 0)).Times(2)
		rootDirectory.EXPECT().VirtualOpenChild(
			ctx,
			path.MustNewComponent("Hello"),
			virtual.ShareMaskRead,
			nil,
			&virtual.OpenExistingOptions{},
			virtual.AttributesMaskFileHandle,
			gomock.Any(),
		).Return(nil, virtual.AttributesMask(0), virtual.ChangeInfo{}, virtual.StatusErrNoEnt)

		res, err := program.NfsV4Nfsproc4Compound(ctx, &nfsv4_xdr.Compound4args{
			Tag:          "open",
			Minorversion: 1,
			Argarray: []nfsv4_xdr.NfsArgop4{
				&nfsv4_xdr.NfsArgop4_OP_SEQUENCE{
					Opsequence: nfsv4_xdr.Sequence4args{
						SaSessionid: [...]byte{
							0x32, 0xfe, 0xf3, 0x62, 0xa8, 0x17, 0x31, 0xdd,
							0xe0, 0xd7, 0x10, 0x1e, 0xb2, 0x69, 0x1f, 0x8e,
						},
						SaSequenceid:    4,
						SaSlotid:        0,
						SaHighestSlotid: 0,
					},
				},
				&nfsv4_xdr.NfsArgop4_OP_PUTROOTFH{},
				&nfsv4_xdr.NfsArgop4_OP_OPEN{
					Opopen: nfsv4_xdr.Open4args{
						ShareAccess: nfsv4_xdr.OPEN4_SHARE_ACCESS_READ,
						ShareDeny:   nfsv4_xdr.OPEN4_SHARE_DENY_NONE,
						Owner: nfsv4_xdr.OpenOwner4{
							Owner: []byte{0x83, 0xdc, 0x75, 0x35, 0x97, 0x07, 0xdd, 0x4b},
						},
						Openhow: &nfsv4_xdr.Openflag4_default{
							Opentype: nfsv4_xdr.OPEN4_NOCREATE,
						},
						Claim: &nfsv4_xdr.OpenClaim4_CLAIM_NULL{
							File: "Hello",
						},
					},
				},
			},
		})
		require.NoError(t, err)
		require.Equal(t, &nfsv4_xdr.Compound4res{
			Tag: "open",
			Resarray: []nfsv4_xdr.NfsResop4{
				&nfsv4_xdr.NfsResop4_OP_SEQUENCE{
					Opsequence: &nfsv4_xdr.Sequence4res_NFS4_OK{
						SrResok4: nfsv4_xdr.Sequence4resok{
							SrSessionid: [...]byte{
								0x32, 0xfe, 0xf3, 0x62, 0xa8, 0x17, 0x31, 0xdd,
								0xe0, 0xd7, 0x10, 0x1e, 0xb2, 0x69, 0x1f, 0x8e,
							},
							SrSequenceid:          4,
							SrSlotid:              0,
							SrHighestSlotid:       99,
							SrTargetHighestSlotid: 99,
							SrStatusFlags:         0,
						},
					},
				},
				&nfsv4_xdr.NfsResop4_OP_PUTROOTFH{
					Opputrootfh: nfsv4_xdr.Putrootfh4res{
						Status: nfsv4_xdr.NFS4_OK,
					},
				},
				&nfsv4_xdr.NfsResop4_OP_OPEN{
					Opopen: &nfsv4_xdr.Open4res_default{
						Status: nfsv4_xdr.NFS4ERR_NOENT,
					},
				},
			},
			Status: nfsv4_xdr.NFS4ERR_NOENT,
		}, res)
	})

	leaf := mock.NewMockVirtualLeaf(ctrl)

	t.Run("SuccessClaimNull", func(t *testing.T) {
		// Successfully open a file for reading.
		clock.EXPECT().Now().Return(time.Unix(1007, 0)).Times(2)
		rootDirectory.EXPECT().VirtualOpenChild(
			ctx,
			path.MustNewComponent("Hello"),
			virtual.ShareMaskRead,
			nil,
			&virtual.OpenExistingOptions{},
			virtual.AttributesMaskFileHandle,
			gomock.Any(),
		).DoAndReturn(func(ctx context.Context, name path.Component, shareAccess virtual.ShareMask, createAttributes *virtual.Attributes, existingOptions *virtual.OpenExistingOptions, requested virtual.AttributesMask, openedFileAttributes *virtual.Attributes) (virtual.Leaf, virtual.AttributesMask, virtual.ChangeInfo, virtual.Status) {
			openedFileAttributes.SetFileHandle([]byte{0x7a, 0x39, 0x6b, 0x3a, 0xaa, 0x83, 0x65, 0x7f})
			return leaf, 0, virtual.ChangeInfo{
				Before: 0x8180271b483379a7,
				After:  0x068a9f31b3a7666c,
			}, virtual.StatusOK
		})

		res, err := program.NfsV4Nfsproc4Compound(ctx, &nfsv4_xdr.Compound4args{
			Tag:          "open",
			Minorversion: 1,
			Argarray: []nfsv4_xdr.NfsArgop4{
				&nfsv4_xdr.NfsArgop4_OP_SEQUENCE{
					Opsequence: nfsv4_xdr.Sequence4args{
						SaSessionid: [...]byte{
							0x32, 0xfe, 0xf3, 0x62, 0xa8, 0x17, 0x31, 0xdd,
							0xe0, 0xd7, 0x10, 0x1e, 0xb2, 0x69, 0x1f, 0x8e,
						},
						SaSequenceid:    5,
						SaSlotid:        0,
						SaHighestSlotid: 0,
					},
				},
				&nfsv4_xdr.NfsArgop4_OP_PUTROOTFH{},
				&nfsv4_xdr.NfsArgop4_OP_OPEN{
					Opopen: nfsv4_xdr.Open4args{
						ShareAccess: nfsv4_xdr.OPEN4_SHARE_ACCESS_READ,
						ShareDeny:   nfsv4_xdr.OPEN4_SHARE_DENY_NONE,
						Owner: nfsv4_xdr.OpenOwner4{
							Owner: []byte{0xed, 0x23, 0xda, 0x88, 0x95, 0x09, 0xad, 0xbc},
						},
						Openhow: &nfsv4_xdr.Openflag4_default{
							Opentype: nfsv4_xdr.OPEN4_NOCREATE,
						},
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
				&nfsv4_xdr.NfsResop4_OP_SEQUENCE{
					Opsequence: &nfsv4_xdr.Sequence4res_NFS4_OK{
						SrResok4: nfsv4_xdr.Sequence4resok{
							SrSessionid: [...]byte{
								0x32, 0xfe, 0xf3, 0x62, 0xa8, 0x17, 0x31, 0xdd,
								0xe0, 0xd7, 0x10, 0x1e, 0xb2, 0x69, 0x1f, 0x8e,
							},
							SrSequenceid:          5,
							SrSlotid:              0,
							SrHighestSlotid:       99,
							SrTargetHighestSlotid: 99,
							SrStatusFlags:         0,
						},
					},
				},
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
								Other: [...]byte{
									0x01, 0x00, 0x00, 0x00,
									0x00, 0x00, 0x00, 0x00,
									0x00, 0x00, 0x00, 0x00,
								},
							},
							Cinfo: nfsv4_xdr.ChangeInfo4{
								Atomic: true,
								Before: 0x8180271b483379a7,
								After:  0x068a9f31b3a7666c,
							},
							Rflags:     nfsv4_xdr.OPEN4_RESULT_LOCKTYPE_POSIX | nfsv4_xdr.OPEN4_RESULT_PRESERVE_UNLINKED,
							Attrset:    nfsv4_xdr.Bitmap4{},
							Delegation: &nfsv4_xdr.OpenDelegation4_OPEN_DELEGATE_NONE{},
						},
					},
				},
				&nfsv4_xdr.NfsResop4_OP_GETFH{
					Opgetfh: &nfsv4_xdr.Getfh4res_NFS4_OK{
						Resok4: nfsv4_xdr.Getfh4resok{
							Object: nfsv4_xdr.NfsFh4{0x7a, 0x39, 0x6b, 0x3a, 0xaa, 0x83, 0x65, 0x7f},
						},
					},
				},
			},
			Status: nfsv4_xdr.NFS4_OK,
		}, res)
	})

	t.Run("SuccessClaimFHUpgrade", func(t *testing.T) {
		// Opening the same file a second. This time both for
		// reading and writing. This means that after opening,
		// we've opened the file for reading redundantly. We
		// should therefore see a call to VirtualClose().
		clock.EXPECT().Now().Return(time.Unix(1008, 0)).Times(2)
		leaf.EXPECT().VirtualOpenSelf(
			ctx,
			virtual.ShareMaskRead|virtual.ShareMaskWrite,
			&virtual.OpenExistingOptions{},
			virtual.AttributesMask(0),
			gomock.Any(),
		)
		leaf.EXPECT().VirtualClose(virtual.ShareMaskRead)

		res, err := program.NfsV4Nfsproc4Compound(ctx, &nfsv4_xdr.Compound4args{
			Tag:          "open",
			Minorversion: 1,
			Argarray: []nfsv4_xdr.NfsArgop4{
				&nfsv4_xdr.NfsArgop4_OP_SEQUENCE{
					Opsequence: nfsv4_xdr.Sequence4args{
						SaSessionid: [...]byte{
							0x32, 0xfe, 0xf3, 0x62, 0xa8, 0x17, 0x31, 0xdd,
							0xe0, 0xd7, 0x10, 0x1e, 0xb2, 0x69, 0x1f, 0x8e,
						},
						SaSequenceid:    6,
						SaSlotid:        0,
						SaHighestSlotid: 0,
					},
				},
				&nfsv4_xdr.NfsArgop4_OP_PUTFH{
					Opputfh: nfsv4_xdr.Putfh4args{
						Object: nfsv4_xdr.NfsFh4{0x7a, 0x39, 0x6b, 0x3a, 0xaa, 0x83, 0x65, 0x7f},
					},
				},
				&nfsv4_xdr.NfsArgop4_OP_OPEN{
					Opopen: nfsv4_xdr.Open4args{
						ShareAccess: nfsv4_xdr.OPEN4_SHARE_ACCESS_BOTH | nfsv4_xdr.OPEN4_SHARE_ACCESS_WANT_NO_DELEG,
						ShareDeny:   nfsv4_xdr.OPEN4_SHARE_DENY_NONE,
						Owner: nfsv4_xdr.OpenOwner4{
							Owner: []byte{0xed, 0x23, 0xda, 0x88, 0x95, 0x09, 0xad, 0xbc},
						},
						Openhow: &nfsv4_xdr.Openflag4_default{
							Opentype: nfsv4_xdr.OPEN4_NOCREATE,
						},
						Claim: &nfsv4_xdr.OpenClaim4_CLAIM_FH{},
					},
				},
			},
		})
		require.NoError(t, err)
		require.Equal(t, &nfsv4_xdr.Compound4res{
			Tag: "open",
			Resarray: []nfsv4_xdr.NfsResop4{
				&nfsv4_xdr.NfsResop4_OP_SEQUENCE{
					Opsequence: &nfsv4_xdr.Sequence4res_NFS4_OK{
						SrResok4: nfsv4_xdr.Sequence4resok{
							SrSessionid: [...]byte{
								0x32, 0xfe, 0xf3, 0x62, 0xa8, 0x17, 0x31, 0xdd,
								0xe0, 0xd7, 0x10, 0x1e, 0xb2, 0x69, 0x1f, 0x8e,
							},
							SrSequenceid:          6,
							SrSlotid:              0,
							SrHighestSlotid:       99,
							SrTargetHighestSlotid: 99,
							SrStatusFlags:         0,
						},
					},
				},
				&nfsv4_xdr.NfsResop4_OP_PUTFH{
					Opputfh: nfsv4_xdr.Putfh4res{
						Status: nfsv4_xdr.NFS4_OK,
					},
				},
				&nfsv4_xdr.NfsResop4_OP_OPEN{
					Opopen: &nfsv4_xdr.Open4res_NFS4_OK{
						Resok4: nfsv4_xdr.Open4resok{
							Stateid: nfsv4_xdr.Stateid4{
								Seqid: 2,
								Other: [...]byte{
									0x01, 0x00, 0x00, 0x00,
									0x00, 0x00, 0x00, 0x00,
									0x00, 0x00, 0x00, 0x00,
								},
							},
							Cinfo:      nfsv4_xdr.ChangeInfo4{Atomic: true},
							Rflags:     nfsv4_xdr.OPEN4_RESULT_LOCKTYPE_POSIX | nfsv4_xdr.OPEN4_RESULT_PRESERVE_UNLINKED,
							Attrset:    nfsv4_xdr.Bitmap4{},
							Delegation: &nfsv4_xdr.OpenDelegation4_OPEN_DELEGATE_NONE{},
						},
					},
				},
			},
			Status: nfsv4_xdr.NFS4_OK,
		}, res)
	})

	// Invoke SEQUENCE at a time far in the future. This should
	// cause the client state to be cleaned up. The opened file
	// should be closed.
	clock.EXPECT().Now().Return(time.Unix(10000, 0)).Times(2)
	leaf.EXPECT().VirtualClose(virtual.ShareMaskRead | virtual.ShareMaskWrite)

	res, err := program.NfsV4Nfsproc4Compound(ctx, &nfsv4_xdr.Compound4args{
		Tag:          "sequence",
		Minorversion: 1,
		Argarray: []nfsv4_xdr.NfsArgop4{
			&nfsv4_xdr.NfsArgop4_OP_SEQUENCE{
				Opsequence: nfsv4_xdr.Sequence4args{
					SaSessionid: [...]byte{
						0x32, 0xfe, 0xf3, 0x62, 0xa8, 0x17, 0x31, 0xdd,
						0xe0, 0xd7, 0x10, 0x1e, 0xb2, 0x69, 0x1f, 0x8e,
					},
					SaSequenceid:    1000,
					SaSlotid:        0,
					SaHighestSlotid: 0,
				},
			},
		},
	})
	require.NoError(t, err)
	require.Equal(t, &nfsv4_xdr.Compound4res{
		Tag: "sequence",
		Resarray: []nfsv4_xdr.NfsResop4{
			&nfsv4_xdr.NfsResop4_OP_SEQUENCE{
				Opsequence: &nfsv4_xdr.Sequence4res_default{
					SrStatus: nfsv4_xdr.NFS4ERR_BADSESSION,
				},
			},
		},
		Status: nfsv4_xdr.NFS4ERR_BADSESSION,
	}, res)
}

func TestNFS41ProgramCompound_OP_OPEN_DOWNGRADE(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	rootDirectory := mock.NewMockVirtualDirectory(ctrl)
	rootDirectory.EXPECT().VirtualGetAttributes(gomock.Any(), virtual.AttributesMaskFileHandle, gomock.Any()).
		Do(func(ctx context.Context, requested virtual.AttributesMask, attributes *virtual.Attributes) {
			attributes.SetFileHandle([]byte{0xc9, 0x80, 0xbb, 0xc0, 0x1a, 0xbe, 0x81, 0x85})
		})
	handleResolver := mock.NewMockHandleResolver(ctrl)
	serverOwner := nfsv4_xdr.ServerOwner4{
		SoMinorId: 0x294182316e42d8e9,
		SoMajorId: []byte{0xb8, 0x6d, 0x25, 0x82, 0xb1, 0x77, 0xe1, 0xf0},
	}
	serverScope := []byte{0x51, 0x0a, 0x4a, 0xd7, 0x79, 0xdc, 0xf5, 0x13}
	randomNumberGenerator := mock.NewMockSingleThreadedGenerator(ctrl)
	rebootVerifier := nfsv4_xdr.Verifier4{0xa6, 0x7a, 0x9d, 0x94, 0x25, 0x6b, 0x48, 0xd8}
	clock := mock.NewMockClock(ctrl)
	program := nfsv4.NewNFS41Program(
		rootDirectory,
		nfsv4.NewOpenedFilesPool(handleResolver.Call),
		serverOwner,
		serverScope,
		&nfsv4_xdr.ChannelAttrs4{
			CaHeaderpadsize:         0,
			CaMaxrequestsize:        1 * 1024 * 1024,
			CaMaxresponsesize:       2 * 1024 * 1024,
			CaMaxresponsesizeCached: 64 * 1024,
			CaMaxoperations:         100,
			CaMaxrequests:           100,
		},
		randomNumberGenerator,
		rebootVerifier,
		clock,
		/* enforcedLeaseTime = */ 2*time.Minute,
		/* announcedLeaseTime = */ time.Minute,
	)

	clock.EXPECT().Now().Return(time.Unix(1000, 0)).Times(2)
	exchangeIDAndCreateSessionForTesting(
		ctx,
		t,
		serverOwner,
		serverScope,
		randomNumberGenerator,
		program,
		/* sessionID = */ [...]byte{
			0xdb, 0xa7, 0xe2, 0xb6, 0x96, 0x5d, 0x27, 0xcf,
			0x69, 0xf4, 0x25, 0x1c, 0xb1, 0xd0, 0xa8, 0x9b,
		},
	)

	t.Run("AnonymousStateID", func(t *testing.T) {
		// Calling OPEN_DOWNGRADE against the anonymous state ID
		// is of course not permitted. This operation only works
		// when called against regular state IDs.
		clock.EXPECT().Now().Return(time.Unix(1001, 0)).Times(2)
		leaf := mock.NewMockVirtualLeaf(ctrl)
		handleResolverExpectCall(t, handleResolver, []byte{0x2c, 0x64, 0xd4, 0x7b, 0x00, 0xe2, 0xf0, 0x98}, virtual.DirectoryChild{}.FromLeaf(leaf), virtual.StatusOK)

		res, err := program.NfsV4Nfsproc4Compound(ctx, &nfsv4_xdr.Compound4args{
			Tag:          "open_downgrade",
			Minorversion: 1,
			Argarray: []nfsv4_xdr.NfsArgop4{
				&nfsv4_xdr.NfsArgop4_OP_SEQUENCE{
					Opsequence: nfsv4_xdr.Sequence4args{
						SaSessionid: [...]byte{
							0xdb, 0xa7, 0xe2, 0xb6, 0x96, 0x5d, 0x27, 0xcf,
							0x69, 0xf4, 0x25, 0x1c, 0xb1, 0xd0, 0xa8, 0x9b,
						},
						SaSequenceid:    1,
						SaSlotid:        0,
						SaHighestSlotid: 24,
						SaCachethis:     true,
					},
				},
				&nfsv4_xdr.NfsArgop4_OP_PUTFH{
					Opputfh: nfsv4_xdr.Putfh4args{
						Object: nfsv4_xdr.NfsFh4{0x2c, 0x64, 0xd4, 0x7b, 0x00, 0xe2, 0xf0, 0x98},
					},
				},
				&nfsv4_xdr.NfsArgop4_OP_OPEN_DOWNGRADE{
					OpopenDowngrade: nfsv4_xdr.OpenDowngrade4args{
						ShareAccess: nfsv4_xdr.OPEN4_SHARE_ACCESS_READ,
						ShareDeny:   nfsv4_xdr.OPEN4_SHARE_DENY_NONE,
					},
				},
			},
		})
		require.NoError(t, err)
		require.Equal(t, &nfsv4_xdr.Compound4res{
			Tag: "open_downgrade",
			Resarray: []nfsv4_xdr.NfsResop4{
				&nfsv4_xdr.NfsResop4_OP_SEQUENCE{
					Opsequence: &nfsv4_xdr.Sequence4res_NFS4_OK{
						SrResok4: nfsv4_xdr.Sequence4resok{
							SrSessionid: [...]byte{
								0xdb, 0xa7, 0xe2, 0xb6, 0x96, 0x5d, 0x27, 0xcf,
								0x69, 0xf4, 0x25, 0x1c, 0xb1, 0xd0, 0xa8, 0x9b,
							},
							SrSequenceid:          1,
							SrSlotid:              0,
							SrHighestSlotid:       99,
							SrTargetHighestSlotid: 99,
							SrStatusFlags:         0,
						},
					},
				},
				&nfsv4_xdr.NfsResop4_OP_PUTFH{
					Opputfh: nfsv4_xdr.Putfh4res{
						Status: nfsv4_xdr.NFS4_OK,
					},
				},
				&nfsv4_xdr.NfsResop4_OP_OPEN_DOWNGRADE{
					OpopenDowngrade: &nfsv4_xdr.OpenDowngrade4res_default{
						Status: nfsv4_xdr.NFS4ERR_BAD_STATEID,
					},
				},
			},
			Status: nfsv4_xdr.NFS4ERR_BAD_STATEID,
		}, res)
	})

	// The remainder of this test assumes that a file has been opened.
	leaf := mock.NewMockVirtualLeaf(ctrl)
	handleResolverExpectCall(t, handleResolver, []byte{0x1e, 0xf9, 0x11, 0xc9, 0xb7, 0xb2, 0x6b, 0xeb}, virtual.DirectoryChild{}.FromLeaf(leaf), virtual.StatusOK)
	leaf.EXPECT().VirtualOpenSelf(
		ctx,
		virtual.ShareMaskRead,
		&virtual.OpenExistingOptions{},
		virtual.AttributesMask(0),
		gomock.Any(),
	)

	clock.EXPECT().Now().Return(time.Unix(1002, 0)).Times(2)
	res, err := program.NfsV4Nfsproc4Compound(ctx, &nfsv4_xdr.Compound4args{
		Tag:          "open",
		Minorversion: 1,
		Argarray: []nfsv4_xdr.NfsArgop4{
			&nfsv4_xdr.NfsArgop4_OP_SEQUENCE{
				Opsequence: nfsv4_xdr.Sequence4args{
					SaSessionid: [...]byte{
						0xdb, 0xa7, 0xe2, 0xb6, 0x96, 0x5d, 0x27, 0xcf,
						0x69, 0xf4, 0x25, 0x1c, 0xb1, 0xd0, 0xa8, 0x9b,
					},
					SaSequenceid:    2,
					SaSlotid:        0,
					SaHighestSlotid: 0,
				},
			},
			&nfsv4_xdr.NfsArgop4_OP_PUTFH{
				Opputfh: nfsv4_xdr.Putfh4args{
					Object: nfsv4_xdr.NfsFh4{0x1e, 0xf9, 0x11, 0xc9, 0xb7, 0xb2, 0x6b, 0xeb},
				},
			},
			&nfsv4_xdr.NfsArgop4_OP_OPEN{
				Opopen: nfsv4_xdr.Open4args{
					ShareAccess: nfsv4_xdr.OPEN4_SHARE_ACCESS_READ,
					ShareDeny:   nfsv4_xdr.OPEN4_SHARE_DENY_NONE,
					Owner: nfsv4_xdr.OpenOwner4{
						Owner: []byte{0x78, 0xb3, 0xd6, 0x8b, 0x85, 0x72, 0xa6, 0xae},
					},
					Openhow: &nfsv4_xdr.Openflag4_default{
						Opentype: nfsv4_xdr.OPEN4_NOCREATE,
					},
					Claim: &nfsv4_xdr.OpenClaim4_CLAIM_FH{},
				},
			},
		},
	})
	require.NoError(t, err)
	require.Equal(t, &nfsv4_xdr.Compound4res{
		Tag: "open",
		Resarray: []nfsv4_xdr.NfsResop4{
			&nfsv4_xdr.NfsResop4_OP_SEQUENCE{
				Opsequence: &nfsv4_xdr.Sequence4res_NFS4_OK{
					SrResok4: nfsv4_xdr.Sequence4resok{
						SrSessionid: [...]byte{
							0xdb, 0xa7, 0xe2, 0xb6, 0x96, 0x5d, 0x27, 0xcf,
							0x69, 0xf4, 0x25, 0x1c, 0xb1, 0xd0, 0xa8, 0x9b,
						},
						SrSequenceid:          2,
						SrSlotid:              0,
						SrHighestSlotid:       99,
						SrTargetHighestSlotid: 99,
						SrStatusFlags:         0,
					},
				},
			},
			&nfsv4_xdr.NfsResop4_OP_PUTFH{
				Opputfh: nfsv4_xdr.Putfh4res{
					Status: nfsv4_xdr.NFS4_OK,
				},
			},
			&nfsv4_xdr.NfsResop4_OP_OPEN{
				Opopen: &nfsv4_xdr.Open4res_NFS4_OK{
					Resok4: nfsv4_xdr.Open4resok{
						Stateid: nfsv4_xdr.Stateid4{
							Seqid: 1,
							Other: [...]byte{
								0x01, 0x00, 0x00, 0x00,
								0x00, 0x00, 0x00, 0x00,
								0x00, 0x00, 0x00, 0x00,
							},
						},
						Cinfo:      nfsv4_xdr.ChangeInfo4{Atomic: true},
						Rflags:     nfsv4_xdr.OPEN4_RESULT_LOCKTYPE_POSIX | nfsv4_xdr.OPEN4_RESULT_PRESERVE_UNLINKED,
						Attrset:    nfsv4_xdr.Bitmap4{},
						Delegation: &nfsv4_xdr.OpenDelegation4_OPEN_DELEGATE_NONE{},
					},
				},
			},
		},
		Status: nfsv4_xdr.NFS4_OK,
	}, res)

	t.Run("Upgrade", func(t *testing.T) {
		// It's not permitted to use OPEN_DOWNGRADE to upgrade
		// the share reservations of a file. A file that's been
		// opened for reading can't be upgraded to reading and
		// writing.
		clock.EXPECT().Now().Return(time.Unix(1003, 0)).Times(2)

		res, err := program.NfsV4Nfsproc4Compound(ctx, &nfsv4_xdr.Compound4args{
			Tag:          "open_downgrade",
			Minorversion: 1,
			Argarray: []nfsv4_xdr.NfsArgop4{
				&nfsv4_xdr.NfsArgop4_OP_SEQUENCE{
					Opsequence: nfsv4_xdr.Sequence4args{
						SaSessionid: [...]byte{
							0xdb, 0xa7, 0xe2, 0xb6, 0x96, 0x5d, 0x27, 0xcf,
							0x69, 0xf4, 0x25, 0x1c, 0xb1, 0xd0, 0xa8, 0x9b,
						},
						SaSequenceid:    3,
						SaSlotid:        0,
						SaHighestSlotid: 0,
					},
				},
				&nfsv4_xdr.NfsArgop4_OP_PUTFH{
					Opputfh: nfsv4_xdr.Putfh4args{
						Object: nfsv4_xdr.NfsFh4{0x1e, 0xf9, 0x11, 0xc9, 0xb7, 0xb2, 0x6b, 0xeb},
					},
				},
				&nfsv4_xdr.NfsArgop4_OP_OPEN_DOWNGRADE{
					OpopenDowngrade: nfsv4_xdr.OpenDowngrade4args{
						OpenStateid: nfsv4_xdr.Stateid4{
							Seqid: 1,
							Other: [...]byte{
								0x01, 0x00, 0x00, 0x00,
								0x00, 0x00, 0x00, 0x00,
								0x00, 0x00, 0x00, 0x00,
							},
						},
						ShareAccess: nfsv4_xdr.OPEN4_SHARE_ACCESS_BOTH,
						ShareDeny:   nfsv4_xdr.OPEN4_SHARE_DENY_NONE,
					},
				},
			},
		})
		require.NoError(t, err)
		require.Equal(t, &nfsv4_xdr.Compound4res{
			Tag: "open_downgrade",
			Resarray: []nfsv4_xdr.NfsResop4{
				&nfsv4_xdr.NfsResop4_OP_SEQUENCE{
					Opsequence: &nfsv4_xdr.Sequence4res_NFS4_OK{
						SrResok4: nfsv4_xdr.Sequence4resok{
							SrSessionid: [...]byte{
								0xdb, 0xa7, 0xe2, 0xb6, 0x96, 0x5d, 0x27, 0xcf,
								0x69, 0xf4, 0x25, 0x1c, 0xb1, 0xd0, 0xa8, 0x9b,
							},
							SrSequenceid:          3,
							SrSlotid:              0,
							SrHighestSlotid:       99,
							SrTargetHighestSlotid: 99,
							SrStatusFlags:         0,
						},
					},
				},
				&nfsv4_xdr.NfsResop4_OP_PUTFH{
					Opputfh: nfsv4_xdr.Putfh4res{
						Status: nfsv4_xdr.NFS4_OK,
					},
				},
				&nfsv4_xdr.NfsResop4_OP_OPEN_DOWNGRADE{
					OpopenDowngrade: &nfsv4_xdr.OpenDowngrade4res_default{
						Status: nfsv4_xdr.NFS4ERR_INVAL,
					},
				},
			},
			Status: nfsv4_xdr.NFS4ERR_INVAL,
		}, res)
	})

	t.Run("Noop", func(t *testing.T) {
		// Though pointless, it is permitted to downgrade a file
		// to exactly the same set of share reservations.
		clock.EXPECT().Now().Return(time.Unix(1004, 0)).Times(2)

		res, err := program.NfsV4Nfsproc4Compound(ctx, &nfsv4_xdr.Compound4args{
			Tag:          "open_downgrade",
			Minorversion: 1,
			Argarray: []nfsv4_xdr.NfsArgop4{
				&nfsv4_xdr.NfsArgop4_OP_SEQUENCE{
					Opsequence: nfsv4_xdr.Sequence4args{
						SaSessionid: [...]byte{
							0xdb, 0xa7, 0xe2, 0xb6, 0x96, 0x5d, 0x27, 0xcf,
							0x69, 0xf4, 0x25, 0x1c, 0xb1, 0xd0, 0xa8, 0x9b,
						},
						SaSequenceid:    4,
						SaSlotid:        0,
						SaHighestSlotid: 0,
					},
				},
				&nfsv4_xdr.NfsArgop4_OP_PUTFH{
					Opputfh: nfsv4_xdr.Putfh4args{
						Object: nfsv4_xdr.NfsFh4{0x1e, 0xf9, 0x11, 0xc9, 0xb7, 0xb2, 0x6b, 0xeb},
					},
				},
				&nfsv4_xdr.NfsArgop4_OP_OPEN_DOWNGRADE{
					OpopenDowngrade: nfsv4_xdr.OpenDowngrade4args{
						OpenStateid: nfsv4_xdr.Stateid4{
							Seqid: 1,
							Other: [...]byte{
								0x01, 0x00, 0x00, 0x00,
								0x00, 0x00, 0x00, 0x00,
								0x00, 0x00, 0x00, 0x00,
							},
						},
						ShareAccess: nfsv4_xdr.OPEN4_SHARE_ACCESS_READ,
						ShareDeny:   nfsv4_xdr.OPEN4_SHARE_DENY_NONE,
					},
				},
			},
		})
		require.NoError(t, err)
		require.Equal(t, &nfsv4_xdr.Compound4res{
			Tag: "open_downgrade",
			Resarray: []nfsv4_xdr.NfsResop4{
				&nfsv4_xdr.NfsResop4_OP_SEQUENCE{
					Opsequence: &nfsv4_xdr.Sequence4res_NFS4_OK{
						SrResok4: nfsv4_xdr.Sequence4resok{
							SrSessionid: [...]byte{
								0xdb, 0xa7, 0xe2, 0xb6, 0x96, 0x5d, 0x27, 0xcf,
								0x69, 0xf4, 0x25, 0x1c, 0xb1, 0xd0, 0xa8, 0x9b,
							},
							SrSequenceid:          4,
							SrSlotid:              0,
							SrHighestSlotid:       99,
							SrTargetHighestSlotid: 99,
							SrStatusFlags:         0,
						},
					},
				},
				&nfsv4_xdr.NfsResop4_OP_PUTFH{
					Opputfh: nfsv4_xdr.Putfh4res{
						Status: nfsv4_xdr.NFS4_OK,
					},
				},
				&nfsv4_xdr.NfsResop4_OP_OPEN_DOWNGRADE{
					OpopenDowngrade: &nfsv4_xdr.OpenDowngrade4res_NFS4_OK{
						Resok4: nfsv4_xdr.OpenDowngrade4resok{
							OpenStateid: nfsv4_xdr.Stateid4{
								Seqid: 2,
								Other: [...]byte{
									0x01, 0x00, 0x00, 0x00,
									0x00, 0x00, 0x00, 0x00,
									0x00, 0x00, 0x00, 0x00,
								},
							},
						},
					},
				},
			},
			Status: nfsv4_xdr.NFS4_OK,
		}, res)
	})

	t.Run("OldStateID", func(t *testing.T) {
		// Can't call OPEN_DOWNGRADE on a state ID from the past.
		clock.EXPECT().Now().Return(time.Unix(1005, 0)).Times(2)

		res, err := program.NfsV4Nfsproc4Compound(ctx, &nfsv4_xdr.Compound4args{
			Tag:          "open_downgrade",
			Minorversion: 1,
			Argarray: []nfsv4_xdr.NfsArgop4{
				&nfsv4_xdr.NfsArgop4_OP_SEQUENCE{
					Opsequence: nfsv4_xdr.Sequence4args{
						SaSessionid: [...]byte{
							0xdb, 0xa7, 0xe2, 0xb6, 0x96, 0x5d, 0x27, 0xcf,
							0x69, 0xf4, 0x25, 0x1c, 0xb1, 0xd0, 0xa8, 0x9b,
						},
						SaSequenceid:    5,
						SaSlotid:        0,
						SaHighestSlotid: 0,
					},
				},
				&nfsv4_xdr.NfsArgop4_OP_PUTFH{
					Opputfh: nfsv4_xdr.Putfh4args{
						Object: nfsv4_xdr.NfsFh4{0x1e, 0xf9, 0x11, 0xc9, 0xb7, 0xb2, 0x6b, 0xeb},
					},
				},
				&nfsv4_xdr.NfsArgop4_OP_OPEN_DOWNGRADE{
					OpopenDowngrade: nfsv4_xdr.OpenDowngrade4args{
						OpenStateid: nfsv4_xdr.Stateid4{
							Seqid: 1,
							Other: [...]byte{
								0x01, 0x00, 0x00, 0x00,
								0x00, 0x00, 0x00, 0x00,
								0x00, 0x00, 0x00, 0x00,
							},
						},
						ShareAccess: nfsv4_xdr.OPEN4_SHARE_ACCESS_READ,
						ShareDeny:   nfsv4_xdr.OPEN4_SHARE_DENY_NONE,
					},
				},
			},
		})
		require.NoError(t, err)
		require.Equal(t, &nfsv4_xdr.Compound4res{
			Tag: "open_downgrade",
			Resarray: []nfsv4_xdr.NfsResop4{
				&nfsv4_xdr.NfsResop4_OP_SEQUENCE{
					Opsequence: &nfsv4_xdr.Sequence4res_NFS4_OK{
						SrResok4: nfsv4_xdr.Sequence4resok{
							SrSessionid: [...]byte{
								0xdb, 0xa7, 0xe2, 0xb6, 0x96, 0x5d, 0x27, 0xcf,
								0x69, 0xf4, 0x25, 0x1c, 0xb1, 0xd0, 0xa8, 0x9b,
							},
							SrSequenceid:          5,
							SrSlotid:              0,
							SrHighestSlotid:       99,
							SrTargetHighestSlotid: 99,
							SrStatusFlags:         0,
						},
					},
				},
				&nfsv4_xdr.NfsResop4_OP_PUTFH{
					Opputfh: nfsv4_xdr.Putfh4res{
						Status: nfsv4_xdr.NFS4_OK,
					},
				},
				&nfsv4_xdr.NfsResop4_OP_OPEN_DOWNGRADE{
					OpopenDowngrade: &nfsv4_xdr.OpenDowngrade4res_default{
						Status: nfsv4_xdr.NFS4ERR_OLD_STATEID,
					},
				},
			},
			Status: nfsv4_xdr.NFS4ERR_OLD_STATEID,
		}, res)
	})

	t.Run("SuccessWithImmediateClose", func(t *testing.T) {
		// When upgrading the file to being both readable and
		// writable, a subsequent downgrade to read-only should
		// close the file for writing.
		clock.EXPECT().Now().Return(time.Unix(1006, 0)).Times(2)
		leaf.EXPECT().VirtualOpenSelf(ctx, virtual.ShareMaskRead|virtual.ShareMaskWrite, &virtual.OpenExistingOptions{}, virtual.AttributesMask(0), gomock.Any())
		leaf.EXPECT().VirtualClose(virtual.ShareMaskRead)

		res, err := program.NfsV4Nfsproc4Compound(ctx, &nfsv4_xdr.Compound4args{
			Tag:          "open",
			Minorversion: 1,
			Argarray: []nfsv4_xdr.NfsArgop4{
				&nfsv4_xdr.NfsArgop4_OP_SEQUENCE{
					Opsequence: nfsv4_xdr.Sequence4args{
						SaSessionid: [...]byte{
							0xdb, 0xa7, 0xe2, 0xb6, 0x96, 0x5d, 0x27, 0xcf,
							0x69, 0xf4, 0x25, 0x1c, 0xb1, 0xd0, 0xa8, 0x9b,
						},
						SaSequenceid:    6,
						SaSlotid:        0,
						SaHighestSlotid: 0,
					},
				},
				&nfsv4_xdr.NfsArgop4_OP_PUTFH{
					Opputfh: nfsv4_xdr.Putfh4args{
						Object: nfsv4_xdr.NfsFh4{0x1e, 0xf9, 0x11, 0xc9, 0xb7, 0xb2, 0x6b, 0xeb},
					},
				},
				&nfsv4_xdr.NfsArgop4_OP_OPEN{
					Opopen: nfsv4_xdr.Open4args{
						ShareAccess: nfsv4_xdr.OPEN4_SHARE_ACCESS_BOTH,
						ShareDeny:   nfsv4_xdr.OPEN4_SHARE_DENY_NONE,
						Owner: nfsv4_xdr.OpenOwner4{
							Owner: []byte{0x78, 0xb3, 0xd6, 0x8b, 0x85, 0x72, 0xa6, 0xae},
						},
						Openhow: &nfsv4_xdr.Openflag4_default{},
						Claim:   &nfsv4_xdr.OpenClaim4_CLAIM_FH{},
					},
				},
			},
		})
		require.NoError(t, err)
		require.Equal(t, &nfsv4_xdr.Compound4res{
			Tag: "open",
			Resarray: []nfsv4_xdr.NfsResop4{
				&nfsv4_xdr.NfsResop4_OP_SEQUENCE{
					Opsequence: &nfsv4_xdr.Sequence4res_NFS4_OK{
						SrResok4: nfsv4_xdr.Sequence4resok{
							SrSessionid: [...]byte{
								0xdb, 0xa7, 0xe2, 0xb6, 0x96, 0x5d, 0x27, 0xcf,
								0x69, 0xf4, 0x25, 0x1c, 0xb1, 0xd0, 0xa8, 0x9b,
							},
							SrSequenceid:          6,
							SrSlotid:              0,
							SrHighestSlotid:       99,
							SrTargetHighestSlotid: 99,
							SrStatusFlags:         0,
						},
					},
				},
				&nfsv4_xdr.NfsResop4_OP_PUTFH{
					Opputfh: nfsv4_xdr.Putfh4res{
						Status: nfsv4_xdr.NFS4_OK,
					},
				},
				&nfsv4_xdr.NfsResop4_OP_OPEN{
					Opopen: &nfsv4_xdr.Open4res_NFS4_OK{
						Resok4: nfsv4_xdr.Open4resok{
							Stateid: nfsv4_xdr.Stateid4{
								Seqid: 3,
								Other: [...]byte{
									0x01, 0x00, 0x00, 0x00,
									0x00, 0x00, 0x00, 0x00,
									0x00, 0x00, 0x00, 0x00,
								},
							},
							Cinfo:      nfsv4_xdr.ChangeInfo4{Atomic: true},
							Rflags:     nfsv4_xdr.OPEN4_RESULT_LOCKTYPE_POSIX | nfsv4_xdr.OPEN4_RESULT_PRESERVE_UNLINKED,
							Attrset:    nfsv4_xdr.Bitmap4{},
							Delegation: &nfsv4_xdr.OpenDelegation4_OPEN_DELEGATE_NONE{},
						},
					},
				},
			},
			Status: nfsv4_xdr.NFS4_OK,
		}, res)

		clock.EXPECT().Now().Return(time.Unix(1007, 0)).Times(2)
		leaf.EXPECT().VirtualClose(virtual.ShareMaskWrite)

		res, err = program.NfsV4Nfsproc4Compound(ctx, &nfsv4_xdr.Compound4args{
			Tag:          "open_downgrade",
			Minorversion: 1,
			Argarray: []nfsv4_xdr.NfsArgop4{
				&nfsv4_xdr.NfsArgop4_OP_SEQUENCE{
					Opsequence: nfsv4_xdr.Sequence4args{
						SaSessionid: [...]byte{
							0xdb, 0xa7, 0xe2, 0xb6, 0x96, 0x5d, 0x27, 0xcf,
							0x69, 0xf4, 0x25, 0x1c, 0xb1, 0xd0, 0xa8, 0x9b,
						},
						SaSequenceid:    7,
						SaSlotid:        0,
						SaHighestSlotid: 0,
					},
				},
				&nfsv4_xdr.NfsArgop4_OP_PUTFH{
					Opputfh: nfsv4_xdr.Putfh4args{
						Object: nfsv4_xdr.NfsFh4{0x1e, 0xf9, 0x11, 0xc9, 0xb7, 0xb2, 0x6b, 0xeb},
					},
				},
				&nfsv4_xdr.NfsArgop4_OP_OPEN_DOWNGRADE{
					OpopenDowngrade: nfsv4_xdr.OpenDowngrade4args{
						OpenStateid: nfsv4_xdr.Stateid4{
							Seqid: 3,
							Other: [...]byte{
								0x01, 0x00, 0x00, 0x00,
								0x00, 0x00, 0x00, 0x00,
								0x00, 0x00, 0x00, 0x00,
							},
						},
						ShareAccess: nfsv4_xdr.OPEN4_SHARE_ACCESS_READ,
						ShareDeny:   nfsv4_xdr.OPEN4_SHARE_DENY_NONE,
					},
				},
			},
		})
		require.NoError(t, err)
		require.Equal(t, &nfsv4_xdr.Compound4res{
			Tag: "open_downgrade",
			Resarray: []nfsv4_xdr.NfsResop4{
				&nfsv4_xdr.NfsResop4_OP_SEQUENCE{
					Opsequence: &nfsv4_xdr.Sequence4res_NFS4_OK{
						SrResok4: nfsv4_xdr.Sequence4resok{
							SrSessionid: [...]byte{
								0xdb, 0xa7, 0xe2, 0xb6, 0x96, 0x5d, 0x27, 0xcf,
								0x69, 0xf4, 0x25, 0x1c, 0xb1, 0xd0, 0xa8, 0x9b,
							},
							SrSequenceid:          7,
							SrSlotid:              0,
							SrHighestSlotid:       99,
							SrTargetHighestSlotid: 99,
							SrStatusFlags:         0,
						},
					},
				},
				&nfsv4_xdr.NfsResop4_OP_PUTFH{
					Opputfh: nfsv4_xdr.Putfh4res{
						Status: nfsv4_xdr.NFS4_OK,
					},
				},
				&nfsv4_xdr.NfsResop4_OP_OPEN_DOWNGRADE{
					OpopenDowngrade: &nfsv4_xdr.OpenDowngrade4res_NFS4_OK{
						Resok4: nfsv4_xdr.OpenDowngrade4resok{
							OpenStateid: nfsv4_xdr.Stateid4{
								Seqid: 4,
								Other: [...]byte{
									0x01, 0x00, 0x00, 0x00,
									0x00, 0x00, 0x00, 0x00,
									0x00, 0x00, 0x00, 0x00,
								},
							},
						},
					},
				},
			},
			Status: nfsv4_xdr.NFS4_OK,
		}, res)
	})

	t.Run("SuccessWithDelayedClose", func(t *testing.T) {
		// If a lock has been acquired against a file, a call to
		// OPEN_DOWNGRADE won't have an immediate effect. This
		// is because the lock-owner state ID can still be used
		// to access the file using its original share
		// reservations. Calling FREE_STATEID should cause
		// the file to be closed partially.
		clock.EXPECT().Now().Return(time.Unix(1008, 0)).Times(2)
		leaf.EXPECT().VirtualOpenSelf(ctx, virtual.ShareMaskWrite, &virtual.OpenExistingOptions{}, virtual.AttributesMask(0), gomock.Any())

		res, err := program.NfsV4Nfsproc4Compound(ctx, &nfsv4_xdr.Compound4args{
			Tag:          "open",
			Minorversion: 1,
			Argarray: []nfsv4_xdr.NfsArgop4{
				&nfsv4_xdr.NfsArgop4_OP_SEQUENCE{
					Opsequence: nfsv4_xdr.Sequence4args{
						SaSessionid: [...]byte{
							0xdb, 0xa7, 0xe2, 0xb6, 0x96, 0x5d, 0x27, 0xcf,
							0x69, 0xf4, 0x25, 0x1c, 0xb1, 0xd0, 0xa8, 0x9b,
						},
						SaSequenceid:    8,
						SaSlotid:        0,
						SaHighestSlotid: 0,
					},
				},
				&nfsv4_xdr.NfsArgop4_OP_PUTFH{
					Opputfh: nfsv4_xdr.Putfh4args{
						Object: nfsv4_xdr.NfsFh4{0x1e, 0xf9, 0x11, 0xc9, 0xb7, 0xb2, 0x6b, 0xeb},
					},
				},
				&nfsv4_xdr.NfsArgop4_OP_OPEN{
					Opopen: nfsv4_xdr.Open4args{
						ShareAccess: nfsv4_xdr.OPEN4_SHARE_ACCESS_WRITE,
						ShareDeny:   nfsv4_xdr.OPEN4_SHARE_DENY_NONE,
						Owner: nfsv4_xdr.OpenOwner4{
							Owner: []byte{0x78, 0xb3, 0xd6, 0x8b, 0x85, 0x72, 0xa6, 0xae},
						},
						Openhow: &nfsv4_xdr.Openflag4_default{},
						Claim: &nfsv4_xdr.OpenClaim4_CLAIM_PREVIOUS{
							DelegateType: nfsv4_xdr.OPEN_DELEGATE_NONE,
						},
					},
				},
			},
		})
		require.NoError(t, err)
		require.Equal(t, &nfsv4_xdr.Compound4res{
			Tag: "open",
			Resarray: []nfsv4_xdr.NfsResop4{
				&nfsv4_xdr.NfsResop4_OP_SEQUENCE{
					Opsequence: &nfsv4_xdr.Sequence4res_NFS4_OK{
						SrResok4: nfsv4_xdr.Sequence4resok{
							SrSessionid: [...]byte{
								0xdb, 0xa7, 0xe2, 0xb6, 0x96, 0x5d, 0x27, 0xcf,
								0x69, 0xf4, 0x25, 0x1c, 0xb1, 0xd0, 0xa8, 0x9b,
							},
							SrSequenceid:          8,
							SrSlotid:              0,
							SrHighestSlotid:       99,
							SrTargetHighestSlotid: 99,
							SrStatusFlags:         0,
						},
					},
				},
				&nfsv4_xdr.NfsResop4_OP_PUTFH{
					Opputfh: nfsv4_xdr.Putfh4res{
						Status: nfsv4_xdr.NFS4_OK,
					},
				},
				&nfsv4_xdr.NfsResop4_OP_OPEN{
					Opopen: &nfsv4_xdr.Open4res_NFS4_OK{
						Resok4: nfsv4_xdr.Open4resok{
							Stateid: nfsv4_xdr.Stateid4{
								Seqid: 5,
								Other: [...]byte{
									0x01, 0x00, 0x00, 0x00,
									0x00, 0x00, 0x00, 0x00,
									0x00, 0x00, 0x00, 0x00,
								},
							},
							Cinfo:      nfsv4_xdr.ChangeInfo4{Atomic: true},
							Rflags:     nfsv4_xdr.OPEN4_RESULT_LOCKTYPE_POSIX | nfsv4_xdr.OPEN4_RESULT_PRESERVE_UNLINKED,
							Attrset:    nfsv4_xdr.Bitmap4{},
							Delegation: &nfsv4_xdr.OpenDelegation4_OPEN_DELEGATE_NONE{},
						},
					},
				},
			},
			Status: nfsv4_xdr.NFS4_OK,
		}, res)

		clock.EXPECT().Now().Return(time.Unix(1009, 0)).Times(2)

		resLock, err := program.NfsV4Nfsproc4Compound(ctx, &nfsv4_xdr.Compound4args{
			Tag:          "lock",
			Minorversion: 1,
			Argarray: []nfsv4_xdr.NfsArgop4{
				&nfsv4_xdr.NfsArgop4_OP_SEQUENCE{
					Opsequence: nfsv4_xdr.Sequence4args{
						SaSessionid: [...]byte{
							0xdb, 0xa7, 0xe2, 0xb6, 0x96, 0x5d, 0x27, 0xcf,
							0x69, 0xf4, 0x25, 0x1c, 0xb1, 0xd0, 0xa8, 0x9b,
						},
						SaSequenceid:    9,
						SaSlotid:        0,
						SaHighestSlotid: 0,
					},
				},
				&nfsv4_xdr.NfsArgop4_OP_PUTFH{
					Opputfh: nfsv4_xdr.Putfh4args{
						Object: nfsv4_xdr.NfsFh4{0x1e, 0xf9, 0x11, 0xc9, 0xb7, 0xb2, 0x6b, 0xeb},
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
								OpenStateid: nfsv4_xdr.Stateid4{
									Seqid: 5,
									Other: [...]byte{
										0x01, 0x00, 0x00, 0x00,
										0x00, 0x00, 0x00, 0x00,
										0x00, 0x00, 0x00, 0x00,
									},
								},
								LockOwner: nfsv4_xdr.LockOwner4{
									Owner: []byte{0xa0, 0xfa, 0x30, 0x1a, 0x95, 0x48, 0xba, 0x1f},
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
				&nfsv4_xdr.NfsResop4_OP_SEQUENCE{
					Opsequence: &nfsv4_xdr.Sequence4res_NFS4_OK{
						SrResok4: nfsv4_xdr.Sequence4resok{
							SrSessionid: [...]byte{
								0xdb, 0xa7, 0xe2, 0xb6, 0x96, 0x5d, 0x27, 0xcf,
								0x69, 0xf4, 0x25, 0x1c, 0xb1, 0xd0, 0xa8, 0x9b,
							},
							SrSequenceid:          9,
							SrSlotid:              0,
							SrHighestSlotid:       99,
							SrTargetHighestSlotid: 99,
							SrStatusFlags:         0,
						},
					},
				},
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
									0x02, 0x00, 0x00, 0x00,
									0x00, 0x00, 0x00, 0x00,
									0x00, 0x00, 0x00, 0x00,
								},
							},
						},
					},
				},
			},
			Status: nfsv4_xdr.NFS4_OK,
		}, resLock)

		clock.EXPECT().Now().Return(time.Unix(1010, 0)).Times(2)

		res, err = program.NfsV4Nfsproc4Compound(ctx, &nfsv4_xdr.Compound4args{
			Tag:          "open_downgrade",
			Minorversion: 1,
			Argarray: []nfsv4_xdr.NfsArgop4{
				&nfsv4_xdr.NfsArgop4_OP_SEQUENCE{
					Opsequence: nfsv4_xdr.Sequence4args{
						SaSessionid: [...]byte{
							0xdb, 0xa7, 0xe2, 0xb6, 0x96, 0x5d, 0x27, 0xcf,
							0x69, 0xf4, 0x25, 0x1c, 0xb1, 0xd0, 0xa8, 0x9b,
						},
						SaSequenceid:    10,
						SaSlotid:        0,
						SaHighestSlotid: 0,
					},
				},
				&nfsv4_xdr.NfsArgop4_OP_PUTFH{
					Opputfh: nfsv4_xdr.Putfh4args{
						Object: nfsv4_xdr.NfsFh4{0x1e, 0xf9, 0x11, 0xc9, 0xb7, 0xb2, 0x6b, 0xeb},
					},
				},
				&nfsv4_xdr.NfsArgop4_OP_OPEN_DOWNGRADE{
					OpopenDowngrade: nfsv4_xdr.OpenDowngrade4args{
						OpenStateid: nfsv4_xdr.Stateid4{
							Seqid: 5,
							Other: [...]byte{
								0x01, 0x00, 0x00, 0x00,
								0x00, 0x00, 0x00, 0x00,
								0x00, 0x00, 0x00, 0x00,
							},
						},
						ShareAccess: nfsv4_xdr.OPEN4_SHARE_ACCESS_READ,
						ShareDeny:   nfsv4_xdr.OPEN4_SHARE_DENY_NONE,
					},
				},
			},
		})
		require.NoError(t, err)
		require.Equal(t, &nfsv4_xdr.Compound4res{
			Tag: "open_downgrade",
			Resarray: []nfsv4_xdr.NfsResop4{
				&nfsv4_xdr.NfsResop4_OP_SEQUENCE{
					Opsequence: &nfsv4_xdr.Sequence4res_NFS4_OK{
						SrResok4: nfsv4_xdr.Sequence4resok{
							SrSessionid: [...]byte{
								0xdb, 0xa7, 0xe2, 0xb6, 0x96, 0x5d, 0x27, 0xcf,
								0x69, 0xf4, 0x25, 0x1c, 0xb1, 0xd0, 0xa8, 0x9b,
							},
							SrSequenceid:          10,
							SrSlotid:              0,
							SrHighestSlotid:       99,
							SrTargetHighestSlotid: 99,
							SrStatusFlags:         0,
						},
					},
				},
				&nfsv4_xdr.NfsResop4_OP_PUTFH{
					Opputfh: nfsv4_xdr.Putfh4res{
						Status: nfsv4_xdr.NFS4_OK,
					},
				},
				&nfsv4_xdr.NfsResop4_OP_OPEN_DOWNGRADE{
					OpopenDowngrade: &nfsv4_xdr.OpenDowngrade4res_NFS4_OK{
						Resok4: nfsv4_xdr.OpenDowngrade4resok{
							OpenStateid: nfsv4_xdr.Stateid4{
								Seqid: 6,
								Other: [...]byte{
									0x01, 0x00, 0x00, 0x00,
									0x00, 0x00, 0x00, 0x00,
									0x00, 0x00, 0x00, 0x00,
								},
							},
						},
					},
				},
			},
			Status: nfsv4_xdr.NFS4_OK,
		}, res)

		clock.EXPECT().Now().Return(time.Unix(1011, 0)).Times(2)

		res, err = program.NfsV4Nfsproc4Compound(ctx, &nfsv4_xdr.Compound4args{
			Tag:          "unlock",
			Minorversion: 1,
			Argarray: []nfsv4_xdr.NfsArgop4{
				&nfsv4_xdr.NfsArgop4_OP_SEQUENCE{
					Opsequence: nfsv4_xdr.Sequence4args{
						SaSessionid: [...]byte{
							0xdb, 0xa7, 0xe2, 0xb6, 0x96, 0x5d, 0x27, 0xcf,
							0x69, 0xf4, 0x25, 0x1c, 0xb1, 0xd0, 0xa8, 0x9b,
						},
						SaSequenceid:    11,
						SaSlotid:        0,
						SaHighestSlotid: 0,
					},
				},
				&nfsv4_xdr.NfsArgop4_OP_PUTFH{
					Opputfh: nfsv4_xdr.Putfh4args{
						Object: nfsv4_xdr.NfsFh4{0x1e, 0xf9, 0x11, 0xc9, 0xb7, 0xb2, 0x6b, 0xeb},
					},
				},
				&nfsv4_xdr.NfsArgop4_OP_LOCKU{
					Oplocku: nfsv4_xdr.Locku4args{
						Locktype: nfsv4_xdr.WRITE_LT,
						LockStateid: nfsv4_xdr.Stateid4{
							Seqid: 1,
							Other: [...]byte{
								0x02, 0x00, 0x00, 0x00,
								0x00, 0x00, 0x00, 0x00,
								0x00, 0x00, 0x00, 0x00,
							},
						},
						Offset: 100,
						Length: 100,
					},
				},
			},
		})
		require.NoError(t, err)
		require.Equal(t, &nfsv4_xdr.Compound4res{
			Tag: "unlock",
			Resarray: []nfsv4_xdr.NfsResop4{
				&nfsv4_xdr.NfsResop4_OP_SEQUENCE{
					Opsequence: &nfsv4_xdr.Sequence4res_NFS4_OK{
						SrResok4: nfsv4_xdr.Sequence4resok{
							SrSessionid: [...]byte{
								0xdb, 0xa7, 0xe2, 0xb6, 0x96, 0x5d, 0x27, 0xcf,
								0x69, 0xf4, 0x25, 0x1c, 0xb1, 0xd0, 0xa8, 0x9b,
							},
							SrSequenceid:          11,
							SrSlotid:              0,
							SrHighestSlotid:       99,
							SrTargetHighestSlotid: 99,
							SrStatusFlags:         0,
						},
					},
				},
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
								0x02, 0x00, 0x00, 0x00,
								0x00, 0x00, 0x00, 0x00,
								0x00, 0x00, 0x00, 0x00,
							},
						},
					},
				},
			},
			Status: nfsv4_xdr.NFS4_OK,
		}, res)

		clock.EXPECT().Now().Return(time.Unix(1012, 0)).Times(2)
		leaf.EXPECT().VirtualClose(virtual.ShareMaskWrite)

		res, err = program.NfsV4Nfsproc4Compound(ctx, &nfsv4_xdr.Compound4args{
			Tag:          "free_stateid",
			Minorversion: 1,
			Argarray: []nfsv4_xdr.NfsArgop4{
				&nfsv4_xdr.NfsArgop4_OP_SEQUENCE{
					Opsequence: nfsv4_xdr.Sequence4args{
						SaSessionid: [...]byte{
							0xdb, 0xa7, 0xe2, 0xb6, 0x96, 0x5d, 0x27, 0xcf,
							0x69, 0xf4, 0x25, 0x1c, 0xb1, 0xd0, 0xa8, 0x9b,
						},
						SaSequenceid:    12,
						SaSlotid:        0,
						SaHighestSlotid: 0,
					},
				},
				&nfsv4_xdr.NfsArgop4_OP_FREE_STATEID{
					OpfreeStateid: nfsv4_xdr.FreeStateid4args{
						FsaStateid: nfsv4_xdr.Stateid4{
							Seqid: 2,
							Other: [...]byte{
								0x02, 0x00, 0x00, 0x00,
								0x00, 0x00, 0x00, 0x00,
								0x00, 0x00, 0x00, 0x00,
							},
						},
					},
				},
			},
		})
		require.NoError(t, err)
		require.Equal(t, &nfsv4_xdr.Compound4res{
			Tag: "free_stateid",
			Resarray: []nfsv4_xdr.NfsResop4{
				&nfsv4_xdr.NfsResop4_OP_SEQUENCE{
					Opsequence: &nfsv4_xdr.Sequence4res_NFS4_OK{
						SrResok4: nfsv4_xdr.Sequence4resok{
							SrSessionid: [...]byte{
								0xdb, 0xa7, 0xe2, 0xb6, 0x96, 0x5d, 0x27, 0xcf,
								0x69, 0xf4, 0x25, 0x1c, 0xb1, 0xd0, 0xa8, 0x9b,
							},
							SrSequenceid:          12,
							SrSlotid:              0,
							SrHighestSlotid:       99,
							SrTargetHighestSlotid: 99,
							SrStatusFlags:         0,
						},
					},
				},
				&nfsv4_xdr.NfsResop4_OP_FREE_STATEID{
					OpfreeStateid: nfsv4_xdr.FreeStateid4res{
						FsrStatus: nfsv4_xdr.NFS4_OK,
					},
				},
			},
			Status: nfsv4_xdr.NFS4_OK,
		}, res)
	})

	t.Run("CurrentStateID", func(t *testing.T) {
		clock.EXPECT().Now().Return(time.Unix(1013, 0)).Times(2)
		leaf := mock.NewMockVirtualLeaf(ctrl)
		handleResolverExpectCall(t, handleResolver, []byte{0xa6, 0xb3, 0xf7, 0x96, 0x8f, 0x99, 0x11, 0x50}, virtual.DirectoryChild{}.FromLeaf(leaf), virtual.StatusOK)
		leaf.EXPECT().VirtualOpenSelf(ctx, virtual.ShareMaskRead|virtual.ShareMaskWrite, &virtual.OpenExistingOptions{}, virtual.AttributesMask(0), gomock.Any())
		leaf.EXPECT().VirtualClose(virtual.ShareMaskWrite)
		leaf.EXPECT().VirtualClose(virtual.ShareMaskRead)

		res, err := program.NfsV4Nfsproc4Compound(ctx, &nfsv4_xdr.Compound4args{
			Tag:          "all_in_one",
			Minorversion: 1,
			Argarray: []nfsv4_xdr.NfsArgop4{
				&nfsv4_xdr.NfsArgop4_OP_SEQUENCE{
					Opsequence: nfsv4_xdr.Sequence4args{
						SaSessionid: [...]byte{
							0xdb, 0xa7, 0xe2, 0xb6, 0x96, 0x5d, 0x27, 0xcf,
							0x69, 0xf4, 0x25, 0x1c, 0xb1, 0xd0, 0xa8, 0x9b,
						},
						SaSequenceid:    13,
						SaSlotid:        0,
						SaHighestSlotid: 0,
					},
				},
				&nfsv4_xdr.NfsArgop4_OP_PUTFH{
					Opputfh: nfsv4_xdr.Putfh4args{
						Object: nfsv4_xdr.NfsFh4{0xa6, 0xb3, 0xf7, 0x96, 0x8f, 0x99, 0x11, 0x50},
					},
				},
				&nfsv4_xdr.NfsArgop4_OP_OPEN{
					Opopen: nfsv4_xdr.Open4args{
						ShareAccess: nfsv4_xdr.OPEN4_SHARE_ACCESS_BOTH,
						ShareDeny:   nfsv4_xdr.OPEN4_SHARE_DENY_NONE,
						Owner: nfsv4_xdr.OpenOwner4{
							Owner: []byte{0x8a, 0x40, 0x8f, 0x00, 0x44, 0x1f, 0x6f, 0x9b},
						},
						Openhow: &nfsv4_xdr.Openflag4_default{},
						Claim:   &nfsv4_xdr.OpenClaim4_CLAIM_FH{},
					},
				},
				&nfsv4_xdr.NfsArgop4_OP_OPEN_DOWNGRADE{
					OpopenDowngrade: nfsv4_xdr.OpenDowngrade4args{
						OpenStateid: nfsv4_xdr.Stateid4{
							Seqid: 1,
							Other: [...]byte{
								0x00, 0x00, 0x00, 0x00,
								0x00, 0x00, 0x00, 0x00,
								0x00, 0x00, 0x00, 0x00,
							},
						},
						ShareAccess: nfsv4_xdr.OPEN4_SHARE_ACCESS_READ,
						ShareDeny:   nfsv4_xdr.OPEN4_SHARE_DENY_NONE,
					},
				},
				&nfsv4_xdr.NfsArgop4_OP_CLOSE{
					Opclose: nfsv4_xdr.Close4args{
						OpenStateid: nfsv4_xdr.Stateid4{
							Seqid: 1,
							Other: [...]byte{
								0x00, 0x00, 0x00, 0x00,
								0x00, 0x00, 0x00, 0x00,
								0x00, 0x00, 0x00, 0x00,
							},
						},
					},
				},
			},
		})
		require.NoError(t, err)
		require.Equal(t, &nfsv4_xdr.Compound4res{
			Tag: "all_in_one",
			Resarray: []nfsv4_xdr.NfsResop4{
				&nfsv4_xdr.NfsResop4_OP_SEQUENCE{
					Opsequence: &nfsv4_xdr.Sequence4res_NFS4_OK{
						SrResok4: nfsv4_xdr.Sequence4resok{
							SrSessionid: [...]byte{
								0xdb, 0xa7, 0xe2, 0xb6, 0x96, 0x5d, 0x27, 0xcf,
								0x69, 0xf4, 0x25, 0x1c, 0xb1, 0xd0, 0xa8, 0x9b,
							},
							SrSequenceid:          13,
							SrSlotid:              0,
							SrHighestSlotid:       99,
							SrTargetHighestSlotid: 99,
							SrStatusFlags:         0,
						},
					},
				},
				&nfsv4_xdr.NfsResop4_OP_PUTFH{
					Opputfh: nfsv4_xdr.Putfh4res{
						Status: nfsv4_xdr.NFS4_OK,
					},
				},
				&nfsv4_xdr.NfsResop4_OP_OPEN{
					Opopen: &nfsv4_xdr.Open4res_NFS4_OK{
						Resok4: nfsv4_xdr.Open4resok{
							Stateid: nfsv4_xdr.Stateid4{
								Seqid: 1,
								Other: [...]byte{
									0x03, 0x00, 0x00, 0x00,
									0x00, 0x00, 0x00, 0x00,
									0x00, 0x00, 0x00, 0x00,
								},
							},
							Cinfo:      nfsv4_xdr.ChangeInfo4{Atomic: true},
							Rflags:     nfsv4_xdr.OPEN4_RESULT_LOCKTYPE_POSIX | nfsv4_xdr.OPEN4_RESULT_PRESERVE_UNLINKED,
							Attrset:    nfsv4_xdr.Bitmap4{},
							Delegation: &nfsv4_xdr.OpenDelegation4_OPEN_DELEGATE_NONE{},
						},
					},
				},
				&nfsv4_xdr.NfsResop4_OP_OPEN_DOWNGRADE{
					OpopenDowngrade: &nfsv4_xdr.OpenDowngrade4res_NFS4_OK{
						Resok4: nfsv4_xdr.OpenDowngrade4resok{
							OpenStateid: nfsv4_xdr.Stateid4{
								Seqid: 2,
								Other: [...]byte{
									0x03, 0x00, 0x00, 0x00,
									0x00, 0x00, 0x00, 0x00,
									0x00, 0x00, 0x00, 0x00,
								},
							},
						},
					},
				},
				&nfsv4_xdr.NfsResop4_OP_CLOSE{
					Opclose: &nfsv4_xdr.Close4res_NFS4_OK{
						OpenStateid: nfsv4_xdr.Stateid4{
							Seqid: nfsv4_xdr.NFS4_UINT32_MAX,
							Other: [...]byte{
								0x00, 0x00, 0x00, 0x00,
								0x00, 0x00, 0x00, 0x00,
								0x00, 0x00, 0x00, 0x00,
							},
						},
					},
				},
			},
			Status: nfsv4_xdr.NFS4_OK,
		}, res)
	})
}

func TestNFS41ProgramCompound_OP_SEQUENCE(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	rootDirectory := mock.NewMockVirtualDirectory(ctrl)
	rootDirectory.EXPECT().VirtualGetAttributes(gomock.Any(), virtual.AttributesMaskFileHandle, gomock.Any()).
		Do(func(ctx context.Context, requested virtual.AttributesMask, attributes *virtual.Attributes) {
			attributes.SetFileHandle([]byte{0xce, 0xb3, 0xe0, 0x45, 0xd1, 0xf4, 0x9d, 0x58})
		})
	handleResolver := mock.NewMockHandleResolver(ctrl)
	serverOwner := nfsv4_xdr.ServerOwner4{
		SoMinorId: 0x7ce2ca8e8dba70ef,
		SoMajorId: []byte{0xb8, 0x6d, 0x25, 0x82, 0xb1, 0x77, 0xe1, 0xf0},
	}
	serverScope := []byte{0x4a, 0x92, 0xbc, 0x46, 0xf9, 0xde, 0x21, 0xc1}
	randomNumberGenerator := mock.NewMockSingleThreadedGenerator(ctrl)
	rebootVerifier := nfsv4_xdr.Verifier4{0xe7, 0x04, 0x52, 0x70, 0xc4, 0x8a, 0x5b, 0xc3}
	clock := mock.NewMockClock(ctrl)
	program := nfsv4.NewNFS41Program(
		rootDirectory,
		nfsv4.NewOpenedFilesPool(handleResolver.Call),
		serverOwner,
		serverScope,
		&nfsv4_xdr.ChannelAttrs4{
			CaHeaderpadsize:         0,
			CaMaxrequestsize:        1 * 1024 * 1024,
			CaMaxresponsesize:       2 * 1024 * 1024,
			CaMaxresponsesizeCached: 64 * 1024,
			CaMaxoperations:         100,
			CaMaxrequests:           100,
		},
		randomNumberGenerator,
		rebootVerifier,
		clock,
		/* enforcedLeaseTime = */ 2*time.Minute,
		/* announcedLeaseTime = */ time.Minute,
	)

	t.Run("BadSession", func(t *testing.T) {
		// It's only possible to call SEQUENCE after calling
		// EXCHANGE_ID and CREATE_SESSION.
		clock.EXPECT().Now().Return(time.Unix(1000, 0))

		res, err := program.NfsV4Nfsproc4Compound(ctx, &nfsv4_xdr.Compound4args{
			Tag:          "sequence",
			Minorversion: 1,
			Argarray: []nfsv4_xdr.NfsArgop4{
				&nfsv4_xdr.NfsArgop4_OP_SEQUENCE{
					Opsequence: nfsv4_xdr.Sequence4args{
						SaSessionid: [...]byte{
							0x14, 0x2e, 0xc9, 0x50, 0x3f, 0x29, 0xe7, 0x13,
							0xf5, 0xc9, 0x30, 0xd9, 0xb1, 0x58, 0x30, 0x20,
						},
						SaSequenceid:    0x669aa033,
						SaSlotid:        11,
						SaHighestSlotid: 24,
						SaCachethis:     true,
					},
				},
			},
		})
		require.NoError(t, err)
		require.Equal(t, &nfsv4_xdr.Compound4res{
			Tag: "sequence",
			Resarray: []nfsv4_xdr.NfsResop4{
				&nfsv4_xdr.NfsResop4_OP_SEQUENCE{
					Opsequence: &nfsv4_xdr.Sequence4res_default{
						SrStatus: nfsv4_xdr.NFS4ERR_BADSESSION,
					},
				},
			},
			Status: nfsv4_xdr.NFS4ERR_BADSESSION,
		}, res)
	})

	// The remainder of the test assumes the presence of a session.
	clock.EXPECT().Now().Return(time.Unix(1001, 0)).Times(2)
	exchangeIDAndCreateSessionForTesting(
		ctx,
		t,
		serverOwner,
		serverScope,
		randomNumberGenerator,
		program,
		/* sessionID = */ [...]byte{
			0x0c, 0x7a, 0x3c, 0x7a, 0xa2, 0xd4, 0x2d, 0x9e,
			0xd3, 0x00, 0x96, 0x80, 0x6d, 0x93, 0xd1, 0x1c,
		},
	)

	t.Run("BadSlotID", func(t *testing.T) {
		// If a slot ID is provided that is in excess of
		// ca_maxrequests, the request should fail with
		// NFS4ERR_BADSLOT.
		for sequenceID := nfsv4_xdr.Sequenceid4(0); sequenceID < 10; sequenceID++ {
			if sequenceID != 1 {
				clock.EXPECT().Now().Return(time.Unix(1003, 0))

				res, err := program.NfsV4Nfsproc4Compound(ctx, &nfsv4_xdr.Compound4args{
					Tag:          "sequence",
					Minorversion: 1,
					Argarray: []nfsv4_xdr.NfsArgop4{
						&nfsv4_xdr.NfsArgop4_OP_SEQUENCE{
							Opsequence: nfsv4_xdr.Sequence4args{
								SaSessionid: [...]byte{
									0x0c, 0x7a, 0x3c, 0x7a, 0xa2, 0xd4, 0x2d, 0x9e,
									0xd3, 0x00, 0x96, 0x80, 0x6d, 0x93, 0xd1, 0x1c,
								},
								SaSequenceid:    sequenceID,
								SaSlotid:        300,
								SaHighestSlotid: 300,
								SaCachethis:     true,
							},
						},
					},
				})
				require.NoError(t, err)
				require.Equal(t, &nfsv4_xdr.Compound4res{
					Tag: "sequence",
					Resarray: []nfsv4_xdr.NfsResop4{
						&nfsv4_xdr.NfsResop4_OP_SEQUENCE{
							Opsequence: &nfsv4_xdr.Sequence4res_default{
								SrStatus: nfsv4_xdr.NFS4ERR_BADSLOT,
							},
						},
					},
					Status: nfsv4_xdr.NFS4ERR_BADSLOT,
				}, res)
			}
		}
	})

	t.Run("MisorderedSequence", func(t *testing.T) {
		// When a slot is in its initial state, the only valid
		// sequence ID that can be provides is 1.
		for sequenceID := nfsv4_xdr.Sequenceid4(0); sequenceID < 10; sequenceID++ {
			if sequenceID != 1 {
				clock.EXPECT().Now().Return(time.Unix(1004, 0))

				res, err := program.NfsV4Nfsproc4Compound(ctx, &nfsv4_xdr.Compound4args{
					Tag:          "sequence",
					Minorversion: 1,
					Argarray: []nfsv4_xdr.NfsArgop4{
						&nfsv4_xdr.NfsArgop4_OP_SEQUENCE{
							Opsequence: nfsv4_xdr.Sequence4args{
								SaSessionid: [...]byte{
									0x0c, 0x7a, 0x3c, 0x7a, 0xa2, 0xd4, 0x2d, 0x9e,
									0xd3, 0x00, 0x96, 0x80, 0x6d, 0x93, 0xd1, 0x1c,
								},
								SaSequenceid:    sequenceID,
								SaSlotid:        11,
								SaHighestSlotid: 24,
								SaCachethis:     true,
							},
						},
					},
				})
				require.NoError(t, err)
				require.Equal(t, &nfsv4_xdr.Compound4res{
					Tag: "sequence",
					Resarray: []nfsv4_xdr.NfsResop4{
						&nfsv4_xdr.NfsResop4_OP_SEQUENCE{
							Opsequence: &nfsv4_xdr.Sequence4res_default{
								SrStatus: nfsv4_xdr.NFS4ERR_SEQ_MISORDERED,
							},
						},
					},
					Status: nfsv4_xdr.NFS4ERR_SEQ_MISORDERED,
				}, res)
			}
		}
	})

	t.Run("SuccessNoopSequencesWithCaching", func(t *testing.T) {
		// Attempt to execute a couple of sequence that don't
		// contain any operations. Execute them multiple times
		// to ensure caching works as expected.
		for sequenceID := nfsv4_xdr.Sequenceid4(1); sequenceID <= 10; sequenceID++ {
			clock.EXPECT().Now().Return(time.Unix(1005, 0)).Times(11)

			for i := 0; i < 10; i++ {
				res, err := program.NfsV4Nfsproc4Compound(ctx, &nfsv4_xdr.Compound4args{
					Tag:          "sequence",
					Minorversion: 1,
					Argarray: []nfsv4_xdr.NfsArgop4{
						&nfsv4_xdr.NfsArgop4_OP_SEQUENCE{
							Opsequence: nfsv4_xdr.Sequence4args{
								SaSessionid: [...]byte{
									0x0c, 0x7a, 0x3c, 0x7a, 0xa2, 0xd4, 0x2d, 0x9e,
									0xd3, 0x00, 0x96, 0x80, 0x6d, 0x93, 0xd1, 0x1c,
								},
								SaSequenceid:    sequenceID,
								SaSlotid:        0,
								SaHighestSlotid: 0,
								SaCachethis:     true,
							},
						},
					},
				})
				require.NoError(t, err)
				require.Equal(t, &nfsv4_xdr.Compound4res{
					Tag: "sequence",
					Resarray: []nfsv4_xdr.NfsResop4{
						&nfsv4_xdr.NfsResop4_OP_SEQUENCE{
							Opsequence: &nfsv4_xdr.Sequence4res_NFS4_OK{
								SrResok4: nfsv4_xdr.Sequence4resok{
									SrSessionid: [...]byte{
										0x0c, 0x7a, 0x3c, 0x7a, 0xa2, 0xd4, 0x2d, 0x9e,
										0xd3, 0x00, 0x96, 0x80, 0x6d, 0x93, 0xd1, 0x1c,
									},
									SrSequenceid:          sequenceID,
									SrSlotid:              0,
									SrHighestSlotid:       99,
									SrTargetHighestSlotid: 99,
									SrStatusFlags:         0,
								},
							},
						},
					},
					Status: nfsv4_xdr.NFS4_OK,
				}, res)
			}
		}
	})

	t.Run("SuccessNoopSequencesWithoutCaching", func(t *testing.T) {
		// Even without sa_cachethis set, empty sequences MUST
		// be cached. The reason being that
		// NFS4ERR_RETRY_UNCACHED_REP can only be attached to
		// the second operation in the sequence.
		for sequenceID := nfsv4_xdr.Sequenceid4(11); sequenceID <= 20; sequenceID++ {
			clock.EXPECT().Now().Return(time.Unix(1006, 0)).Times(11)

			for i := 0; i < 10; i++ {
				res, err := program.NfsV4Nfsproc4Compound(ctx, &nfsv4_xdr.Compound4args{
					Tag:          "sequence",
					Minorversion: 1,
					Argarray: []nfsv4_xdr.NfsArgop4{
						&nfsv4_xdr.NfsArgop4_OP_SEQUENCE{
							Opsequence: nfsv4_xdr.Sequence4args{
								SaSessionid: [...]byte{
									0x0c, 0x7a, 0x3c, 0x7a, 0xa2, 0xd4, 0x2d, 0x9e,
									0xd3, 0x00, 0x96, 0x80, 0x6d, 0x93, 0xd1, 0x1c,
								},
								SaSequenceid:    sequenceID,
								SaSlotid:        0,
								SaHighestSlotid: 0,
							},
						},
					},
				})
				require.NoError(t, err)
				require.Equal(t, &nfsv4_xdr.Compound4res{
					Tag: "sequence",
					Resarray: []nfsv4_xdr.NfsResop4{
						&nfsv4_xdr.NfsResop4_OP_SEQUENCE{
							Opsequence: &nfsv4_xdr.Sequence4res_NFS4_OK{
								SrResok4: nfsv4_xdr.Sequence4resok{
									SrSessionid: [...]byte{
										0x0c, 0x7a, 0x3c, 0x7a, 0xa2, 0xd4, 0x2d, 0x9e,
										0xd3, 0x00, 0x96, 0x80, 0x6d, 0x93, 0xd1, 0x1c,
									},
									SrSequenceid:          sequenceID,
									SrSlotid:              0,
									SrHighestSlotid:       99,
									SrTargetHighestSlotid: 99,
									SrStatusFlags:         0,
								},
							},
						},
					},
					Status: nfsv4_xdr.NFS4_OK,
				}, res)
			}
		}
	})

	t.Run("SuccessIllegalOperationWithoutCaching", func(t *testing.T) {
		// If the second operation is an illegal operation, the
		// server MUST NOT ever return NFS4ERR_RETRY_UNCACHED_REP.
		for sequenceID := nfsv4_xdr.Sequenceid4(21); sequenceID <= 30; sequenceID++ {
			clock.EXPECT().Now().Return(time.Unix(1007, 0)).Times(11)

			for i := 0; i < 10; i++ {
				res, err := program.NfsV4Nfsproc4Compound(ctx, &nfsv4_xdr.Compound4args{
					Tag:          "setclientid",
					Minorversion: 1,
					Argarray: []nfsv4_xdr.NfsArgop4{
						&nfsv4_xdr.NfsArgop4_OP_SEQUENCE{
							Opsequence: nfsv4_xdr.Sequence4args{
								SaSessionid: [...]byte{
									0x0c, 0x7a, 0x3c, 0x7a, 0xa2, 0xd4, 0x2d, 0x9e,
									0xd3, 0x00, 0x96, 0x80, 0x6d, 0x93, 0xd1, 0x1c,
								},
								SaSequenceid:    sequenceID,
								SaSlotid:        0,
								SaHighestSlotid: 0,
							},
						},
						&nfsv4_xdr.NfsArgop4_OP_SETCLIENTID{
							Opsetclientid: nfsv4_xdr.Setclientid4args{
								Client: nfsv4_xdr.NfsClientId4{
									Verifier: nfsv4_xdr.Verifier4{0x0f, 0x64, 0xe8, 0x55, 0xbc, 0xfe, 0x47, 0x1f},
									Id:       []byte{0x7d, 0xe8, 0xe3, 0x61, 0x92, 0xef, 0x0a, 0xce},
								},
								Callback: nfsv4_xdr.CbClient4{
									CbProgram: 0xc108f968,
									CbLocation: nfsv4_xdr.Clientaddr4{
										NaRNetid: "tcp",
										NaRAddr:  "127.0.0.1.196.95",
									},
								},
								CallbackIdent: 0xbea3e2f0,
							},
						},
					},
				})
				require.NoError(t, err)
				require.Equal(t, &nfsv4_xdr.Compound4res{
					Tag: "setclientid",
					Resarray: []nfsv4_xdr.NfsResop4{
						&nfsv4_xdr.NfsResop4_OP_SEQUENCE{
							Opsequence: &nfsv4_xdr.Sequence4res_NFS4_OK{
								SrResok4: nfsv4_xdr.Sequence4resok{
									SrSessionid: [...]byte{
										0x0c, 0x7a, 0x3c, 0x7a, 0xa2, 0xd4, 0x2d, 0x9e,
										0xd3, 0x00, 0x96, 0x80, 0x6d, 0x93, 0xd1, 0x1c,
									},
									SrSequenceid:          sequenceID,
									SrSlotid:              0,
									SrHighestSlotid:       99,
									SrTargetHighestSlotid: 99,
									SrStatusFlags:         0,
								},
							},
						},
						&nfsv4_xdr.NfsResop4_OP_ILLEGAL{
							Opillegal: nfsv4_xdr.Illegal4res{
								Status: nfsv4_xdr.NFS4ERR_OP_ILLEGAL,
							},
						},
					},
					Status: nfsv4_xdr.NFS4ERR_OP_ILLEGAL,
				}, res)
			}
		}
	})

	t.Run("SuccessMkdirWithoutCaching", func(t *testing.T) {
		clock.EXPECT().Now().Return(time.Unix(1008, 0)).Times(2)
		directory := mock.NewMockVirtualDirectory(ctrl)
		rootDirectory.EXPECT().VirtualMkdir(
			path.MustNewComponent("dir"),
			virtual.AttributesMaskFileHandle,
			gomock.Any(),
		).DoAndReturn(func(name path.Component, requested virtual.AttributesMask, attributes *virtual.Attributes) (virtual.Directory, virtual.ChangeInfo, virtual.Status) {
			attributes.SetFileHandle([]byte{0x47, 0xec, 0xdd, 0x99, 0x8b, 0x03, 0xc7, 0x0a})
			return directory, virtual.ChangeInfo{
				Before: 0x65756cf36a69b8e2,
				After:  0xe9400721315a8797,
			}, virtual.StatusOK
		})

		res, err := program.NfsV4Nfsproc4Compound(ctx, &nfsv4_xdr.Compound4args{
			Tag:          "mkdir",
			Minorversion: 1,
			Argarray: []nfsv4_xdr.NfsArgop4{
				&nfsv4_xdr.NfsArgop4_OP_SEQUENCE{
					Opsequence: nfsv4_xdr.Sequence4args{
						SaSessionid: [...]byte{
							0x0c, 0x7a, 0x3c, 0x7a, 0xa2, 0xd4, 0x2d, 0x9e,
							0xd3, 0x00, 0x96, 0x80, 0x6d, 0x93, 0xd1, 0x1c,
						},
						SaSequenceid:    31,
						SaSlotid:        0,
						SaHighestSlotid: 0,
					},
				},
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
			Tag: "mkdir",
			Resarray: []nfsv4_xdr.NfsResop4{
				&nfsv4_xdr.NfsResop4_OP_SEQUENCE{
					Opsequence: &nfsv4_xdr.Sequence4res_NFS4_OK{
						SrResok4: nfsv4_xdr.Sequence4resok{
							SrSessionid: [...]byte{
								0x0c, 0x7a, 0x3c, 0x7a, 0xa2, 0xd4, 0x2d, 0x9e,
								0xd3, 0x00, 0x96, 0x80, 0x6d, 0x93, 0xd1, 0x1c,
							},
							SrSequenceid:          31,
							SrSlotid:              0,
							SrHighestSlotid:       99,
							SrTargetHighestSlotid: 99,
							SrStatusFlags:         0,
						},
					},
				},
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
								Before: 0x65756cf36a69b8e2,
								After:  0xe9400721315a8797,
							},
						},
					},
				},
				&nfsv4_xdr.NfsResop4_OP_GETFH{
					Opgetfh: &nfsv4_xdr.Getfh4res_NFS4_OK{
						Resok4: nfsv4_xdr.Getfh4resok{
							Object: nfsv4_xdr.NfsFh4{
								0x47, 0xec, 0xdd, 0x99, 0x8b, 0x03, 0xc7, 0x0a,
							},
						},
					},
				},
			},
			Status: nfsv4_xdr.NFS4_OK,
		}, res)

		for i := 0; i < 10; i++ {
			clock.EXPECT().Now().Return(time.Unix(1009, 0))

			res, err := program.NfsV4Nfsproc4Compound(ctx, &nfsv4_xdr.Compound4args{
				Tag:          "mkdir",
				Minorversion: 1,
				Argarray: []nfsv4_xdr.NfsArgop4{
					&nfsv4_xdr.NfsArgop4_OP_SEQUENCE{
						Opsequence: nfsv4_xdr.Sequence4args{
							SaSessionid: [...]byte{
								0x0c, 0x7a, 0x3c, 0x7a, 0xa2, 0xd4, 0x2d, 0x9e,
								0xd3, 0x00, 0x96, 0x80, 0x6d, 0x93, 0xd1, 0x1c,
							},
							SaSequenceid:    31,
							SaSlotid:        0,
							SaHighestSlotid: 0,
						},
					},
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
				Tag: "mkdir",
				Resarray: []nfsv4_xdr.NfsResop4{
					&nfsv4_xdr.NfsResop4_OP_SEQUENCE{
						Opsequence: &nfsv4_xdr.Sequence4res_NFS4_OK{
							SrResok4: nfsv4_xdr.Sequence4resok{
								SrSessionid: [...]byte{
									0x0c, 0x7a, 0x3c, 0x7a, 0xa2, 0xd4, 0x2d, 0x9e,
									0xd3, 0x00, 0x96, 0x80, 0x6d, 0x93, 0xd1, 0x1c,
								},
								SrSequenceid:          31,
								SrSlotid:              0,
								SrHighestSlotid:       99,
								SrTargetHighestSlotid: 99,
								SrStatusFlags:         0,
							},
						},
					},
					&nfsv4_xdr.NfsResop4_OP_PUTROOTFH{
						Opputrootfh: nfsv4_xdr.Putrootfh4res{
							Status: nfsv4_xdr.NFS4ERR_RETRY_UNCACHED_REP,
						},
					},
				},
				Status: nfsv4_xdr.NFS4ERR_RETRY_UNCACHED_REP,
			}, res)
		}
	})

	t.Run("SequenceInsideSequence", func(t *testing.T) {
		// SEQUENCE operations can only appear at the start of a
		// compound procedure.
		clock.EXPECT().Now().Return(time.Unix(1010, 0)).Times(2)

		res, err := program.NfsV4Nfsproc4Compound(ctx, &nfsv4_xdr.Compound4args{
			Tag:          "sequence",
			Minorversion: 1,
			Argarray: []nfsv4_xdr.NfsArgop4{
				&nfsv4_xdr.NfsArgop4_OP_SEQUENCE{
					Opsequence: nfsv4_xdr.Sequence4args{
						SaSessionid: [...]byte{
							0x0c, 0x7a, 0x3c, 0x7a, 0xa2, 0xd4, 0x2d, 0x9e,
							0xd3, 0x00, 0x96, 0x80, 0x6d, 0x93, 0xd1, 0x1c,
						},
						SaSequenceid:    32,
						SaSlotid:        0,
						SaHighestSlotid: 0,
					},
				},
				&nfsv4_xdr.NfsArgop4_OP_SEQUENCE{
					Opsequence: nfsv4_xdr.Sequence4args{
						SaSessionid: [...]byte{
							0x0c, 0x7a, 0x3c, 0x7a, 0xa2, 0xd4, 0x2d, 0x9e,
							0xd3, 0x00, 0x96, 0x80, 0x6d, 0x93, 0xd1, 0x1c,
						},
						SaSequenceid:    33,
						SaSlotid:        0,
						SaHighestSlotid: 0,
					},
				},
			},
		})
		require.NoError(t, err)
		require.Equal(t, &nfsv4_xdr.Compound4res{
			Tag: "sequence",
			Resarray: []nfsv4_xdr.NfsResop4{
				&nfsv4_xdr.NfsResop4_OP_SEQUENCE{
					Opsequence: &nfsv4_xdr.Sequence4res_NFS4_OK{
						SrResok4: nfsv4_xdr.Sequence4resok{
							SrSessionid: [...]byte{
								0x0c, 0x7a, 0x3c, 0x7a, 0xa2, 0xd4, 0x2d, 0x9e,
								0xd3, 0x00, 0x96, 0x80, 0x6d, 0x93, 0xd1, 0x1c,
							},
							SrSequenceid:          32,
							SrSlotid:              0,
							SrHighestSlotid:       99,
							SrTargetHighestSlotid: 99,
							SrStatusFlags:         0,
						},
					},
				},
				&nfsv4_xdr.NfsResop4_OP_SEQUENCE{
					Opsequence: &nfsv4_xdr.Sequence4res_default{
						SrStatus: nfsv4_xdr.NFS4ERR_SEQUENCE_POS,
					},
				},
			},
			Status: nfsv4_xdr.NFS4ERR_SEQUENCE_POS,
		}, res)
	})

	t.Run("FalseRetries", func(t *testing.T) {
		// The server should perform some basic checks to ensure
		// that a client gets its sequencing right. When it
		// retransmits a response, it should ensure that the
		// shape of the response matches the provided request.
		for i := 0; i < 10; i++ {
			clock.EXPECT().Now().Return(time.Unix(1011, 0))
			if i == 0 {
				clock.EXPECT().Now().Return(time.Unix(1011, 0))
			}
			res, err := program.NfsV4Nfsproc4Compound(ctx, &nfsv4_xdr.Compound4args{
				Tag:          "sequence",
				Minorversion: 1,
				Argarray: []nfsv4_xdr.NfsArgop4{
					&nfsv4_xdr.NfsArgop4_OP_SEQUENCE{
						Opsequence: nfsv4_xdr.Sequence4args{
							SaSessionid: [...]byte{
								0x0c, 0x7a, 0x3c, 0x7a, 0xa2, 0xd4, 0x2d, 0x9e,
								0xd3, 0x00, 0x96, 0x80, 0x6d, 0x93, 0xd1, 0x1c,
							},
							SaSequenceid:    33,
							SaSlotid:        0,
							SaHighestSlotid: 0,
							SaCachethis:     true,
						},
					},
					&nfsv4_xdr.NfsArgop4_OP_PUTROOTFH{},
					&nfsv4_xdr.NfsArgop4_OP_PUTROOTFH{},
				},
			})
			require.NoError(t, err)
			require.Equal(t, &nfsv4_xdr.Compound4res{
				Tag: "sequence",
				Resarray: []nfsv4_xdr.NfsResop4{
					&nfsv4_xdr.NfsResop4_OP_SEQUENCE{
						Opsequence: &nfsv4_xdr.Sequence4res_NFS4_OK{
							SrResok4: nfsv4_xdr.Sequence4resok{
								SrSessionid: [...]byte{
									0x0c, 0x7a, 0x3c, 0x7a, 0xa2, 0xd4, 0x2d, 0x9e,
									0xd3, 0x00, 0x96, 0x80, 0x6d, 0x93, 0xd1, 0x1c,
								},
								SrSequenceid:          33,
								SrSlotid:              0,
								SrHighestSlotid:       99,
								SrTargetHighestSlotid: 99,
								SrStatusFlags:         0,
							},
						},
					},
					&nfsv4_xdr.NfsResop4_OP_PUTROOTFH{
						Opputrootfh: nfsv4_xdr.Putrootfh4res{
							Status: nfsv4_xdr.NFS4_OK,
						},
					},
					&nfsv4_xdr.NfsResop4_OP_PUTROOTFH{
						Opputrootfh: nfsv4_xdr.Putrootfh4res{
							Status: nfsv4_xdr.NFS4_OK,
						},
					},
				},
				Status: nfsv4_xdr.NFS4_OK,
			}, res)
		}

		t.Run("NotEnoughOperations", func(t *testing.T) {
			// The cached response contains results for
			// three operations, whereas the retransmitted
			// requests contains only contains two.
			clock.EXPECT().Now().Return(time.Unix(1012, 0))

			res, err := program.NfsV4Nfsproc4Compound(ctx, &nfsv4_xdr.Compound4args{
				Tag:          "sequence",
				Minorversion: 1,
				Argarray: []nfsv4_xdr.NfsArgop4{
					&nfsv4_xdr.NfsArgop4_OP_SEQUENCE{
						Opsequence: nfsv4_xdr.Sequence4args{
							SaSessionid: [...]byte{
								0x0c, 0x7a, 0x3c, 0x7a, 0xa2, 0xd4, 0x2d, 0x9e,
								0xd3, 0x00, 0x96, 0x80, 0x6d, 0x93, 0xd1, 0x1c,
							},
							SaSequenceid:    33,
							SaSlotid:        0,
							SaHighestSlotid: 0,
							SaCachethis:     true,
						},
					},
					&nfsv4_xdr.NfsArgop4_OP_PUTROOTFH{},
				},
			})
			require.NoError(t, err)
			require.Equal(t, &nfsv4_xdr.Compound4res{
				Tag: "sequence",
				Resarray: []nfsv4_xdr.NfsResop4{
					&nfsv4_xdr.NfsResop4_OP_SEQUENCE{
						Opsequence: &nfsv4_xdr.Sequence4res_default{
							SrStatus: nfsv4_xdr.NFS4ERR_SEQ_FALSE_RETRY,
						},
					},
				},
				Status: nfsv4_xdr.NFS4ERR_SEQ_FALSE_RETRY,
			}, res)
		})

		t.Run("TooManyOperations", func(t *testing.T) {
			// The cached response contains results for
			// three operations, whereas the retransmitted
			// requests contains four. This is impossible,
			// as the cached response is successful.
			clock.EXPECT().Now().Return(time.Unix(1013, 0))

			res, err := program.NfsV4Nfsproc4Compound(ctx, &nfsv4_xdr.Compound4args{
				Tag:          "sequence",
				Minorversion: 1,
				Argarray: []nfsv4_xdr.NfsArgop4{
					&nfsv4_xdr.NfsArgop4_OP_SEQUENCE{
						Opsequence: nfsv4_xdr.Sequence4args{
							SaSessionid: [...]byte{
								0x0c, 0x7a, 0x3c, 0x7a, 0xa2, 0xd4, 0x2d, 0x9e,
								0xd3, 0x00, 0x96, 0x80, 0x6d, 0x93, 0xd1, 0x1c,
							},
							SaSequenceid:    33,
							SaSlotid:        0,
							SaHighestSlotid: 0,
							SaCachethis:     true,
						},
					},
					&nfsv4_xdr.NfsArgop4_OP_PUTROOTFH{},
					&nfsv4_xdr.NfsArgop4_OP_PUTROOTFH{},
					&nfsv4_xdr.NfsArgop4_OP_PUTROOTFH{},
				},
			})
			require.NoError(t, err)
			require.Equal(t, &nfsv4_xdr.Compound4res{
				Tag: "sequence",
				Resarray: []nfsv4_xdr.NfsResop4{
					&nfsv4_xdr.NfsResop4_OP_SEQUENCE{
						Opsequence: &nfsv4_xdr.Sequence4res_default{
							SrStatus: nfsv4_xdr.NFS4ERR_SEQ_FALSE_RETRY,
						},
					},
				},
				Status: nfsv4_xdr.NFS4ERR_SEQ_FALSE_RETRY,
			}, res)
		})

		t.Run("MismatchingOperationType", func(t *testing.T) {
			// The number of operations in the cached
			// response is the same as in the request, but
			// the operation types don't match.
			clock.EXPECT().Now().Return(time.Unix(1014, 0))

			res, err := program.NfsV4Nfsproc4Compound(ctx, &nfsv4_xdr.Compound4args{
				Tag:          "sequence",
				Minorversion: 1,
				Argarray: []nfsv4_xdr.NfsArgop4{
					&nfsv4_xdr.NfsArgop4_OP_SEQUENCE{
						Opsequence: nfsv4_xdr.Sequence4args{
							SaSessionid: [...]byte{
								0x0c, 0x7a, 0x3c, 0x7a, 0xa2, 0xd4, 0x2d, 0x9e,
								0xd3, 0x00, 0x96, 0x80, 0x6d, 0x93, 0xd1, 0x1c,
							},
							SaSequenceid:    33,
							SaSlotid:        0,
							SaHighestSlotid: 0,
							SaCachethis:     true,
						},
					},
					&nfsv4_xdr.NfsArgop4_OP_PUTROOTFH{},
					&nfsv4_xdr.NfsArgop4_OP_OPEN{
						Opopen: nfsv4_xdr.Open4args{
							ShareAccess: nfsv4_xdr.OPEN4_SHARE_ACCESS_BOTH,
							ShareDeny:   nfsv4_xdr.OPEN4_SHARE_DENY_NONE,
							Owner: nfsv4_xdr.OpenOwner4{
								Owner: []byte{0x8c, 0x2b, 0x0d, 0xca, 0xa2, 0x9c, 0x49, 0x08},
							},
							Openhow: &nfsv4_xdr.Openflag4_default{
								Opentype: nfsv4_xdr.OPEN4_NOCREATE,
							},
							Claim: &nfsv4_xdr.OpenClaim4_CLAIM_NULL{
								File: "Hello",
							},
						},
					},
				},
			})
			require.NoError(t, err)
			require.Equal(t, &nfsv4_xdr.Compound4res{
				Tag: "sequence",
				Resarray: []nfsv4_xdr.NfsResop4{
					&nfsv4_xdr.NfsResop4_OP_SEQUENCE{
						Opsequence: &nfsv4_xdr.Sequence4res_default{
							SrStatus: nfsv4_xdr.NFS4ERR_SEQ_FALSE_RETRY,
						},
					},
				},
				Status: nfsv4_xdr.NFS4ERR_SEQ_FALSE_RETRY,
			}, res)
		})
	})
}

func TestNFS41ProgramCompound_OP_TEST_STATEID(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	rootDirectory := mock.NewMockVirtualDirectory(ctrl)
	rootDirectory.EXPECT().VirtualGetAttributes(gomock.Any(), virtual.AttributesMaskFileHandle, gomock.Any()).
		Do(func(ctx context.Context, requested virtual.AttributesMask, attributes *virtual.Attributes) {
			attributes.SetFileHandle([]byte{0x26, 0x81, 0x05, 0xaf, 0x27, 0xaa, 0xd3, 0x4a})
		})
	handleResolver := mock.NewMockHandleResolver(ctrl)
	serverOwner := nfsv4_xdr.ServerOwner4{
		SoMinorId: 0x3a644b21cbecc828,
		SoMajorId: []byte{0xa8, 0xfa, 0xdd, 0x85, 0xf4, 0x89, 0x23, 0x25},
	}
	serverScope := []byte{0x2c, 0xbc, 0xf7, 0x1c, 0xce, 0x2d, 0xea, 0x4e}
	randomNumberGenerator := mock.NewMockSingleThreadedGenerator(ctrl)
	rebootVerifier := nfsv4_xdr.Verifier4{0xde, 0x24, 0xc6, 0x26, 0x14, 0x94, 0x39, 0x64}
	clock := mock.NewMockClock(ctrl)
	program := nfsv4.NewNFS41Program(
		rootDirectory,
		nfsv4.NewOpenedFilesPool(handleResolver.Call),
		serverOwner,
		serverScope,
		&nfsv4_xdr.ChannelAttrs4{
			CaHeaderpadsize:         0,
			CaMaxrequestsize:        1 * 1024 * 1024,
			CaMaxresponsesize:       2 * 1024 * 1024,
			CaMaxresponsesizeCached: 64 * 1024,
			CaMaxoperations:         100,
			CaMaxrequests:           100,
		},
		randomNumberGenerator,
		rebootVerifier,
		clock,
		/* enforcedLeaseTime = */ 2*time.Minute,
		/* announcedLeaseTime = */ time.Minute,
	)

	// The remainder of the test assumes the presence of a session.
	clock.EXPECT().Now().Return(time.Unix(1001, 0)).Times(2)
	exchangeIDAndCreateSessionForTesting(
		ctx,
		t,
		serverOwner,
		serverScope,
		randomNumberGenerator,
		program,
		/* sessionID = */ [...]byte{
			0x22, 0xa7, 0xd9, 0xef, 0x49, 0x6c, 0x95, 0x0a,
			0x9f, 0xc5, 0xab, 0xe3, 0x91, 0x57, 0x45, 0x7a,
		},
	)

	// Open a file and acquire some locks on it, so that we have a
	// valid OPEN state ID and byte-range lock state ID.
	clock.EXPECT().Now().Return(time.Unix(1000, 0)).Times(2)
	leaf := mock.NewMockVirtualLeaf(ctrl)
	handleResolverExpectCall(t, handleResolver, []byte{0xe8, 0x60, 0x20, 0x59, 0x75, 0x10, 0x74, 0x9b}, virtual.DirectoryChild{}.FromLeaf(leaf), virtual.StatusOK)
	leaf.EXPECT().VirtualOpenSelf(
		ctx,
		virtual.ShareMaskRead|virtual.ShareMaskWrite,
		&virtual.OpenExistingOptions{},
		virtual.AttributesMask(0),
		gomock.Any(),
	)
	leaf.EXPECT().VirtualClose(virtual.ShareMaskWrite)

	res, err := program.NfsV4Nfsproc4Compound(ctx, &nfsv4_xdr.Compound4args{
		Tag:          "open_and_lock",
		Minorversion: 1,
		Argarray: []nfsv4_xdr.NfsArgop4{
			&nfsv4_xdr.NfsArgop4_OP_SEQUENCE{
				Opsequence: nfsv4_xdr.Sequence4args{
					SaSessionid: [...]byte{
						0x22, 0xa7, 0xd9, 0xef, 0x49, 0x6c, 0x95, 0x0a,
						0x9f, 0xc5, 0xab, 0xe3, 0x91, 0x57, 0x45, 0x7a,
					},
					SaSequenceid:    1,
					SaSlotid:        0,
					SaHighestSlotid: 0,
				},
			},
			&nfsv4_xdr.NfsArgop4_OP_PUTFH{
				Opputfh: nfsv4_xdr.Putfh4args{
					Object: nfsv4_xdr.NfsFh4{0xe8, 0x60, 0x20, 0x59, 0x75, 0x10, 0x74, 0x9b},
				},
			},
			&nfsv4_xdr.NfsArgop4_OP_OPEN{
				Opopen: nfsv4_xdr.Open4args{
					ShareAccess: nfsv4_xdr.OPEN4_SHARE_ACCESS_BOTH,
					ShareDeny:   nfsv4_xdr.OPEN4_SHARE_DENY_NONE,
					Owner: nfsv4_xdr.OpenOwner4{
						Owner: []byte{0x5c, 0x95, 0xf6, 0xf0, 0x49, 0xbc, 0xf0, 0xe0},
					},
					Openhow: &nfsv4_xdr.Openflag4_default{
						Opentype: nfsv4_xdr.OPEN4_NOCREATE,
					},
					Claim: &nfsv4_xdr.OpenClaim4_CLAIM_FH{},
				},
			},
			&nfsv4_xdr.NfsArgop4_OP_OPEN_DOWNGRADE{
				OpopenDowngrade: nfsv4_xdr.OpenDowngrade4args{
					OpenStateid: nfsv4_xdr.Stateid4{
						Seqid: 1,
						Other: [...]byte{
							0x00, 0x00, 0x00, 0x00,
							0x00, 0x00, 0x00, 0x00,
							0x00, 0x00, 0x00, 0x00,
						},
					},
					ShareAccess: nfsv4_xdr.OPEN4_SHARE_ACCESS_READ,
					ShareDeny:   nfsv4_xdr.OPEN4_SHARE_DENY_NONE,
				},
			},
			&nfsv4_xdr.NfsArgop4_OP_LOCK{
				Oplock: nfsv4_xdr.Lock4args{
					Locktype: nfsv4_xdr.READ_LT,
					Reclaim:  false,
					Offset:   100,
					Length:   100,
					Locker: &nfsv4_xdr.Locker4_TRUE{
						OpenOwner: nfsv4_xdr.OpenToLockOwner4{
							OpenStateid: nfsv4_xdr.Stateid4{
								Seqid: 1,
								Other: [...]byte{
									0x00, 0x00, 0x00, 0x00,
									0x00, 0x00, 0x00, 0x00,
									0x00, 0x00, 0x00, 0x00,
								},
							},
							LockOwner: nfsv4_xdr.LockOwner4{
								Owner: []byte{0xb5, 0x4e, 0x5b, 0x6f, 0x30, 0xb7, 0x32, 0xbf},
							},
						},
					},
				},
			},
			&nfsv4_xdr.NfsArgop4_OP_LOCKU{
				Oplocku: nfsv4_xdr.Locku4args{
					Locktype: nfsv4_xdr.READ_LT,
					LockStateid: nfsv4_xdr.Stateid4{
						Seqid: 1,
						Other: [...]byte{
							0x00, 0x00, 0x00, 0x00,
							0x00, 0x00, 0x00, 0x00,
							0x00, 0x00, 0x00, 0x00,
						},
					},
					Offset: 100,
					Length: 100,
				},
			},
		},
	})
	require.NoError(t, err)
	require.Equal(t, &nfsv4_xdr.Compound4res{
		Tag: "open_and_lock",
		Resarray: []nfsv4_xdr.NfsResop4{
			&nfsv4_xdr.NfsResop4_OP_SEQUENCE{
				Opsequence: &nfsv4_xdr.Sequence4res_NFS4_OK{
					SrResok4: nfsv4_xdr.Sequence4resok{
						SrSessionid: [...]byte{
							0x22, 0xa7, 0xd9, 0xef, 0x49, 0x6c, 0x95, 0x0a,
							0x9f, 0xc5, 0xab, 0xe3, 0x91, 0x57, 0x45, 0x7a,
						},
						SrSequenceid:          1,
						SrSlotid:              0,
						SrHighestSlotid:       99,
						SrTargetHighestSlotid: 99,
						SrStatusFlags:         0,
					},
				},
			},
			&nfsv4_xdr.NfsResop4_OP_PUTFH{
				Opputfh: nfsv4_xdr.Putfh4res{
					Status: nfsv4_xdr.NFS4_OK,
				},
			},
			&nfsv4_xdr.NfsResop4_OP_OPEN{
				Opopen: &nfsv4_xdr.Open4res_NFS4_OK{
					Resok4: nfsv4_xdr.Open4resok{
						Stateid: nfsv4_xdr.Stateid4{
							Seqid: 1,
							Other: [...]byte{
								0x01, 0x00, 0x00, 0x00,
								0x00, 0x00, 0x00, 0x00,
								0x00, 0x00, 0x00, 0x00,
							},
						},
						Cinfo:      nfsv4_xdr.ChangeInfo4{Atomic: true},
						Rflags:     nfsv4_xdr.OPEN4_RESULT_LOCKTYPE_POSIX | nfsv4_xdr.OPEN4_RESULT_PRESERVE_UNLINKED,
						Attrset:    nfsv4_xdr.Bitmap4{},
						Delegation: &nfsv4_xdr.OpenDelegation4_OPEN_DELEGATE_NONE{},
					},
				},
			},
			&nfsv4_xdr.NfsResop4_OP_OPEN_DOWNGRADE{
				OpopenDowngrade: &nfsv4_xdr.OpenDowngrade4res_NFS4_OK{
					Resok4: nfsv4_xdr.OpenDowngrade4resok{
						OpenStateid: nfsv4_xdr.Stateid4{
							Seqid: 2,
							Other: [...]byte{
								0x01, 0x00, 0x00, 0x00,
								0x00, 0x00, 0x00, 0x00,
								0x00, 0x00, 0x00, 0x00,
							},
						},
					},
				},
			},
			&nfsv4_xdr.NfsResop4_OP_LOCK{
				Oplock: &nfsv4_xdr.Lock4res_NFS4_OK{
					Resok4: nfsv4_xdr.Lock4resok{
						LockStateid: nfsv4_xdr.Stateid4{
							Seqid: 1,
							Other: [...]byte{
								0x02, 0x00, 0x00, 0x00,
								0x00, 0x00, 0x00, 0x00,
								0x00, 0x00, 0x00, 0x00,
							},
						},
					},
				},
			},
			&nfsv4_xdr.NfsResop4_OP_LOCKU{
				Oplocku: &nfsv4_xdr.Locku4res_NFS4_OK{
					LockStateid: nfsv4_xdr.Stateid4{
						Seqid: 2,
						Other: [...]byte{
							0x02, 0x00, 0x00, 0x00,
							0x00, 0x00, 0x00, 0x00,
							0x00, 0x00, 0x00, 0x00,
						},
					},
				},
			},
		},
		Status: nfsv4_xdr.NFS4_OK,
	}, res)

	t.Run("Success", func(t *testing.T) {
		clock.EXPECT().Now().Return(time.Unix(1001, 0)).Times(2)

		res, err = program.NfsV4Nfsproc4Compound(ctx, &nfsv4_xdr.Compound4args{
			Tag:          "test_stateid",
			Minorversion: 1,
			Argarray: []nfsv4_xdr.NfsArgop4{
				&nfsv4_xdr.NfsArgop4_OP_SEQUENCE{
					Opsequence: nfsv4_xdr.Sequence4args{
						SaSessionid: [...]byte{
							0x22, 0xa7, 0xd9, 0xef, 0x49, 0x6c, 0x95, 0x0a,
							0x9f, 0xc5, 0xab, 0xe3, 0x91, 0x57, 0x45, 0x7a,
						},
						SaSequenceid:    2,
						SaSlotid:        0,
						SaHighestSlotid: 0,
					},
				},
				&nfsv4_xdr.NfsArgop4_OP_TEST_STATEID{
					OptestStateid: nfsv4_xdr.TestStateid4args{
						TsStateids: []nfsv4_xdr.Stateid4{
							// Open-owner.
							{
								Seqid: 0,
								Other: [...]byte{
									0x01, 0x00, 0x00, 0x00,
									0x00, 0x00, 0x00, 0x00,
									0x00, 0x00, 0x00, 0x00,
								},
							},
							{
								Seqid: 1,
								Other: [...]byte{
									0x01, 0x00, 0x00, 0x00,
									0x00, 0x00, 0x00, 0x00,
									0x00, 0x00, 0x00, 0x00,
								},
							},
							{
								Seqid: 2,
								Other: [...]byte{
									0x01, 0x00, 0x00, 0x00,
									0x00, 0x00, 0x00, 0x00,
									0x00, 0x00, 0x00, 0x00,
								},
							},
							{
								Seqid: 3,
								Other: [...]byte{
									0x01, 0x00, 0x00, 0x00,
									0x00, 0x00, 0x00, 0x00,
									0x00, 0x00, 0x00, 0x00,
								},
							},

							// Lock-owner.
							{
								Seqid: 0,
								Other: [...]byte{
									0x02, 0x00, 0x00, 0x00,
									0x00, 0x00, 0x00, 0x00,
									0x00, 0x00, 0x00, 0x00,
								},
							},
							{
								Seqid: 1,
								Other: [...]byte{
									0x02, 0x00, 0x00, 0x00,
									0x00, 0x00, 0x00, 0x00,
									0x00, 0x00, 0x00, 0x00,
								},
							},
							{
								Seqid: 2,
								Other: [...]byte{
									0x02, 0x00, 0x00, 0x00,
									0x00, 0x00, 0x00, 0x00,
									0x00, 0x00, 0x00, 0x00,
								},
							},
							{
								Seqid: 3,
								Other: [...]byte{
									0x02, 0x00, 0x00, 0x00,
									0x00, 0x00, 0x00, 0x00,
									0x00, 0x00, 0x00, 0x00,
								},
							},
						},
					},
				},
			},
		})
		require.NoError(t, err)
		require.Equal(t, &nfsv4_xdr.Compound4res{
			Tag: "test_stateid",
			Resarray: []nfsv4_xdr.NfsResop4{
				&nfsv4_xdr.NfsResop4_OP_SEQUENCE{
					Opsequence: &nfsv4_xdr.Sequence4res_NFS4_OK{
						SrResok4: nfsv4_xdr.Sequence4resok{
							SrSessionid: [...]byte{
								0x22, 0xa7, 0xd9, 0xef, 0x49, 0x6c, 0x95, 0x0a,
								0x9f, 0xc5, 0xab, 0xe3, 0x91, 0x57, 0x45, 0x7a,
							},
							SrSequenceid:          2,
							SrSlotid:              0,
							SrHighestSlotid:       99,
							SrTargetHighestSlotid: 99,
							SrStatusFlags:         0,
						},
					},
				},
				&nfsv4_xdr.NfsResop4_OP_TEST_STATEID{
					OptestStateid: &nfsv4_xdr.TestStateid4res_NFS4_OK{
						TsrResok4: nfsv4_xdr.TestStateid4resok{
							TsrStatusCodes: []nfsv4_xdr.Nfsstat4{
								// Open-owner.
								nfsv4_xdr.NFS4_OK,
								nfsv4_xdr.NFS4ERR_OLD_STATEID,
								nfsv4_xdr.NFS4_OK,
								nfsv4_xdr.NFS4ERR_BAD_STATEID,

								// Lock-owner.
								nfsv4_xdr.NFS4_OK,
								nfsv4_xdr.NFS4ERR_OLD_STATEID,
								nfsv4_xdr.NFS4_OK,
								nfsv4_xdr.NFS4ERR_BAD_STATEID,
							},
						},
					},
				},
			},
			Status: nfsv4_xdr.NFS4_OK,
		}, res)
	})

	// Invoke SEQUENCE at a time far in the future. This should
	// cause the client state to be cleaned up. The opened file
	// should be closed.
	clock.EXPECT().Now().Return(time.Unix(10000, 0)).Times(2)
	leaf.EXPECT().VirtualClose(virtual.ShareMaskRead)

	res, err = program.NfsV4Nfsproc4Compound(ctx, &nfsv4_xdr.Compound4args{
		Tag:          "sequence",
		Minorversion: 1,
		Argarray: []nfsv4_xdr.NfsArgop4{
			&nfsv4_xdr.NfsArgop4_OP_SEQUENCE{
				Opsequence: nfsv4_xdr.Sequence4args{
					SaSessionid: [...]byte{
						0x22, 0xa7, 0xd9, 0xef, 0x49, 0x6c, 0x95, 0x0a,
						0x9f, 0xc5, 0xab, 0xe3, 0x91, 0x57, 0x45, 0x7a,
					},
					SaSequenceid:    3,
					SaSlotid:        0,
					SaHighestSlotid: 0,
				},
			},
		},
	})
	require.NoError(t, err)
	require.Equal(t, &nfsv4_xdr.Compound4res{
		Tag: "sequence",
		Resarray: []nfsv4_xdr.NfsResop4{
			&nfsv4_xdr.NfsResop4_OP_SEQUENCE{
				Opsequence: &nfsv4_xdr.Sequence4res_default{
					SrStatus: nfsv4_xdr.NFS4ERR_BADSESSION,
				},
			},
		},
		Status: nfsv4_xdr.NFS4ERR_BADSESSION,
	}, res)
}
