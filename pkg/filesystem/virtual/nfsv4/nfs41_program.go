package nfsv4

import (
	"bytes"
	"context"
	"encoding/binary"
	"io"
	"sync"
	"time"

	"github.com/buildbarn/bb-remote-execution/pkg/filesystem/virtual"
	"github.com/buildbarn/bb-storage/pkg/clock"
	"github.com/buildbarn/bb-storage/pkg/filesystem"
	"github.com/buildbarn/bb-storage/pkg/random"
	"github.com/buildbarn/go-xdr/pkg/protocols/nfsv4"
	"github.com/buildbarn/go-xdr/pkg/protocols/rpcv2"
	"github.com/buildbarn/go-xdr/pkg/runtime"
)

// Special state IDs, as documented in RFC 8881, section 8.2.3.
var (
	anonymousStateID  = nfsv4.Stateid4{}
	readBypassStateID = nfsv4.Stateid4{
		Seqid: nfsv4.NFS4_UINT32_MAX,
		Other: [...]byte{
			0xff, 0xff, 0xff, 0xff,
			0xff, 0xff, 0xff, 0xff,
			0xff, 0xff, 0xff, 0xff,
		},
	}
	currentStateID = nfsv4.Stateid4{
		Seqid: 1,
	}
	invalidStateID = nfsv4.Stateid4{
		Seqid: nfsv4.NFS4_UINT32_MAX,
	}
)

type nfs41Program struct {
	rootFileHandle     nfs41FileHandle
	openedFilesPool    *OpenedFilesPool
	serverOwner        nfsv4.ServerOwner4
	serverScope        []byte
	maxForeChanAttrs   *nfsv4.ChannelAttrs4
	rebootVerifier     nfsv4.Verifier4
	clock              clock.Clock
	enforcedLeaseTime  time.Duration
	announcedLeaseTime nfsv4.NfsLease4

	clientsLock                  sync.Mutex
	now                          time.Time
	randomNumberGenerator        random.SingleThreadedGenerator
	clientsByOwnerID             map[string]*nfs41ClientState
	clientIncarnationsByClientID map[nfsv4.Clientid4]*clientIncarnationState
	sessionsBySessionID          map[nfsv4.Sessionid4]*sessionState
	idleClientIncarnations       clientIncarnationState
}

// NewNFS41Program creates an nfsv4.Nfs4Program that forwards all
// operations to a virtual file system. It implements most of the
// features of NFSv4.1.
func NewNFS41Program(rootDirectory virtual.Directory, openedFilesPool *OpenedFilesPool, serverOwner nfsv4.ServerOwner4, serverScope []byte, maxForeChanAttrs *nfsv4.ChannelAttrs4, randomNumberGenerator random.SingleThreadedGenerator, rebootVerifier nfsv4.Verifier4, clock clock.Clock, enforcedLeaseTime, announcedLeaseTime time.Duration) nfsv4.Nfs4Program {
	var attributes virtual.Attributes
	rootDirectory.VirtualGetAttributes(context.Background(), virtual.AttributesMaskFileHandle, &attributes)
	p := &nfs41Program{
		rootFileHandle: nfs41FileHandle{
			handle: attributes.GetFileHandle(),
			node:   virtual.DirectoryChild{}.FromDirectory(rootDirectory),
		},
		openedFilesPool:    openedFilesPool,
		serverOwner:        serverOwner,
		serverScope:        serverScope,
		maxForeChanAttrs:   maxForeChanAttrs,
		rebootVerifier:     rebootVerifier,
		clock:              clock,
		enforcedLeaseTime:  enforcedLeaseTime,
		announcedLeaseTime: nfsv4.NfsLease4(announcedLeaseTime.Seconds()),

		randomNumberGenerator:        randomNumberGenerator,
		clientsByOwnerID:             map[string]*nfs41ClientState{},
		clientIncarnationsByClientID: map[nfsv4.Clientid4]*clientIncarnationState{},
		sessionsBySessionID:          map[nfsv4.Sessionid4]*sessionState{},
	}
	p.idleClientIncarnations.previousIdle = &p.idleClientIncarnations
	p.idleClientIncarnations.nextIdle = &p.idleClientIncarnations
	return p
}

func (p *nfs41Program) enter() {
	for {
		now := p.clock.Now()
		p.clientsLock.Lock()
		if p.now.Before(now) {
			p.now = now
		}

		// Remove clients that have not renewed their state in
		// some time. Close all of the files and release all
		// locks owned by these clients.
		var ll leavesToClose
		minimumLastSeen := p.now.Add(-p.enforcedLeaseTime)
		for p.idleClientIncarnations.nextIdle != &p.idleClientIncarnations && p.idleClientIncarnations.nextIdle.lastSeen.Before(minimumLastSeen) {
			p.idleClientIncarnations.nextIdle.emptyAndRemove(p, &ll)
		}

		// If the code above ended up yielding files that need
		// to be closed, we close the files and retry.
		if ll.empty() {
			return
		}
		p.clientsLock.Unlock()
		ll.closeAll()
	}
}

func (p *nfs41Program) leave() {
	p.clientsLock.Unlock()
}

func (*nfs41Program) NfsV4Nfsproc4Null(ctx context.Context) error {
	return nil
}

func (p *nfs41Program) NfsV4Nfsproc4Compound(ctx context.Context, arguments *nfsv4.Compound4args) (*nfsv4.Compound4res, error) {
	if arguments.Minorversion != 1 {
		return &nfsv4.Compound4res{
			Status: nfsv4.NFS4ERR_MINOR_VERS_MISMATCH,
			Tag:    arguments.Tag,
		}, nil
	}

	var resArray []nfsv4.NfsResop4
	status := nfsv4.NFS4_OK
	if len(arguments.Argarray) >= 1 {
		// There are only six operations that are permitted to
		// be used as the first operation in a compound request.
		switch firstOp := arguments.Argarray[0].(type) {
		case *nfsv4.NfsArgop4_OP_BIND_CONN_TO_SESSION:
			var res nfsv4.BindConnToSession4res
			if len(arguments.Argarray) > 1 {
				res = &nfsv4.BindConnToSession4res_default{BctsrStatus: nfsv4.NFS4ERR_NOT_ONLY_OP}
			} else {
				res = p.opBindConnToSession(&firstOp.OpbindConnToSession)
			}
			resArray = []nfsv4.NfsResop4{&nfsv4.NfsResop4_OP_BIND_CONN_TO_SESSION{
				OpbindConnToSession: res,
			}}
			status = res.GetBctsrStatus()
		case *nfsv4.NfsArgop4_OP_CREATE_SESSION:
			var res nfsv4.CreateSession4res
			if len(arguments.Argarray) > 1 {
				res = &nfsv4.CreateSession4res_default{CsrStatus: nfsv4.NFS4ERR_NOT_ONLY_OP}
			} else {
				res = p.opCreateSession(&firstOp.OpcreateSession)
			}
			resArray = []nfsv4.NfsResop4{&nfsv4.NfsResop4_OP_CREATE_SESSION{
				OpcreateSession: res,
			}}
			status = res.GetCsrStatus()
		case *nfsv4.NfsArgop4_OP_DESTROY_CLIENTID:
			var res nfsv4.DestroyClientid4res
			if len(arguments.Argarray) > 1 {
				res = nfsv4.DestroyClientid4res{
					DcrStatus: nfsv4.NFS4ERR_NOT_ONLY_OP,
				}
			} else {
				res = p.opDestroyClientID(&firstOp.OpdestroyClientid)
			}
			resArray = []nfsv4.NfsResop4{&nfsv4.NfsResop4_OP_DESTROY_CLIENTID{
				OpdestroyClientid: res,
			}}
			status = res.DcrStatus
		case *nfsv4.NfsArgop4_OP_DESTROY_SESSION:
			var res nfsv4.DestroySession4res
			if len(arguments.Argarray) > 1 {
				res = nfsv4.DestroySession4res{
					DsrStatus: nfsv4.NFS4ERR_NOT_ONLY_OP,
				}
			} else {
				res = p.opDestroySession(&firstOp.OpdestroySession)
			}
			resArray = []nfsv4.NfsResop4{&nfsv4.NfsResop4_OP_DESTROY_SESSION{
				OpdestroySession: res,
			}}
			status = res.DsrStatus
		case *nfsv4.NfsArgop4_OP_EXCHANGE_ID:
			var res nfsv4.ExchangeId4res
			if len(arguments.Argarray) > 1 {
				res = &nfsv4.ExchangeId4res_default{EirStatus: nfsv4.NFS4ERR_NOT_ONLY_OP}
			} else {
				res = p.opExchangeID(&firstOp.OpexchangeId)
			}
			resArray = []nfsv4.NfsResop4{&nfsv4.NfsResop4_OP_EXCHANGE_ID{
				OpexchangeId: res,
			}}
			status = res.GetEirStatus()
		case *nfsv4.NfsArgop4_OP_SEQUENCE:
			res := p.opSequence(ctx, &firstOp.Opsequence, arguments.Argarray[1:])
			resArray = res.resArray
			status = res.status
		default:
			res := nfsv4.Illegal4res{Status: nfsv4.NFS4ERR_OP_NOT_IN_SESSION}
			resArray = []nfsv4.NfsResop4{&nfsv4.NfsResop4_OP_ILLEGAL{
				Opillegal: res,
			}}
			status = res.Status
		}
	}
	return &nfsv4.Compound4res{
		Status:   status,
		Tag:      arguments.Tag,
		Resarray: resArray,
	}, nil
}

func (p *nfs41Program) opBindConnToSession(args *nfsv4.BindConnToSession4args) nfsv4.BindConnToSession4res {
	// As this server does not implement state protection and does
	// not send any requests over the backchannel, we only provide a
	// stub implementation that validates that the session ID is
	// valid.
	var dirFromServer nfsv4.ChannelDirFromServer4
	switch args.BctsaDir {
	case nfsv4.CDFC4_FORE:
		dirFromServer = nfsv4.CDFS4_FORE
	case nfsv4.CDFC4_BACK:
		dirFromServer = nfsv4.CDFS4_BACK
	case nfsv4.CDFC4_FORE_OR_BOTH:
		dirFromServer = nfsv4.CDFS4_BOTH
	case nfsv4.CDFC4_BACK_OR_BOTH:
		dirFromServer = nfsv4.CDFS4_BOTH
	default:
		return &nfsv4.BindConnToSession4res_default{BctsrStatus: nfsv4.NFS4ERR_INVAL}
	}

	sessID := args.BctsaSessid
	p.enter()
	_, ok := p.sessionsBySessionID[sessID]
	p.leave()
	if !ok {
		return &nfsv4.BindConnToSession4res_default{BctsrStatus: nfsv4.NFS4ERR_BADSESSION}
	}

	return &nfsv4.BindConnToSession4res_NFS4_OK{
		BctsrResok4: nfsv4.BindConnToSession4resok{
			BctsrSessid: sessID,
			BctsrDir:    dirFromServer,
		},
	}
}

func (p *nfs41Program) opCreateSession(args *nfsv4.CreateSession4args) nfsv4.CreateSession4res {
	var ll leavesToClose
	defer ll.closeAll()
	p.enter()
	defer p.leave()

	cis, ok := p.clientIncarnationsByClientID[args.CsaClientid]
	if !ok {
		return &nfsv4.CreateSession4res_default{CsrStatus: nfsv4.NFS4ERR_STALE_CLIENTID}
	}

	switch args.CsaSequence {
	case cis.lastSequenceID:
		// Replay of the previous CREATE_SESSION request.
		return cis.lastCreateSessionResponse
	case cis.lastSequenceID + 1:
		// Hold and release the incarnation, so that the time at
		// which the incarnation gets garbage collected is
		// extended.
		cis.hold()
		defer cis.release(p)

		client := cis.client
		if confirmedIncarnation := client.confirmedIncarnation; confirmedIncarnation != cis {
			if confirmedIncarnation != nil {
				// The client has another confirmed incarnation.
				// Remove all state, such as open files and
				// locks.
				if confirmedIncarnation.holdCount > 0 {
					// The incarnation is currently running
					// one or more blocking operations. This
					// prevents us from closing files and
					// releasing locks.
					return &nfsv4.CreateSession4res_default{CsrStatus: nfsv4.NFS4ERR_DELAY}
				}
				confirmedIncarnation.emptyAndRemove(p, &ll)
			}
			client.confirmedIncarnation = cis
		}

		// Allocate a new session.
		slots := make([]slotState, 0, p.maxForeChanAttrs.CaMaxrequests)
		for i := nfsv4.Count4(0); i < p.maxForeChanAttrs.CaMaxrequests; i++ {
			slots = append(slots, slotState{
				lastSequenceID: 0,
				lastResult:     sequenceCompoundResultSeqMisordered,
			})
		}
		var sessionID nfsv4.Sessionid4
		p.randomNumberGenerator.Read(sessionID[:])
		session := &sessionState{
			sessionID:         sessionID,
			clientIncarnation: cis,

			previous: &cis.sessions,
			next:     cis.sessions.next,
			slots:    slots,
		}
		session.previous.next = session
		session.next.previous = session
		p.sessionsBySessionID[sessionID] = session

		res := &nfsv4.CreateSession4res_NFS4_OK{
			CsrResok4: nfsv4.CreateSession4resok{
				CsrSessionid: sessionID,
				CsrSequence:  args.CsaSequence,
				// As for this use case we don't care
				// about server restarts, announce that
				// sessions are persistent. This forces
				// the client to use GUARDED4 instead of
				// EXCLUSIVE4_1 when creating files.
				CsrFlags: args.CsaFlags & (nfsv4.CREATE_SESSION4_FLAG_CONN_BACK_CHAN | nfsv4.CREATE_SESSION4_FLAG_PERSIST),
				CsrForeChanAttrs: nfsv4.ChannelAttrs4{
					CaHeaderpadsize:         min(args.CsaForeChanAttrs.CaHeaderpadsize, p.maxForeChanAttrs.CaHeaderpadsize),
					CaMaxrequestsize:        min(args.CsaForeChanAttrs.CaMaxrequestsize, p.maxForeChanAttrs.CaMaxrequestsize),
					CaMaxresponsesize:       min(args.CsaForeChanAttrs.CaMaxresponsesize, p.maxForeChanAttrs.CaMaxresponsesize),
					CaMaxresponsesizeCached: min(args.CsaForeChanAttrs.CaMaxresponsesizeCached, p.maxForeChanAttrs.CaMaxresponsesizeCached),
					CaMaxoperations:         p.maxForeChanAttrs.CaMaxoperations,
					CaMaxrequests:           p.maxForeChanAttrs.CaMaxrequests,
				},
				// We don't use the back channel right
				// now, so simply respect the values
				// provided by the client.
				CsrBackChanAttrs: args.CsaBackChanAttrs,
			},
		}

		cis.lastSequenceID = args.CsaSequence
		cis.lastCreateSessionResponse = res
		return res
	default:
		return &nfsv4.CreateSession4res_default{CsrStatus: nfsv4.NFS4ERR_SEQ_MISORDERED}
	}
}

func (p *nfs41Program) opDestroyClientID(args *nfsv4.DestroyClientid4args) nfsv4.DestroyClientid4res {
	p.enter()
	defer p.leave()

	cis, ok := p.clientIncarnationsByClientID[args.DcaClientid]
	if !ok {
		return nfsv4.DestroyClientid4res{DcrStatus: nfsv4.NFS4ERR_STALE_CLIENTID}
	}
	if cis.holdCount != 0 || len(cis.openOwnersByOwner) != 0 || cis.sessions.next != &cis.sessions {
		return nfsv4.DestroyClientid4res{DcrStatus: nfsv4.NFS4ERR_CLIENTID_BUSY}
	}
	cis.remove(p)
	return nfsv4.DestroyClientid4res{DcrStatus: nfsv4.NFS4_OK}
}

func (p *nfs41Program) opDestroySession(args *nfsv4.DestroySession4args) nfsv4.DestroySession4res {
	p.enter()
	defer p.leave()

	session, ok := p.sessionsBySessionID[args.DsaSessionid]
	if !ok {
		return nfsv4.DestroySession4res{DsrStatus: nfsv4.NFS4ERR_BADSESSION}
	}
	session.remove(p)
	return nfsv4.DestroySession4res{DsrStatus: nfsv4.NFS4_OK}
}

// writeAttributes converts file attributes returned by the virtual file
// system into the NFSv4 wire format. It also returns a bitmask
// indicating which attributes were actually emitted.
func (p *nfs41Program) writeAttributes(attributes *virtual.Attributes, attrRequest nfsv4.Bitmap4, w io.Writer) nfsv4.Bitmap4 {
	attrMask := make(nfsv4.Bitmap4, len(attrRequest))
	if len(attrRequest) > 0 {
		// Attributes 0 to 31.
		f := attrRequest[0]
		var s uint32
		if b := uint32(1 << nfsv4.FATTR4_SUPPORTED_ATTRS); f&b != 0 {
			s |= b
			nfsv4.WriteBitmap4(w, nfsv4.Bitmap4{
				(1 << nfsv4.FATTR4_SUPPORTED_ATTRS) |
					(1 << nfsv4.FATTR4_TYPE) |
					(1 << nfsv4.FATTR4_FH_EXPIRE_TYPE) |
					(1 << nfsv4.FATTR4_CHANGE) |
					(1 << nfsv4.FATTR4_SIZE) |
					(1 << nfsv4.FATTR4_LINK_SUPPORT) |
					(1 << nfsv4.FATTR4_SYMLINK_SUPPORT) |
					(1 << nfsv4.FATTR4_NAMED_ATTR) |
					(1 << nfsv4.FATTR4_FSID) |
					(1 << nfsv4.FATTR4_UNIQUE_HANDLES) |
					(1 << nfsv4.FATTR4_LEASE_TIME) |
					(1 << nfsv4.FATTR4_RDATTR_ERROR) |
					(1 << nfsv4.FATTR4_FILEHANDLE) |
					(1 << nfsv4.FATTR4_FILEID),
				(1 << (nfsv4.FATTR4_MODE - 32)) |
					(1 << (nfsv4.FATTR4_NUMLINKS - 32)) |
					(1 << (nfsv4.FATTR4_TIME_ACCESS - 32)) |
					(1 << (nfsv4.FATTR4_TIME_METADATA - 32)) |
					(1 << (nfsv4.FATTR4_TIME_MODIFY - 32)),
				1 << (nfsv4.FATTR4_SUPPATTR_EXCLCREAT - 64),
			})
		}
		if b := uint32(1 << nfsv4.FATTR4_TYPE); f&b != 0 {
			s |= b
			switch attributes.GetFileType() {
			case filesystem.FileTypeRegularFile:
				nfsv4.NF4REG.WriteTo(w)
			case filesystem.FileTypeDirectory:
				nfsv4.NF4DIR.WriteTo(w)
			case filesystem.FileTypeSymlink:
				nfsv4.NF4LNK.WriteTo(w)
			case filesystem.FileTypeBlockDevice:
				nfsv4.NF4BLK.WriteTo(w)
			case filesystem.FileTypeCharacterDevice:
				nfsv4.NF4CHR.WriteTo(w)
			case filesystem.FileTypeFIFO:
				nfsv4.NF4FIFO.WriteTo(w)
			case filesystem.FileTypeSocket:
				nfsv4.NF4SOCK.WriteTo(w)
			default:
				panic("Unknown file type")
			}
		}
		if b := uint32(1 << nfsv4.FATTR4_FH_EXPIRE_TYPE); f&b != 0 {
			s |= b
			// Using HandleResolver, we can resolve any
			// object in the file system until it is removed
			// from the file system.
			nfsv4.WriteUint32T(w, nfsv4.FH4_PERSISTENT)
		}
		if b := uint32(1 << nfsv4.FATTR4_CHANGE); f&b != 0 {
			s |= b
			nfsv4.WriteChangeid4(w, attributes.GetChangeID())
		}
		if b := uint32(1 << nfsv4.FATTR4_SIZE); f&b != 0 {
			sizeBytes, ok := attributes.GetSizeBytes()
			if !ok {
				panic("FATTR4_SIZE is a required attribute")
			}
			s |= b
			nfsv4.WriteUint64T(w, sizeBytes)
		}
		if b := uint32(1 << nfsv4.FATTR4_LINK_SUPPORT); f&b != 0 {
			s |= b
			runtime.WriteBool(w, true)
		}
		if b := uint32(1 << nfsv4.FATTR4_SYMLINK_SUPPORT); f&b != 0 {
			s |= b
			runtime.WriteBool(w, true)
		}
		if b := uint32(1 << nfsv4.FATTR4_NAMED_ATTR); f&b != 0 {
			s |= b
			runtime.WriteBool(w, false)
		}
		if b := uint32(1 << nfsv4.FATTR4_FSID); f&b != 0 {
			s |= b
			fsid := nfsv4.Fsid4{
				Major: 1,
				Minor: 1,
			}
			fsid.WriteTo(w)
		}
		if b := uint32(1 << nfsv4.FATTR4_UNIQUE_HANDLES); f&b != 0 {
			s |= b
			runtime.WriteBool(w, true)
		}
		if b := uint32(1 << nfsv4.FATTR4_LEASE_TIME); f&b != 0 {
			s |= b
			nfsv4.WriteNfsLease4(w, p.announcedLeaseTime)
		}
		if b := uint32(1 << nfsv4.FATTR4_FILEHANDLE); f&b != 0 {
			s |= b
			nfsv4.WriteNfsFh4(w, attributes.GetFileHandle())
		}
		if b := uint32(1 << nfsv4.FATTR4_FILEID); f&b != 0 {
			s |= b
			nfsv4.WriteUint64T(w, attributes.GetInodeNumber())
		}
		attrMask[0] = s
	}
	if len(attrRequest) > 1 {
		// Attributes 32 to 63.
		f := attrRequest[1]
		var s uint32
		if b := uint32(1 << (nfsv4.FATTR4_MODE - 32)); f&b != 0 {
			if permissions, ok := attributes.GetPermissions(); ok {
				s |= b
				nfsv4.WriteMode4(w, permissions.ToMode())
			}
		}
		if b := uint32(1 << (nfsv4.FATTR4_NUMLINKS - 32)); f&b != 0 {
			s |= b
			nfsv4.WriteUint32T(w, attributes.GetLinkCount())
		}
		if b := uint32(1 << (nfsv4.FATTR4_TIME_ACCESS - 32)); f&b != 0 {
			s |= b
			deterministicNfstime4.WriteTo(w)
		}
		if b := uint32(1 << (nfsv4.FATTR4_TIME_METADATA - 32)); f&b != 0 {
			s |= b
			deterministicNfstime4.WriteTo(w)
		}
		if b := uint32(1 << (nfsv4.FATTR4_TIME_MODIFY - 32)); f&b != 0 {
			s |= b
			t := deterministicNfstime4
			if lastDataModificationTime, ok := attributes.GetLastDataModificationTime(); ok {
				t = timeToNfstime4(lastDataModificationTime)
			}
			t.WriteTo(w)
		}
		attrMask[1] = s
	}
	if len(attrRequest) > 2 {
		// Attributes 64 to 95.
		f := attrRequest[2]
		var s uint32
		if b := uint32(1 << (nfsv4.FATTR4_SUPPATTR_EXCLCREAT - 64)); f&b != 0 {
			s |= b
			// Considering that this server always announces
			// sessions to be persistent, clients should
			// never use EXCLUSIVE4_1. We therefore don't
			// need to announce any supported attributes.
			nfsv4.WriteBitmap4(w, nil)
		}
		attrMask[2] = s
	}
	return attrMask
}

// attributesToFattr4 converts attributes returned by the virtual file
// system layer to an NFSv4 fattr4 structure. As required by the
// protocol, attributes are stored in the order of the FATTR4_*
// constants.
func (p *nfs41Program) attributesToFattr4(attributes *virtual.Attributes, attrRequest nfsv4.Bitmap4) nfsv4.Fattr4 {
	w := bytes.NewBuffer(nil)
	attrMask := p.writeAttributes(attributes, attrRequest, w)
	return nfsv4.Fattr4{
		Attrmask: attrMask,
		AttrVals: w.Bytes(),
	}
}

var serverImplementationID = []nfsv4.NfsImplId4{{
	NiiDomain: "buildbarn.github.io",
}}

// For every client incarnation, we need to store a cached response to
// the last call to CREATE_SESSION. This is the cached response that is
// returned if the client hasn't called CREATE_SESSION yet. This
// prevents the need for having a special case in CREATE_SESSION.
//
// More details: RFC 8881, section 18.36.4, paragraph 4.
var initialLastCreateSessionResponse = nfsv4.CreateSession4res_default{CsrStatus: nfsv4.NFS4ERR_SEQ_MISORDERED}

func (p *nfs41Program) opExchangeID(args *nfsv4.ExchangeId4args) nfsv4.ExchangeId4res {
	p.enter()
	defer p.leave()

	clientOwnerID := string(args.EiaClientowner.CoOwnerid)
	client, ok := p.clientsByOwnerID[clientOwnerID]
	if ok {
	} else {
		client = &nfs41ClientState{
			ownerID:                      clientOwnerID,
			incarnationsByClientVerifier: map[nfsv4.Verifier4]*clientIncarnationState{},
		}
		p.clientsByOwnerID[clientOwnerID] = client
	}

	clientVerifier := args.EiaClientowner.CoVerifier
	cis, ok := client.incarnationsByClientVerifier[clientVerifier]
	if !ok {
		cis = &clientIncarnationState{
			client:         client,
			clientID:       p.randomNumberGenerator.Uint64(),
			clientVerifier: clientVerifier,

			lastSequenceID:            p.randomNumberGenerator.Uint32(),
			lastCreateSessionResponse: &initialLastCreateSessionResponse,

			openOwnersByOwner:     map[string]*nfs41OpenOwnerState{},
			openOwnerFilesByOther: map[uint64]*nfs41OpenOwnerFileState{},
			lockOwnersByOwner:     map[string]*nfs41LockOwnerState{},
			lockOwnerFilesByOther: map[uint64]*nfs41LockOwnerFileState{},
		}
		cis.sessions.previous = &cis.sessions
		cis.sessions.next = &cis.sessions
		client.incarnationsByClientVerifier[clientVerifier] = cis
		p.clientIncarnationsByClientID[cis.clientID] = cis
		cis.insertIntoIdleList(p)
	}

	res := &nfsv4.ExchangeId4res_NFS4_OK{
		EirResok4: nfsv4.ExchangeId4resok{
			EirClientid:     cis.clientID,
			EirFlags:        nfsv4.EXCHGID4_FLAG_USE_NON_PNFS,
			EirStateProtect: &nfsv4.StateProtect4R_SP4_NONE{},
			EirServerOwner:  p.serverOwner,
			EirServerScope:  p.serverScope,
			EirServerImplId: serverImplementationID,
		},
	}
	if client.confirmedIncarnation == cis {
		res.EirResok4.EirFlags |= nfsv4.EXCHGID4_FLAG_CONFIRMED_R
	} else {
		res.EirResok4.EirSequenceid = cis.lastSequenceID + 1
	}
	return res
}

// getNfsResop4WithError returns an nfs_resop4 that uses the same
// operation type, but has the status set to the provided value.
//
// Because each operation type has its own error message, we must resort
// to marshaling/unmarshaling the message to get the right type.
func getNfsResop4WithError(original nfsv4.NfsResop4, status nfsv4.Nfsstat4) nfsv4.NfsResop4 {
	// Manually construct a marshaled nfs_resop4.
	b := bytes.NewBuffer(make([]byte, 0, nfsv4.NfsOpnum4EncodedSizeBytes+nfsv4.Nfsstat4EncodedSizeBytes))
	if _, err := original.GetResop().WriteTo(b); err != nil {
		panic(err)
	}
	if _, err := status.WriteTo(b); err != nil {
		panic(err)
	}

	// Unmarshal it again.
	withError, _, err := nfsv4.ReadNfsResop4(b)
	if err != nil {
		panic(err)
	}
	return withError
}

func (p *nfs41Program) opSequence(ctx context.Context, args *nfsv4.Sequence4args, argArray []nfsv4.NfsArgop4) compoundResult {
	// Look up the session and slot referenced by the request.
	p.enter()
	session, ok := p.sessionsBySessionID[args.SaSessionid]
	if !ok {
		p.leave()
		return sequenceCompoundResultBadSession
	}

	slotCount := nfsv4.Slotid4(len(session.slots))
	if args.SaSlotid >= slotCount {
		p.leave()
		return sequenceCompoundResultBadSlot
	}
	slot := &session.slots[args.SaSlotid]

	switch args.SaSequenceid {
	case slot.lastSequenceID:
		// Replay of the previous SEQUENCE. Return the results
		// of the previous call, only after making sure that the
		// cached response has the same shape as the request.
		defer p.leave()
		cachedResults := slot.lastResult.resArray[1:]
		if len(cachedResults) > len(argArray) || (slot.lastResult.status == nfsv4.NFS4_OK && len(cachedResults) != len(argArray)) {
			return sequenceCompoundResultSeqFalseRetry
		}
		for i, res := range cachedResults {
			if opNum := res.GetResop(); opNum != argArray[i].GetArgop() && opNum != nfsv4.OP_ILLEGAL {
				return sequenceCompoundResultSeqFalseRetry
			}
		}
		return slot.lastResult
	case slot.lastSequenceID + 1:
		// Start of a new sequence. This could be a duplicate
		// call, so deduplicate it if needed.
		if slot.currentSequenceWaiters != nil {
			ch := make(chan compoundResult, 1)
			slot.currentSequenceWaiters = append(slot.currentSequenceWaiters)
			p.leave()
			return <-ch
		}

		// Throw away the previously cached results, as we know
		// the client no longer has a need requesting them.
		slot.lastResult = sequenceCompoundResultSeqMisordered

		if 1+len(argArray) > int(p.maxForeChanAttrs.CaMaxoperations) {
			p.leave()
			return sequenceCompoundResultTooManyOps
		}

		// Mark the slot as busy, so that we can process
		// operations in the compound request without holding
		// locks.
		slot.currentSequenceWaiters = make([]chan<- compoundResult, 0)

		session.clientIncarnation.hold()
		p.leave()

		result := compoundResult{
			resArray: append(
				make([]nfsv4.NfsResop4, 0, 1+len(argArray)),
				&nfsv4.NfsResop4_OP_SEQUENCE{
					Opsequence: &nfsv4.Sequence4res_NFS4_OK{
						SrResok4: nfsv4.Sequence4resok{
							SrSessionid:           args.SaSessionid,
							SrSequenceid:          args.SaSequenceid,
							SrSlotid:              args.SaSlotid,
							SrHighestSlotid:       slotCount - 1,
							SrTargetHighestSlotid: slotCount - 1,
							SrStatusFlags:         0,
						},
					},
				},
			),
			status: nfsv4.NFS4_OK,
		}

		state := sequenceState{
			program: p,
			session: session,
		}
		for _, operation := range argArray {
			switch op := operation.(type) {
			case *nfsv4.NfsArgop4_OP_ACCESS:
				res := state.opAccess(ctx, &op.Opaccess)
				result.resArray = append(result.resArray, &nfsv4.NfsResop4_OP_ACCESS{
					Opaccess: res,
				})
				result.status = res.GetStatus()
			case *nfsv4.NfsArgop4_OP_BACKCHANNEL_CTL:
				res := state.opBackchannelCtl(&op.OpbackchannelCtl)
				result.resArray = append(result.resArray, &nfsv4.NfsResop4_OP_BACKCHANNEL_CTL{
					OpbackchannelCtl: res,
				})
				result.status = res.BcrStatus
			case *nfsv4.NfsArgop4_OP_BIND_CONN_TO_SESSION:
				res := state.opBindConnToSession(&op.OpbindConnToSession)
				result.resArray = append(result.resArray, &nfsv4.NfsResop4_OP_BIND_CONN_TO_SESSION{
					OpbindConnToSession: res,
				})
				result.status = res.GetBctsrStatus()
			case *nfsv4.NfsArgop4_OP_CLOSE:
				res := state.opClose(&op.Opclose)
				result.resArray = append(result.resArray, &nfsv4.NfsResop4_OP_CLOSE{
					Opclose: res,
				})
				result.status = res.GetStatus()
			case *nfsv4.NfsArgop4_OP_COMMIT:
				res := state.opCommit(&op.Opcommit)
				result.resArray = append(result.resArray, &nfsv4.NfsResop4_OP_COMMIT{
					Opcommit: res,
				})
				result.status = res.GetStatus()
			case *nfsv4.NfsArgop4_OP_CREATE:
				res := state.opCreate(ctx, &op.Opcreate)
				result.resArray = append(result.resArray, &nfsv4.NfsResop4_OP_CREATE{
					Opcreate: res,
				})
				result.status = res.GetStatus()
			case *nfsv4.NfsArgop4_OP_CREATE_SESSION:
				res := p.opCreateSession(&op.OpcreateSession)
				result.resArray = append(result.resArray, &nfsv4.NfsResop4_OP_CREATE_SESSION{
					OpcreateSession: res,
				})
				result.status = res.GetCsrStatus()
			case *nfsv4.NfsArgop4_OP_DELEGPURGE:
				res := state.opDelegPurge(&op.Opdelegpurge)
				result.resArray = append(result.resArray, &nfsv4.NfsResop4_OP_DELEGPURGE{
					Opdelegpurge: res,
				})
				result.status = res.Status
			case *nfsv4.NfsArgop4_OP_DELEGRETURN:
				res := state.opDelegReturn(&op.Opdelegreturn)
				result.resArray = append(result.resArray, &nfsv4.NfsResop4_OP_DELEGRETURN{
					Opdelegreturn: res,
				})
				result.status = res.Status
			case *nfsv4.NfsArgop4_OP_DESTROY_CLIENTID:
				res := p.opDestroyClientID(&op.OpdestroyClientid)
				result.resArray = append(result.resArray, &nfsv4.NfsResop4_OP_DESTROY_CLIENTID{
					OpdestroyClientid: res,
				})
				result.status = res.DcrStatus
			case *nfsv4.NfsArgop4_OP_DESTROY_SESSION:
				res := p.opDestroySession(&op.OpdestroySession)
				result.resArray = append(result.resArray, &nfsv4.NfsResop4_OP_DESTROY_SESSION{
					OpdestroySession: res,
				})
				result.status = res.DsrStatus
			case *nfsv4.NfsArgop4_OP_EXCHANGE_ID:
				res := p.opExchangeID(&op.OpexchangeId)
				result.resArray = append(result.resArray, &nfsv4.NfsResop4_OP_EXCHANGE_ID{
					OpexchangeId: res,
				})
				result.status = res.GetEirStatus()
			case *nfsv4.NfsArgop4_OP_FREE_STATEID:
				res := state.opFreeStateID(&op.OpfreeStateid)
				result.resArray = append(result.resArray, &nfsv4.NfsResop4_OP_FREE_STATEID{
					OpfreeStateid: res,
				})
				result.status = res.FsrStatus
			case *nfsv4.NfsArgop4_OP_GETATTR:
				res := state.opGetAttr(ctx, &op.Opgetattr)
				result.resArray = append(result.resArray, &nfsv4.NfsResop4_OP_GETATTR{
					Opgetattr: res,
				})
				result.status = res.GetStatus()
			case *nfsv4.NfsArgop4_OP_GETDEVICEINFO:
				res := state.opGetDeviceInfo(&op.Opgetdeviceinfo)
				result.resArray = append(result.resArray, &nfsv4.NfsResop4_OP_GETDEVICEINFO{
					Opgetdeviceinfo: res,
				})
				result.status = res.GetGdirStatus()
			case *nfsv4.NfsArgop4_OP_GETDEVICELIST:
				res := state.opGetDeviceList(&op.Opgetdevicelist)
				result.resArray = append(result.resArray, &nfsv4.NfsResop4_OP_GETDEVICELIST{
					Opgetdevicelist: res,
				})
				result.status = res.GetGdlrStatus()
			case *nfsv4.NfsArgop4_OP_GETFH:
				res := state.opGetFH()
				result.resArray = append(result.resArray, &nfsv4.NfsResop4_OP_GETFH{
					Opgetfh: res,
				})
				result.status = res.GetStatus()
			case *nfsv4.NfsArgop4_OP_GET_DIR_DELEGATION:
				res := state.opGetDirDelegation(&op.OpgetDirDelegation)
				result.resArray = append(result.resArray, &nfsv4.NfsResop4_OP_GET_DIR_DELEGATION{
					OpgetDirDelegation: res,
				})
				result.status = res.GetGddrStatus()
			case *nfsv4.NfsArgop4_OP_LAYOUTCOMMIT:
				res := state.opLayoutCommit(&op.Oplayoutcommit)
				result.resArray = append(result.resArray, &nfsv4.NfsResop4_OP_LAYOUTCOMMIT{
					Oplayoutcommit: res,
				})
				result.status = res.GetLocrStatus()
			case *nfsv4.NfsArgop4_OP_LAYOUTGET:
				res := state.opLayoutGet(&op.Oplayoutget)
				result.resArray = append(result.resArray, &nfsv4.NfsResop4_OP_LAYOUTGET{
					Oplayoutget: res,
				})
				result.status = res.GetLogrStatus()
			case *nfsv4.NfsArgop4_OP_LAYOUTRETURN:
				res := state.opLayoutReturn(&op.Oplayoutreturn)
				result.resArray = append(result.resArray, &nfsv4.NfsResop4_OP_LAYOUTRETURN{
					Oplayoutreturn: res,
				})
				result.status = res.GetLorrStatus()
			case *nfsv4.NfsArgop4_OP_LINK:
				res := state.opLink(ctx, &op.Oplink)
				result.resArray = append(result.resArray, &nfsv4.NfsResop4_OP_LINK{
					Oplink: res,
				})
				result.status = res.GetStatus()
			case *nfsv4.NfsArgop4_OP_LOCK:
				res := state.opLock(&op.Oplock)
				result.resArray = append(result.resArray, &nfsv4.NfsResop4_OP_LOCK{
					Oplock: res,
				})
				result.status = res.GetStatus()
			case *nfsv4.NfsArgop4_OP_LOCKT:
				res := state.opLockT(&op.Oplockt)
				result.resArray = append(result.resArray, &nfsv4.NfsResop4_OP_LOCKT{
					Oplockt: res,
				})
				result.status = res.GetStatus()
			case *nfsv4.NfsArgop4_OP_LOCKU:
				res := state.opLockU(&op.Oplocku)
				result.resArray = append(result.resArray, &nfsv4.NfsResop4_OP_LOCKU{
					Oplocku: res,
				})
				result.status = res.GetStatus()
			case *nfsv4.NfsArgop4_OP_LOOKUP:
				res := state.opLookup(ctx, &op.Oplookup)
				result.resArray = append(result.resArray, &nfsv4.NfsResop4_OP_LOOKUP{
					Oplookup: res,
				})
				result.status = res.Status
			case *nfsv4.NfsArgop4_OP_LOOKUPP:
				res := state.opLookupP(ctx)
				result.resArray = append(result.resArray, &nfsv4.NfsResop4_OP_LOOKUPP{
					Oplookupp: res,
				})
				result.status = res.Status
			case *nfsv4.NfsArgop4_OP_NVERIFY:
				res := state.opNVerify(ctx, &op.Opnverify)
				result.resArray = append(result.resArray, &nfsv4.NfsResop4_OP_NVERIFY{
					Opnverify: res,
				})
				result.status = res.Status
			case *nfsv4.NfsArgop4_OP_OPEN:
				res := state.opOpen(ctx, &op.Opopen)
				result.resArray = append(result.resArray, &nfsv4.NfsResop4_OP_OPEN{
					Opopen: res,
				})
				result.status = res.GetStatus()
			case *nfsv4.NfsArgop4_OP_OPENATTR:
				res := state.opOpenAttr(&op.Opopenattr)
				result.resArray = append(result.resArray, &nfsv4.NfsResop4_OP_OPENATTR{
					Opopenattr: res,
				})
				result.status = res.Status
			case *nfsv4.NfsArgop4_OP_OPEN_DOWNGRADE:
				res := state.opOpenDowngrade(&op.OpopenDowngrade)
				result.resArray = append(result.resArray, &nfsv4.NfsResop4_OP_OPEN_DOWNGRADE{
					OpopenDowngrade: res,
				})
				result.status = res.GetStatus()
			case *nfsv4.NfsArgop4_OP_PUTFH:
				res := state.opPutFH(&op.Opputfh)
				result.resArray = append(result.resArray, &nfsv4.NfsResop4_OP_PUTFH{
					Opputfh: res,
				})
				result.status = res.Status
			case *nfsv4.NfsArgop4_OP_PUTPUBFH:
				res := state.opPutPubFH()
				result.resArray = append(result.resArray, &nfsv4.NfsResop4_OP_PUTPUBFH{
					Opputpubfh: res,
				})
				result.status = res.Status
			case *nfsv4.NfsArgop4_OP_PUTROOTFH:
				res := state.opPutRootFH()
				result.resArray = append(result.resArray, &nfsv4.NfsResop4_OP_PUTROOTFH{
					Opputrootfh: res,
				})
				result.status = res.Status
			case *nfsv4.NfsArgop4_OP_READ:
				res := state.opRead(ctx, &op.Opread)
				result.resArray = append(result.resArray, &nfsv4.NfsResop4_OP_READ{
					Opread: res,
				})
				result.status = res.GetStatus()
			case *nfsv4.NfsArgop4_OP_READDIR:
				res := state.opReadDir(ctx, &op.Opreaddir)
				result.resArray = append(result.resArray, &nfsv4.NfsResop4_OP_READDIR{
					Opreaddir: res,
				})
				result.status = res.GetStatus()
			case *nfsv4.NfsArgop4_OP_READLINK:
				res := state.opReadLink(ctx)
				result.resArray = append(result.resArray, &nfsv4.NfsResop4_OP_READLINK{
					Opreadlink: res,
				})
				result.status = res.GetStatus()
			case *nfsv4.NfsArgop4_OP_RECLAIM_COMPLETE:
				res := state.opReclaimComplete(&op.OpreclaimComplete)
				result.resArray = append(result.resArray, &nfsv4.NfsResop4_OP_RECLAIM_COMPLETE{
					OpreclaimComplete: res,
				})
				result.status = res.RcrStatus
			case *nfsv4.NfsArgop4_OP_REMOVE:
				res := state.opRemove(&op.Opremove)
				result.resArray = append(result.resArray, &nfsv4.NfsResop4_OP_REMOVE{
					Opremove: res,
				})
				result.status = res.GetStatus()
			case *nfsv4.NfsArgop4_OP_RENAME:
				res := state.opRename(&op.Oprename)
				result.resArray = append(result.resArray, &nfsv4.NfsResop4_OP_RENAME{
					Oprename: res,
				})
				result.status = res.GetStatus()
			case *nfsv4.NfsArgop4_OP_RESTOREFH:
				res := state.opRestoreFH()
				result.resArray = append(result.resArray, &nfsv4.NfsResop4_OP_RESTOREFH{
					Oprestorefh: res,
				})
				result.status = res.Status
			case *nfsv4.NfsArgop4_OP_SAVEFH:
				res := state.opSaveFH()
				result.resArray = append(result.resArray, &nfsv4.NfsResop4_OP_SAVEFH{
					Opsavefh: res,
				})
				result.status = res.Status
			case *nfsv4.NfsArgop4_OP_SECINFO:
				res := state.opSecInfo(ctx, &op.Opsecinfo)
				result.resArray = append(result.resArray, &nfsv4.NfsResop4_OP_SECINFO{
					Opsecinfo: res,
				})
				result.status = res.GetStatus()
			case *nfsv4.NfsArgop4_OP_SECINFO_NO_NAME:
				res := state.opSecInfoNoName(&op.OpsecinfoNoName)
				result.resArray = append(result.resArray, &nfsv4.NfsResop4_OP_SECINFO_NO_NAME{
					OpsecinfoNoName: res,
				})
				result.status = res.GetStatus()
			case *nfsv4.NfsArgop4_OP_SEQUENCE:
				res := state.opSequence(&op.Opsequence)
				result.resArray = append(result.resArray, &nfsv4.NfsResop4_OP_SEQUENCE{
					Opsequence: res,
				})
				result.status = res.GetSrStatus()
			case *nfsv4.NfsArgop4_OP_SETATTR:
				res := state.opSetAttr(ctx, &op.Opsetattr)
				result.resArray = append(result.resArray, &nfsv4.NfsResop4_OP_SETATTR{
					Opsetattr: res,
				})
				result.status = res.Status
			case *nfsv4.NfsArgop4_OP_SET_SSV:
				res := state.opSetSSV(&op.OpsetSsv)
				result.resArray = append(result.resArray, &nfsv4.NfsResop4_OP_SET_SSV{
					OpsetSsv: res,
				})
				result.status = res.GetSsrStatus()
			case *nfsv4.NfsArgop4_OP_TEST_STATEID:
				res := state.opTestStateID(&op.OptestStateid)
				result.resArray = append(result.resArray, &nfsv4.NfsResop4_OP_TEST_STATEID{
					OptestStateid: res,
				})
				result.status = res.GetTsrStatus()
			case *nfsv4.NfsArgop4_OP_VERIFY:
				res := state.opVerify(ctx, &op.Opverify)
				result.resArray = append(result.resArray, &nfsv4.NfsResop4_OP_VERIFY{
					Opverify: res,
				})
				result.status = res.Status
			case *nfsv4.NfsArgop4_OP_WANT_DELEGATION:
				res := state.opWantDelegation(&op.OpwantDelegation)
				result.resArray = append(result.resArray, &nfsv4.NfsResop4_OP_WANT_DELEGATION{
					OpwantDelegation: res,
				})
				result.status = res.GetWdrStatus()
			case *nfsv4.NfsArgop4_OP_WRITE:
				res := state.opWrite(ctx, &op.Opwrite)
				result.resArray = append(result.resArray, &nfsv4.NfsResop4_OP_WRITE{
					Opwrite: res,
				})
				result.status = res.GetStatus()
			default:
				res := nfsv4.Illegal4res{Status: nfsv4.NFS4ERR_OP_ILLEGAL}
				result.resArray = append(result.resArray, &nfsv4.NfsResop4_OP_ILLEGAL{
					Opillegal: res,
				})
				result.status = res.Status
			}
			if result.status != nfsv4.NFS4_OK {
				// Terminate evaluation of further operations
				// upon failure.
				break
			}
		}

		// Optional reply caching. If either the client asked us
		// to cache the reply, or the reply is sufficiently
		// small, we should always cache it. If not, we should
		// only preserve the first SEQUENCE response, and let
		// the second response be NFS4ERR_RETRY_UNCACHED_REP.
		//
		// More details: RFC 8881, section 2.10.6.1.3, paragraph 2.
		var cachedResult compoundResult
		if args.SaCachethis || len(result.resArray) < 2 || (len(result.resArray) == 2 && result.status != nfsv4.NFS4_OK) {
			cachedResult = result
		} else {
			cachedResult = compoundResult{
				resArray: []nfsv4.NfsResop4{
					result.resArray[0],
					getNfsResop4WithError(result.resArray[1], nfsv4.NFS4ERR_RETRY_UNCACHED_REP),
				},
				status: nfsv4.NFS4ERR_RETRY_UNCACHED_REP,
			}
		}

		p.enter()
		session.clientIncarnation.release(p)

		sequenceWaiters := slot.currentSequenceWaiters
		slot.lastSequenceID = args.SaSequenceid
		slot.lastResult = cachedResult
		slot.currentSequenceWaiters = nil
		p.leave()

		// Broadcast result to parallel calls with the same sequence ID.
		for _, waiter := range sequenceWaiters {
			waiter <- result
		}
		return result
	default:
		p.leave()
		return sequenceCompoundResultSeqMisordered
	}
}

// nfs41FileHandle contains information on the current or saved file
// handle that is tracked in a SEQUENCE request.
type nfs41FileHandle struct {
	handle  nfsv4.NfsFh4
	node    virtual.DirectoryChild
	stateID nfs41RegularStateID
}

func (fh *nfs41FileHandle) getNode() (virtual.Node, bool, nfsv4.Nfsstat4) {
	if directory, leaf := fh.node.GetPair(); directory != nil {
		return directory, true, nfsv4.NFS4_OK
	} else if leaf != nil {
		return leaf, false, nfsv4.NFS4_OK
	}
	return nil, false, nfsv4.NFS4ERR_NOFILEHANDLE
}

func (fh *nfs41FileHandle) getDirectory() (virtual.Directory, nfsv4.Nfsstat4) {
	if directory, leaf := fh.node.GetPair(); directory != nil {
		return directory, nfsv4.NFS4_OK
	} else if leaf != nil {
		return nil, nfsv4.NFS4ERR_NOTDIR
	}
	return nil, nfsv4.NFS4ERR_NOFILEHANDLE
}

func (fh *nfs41FileHandle) getDirectoryOrSymlink(ctx context.Context) (virtual.Directory, nfsv4.Nfsstat4) {
	if directory, leaf := fh.node.GetPair(); directory != nil {
		return directory, nfsv4.NFS4_OK
	} else if leaf != nil {
		// This call requires that we return NFS4ERR_SYMLINK if
		// we stumble upon a symlink. That way the client knows
		// that symlink expansion needs to be performed.
		var attributes virtual.Attributes
		leaf.VirtualGetAttributes(ctx, virtual.AttributesMaskFileType, &attributes)
		if attributes.GetFileType() == filesystem.FileTypeSymlink {
			return nil, nfsv4.NFS4ERR_SYMLINK
		}
		return nil, nfsv4.NFS4ERR_NOTDIR
	}
	return nil, nfsv4.NFS4ERR_NOFILEHANDLE
}

func (fh *nfs41FileHandle) getLeaf() (virtual.Leaf, nfsv4.Nfsstat4) {
	if directory, leaf := fh.node.GetPair(); directory != nil {
		return nil, nfsv4.NFS4ERR_ISDIR
	} else if leaf != nil {
		return leaf, nfsv4.NFS4_OK
	}
	return nil, nfsv4.NFS4ERR_NOFILEHANDLE
}

// nfs41RegularStateID is an internal representation of non-special
// open-owner or lock-owner state IDs.
type nfs41RegularStateID struct {
	seqID uint32
	other uint64
}

func newNFS41RegularStateID(stateID *nfsv4.Stateid4) (nfs41RegularStateID, nfsv4.Nfsstat4) {
	// The "other" field in state IDs are 12 bytes in size,
	// but we only use the first eight bytes. Require that
	// the final 4 bytes are set to zero.
	if *(*[4]byte)(stateID.Other[8:]) != [4]byte{} {
		return nfs41RegularStateID{}, nfsv4.NFS4ERR_BAD_STATEID
	}
	return nfs41RegularStateID{
		seqID: stateID.Seqid,
		other: binary.LittleEndian.Uint64(stateID.Other[:]),
	}, nfsv4.NFS4_OK
}

// externalize converts the regular state ID to the NFSv4 wire format.
func (sid *nfs41RegularStateID) externalize() (stateID nfsv4.Stateid4) {
	stateID.Seqid = sid.seqID
	binary.LittleEndian.PutUint64(stateID.Other[:], sid.other)
	return
}

// incrementSeqID increments a sequence ID of a state ID according to
// the rules described in RFC 8881, section 8.2.2, paragraph 2.
func (sid *nfs41RegularStateID) incrementSeqID() {
	if sid.seqID == nfsv4.NFS4_UINT32_MAX {
		sid.seqID = 1
	} else {
		sid.seqID++
	}
}

type compoundResult struct {
	resArray []nfsv4.NfsResop4
	status   nfsv4.Nfsstat4
}

func newSequenceCompoundResultForError(status nfsv4.Nfsstat4) compoundResult {
	return compoundResult{
		resArray: []nfsv4.NfsResop4{
			&nfsv4.NfsResop4_OP_SEQUENCE{
				Opsequence: &nfsv4.Sequence4res_default{
					SrStatus: status,
				},
			},
		},
		status: status,
	}
}

// Predeclared responses for calls to SEQUENC that fail.
var (
	sequenceCompoundResultBadSession    = newSequenceCompoundResultForError(nfsv4.NFS4ERR_BADSESSION)
	sequenceCompoundResultBadSlot       = newSequenceCompoundResultForError(nfsv4.NFS4ERR_BADSLOT)
	sequenceCompoundResultSeqFalseRetry = newSequenceCompoundResultForError(nfsv4.NFS4ERR_SEQ_FALSE_RETRY)
	sequenceCompoundResultSeqMisordered = newSequenceCompoundResultForError(nfsv4.NFS4ERR_SEQ_MISORDERED)
	sequenceCompoundResultTooManyOps    = newSequenceCompoundResultForError(nfsv4.NFS4ERR_TOO_MANY_OPS)
)

// nfs41ClientState keeps track of all state corresponding to a single
// client. For every client we track one or more incarnations that are
// created through EXCHANGE_ID and confirmed through an initial call to
// CREATE_SESSION.
type nfs41ClientState struct {
	ownerID string

	incarnationsByClientVerifier map[nfsv4.Verifier4]*clientIncarnationState
	confirmedIncarnation         *clientIncarnationState
}

// clientIncarnationState keeps track of all state corresponding to a
// single incarnation of a client created through EXCHANGE_ID.
type clientIncarnationState struct {
	// Constant fields.
	client         *nfs41ClientState
	clientID       nfsv4.Clientid4
	clientVerifier nfsv4.Verifier4

	// Fields protected by nfs41ProgramState.clientsLock.
	previousIdle              *clientIncarnationState
	nextIdle                  *clientIncarnationState
	lastSeen                  time.Time
	holdCount                 int
	lastSequenceID            nfsv4.Sequenceid4
	lastCreateSessionResponse nfsv4.CreateSession4res
	sessions                  sessionState

	// Fields that are protected by nfs41ProgramState.clientsLock if
	// clientIncarnationState.holdCount == 0, and
	// clientIncarnationState.lock otherwise.
	lock                  sync.RWMutex
	openOwnersByOwner     map[string]*nfs41OpenOwnerState
	openOwnerFilesByOther map[uint64]*nfs41OpenOwnerFileState
	lockOwnersByOwner     map[string]*nfs41LockOwnerState
	lockOwnerFilesByOther map[uint64]*nfs41LockOwnerFileState
	lastStateIDOther      uint64
}

// removeFromIdleList removes the client incarnation from the list of
// clients that are currently not performing any operations against the
// server.
func (cis *clientIncarnationState) removeFromIdleList() {
	cis.previousIdle.nextIdle = cis.nextIdle
	cis.nextIdle.previousIdle = cis.previousIdle
	cis.previousIdle = nil
	cis.nextIdle = nil
}

// insertIntoIdleList inserts the client incarnation into the list of
// clients that are currently not performing any operations against the
// server.
func (cis *clientIncarnationState) insertIntoIdleList(p *nfs41Program) {
	cis.previousIdle = p.idleClientIncarnations.previousIdle
	cis.nextIdle = &p.idleClientIncarnations
	cis.previousIdle.nextIdle = cis
	cis.nextIdle.previousIdle = cis
	cis.lastSeen = p.now
}

// hold the client incarnation in such a way that it's not garbage
// collected. This needs to be called prior to performing a blocking
// operation.
func (cis *clientIncarnationState) hold() {
	if cis.holdCount == 0 {
		cis.removeFromIdleList()
	}
	cis.holdCount++
}

// release the client incarnation in such a way that it may be garbage
// collected.
func (cis *clientIncarnationState) release(p *nfs41Program) {
	if cis.holdCount == 0 {
		panic("Attempted to decrease zero hold count")
	}
	cis.holdCount--
	if cis.holdCount == 0 {
		cis.insertIntoIdleList(p)
	}
}

// emptyAndRemove forcefully removes a client incarnation by, destroying
// any of its sessions, closing any files, and releasing any locks it
// had acquired. This is performed when clients disappear without
// calling DESTROY_CLIENTID.
func (cis *clientIncarnationState) emptyAndRemove(p *nfs41Program, ll *leavesToClose) {
	for _, oos := range cis.openOwnersByOwner {
		for _, oofs := range oos.filesByHandle {
			oofs.remove(p, cis, ll)
		}
	}
	for cis.sessions.next != &cis.sessions {
		cis.sessions.next.remove(p)
	}
	cis.remove(p)
}

// remove a client incarnation that no longer has any sessions, opened
// files or locks.
func (cis *clientIncarnationState) remove(p *nfs41Program) {
	if cis.holdCount != 0 {
		panic("Client incarnation is still running one or more blocking operations")
	}
	if len(cis.openOwnersByOwner) != 0 {
		panic("Client incarnation still has one or more open-owners")
	}
	if len(cis.openOwnerFilesByOther) != 0 {
		panic("Client incarnation still has one or more open-owner files")
	}
	if len(cis.lockOwnerFilesByOther) != 0 {
		panic("Client incarnation still has one or more lock-owner files")
	}
	if cis.sessions.next != &cis.sessions {
		panic("Client incarnation still has one or more sessions")
	}

	client := cis.client
	if client.confirmedIncarnation == cis {
		client.confirmedIncarnation = nil
	}

	// Remove the client incarnation.
	delete(client.incarnationsByClientVerifier, cis.clientVerifier)
	delete(p.clientIncarnationsByClientID, cis.clientID)
	cis.removeFromIdleList()

	// Remove the client if it no longer contains any incarnations.
	if len(client.incarnationsByClientVerifier) == 0 {
		delete(p.clientsByOwnerID, client.ownerID)
	}
}

// newRegularStateID allocates a new open-owner or lock-owner state ID.
func (cis *clientIncarnationState) newRegularStateID() nfs41RegularStateID {
	cis.lastStateIDOther++
	return nfs41RegularStateID{
		other: cis.lastStateIDOther,
	}
}

// testStateID tests the validity of a single state ID, which is used by
// the TEST_STATEID operation.
func (cis *clientIncarnationState) testStateID(rawStateID *nfsv4.Stateid4) nfsv4.Nfsstat4 {
	stateID, st := newNFS41RegularStateID(rawStateID)
	if st != nfsv4.NFS4_OK {
		return st
	}
	if oofs, ok := cis.openOwnerFilesByOther[stateID.other]; ok {
		return nfs41CompareStateSeqID(stateID.seqID, oofs.stateID.seqID)
	}
	if lofs, ok := cis.lockOwnerFilesByOther[stateID.other]; ok {
		return nfs41CompareStateSeqID(stateID.seqID, lofs.stateID.seqID)
	}
	return nfsv4.NFS4ERR_BAD_STATEID
}

// sessionState keeps track of all state corresponding to a session that
// was created by calling CREATE_SESSION.
type sessionState struct {
	sessionID         nfsv4.Sessionid4
	clientIncarnation *clientIncarnationState

	previous *sessionState
	next     *sessionState
	slots    []slotState
}

func (ss *sessionState) remove(p *nfs41Program) {
	ss.previous.next = ss.next
	ss.next.previous = ss.previous
	ss.previous = nil
	ss.next = nil
	delete(p.sessionsBySessionID, ss.sessionID)
}

// slotState keeps track of all state corresponding to a single slot of
// execution within a session.
type slotState struct {
	lastSequenceID         nfsv4.Sequenceid4
	lastResult             compoundResult
	currentSequenceWaiters []chan<- compoundResult
}

// nfs41OpenOwnerState stores information on a single open-owner, which
// is a single process running on the client that opens files through
// the mount.
type nfs41OpenOwnerState struct {
	// Constant fields.
	key string

	// Fields that are protected by nfs41ProgramState.clientsLock if
	// clientIncarnationState.holdCount == 0, and
	// clientIncarnationState.lock otherwise.
	filesByHandle map[string]*nfs41OpenOwnerFileState
}

// nfs41OpenOwnerFileState stores information on a file that is
// currently opened within the context of a single open-owner.
type nfs41OpenOwnerFileState struct {
	// Constant fields.
	openOwner  *nfs41OpenOwnerState
	openedFile *OpenedFile

	// Fields that are protected by nfs41ProgramState.clientsLock if
	// clientIncarnationState.holdCount == 0, and
	// clientIncarnationState.lock otherwise.
	shareAccess    virtual.ShareMask
	stateID        nfs41RegularStateID
	shareCount     shareCount
	lockOwnerFiles map[*nfs41LockOwnerState]*nfs41LockOwnerFileState
}

// downgradeShareAccess downgrades the share reservations of an
// open-owner file or lock-owner file. If this causes a given share
// reservation to become unused by the open-owner file and all of its
// lock-owner files, it schedules (partial) closure of the underlying
// virtual.Leaf object.
//
// This method is called at the end of CLOSE and FREE_STATEID, but also
// at the end of READ or WRITE. A call to CLOSE/FREE_STATEID may not
// immediately close a file if one or more READ/WRITE operations are
// still in progress.
func (oofs *nfs41OpenOwnerFileState) downgradeShareAccess(shareAccess *virtual.ShareMask, newShareAccess virtual.ShareMask, ll *leavesToClose) {
	if shareAccessToClose := oofs.shareCount.downgrade(shareAccess, newShareAccess); shareAccessToClose != 0 {
		ll.leaves = append(ll.leaves, leafToClose{
			leaf:        oofs.openedFile.GetLeaf(),
			shareAccess: shareAccessToClose,
		})
	}
}

// remove an open-owner file. This is normally done by calling CLOSE.
func (oofs *nfs41OpenOwnerFileState) remove(p *nfs41Program, cis *clientIncarnationState, ll *leavesToClose) {
	// Release lock state IDs associated with the file. If these
	// have one or more byte ranges locked, we unlock them. It would
	// also be permitted to let CLOSE return NFS4ERR_LOCKS_HELD,
	// requiring that the client issues LOCKU operations before
	// retrying, but that is less efficient.
	//
	// More details:
	// - RFC 8881, section 8.2.4, paragraph 1.
	// - RFC 8881, section 9.8, paragraph 3.
	// - RFC 8881, section 18.2.3, paragraph 2.
	for _, lofs := range oofs.lockOwnerFiles {
		lofs.unlockAndRemove(cis, ll)
	}
	oofs.downgradeShareAccess(&oofs.shareAccess, 0, ll)

	// We no longer need to guarantee that the filehandle remains
	// resolvable via PUTFH, and also don't need to track byte-range
	// locks.
	oofs.openedFile.Close()

	// Remove the nfs41OpenOwnerFileState.
	oos := oofs.openOwner
	delete(oos.filesByHandle, string(oofs.openedFile.GetHandle()))
	delete(cis.openOwnerFilesByOther, oofs.stateID.other)

	// Remove the nfs41OpenOwnerState if it has become empty.
	if len(oos.filesByHandle) == 0 {
		delete(cis.openOwnersByOwner, oos.key)
	}
}

// upgrade the share access mask of an opened file (e.g., from reading
// to reading and writing). This needs to be performed if an open-owner
// attempts to open the same file multiple times.
func (oofs *nfs41OpenOwnerFileState) upgrade(shareAccess virtual.ShareMask, leaf virtual.Leaf, ll *leavesToClose) {
	if overlap := oofs.shareCount.upgrade(&oofs.shareAccess, shareAccess); overlap != 0 {
		// As there is overlap between the share access masks,
		// the file is opened redundantly. Close it.
		ll.leaves = append(ll.leaves, leafToClose{
			leaf:        leaf,
			shareAccess: overlap,
		})
	}
	oofs.stateID.incrementSeqID()
}

// nfs41OpenOwnerState stores information on a single lock-owner, which
// is a single process running on the client that acquires locks against
// files through the mount.
type nfs41LockOwnerState struct {
	// Constant fields.
	clientIncarnation *clientIncarnationState
	owner             nfsv4.LockOwner4

	// Fields that are protected by nfs41ProgramState.clientsLock if
	// clientIncarnationState.holdCount == 0, and
	// clientIncarnationState.lock otherwise.
	fileCount referenceCount
}

func (los *nfs41LockOwnerState) decreaseFileCount() {
	if los.fileCount.decrease() {
		delete(los.clientIncarnation.lockOwnersByOwner, string(los.owner.Owner))
	}
}

// nfs41LockOwnerFileState represents byte-range locking state
// associated with a given opened file and given lock-owner.
type nfs41LockOwnerFileState struct {
	// Constant fields.
	lockOwner     *nfs41LockOwnerState
	openOwnerFile *nfs41OpenOwnerFileState
	shareAccess   virtual.ShareMask

	// Fields that are protected by nfs41ProgramState.clientsLock if
	// clientIncarnationState.holdCount == 0, and
	// clientIncarnationState.lock otherwise.
	lockCount int
	stateID   nfs41RegularStateID
}

// unlockAndRemove releases any locks in the file that are owned by the
// lock-owner, and subsequently removes the lock-owner file state. This
// is normally performed when closing the file or forcefully removing
// the client incarnation due to inactivity.
func (lofs *nfs41LockOwnerFileState) unlockAndRemove(cis *clientIncarnationState, ll *leavesToClose) {
	if lofs.lockCount > 0 {
		// Lock-owner still has one or more locks held on this
		// file. Issue an unlock operation that spans the full
		// range of the file to release all locks at once.
		lofs.lockCount += lofs.openOwnerFile.openedFile.UnlockAll(&lofs.lockOwner.owner)
	}
	lofs.remove(cis, ll)
}

// remove the lock-owner file, under the assumption that it currently
// doesn't hold any locks in the file. This is used by FREE_STATEID.
func (lofs *nfs41LockOwnerFileState) remove(cis *clientIncarnationState, ll *leavesToClose) {
	if lofs.lockCount != 0 {
		panic("Lock-owner file still holds one or more locks")
	}

	// Remove the lock-owner file from maps.
	delete(cis.lockOwnerFilesByOther, lofs.stateID.other)
	los := lofs.lockOwner
	oofs := lofs.openOwnerFile
	delete(oofs.lockOwnerFiles, los)
	oofs.downgradeShareAccess(&lofs.shareAccess, 0, ll)
	los.decreaseFileCount()
}

// sequenceState contains the state that needs to be tracked during the
// lifetime of a single NFSv4 COMPOUND procedure, where the first
// operation is of type SEQUENCE. It provides implementations of each of
// the operations that can only be performed within a sequenced request.
type sequenceState struct {
	program *nfs41Program
	session *sessionState

	currentFileHandle nfs41FileHandle
	savedFileHandle   nfs41FileHandle
}

// getOpenOwnerFileByStateID obtains an open-owner file by open state
// ID. It also checks whether the open state ID corresponds to the
// current file handle, and that the client provided sequence ID matches
// the server's value.
func (s *sequenceState) getOpenOwnerFileByStateID(rawStateID *nfsv4.Stateid4, willDowngrade bool) (*nfs41OpenOwnerFileState, nfsv4.Nfsstat4) {
	if !s.currentFileHandle.node.IsSet() {
		return nil, nfsv4.NFS4ERR_NOFILEHANDLE
	}

	cis := s.session.clientIncarnation
	if *rawStateID == currentStateID {
		// The state ID was yielded by a previous operation in
		// the sequence. Only in the case of CLOSE and
		// OPEN_DOWNGRADE should we enforce the sequence ID.
		//
		// More details: RFC 8881, section 8.2.3, bullet point
		// 3, and section 16.2.3.1.2.
		oofs, ok := cis.openOwnerFilesByOther[s.currentFileHandle.stateID.other]
		if !ok {
			return nil, nfsv4.NFS4ERR_BAD_STATEID
		}
		if willDowngrade && s.currentFileHandle.stateID.seqID != oofs.stateID.seqID {
			return nil, nfsv4.NFS4ERR_OLD_STATEID
		}
		return oofs, nfsv4.NFS4_OK
	}

	stateID, st := newNFS41RegularStateID(rawStateID)
	if st != nfsv4.NFS4_OK {
		return nil, st
	}
	oofs, ok := cis.openOwnerFilesByOther[stateID.other]
	if !ok {
		return nil, nfsv4.NFS4ERR_BAD_STATEID
	}
	if !bytes.Equal(s.currentFileHandle.handle, oofs.openedFile.GetHandle()) {
		return nil, nfsv4.NFS4ERR_BAD_STATEID
	}
	return oofs, nfs41CompareStateSeqID(stateID.seqID, oofs.stateID.seqID)
}

// getLockOwnerFileByStateID obtains an lock-owner file by lock state
// ID. It also checks whether the lock state ID corresponds to the
// current file handle, and that the client provided sequence ID matches
// the server's value.
func (s *sequenceState) getLockOwnerFileByStateID(rawStateID *nfsv4.Stateid4) (*nfs41LockOwnerFileState, nfsv4.Nfsstat4) {
	if !s.currentFileHandle.node.IsSet() {
		return nil, nfsv4.NFS4ERR_NOFILEHANDLE
	}

	cis := s.session.clientIncarnation
	if *rawStateID == currentStateID {
		lofs, ok := cis.lockOwnerFilesByOther[s.currentFileHandle.stateID.other]
		if !ok {
			return nil, nfsv4.NFS4ERR_BAD_STATEID
		}
		return lofs, nfsv4.NFS4_OK
	}

	stateID, st := newNFS41RegularStateID(rawStateID)
	if st != nfsv4.NFS4_OK {
		return nil, st
	}
	lofs, ok := cis.lockOwnerFilesByOther[stateID.other]
	if !ok {
		return nil, nfsv4.NFS4ERR_BAD_STATEID
	}
	if !bytes.Equal(s.currentFileHandle.handle, lofs.openOwnerFile.openedFile.GetHandle()) {
		return nil, nfsv4.NFS4ERR_BAD_STATEID
	}
	return lofs, nfs41CompareStateSeqID(stateID.seqID, lofs.stateID.seqID)
}

// getOpenedLeaf is used by READ and WRITE operations to obtain an
// opened leaf corresponding to a file handle and open-owner state ID.
//
// When a special state ID is provided, it ensures the file is
// temporarily opened for the duration of the operation. When a
// non-special state ID is provided, it ensures that the file was
// originally opened with the correct share access mask.
func (s *sequenceState) getOpenedLeaf(ctx context.Context, stateID *nfsv4.Stateid4, shareAccess virtual.ShareMask) (virtual.Leaf, func(), nfsv4.Nfsstat4) {
	if *stateID == anonymousStateID || *stateID == readBypassStateID {
		// Client provided the anonymous state ID or READ bypass
		// state ID. Temporarily open the file to perform the
		// operation.
		currentLeaf, st := s.currentFileHandle.getLeaf()
		if st != nfsv4.NFS4_OK {
			return nil, nil, st
		}
		if vs := currentLeaf.VirtualOpenSelf(
			ctx,
			shareAccess,
			&virtual.OpenExistingOptions{},
			0,
			&virtual.Attributes{},
		); vs != virtual.StatusOK {
			return nil, nil, toNFSv4Status(vs)
		}
		return currentLeaf, func() {
			currentLeaf.VirtualClose(shareAccess)
		}, nfsv4.NFS4_OK
	}

	cis := s.session.clientIncarnation
	cis.lock.Lock()
	defer cis.lock.Unlock()

	oofs, st := s.getOpenOwnerFileByStateID(stateID, false)
	switch st {
	case nfsv4.NFS4_OK:
		if shareAccess&^oofs.shareAccess != 0 {
			// Attempting to write to a file opened for
			// reading, or vice versa.
			return nil, nil, nfsv4.NFS4ERR_OPENMODE
		}
	case nfsv4.NFS4ERR_BAD_STATEID:
		// Client may have provided a lock state ID.
		lofs, st := s.getLockOwnerFileByStateID(stateID)
		if st != nfsv4.NFS4_OK {
			return nil, nil, st
		}
		oofs = lofs.openOwnerFile
		if shareAccess&^lofs.shareAccess != 0 {
			// Attempted to write to a file that was opened
			// for reading at the time the lock-owner state
			// was established, or vice versa.
			//
			// More details: RFC 8881, section 9.1.2,
			// paragraph 7.
			return nil, nil, nfsv4.NFS4ERR_OPENMODE
		}
	default:
		return nil, nil, st
	}

	// Ensure that the file is not released while the I/O operation
	// is taking place.
	clonedShareAccess := oofs.shareCount.clone(shareAccess)
	return oofs.openedFile.GetLeaf(), func() {
		var ll leavesToClose
		cis.lock.Lock()
		oofs.downgradeShareAccess(&clonedShareAccess, 0, &ll)
		cis.lock.Unlock()
		ll.closeAll()
	}, nfsv4.NFS4_OK
}

// verifyAttributes is the common implementation of the VERIFY and
// NVERIFY operations.
func (s *sequenceState) verifyAttributes(ctx context.Context, fattr *nfsv4.Fattr4) nfsv4.Nfsstat4 {
	currentNode, _, st := s.currentFileHandle.getNode()
	if st != nfsv4.NFS4_OK {
		return st
	}

	// Request attributes of the file. Don't actually store them in
	// a fattr4 structure. Use comparingWriter to check whether the
	// generated attributes are equal to the ones provided.
	attrRequest := fattr.Attrmask
	var attributes virtual.Attributes
	currentNode.VirtualGetAttributes(ctx, attrRequestToAttributesMask(attrRequest), &attributes)
	w := comparingWriter{
		reference: fattr.AttrVals,
		status:    nfsv4.NFS4ERR_SAME,
	}
	p := s.program
	attrMask := p.writeAttributes(&attributes, attrRequest, &w)

	for i := 0; i < len(attrRequest); i++ {
		if attrMask[i] != attrRequest[i] {
			// One or more of the provided attributes were
			// not generated. This either means that the
			// client provided unsupported attributes or
			// ones that are write-only.
			if attrRequest[0]&(1<<nfsv4.FATTR4_RDATTR_ERROR) != 0 {
				return nfsv4.NFS4ERR_INVAL
			}
			return nfsv4.NFS4ERR_ATTRNOTSUPP
		}
	}
	if len(w.reference) > 0 {
		// Provided attributes contain trailing data.
		return nfsv4.NFS4ERR_BADXDR
	}
	return w.status
}

func (s *sequenceState) opAccess(ctx context.Context, args *nfsv4.Access4args) nfsv4.Access4res {
	currentNode, isDirectory, st := s.currentFileHandle.getNode()
	if st != nfsv4.NFS4_OK {
		return &nfsv4.Access4res_default{Status: st}
	}

	// Depending on whether the node is a directory or a leaf, we
	// need to report different NFSv4 acccess permissions.
	readMask := uint32(nfsv4.ACCESS4_READ)
	writeMask := uint32(nfsv4.ACCESS4_EXTEND | nfsv4.ACCESS4_MODIFY)
	executeMask := uint32(0)
	if isDirectory {
		writeMask |= nfsv4.ACCESS4_DELETE
		executeMask |= nfsv4.ACCESS4_LOOKUP
	} else {
		executeMask |= nfsv4.ACCESS4_EXECUTE
	}

	// Request node permissions and convert them to NFSv4 values.
	var attributes virtual.Attributes
	currentNode.VirtualGetAttributes(ctx, virtual.AttributesMaskPermissions, &attributes)
	permissions, ok := attributes.GetPermissions()
	if !ok {
		panic("Permissions attribute requested, but not returned")
	}
	var access nfsv4.Uint32T
	if permissions&virtual.PermissionsRead != 0 {
		access |= readMask
	}
	if permissions&virtual.PermissionsWrite != 0 {
		access |= writeMask
	}
	if permissions&virtual.PermissionsExecute != 0 {
		access |= executeMask
	}

	return &nfsv4.Access4res_NFS4_OK{
		Resok4: nfsv4.Access4resok{
			Supported: (readMask | writeMask | executeMask) & args.Access,
			Access:    access & args.Access,
		},
	}
}

func (s *sequenceState) opBackchannelCtl(args *nfsv4.BackchannelCtl4args) nfsv4.BackchannelCtl4res {
	// We don't send any traffic across the backchannel, so there is
	// nothing in the arguments that needs to be preserved.
	return nfsv4.BackchannelCtl4res{BcrStatus: nfsv4.NFS4_OK}
}

func (s *sequenceState) opBindConnToSession(args *nfsv4.BindConnToSession4args) nfsv4.BindConnToSession4res {
	return &nfsv4.BindConnToSession4res_default{BctsrStatus: nfsv4.NFS4ERR_NOT_ONLY_OP}
}

func (s *sequenceState) opClose(args *nfsv4.Close4args) nfsv4.Close4res {
	var ll leavesToClose
	defer ll.closeAll()
	cis := s.session.clientIncarnation
	cis.lock.Lock()
	defer cis.lock.Unlock()

	oofs, st := s.getOpenOwnerFileByStateID(&args.OpenStateid, true)
	if st != nfsv4.NFS4_OK {
		return &nfsv4.Close4res_default{Status: st}
	}
	oofs.remove(s.program, cis, &ll)
	return &nfsv4.Close4res_NFS4_OK{
		OpenStateid: invalidStateID,
	}
}

func (s *sequenceState) opCommit(args *nfsv4.Commit4args) nfsv4.Commit4res {
	// As this implementation is purely built for the purpose of
	// doing builds, there is no need to actually commit to storage.
	if _, st := s.currentFileHandle.getLeaf(); st != nfsv4.NFS4_OK {
		return &nfsv4.Commit4res_default{Status: st}
	}
	return &nfsv4.Commit4res_NFS4_OK{
		Resok4: nfsv4.Commit4resok{
			Writeverf: s.program.rebootVerifier,
		},
	}
}

func (s *sequenceState) opCreate(ctx context.Context, args *nfsv4.Create4args) nfsv4.Create4res {
	currentDirectory, st := s.currentFileHandle.getDirectory()
	if st != nfsv4.NFS4_OK {
		return &nfsv4.Create4res_default{Status: st}
	}
	name, st := nfsv4NewComponent(args.Objname)
	if st != nfsv4.NFS4_OK {
		return &nfsv4.Create4res_default{Status: st}
	}

	var attributes virtual.Attributes
	var changeInfo virtual.ChangeInfo
	var fileHandle nfs41FileHandle
	var vs virtual.Status
	switch objectType := args.Objtype.(type) {
	case *nfsv4.Createtype4_NF4BLK, *nfsv4.Createtype4_NF4CHR:
		// Character and block devices can only be provided as
		// part of input roots, if workers are set up to provide
		// them. They can't be created through the virtual file
		// system.
		return &nfsv4.Create4res_default{Status: nfsv4.NFS4ERR_PERM}
	case *nfsv4.Createtype4_NF4DIR:
		var directory virtual.Directory
		directory, changeInfo, vs = currentDirectory.VirtualMkdir(name, virtual.AttributesMaskFileHandle, &attributes)
		fileHandle.node = virtual.DirectoryChild{}.FromDirectory(directory)
	case *nfsv4.Createtype4_NF4FIFO:
		var leaf virtual.Leaf
		leaf, changeInfo, vs = currentDirectory.VirtualMknod(ctx, name, filesystem.FileTypeFIFO, virtual.AttributesMaskFileHandle, &attributes)
		fileHandle.node = virtual.DirectoryChild{}.FromLeaf(leaf)
	case *nfsv4.Createtype4_NF4LNK:
		var leaf virtual.Leaf
		leaf, changeInfo, vs = currentDirectory.VirtualSymlink(ctx, objectType.Linkdata, name, virtual.AttributesMaskFileHandle, &attributes)
		fileHandle.node = virtual.DirectoryChild{}.FromLeaf(leaf)
	case *nfsv4.Createtype4_NF4SOCK:
		var leaf virtual.Leaf
		leaf, changeInfo, vs = currentDirectory.VirtualMknod(ctx, name, filesystem.FileTypeSocket, virtual.AttributesMaskFileHandle, &attributes)
		fileHandle.node = virtual.DirectoryChild{}.FromLeaf(leaf)
	default:
		return &nfsv4.Create4res_default{Status: nfsv4.NFS4ERR_BADTYPE}
	}
	if vs != virtual.StatusOK {
		return &nfsv4.Create4res_default{Status: toNFSv4Status(vs)}
	}
	fileHandle.handle = attributes.GetFileHandle()

	s.currentFileHandle = fileHandle
	return &nfsv4.Create4res_NFS4_OK{
		Resok4: nfsv4.Create4resok{
			Cinfo: toNFSv4ChangeInfo(&changeInfo),
		},
	}
}

func (s *sequenceState) opDelegPurge(args *nfsv4.Delegpurge4args) nfsv4.Delegpurge4res {
	// This implementation does not support CLAIM_DELEGATE_PREV, so
	// there is no need to implement DELEGPURGE.
	return nfsv4.Delegpurge4res{Status: nfsv4.NFS4ERR_NOTSUPP}
}

func (s *sequenceState) opDelegReturn(args *nfsv4.Delegreturn4args) nfsv4.Delegreturn4res {
	// This implementation never hands out any delegations to the
	// client, meaning that any state ID provided to this operation
	// is invalid.
	return nfsv4.Delegreturn4res{Status: nfsv4.NFS4ERR_BAD_STATEID}
}

func (s *sequenceState) opFreeStateID(args *nfsv4.FreeStateid4args) nfsv4.FreeStateid4res {
	stateID, st := newNFS41RegularStateID(&args.FsaStateid)
	if st != nfsv4.NFS4_OK {
		return nfsv4.FreeStateid4res{FsrStatus: st}
	}

	var ll leavesToClose
	defer ll.closeAll()
	cis := s.session.clientIncarnation
	cis.lock.Lock()
	defer cis.lock.Unlock()

	lofs, ok := cis.lockOwnerFilesByOther[stateID.other]
	if !ok {
		return nfsv4.FreeStateid4res{FsrStatus: nfsv4.NFS4ERR_BAD_STATEID}
	}
	if st := nfs41CompareStateSeqID(stateID.seqID, lofs.stateID.seqID); st != nfsv4.NFS4_OK {
		return nfsv4.FreeStateid4res{FsrStatus: st}
	}
	lofs.remove(cis, &ll)
	return nfsv4.FreeStateid4res{FsrStatus: nfsv4.NFS4_OK}
}

func (s *sequenceState) opGetAttr(ctx context.Context, args *nfsv4.Getattr4args) nfsv4.Getattr4res {
	currentNode, _, st := s.currentFileHandle.getNode()
	if st != nfsv4.NFS4_OK {
		return &nfsv4.Getattr4res_default{Status: st}
	}
	var attributes virtual.Attributes
	currentNode.VirtualGetAttributes(ctx, attrRequestToAttributesMask(args.AttrRequest), &attributes)
	p := s.program
	return &nfsv4.Getattr4res_NFS4_OK{
		Resok4: nfsv4.Getattr4resok{
			ObjAttributes: p.attributesToFattr4(&attributes, args.AttrRequest),
		},
	}
}

func (s *sequenceState) opGetDeviceInfo(args *nfsv4.Getdeviceinfo4args) nfsv4.Getdeviceinfo4res {
	return &nfsv4.Getdeviceinfo4res_default{GdirStatus: nfsv4.NFS4ERR_NOTSUPP}
}

func (s *sequenceState) opGetDeviceList(args *nfsv4.Getdevicelist4args) nfsv4.Getdevicelist4res {
	return &nfsv4.Getdevicelist4res_default{GdlrStatus: nfsv4.NFS4ERR_NOTSUPP}
}

func (s *sequenceState) opGetFH() nfsv4.Getfh4res {
	_, _, st := s.currentFileHandle.getNode()
	if st != nfsv4.NFS4_OK {
		return &nfsv4.Getfh4res_default{Status: st}
	}
	return &nfsv4.Getfh4res_NFS4_OK{
		Resok4: nfsv4.Getfh4resok{
			Object: s.currentFileHandle.handle,
		},
	}
}

func (s *sequenceState) opGetDirDelegation(args *nfsv4.GetDirDelegation4args) nfsv4.GetDirDelegation4res {
	return &nfsv4.GetDirDelegation4res_default{GddrStatus: nfsv4.NFS4ERR_NOTSUPP}
}

func (s *sequenceState) opLayoutCommit(args *nfsv4.Layoutcommit4args) nfsv4.Layoutcommit4res {
	return &nfsv4.Layoutcommit4res_default{LocrStatus: nfsv4.NFS4ERR_NOTSUPP}
}

func (s *sequenceState) opLayoutGet(args *nfsv4.Layoutget4args) nfsv4.Layoutget4res {
	return &nfsv4.Layoutget4res_default{LogrStatus: nfsv4.NFS4ERR_NOTSUPP}
}

func (s *sequenceState) opLayoutReturn(args *nfsv4.Layoutreturn4args) nfsv4.Layoutreturn4res {
	return &nfsv4.Layoutreturn4res_default{LorrStatus: nfsv4.NFS4ERR_NOTSUPP}
}

func (s *sequenceState) opLink(ctx context.Context, args *nfsv4.Link4args) nfsv4.Link4res {
	sourceLeaf, st := s.savedFileHandle.getLeaf()
	if st != nfsv4.NFS4_OK {
		return &nfsv4.Link4res_default{Status: st}
	}
	targetDirectory, st := s.currentFileHandle.getDirectory()
	if st != nfsv4.NFS4_OK {
		return &nfsv4.Link4res_default{Status: st}
	}
	name, st := nfsv4NewComponent(args.Newname)
	if st != nfsv4.NFS4_OK {
		return &nfsv4.Link4res_default{Status: st}
	}
	changeInfo, vs := targetDirectory.VirtualLink(ctx, name, sourceLeaf, 0, &virtual.Attributes{})
	if vs != virtual.StatusOK {
		return &nfsv4.Link4res_default{Status: toNFSv4Status(vs)}
	}
	return &nfsv4.Link4res_NFS4_OK{
		Resok4: nfsv4.Link4resok{
			Cinfo: toNFSv4ChangeInfo(&changeInfo),
		},
	}
}

func (s *sequenceState) opLock(args *nfsv4.Lock4args) nfsv4.Lock4res {
	cis := s.session.clientIncarnation
	cis.lock.Lock()
	defer cis.lock.Unlock()

	var lofs *nfs41LockOwnerFileState
	var los *nfs41LockOwnerState
	var oofs *nfs41OpenOwnerFileState
	switch locker := args.Locker.(type) {
	case *nfsv4.Locker4_TRUE:
		// Create a new lock-owner file if needed.
		var st nfsv4.Nfsstat4
		oofs, st = s.getOpenOwnerFileByStateID(&locker.OpenOwner.OpenStateid, false)
		if st != nfsv4.NFS4_OK {
			return &nfsv4.Lock4res_default{Status: st}
		}

		lockOwner := locker.OpenOwner.LockOwner.Owner
		lockOwnerKey := string(lockOwner)
		var ok bool
		los, ok = cis.lockOwnersByOwner[lockOwnerKey]
		if !ok {
			los = &nfs41LockOwnerState{
				clientIncarnation: cis,
				owner: nfsv4.LockOwner4{
					Clientid: cis.clientID,
					Owner:    lockOwner,
				},
				fileCount: 1,
			}
			defer los.decreaseFileCount()
		}
		lofs = oofs.lockOwnerFiles[los]
	case *nfsv4.Locker4_FALSE:
		// Add additional lock to existing lock-owner file.
		var st nfsv4.Nfsstat4
		lofs, st = s.getLockOwnerFileByStateID(&locker.LockOwner.LockStateid)
		if st != nfsv4.NFS4_OK {
			return &nfsv4.Lock4res_default{Status: st}
		}
		los = lofs.lockOwner
		oofs = lofs.openOwnerFile
	default:
		// Incorrectly encoded boolean value.
		return &nfsv4.Lock4res_default{Status: nfsv4.NFS4ERR_BADXDR}
	}

	lockCountDelta, res := oofs.openedFile.Lock(&los.owner, args.Offset, args.Length, args.Locktype)
	if res != nil {
		return res
	}

	if lofs == nil {
		lofs = &nfs41LockOwnerFileState{
			lockOwner:     los,
			openOwnerFile: oofs,
			shareAccess:   oofs.shareCount.clone(oofs.shareAccess),
			stateID:       cis.newRegularStateID(),
		}
		oofs.lockOwnerFiles[los] = lofs
		los.fileCount.increase()
		cis.lockOwnerFilesByOther[lofs.stateID.other] = lofs
	}

	lofs.lockCount += lockCountDelta
	if lofs.lockCount < 0 {
		panic("Negative lock count")
	}
	lofs.stateID.incrementSeqID()

	s.currentFileHandle.stateID = lofs.stateID
	return &nfsv4.Lock4res_NFS4_OK{
		Resok4: nfsv4.Lock4resok{
			LockStateid: lofs.stateID.externalize(),
		},
	}
}

func (s *sequenceState) opLockT(args *nfsv4.Lockt4args) nfsv4.Lockt4res {
	_, st := s.currentFileHandle.getLeaf()
	if st != nfsv4.NFS4_OK {
		return &nfsv4.Lockt4res_default{Status: st}
	}

	cis := s.session.clientIncarnation
	cis.lock.RLock()
	defer cis.lock.RUnlock()

	// Attempt to obtain the lock owner that is provided in the
	// arguments. It may be the case that none exists, in which case
	// we just use a nil value, which happens to be at an address
	// differs from any existing one.
	var owner *nfsv4.LockOwner4
	if los, ok := cis.lockOwnersByOwner[string(args.Owner.Owner)]; ok {
		owner = &los.owner
	}
	return s.program.openedFilesPool.TestLock(
		s.currentFileHandle.handle,
		owner,
		args.Offset,
		args.Length,
		args.Locktype,
	)
}

func (s *sequenceState) opLockU(args *nfsv4.Locku4args) nfsv4.Locku4res {
	cis := s.session.clientIncarnation
	cis.lock.Lock()
	defer cis.lock.Unlock()

	lofs, st := s.getLockOwnerFileByStateID(&args.LockStateid)
	if st != nfsv4.NFS4_OK {
		return &nfsv4.Locku4res_default{Status: st}
	}
	lockCountDelta, st := lofs.openOwnerFile.openedFile.Unlock(&lofs.lockOwner.owner, args.Offset, args.Length)
	if st != nfsv4.NFS4_OK {
		return &nfsv4.Locku4res_default{Status: st}
	}
	lofs.lockCount += lockCountDelta
	if lofs.lockCount < 0 {
		panic("Negative lock count")
	}
	lofs.stateID.incrementSeqID()

	s.currentFileHandle.stateID = lofs.stateID
	return &nfsv4.Locku4res_NFS4_OK{
		LockStateid: lofs.stateID.externalize(),
	}
}

func (s *sequenceState) opLookup(ctx context.Context, args *nfsv4.Lookup4args) nfsv4.Lookup4res {
	currentDirectory, st := s.currentFileHandle.getDirectoryOrSymlink(ctx)
	if st != nfsv4.NFS4_OK {
		return nfsv4.Lookup4res{Status: st}
	}
	name, st := nfsv4NewComponent(args.Objname)
	if st != nfsv4.NFS4_OK {
		return nfsv4.Lookup4res{Status: st}
	}
	var attributes virtual.Attributes
	child, vs := currentDirectory.VirtualLookup(ctx, name, virtual.AttributesMaskFileHandle, &attributes)
	if vs != virtual.StatusOK {
		return nfsv4.Lookup4res{Status: toNFSv4Status(vs)}
	}
	s.currentFileHandle = nfs41FileHandle{
		handle: attributes.GetFileHandle(),
		node:   child,
	}
	return nfsv4.Lookup4res{Status: nfsv4.NFS4_OK}
}

func (s *sequenceState) opLookupP(ctx context.Context) nfsv4.Lookupp4res {
	if _, st := s.currentFileHandle.getDirectoryOrSymlink(ctx); st != nfsv4.NFS4_OK {
		return nfsv4.Lookupp4res{Status: st}
	}

	// TODO: Do we want to implement this method as well? For most
	// directory types (e.g., CAS backed directories) this method is
	// hard to implement, as they don't necessarily have a single
	// parent.
	return nfsv4.Lookupp4res{Status: nfsv4.NFS4ERR_NOENT}
}

func (s *sequenceState) opNVerify(ctx context.Context, args *nfsv4.Nverify4args) nfsv4.Nverify4res {
	if st := s.verifyAttributes(ctx, &args.ObjAttributes); st != nfsv4.NFS4ERR_NOT_SAME {
		return nfsv4.Nverify4res{Status: st}
	}
	return nfsv4.Nverify4res{Status: nfsv4.NFS4_OK}
}

func (s *sequenceState) opOpen(ctx context.Context, args *nfsv4.Open4args) nfsv4.Open4res {
	// Convert share_* fields.
	shareAccess, st := nfs41ShareAccessToShareMask(args.ShareAccess)
	if st != nfsv4.NFS4_OK {
		return &nfsv4.Open4res_default{Status: st}
	}

	// As with most UNIX-like systems, we don't support share_deny.
	// Only permit this field to be set to OPEN4_SHARE_DENY_NONE,
	// behaving as if there's an implicit OPEN4_SHARE_ACCESS_BOTH on
	// all files.
	//
	// More details: RFC 8881, section 18.6.3, paragraph 14.
	switch args.ShareDeny {
	case nfsv4.OPEN4_SHARE_DENY_NONE:
	case nfsv4.OPEN4_SHARE_DENY_READ, nfsv4.OPEN4_SHARE_DENY_WRITE, nfsv4.OPEN4_SHARE_DENY_BOTH:
		return &nfsv4.Open4res_default{Status: nfsv4.NFS4ERR_SHARE_DENIED}
	default:
		return &nfsv4.Open4res_default{Status: nfsv4.NFS4ERR_INVAL}
	}

	// Convert openhow. Don't bother implementing EXCLUSIVE4 and
	// EXCLUSIVE4_1, as we announce supporting a persistent reply
	// cache.
	//
	// More details: RFC 8881, table 18.
	var createAttributes *virtual.Attributes
	var existingOptions *virtual.OpenExistingOptions
	if openHow, ok := args.Openhow.(*nfsv4.Openflag4_OPEN4_CREATE); ok {
		createAttributes = &virtual.Attributes{}
		switch how := openHow.How.(type) {
		case *nfsv4.Createhow4_UNCHECKED4:
			// Create a file, allowing the file to already exist.
			if st := fattr4ToAttributes(&how.Createattrs, createAttributes); st != nfsv4.NFS4_OK {
				return &nfsv4.Open4res_default{Status: st}
			}
			existingOptions = &virtual.OpenExistingOptions{}
			if sizeBytes, ok := createAttributes.GetSizeBytes(); ok && sizeBytes == 0 {
				existingOptions.Truncate = true
			}
		case *nfsv4.Createhow4_GUARDED4:
			// Create a file, disallowing the file to already exist.
			if st := fattr4ToAttributes(&how.Createattrs, createAttributes); st != nfsv4.NFS4_OK {
				return &nfsv4.Open4res_default{Status: st}
			}
		default:
			return &nfsv4.Open4res_default{Status: nfsv4.NFS4ERR_INVAL}
		}
	} else {
		// Don't create a new file. Only open an existing file.
		existingOptions = &virtual.OpenExistingOptions{}
	}

	// Convert claim. As we don't support delegations, we can only
	// meaningfully support CLAIM_NULL, CLAIM_PREVIOUS, and CLAIM_FH.
	var leaf virtual.Leaf
	var respected virtual.AttributesMask
	var changeInfo virtual.ChangeInfo
	var handle []byte
	switch claim := args.Claim.(type) {
	case *nfsv4.OpenClaim4_CLAIM_NULL:
		// Open the file, providing a directory and filename.
		currentDirectory, st := s.currentFileHandle.getDirectory()
		if st != nfsv4.NFS4_OK {
			return &nfsv4.Open4res_default{Status: st}
		}
		name, st := nfsv4NewComponent(claim.File)
		if st != nfsv4.NFS4_OK {
			return &nfsv4.Open4res_default{Status: st}
		}

		var attributes virtual.Attributes
		var vs virtual.Status
		leaf, respected, changeInfo, vs = currentDirectory.VirtualOpenChild(
			ctx,
			name,
			shareAccess,
			createAttributes,
			existingOptions,
			virtual.AttributesMaskFileHandle,
			&attributes)
		if vs != virtual.StatusOK {
			return &nfsv4.Open4res_default{Status: toNFSv4Status(vs)}
		}
		handle = attributes.GetFileHandle()
	case *nfsv4.OpenClaim4_CLAIM_FH, *nfsv4.OpenClaim4_CLAIM_PREVIOUS:
		// Open the file, providing a filehandle.
		leaf, st = s.currentFileHandle.getLeaf()
		if st != nfsv4.NFS4_OK {
			return &nfsv4.Open4res_default{Status: st}
		}
		if existingOptions == nil {
			return &nfsv4.Open4res_default{Status: nfsv4.NFS4ERR_EXIST}
		}

		if vs := leaf.VirtualOpenSelf(
			ctx,
			shareAccess,
			existingOptions,
			0,
			&virtual.Attributes{},
		); vs != virtual.StatusOK {
			return &nfsv4.Open4res_default{Status: toNFSv4Status(vs)}
		}
		handle = s.currentFileHandle.handle
	case *nfsv4.OpenClaim4_CLAIM_DELEGATE_CUR, *nfsv4.OpenClaim4_CLAIM_DELEG_CUR_FH:
		return &nfsv4.Open4res_default{Status: nfsv4.NFS4ERR_RECLAIM_BAD}
	case *nfsv4.OpenClaim4_CLAIM_DELEGATE_PREV, *nfsv4.OpenClaim4_CLAIM_DELEG_PREV_FH:
		return &nfsv4.Open4res_default{Status: nfsv4.NFS4ERR_NOTSUPP}
	default:
		return &nfsv4.Open4res_default{Status: nfsv4.NFS4ERR_INVAL}
	}

	var ll leavesToClose
	defer ll.closeAll()
	cis := s.session.clientIncarnation
	cis.lock.Lock()
	defer cis.lock.Unlock()

	// Look up the open-owner file, so that it can be upgraded.
	openOwnerKey := string(args.Owner.Owner)
	handleKey := string(handle)
	var oofs *nfs41OpenOwnerFileState
	switch claim := args.Claim.(type) {
	case *nfsv4.OpenClaim4_CLAIM_NULL, *nfsv4.OpenClaim4_CLAIM_FH:
		// If needed, create a new open-owner and/or open-owner
		// file state if this file isn't opened yet.
		oos, ok := cis.openOwnersByOwner[openOwnerKey]
		if !ok {
			oos = &nfs41OpenOwnerState{
				key:           openOwnerKey,
				filesByHandle: map[string]*nfs41OpenOwnerFileState{},
			}
			cis.openOwnersByOwner[openOwnerKey] = oos
		}
		oofs, ok = oos.filesByHandle[handleKey]
		if !ok {
			oofs = &nfs41OpenOwnerFileState{
				openOwner:      oos,
				openedFile:     s.program.openedFilesPool.Open(handle, leaf),
				stateID:        cis.newRegularStateID(),
				lockOwnerFiles: map[*nfs41LockOwnerState]*nfs41LockOwnerFileState{},
			}
			oos.filesByHandle[handleKey] = oofs
			cis.openOwnerFilesByOther[oofs.stateID.other] = oofs
		}
	case *nfsv4.OpenClaim4_CLAIM_PREVIOUS:
		// Only permit reusing an existing open-owner file,
		// using the same delegation type.
		oos, ok := cis.openOwnersByOwner[openOwnerKey]
		if ok {
			oofs, ok = oos.filesByHandle[handleKey]
		}
		if !ok || claim.DelegateType != nfsv4.OPEN_DELEGATE_NONE {
			ll.leaves = append(ll.leaves, leafToClose{
				leaf:        leaf,
				shareAccess: shareAccess,
			})
			return &nfsv4.Open4res_default{Status: nfsv4.NFS4ERR_RECLAIM_BAD}
		}
	default:
		panic("Reclaim type should have been handled above")
	}
	oofs.upgrade(shareAccess, leaf, &ll)

	s.currentFileHandle = nfs41FileHandle{
		handle:  handle,
		node:    virtual.DirectoryChild{}.FromLeaf(leaf),
		stateID: oofs.stateID,
	}
	return &nfsv4.Open4res_NFS4_OK{
		Resok4: nfsv4.Open4resok{
			Stateid:    oofs.stateID.externalize(),
			Cinfo:      toNFSv4ChangeInfo(&changeInfo),
			Rflags:     nfsv4.OPEN4_RESULT_LOCKTYPE_POSIX | nfsv4.OPEN4_RESULT_PRESERVE_UNLINKED,
			Attrset:    attributesMaskToBitmap4(respected),
			Delegation: &nfsv4.OpenDelegation4_OPEN_DELEGATE_NONE{},
		},
	}
}

func (s *sequenceState) opOpenAttr(args *nfsv4.Openattr4args) nfsv4.Openattr4res {
	// This implementation does not support named attributes.
	if _, _, st := s.currentFileHandle.getNode(); st != nfsv4.NFS4_OK {
		return nfsv4.Openattr4res{Status: st}
	}
	return nfsv4.Openattr4res{Status: nfsv4.NFS4ERR_NOTSUPP}
}

func (s *sequenceState) opOpenDowngrade(args *nfsv4.OpenDowngrade4args) nfsv4.OpenDowngrade4res {
	shareAccess, st := nfs40ShareAccessToShareMask(args.ShareAccess)
	if st != nfsv4.NFS4_OK {
		return &nfsv4.OpenDowngrade4res_default{Status: st}
	}

	var ll leavesToClose
	defer ll.closeAll()
	cis := s.session.clientIncarnation
	cis.lock.Lock()
	defer cis.lock.Unlock()

	oofs, st := s.getOpenOwnerFileByStateID(&args.OpenStateid, true)
	if st != nfsv4.NFS4_OK {
		return &nfsv4.OpenDowngrade4res_default{Status: st}
	}
	if shareAccess&^oofs.shareAccess != 0 || args.ShareDeny != nfsv4.OPEN4_SHARE_DENY_NONE {
		// Attempted to upgrade. The client should have called OPEN.
		//
		// More details: RFC 8881, section 18.18.3, paragraph 4.
		return &nfsv4.OpenDowngrade4res_default{Status: nfsv4.NFS4ERR_INVAL}
	}
	oofs.downgradeShareAccess(&oofs.shareAccess, shareAccess, &ll)
	oofs.stateID.incrementSeqID()

	s.currentFileHandle.stateID = oofs.stateID
	return &nfsv4.OpenDowngrade4res_NFS4_OK{
		Resok4: nfsv4.OpenDowngrade4resok{
			OpenStateid: oofs.stateID.externalize(),
		},
	}
}

func (s *sequenceState) opPutFH(args *nfsv4.Putfh4args) nfsv4.Putfh4res {
	child, st := s.program.openedFilesPool.Resolve(args.Object)
	if st != nfsv4.NFS4_OK {
		return nfsv4.Putfh4res{Status: st}
	}
	s.currentFileHandle = nfs41FileHandle{
		handle: args.Object,
		node:   child,
	}
	return nfsv4.Putfh4res{Status: nfsv4.NFS4_OK}
}

func (s *sequenceState) opPutPubFH() nfsv4.Putpubfh4res {
	p := s.program
	s.currentFileHandle = p.rootFileHandle
	return nfsv4.Putpubfh4res{Status: nfsv4.NFS4_OK}
}

func (s *sequenceState) opPutRootFH() nfsv4.Putrootfh4res {
	p := s.program
	s.currentFileHandle = p.rootFileHandle
	return nfsv4.Putrootfh4res{Status: nfsv4.NFS4_OK}
}

func (s *sequenceState) opRead(ctx context.Context, args *nfsv4.Read4args) nfsv4.Read4res {
	currentLeaf, cleanup, st := s.getOpenedLeaf(ctx, &args.Stateid, virtual.ShareMaskRead)
	if st != nfsv4.NFS4_OK {
		return &nfsv4.Read4res_default{Status: st}
	}
	defer cleanup()

	buf := make([]byte, args.Count)
	n, eof, vs := currentLeaf.VirtualRead(buf, args.Offset)
	if vs != virtual.StatusOK {
		return &nfsv4.Read4res_default{Status: toNFSv4Status(vs)}
	}
	return &nfsv4.Read4res_NFS4_OK{
		Resok4: nfsv4.Read4resok{
			Eof:  eof,
			Data: buf[:n],
		},
	}
}

func (s *sequenceState) opReadDir(ctx context.Context, args *nfsv4.Readdir4args) nfsv4.Readdir4res {
	currentDirectory, st := s.currentFileHandle.getDirectory()
	if st != nfsv4.NFS4_OK {
		return &nfsv4.Readdir4res_default{Status: st}
	}

	// Validate the cookie verifier.
	p := s.program
	if args.Cookie != 0 && args.Cookieverf != p.rebootVerifier && (args.Cookieverf != nfsv4.Verifier4{}) {
		return &nfsv4.Readdir4res_default{Status: nfsv4.NFS4ERR_NOT_SAME}
	}

	// Restore read offset.
	firstCookie := uint64(0)
	if args.Cookie > lastReservedCookie {
		firstCookie = args.Cookie - lastReservedCookie
	}

	// Empty response.
	res := nfsv4.Readdir4res_NFS4_OK{
		Resok4: nfsv4.Readdir4resok{
			Cookieverf: p.rebootVerifier,
			Reply: nfsv4.Dirlist4{
				Eof: true,
			},
		},
	}

	// Attach entries.
	reporter := readdirReporter{
		program:     p,
		attrRequest: args.AttrRequest,
		maxCount:    args.Maxcount,
		dirCount:    args.Dircount,

		currentMaxCount: nfsv4.Count4(res.Resok4.GetEncodedSizeBytes()),
		nextEntry:       &res.Resok4.Reply.Entries,
		endOfFile:       &res.Resok4.Reply.Eof,
	}
	if vs := currentDirectory.VirtualReadDir(
		ctx,
		firstCookie,
		attrRequestToAttributesMask(args.AttrRequest),
		&reporter,
	); vs != virtual.StatusOK {
		return &nfsv4.Readdir4res_default{Status: toNFSv4Status(vs)}
	}
	if res.Resok4.Reply.Entries == nil && !res.Resok4.Reply.Eof {
		// Not enough space to store a single entry.
		return &nfsv4.Readdir4res_default{Status: nfsv4.NFS4ERR_TOOSMALL}
	}
	return &res
}

func (s *sequenceState) opReadLink(ctx context.Context) nfsv4.Readlink4res {
	currentLeaf, st := s.currentFileHandle.getLeaf()
	if st != nfsv4.NFS4_OK {
		if st == nfsv4.NFS4ERR_ISDIR {
			return &nfsv4.Readlink4res_default{Status: nfsv4.NFS4ERR_INVAL}
		}
		return &nfsv4.Readlink4res_default{Status: st}
	}
	target, vs := currentLeaf.VirtualReadlink(ctx)
	if vs != virtual.StatusOK {
		return &nfsv4.Readlink4res_default{Status: toNFSv4Status(vs)}
	}
	return &nfsv4.Readlink4res_NFS4_OK{
		Resok4: nfsv4.Readlink4resok{
			Link: target,
		},
	}
}

func (s *sequenceState) opReclaimComplete(args *nfsv4.ReclaimComplete4args) nfsv4.ReclaimComplete4res {
	return nfsv4.ReclaimComplete4res{RcrStatus: nfsv4.NFS4_OK}
}

func (s *sequenceState) opRename(args *nfsv4.Rename4args) nfsv4.Rename4res {
	oldDirectory, st := s.savedFileHandle.getDirectory()
	if st != nfsv4.NFS4_OK {
		return &nfsv4.Rename4res_default{Status: st}
	}
	oldName, st := nfsv4NewComponent(args.Oldname)
	if st != nfsv4.NFS4_OK {
		return &nfsv4.Rename4res_default{Status: st}
	}
	newDirectory, st := s.currentFileHandle.getDirectory()
	if st != nfsv4.NFS4_OK {
		return &nfsv4.Rename4res_default{Status: st}
	}
	newName, st := nfsv4NewComponent(args.Newname)
	if st != nfsv4.NFS4_OK {
		return &nfsv4.Rename4res_default{Status: nfsv4.NFS4ERR_BADNAME}
	}

	oldChangeInfo, newChangeInfo, vs := oldDirectory.VirtualRename(oldName, newDirectory, newName)
	if vs != virtual.StatusOK {
		return &nfsv4.Rename4res_default{Status: toNFSv4Status(vs)}
	}
	return &nfsv4.Rename4res_NFS4_OK{
		Resok4: nfsv4.Rename4resok{
			SourceCinfo: toNFSv4ChangeInfo(&oldChangeInfo),
			TargetCinfo: toNFSv4ChangeInfo(&newChangeInfo),
		},
	}
}

func (s *sequenceState) opRemove(args *nfsv4.Remove4args) nfsv4.Remove4res {
	currentDirectory, st := s.currentFileHandle.getDirectory()
	if st != nfsv4.NFS4_OK {
		return &nfsv4.Remove4res_default{Status: st}
	}
	name, st := nfsv4NewComponent(args.Target)
	if st != nfsv4.NFS4_OK {
		return &nfsv4.Remove4res_default{Status: st}
	}

	changeInfo, vs := currentDirectory.VirtualRemove(name, true, true)
	if vs != virtual.StatusOK {
		return &nfsv4.Remove4res_default{Status: toNFSv4Status(vs)}
	}
	return &nfsv4.Remove4res_NFS4_OK{
		Resok4: nfsv4.Remove4resok{
			Cinfo: toNFSv4ChangeInfo(&changeInfo),
		},
	}
}

func (s *sequenceState) opRestoreFH() nfsv4.Restorefh4res {
	if !s.savedFileHandle.node.IsSet() {
		return nfsv4.Restorefh4res{Status: nfsv4.NFS4ERR_RESTOREFH}
	}
	s.currentFileHandle = s.savedFileHandle
	return nfsv4.Restorefh4res{Status: nfsv4.NFS4_OK}
}

func (s *sequenceState) opSaveFH() nfsv4.Savefh4res {
	_, _, st := s.currentFileHandle.getNode()
	if st == nfsv4.NFS4_OK {
		s.savedFileHandle = s.currentFileHandle
	}
	return nfsv4.Savefh4res{Status: st}
}

func (s *sequenceState) opSecInfo(ctx context.Context, args *nfsv4.Secinfo4args) nfsv4.Secinfo4res {
	// The standard states that the SECINFO operation is expected to
	// be used by the NFS client when the error value of
	// NFS4ERR_WRONGSEC is returned from another NFS operation. In
	// practice, we even see it being called if no such error was
	// returned.
	//
	// Because this NFS server is intended to be used for loopback
	// purposes only, simply announce the use of AUTH_NONE.
	currentDirectory, st := s.currentFileHandle.getDirectory()
	if st != nfsv4.NFS4_OK {
		return &nfsv4.Secinfo4res_default{Status: st}
	}
	name, st := nfsv4NewComponent(args.Name)
	if st != nfsv4.NFS4_OK {
		return &nfsv4.Secinfo4res_default{Status: st}
	}
	if _, vs := currentDirectory.VirtualLookup(ctx, name, 0, &virtual.Attributes{}); vs != virtual.StatusOK {
		return &nfsv4.Secinfo4res_default{Status: toNFSv4Status(vs)}
	}

	// PUTFH cannot return NFS4ERR_WRONGSEC when followed by
	// SECINFO{,_NO_NAME}, but it should when followed by other
	// operations such as READ. This is resolved by letting
	// SECINFO{,_NO_NAME} consume the current filehandle. This
	// requires the client to issue another PUTFH/RESTOREFH after
	// SECINFO{,_NO_NAME} to trigger the NFS4ERR_WRONGSEC intended
	// for READ.
	//
	// More details: RFC 8881, section 2.6.3.1.1.8.
	s.currentFileHandle = nfs41FileHandle{}
	return &nfsv4.Secinfo4res_NFS4_OK{
		Resok4: []nfsv4.Secinfo4{
			&nfsv4.Secinfo4_default{
				Flavor: rpcv2.AUTH_NONE,
			},
		},
	}
}

func (s *sequenceState) opSecInfoNoName(args *nfsv4.SecinfoNoName4args) nfsv4.SecinfoNoName4res {
	if _, _, st := s.currentFileHandle.getNode(); st != nfsv4.NFS4_OK {
		return &nfsv4.Secinfo4res_default{Status: st}
	}

	s.currentFileHandle = nfs41FileHandle{}
	return &nfsv4.Secinfo4res_NFS4_OK{
		Resok4: []nfsv4.Secinfo4{
			&nfsv4.Secinfo4_default{
				Flavor: rpcv2.AUTH_NONE,
			},
		},
	}
}

func (s *sequenceState) opSequence(args *nfsv4.Sequence4args) nfsv4.Sequence4res {
	return &nfsv4.Sequence4res_default{SrStatus: nfsv4.NFS4ERR_SEQUENCE_POS}
}

func (s *sequenceState) opSetAttr(ctx context.Context, args *nfsv4.Setattr4args) nfsv4.Setattr4res {
	// TODO: Respect the state ID, if provided!
	currentNode, _, st := s.currentFileHandle.getNode()
	if st != nfsv4.NFS4_OK {
		return nfsv4.Setattr4res{Status: st}
	}
	var attributes virtual.Attributes
	if st := fattr4ToAttributes(&args.ObjAttributes, &attributes); st != nfsv4.NFS4_OK {
		return nfsv4.Setattr4res{Status: st}
	}
	if vs := currentNode.VirtualSetAttributes(ctx, &attributes, 0, &virtual.Attributes{}); vs != virtual.StatusOK {
		return nfsv4.Setattr4res{Status: toNFSv4Status(vs)}
	}
	return nfsv4.Setattr4res{
		Status:   st,
		Attrsset: args.ObjAttributes.Attrmask,
	}
}

func (s *sequenceState) opSetSSV(args *nfsv4.SetSsv4args) nfsv4.SetSsv4res {
	// EXCHANGE_ID always negotiates SP4_NONE, so there should be no
	// need for clients to call SET_SSV.
	return &nfsv4.SetSsv4res_default{SsrStatus: nfsv4.NFS4ERR_INVAL}
}

func (s *sequenceState) opTestStateID(args *nfsv4.TestStateid4args) nfsv4.TestStateid4res {
	cis := s.session.clientIncarnation
	cis.lock.RLock()
	defer cis.lock.RUnlock()

	statusCodes := make([]nfsv4.Nfsstat4, 0, len(args.TsStateids))
	for _, rawStateID := range args.TsStateids {
		statusCodes = append(statusCodes, cis.testStateID(&rawStateID))
	}
	return &nfsv4.TestStateid4res_NFS4_OK{
		TsrResok4: nfsv4.TestStateid4resok{
			TsrStatusCodes: statusCodes,
		},
	}
}

func (s *sequenceState) opVerify(ctx context.Context, args *nfsv4.Verify4args) nfsv4.Verify4res {
	if st := s.verifyAttributes(ctx, &args.ObjAttributes); st != nfsv4.NFS4ERR_SAME {
		return nfsv4.Verify4res{Status: st}
	}
	return nfsv4.Verify4res{Status: nfsv4.NFS4_OK}
}

func (s *sequenceState) opWantDelegation(args *nfsv4.WantDelegation4args) nfsv4.WantDelegation4res {
	return &nfsv4.WantDelegation4res_default{WdrStatus: nfsv4.NFS4ERR_NOTSUPP}
}

func (s *sequenceState) opWrite(ctx context.Context, args *nfsv4.Write4args) nfsv4.Write4res {
	currentLeaf, cleanup, st := s.getOpenedLeaf(ctx, &args.Stateid, virtual.ShareMaskWrite)
	if st != nfsv4.NFS4_OK {
		return &nfsv4.Write4res_default{Status: st}
	}
	defer cleanup()

	n, vs := currentLeaf.VirtualWrite(args.Data, args.Offset)
	if vs != virtual.StatusOK {
		return &nfsv4.Write4res_default{Status: toNFSv4Status(vs)}
	}
	return &nfsv4.Write4res_NFS4_OK{
		Resok4: nfsv4.Write4resok{
			Count:     nfsv4.Count4(n),
			Committed: nfsv4.FILE_SYNC4,
			Writeverf: s.program.rebootVerifier,
		},
	}
}

// nfs41ShareAccessToShareMask converts NFSv4 share_access values that
// are part of OPEN and OPEN_DOWNGRADE requests to our equivalent
// ShareMask values.
func nfs41ShareAccessToShareMask(in uint32) (virtual.ShareMask, nfsv4.Nfsstat4) {
	switch in &^ nfsv4.OPEN4_SHARE_ACCESS_WANT_DELEG_MASK {
	case nfsv4.OPEN4_SHARE_ACCESS_READ:
		return virtual.ShareMaskRead, nfsv4.NFS4_OK
	case nfsv4.OPEN4_SHARE_ACCESS_WRITE:
		return virtual.ShareMaskWrite, nfsv4.NFS4_OK
	case nfsv4.OPEN4_SHARE_ACCESS_BOTH:
		return virtual.ShareMaskRead | virtual.ShareMaskWrite, nfsv4.NFS4_OK
	default:
		return 0, nfsv4.NFS4ERR_INVAL
	}
}

// nfs41CompareStateSeqID compares a client-provided sequence ID value
// with one present on the server. The error that needs to be returned
// in case of non-matching sequence IDs depends on whether the value
// lies in the past or future.
//
// NFSv4.0 allowed the sequence ID to drift by at most 2^31, while
// NFSv4.1 tightened that to be equal to the number of slots.
// Unfortunately this is not correct, as there is no limit to how many
// times a sequence ID is incremented as part of a single COMPOUND
// request. Stick to NFSv4.0's semantics.
//
// More details:
// - RFC 7530, section 9.1.3, last paragraph.
// - RFC 8881, section 8.2.2, last paragraph.
func nfs41CompareStateSeqID(clientValue, serverValue uint32) nfsv4.Nfsstat4 {
	if clientValue == 0 || clientValue == serverValue {
		return nfsv4.NFS4_OK
	}
	if int32(clientValue-serverValue) > 0 {
		return nfsv4.NFS4ERR_BAD_STATEID
	}
	return nfsv4.NFS4ERR_OLD_STATEID
}
