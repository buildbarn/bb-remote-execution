package nfsv4

import (
	"bytes"
	"context"
	"io"
	"math"
	"sync"
	"time"

	"github.com/buildbarn/bb-remote-execution/pkg/filesystem/virtual"
	"github.com/buildbarn/bb-storage/pkg/clock"
	"github.com/buildbarn/bb-storage/pkg/filesystem"
	"github.com/buildbarn/bb-storage/pkg/filesystem/path"
	"github.com/buildbarn/bb-storage/pkg/random"
	"github.com/buildbarn/go-xdr/pkg/protocols/nfsv4"
	"github.com/buildbarn/go-xdr/pkg/protocols/rpcv2"
	"github.com/buildbarn/go-xdr/pkg/runtime"
	"github.com/prometheus/client_golang/prometheus"
)

// stateIDOtherPrefixLength is the number of bytes of a state ID's
// 'other' field that are set to a constant value. This permits the
// server to detect whether state IDs belong to a previous incarnation
// of the server.
const stateIDOtherPrefixLength = 4

var (
	baseProgramPrometheusMetrics sync.Once

	baseProgramOpenOwnersCreated = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: "buildbarn",
			Subsystem: "nfsv4",
			Name:      "base_program_open_owners_created_total",
			Help:      "Number of open-owners created through NFSv4 OPEN operations.",
		})
	baseProgramOpenOwnersRemoved = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: "buildbarn",
			Subsystem: "nfsv4",
			Name:      "base_program_open_owners_removed_total",
			Help:      "Number of open-owners removed due to inactivity.",
		})
)

type baseProgram struct {
	rootFileHandle     fileHandle
	handleResolver     virtual.HandleResolver
	rebootVerifier     nfsv4.Verifier4
	stateIDOtherPrefix [stateIDOtherPrefixLength]byte
	clock              clock.Clock
	enforcedLeaseTime  time.Duration
	announcedLeaseTime nfsv4.NfsLease4

	lock                         sync.Mutex
	now                          time.Time
	randomNumberGenerator        random.SingleThreadedGenerator
	clientsByLongID              map[string]*clientState
	clientConfirmationsByKey     map[clientConfirmationKey]*clientConfirmationState
	clientConfirmationsByShortID map[nfsv4.Clientid4]*clientConfirmationState
	openOwnerFilesByOther        map[regularStateIDOther]*openOwnerFileState
	openedFilesByHandle          map[string]*openedFileState
	lockOwnerFilesByOther        map[regularStateIDOther]*lockOwnerFileState
	idleClientConfirmations      clientConfirmationState
	unusedOpenOwners             openOwnerState
}

// NewBaseProgram creates an nfsv4.Nfs4Program that forwards all
// operations to a virtual file system. It implements most of the
// features of NFSv4.0.
func NewBaseProgram(rootDirectory virtual.Directory, handleResolver virtual.HandleResolver, randomNumberGenerator random.SingleThreadedGenerator, rebootVerifier nfsv4.Verifier4, stateIDOtherPrefix [stateIDOtherPrefixLength]byte, clock clock.Clock, enforcedLeaseTime, announcedLeaseTime time.Duration) nfsv4.Nfs4Program {
	baseProgramPrometheusMetrics.Do(func() {
		prometheus.MustRegister(baseProgramOpenOwnersCreated)
		prometheus.MustRegister(baseProgramOpenOwnersRemoved)
	})

	var attributes virtual.Attributes
	rootDirectory.VirtualGetAttributes(virtual.AttributesMaskFileHandle, &attributes)
	p := &baseProgram{
		rootFileHandle: fileHandle{
			handle:    attributes.GetFileHandle(),
			directory: rootDirectory,
		},
		handleResolver:     handleResolver,
		rebootVerifier:     rebootVerifier,
		stateIDOtherPrefix: stateIDOtherPrefix,
		clock:              clock,
		enforcedLeaseTime:  enforcedLeaseTime,
		announcedLeaseTime: nfsv4.NfsLease4(announcedLeaseTime.Seconds()),

		randomNumberGenerator:        randomNumberGenerator,
		clientsByLongID:              map[string]*clientState{},
		clientConfirmationsByKey:     map[clientConfirmationKey]*clientConfirmationState{},
		clientConfirmationsByShortID: map[nfsv4.Clientid4]*clientConfirmationState{},
		openOwnerFilesByOther:        map[regularStateIDOther]*openOwnerFileState{},
		openedFilesByHandle:          map[string]*openedFileState{},
		lockOwnerFilesByOther:        map[regularStateIDOther]*lockOwnerFileState{},
	}
	p.idleClientConfirmations.previousIdle = &p.idleClientConfirmations
	p.idleClientConfirmations.nextIdle = &p.idleClientConfirmations
	p.unusedOpenOwners.previousUnused = &p.unusedOpenOwners
	p.unusedOpenOwners.nextUnused = &p.unusedOpenOwners
	return p
}

func (*baseProgram) NfsV4Nfsproc4Null(ctx context.Context) error {
	return nil
}

func (p *baseProgram) NfsV4Nfsproc4Compound(ctx context.Context, arguments *nfsv4.Compound4args) (*nfsv4.Compound4res, error) {
	// Create compound state and process all operations sequentially
	// against it.
	state := compoundState{program: p}
	resarray := make([]nfsv4.NfsResop4, 0, len(arguments.Argarray))
	status := nfsv4.NFS4_OK
	for _, operation := range arguments.Argarray {
		switch op := operation.(type) {
		case *nfsv4.NfsArgop4_OP_ACCESS:
			res := state.opAccess(&op.Opaccess)
			resarray = append(resarray, &nfsv4.NfsResop4_OP_ACCESS{
				Opaccess: res,
			})
			status = res.GetStatus()
		case *nfsv4.NfsArgop4_OP_CLOSE:
			res := state.opClose(&op.Opclose)
			resarray = append(resarray, &nfsv4.NfsResop4_OP_CLOSE{
				Opclose: res,
			})
			status = res.GetStatus()
		case *nfsv4.NfsArgop4_OP_COMMIT:
			res := state.opCommit(&op.Opcommit)
			resarray = append(resarray, &nfsv4.NfsResop4_OP_COMMIT{
				Opcommit: res,
			})
			status = res.GetStatus()
		case *nfsv4.NfsArgop4_OP_CREATE:
			res := state.opCreate(&op.Opcreate)
			resarray = append(resarray, &nfsv4.NfsResop4_OP_CREATE{
				Opcreate: res,
			})
			status = res.GetStatus()
		case *nfsv4.NfsArgop4_OP_DELEGPURGE:
			res := state.opDelegpurge(&op.Opdelegpurge)
			resarray = append(resarray, &nfsv4.NfsResop4_OP_DELEGPURGE{
				Opdelegpurge: res,
			})
			status = res.Status
		case *nfsv4.NfsArgop4_OP_DELEGRETURN:
			res := state.opDelegreturn(&op.Opdelegreturn)
			resarray = append(resarray, &nfsv4.NfsResop4_OP_DELEGRETURN{
				Opdelegreturn: res,
			})
			status = res.Status
		case *nfsv4.NfsArgop4_OP_GETATTR:
			res := state.opGetattr(&op.Opgetattr)
			resarray = append(resarray, &nfsv4.NfsResop4_OP_GETATTR{
				Opgetattr: res,
			})
			status = res.GetStatus()
		case *nfsv4.NfsArgop4_OP_GETFH:
			res := state.opGetfh()
			resarray = append(resarray, &nfsv4.NfsResop4_OP_GETFH{
				Opgetfh: res,
			})
			status = res.GetStatus()
		case *nfsv4.NfsArgop4_OP_LINK:
			res := state.opLink(&op.Oplink)
			resarray = append(resarray, &nfsv4.NfsResop4_OP_LINK{
				Oplink: res,
			})
			status = res.GetStatus()
		case *nfsv4.NfsArgop4_OP_LOCK:
			res := state.opLock(&op.Oplock)
			resarray = append(resarray, &nfsv4.NfsResop4_OP_LOCK{
				Oplock: res,
			})
			status = res.GetStatus()
		case *nfsv4.NfsArgop4_OP_LOCKT:
			res := state.opLockt(&op.Oplockt)
			resarray = append(resarray, &nfsv4.NfsResop4_OP_LOCKT{
				Oplockt: res,
			})
			status = res.GetStatus()
		case *nfsv4.NfsArgop4_OP_LOCKU:
			res := state.opLocku(&op.Oplocku)
			resarray = append(resarray, &nfsv4.NfsResop4_OP_LOCKU{
				Oplocku: res,
			})
			status = res.GetStatus()
		case *nfsv4.NfsArgop4_OP_LOOKUP:
			res := state.opLookup(&op.Oplookup)
			resarray = append(resarray, &nfsv4.NfsResop4_OP_LOOKUP{
				Oplookup: res,
			})
			status = res.Status
		case *nfsv4.NfsArgop4_OP_LOOKUPP:
			res := state.opLookupp()
			resarray = append(resarray, &nfsv4.NfsResop4_OP_LOOKUPP{
				Oplookupp: res,
			})
			status = res.Status
		case *nfsv4.NfsArgop4_OP_NVERIFY:
			res := state.opNverify(&op.Opnverify)
			resarray = append(resarray, &nfsv4.NfsResop4_OP_NVERIFY{
				Opnverify: res,
			})
			status = res.Status
		case *nfsv4.NfsArgop4_OP_OPEN:
			res := state.opOpen(&op.Opopen)
			resarray = append(resarray, &nfsv4.NfsResop4_OP_OPEN{
				Opopen: res,
			})
			status = res.GetStatus()
		case *nfsv4.NfsArgop4_OP_OPENATTR:
			res := state.opOpenattr(&op.Opopenattr)
			resarray = append(resarray, &nfsv4.NfsResop4_OP_OPENATTR{
				Opopenattr: res,
			})
			status = res.Status
		case *nfsv4.NfsArgop4_OP_OPEN_CONFIRM:
			res := state.opOpenConfirm(&op.OpopenConfirm)
			resarray = append(resarray, &nfsv4.NfsResop4_OP_OPEN_CONFIRM{
				OpopenConfirm: res,
			})
			status = res.GetStatus()
		case *nfsv4.NfsArgop4_OP_OPEN_DOWNGRADE:
			res := state.opOpenDowngrade(&op.OpopenDowngrade)
			resarray = append(resarray, &nfsv4.NfsResop4_OP_OPEN_DOWNGRADE{
				OpopenDowngrade: res,
			})
			status = res.GetStatus()
		case *nfsv4.NfsArgop4_OP_PUTFH:
			res := state.opPutfh(&op.Opputfh)
			resarray = append(resarray, &nfsv4.NfsResop4_OP_PUTFH{
				Opputfh: res,
			})
			status = res.Status
		case *nfsv4.NfsArgop4_OP_PUTPUBFH:
			res := state.opPutpubfh()
			resarray = append(resarray, &nfsv4.NfsResop4_OP_PUTPUBFH{
				Opputpubfh: res,
			})
			status = res.Status
		case *nfsv4.NfsArgop4_OP_PUTROOTFH:
			res := state.opPutrootfh()
			resarray = append(resarray, &nfsv4.NfsResop4_OP_PUTROOTFH{
				Opputrootfh: res,
			})
			status = res.Status
		case *nfsv4.NfsArgop4_OP_READ:
			res := state.opRead(&op.Opread)
			resarray = append(resarray, &nfsv4.NfsResop4_OP_READ{
				Opread: res,
			})
			status = res.GetStatus()
		case *nfsv4.NfsArgop4_OP_READDIR:
			res := state.opReaddir(&op.Opreaddir)
			resarray = append(resarray, &nfsv4.NfsResop4_OP_READDIR{
				Opreaddir: res,
			})
			status = res.GetStatus()
		case *nfsv4.NfsArgop4_OP_READLINK:
			res := state.opReadlink()
			resarray = append(resarray, &nfsv4.NfsResop4_OP_READLINK{
				Opreadlink: res,
			})
			status = res.GetStatus()
		case *nfsv4.NfsArgop4_OP_RELEASE_LOCKOWNER:
			res := state.opReleaseLockowner(&op.OpreleaseLockowner)
			resarray = append(resarray, &nfsv4.NfsResop4_OP_RELEASE_LOCKOWNER{
				OpreleaseLockowner: res,
			})
			status = res.Status
		case *nfsv4.NfsArgop4_OP_REMOVE:
			res := state.opRemove(&op.Opremove)
			resarray = append(resarray, &nfsv4.NfsResop4_OP_REMOVE{
				Opremove: res,
			})
			status = res.GetStatus()
		case *nfsv4.NfsArgop4_OP_RENAME:
			res := state.opRename(&op.Oprename)
			resarray = append(resarray, &nfsv4.NfsResop4_OP_RENAME{
				Oprename: res,
			})
			status = res.GetStatus()
		case *nfsv4.NfsArgop4_OP_RENEW:
			res := state.opRenew(&op.Oprenew)
			resarray = append(resarray, &nfsv4.NfsResop4_OP_RENEW{
				Oprenew: res,
			})
			status = res.Status
		case *nfsv4.NfsArgop4_OP_RESTOREFH:
			res := state.opRestorefh()
			resarray = append(resarray, &nfsv4.NfsResop4_OP_RESTOREFH{
				Oprestorefh: res,
			})
			status = res.Status
		case *nfsv4.NfsArgop4_OP_SAVEFH:
			res := state.opSavefh()
			resarray = append(resarray, &nfsv4.NfsResop4_OP_SAVEFH{
				Opsavefh: res,
			})
			status = res.Status
		case *nfsv4.NfsArgop4_OP_SECINFO:
			res := state.opSecinfo(&op.Opsecinfo)
			resarray = append(resarray, &nfsv4.NfsResop4_OP_SECINFO{
				Opsecinfo: res,
			})
			status = res.GetStatus()
		case *nfsv4.NfsArgop4_OP_SETATTR:
			res := state.opSetattr(&op.Opsetattr)
			resarray = append(resarray, &nfsv4.NfsResop4_OP_SETATTR{
				Opsetattr: res,
			})
			status = res.Status
		case *nfsv4.NfsArgop4_OP_SETCLIENTID:
			res := state.opSetclientid(&op.Opsetclientid)
			resarray = append(resarray, &nfsv4.NfsResop4_OP_SETCLIENTID{
				Opsetclientid: res,
			})
			status = res.GetStatus()
		case *nfsv4.NfsArgop4_OP_SETCLIENTID_CONFIRM:
			res := state.opSetclientidConfirm(&op.OpsetclientidConfirm)
			resarray = append(resarray, &nfsv4.NfsResop4_OP_SETCLIENTID_CONFIRM{
				OpsetclientidConfirm: res,
			})
			status = res.Status
		case *nfsv4.NfsArgop4_OP_VERIFY:
			res := state.opVerify(&op.Opverify)
			resarray = append(resarray, &nfsv4.NfsResop4_OP_VERIFY{
				Opverify: res,
			})
			status = res.Status
		case *nfsv4.NfsArgop4_OP_WRITE:
			res := state.opWrite(&op.Opwrite)
			resarray = append(resarray, &nfsv4.NfsResop4_OP_WRITE{
				Opwrite: res,
			})
			status = res.GetStatus()
		default:
			res := nfsv4.Illegal4res{Status: nfsv4.NFS4ERR_OP_ILLEGAL}
			resarray = append(resarray, &nfsv4.NfsResop4_OP_ILLEGAL{
				Opillegal: res,
			})
			status = res.Status
		}
		if status != nfsv4.NFS4_OK {
			// Terminate evaluation of further operations
			// upon failure.
			break
		}
	}
	return &nfsv4.Compound4res{
		Status:   status,
		Tag:      arguments.Tag,
		Resarray: resarray,
	}, nil
}

// enter acquires the lock on the NFSv4 server. After acquiring the
// lock, it cleans up state belonging to clients and open-owners that
// have stopped contacting the server.
func (p *baseProgram) enter() {
	for {
		now := p.clock.Now()
		p.lock.Lock()
		if p.now.Before(now) {
			p.now = now
		}

		// Remove clients that have not renewed their state in
		// some time. Close all of the files and release all
		// locks owned by these clients.
		var ll leavesToClose
		minimumLastSeen := p.now.Add(-p.enforcedLeaseTime)
		for p.idleClientConfirmations.nextIdle != &p.idleClientConfirmations && p.idleClientConfirmations.nextIdle.lastSeen.Before(minimumLastSeen) {
			p.idleClientConfirmations.nextIdle.remove(p, &ll)
		}

		// Remove open-owners that no longer have any open files
		// associated with them, or are unconfirmed, and have
		// not been used for some time. If the client decides to
		// use the same open-owner once again, the next OPEN
		// operation will need to be confirmed using
		// OPEN_CONFIRM.
		for p.unusedOpenOwners.nextUnused != &p.unusedOpenOwners && p.unusedOpenOwners.nextUnused.lastUsed.Before(minimumLastSeen) {
			p.unusedOpenOwners.nextUnused.remove(p, &ll)
		}

		// If the code above ended up yielding files that need
		// to be closed, we close the files and retry.
		if ll.empty() {
			return
		}
		p.lock.Unlock()
		ll.closeAll()
	}
}

func (p *baseProgram) leave() {
	p.lock.Unlock()
}

// getConfirmedClientByShortID looks up a confirmed client by short
// client ID.
func (p *baseProgram) getConfirmedClientByShortID(shortID nfsv4.Clientid4) (*confirmedClientState, nfsv4.Nfsstat4) {
	clientConfirmation, ok := p.clientConfirmationsByShortID[shortID]
	if !ok {
		return nil, nfsv4.NFS4ERR_STALE_CLIENTID
	}
	confirmedClient := clientConfirmation.client.confirmed
	if confirmedClient == nil || confirmedClient.confirmation != clientConfirmation {
		return nil, nfsv4.NFS4ERR_STALE_CLIENTID
	}
	return confirmedClient, nfsv4.NFS4_OK
}

// getOpenOwnerByOtherForTransaction looks up an open-owner by
// open-owner state ID. It waits for any existing transactions to
// complete. This makes it possible to start a new transaction.
func (p *baseProgram) getOpenOwnerByOtherForTransaction(other regularStateIDOther) (*openOwnerState, nfsv4.Nfsstat4) {
	for {
		oofs, ok := p.openOwnerFilesByOther[other]
		if !ok {
			return nil, nfsv4.NFS4ERR_BAD_STATEID
		}
		if oos := oofs.openOwner; oos.waitForCurrentTransactionCompletion(p) {
			return oos, nfsv4.NFS4_OK
		}
	}
}

// getLockOwnerByOtherForTransaction looks up a lock-owner by lock-owner
// state ID, for the purpose of starting a new transaction.
//
// Unlike getOpenOwnerByOtherForTransaction() it does not need to wait
// for other transactions to complete, as we don't need to support any
// blocking operations against locks.
func (p *baseProgram) getLockOwnerByOtherForTransaction(other regularStateIDOther) (*lockOwnerState, nfsv4.Nfsstat4) {
	lofs, ok := p.lockOwnerFilesByOther[other]
	if !ok {
		return nil, nfsv4.NFS4ERR_BAD_STATEID
	}
	return lofs.lockOwner, nfsv4.NFS4_OK
}

// newRegularStateID allocates a new open-owner or lock-owner state ID.
func (p *baseProgram) newRegularStateID(seqID nfsv4.Seqid4) (stateID regularStateID) {
	stateID.seqID = seqID
	p.randomNumberGenerator.Read(stateID.other[:])
	return
}

// internalizeStateID converts a state ID that's provided as part of a
// request to the format that's used internally.
//
// This method returns a nil state ID when the provided state ID is
// special (i.e., an anonymous state ID or READ bypass state ID).
func (p *baseProgram) internalizeStateID(stateID *nfsv4.Stateid4) (*regularStateID, nfsv4.Nfsstat4) {
	switch stateID.Other {
	case [nfsv4.NFS4_OTHER_SIZE]byte{}:
		// Anonymous state ID.
		if stateID.Seqid != 0 {
			return nil, nfsv4.NFS4ERR_BAD_STATEID
		}
		return nil, nfsv4.NFS4_OK
	case [...]byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff}:
		// READ bypass state ID.
		if stateID.Seqid != 0xffffffff {
			return nil, nfsv4.NFS4ERR_BAD_STATEID
		}
		return nil, nfsv4.NFS4_OK
	default:
		// Regular state ID. Only permit state IDs with a given
		// prefix, so that we can accurately distinguish between
		// NFS4ERR_STATE_STATEID and NFS4ERR_BAD_STATEID.
		var prefix [stateIDOtherPrefixLength]byte
		copy(prefix[:], stateID.Other[:])
		if prefix != p.stateIDOtherPrefix {
			// State ID is from before a reboot/restart.
			return nil, nfsv4.NFS4ERR_STALE_STATEID
		}
		internalStateID := &regularStateID{seqID: stateID.Seqid}
		copy(internalStateID.other[:], stateID.Other[stateIDOtherPrefixLength:])
		return internalStateID, nfsv4.NFS4_OK
	}
}

// internalizeRegularStateID is identical to internalizeStateID, except
// that it denies the use of special state IDs.
func (p *baseProgram) internalizeRegularStateID(stateID *nfsv4.Stateid4) (regularStateID, nfsv4.Nfsstat4) {
	internalStateID, st := p.internalizeStateID(stateID)
	if st != nfsv4.NFS4_OK {
		return regularStateID{}, st
	}
	if internalStateID == nil {
		return regularStateID{}, nfsv4.NFS4ERR_BAD_STATEID
	}
	return *internalStateID, nfsv4.NFS4_OK
}

// externalizeStateID converts a regular state ID that's encoded in the
// internal format to the format used by the NFSv4 protocol.
func (p *baseProgram) externalizeStateID(stateID regularStateID) nfsv4.Stateid4 {
	externalStateID := nfsv4.Stateid4{Seqid: stateID.seqID}
	copy(externalStateID.Other[:], p.stateIDOtherPrefix[:])
	copy(externalStateID.Other[stateIDOtherPrefixLength:], stateID.other[:])
	return externalStateID
}

// writeAttributes converts file attributes returned by the virtual file
// system into the NFSv4 wire format. It also returns a bitmask
// indicating which attributes were actually emitted.
func (p *baseProgram) writeAttributes(attributes *virtual.Attributes, attrRequest nfsv4.Bitmap4, w io.Writer) nfsv4.Bitmap4 {
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
			deterministicNfstime4.WriteTo(w)
		}
		attrMask[1] = s
	}
	return attrMask
}

// attributesToFattr4 converts attributes returned by the virtual file
// system layer to an NFSv4 fattr4 structure. As required by the
// protocol, attributes are stored in the order of the FATTR4_*
// constants.
func (p *baseProgram) attributesToFattr4(attributes *virtual.Attributes, attrRequest nfsv4.Bitmap4) nfsv4.Fattr4 {
	w := bytes.NewBuffer(nil)
	attrMask := p.writeAttributes(attributes, attrRequest, w)
	return nfsv4.Fattr4{
		Attrmask: attrMask,
		AttrVals: w.Bytes(),
	}
}

// regularStateID is an internal representation of non-special
// open-owner or lock-owner state IDs.
type regularStateID struct {
	seqID nfsv4.Seqid4
	other regularStateIDOther
}

// regularStateIDOther is an internal representation of the 'other'
// field of non-special open-owner or lock-owner state IDs.
type regularStateIDOther [nfsv4.NFS4_OTHER_SIZE - stateIDOtherPrefixLength]byte

// compoundState contains the state that needs to be tracked during the
// lifetime of a single NFSv4 COMPOUND procedure. It provides
// implementations of each of the operations contained in the COMPOUND
// procedure.
type compoundState struct {
	program *baseProgram

	currentFileHandle fileHandle
	savedFileHandle   fileHandle
}

// getOpenOwnerFileByStateID obtains an open-owner file by open state
// ID. It also checks whether the open state ID corresponds to the
// current file handle, and that the client provided sequence ID matches
// the server's value.
func (s *compoundState) getOpenOwnerFileByStateID(stateID regularStateID, allowUnconfirmed bool) (*openOwnerFileState, nfsv4.Nfsstat4) {
	p := s.program
	oofs, ok := p.openOwnerFilesByOther[stateID.other]
	if !ok {
		return nil, nfsv4.NFS4ERR_BAD_STATEID
	}
	if s.currentFileHandle.leaf == nil && s.currentFileHandle.directory == nil {
		return nil, nfsv4.NFS4ERR_NOFILEHANDLE
	}
	if oofs.useCount == 0 {
		// File has already been closed.
		return nil, nfsv4.NFS4ERR_BAD_STATEID
	}
	if !bytes.Equal(s.currentFileHandle.handle, oofs.openedFile.handle) {
		return nil, nfsv4.NFS4ERR_BAD_STATEID
	}
	if !oofs.openOwner.confirmed && !allowUnconfirmed {
		// The state ID was returned by a previous OPEN call
		// that still requires a call to OPEN_CONFIRM. We should
		// treat the state ID as non-existent until OPEN_CONFIRM
		// has been called.
		//
		// More details: RFC 7530, section 16.18.5, paragraph 6.
		return nil, nfsv4.NFS4ERR_BAD_STATEID
	}
	if st := compareStateSeqID(stateID.seqID, oofs.stateID.seqID); st != nfsv4.NFS4_OK {
		return nil, st
	}
	return oofs, nfsv4.NFS4_OK
}

// getLockOwnerFileByStateID obtains a lock-owner file by state ID. It
// also checks whether the lock state ID corresponds to the current file
// handle, and that the client provided sequence ID matches the server's
// value.
func (s *compoundState) getLockOwnerFileByStateID(stateID regularStateID) (*lockOwnerFileState, nfsv4.Nfsstat4) {
	p := s.program
	lofs, ok := p.lockOwnerFilesByOther[stateID.other]
	if !ok {
		return nil, nfsv4.NFS4ERR_BAD_STATEID
	}
	if s.currentFileHandle.leaf == nil && s.currentFileHandle.directory == nil {
		return nil, nfsv4.NFS4ERR_NOFILEHANDLE
	}
	if !bytes.Equal(s.currentFileHandle.handle, lofs.openOwnerFile.openedFile.handle) {
		return nil, nfsv4.NFS4ERR_BAD_STATEID
	}
	if st := compareStateSeqID(stateID.seqID, lofs.stateID.seqID); st != nfsv4.NFS4_OK {
		return nil, st
	}
	return lofs, nfsv4.NFS4_OK
}

// getOpenedLeaf is used by READ and WRITE operations to obtain an
// opened leaf corresponding to a file handle and open-owner state ID.
//
// When a special state ID is provided, it ensures the file is
// temporarily opened for the duration of the operation. When a
// non-special state ID is provided, it ensures that the file was
// originally opened with the correct share access mask.
func (s *compoundState) getOpenedLeaf(stateID *nfsv4.Stateid4, shareAccess virtual.ShareMask) (virtual.Leaf, func(), nfsv4.Nfsstat4) {
	p := s.program
	internalStateID, st := p.internalizeStateID(stateID)
	if st != nfsv4.NFS4_OK {
		return nil, nil, st
	}

	if internalStateID == nil {
		// Client provided the anonymous state ID or READ bypass
		// state ID. Temporarily open the file to perform the
		// operation.
		currentLeaf, st := s.currentFileHandle.getLeaf()
		if st != nfsv4.NFS4_OK {
			return nil, nil, st
		}
		if vs := currentLeaf.VirtualOpenSelf(
			shareAccess,
			&virtual.OpenExistingOptions{},
			0,
			&virtual.Attributes{},
		); vs != virtual.StatusOK {
			return nil, nil, toNFSv4Status(vs)
		}
		return currentLeaf, func() { currentLeaf.VirtualClose(1) }, nfsv4.NFS4_OK
	}

	p.enter()
	defer p.leave()

	oofs, st := s.getOpenOwnerFileByStateID(*internalStateID, false)
	switch st {
	case nfsv4.NFS4_OK:
		if shareAccess&^oofs.shareAccess != 0 {
			// Attempting to write to a file opened for
			// reading, or vice versa.
			return nil, nil, nfsv4.NFS4ERR_OPENMODE
		}
	case nfsv4.NFS4ERR_BAD_STATEID:
		// Client may have provided a lock state ID.
		lofs, st := s.getLockOwnerFileByStateID(*internalStateID)
		if st != nfsv4.NFS4_OK {
			return nil, nil, st
		}
		oofs = lofs.openOwnerFile
		if shareAccess&^lofs.shareAccess != 0 {
			// Attempted to write to a file that was opened
			// for reading at the time the lock-owner state
			// was established, or vice versa.
			//
			// More details: RFC 7530, section 9.1.6,
			// paragraph 7.
			return nil, nil, nfsv4.NFS4ERR_OPENMODE
		}
	default:
		return nil, nil, st
	}

	// Ensure that both the client and file are not released while
	// the I/O operation is taking place.
	clientConfirmation := oofs.openOwner.confirmedClient.confirmation
	clientConfirmation.hold(p)
	oofs.useCount.increase()
	return oofs.openedFile.leaf, func() {
		var ll leavesToClose
		p.enter()
		oofs.maybeClose(&ll)
		clientConfirmation.release(p)
		p.leave()
		ll.closeAll()
	}, nfsv4.NFS4_OK
}

// verifyAttributes is the common implementation of the VERIFY and
// NVERIFY operations.
func (s *compoundState) verifyAttributes(fattr *nfsv4.Fattr4) nfsv4.Nfsstat4 {
	currentNode, _, st := s.currentFileHandle.getNode()
	if st != nfsv4.NFS4_OK {
		return st
	}

	// Request attributes of the file. Don't actually store them in
	// a fattr4 structure. Use comparingWriter to check whether the
	// generated attributes are equal to the ones provided.
	attrRequest := fattr.Attrmask
	var attributes virtual.Attributes
	currentNode.VirtualGetAttributes(attrRequestToAttributesMask(attrRequest), &attributes)
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

func (s *compoundState) opAccess(args *nfsv4.Access4args) nfsv4.Access4res {
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
	currentNode.VirtualGetAttributes(virtual.AttributesMaskPermissions, &attributes)
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

func (s *compoundState) opClose(args *nfsv4.Close4args) nfsv4.Close4res {
	p := s.program
	openStateID, st := p.internalizeRegularStateID(&args.OpenStateid)
	if st != nfsv4.NFS4_OK {
		return &nfsv4.Close4res_default{Status: st}
	}

	var ll leavesToClose
	defer ll.closeAll()

	p.enter()
	defer p.leave()

	oos, st := p.getOpenOwnerByOtherForTransaction(openStateID.other)
	if st != nfsv4.NFS4_OK {
		return &nfsv4.Close4res_default{Status: st}
	}
	transaction, lastResponse, st := oos.startTransaction(p, args.Seqid, &ll, unconfirmedOpenOwnerPolicyDeny)
	if st != nfsv4.NFS4_OK {
		if r, ok := lastResponse.(nfsv4.Close4res); ok {
			return r
		}
		return &nfsv4.Close4res_default{Status: st}
	}
	response, closedFile := s.txClose(openStateID, &ll)
	transaction.complete(&openOwnerLastResponse{
		response:   response,
		closedFile: closedFile,
	})
	return response
}

func (s *compoundState) txClose(openStateID regularStateID, ll *leavesToClose) (nfsv4.Close4res, *openOwnerFileState) {
	oofs, st := s.getOpenOwnerFileByStateID(openStateID, false)
	if st != nfsv4.NFS4_OK {
		return &nfsv4.Close4res_default{Status: st}, nil
	}

	// Only half-close the file, so that the state ID remains valid
	// for doing replays of the CLOSE request.
	//
	// More details: RFC 7530, section 9.10.1.
	p := s.program
	oofs.removeStart(p, ll)
	oofs.stateID.seqID = nextSeqID(oofs.stateID.seqID)

	return &nfsv4.Close4res_NFS4_OK{
		OpenStateid: p.externalizeStateID(oofs.stateID),
	}, oofs
}

func (s *compoundState) opCommit(args *nfsv4.Commit4args) nfsv4.Commit4res {
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

func (s *compoundState) opCreate(args *nfsv4.Create4args) nfsv4.Create4res {
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
	var fileHandle fileHandle
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
		fileHandle.directory = directory
	case *nfsv4.Createtype4_NF4FIFO:
		var leaf virtual.Leaf
		leaf, changeInfo, vs = currentDirectory.VirtualMknod(name, filesystem.FileTypeFIFO, virtual.AttributesMaskFileHandle, &attributes)
		fileHandle.leaf = leaf
	case *nfsv4.Createtype4_NF4LNK:
		var leaf virtual.Leaf
		leaf, changeInfo, vs = currentDirectory.VirtualSymlink(objectType.Linkdata, name, virtual.AttributesMaskFileHandle, &attributes)
		fileHandle.leaf = leaf
	case *nfsv4.Createtype4_NF4SOCK:
		var leaf virtual.Leaf
		leaf, changeInfo, vs = currentDirectory.VirtualMknod(name, filesystem.FileTypeSocket, virtual.AttributesMaskFileHandle, &attributes)
		fileHandle.leaf = leaf
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

func (s *compoundState) opDelegpurge(args *nfsv4.Delegpurge4args) nfsv4.Delegpurge4res {
	// This implementation does not support CLAIM_DELEGATE_PREV, so
	// there is no need to implement DELEGPURGE.
	return nfsv4.Delegpurge4res{Status: nfsv4.NFS4ERR_NOTSUPP}
}

func (s *compoundState) opDelegreturn(args *nfsv4.Delegreturn4args) nfsv4.Delegreturn4res {
	// This implementation never hands out any delegations to the
	// client, meaning that any state ID provided to this operation
	// is invalid.
	return nfsv4.Delegreturn4res{Status: nfsv4.NFS4ERR_BAD_STATEID}
}

func (s *compoundState) opGetattr(args *nfsv4.Getattr4args) nfsv4.Getattr4res {
	currentNode, _, st := s.currentFileHandle.getNode()
	if st != nfsv4.NFS4_OK {
		return &nfsv4.Getattr4res_default{Status: st}
	}
	var attributes virtual.Attributes
	currentNode.VirtualGetAttributes(attrRequestToAttributesMask(args.AttrRequest), &attributes)
	p := s.program
	return &nfsv4.Getattr4res_NFS4_OK{
		Resok4: nfsv4.Getattr4resok{
			ObjAttributes: p.attributesToFattr4(&attributes, args.AttrRequest),
		},
	}
}

func (s *compoundState) opGetfh() nfsv4.Getfh4res {
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

func (s *compoundState) opLink(args *nfsv4.Link4args) nfsv4.Link4res {
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
	changeInfo, vs := targetDirectory.VirtualLink(name, sourceLeaf, 0, &virtual.Attributes{})
	if vs != virtual.StatusOK {
		return &nfsv4.Link4res_default{Status: toNFSv4Status(vs)}
	}
	return &nfsv4.Link4res_NFS4_OK{
		Resok4: nfsv4.Link4resok{
			Cinfo: toNFSv4ChangeInfo(&changeInfo),
		},
	}
}

func (s *compoundState) opLock(args *nfsv4.Lock4args) nfsv4.Lock4res {
	var ll leavesToClose
	defer ll.closeAll()

	p := s.program
	p.enter()
	defer p.leave()

	switch locker := args.Locker.(type) {
	case *nfsv4.Locker4_TRUE:
		// Create a new lock-owner file.
		owner := &locker.OpenOwner
		openStateID, st := p.internalizeRegularStateID(&owner.OpenStateid)
		if st != nfsv4.NFS4_OK {
			return &nfsv4.Lock4res_default{Status: st}
		}

		oos, st := p.getOpenOwnerByOtherForTransaction(openStateID.other)
		if st != nfsv4.NFS4_OK {
			return &nfsv4.Lock4res_default{Status: st}
		}

		transaction, lastResponse, st := oos.startTransaction(p, owner.OpenSeqid, &ll, unconfirmedOpenOwnerPolicyDeny)
		if st != nfsv4.NFS4_OK {
			if r, ok := lastResponse.(nfsv4.Lock4res); ok {
				return r
			}
			return &nfsv4.Lock4res_default{Status: st}
		}
		response := s.txLockInitial(args, openStateID, owner)
		transaction.complete(&openOwnerLastResponse{
			response: response,
		})
		return response
	case *nfsv4.Locker4_FALSE:
		// Add additional lock to existing lock-owner file.
		owner := &locker.LockOwner
		lockStateID, st := p.internalizeRegularStateID(&owner.LockStateid)
		if st != nfsv4.NFS4_OK {
			return &nfsv4.Lock4res_default{Status: st}
		}

		los, st := p.getLockOwnerByOtherForTransaction(lockStateID.other)
		if st != nfsv4.NFS4_OK {
			return &nfsv4.Lock4res_default{Status: st}
		}

		transaction, lastResponse, st := los.startTransaction(p, owner.LockSeqid, false)
		if st != nfsv4.NFS4_OK {
			if r, ok := lastResponse.(nfsv4.Lock4res); ok {
				return r
			}
			return &nfsv4.Lock4res_default{Status: st}
		}
		response := s.txLockSuccessive(args, lockStateID, owner)
		transaction.complete(response)
		return response
	default:
		// Incorrectly encoded boolean value.
		return &nfsv4.Lock4res_default{Status: nfsv4.NFS4ERR_BADXDR}
	}
}

func (s *compoundState) txLockInitial(args *nfsv4.Lock4args, openStateID regularStateID, owner *nfsv4.OpenToLockOwner4) nfsv4.Lock4res {
	oofs, st := s.getOpenOwnerFileByStateID(openStateID, false)
	if st != nfsv4.NFS4_OK {
		return &nfsv4.Lock4res_default{Status: st}
	}

	oos := oofs.openOwner
	if owner.LockOwner.Clientid != oos.confirmedClient.confirmation.key.shortClientID {
		// Provided lock-owner's client ID does not match with
		// that of the open-owner.
		return &nfsv4.Lock4res_default{Status: nfsv4.NFS4ERR_INVAL}
	}

	confirmedClient := oos.confirmedClient
	lockOwnerKey := string(owner.LockOwner.Owner)
	los, ok := confirmedClient.lockOwners[lockOwnerKey]
	initialTransaction := false
	if !ok {
		// Lock-owner does not yet exist. Create a new one.
		los = &lockOwnerState{
			confirmedClient: confirmedClient,
			owner:           owner.LockOwner.Owner,
		}
		confirmedClient.lockOwners[lockOwnerKey] = los
		initialTransaction = true
	} else {
		if _, ok := oofs.lockOwnerFiles[los]; ok {
			// Lock-owner has already been associated with
			// this file. We should have gone through
			// txLockSuccessive() instead.
			//
			// More details: RFC 7530, section 16.10.5,
			// bullet point 2.
			return &nfsv4.Lock4res_default{Status: nfsv4.NFS4ERR_BAD_SEQID}
		}
	}

	// Start a nested transaction on the lock-owner.
	p := s.program
	transaction, lastResponse, st := los.startTransaction(p, owner.LockSeqid, initialTransaction)
	if st != nfsv4.NFS4_OK {
		if initialTransaction {
			panic("Failed to start transaction on a new lock-owner, which is impossible. This would cause the lock-owner to leak.")
		}
		if r, ok := lastResponse.(nfsv4.Lock4res); ok {
			return r
		}
		return &nfsv4.Lock4res_default{Status: st}
	}

	// Create a new lock-owner file. Set the sequence ID to zero, as
	// txLockCommon() will already bump it to one.
	lofs := &lockOwnerFileState{
		lockOwner:      los,
		openOwnerFile:  oofs,
		shareAccess:    oofs.shareAccess,
		lockOwnerIndex: len(los.files),
		stateID:        p.newRegularStateID(0),
	}
	p.lockOwnerFilesByOther[lofs.stateID.other] = lofs
	oofs.lockOwnerFiles[los] = lofs
	los.files = append(los.files, lofs)

	response := s.txLockCommon(args, lofs)
	transaction.complete(response)

	// Upon failure, undo the creation of the newly created
	// lock-owner file. This may also remove the lock-owner if it
	// references no other files.
	if response.GetStatus() != nfsv4.NFS4_OK {
		if lofs.lockCount > 0 {
			panic("Failed to acquire lock on a newly created lock-owner file, yet its lock count is non-zero")
		}
		lofs.remove(p)
	}
	return response
}

func (s *compoundState) txLockSuccessive(args *nfsv4.Lock4args, lockStateID regularStateID, owner *nfsv4.ExistLockOwner4) nfsv4.Lock4res {
	lofs, st := s.getLockOwnerFileByStateID(lockStateID)
	if st != nfsv4.NFS4_OK {
		return &nfsv4.Lock4res_default{Status: st}
	}
	return s.txLockCommon(args, lofs)
}

func (s *compoundState) txLockCommon(args *nfsv4.Lock4args, lofs *lockOwnerFileState) nfsv4.Lock4res {
	start, end, st := offsetLengthToStartEnd(args.Offset, args.Length)
	if st != nfsv4.NFS4_OK {
		return &nfsv4.Lock4res_default{Status: st}
	}
	lockType, st := nfsLockType4ToByteRangeLockType(args.Locktype)
	if st != nfsv4.NFS4_OK {
		return &nfsv4.Lock4res_default{Status: st}
	}

	lock := &virtual.ByteRangeLock[*lockOwnerState]{
		Owner: lofs.lockOwner,
		Start: start,
		End:   end,
		Type:  lockType,
	}

	// Test whether the new lock conflicts with an existing one.
	openedFile := lofs.openOwnerFile.openedFile
	if conflictingLock := openedFile.locks.Test(lock); conflictingLock != nil {
		return &nfsv4.Lock4res_NFS4ERR_DENIED{
			Denied: byteRangeLockToLock4Denied(conflictingLock),
		}
	}

	lofs.lockCount += openedFile.locks.Set(lock)
	if lofs.lockCount < 0 {
		panic("Negative lock count")
	}
	lofs.stateID.seqID = nextSeqID(lofs.stateID.seqID)
	p := s.program
	return &nfsv4.Lock4res_NFS4_OK{
		Resok4: nfsv4.Lock4resok{
			LockStateid: p.externalizeStateID(lofs.stateID),
		},
	}
}

func (s *compoundState) opLockt(args *nfsv4.Lockt4args) nfsv4.Lockt4res {
	_, st := s.currentFileHandle.getLeaf()
	if st != nfsv4.NFS4_OK {
		return &nfsv4.Lockt4res_default{Status: st}
	}
	handleKey := string(s.currentFileHandle.handle)

	p := s.program
	p.enter()
	defer p.leave()

	start, end, st := offsetLengthToStartEnd(args.Offset, args.Length)
	if st != nfsv4.NFS4_OK {
		return &nfsv4.Lockt4res_default{Status: st}
	}
	lockType, st := nfsLockType4ToByteRangeLockType(args.Locktype)
	if st != nfsv4.NFS4_OK {
		return &nfsv4.Lockt4res_default{Status: st}
	}

	openedFile, ok := p.openedFilesByHandle[handleKey]
	if !ok {
		// File isn't opened by anyone, meaning no locks may
		// cause a conflict. Just return success.
		return &nfsv4.Lockt4res_NFS4_OK{}
	}

	confirmedClient, st := p.getConfirmedClientByShortID(args.Owner.Clientid)
	if st != nfsv4.NFS4_OK {
		return &nfsv4.Lockt4res_default{Status: st}
	}
	confirmedClient.confirmation.hold(p)
	defer confirmedClient.confirmation.release(p)

	// Attempt to obtain the lock owner that is provided in the
	// arguments. It may be the case that none exists, in which case
	// we just pass a nil value to ByteRangeLockSet.Test(),
	// indicating a lock-owner that differs from any existing one.
	los := confirmedClient.lockOwners[string(args.Owner.Owner)]
	lock := &virtual.ByteRangeLock[*lockOwnerState]{
		Owner: los,
		Start: start,
		End:   end,
		Type:  lockType,
	}

	if conflictingLock := openedFile.locks.Test(lock); conflictingLock != nil {
		return &nfsv4.Lockt4res_NFS4ERR_DENIED{
			Denied: byteRangeLockToLock4Denied(conflictingLock),
		}
	}
	return &nfsv4.Lockt4res_NFS4_OK{}
}

func (s *compoundState) opLocku(args *nfsv4.Locku4args) nfsv4.Locku4res {
	p := s.program
	p.enter()
	defer p.leave()

	lockStateID, st := p.internalizeRegularStateID(&args.LockStateid)
	if st != nfsv4.NFS4_OK {
		return &nfsv4.Locku4res_default{Status: st}
	}

	los, st := p.getLockOwnerByOtherForTransaction(lockStateID.other)
	if st != nfsv4.NFS4_OK {
		return &nfsv4.Locku4res_default{Status: st}
	}

	transaction, lastResponse, st := los.startTransaction(p, args.Seqid, false)
	if st != nfsv4.NFS4_OK {
		if r, ok := lastResponse.(nfsv4.Locku4res); ok {
			return r
		}
		return &nfsv4.Locku4res_default{Status: st}
	}
	response := s.txLocku(args, lockStateID)
	transaction.complete(response)
	return response
}

func (s *compoundState) txLocku(args *nfsv4.Locku4args, lockStateID regularStateID) nfsv4.Locku4res {
	lofs, st := s.getLockOwnerFileByStateID(lockStateID)
	if st != nfsv4.NFS4_OK {
		return &nfsv4.Locku4res_default{Status: st}
	}
	start, end, st := offsetLengthToStartEnd(args.Offset, args.Length)
	if st != nfsv4.NFS4_OK {
		return &nfsv4.Locku4res_default{Status: st}
	}

	lock := &virtual.ByteRangeLock[*lockOwnerState]{
		Owner: lofs.lockOwner,
		Start: start,
		End:   end,
		Type:  virtual.ByteRangeLockTypeUnlocked,
	}

	lofs.lockCount += lofs.openOwnerFile.openedFile.locks.Set(lock)
	if lofs.lockCount < 0 {
		panic("Negative lock count")
	}
	lofs.stateID.seqID = nextSeqID(lofs.stateID.seqID)
	p := s.program
	return &nfsv4.Locku4res_NFS4_OK{
		LockStateid: p.externalizeStateID(lofs.stateID),
	}
}

func (s *compoundState) opLookup(args *nfsv4.Lookup4args) nfsv4.Lookup4res {
	currentDirectory, st := s.currentFileHandle.getDirectoryOrSymlink()
	if st != nfsv4.NFS4_OK {
		return nfsv4.Lookup4res{Status: st}
	}
	name, st := nfsv4NewComponent(args.Objname)
	if st != nfsv4.NFS4_OK {
		return nfsv4.Lookup4res{Status: st}
	}
	var attributes virtual.Attributes
	directory, leaf, vs := currentDirectory.VirtualLookup(name, virtual.AttributesMaskFileHandle, &attributes)
	if vs != virtual.StatusOK {
		return nfsv4.Lookup4res{Status: toNFSv4Status(vs)}
	}
	s.currentFileHandle = fileHandle{
		handle:    attributes.GetFileHandle(),
		directory: directory,
		leaf:      leaf,
	}
	return nfsv4.Lookup4res{Status: nfsv4.NFS4_OK}
}

func (s *compoundState) opLookupp() nfsv4.Lookupp4res {
	if _, st := s.currentFileHandle.getDirectoryOrSymlink(); st != nfsv4.NFS4_OK {
		return nfsv4.Lookupp4res{Status: st}
	}

	// TODO: Do we want to implement this method as well? For most
	// directory types (e.g., CAS backed directories) this method is
	// hard to implement, as they don't necessarily have a single
	// parent.
	return nfsv4.Lookupp4res{Status: nfsv4.NFS4ERR_NOENT}
}

func (s *compoundState) opNverify(args *nfsv4.Nverify4args) nfsv4.Nverify4res {
	if st := s.verifyAttributes(&args.ObjAttributes); st != nfsv4.NFS4ERR_NOT_SAME {
		return nfsv4.Nverify4res{Status: st}
	}
	return nfsv4.Nverify4res{Status: nfsv4.NFS4_OK}
}

func (s *compoundState) opOpen(args *nfsv4.Open4args) nfsv4.Open4res {
	var ll leavesToClose
	defer ll.closeAll()

	p := s.program
	p.enter()
	defer p.leave()

	openOwnerKey := string(args.Owner.Owner)
	var oos *openOwnerState
	for {
		// Obtain confirmed client state.
		confirmedClient, st := p.getConfirmedClientByShortID(args.Owner.Clientid)
		if st != nfsv4.NFS4_OK {
			return &nfsv4.Open4res_default{Status: st}
		}

		var ok bool
		oos, ok = confirmedClient.openOwners[openOwnerKey]
		if !ok {
			// Open-owner has never been seen before. Create
			// a new one that is in the unconfirmed tate.
			oos = &openOwnerState{
				confirmedClient: confirmedClient,
				key:             openOwnerKey,
				filesByHandle:   map[string]*openOwnerFileState{},
			}
			confirmedClient.openOwners[openOwnerKey] = oos
			baseProgramOpenOwnersCreated.Inc()
		}

		if oos.waitForCurrentTransactionCompletion(p) {
			break
		}
	}

	transaction, lastResponse, st := oos.startTransaction(p, args.Seqid, &ll, unconfirmedOpenOwnerPolicyReinitialize)
	if st != nfsv4.NFS4_OK {
		if r, ok := lastResponse.(nfsv4.Open4res); ok {
			// Last call was also an OPEN. Return cached response.
			return r
		}
		return &nfsv4.Open4res_default{Status: st}
	}
	response := s.txOpen(args, oos)
	transaction.complete(&openOwnerLastResponse{
		response: response,
	})
	return response
}

func (s *compoundState) txOpen(args *nfsv4.Open4args, oos *openOwnerState) nfsv4.Open4res {
	// Drop the lock, as VirtualOpenChild may block. This is safe to
	// do within open-owner transactions.
	p := s.program
	p.leave()
	isLocked := false
	defer func() {
		if !isLocked {
			p.enter()
		}
	}()

	currentDirectory, st := s.currentFileHandle.getDirectory()
	if st != nfsv4.NFS4_OK {
		return &nfsv4.Open4res_default{Status: st}
	}

	// Convert share_* fields.
	shareAccess, st := shareAccessToShareMask(args.ShareAccess)
	if st != nfsv4.NFS4_OK {
		return &nfsv4.Open4res_default{Status: st}
	}

	// As with most UNIX-like systems, we don't support share_deny.
	// Only permit this field to be set to OPEN4_SHARE_DENY_NONE,
	// behaving as if there's an implicit OPEN4_SHARE_ACCESS_BOTH on
	// all files.
	//
	// More details: RFC 7530, section 16.16.5, paragraph 6.
	switch args.ShareDeny {
	case nfsv4.OPEN4_SHARE_DENY_NONE:
	case nfsv4.OPEN4_SHARE_DENY_READ, nfsv4.OPEN4_SHARE_DENY_WRITE, nfsv4.OPEN4_SHARE_DENY_BOTH:
		return &nfsv4.Open4res_default{Status: nfsv4.NFS4ERR_SHARE_DENIED}
	default:
		return &nfsv4.Open4res_default{Status: nfsv4.NFS4ERR_INVAL}
	}

	// Convert openhow.
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
		case *nfsv4.Createhow4_EXCLUSIVE4:
			// Create a file, allowing the file to exist if
			// it was created by a previous call that
			// provided the same verifier.
			// TODO: Implement this!
		default:
			return &nfsv4.Open4res_default{Status: nfsv4.NFS4ERR_INVAL}
		}
	} else {
		// Don't create a new file. Only open an existing file.
		existingOptions = &virtual.OpenExistingOptions{}
	}

	// Convert claim. As we don't support delegations, we can only
	// meaningfully support CLAIM_NULL.
	var name path.Component
	switch claim := args.Claim.(type) {
	case *nfsv4.OpenClaim4_CLAIM_NULL:
		var st nfsv4.Nfsstat4
		name, st = nfsv4NewComponent(claim.File)
		if st != nfsv4.NFS4_OK {
			return &nfsv4.Open4res_default{Status: st}
		}
	case *nfsv4.OpenClaim4_CLAIM_PREVIOUS, *nfsv4.OpenClaim4_CLAIM_DELEGATE_CUR:
		return &nfsv4.Open4res_default{Status: nfsv4.NFS4ERR_RECLAIM_BAD}
	case *nfsv4.OpenClaim4_CLAIM_DELEGATE_PREV:
		return &nfsv4.Open4res_default{Status: nfsv4.NFS4ERR_NOTSUPP}
	default:
		return &nfsv4.Open4res_default{Status: nfsv4.NFS4ERR_INVAL}
	}

	// Open the file.
	var attributes virtual.Attributes
	leaf, respected, changeInfo, vs := currentDirectory.VirtualOpenChild(
		name,
		shareAccess,
		createAttributes,
		existingOptions,
		virtual.AttributesMaskFileHandle,
		&attributes)
	if vs != virtual.StatusOK {
		return &nfsv4.Open4res_default{Status: toNFSv4Status(vs)}
	}

	handle := attributes.GetFileHandle()
	handleKey := string(handle)

	s.currentFileHandle = fileHandle{
		handle: handle,
		leaf:   leaf,
	}

	response := &nfsv4.Open4res_NFS4_OK{
		Resok4: nfsv4.Open4resok{
			Cinfo:      toNFSv4ChangeInfo(&changeInfo),
			Rflags:     nfsv4.OPEN4_RESULT_LOCKTYPE_POSIX,
			Attrset:    attributesMaskToBitmap4(respected),
			Delegation: &nfsv4.OpenDelegation4_OPEN_DELEGATE_NONE{},
		},
	}

	p.enter()
	isLocked = true

	oofs, ok := oos.filesByHandle[handleKey]
	if ok {
		// This file has already been opened by this open-owner,
		// meaning we should upgrade the open file. Increase
		// closeCount, so that the file is released a sufficient
		// number of times upon last close.
		//
		// More details: RFC 7530, section 9.11.
		oofs.shareAccess |= shareAccess
		oofs.stateID.seqID = nextSeqID(oofs.stateID.seqID)
		if oofs.closeCount == 0 {
			panic("Attempted to use file that has already been closed. It should have been removed before this transaction started.")
		}
		oofs.closeCount++
	} else {
		openedFile, ok := p.openedFilesByHandle[handleKey]
		if ok {
			openedFile.openOwnersCount.increase()
		} else {
			// This file has not been opened by any
			// open-owner. Keep track of it, so that we
			// don't need to call into HandleResolver. This
			// ensures that the file remains accessible
			// while opened, even when unlinked.
			openedFile = &openedFileState{
				handle:          handle,
				handleKey:       handleKey,
				leaf:            leaf,
				openOwnersCount: 1,
			}
			openedFile.locks.Initialize()
			p.openedFilesByHandle[handleKey] = openedFile
		}

		// This file has not been opened by this open-owner.
		// Create a new state ID.
		oofs = &openOwnerFileState{
			openOwner:      oos,
			openedFile:     openedFile,
			shareAccess:    shareAccess,
			stateID:        p.newRegularStateID(1),
			closeCount:     1,
			useCount:       1,
			lockOwnerFiles: map[*lockOwnerState]*lockOwnerFileState{},
		}
		oos.filesByHandle[handleKey] = oofs
		p.openOwnerFilesByOther[oofs.stateID.other] = oofs
	}

	response.Resok4.Stateid = p.externalizeStateID(oofs.stateID)
	if !oos.confirmed {
		// The first time that this open-owner is used. Request
		// that the caller issues an OPEN_CONFIRM operation.
		response.Resok4.Rflags |= nfsv4.OPEN4_RESULT_CONFIRM
	}
	return response
}

func (s *compoundState) opOpenattr(args *nfsv4.Openattr4args) nfsv4.Openattr4res {
	// This implementation does not support named attributes.
	if _, _, st := s.currentFileHandle.getNode(); st != nfsv4.NFS4_OK {
		return nfsv4.Openattr4res{Status: st}
	}
	return nfsv4.Openattr4res{Status: nfsv4.NFS4ERR_NOTSUPP}
}

func (s *compoundState) opOpenConfirm(args *nfsv4.OpenConfirm4args) nfsv4.OpenConfirm4res {
	p := s.program
	openStateID, st := p.internalizeRegularStateID(&args.OpenStateid)
	if st != nfsv4.NFS4_OK {
		return &nfsv4.OpenConfirm4res_default{Status: st}
	}

	var ll leavesToClose
	defer ll.closeAll()

	p.enter()
	defer p.leave()

	oos, st := p.getOpenOwnerByOtherForTransaction(openStateID.other)
	if st != nfsv4.NFS4_OK {
		return &nfsv4.OpenConfirm4res_default{Status: st}
	}
	transaction, lastResponse, st := oos.startTransaction(p, args.Seqid, &ll, unconfirmedOpenOwnerPolicyAllow)
	if st != nfsv4.NFS4_OK {
		if r, ok := lastResponse.(nfsv4.OpenConfirm4res); ok {
			return r
		}
		return &nfsv4.OpenConfirm4res_default{Status: st}
	}
	response := s.txOpenConfirm(openStateID, &ll)
	transaction.complete(&openOwnerLastResponse{
		response: response,
	})
	return response
}

func (s *compoundState) txOpenConfirm(openStateID regularStateID, ll *leavesToClose) nfsv4.OpenConfirm4res {
	oofs, st := s.getOpenOwnerFileByStateID(openStateID, true)
	if st != nfsv4.NFS4_OK {
		return &nfsv4.OpenConfirm4res_default{Status: st}
	}
	oofs.openOwner.confirmed = true
	oofs.stateID.seqID = nextSeqID(oofs.stateID.seqID)

	p := s.program
	return &nfsv4.OpenConfirm4res_NFS4_OK{
		Resok4: nfsv4.OpenConfirm4resok{
			OpenStateid: p.externalizeStateID(oofs.stateID),
		},
	}
}

func (s *compoundState) opOpenDowngrade(args *nfsv4.OpenDowngrade4args) nfsv4.OpenDowngrade4res {
	p := s.program
	openStateID, st := p.internalizeRegularStateID(&args.OpenStateid)
	if st != nfsv4.NFS4_OK {
		return &nfsv4.OpenDowngrade4res_default{Status: st}
	}

	var ll leavesToClose
	defer ll.closeAll()

	p.enter()
	defer p.leave()

	oos, st := p.getOpenOwnerByOtherForTransaction(openStateID.other)
	if st != nfsv4.NFS4_OK {
		return &nfsv4.OpenDowngrade4res_default{Status: st}
	}
	transaction, lastResponse, st := oos.startTransaction(p, args.Seqid, &ll, unconfirmedOpenOwnerPolicyDeny)
	if st != nfsv4.NFS4_OK {
		if r, ok := lastResponse.(nfsv4.OpenDowngrade4res); ok {
			return r
		}
		return &nfsv4.OpenDowngrade4res_default{Status: st}
	}
	response := s.txOpenDowngrade(args, openStateID)
	transaction.complete(&openOwnerLastResponse{
		response: response,
	})
	return response
}

func (s *compoundState) txOpenDowngrade(args *nfsv4.OpenDowngrade4args, openStateID regularStateID) nfsv4.OpenDowngrade4res {
	oofs, st := s.getOpenOwnerFileByStateID(openStateID, false)
	if st != nfsv4.NFS4_OK {
		return &nfsv4.OpenDowngrade4res_default{Status: st}
	}

	shareAccess, st := shareAccessToShareMask(args.ShareAccess)
	if st != nfsv4.NFS4_OK {
		return &nfsv4.OpenDowngrade4res_default{Status: st}
	}
	if shareAccess&^oofs.shareAccess != 0 || args.ShareDeny != nfsv4.OPEN4_SHARE_DENY_NONE {
		// Attempted to upgrade. The client should have called OPEN.
		//
		// More details: RFC 7530, section 16.19.4, paragraph 2.
		return &nfsv4.OpenDowngrade4res_default{Status: nfsv4.NFS4ERR_INVAL}
	}

	// We don't actually reopen/downgrade the underlying virtual
	// file system object. The original access mode may have been
	// duplicated into lock state IDs, meaning we may still see
	// READ, WRITE and SETATTR operations that assume the original
	// access mode.
	//
	// More details: RFC 7530, section 9.1.6, paragraph 7.
	oofs.shareAccess = shareAccess
	oofs.stateID.seqID = nextSeqID(oofs.stateID.seqID)

	p := s.program
	return &nfsv4.OpenDowngrade4res_NFS4_OK{
		Resok4: nfsv4.OpenDowngrade4resok{
			OpenStateid: p.externalizeStateID(oofs.stateID),
		},
	}
}

func (s *compoundState) opPutfh(args *nfsv4.Putfh4args) nfsv4.Putfh4res {
	p := s.program
	p.enter()
	if openedFile, ok := p.openedFilesByHandle[string(args.Object)]; ok {
		// File is opened at least once. Return this copy, so
		// that we're guaranteed to work, even if the file has
		// been removed from the file system.
		s.currentFileHandle = fileHandle{
			handle: openedFile.handle,
			leaf:   openedFile.leaf,
		}
		p.leave()
	} else {
		// File is currently not open. Call into the handle
		// resolver to do a lookup.
		p.leave()
		directory, leaf, vs := p.handleResolver(bytes.NewBuffer(args.Object))
		if vs != virtual.StatusOK {
			return nfsv4.Putfh4res{Status: toNFSv4Status(vs)}
		}
		s.currentFileHandle = fileHandle{
			handle:    args.Object,
			directory: directory,
			leaf:      leaf,
		}
	}
	return nfsv4.Putfh4res{Status: nfsv4.NFS4_OK}
}

func (s *compoundState) opPutpubfh() nfsv4.Putpubfh4res {
	p := s.program
	s.currentFileHandle = p.rootFileHandle
	return nfsv4.Putpubfh4res{Status: nfsv4.NFS4_OK}
}

func (s *compoundState) opPutrootfh() nfsv4.Putrootfh4res {
	p := s.program
	s.currentFileHandle = p.rootFileHandle
	return nfsv4.Putrootfh4res{Status: nfsv4.NFS4_OK}
}

func (s *compoundState) opRead(args *nfsv4.Read4args) nfsv4.Read4res {
	currentLeaf, cleanup, st := s.getOpenedLeaf(&args.Stateid, virtual.ShareMaskRead)
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

func (s *compoundState) opReaddir(args *nfsv4.Readdir4args) nfsv4.Readdir4res {
	currentDirectory, st := s.currentFileHandle.getDirectory()
	if st != nfsv4.NFS4_OK {
		return &nfsv4.Readdir4res_default{Status: st}
	}

	// Validate the cookie verifier.
	p := s.program
	if args.Cookie != 0 && args.Cookieverf != p.rebootVerifier {
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

func (s *compoundState) opReadlink() nfsv4.Readlink4res {
	currentLeaf, st := s.currentFileHandle.getLeaf()
	if st != nfsv4.NFS4_OK {
		if st == nfsv4.NFS4ERR_ISDIR {
			return &nfsv4.Readlink4res_default{Status: nfsv4.NFS4ERR_INVAL}
		}
		return &nfsv4.Readlink4res_default{Status: st}
	}
	target, vs := currentLeaf.VirtualReadlink()
	if vs != virtual.StatusOK {
		return &nfsv4.Readlink4res_default{Status: toNFSv4Status(vs)}
	}
	return &nfsv4.Readlink4res_NFS4_OK{
		Resok4: nfsv4.Readlink4resok{
			Link: target,
		},
	}
}

func (s *compoundState) opReleaseLockowner(args *nfsv4.ReleaseLockowner4args) nfsv4.ReleaseLockowner4res {
	p := s.program
	p.enter()
	defer p.leave()

	confirmedClient, st := p.getConfirmedClientByShortID(args.LockOwner.Clientid)
	if st != nfsv4.NFS4_OK {
		return nfsv4.ReleaseLockowner4res{Status: st}
	}
	confirmedClient.confirmation.hold(p)
	defer confirmedClient.confirmation.release(p)

	lockOwnerKey := string(args.LockOwner.Owner)
	if los, ok := confirmedClient.lockOwners[lockOwnerKey]; ok {
		// Check whether any of the files associated with this
		// lock-owner still have locks held. In that case the
		// client should call LOCKU first.
		//
		// More details: RFC 7530, section 16.37.4, last sentence.
		for _, lofs := range los.files {
			if lofs.lockCount > 0 {
				return nfsv4.ReleaseLockowner4res{Status: nfsv4.NFS4ERR_LOCKS_HELD}
			}
		}

		// None of the files have locks held. Remove the state
		// associated with all files. The final call to remove()
		// will also remove the lock-owner state.
		for len(los.files) > 0 {
			los.files[len(los.files)-1].remove(p)
		}
		if _, ok := confirmedClient.lockOwners[lockOwnerKey]; ok {
			panic("Removing all lock-owner files did not remove lock-owner")
		}
	}

	return nfsv4.ReleaseLockowner4res{Status: nfsv4.NFS4_OK}
}

func (s *compoundState) opRename(args *nfsv4.Rename4args) nfsv4.Rename4res {
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

func (s *compoundState) opRemove(args *nfsv4.Remove4args) nfsv4.Remove4res {
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

func (s *compoundState) opRenew(args *nfsv4.Renew4args) nfsv4.Renew4res {
	p := s.program
	p.enter()
	defer p.leave()

	confirmedClient, st := p.getConfirmedClientByShortID(args.Clientid)
	if st != nfsv4.NFS4_OK {
		return nfsv4.Renew4res{Status: st}
	}

	// Hold and release the client, so that the time at which the
	// client gets garbage collected is extended.
	confirmedClient.confirmation.hold(p)
	defer confirmedClient.confirmation.release(p)

	return nfsv4.Renew4res{Status: nfsv4.NFS4_OK}
}

func (s *compoundState) opRestorefh() nfsv4.Restorefh4res {
	if s.savedFileHandle.directory == nil && s.savedFileHandle.leaf == nil {
		return nfsv4.Restorefh4res{Status: nfsv4.NFS4ERR_RESTOREFH}
	}
	s.currentFileHandle = s.savedFileHandle
	return nfsv4.Restorefh4res{Status: nfsv4.NFS4_OK}
}

func (s *compoundState) opSavefh() nfsv4.Savefh4res {
	_, _, st := s.currentFileHandle.getNode()
	if st == nfsv4.NFS4_OK {
		s.savedFileHandle = s.currentFileHandle
	}
	return nfsv4.Savefh4res{Status: st}
}

func (s *compoundState) opSecinfo(args *nfsv4.Secinfo4args) nfsv4.Secinfo4res {
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
	if _, _, vs := currentDirectory.VirtualLookup(name, 0, &virtual.Attributes{}); vs != virtual.StatusOK {
		return &nfsv4.Secinfo4res_default{Status: toNFSv4Status(vs)}
	}
	return &nfsv4.Secinfo4res_NFS4_OK{
		Resok4: []nfsv4.Secinfo4{
			&nfsv4.Secinfo4_default{
				Flavor: rpcv2.AUTH_NONE,
			},
		},
	}
}

func (s *compoundState) opSetattr(args *nfsv4.Setattr4args) nfsv4.Setattr4res {
	// TODO: Respect the state ID, if provided!
	currentNode, _, st := s.currentFileHandle.getNode()
	if st != nfsv4.NFS4_OK {
		return nfsv4.Setattr4res{Status: st}
	}
	var attributes virtual.Attributes
	if st := fattr4ToAttributes(&args.ObjAttributes, &attributes); st != nfsv4.NFS4_OK {
		return nfsv4.Setattr4res{Status: st}
	}
	if vs := currentNode.VirtualSetAttributes(&attributes, 0, &virtual.Attributes{}); vs != virtual.StatusOK {
		return nfsv4.Setattr4res{Status: toNFSv4Status(vs)}
	}
	return nfsv4.Setattr4res{
		Status:   st,
		Attrsset: args.ObjAttributes.Attrmask,
	}
}

func (s *compoundState) opSetclientid(args *nfsv4.Setclientid4args) nfsv4.Setclientid4res {
	p := s.program
	p.enter()
	defer p.leave()

	// As we don't care about using the client callback, our
	// implementation of SETCLIENTID can be a lot simpler than
	// what's described by the spec. SETCLIENTID can normally be
	// used to update the client callback as well, which is
	// something we don't need to care about.

	longID := string(args.Client.Id)
	clientVerifier := args.Client.Verifier
	client, ok := p.clientsByLongID[longID]
	if !ok {
		// Client has not been observed before. Create it.
		client = &clientState{
			longID:                        longID,
			confirmationsByClientVerifier: map[nfsv4.Verifier4]*clientConfirmationState{},
		}
		p.clientsByLongID[longID] = client
	}

	confirmation, ok := client.confirmationsByClientVerifier[clientVerifier]
	if !ok {
		// Create a new confirmation record for SETCLIENTID_CONFIRM.
		confirmation = &clientConfirmationState{
			client:         client,
			clientVerifier: clientVerifier,
			key: clientConfirmationKey{
				shortClientID: p.randomNumberGenerator.Uint64(),
			},
		}
		p.randomNumberGenerator.Read(confirmation.key.serverVerifier[:])
		client.confirmationsByClientVerifier[clientVerifier] = confirmation
		p.clientConfirmationsByKey[confirmation.key] = confirmation
		p.clientConfirmationsByShortID[confirmation.key.shortClientID] = confirmation
		confirmation.insertIntoIdleList(p)
	}

	return &nfsv4.Setclientid4res_NFS4_OK{
		Resok4: nfsv4.Setclientid4resok{
			Clientid:           confirmation.key.shortClientID,
			SetclientidConfirm: confirmation.key.serverVerifier,
		},
	}
}

func (s *compoundState) opSetclientidConfirm(args *nfsv4.SetclientidConfirm4args) nfsv4.SetclientidConfirm4res {
	var ll leavesToClose
	defer ll.closeAll()

	p := s.program
	p.enter()
	defer p.leave()

	key := clientConfirmationKey{
		shortClientID:  args.Clientid,
		serverVerifier: args.SetclientidConfirm,
	}
	confirmation, ok := p.clientConfirmationsByKey[key]
	if !ok {
		return nfsv4.SetclientidConfirm4res{Status: nfsv4.NFS4ERR_STALE_CLIENTID}
	}

	client := confirmation.client
	if confirmedClient := client.confirmed; confirmedClient == nil || confirmedClient.confirmation != confirmation {
		// Client record has not been confirmed yet.
		confirmation.hold(p)
		defer confirmation.release(p)

		if confirmedClient != nil {
			// The client has another confirmed entry.
			// Remove all state, such as open files and locks.
			oldConfirmation := confirmedClient.confirmation
			if oldConfirmation.holdCount > 0 {
				// The client is currently running one
				// or more blocking operations. This
				// prevents us from closing files and
				// releasing locks.
				return nfsv4.SetclientidConfirm4res{Status: nfsv4.NFS4ERR_DELAY}
			}
			oldConfirmation.remove(p, &ll)
		}

		if client.confirmed != nil {
			panic("Attempted to replace confirmed client record")
		}
		client.confirmed = &confirmedClientState{
			confirmation: confirmation,
			openOwners:   map[string]*openOwnerState{},
			lockOwners:   map[string]*lockOwnerState{},
		}
	}

	return nfsv4.SetclientidConfirm4res{Status: nfsv4.NFS4_OK}
}

func (s *compoundState) opWrite(args *nfsv4.Write4args) nfsv4.Write4res {
	currentLeaf, cleanup, st := s.getOpenedLeaf(&args.Stateid, virtual.ShareMaskWrite)
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

func (s *compoundState) opVerify(args *nfsv4.Verify4args) nfsv4.Verify4res {
	if st := s.verifyAttributes(&args.ObjAttributes); st != nfsv4.NFS4ERR_SAME {
		return nfsv4.Verify4res{Status: st}
	}
	return nfsv4.Verify4res{Status: nfsv4.NFS4_OK}
}

// comparingWriter is an io.Writer that merely compares data that is
// written to a reference value.
type comparingWriter struct {
	reference []byte
	status    nfsv4.Nfsstat4
}

func (w *comparingWriter) Write(p []byte) (int, error) {
	if w.status == nfsv4.NFS4ERR_SAME {
		if len(p) > len(w.reference) {
			if bytes.Equal(p[:len(w.reference)], w.reference) {
				// Reference value is a prefix of the provided
				// data. With XDR this is never possible.
				*w = comparingWriter{status: nfsv4.NFS4ERR_BADXDR}
			} else {
				*w = comparingWriter{status: nfsv4.NFS4ERR_NOT_SAME}
			}
		} else {
			if bytes.Equal(p, w.reference[:len(p)]) {
				w.reference = w.reference[len(p):]
			} else {
				*w = comparingWriter{status: nfsv4.NFS4ERR_NOT_SAME}
			}
		}
	}
	return len(p), nil
}

type referenceCount int

func (rc *referenceCount) increase() {
	if *rc <= 0 {
		panic("Attempted to increase zero reference count")
	}
	(*rc)++
}

func (rc *referenceCount) decrease() bool {
	if *rc <= 0 {
		panic("Attempted to decrease zero reference count")
	}
	(*rc)--
	return *rc == 0
}

// fileHandle contains information on the current or saved file handle
// that is tracked in a COMPOUND procedure.
type fileHandle struct {
	handle    nfsv4.NfsFh4
	directory virtual.Directory
	leaf      virtual.Leaf
}

func (fh *fileHandle) getNode() (virtual.Node, bool, nfsv4.Nfsstat4) {
	if fh.directory != nil {
		return fh.directory, true, nfsv4.NFS4_OK
	}
	if fh.leaf != nil {
		return fh.leaf, false, nfsv4.NFS4_OK
	}
	return nil, false, nfsv4.NFS4ERR_NOFILEHANDLE
}

func (fh *fileHandle) getDirectory() (virtual.Directory, nfsv4.Nfsstat4) {
	if fh.directory != nil {
		return fh.directory, nfsv4.NFS4_OK
	}
	if fh.leaf != nil {
		return nil, nfsv4.NFS4ERR_NOTDIR
	}
	return nil, nfsv4.NFS4ERR_NOFILEHANDLE
}

func (fh *fileHandle) getDirectoryOrSymlink() (virtual.Directory, nfsv4.Nfsstat4) {
	if fh.directory != nil {
		return fh.directory, nfsv4.NFS4_OK
	}
	if fh.leaf != nil {
		// This call requires that we return NFS4ERR_SYMLINK if
		// we stumble upon a symlink. That way the client knows
		// that symlink expansion needs to be performed.
		var attributes virtual.Attributes
		fh.leaf.VirtualGetAttributes(virtual.AttributesMaskFileType, &attributes)
		if attributes.GetFileType() == filesystem.FileTypeSymlink {
			return nil, nfsv4.NFS4ERR_SYMLINK
		}
		return nil, nfsv4.NFS4ERR_NOTDIR
	}
	return nil, nfsv4.NFS4ERR_NOFILEHANDLE
}

func (fh *fileHandle) getLeaf() (virtual.Leaf, nfsv4.Nfsstat4) {
	if fh.leaf != nil {
		return fh.leaf, nfsv4.NFS4_OK
	}
	if fh.directory != nil {
		return nil, nfsv4.NFS4ERR_ISDIR
	}
	return nil, nfsv4.NFS4ERR_NOFILEHANDLE
}

// toNFSv4Status converts a status code returned by the virtual file
// system to its NFSv4 equivalent.
func toNFSv4Status(s virtual.Status) nfsv4.Nfsstat4 {
	switch s {
	case virtual.StatusErrAccess:
		return nfsv4.NFS4ERR_ACCESS
	case virtual.StatusErrBadHandle:
		return nfsv4.NFS4ERR_BADHANDLE
	case virtual.StatusErrExist:
		return nfsv4.NFS4ERR_EXIST
	case virtual.StatusErrInval:
		return nfsv4.NFS4ERR_INVAL
	case virtual.StatusErrIO:
		return nfsv4.NFS4ERR_IO
	case virtual.StatusErrIsDir:
		return nfsv4.NFS4ERR_ISDIR
	case virtual.StatusErrNoEnt:
		return nfsv4.NFS4ERR_NOENT
	case virtual.StatusErrNotDir:
		return nfsv4.NFS4ERR_NOTDIR
	case virtual.StatusErrNotEmpty:
		return nfsv4.NFS4ERR_NOTEMPTY
	case virtual.StatusErrNXIO:
		return nfsv4.NFS4ERR_NXIO
	case virtual.StatusErrPerm:
		return nfsv4.NFS4ERR_PERM
	case virtual.StatusErrROFS:
		return nfsv4.NFS4ERR_ROFS
	case virtual.StatusErrStale:
		return nfsv4.NFS4ERR_STALE
	case virtual.StatusErrSymlink:
		return nfsv4.NFS4ERR_SYMLINK
	case virtual.StatusErrXDev:
		return nfsv4.NFS4ERR_XDEV
	default:
		panic("Unknown status")
	}
}

// toNFSv4ChangeInfo converts directory change information returned by
// the virtual file system to its NFSv4 equivalent.
func toNFSv4ChangeInfo(changeInfo *virtual.ChangeInfo) nfsv4.ChangeInfo4 {
	// Implementations of virtual.Directory should make sure that
	// mutations are implemented atomically, so it's safe to report
	// the operation as being atomic.
	return nfsv4.ChangeInfo4{
		Atomic: true,
		Before: changeInfo.Before,
		After:  changeInfo.After,
	}
}

// clientState keeps track of all state corresponding to a single
// client. For every client we track one or more confirmations that can
// be completed using SETCLIENTID_CONFIRM. If SETCLIENTID_CONFIRM is
// called at least once, we track a confirmed client state.
type clientState struct {
	longID string

	confirmationsByClientVerifier map[nfsv4.Verifier4]*clientConfirmationState
	confirmed                     *confirmedClientState
}

// clientConfirmationState keeps track of all state corresponding to a
// single client confirmation record created through SETCLIENTID.
type clientConfirmationState struct {
	client         *clientState
	clientVerifier nfsv4.Verifier4
	key            clientConfirmationKey

	nextIdle     *clientConfirmationState
	previousIdle *clientConfirmationState
	lastSeen     time.Time
	holdCount    int
}

// removeFromIdleList removes the client confirmation from the list of
// clients that are currently not performing any operations against the
// server.
func (ccs *clientConfirmationState) removeFromIdleList() {
	ccs.previousIdle.nextIdle = ccs.nextIdle
	ccs.nextIdle.previousIdle = ccs.previousIdle
	ccs.previousIdle = nil
	ccs.nextIdle = nil
}

// insertIntoIdleList inserts the client confirmation into the list of
// clients that are currently not performing any operations against the
// server.
func (ccs *clientConfirmationState) insertIntoIdleList(p *baseProgram) {
	ccs.previousIdle = p.idleClientConfirmations.previousIdle
	ccs.nextIdle = &p.idleClientConfirmations
	ccs.previousIdle.nextIdle = ccs
	ccs.nextIdle.previousIdle = ccs
	ccs.lastSeen = p.now
}

// hold the client confirmation in such a way that it's not garbage
// collected. This needs to be called prior to performing a blocking
// operation.
func (ccs *clientConfirmationState) hold(p *baseProgram) {
	if ccs.holdCount == 0 {
		ccs.removeFromIdleList()
	}
	ccs.holdCount++
}

// release the client confirmation in such a way that it may be garbage
// collected.
func (ccs *clientConfirmationState) release(p *baseProgram) {
	if ccs.holdCount == 0 {
		panic("Attempted to decrease zero hold count")
	}
	ccs.holdCount--
	if ccs.holdCount == 0 {
		ccs.insertIntoIdleList(p)
	}
}

// remove the client confirmation. If the client was confirmed through
// SETCLIENTID_CONFIRM, all open files and acquired locks will be
// released.
func (ccs *clientConfirmationState) remove(p *baseProgram, ll *leavesToClose) {
	if ccs.holdCount != 0 {
		panic("Attempted to remove a client confirmation that was running one or more blocking operations")
	}

	client := ccs.client
	confirmedClient := client.confirmed
	if confirmedClient != nil && confirmedClient.confirmation == ccs {
		// This client confirmation record was confirmed,
		// meaning that removing it should also close all opened
		// files and release all locks.
		for _, oos := range confirmedClient.openOwners {
			oos.remove(p, ll)
		}
		if len(confirmedClient.lockOwners) != 0 {
			panic("Removing open-owners should have removed lock-owners as well")
		}
		client.confirmed = nil
	}

	// Remove the client confirmation.
	delete(client.confirmationsByClientVerifier, ccs.clientVerifier)
	delete(p.clientConfirmationsByKey, ccs.key)
	delete(p.clientConfirmationsByShortID, ccs.key.shortClientID)
	ccs.removeFromIdleList()

	// Remove the client if it no longer contains any confirmations.
	if len(client.confirmationsByClientVerifier) == 0 {
		delete(p.clientsByLongID, client.longID)
	}
}

// confirmedClientState stores all state for a client that has been
// confirmed through SETCLIENTID_CONFIRM.
type confirmedClientState struct {
	confirmation *clientConfirmationState
	openOwners   map[string]*openOwnerState
	lockOwners   map[string]*lockOwnerState
}

// clientConfirmationKey contains the information that a client must
// provide through SETCLIENTID_CONFIRM to confirm the client's
// registration.
type clientConfirmationKey struct {
	shortClientID  uint64
	serverVerifier nfsv4.Verifier4
}

// openOwnerState stores information on a single open-owner, which is a
// single process running on the client that opens files through the
// mount.
type openOwnerState struct {
	confirmedClient *confirmedClientState
	key             string

	// When not nil, an OPEN or OPEN_CONFIRM operation is in progress.
	currentTransactionWait <-chan struct{}

	confirmed     bool
	lastSeqID     nfsv4.Seqid4
	lastResponse  *openOwnerLastResponse
	filesByHandle map[string]*openOwnerFileState

	// Double linked list for open-owners that are unused. These
	// need to be garbage collected after some time, as the client
	// does not do that explicitly.
	nextUnused     *openOwnerState
	previousUnused *openOwnerState
	lastUsed       time.Time
}

// waitForCurrentTransactionCompletion blocks until any transaction that
// is running right now completes. Because it needs to drop the lock
// while waiting, this method returns a boolean value indicating whether
// it's safe to progress. If not, the caller should retry the lookup of
// the open-owner state.
func (oos *openOwnerState) waitForCurrentTransactionCompletion(p *baseProgram) bool {
	if wait := oos.currentTransactionWait; wait != nil {
		p.leave()
		<-wait
		p.enter()
		return false
	}
	return true
}

// forgetLastResponse can be called when the cached response of the last
// transaction needs to be removed. This is at the start of any
// subsequent transaction, or when reinitializing/removing the
// open-owner state.
//
// This method MUST be called before making any mutations to the
// open-owner state, as it also removes resources that were released
// during the previous transaction.
func (oos *openOwnerState) forgetLastResponse(p *baseProgram) {
	if oolr := oos.lastResponse; oolr != nil {
		oos.lastResponse = nil
		if oofs := oolr.closedFile; oofs != nil {
			oofs.removeFinalize(p)
		}
	}
}

// reinitialize the open-owner state in such a way that no files are
// opened. This method can be called when an unconfirmed open-owner is
// repurposed, or prior to forcefully removing an open-owner.
func (oos *openOwnerState) reinitialize(p *baseProgram, ll *leavesToClose) {
	if oos.currentTransactionWait != nil {
		panic("Attempted to reinitialize an open-owner while a transaction is in progress")
	}

	oos.forgetLastResponse(p)
	for _, oofs := range oos.filesByHandle {
		oofs.removeStart(p, ll)
		oofs.removeFinalize(p)
	}
}

// isUnused returns whether the open-owner state is unused, meaning that
// it should be garbage collected if a sufficient amount of time passes.
func (oos *openOwnerState) isUnused() bool {
	return len(oos.filesByHandle) == 0 ||
		(len(oos.filesByHandle) == 1 && oos.lastResponse != nil && oos.lastResponse.closedFile != nil) ||
		!oos.confirmed
}

// removeFromUnusedList removes the open-owner state from the list of
// open-owner states that have no open files or are not confirmed. These
// are garbage collected if a sufficient amount of time passes.
func (oos *openOwnerState) removeFromUnusedList() {
	oos.previousUnused.nextUnused = oos.nextUnused
	oos.nextUnused.previousUnused = oos.previousUnused
	oos.previousUnused = nil
	oos.nextUnused = nil
}

// remove the open-owner state. All opened files will be closed.
func (oos *openOwnerState) remove(p *baseProgram, ll *leavesToClose) {
	oos.reinitialize(p, ll)
	if oos.nextUnused != nil {
		oos.removeFromUnusedList()
	}
	delete(oos.confirmedClient.openOwners, oos.key)
	baseProgramOpenOwnersRemoved.Inc()
}

// unconfirmedOpenOwnerPolicy is an enumeration that describes how
// startTransaction() should behave when called against an open-owner
// that has not been confirmed.
type unconfirmedOpenOwnerPolicy int

const (
	// Allow the transaction to take place against unconfirmed
	// open-owners. This should be used by OPEN_CONFIRM.
	unconfirmedOpenOwnerPolicyAllow = iota
	// Don't allow the transaction to take place against unconfirmed
	// open-owners. This should be used by CLOSE, OPEN_DOWNGRADE,
	// etc..
	unconfirmedOpenOwnerPolicyDeny
	// Allow the transaction to take place against unconfirmed
	// open-owners, but do reinitialize them before progressing.
	// This should be used by OPEN, as it should assume that the
	// previously sent operation was a replay.
	//
	// More details: RFC 7530, section 16.18.5, paragraph 5.
	unconfirmedOpenOwnerPolicyReinitialize
)

func (oos *openOwnerState) startTransaction(p *baseProgram, seqID nfsv4.Seqid4, ll *leavesToClose, policy unconfirmedOpenOwnerPolicy) (*openOwnerTransaction, interface{}, nfsv4.Nfsstat4) {
	if oos.currentTransactionWait != nil {
		panic("Attempted to start a new transaction while another one is in progress")
	}

	if lastResponse := oos.lastResponse; lastResponse != nil && seqID == oos.lastSeqID {
		// Replay of the last request, meaning we should return
		// a cached response. This can only be done when the
		// type of operation is the same, which is determined by
		// the caller.
		//
		// More details: RFC 7530, section 9.1.9, bullet point 3.
		return nil, lastResponse.response, nfsv4.NFS4ERR_BAD_SEQID
	}

	if oos.confirmed {
		// For confirmed open-owners, only permit operations
		// that start the next transaction.
		if seqID != nextSeqID(oos.lastSeqID) {
			return nil, nil, nfsv4.NFS4ERR_BAD_SEQID
		}
	} else {
		switch policy {
		case unconfirmedOpenOwnerPolicyAllow:
			if seqID != nextSeqID(oos.lastSeqID) {
				return nil, nil, nfsv4.NFS4ERR_BAD_SEQID
			}
		case unconfirmedOpenOwnerPolicyDeny:
			return nil, nil, nfsv4.NFS4ERR_BAD_SEQID
		case unconfirmedOpenOwnerPolicyReinitialize:
			oos.reinitialize(p, ll)
		}
	}

	// Start a new transaction. Because the client has sent a
	// request with a new sequence ID, we know it will no longer
	// attempt to retry the previous operation. Release the last
	// response and any state IDs that were closed during the
	// previous operation.
	oos.forgetLastResponse(p)
	wait := make(chan struct{}, 1)
	oos.currentTransactionWait = wait

	if oos.nextUnused != nil {
		// Prevent garbage collection of the open-owner while
		// operation takes place. It will be reinserted upon
		// completion of the transaction, if needed.
		oos.removeFromUnusedList()
	}

	// Prevent garbage collection of the client.
	oos.confirmedClient.confirmation.hold(p)

	return &openOwnerTransaction{
		program: p,
		state:   oos,
		seqID:   seqID,
		wait:    wait,
	}, nil, nfsv4.NFS4_OK
}

// openOwnerTransaction is a helper object for completing a transaction
// that was created using startTransaction().
type openOwnerTransaction struct {
	program *baseProgram
	state   *openOwnerState
	seqID   nfsv4.Seqid4
	wait    chan<- struct{}
}

func (oot *openOwnerTransaction) complete(lastResponse *openOwnerLastResponse) {
	close(oot.wait)
	oos := oot.state
	oos.currentTransactionWait = nil

	if transactionShouldComplete(lastResponse.response.GetStatus()) {
		oos.lastSeqID = oot.seqID
		oos.lastResponse = lastResponse
	}

	p := oot.program
	if oos.isUnused() {
		// Open-owner should be garbage collected. Insert it
		// into the list of open-owners to be removed.
		oos.previousUnused = p.unusedOpenOwners.previousUnused
		oos.nextUnused = &p.unusedOpenOwners
		oos.previousUnused.nextUnused = oos
		oos.nextUnused.previousUnused = oos
		oos.lastUsed = p.now
	}

	// Re-enable garbage collection of the client.
	oos.confirmedClient.confirmation.release(p)
}

// responseMessage is an interface for response messages of an
// open-owner or lock-owner transaction.
type responseMessage interface{ GetStatus() nfsv4.Nfsstat4 }

// openOwnerLastResponse contains information on the outcome of the last
// transaction of a given open-owner. This information is needed both to
// respond to retries, but also to definitively remove state IDs closed
// by the last transaction.
type openOwnerLastResponse struct {
	response   responseMessage
	closedFile *openOwnerFileState
}

// openOwnerFileState stores information on a file that is currently
// opened within the context of a single open-owner.
type openOwnerFileState struct {
	// Constant fields.
	openedFile *openedFileState

	// Variable fields.
	openOwner      *openOwnerState
	shareAccess    virtual.ShareMask
	stateID        regularStateID
	closeCount     uint
	useCount       referenceCount
	lockOwnerFiles map[*lockOwnerState]*lockOwnerFileState
}

// maybeClose decreases the use count on an opened file. If zero, it
// schedules closure of the underlying virtual.Leaf object. This method
// is called at the end of CLOSE, but also at the end of READ or WRITE.
// A call to CLOSE may not immediately close a file if one or more
// READ/WRITE operations are still in progress.
func (oofs *openOwnerFileState) maybeClose(ll *leavesToClose) {
	if oofs.useCount.decrease() {
		ll.leaves = append(ll.leaves, leafToClose{
			leaf:       oofs.openedFile.leaf,
			closeCount: oofs.closeCount,
		})
		if oofs.closeCount == 0 {
			panic("Attempted to close file multiple times")
		}
		oofs.closeCount = 0
	}
}

func (oofs *openOwnerFileState) removeStart(p *baseProgram, ll *leavesToClose) {
	// Release lock state IDs associated with the file. This should
	// be done as part of CLOSE; not LOCKU. If these have one or
	// more byte ranges locked, we unlock them. It would also be
	// permitted to return NFS4ERR_LOCKS_HELD, requiring that the
	// client issues LOCKU operations before retrying, but that is
	// less efficient.
	//
	// More details:
	// - RFC 7530, section 9.1.4.4, paragraph 1.
	// - RFC 7530, section 9.10, paragraph 3.
	// - RFC 7530, section 16.2.4, paragraph 2.
	for _, lofs := range oofs.lockOwnerFiles {
		lofs.remove(p)
	}

	oofs.maybeClose(ll)
}

// removeFinalize removes an opened file from the open-owner state. This
// method is not called during CLOSE, but during the next transaction on
// an open-owner. This ensures that its state ID remains resolvable,
// allowing the CLOSE operation to be retried.
func (oofs *openOwnerFileState) removeFinalize(p *baseProgram) {
	// Disconnect the openOwnerFileState.
	handleKey := oofs.openedFile.handleKey
	delete(oofs.openOwner.filesByHandle, handleKey)
	delete(p.openOwnerFilesByOther, oofs.stateID.other)
	oofs.openOwner = nil

	// Disconnect the openedFileState. Do leave it attached to the
	// openOwnerFileState, so that in-flight READ and WRITE
	// operations can still safely call close().
	if oofs.openedFile.openOwnersCount.decrease() {
		delete(p.openedFilesByHandle, handleKey)
	}
}

// openedFileState stores information on a file that is currently opened
// at least once. It is stored in the openedFilesByHandle map. This
// allows these files to be resolvable through PUTFH, even if they are
// no longer linked in the file system.
type openedFileState struct {
	// Constant fields.
	handle    nfsv4.NfsFh4
	handleKey string
	leaf      virtual.Leaf

	// Variable fields.
	openOwnersCount referenceCount
	locks           virtual.ByteRangeLockSet[*lockOwnerState]
}

// lockOwnerState represents byte-range locking state associated with a
// given opened file and given lock-owner. Because lock-owners are bound
// to a single file (i.e., they can't contain locks belonging to
// different files), it is contained in the openedFileState.
//
// More details: RFC 7530, section 16.10.5, paragraph 6.
type lockOwnerState struct {
	confirmedClient *confirmedClientState
	owner           []byte

	lastSeqID    nfsv4.Seqid4
	lastResponse responseMessage
	files        []*lockOwnerFileState
}

func (los *lockOwnerState) forgetLastResponse(p *baseProgram) {
	los.lastResponse = nil
}

func (los *lockOwnerState) startTransaction(p *baseProgram, seqID nfsv4.Seqid4, initialTransaction bool) (*lockOwnerTransaction, interface{}, nfsv4.Nfsstat4) {
	if lastResponse := los.lastResponse; lastResponse != nil && seqID == los.lastSeqID {
		// Replay of the last request, meaning we should return
		// a cached response. This can only be done when the
		// type of operation is the same, which is determined by
		// the caller.
		//
		// More details: RFC 7530, section 9.1.9, bullet point 3.
		return nil, lastResponse, nfsv4.NFS4ERR_BAD_SEQID
	}

	if !initialTransaction && seqID != nextSeqID(los.lastSeqID) {
		return nil, nil, nfsv4.NFS4ERR_BAD_SEQID
	}

	// Start a new transaction. Because the client has sent a
	// request with a new sequence ID, we know it will no longer
	// attempt to retry the previous operation. Release the last
	// response and any state IDs that were closed during the
	// previous operation.
	los.forgetLastResponse(p)

	// Prevent garbage collection of the client.
	los.confirmedClient.confirmation.hold(p)

	return &lockOwnerTransaction{
		program: p,
		state:   los,
		seqID:   seqID,
	}, nil, nfsv4.NFS4_OK
}

type lockOwnerTransaction struct {
	program *baseProgram
	state   *lockOwnerState
	seqID   nfsv4.Seqid4
}

func (lot *lockOwnerTransaction) complete(lastResponse responseMessage) {
	los := lot.state
	if transactionShouldComplete(lastResponse.GetStatus()) {
		los.lastSeqID = lot.seqID
		los.lastResponse = lastResponse
	}

	// Re-enable garbage collection of the client.
	p := lot.program
	los.confirmedClient.confirmation.release(p)
}

type lockOwnerFileState struct {
	// Constant fields.
	lockOwner     *lockOwnerState
	openOwnerFile *openOwnerFileState
	shareAccess   virtual.ShareMask

	// Variable fields.
	lockOwnerIndex int
	stateID        regularStateID
	lockCount      int
}

func (lofs *lockOwnerFileState) remove(p *baseProgram) {
	if lofs.lockCount > 0 {
		// Lock-owner still has one or more locks held on this
		// file. Issue an unlock operation that spans the full
		// range of the file to release all locks at once.
		lock := &virtual.ByteRangeLock[*lockOwnerState]{
			Owner: lofs.lockOwner,
			Start: 0,
			End:   math.MaxUint64,
			Type:  virtual.ByteRangeLockTypeUnlocked,
		}
		lofs.lockCount += lofs.openOwnerFile.openedFile.locks.Set(lock)
		if lofs.lockCount != 0 {
			panic("Failed to release locks")
		}
	}

	// Remove the lock-owner file from maps.
	delete(p.lockOwnerFilesByOther, lofs.stateID.other)
	los := lofs.lockOwner
	delete(lofs.openOwnerFile.lockOwnerFiles, los)

	// Remove the lock-owner file from the list in the lock-owner.
	// We do need to make sure the list remains contiguous.
	lastIndex := len(los.files) - 1
	lastLOFS := los.files[lastIndex]
	lastLOFS.lockOwnerIndex = lofs.lockOwnerIndex
	los.files[lastLOFS.lockOwnerIndex] = lastLOFS
	los.files[lastIndex] = nil
	los.files = los.files[:lastIndex]
	lofs.lockOwnerIndex = -1

	// Remove the lock-owner if there are no longer any files
	// associated with it.
	if len(los.files) == 0 {
		delete(los.confirmedClient.lockOwners, string(los.owner))
	}
}

// leafToClose contains information on a virtual file system leaf node
// that needs to be closed at the end of the current operation, after
// locks have been released.
type leafToClose struct {
	leaf       virtual.Leaf
	closeCount uint
}

// leavesToClose is a list of virtual file system leaf nodes that need
// to be closed at the end of the current operation, after locks have
// been released.
type leavesToClose struct {
	leaves []leafToClose
}

func (ll *leavesToClose) empty() bool {
	return len(ll.leaves) == 0
}

func (ll *leavesToClose) closeAll() {
	for _, l := range ll.leaves {
		l.leaf.VirtualClose(l.closeCount)
	}
}

// attrRequestToAttributesMask converts a bitmap of NFSv4 attributes to
// their virtual file system counterparts. This method is used by
// GETATTR and READDIR to determine which attributes need to be
// requested.
func attrRequestToAttributesMask(attrRequest nfsv4.Bitmap4) virtual.AttributesMask {
	var attributesMask virtual.AttributesMask
	if len(attrRequest) > 0 {
		// Attributes 0 to 31.
		f := attrRequest[0]
		if f&uint32(1<<nfsv4.FATTR4_TYPE) != 0 {
			attributesMask |= virtual.AttributesMaskFileType
		}
		if f&uint32(1<<nfsv4.FATTR4_CHANGE) != 0 {
			attributesMask |= virtual.AttributesMaskChangeID
		}
		if f&uint32(1<<nfsv4.FATTR4_SIZE) != 0 {
			attributesMask |= virtual.AttributesMaskSizeBytes
		}
		if f&uint32(1<<nfsv4.FATTR4_FILEHANDLE) != 0 {
			attributesMask |= virtual.AttributesMaskFileHandle
		}
		if f&uint32(1<<nfsv4.FATTR4_FILEID) != 0 {
			attributesMask |= virtual.AttributesMaskInodeNumber
		}
	}
	if len(attrRequest) > 1 {
		// Attributes 32 to 63.
		f := attrRequest[1]
		if f&uint32(1<<(nfsv4.FATTR4_MODE-32)) != 0 {
			attributesMask |= virtual.AttributesMaskPermissions
		}
		if f&uint32(1<<(nfsv4.FATTR4_NUMLINKS-32)) != 0 {
			attributesMask |= virtual.AttributesMaskLinkCount
		}
	}
	return attributesMask
}

// deterministicNfstime4 is a timestamp that is reported as the access,
// metadata and modify time of all files. If these timestamps were not
// returned, clients would use 1970-01-01T00:00:00Z. As this tends to
// confuse many tools, a deterministic timestamp is used instead.
var deterministicNfstime4 = nfsv4.Nfstime4{
	Seconds: filesystem.DeterministicFileModificationTimestamp.Unix(),
}

func attributesMaskToBitmap4(in virtual.AttributesMask) []uint32 {
	out := make([]uint32, 2)
	if in&virtual.AttributesMaskPermissions != 0 {
		out[1] |= (1 << (nfsv4.FATTR4_MODE - 32))
	}
	if in&virtual.AttributesMaskSizeBytes != 0 {
		out[0] |= (1 << nfsv4.FATTR4_SIZE)
	}
	for len(out) > 0 && out[len(out)-1] == 0 {
		out = out[:len(out)-1]
	}
	return out
}

// nextSeqID increments a sequence ID according to the rules decribed in
// RFC 7530, section 9.1.3.
func nextSeqID(seqID nfsv4.Seqid4) nfsv4.Seqid4 {
	if seqID == math.MaxUint32 {
		return 1
	}
	return seqID + 1
}

// toShareMask converts NFSv4 share_access values that are part of OPEN
// and OPEN_DOWNGRADE requests to our equivalent ShareMask values.
func shareAccessToShareMask(in uint32) (virtual.ShareMask, nfsv4.Nfsstat4) {
	switch in {
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

// Even though no "." and ".." entries should be returned, the NFSv4
// spec requires that cookie values 0, 1 and 2 are never returned.
// Offset all responses by this value.
const lastReservedCookie = 2

// readdirReporter is an implementation of DirectoryEntryReporter that
// reports the contents of a directory in the NFSv4 directory entry
// format.
type readdirReporter struct {
	program     *baseProgram
	attrRequest nfsv4.Bitmap4
	maxCount    nfsv4.Count4
	dirCount    nfsv4.Count4

	currentMaxCount nfsv4.Count4
	currentDirCount nfsv4.Count4
	nextEntry       **nfsv4.Entry4
	endOfFile       *bool
}

func (r *readdirReporter) report(nextCookie uint64, name path.Component, attributes *virtual.Attributes) bool {
	// The dircount field is a hint of the maximum number of bytes
	// of directory information that should be returned. Only the
	// size of the XDR encoded filename and cookie should contribute
	// to its value.
	filename := name.String()
	if r.dirCount != 0 {
		r.currentDirCount += nfsv4.Count4(nfsv4.GetComponent4EncodedSizeBytes(filename) + nfsv4.NfsCookie4EncodedSizeBytes)
		if r.currentDirCount > r.dirCount {
			*r.endOfFile = false
			return false
		}
	}

	p := r.program
	entry := nfsv4.Entry4{
		Cookie: lastReservedCookie + nextCookie,
		Name:   filename,
		Attrs:  p.attributesToFattr4(attributes, r.attrRequest),
	}

	// The maxcount field is the maximum number of bytes for the
	// READDIR4resok structure.
	r.currentMaxCount += nfsv4.Count4(entry.GetEncodedSizeBytes())
	if r.currentMaxCount > r.maxCount {
		*r.endOfFile = false
		return false
	}

	*r.nextEntry = &entry
	r.nextEntry = &entry.Nextentry
	return true
}

func (r *readdirReporter) ReportDirectory(nextCookie uint64, name path.Component, directory virtual.Directory, attributes *virtual.Attributes) bool {
	return r.report(nextCookie, name, attributes)
}

func (r *readdirReporter) ReportLeaf(nextCookie uint64, name path.Component, leaf virtual.Leaf, attributes *virtual.Attributes) bool {
	return r.report(nextCookie, name, attributes)
}

// fattr4ToAttributes converts a client-provided NFSv4 fattr4 to a set
// of virtual file system attributes. Only attributes that are both
// writable and supported by this implementation are accepted.
func fattr4ToAttributes(in *nfsv4.Fattr4, out *virtual.Attributes) nfsv4.Nfsstat4 {
	r := bytes.NewBuffer(in.AttrVals)
	if len(in.Attrmask) > 0 {
		// Attributes 0 to 31.
		f := in.Attrmask[0]
		if f&^(1<<nfsv4.FATTR4_SIZE) != 0 {
			return nfsv4.NFS4ERR_ATTRNOTSUPP
		}
		if f&(1<<nfsv4.FATTR4_SIZE) != 0 {
			sizeBytes, _, err := nfsv4.ReadUint64T(r)
			if err != nil {
				return nfsv4.NFS4ERR_BADXDR
			}
			out.SetSizeBytes(sizeBytes)
		}
	}
	if len(in.Attrmask) > 1 {
		// Attributes 32 to 63.
		f := in.Attrmask[1]
		if f&^(1<<(nfsv4.FATTR4_MODE-32)) != 0 {
			return nfsv4.NFS4ERR_ATTRNOTSUPP
		}
		if f&(1<<(nfsv4.FATTR4_MODE-32)) != 0 {
			mode, _, err := nfsv4.ReadMode4(r)
			if err != nil {
				return nfsv4.NFS4ERR_BADXDR
			}
			out.SetPermissions(virtual.NewPermissionsFromMode(mode))
		}
	}
	for i := 2; i < len(in.Attrmask); i++ {
		// Attributes 64 or higher.
		if in.Attrmask[i] != 0 {
			return nfsv4.NFS4ERR_ATTRNOTSUPP
		}
	}
	if r.Len() != 0 {
		// Provided attributes contain trailing data.
		return nfsv4.NFS4ERR_BADXDR
	}
	return nfsv4.NFS4_OK
}

// transactionShouldComplete returns whether a transaction should be
// completed, based on the resulting status code of the transaction.
// Even in the case where an errors occurs, should the sequence number
// of the client be advanced. The only exception is if the operation
// fails with any of the errors listed below.
//
// More details: RFC 7530, section 9.1.7, last paragraph.
func transactionShouldComplete(st nfsv4.Nfsstat4) bool {
	return st != nfsv4.NFS4ERR_STALE_CLIENTID &&
		st != nfsv4.NFS4ERR_STALE_STATEID &&
		st != nfsv4.NFS4ERR_BAD_STATEID &&
		st != nfsv4.NFS4ERR_BAD_SEQID &&
		st != nfsv4.NFS4ERR_BADXDR &&
		st != nfsv4.NFS4ERR_RESOURCE &&
		st != nfsv4.NFS4ERR_NOFILEHANDLE &&
		st != nfsv4.NFS4ERR_MOVED
}

// compareStateSeqID compares a client-provided sequence ID value with
// one present on the server. The error that needs to be returned in
// case of non-matching sequence IDs depends on whether the value lies
// in the past or future.
//
// More details: RFC 7530, section 9.1.3, last paragraph.
func compareStateSeqID(clientValue, serverValue nfsv4.Seqid4) nfsv4.Nfsstat4 {
	if clientValue == serverValue {
		return nfsv4.NFS4_OK
	}
	if int32(clientValue-serverValue) > 0 {
		return nfsv4.NFS4ERR_BAD_STATEID
	}
	return nfsv4.NFS4ERR_OLD_STATEID
}

// nfsLockType4ToByteRangeLockType converts an NFSv4 lock type to a
// virtual file system byte range lock type. As this implementation does
// not attempt to provide any fairness, no distinction is made between
// waiting and non-waiting lock type variants.
func nfsLockType4ToByteRangeLockType(in nfsv4.NfsLockType4) (virtual.ByteRangeLockType, nfsv4.Nfsstat4) {
	switch in {
	case nfsv4.READ_LT, nfsv4.READW_LT:
		return virtual.ByteRangeLockTypeLockedShared, nfsv4.NFS4_OK
	case nfsv4.WRITE_LT, nfsv4.WRITEW_LT:
		return virtual.ByteRangeLockTypeLockedExclusive, nfsv4.NFS4_OK
	default:
		return 0, nfsv4.NFS4ERR_INVAL
	}
}

// offsetLengthToStartEnd converts an (offset, length) pair to a
// (start, end) pair. The former is used by NFSv4, while the latter is
// used by ByteRangeLock.
//
// More details: RFC 7530, section 16.10.4, paragraph 2.
func offsetLengthToStartEnd(offset, length uint64) (uint64, uint64, nfsv4.Nfsstat4) {
	switch length {
	case 0:
		return 0, 0, nfsv4.NFS4ERR_INVAL
	case math.MaxUint64:
		// A length of all ones indicates end-of-file.
		return offset, math.MaxUint64, nfsv4.NFS4_OK
	default:
		if length > math.MaxUint64-offset {
			// The end exceeds the maximum 64-bit unsigned
			// integer value.
			return 0, 0, nfsv4.NFS4ERR_INVAL
		}
		return offset, offset + length, nfsv4.NFS4_OK
	}
}

// byteRangeLockToLock4Denied converts information on a conflicting byte
// range lock into a LOCK4denied response.
func byteRangeLockToLock4Denied(lock *virtual.ByteRangeLock[*lockOwnerState]) nfsv4.Lock4denied {
	length := uint64(math.MaxUint64)
	if lock.End != math.MaxUint64 {
		length = lock.End - lock.Start
	}
	var lockType nfsv4.NfsLockType4
	switch lock.Type {
	case virtual.ByteRangeLockTypeLockedShared:
		lockType = nfsv4.READ_LT
	case virtual.ByteRangeLockTypeLockedExclusive:
		lockType = nfsv4.WRITE_LT
	default:
		panic("Unexpected lock type")
	}
	los := lock.Owner
	return nfsv4.Lock4denied{
		Offset:   lock.Start,
		Length:   length,
		Locktype: lockType,
		Owner: nfsv4.LockOwner4{
			Clientid: los.confirmedClient.confirmation.key.shortClientID,
			Owner:    los.owner,
		},
	}
}

// nfsv4NewComponent converts a filename string that's provided as part
// of an incoming request to a pathname component that can be provided
// to the virtual file system layer.
func nfsv4NewComponent(name string) (path.Component, nfsv4.Nfsstat4) {
	if name == "" {
		// Inherently invalid name.
		return path.Component{}, nfsv4.NFS4ERR_INVAL
	}
	component, ok := path.NewComponent(name)
	if !ok {
		// Name that is invalid for this implementation.
		return path.Component{}, nfsv4.NFS4ERR_BADNAME
	}
	return component, nfsv4.NFS4_OK
}
