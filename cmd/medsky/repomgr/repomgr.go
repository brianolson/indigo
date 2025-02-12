package repomgr

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	atproto "github.com/bluesky-social/indigo/api/atproto"
	lexutil "github.com/bluesky-social/indigo/lex/util"
	"github.com/bluesky-social/indigo/models"
	"github.com/bluesky-social/indigo/mst"
	"github.com/bluesky-social/indigo/repo"
	blockformat "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	blockstore "github.com/ipfs/go-ipfs-blockstore"
	cbor "github.com/ipfs/go-ipld-cbor"
	"github.com/ipld/go-car"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"gorm.io/gorm"
	"io"
	"log/slog"
	"strings"
	"sync"
)

func NewRepoManager(kmgr KeyManager) *RepoManager {

	return &RepoManager{
		userLocks: make(map[models.Uid]*userLock),
		kmgr:      kmgr,
		log:       slog.Default().With("system", "repomgr"),
		noArchive: true,
	}
}

type KeyManager interface {
	VerifyUserSignature(context.Context, string, []byte, []byte) error
	SignForUser(context.Context, string, []byte) ([]byte, error)
}

func (rm *RepoManager) SetEventHandler(cb func(context.Context, *RepoEvent), hydrateRecords bool) {
	rm.events = cb
	rm.hydrateRecords = hydrateRecords
}

type RepoManager struct {
	kmgr KeyManager

	lklk      sync.Mutex
	userLocks map[models.Uid]*userLock

	events         func(context.Context, *RepoEvent)
	hydrateRecords bool

	log       *slog.Logger
	noArchive bool
}

type ActorInfo struct {
	Did         string
	Handle      string
	DisplayName string
	Type        string
}

type RepoEvent struct {
	User      models.Uid
	OldRoot   *cid.Cid
	NewRoot   cid.Cid
	Since     *string
	Rev       string
	RepoSlice []byte
	PDS       uint
	Ops       []RepoOp
}

type RepoOp struct {
	Kind       EventKind
	Collection string
	Rkey       string
	RecCid     *cid.Cid
	Record     any
	ActorInfo  *ActorInfo
}

type EventKind string

const (
	EvtKindCreateRecord = EventKind("create")
	EvtKindUpdateRecord = EventKind("update")
	EvtKindDeleteRecord = EventKind("delete")
)

type RepoHead struct {
	gorm.Model
	Usr  models.Uid `gorm:"uniqueIndex"`
	Root string
}

type userLock struct {
	lk    sync.Mutex
	count int
}

func (rm *RepoManager) lockUser(ctx context.Context, user models.Uid) func() {
	ctx, span := otel.Tracer("repoman").Start(ctx, "userLock")
	defer span.End()

	rm.lklk.Lock()

	ulk, ok := rm.userLocks[user]
	if !ok {
		ulk = &userLock{}
		rm.userLocks[user] = ulk
	}

	ulk.count++

	rm.lklk.Unlock()

	ulk.lk.Lock()

	return func() {
		rm.lklk.Lock()

		ulk.lk.Unlock()
		ulk.count--

		if ulk.count == 0 {
			delete(rm.userLocks, user)
		}
		rm.lklk.Unlock()
	}
}

func (rm *RepoManager) CheckRepoSig(ctx context.Context, r *repo.Repo, expdid string) error {
	ctx, span := otel.Tracer("repoman").Start(ctx, "CheckRepoSig")
	defer span.End()

	repoDid := r.RepoDid()
	if expdid != repoDid {
		return fmt.Errorf("DID in repo did not match (%q != %q)", expdid, repoDid)
	}

	scom := r.SignedCommit()

	usc := scom.Unsigned()
	sb, err := usc.BytesForSigning()
	if err != nil {
		return fmt.Errorf("commit serialization failed: %w", err)
	}
	if err := rm.kmgr.VerifyUserSignature(ctx, repoDid, scom.Sig, sb); err != nil {
		return fmt.Errorf("signature check failed (sig: %x) (sb: %x) : %w", scom.Sig, sb, err)
	}

	return nil
}

var ErrRepoBaseMismatch = fmt.Errorf("attempted a delta session on top of the wrong previous head")

var ErrNoRootBlock = errors.New("no root block")

type mapIpldBlockstore struct {
	blocks map[cid.Cid]blockformat.Block
}

func (m mapIpldBlockstore) Get(ctx context.Context, c cid.Cid) (blockformat.Block, error) {
	v, ok := m.blocks[c]
	if ok {
		return v, nil
	}
	return nil, cbor.ErrEmptyLink
}

func (m mapIpldBlockstore) Put(ctx context.Context, block blockformat.Block) error {
	m.blocks[block.Cid()] = block
	return nil
}

var _ cbor.IpldBlockstore = &mapIpldBlockstore{}

// parse car slice, check signature, return repo fragment accessor
// TODO: rename
func (rm *RepoManager) checkSliceSignature(ctx context.Context, repoDid string, carslice []byte) (repoFragment *repo.Repo, root cid.Cid, err error) {
	carr, err := car.NewCarReader(bytes.NewReader(carslice))
	if err != nil {
		return nil, root, err
	}
	if len(carr.Header.Roots) != 1 {
		return nil, root, fmt.Errorf("invalid car file, header must have a single root (has %d)", len(carr.Header.Roots))
	}
	cablocks := make(map[cid.Cid]blockformat.Block)
	for {
		blk, err := carr.Next()
		if err != nil {
			if err == io.EOF {
				break
			}
			return nil, root, fmt.Errorf("invalid car file: %w", err)
		}
		cablocks[blk.Cid()] = blk
	}
	rootblock, ok := cablocks[carr.Header.Roots[0]]
	if !ok {
		return nil, root, ErrNoRootBlock
	}
	var sc repo.SignedCommit
	err = sc.UnmarshalCBOR(bytes.NewReader(rootblock.RawData()))
	if err != nil {
		return nil, root, fmt.Errorf("root block bad cbor, %w", err)
	}
	usc := sc.Unsigned()
	sb, err := usc.BytesForSigning()
	if err != nil {
		return nil, root, fmt.Errorf("commit serialization failed: %w", err)
	}
	if err := rm.kmgr.VerifyUserSignature(ctx, repoDid, sc.Sig, sb); err != nil {
		return nil, root, fmt.Errorf("signature check failed (sig: %x) (sb: %x) : %w", sc.Sig, sb, err)
	}

	bs := &mapIpldBlockstore{blocks: cablocks}
	carepo, err := repo.OpenRepo(ctx, bs, carr.Header.Roots[0])
	if err != nil {
		return nil, root, fmt.Errorf("car as repo fragment (%s, root=%s): %w", repoDid, carr.Header.Roots[0], err)
	}

	root = carr.Header.Roots[0]
	return carepo, root, nil
}

// TODO: move this to its own thing out of repomgr
func (rm *RepoManager) HandleExternalUserEvent(ctx context.Context, pdsid uint, uid models.Uid, did string, since *string, nrev string, carslice []byte, ops []*atproto.SyncSubscribeRepos_RepoOp) error {
	return rm.handleExternalUserEventNoArchive(ctx, pdsid, uid, did, since, nrev, carslice, ops)
}

func (rm *RepoManager) handleExternalUserEventNoArchive(ctx context.Context, pdsid uint, uid models.Uid, did string, since *string, nrev string, carslice []byte, ops []*atproto.SyncSubscribeRepos_RepoOp) error {
	ctx, span := otel.Tracer("repoman").Start(ctx, "HandleExternalUserEvent")
	defer span.End()

	span.SetAttributes(attribute.Int64("uid", int64(uid)))

	rm.log.Debug("HandleExternalUserEvent", "pds", pdsid, "uid", uid, "since", since, "nrev", nrev)

	unlock := rm.lockUser(ctx, uid)
	defer unlock()

	repoFragment, root, err := rm.checkSliceSignature(ctx, did, carslice)
	if err != nil {
		return err
	}

	evtops := make([]RepoOp, 0, len(ops))
	for _, op := range ops {
		parts := strings.SplitN(op.Path, "/", 2)
		if len(parts) != 2 {
			return fmt.Errorf("invalid rpath in mst diff, must have collection and rkey")
		}

		switch EventKind(op.Action) {
		case EvtKindCreateRecord:
			rop := RepoOp{
				Kind:       EvtKindCreateRecord,
				Collection: parts[0],
				Rkey:       parts[1],
				RecCid:     (*cid.Cid)(op.Cid),
			}

			if rm.hydrateRecords {
				_, rec, err := repoFragment.GetRecord(ctx, op.Path)
				if err != nil {
					return fmt.Errorf("reading changed record from car slice: %w", err)
				}
				rop.Record = rec
			}

			evtops = append(evtops, rop)
		case EvtKindUpdateRecord:
			rop := RepoOp{
				Kind:       EvtKindUpdateRecord,
				Collection: parts[0],
				Rkey:       parts[1],
				RecCid:     (*cid.Cid)(op.Cid),
			}

			if rm.hydrateRecords {
				_, rec, err := repoFragment.GetRecord(ctx, op.Path)
				if err != nil {
					return fmt.Errorf("reading changed record from car slice: %w", err)
				}

				rop.Record = rec
			}

			evtops = append(evtops, rop)
		case EvtKindDeleteRecord:
			evtops = append(evtops, RepoOp{
				Kind:       EvtKindDeleteRecord,
				Collection: parts[0],
				Rkey:       parts[1],
			})
		default:
			return fmt.Errorf("unrecognized external user event kind: %q", op.Action)
		}
	}

	if rm.events != nil {
		rm.events(ctx, &RepoEvent{
			User: uid,
			//OldRoot:   prev,
			NewRoot:   root,
			Rev:       nrev,
			Since:     since,
			Ops:       evtops,
			RepoSlice: carslice,
			PDS:       pdsid,
		})
	}

	return nil
}

func (rm *RepoManager) processOp(ctx context.Context, bs blockstore.Blockstore, op *mst.DiffOp, hydrateRecords bool) (*RepoOp, error) {
	parts := strings.SplitN(op.Rpath, "/", 2)
	if len(parts) != 2 {
		return nil, fmt.Errorf("repo mst had invalid rpath: %q", op.Rpath)
	}

	switch op.Op {
	case "add", "mut":

		kind := EvtKindCreateRecord
		if op.Op == "mut" {
			kind = EvtKindUpdateRecord
		}

		outop := &RepoOp{
			Kind:       kind,
			Collection: parts[0],
			Rkey:       parts[1],
			RecCid:     &op.NewCid,
		}

		if hydrateRecords {
			blk, err := bs.Get(ctx, op.NewCid)
			if err != nil {
				return nil, err
			}

			rec, err := lexutil.CborDecodeValue(blk.RawData())
			if err != nil {
				if !errors.Is(err, lexutil.ErrUnrecognizedType) {
					return nil, err
				}

				rm.log.Warn("failed processing repo diff", "err", err)
			} else {
				outop.Record = rec
			}
		}

		return outop, nil
	case "del":
		return &RepoOp{
			Kind:       EvtKindDeleteRecord,
			Collection: parts[0],
			Rkey:       parts[1],
			RecCid:     nil,
		}, nil

	default:
		return nil, fmt.Errorf("diff returned invalid op type: %q", op.Op)
	}
}

//
//// walkTree returns all cids linked recursively by the root, skipping any cids
//// in the 'skip' map, and not erroring on 'not found' if prevMissing is set
//func (rm *RepoManager) walkTree(ctx context.Context, skip map[cid.Cid]bool, root cid.Cid, bs blockstore.Blockstore, prevMissing bool) ([]cid.Cid, error) {
//	// TODO: what if someone puts non-cbor links in their repo?
//	if root.Prefix().Codec != cid.DagCBOR {
//		return nil, fmt.Errorf("can only handle dag-cbor objects in repos (%s is %d)", root, root.Prefix().Codec)
//	}
//
//	blk, err := bs.Get(ctx, root)
//	if err != nil {
//		return nil, err
//	}
//
//	var links []cid.Cid
//	if err := cbg.ScanForLinks(bytes.NewReader(blk.RawData()), func(c cid.Cid) {
//		if c.Prefix().Codec == cid.Raw {
//			rm.log.Debug("skipping 'raw' CID in record", "recordCid", root, "rawCid", c)
//			return
//		}
//		if skip[c] {
//			return
//		}
//
//		links = append(links, c)
//		skip[c] = true
//
//		return
//	}); err != nil {
//		return nil, err
//	}
//
//	out := []cid.Cid{root}
//	skip[root] = true
//
//	// TODO: should do this non-recursive since i expect these may get deep
//	for _, c := range links {
//		sub, err := rm.walkTree(ctx, skip, c, bs, prevMissing)
//		if err != nil {
//			if prevMissing && !ipld.IsNotFound(err) {
//				return nil, err
//			}
//		}
//
//		out = append(out, sub...)
//	}
//
//	return out, nil
//}
