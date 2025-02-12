package repomgr

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	blockformat "github.com/ipfs/go-block-format"
	"io"
	"log/slog"
	"strings"
	"sync"
	"time"

	atproto "github.com/bluesky-social/indigo/api/atproto"
	lexutil "github.com/bluesky-social/indigo/lex/util"
	"github.com/bluesky-social/indigo/models"
	"github.com/bluesky-social/indigo/mst"
	"github.com/bluesky-social/indigo/repo"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	blockstore "github.com/ipfs/go-ipfs-blockstore"
	ipld "github.com/ipfs/go-ipld-format"
	"github.com/ipld/go-car"
	cbg "github.com/whyrusleeping/cbor-gen"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"gorm.io/gorm"
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

// subset of blockstore.Blockstore that we actually use here
type minBlockstore interface {
	Get(ctx context.Context, bcid cid.Cid) (blockformat.Block, error)
	Has(ctx context.Context, bcid cid.Cid) (bool, error)
	GetSize(ctx context.Context, bcid cid.Cid) (int, error)
}

// shardWriter.writeNewShard called from inside DeltaSession.CloseWithRoot
type shardWriter interface {
	// writeNewShard stores blocks in `blks` arg and creates a new shard to propagate out to our firehose
	writeNewShard(ctx context.Context, root cid.Cid, rev string, user models.Uid, seq int, blks map[cid.Cid]blockformat.Block, rmcids map[cid.Cid]bool) ([]byte, error)
}

// minimal pds-repo state for classic narelay firehose
type commitRefInfo struct {
	ID   uint       `gorm:"primarykey"`
	Uid  models.Uid `gorm:"uniqueIndex"`
	Rev  string
	Root models.DbCID
}

var commitRefZero = commitRefInfo{}

type DeltaSession struct {
	blks     map[cid.Cid]blockformat.Block
	rmcids   map[cid.Cid]bool
	base     minBlockstore
	user     models.Uid
	baseCid  cid.Cid
	seq      int
	readonly bool
	cs       shardWriter
	lastRev  string
}

// userView needs these things to get into the underlying block store
// implemented by CarStoreGormMeta
type userViewSource interface {
	HasUidCid(ctx context.Context, user models.Uid, k cid.Cid) (bool, error)
	LookupBlockRef(ctx context.Context, k cid.Cid) (path string, offset int64, user models.Uid, err error)
}

// wrapper into a block store that keeps track of which user we are working on behalf of
type userView struct {
	cs   userViewSource
	user models.Uid

	cache    map[cid.Cid]blockformat.Block
	prefetch bool
}

func getCommitRefInfo(ctx context.Context, user models.Uid) (*commitRefInfo, error) {
	return nil, nil
}

var ErrRepoBaseMismatch = fmt.Errorf("attempted a delta session on top of the wrong previous head")

func NewDeltaSession(ctx context.Context, user models.Uid, since *string) (*DeltaSession, error) {
	ctx, span := otel.Tracer("carstore").Start(ctx, "NewSession")
	defer span.End()

	// TODO: ensure that we don't write updates on top of the wrong head
	// this needs to be a compare and swap type operation
	// TODO: need some db connection here
	lastShard, err := getCommitRefInfo(ctx, user)
	if err != nil {
		return nil, err
	}

	if lastShard == nil {
		// ok, no previous user state to refer to
		lastShard = &commitRefZero
	} else if since != nil && *since != lastShard.Rev {
		slog.Warn("revision mismatch", "commitSince", since, "lastRev", lastShard.Rev, "err", ErrRepoBaseMismatch)
	}

	return &DeltaSession{
		blks: make(map[cid.Cid]blockformat.Block),
		base: nil,
		//base: &userView{
		//	user:     user,
		//	cs:       nil, // TODO: this should be okay?
		//	prefetch: true,
		//	cache:    make(map[cid.Cid]blockformat.Block),
		//},
		user:    user,
		baseCid: lastShard.Root.CID,
		cs:      nil,
		seq:     0,
		lastRev: lastShard.Rev,
	}, nil
}
func ImportSlice(ctx context.Context, uid models.Uid, since *string, carslice []byte) (cid.Cid, *DeltaSession, error) {
	ctx, span := otel.Tracer("carstore").Start(ctx, "ImportSlice")
	defer span.End()

	carr, err := car.NewCarReader(bytes.NewReader(carslice))
	if err != nil {
		return cid.Undef, nil, err
	}

	if len(carr.Header.Roots) != 1 {
		return cid.Undef, nil, fmt.Errorf("invalid car file, header must have a single root (has %d)", len(carr.Header.Roots))
	}

	ds, err := NewDeltaSession(ctx, uid, since)
	if err != nil {
		return cid.Undef, nil, fmt.Errorf("new delta session failed: %w", err)
	}

	var cids []cid.Cid
	for {
		blk, err := carr.Next()
		if err != nil {
			if err == io.EOF {
				break
			}
			return cid.Undef, nil, err
		}

		cids = append(cids, blk.Cid())

		if err := ds.Put(ctx, blk); err != nil {
			return cid.Undef, nil, err
		}
	}

	return carr.Header.Roots[0], ds, nil
}

// TODO: move this to its own thing out of repomgr
func (rm *RepoManager) HandleExternalUserEvent(ctx context.Context, pdsid uint, uid models.Uid, did string, since *string, nrev string, carslice []byte, ops []*atproto.SyncSubscribeRepos_RepoOp) error {
	if rm.noArchive {
		return rm.handleExternalUserEventNoArchive(ctx, pdsid, uid, did, since, nrev, carslice, ops)
	} else {
		return rm.handleExternalUserEventArchive(ctx, pdsid, uid, did, since, nrev, carslice, ops)
	}
}

func (rm *RepoManager) handleExternalUserEventNoArchive(ctx context.Context, pdsid uint, uid models.Uid, did string, since *string, nrev string, carslice []byte, ops []*atproto.SyncSubscribeRepos_RepoOp) error {
	ctx, span := otel.Tracer("repoman").Start(ctx, "HandleExternalUserEvent")
	defer span.End()

	span.SetAttributes(attribute.Int64("uid", int64(uid)))

	rm.log.Debug("HandleExternalUserEvent", "pds", pdsid, "uid", uid, "since", since, "nrev", nrev)

	unlock := rm.lockUser(ctx, uid)
	defer unlock()

	// TODO: replace ImportSlice with something that _just_ gets the commit block and checks the signature; drop the rest of the "repo" trappings
	//start := time.Now()
	//root, ds, err := rm.cs.ImportSlice(ctx, uid, since, carslice)
	//if err != nil {
	//	return fmt.Errorf("importing external carslice: %w", err)
	//}
	//
	//r, err := repo.OpenRepo(ctx, ds, root)
	//if err != nil {
	//	return fmt.Errorf("opening external user repo (%d, root=%s): %w", uid, root, err)
	//}
	//
	//if err := rm.CheckRepoSig(ctx, r, did); err != nil {
	//	return fmt.Errorf("check repo sig: %w", err)
	//}
	//openAndSigCheckDuration.Observe(time.Since(start).Seconds())

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
				_, rec, err := r.GetRecord(ctx, op.Path)
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
				_, rec, err := r.GetRecord(ctx, op.Path)
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

func (rm *RepoManager) handleExternalUserEventArchive(ctx context.Context, pdsid uint, uid models.Uid, did string, since *string, nrev string, carslice []byte, ops []*atproto.SyncSubscribeRepos_RepoOp) error {
	ctx, span := otel.Tracer("repoman").Start(ctx, "HandleExternalUserEvent")
	defer span.End()

	span.SetAttributes(attribute.Int64("uid", int64(uid)))

	rm.log.Debug("HandleExternalUserEvent", "pds", pdsid, "uid", uid, "since", since, "nrev", nrev)

	unlock := rm.lockUser(ctx, uid)
	defer unlock()

	start := time.Now()
	root, ds, err := rm.cs.ImportSlice(ctx, uid, since, carslice)
	if err != nil {
		return fmt.Errorf("importing external carslice: %w", err)
	}

	r, err := repo.OpenRepo(ctx, ds, root)
	if err != nil {
		return fmt.Errorf("opening external user repo (%d, root=%s): %w", uid, root, err)
	}

	if err := rm.CheckRepoSig(ctx, r, did); err != nil {
		return err
	}
	openAndSigCheckDuration.Observe(time.Since(start).Seconds())

	var skipcids map[cid.Cid]bool
	if ds.BaseCid().Defined() {
		oldrepo, err := repo.OpenRepo(ctx, ds, ds.BaseCid())
		if err != nil {
			return fmt.Errorf("failed to check data root in old repo: %w", err)
		}

		// if the old commit has a 'prev', CalcDiff will error out while trying
		// to walk it. This is an old repo thing that is being deprecated.
		// This check is a temporary workaround until all repos get migrated
		// and this becomes no longer an issue
		prev, _ := oldrepo.PrevCommit(ctx)
		if prev != nil {
			skipcids = map[cid.Cid]bool{
				*prev: true,
			}
		}
	}

	start = time.Now()
	if err := ds.CalcDiff(ctx, skipcids); err != nil {
		return fmt.Errorf("failed while calculating mst diff (since=%v): %w", since, err)
	}
	calcDiffDuration.Observe(time.Since(start).Seconds())

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
				_, rec, err := r.GetRecord(ctx, op.Path)
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
				_, rec, err := r.GetRecord(ctx, op.Path)
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

	start = time.Now()
	rslice, err := ds.CloseWithRoot(ctx, root, nrev)
	if err != nil {
		return fmt.Errorf("close with root: %w", err)
	}
	writeCarSliceDuration.Observe(time.Since(start).Seconds())

	if rm.events != nil {
		rm.events(ctx, &RepoEvent{
			User: uid,
			//OldRoot:   prev,
			NewRoot:   root,
			Rev:       nrev,
			Since:     since,
			Ops:       evtops,
			RepoSlice: rslice,
			PDS:       pdsid,
		})
	}

	return nil
}

func (rm *RepoManager) ImportNewRepo(ctx context.Context, user models.Uid, repoDid string, r io.Reader, rev *string) error {
	ctx, span := otel.Tracer("repoman").Start(ctx, "ImportNewRepo")
	defer span.End()

	unlock := rm.lockUser(ctx, user)
	defer unlock()

	currev, err := rm.cs.GetUserRepoRev(ctx, user)
	if err != nil {
		return err
	}

	curhead, err := rm.cs.GetUserRepoHead(ctx, user)
	if err != nil {
		return err
	}

	if rev != nil && *rev == "" {
		rev = nil
	}
	if rev == nil {
		// if 'rev' is nil, this implies a fresh sync.
		// in this case, ignore any existing blocks we have and treat this like a clean import.
		curhead = cid.Undef
	}

	if rev != nil && *rev != currev {
		// TODO: we could probably just deal with this
		return fmt.Errorf("ImportNewRepo called with incorrect base")
	}

	err = rm.processNewRepo(ctx, user, r, rev, func(ctx context.Context, root cid.Cid, finish func(context.Context, string) ([]byte, error), bs blockstore.Blockstore) error {
		r, err := repo.OpenRepo(ctx, bs, root)
		if err != nil {
			return fmt.Errorf("opening new repo: %w", err)
		}

		scom := r.SignedCommit()

		usc := scom.Unsigned()
		sb, err := usc.BytesForSigning()
		if err != nil {
			return fmt.Errorf("commit serialization failed: %w", err)
		}
		if err := rm.kmgr.VerifyUserSignature(ctx, repoDid, scom.Sig, sb); err != nil {
			return fmt.Errorf("new user signature check failed: %w", err)
		}

		diffops, err := r.DiffSince(ctx, curhead)
		if err != nil {
			return fmt.Errorf("diff trees (curhead: %s): %w", curhead, err)
		}

		ops := make([]RepoOp, 0, len(diffops))
		for _, op := range diffops {
			repoOpsImported.Inc()
			out, err := rm.processOp(ctx, bs, op, rm.hydrateRecords)
			if err != nil {
				rm.log.Error("failed to process repo op", "err", err, "path", op.Rpath, "repo", repoDid)
			}

			if out != nil {
				ops = append(ops, *out)
			}
		}

		slice, err := finish(ctx, scom.Rev)
		if err != nil {
			return err
		}

		if rm.events != nil {
			rm.events(ctx, &RepoEvent{
				User: user,
				//OldRoot:   oldroot,
				NewRoot:   root,
				Rev:       scom.Rev,
				Since:     &currev,
				RepoSlice: slice,
				Ops:       ops,
			})
		}

		return nil
	})
	if err != nil {
		return fmt.Errorf("process new repo (current rev: %s): %w:", currev, err)
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

func (rm *RepoManager) processNewRepo(ctx context.Context, user models.Uid, r io.Reader, rev *string, cb func(ctx context.Context, root cid.Cid, finish func(context.Context, string) ([]byte, error), bs blockstore.Blockstore) error) error {
	ctx, span := otel.Tracer("repoman").Start(ctx, "processNewRepo")
	defer span.End()

	carr, err := car.NewCarReader(r)
	if err != nil {
		return err
	}

	if len(carr.Header.Roots) != 1 {
		return fmt.Errorf("invalid car file, header must have a single root (has %d)", len(carr.Header.Roots))
	}

	membs := blockstore.NewBlockstore(datastore.NewMapDatastore())

	for {
		blk, err := carr.Next()
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}

		if err := membs.Put(ctx, blk); err != nil {
			return err
		}
	}

	seen := make(map[cid.Cid]bool)

	root := carr.Header.Roots[0]
	// TODO: if there are blocks that get convergently recreated throughout
	// the repos lifecycle, this will end up erroneously not including
	// them. We should compute the set of blocks needed to read any repo
	// ops that happened in the commit and use that for our 'output' blocks
	cids, err := rm.walkTree(ctx, seen, root, membs, true)
	if err != nil {
		return fmt.Errorf("walkTree: %w", err)
	}

	ds, err := rm.cs.NewDeltaSession(ctx, user, rev)
	if err != nil {
		return fmt.Errorf("opening delta session: %w", err)
	}

	for _, c := range cids {
		blk, err := membs.Get(ctx, c)
		if err != nil {
			return fmt.Errorf("copying walked cids to carstore: %w", err)
		}

		if err := ds.Put(ctx, blk); err != nil {
			return err
		}
	}

	finish := func(ctx context.Context, nrev string) ([]byte, error) {
		return ds.CloseWithRoot(ctx, root, nrev)
	}

	if err := cb(ctx, root, finish, ds); err != nil {
		return fmt.Errorf("cb errored root: %s, rev: %s: %w", root, stringOrNil(rev), err)
	}

	return nil
}

func stringOrNil(s *string) string {
	if s == nil {
		return "nil"
	}
	return *s
}

// walkTree returns all cids linked recursively by the root, skipping any cids
// in the 'skip' map, and not erroring on 'not found' if prevMissing is set
func (rm *RepoManager) walkTree(ctx context.Context, skip map[cid.Cid]bool, root cid.Cid, bs blockstore.Blockstore, prevMissing bool) ([]cid.Cid, error) {
	// TODO: what if someone puts non-cbor links in their repo?
	if root.Prefix().Codec != cid.DagCBOR {
		return nil, fmt.Errorf("can only handle dag-cbor objects in repos (%s is %d)", root, root.Prefix().Codec)
	}

	blk, err := bs.Get(ctx, root)
	if err != nil {
		return nil, err
	}

	var links []cid.Cid
	if err := cbg.ScanForLinks(bytes.NewReader(blk.RawData()), func(c cid.Cid) {
		if c.Prefix().Codec == cid.Raw {
			rm.log.Debug("skipping 'raw' CID in record", "recordCid", root, "rawCid", c)
			return
		}
		if skip[c] {
			return
		}

		links = append(links, c)
		skip[c] = true

		return
	}); err != nil {
		return nil, err
	}

	out := []cid.Cid{root}
	skip[root] = true

	// TODO: should do this non-recursive since i expect these may get deep
	for _, c := range links {
		sub, err := rm.walkTree(ctx, skip, c, bs, prevMissing)
		if err != nil {
			if prevMissing && !ipld.IsNotFound(err) {
				return nil, err
			}
		}

		out = append(out, sub...)
	}

	return out, nil
}

func (rm *RepoManager) TakeDownRepo(ctx context.Context, uid models.Uid) error {
	unlock := rm.lockUser(ctx, uid)
	defer unlock()

	return rm.cs.WipeUserData(ctx, uid)
}

// technically identical to TakeDownRepo, for now
func (rm *RepoManager) ResetRepo(ctx context.Context, uid models.Uid) error {
	unlock := rm.lockUser(ctx, uid)
	defer unlock()

	return rm.cs.WipeUserData(ctx, uid)
}

func (rm *RepoManager) VerifyRepo(ctx context.Context, uid models.Uid) error {
	ses, err := rm.cs.ReadOnlySession(uid)
	if err != nil {
		return err
	}

	r, err := repo.OpenRepo(ctx, ses, ses.BaseCid())
	if err != nil {
		return err
	}

	if err := r.ForEach(ctx, "", func(k string, v cid.Cid) error {
		_, err := ses.Get(ctx, v)
		if err != nil {
			return fmt.Errorf("failed to get record %s (%s): %w", k, v, err)
		}

		return nil
	}); err != nil {
		return err
	}

	return nil
}
