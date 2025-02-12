package repomgr

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	atproto "github.com/bluesky-social/indigo/api/atproto"
	"github.com/bluesky-social/indigo/events"
	"github.com/bluesky-social/indigo/models"
	"github.com/bluesky-social/indigo/repo"
	blockformat "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	cbor "github.com/ipfs/go-ipld-cbor"
	"github.com/ipld/go-car"
	"go.opentelemetry.io/otel"
	"gorm.io/gorm"
	"io"
	"log/slog"
	"sync"
)

func NewRepoManager(kmgr *KeyManager) *RepoManager {

	return &RepoManager{
		userLocks: make(map[models.Uid]*userLock),
		kmgr:      kmgr,
		log:       slog.Default().With("system", "repomgr"),
		noArchive: true,
	}
}

func (rm *RepoManager) SetEventManager(events *events.EventManager) {
	rm.events = events
}

type RepoManager struct {
	kmgr *KeyManager

	lklk      sync.Mutex
	userLocks map[models.Uid]*userLock

	events *events.EventManager

	log       *slog.Logger
	noArchive bool
}

type NextCommitHandler interface {
	HandleCommit(ctx context.Context, host *models.PDS, uid models.Uid, did string, commit *atproto.SyncSubscribeRepos_Commit) error
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
	return checkSliceSignature(ctx, rm.kmgr, repoDid, carslice)
}

// TODO: maybe this _doesn't_ need to return (repoFragement,root), but maybe still want them for induction firehose?
func checkSliceSignature(ctx context.Context, kmgr *KeyManager, repoDid string, carslice []byte) (repoFragment *repo.Repo, root cid.Cid, err error) {
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
	if err := kmgr.VerifyUserSignature(ctx, repoDid, sc.Sig, sb); err != nil {
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

type IUser interface {
	GetUid() models.Uid
	GetDid() string
}

// TODO: move this to its own thing out of repomgr
func (rm *RepoManager) HandleCommit(ctx context.Context, host *models.PDS, user IUser, commit *atproto.SyncSubscribeRepos_Commit) error {
	uid := user.GetUid()
	unlock := rm.lockUser(ctx, uid)
	defer unlock()
	_, _, err := rm.checkSliceSignature(ctx, user.GetDid(), commit.Blocks)
	if err != nil {
		return err
	}
	if rm.events != nil {
		xe := &events.XRPCStreamEvent{
			RepoCommit: commit,
			PrivUid:    uid,
		}
		err = rm.events.AddEvent(ctx, xe)
		if err != nil {
			rm.log.Error("events handle commit", "err", err)
		}
	}
	return nil
}
