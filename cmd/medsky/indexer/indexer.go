package indexer

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	comatproto "github.com/bluesky-social/indigo/api/atproto"
	"github.com/bluesky-social/indigo/cmd/medsky/repomgr"
	"github.com/bluesky-social/indigo/did"
	"github.com/bluesky-social/indigo/events"
	lexutil "github.com/bluesky-social/indigo/lex/util"
	"github.com/bluesky-social/indigo/models"
	"github.com/bluesky-social/indigo/notifs"
	"github.com/bluesky-social/indigo/util"
	"github.com/bluesky-social/indigo/xrpc"

	"go.opentelemetry.io/otel"
	"gorm.io/gorm"
)

const MaxEventSliceLength = 1000000
const MaxOpsSliceLength = 200

type Indexer struct {
	db *gorm.DB

	notifman notifs.NotificationManager
	events   *events.EventManager
	didr     did.Resolver

	SendRemoteFollow       func(context.Context, string, uint) error
	CreateExternalUser     func(context.Context, string) (*models.ActorInfo, error)
	ApplyPDSClientSettings func(*xrpc.Client)

	log *slog.Logger
}

func NewIndexer(db *gorm.DB, notifman notifs.NotificationManager, evtman *events.EventManager, didr did.Resolver) (*Indexer, error) {
	// TODO: revamp all these schemas
	db.AutoMigrate(&models.ActorInfo{})

	ix := &Indexer{
		db:       db,
		notifman: notifman,
		events:   evtman,
		didr:     didr,
		SendRemoteFollow: func(context.Context, string, uint) error {
			return nil
		},
		ApplyPDSClientSettings: func(*xrpc.Client) {},
		log:                    slog.Default().With("system", "indexer"),
	}

	return ix, nil
}

func (ix *Indexer) Shutdown() {
}

func (ix *Indexer) HandleRepoEvent(ctx context.Context, evt *repomgr.RepoEvent) error {
	ctx, span := otel.Tracer("indexer").Start(ctx, "HandleRepoEvent")
	defer span.End()

	ix.log.Debug("Handling Repo Event!", "uid", evt.User)

	outops := make([]*comatproto.SyncSubscribeRepos_RepoOp, 0, len(evt.Ops))
	for _, op := range evt.Ops {
		link := (*lexutil.LexLink)(op.RecCid)
		outops = append(outops, &comatproto.SyncSubscribeRepos_RepoOp{
			Path:   op.Collection + "/" + op.Rkey,
			Action: string(op.Kind),
			Cid:    link,
		})
	}

	did, err := ix.DidForUser(ctx, evt.User)
	if err != nil {
		return err
	}

	toobig := false
	slice := evt.RepoSlice
	if len(slice) > MaxEventSliceLength || len(outops) > MaxOpsSliceLength {
		slice = []byte{}
		outops = nil
		toobig = true
	}

	ix.log.Debug("Sending event", "did", did)
	if err := ix.events.AddEvent(ctx, &events.XRPCStreamEvent{
		RepoCommit: &comatproto.SyncSubscribeRepos_Commit{
			Repo:   did,
			Prev:   (*lexutil.LexLink)(evt.OldRoot),
			Blocks: slice,
			Rev:    evt.Rev,
			Since:  evt.Since,
			Commit: lexutil.LexLink(evt.NewRoot),
			Time:   time.Now().Format(util.ISO8601),
			Ops:    outops,
			TooBig: toobig,
		},
		PrivUid: evt.User,
	}); err != nil {
		return fmt.Errorf("failed to push event: %s", err)
	}

	return nil
}

func (ix *Indexer) GetUserOrMissing(ctx context.Context, did string) (*models.ActorInfo, error) {
	ctx, span := otel.Tracer("indexer").Start(ctx, "getUserOrMissing")
	defer span.End()

	ai, err := ix.LookupUserByDid(ctx, did)
	if err == nil {
		return ai, nil
	}

	if !isNotFound(err) {
		return nil, err
	}

	// unknown user... create it and send it off to the crawler
	return ix.createMissingUserRecord(ctx, did)
}

func (ix *Indexer) createMissingUserRecord(ctx context.Context, did string) (*models.ActorInfo, error) {
	ctx, span := otel.Tracer("indexer").Start(ctx, "createMissingUserRecord")
	defer span.End()

	externalUserCreationAttempts.Inc()

	ai, err := ix.CreateExternalUser(ctx, did)
	if err != nil {
		return nil, err
	}

	return ai, nil
}

func (ix *Indexer) DidForUser(ctx context.Context, uid models.Uid) (string, error) {
	var ai models.ActorInfo
	if err := ix.db.First(&ai, "uid = ?", uid).Error; err != nil {
		return "", err
	}

	return ai.Did, nil
}

func (ix *Indexer) LookupUser(ctx context.Context, id models.Uid) (*models.ActorInfo, error) {
	var ai models.ActorInfo
	if err := ix.db.First(&ai, "uid = ?", id).Error; err != nil {
		return nil, err
	}

	return &ai, nil
}

func (ix *Indexer) LookupUserByDid(ctx context.Context, did string) (*models.ActorInfo, error) {
	var ai models.ActorInfo
	if err := ix.db.Find(&ai, "did = ?", did).Error; err != nil {
		return nil, err
	}

	if ai.ID == 0 {
		return nil, gorm.ErrRecordNotFound
	}

	return &ai, nil
}

func (ix *Indexer) LookupUserByHandle(ctx context.Context, handle string) (*models.ActorInfo, error) {
	var ai models.ActorInfo
	if err := ix.db.Find(&ai, "handle = ?", handle).Error; err != nil {
		return nil, err
	}

	if ai.ID == 0 {
		return nil, gorm.ErrRecordNotFound
	}

	return &ai, nil
}

func isNotFound(err error) bool {
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return true
	}

	return false
}
