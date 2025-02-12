package indexer

import (
	"context"
	"errors"
	"fmt"
	comatproto "github.com/bluesky-social/indigo/api/atproto"
	"github.com/bluesky-social/indigo/did"
	"github.com/bluesky-social/indigo/events"
	"github.com/bluesky-social/indigo/models"
	"github.com/bluesky-social/indigo/notifs"
	"github.com/bluesky-social/indigo/xrpc"
	"log/slog"

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

func (ix *Indexer) HandleCommit(ctx context.Context, host *models.PDS, uid models.Uid, did string, commit *comatproto.SyncSubscribeRepos_Commit) error {
	xe := &events.XRPCStreamEvent{
		RepoCommit: commit,
		PrivUid:    uid,
	}
	if err := ix.events.AddEvent(ctx, xe); err != nil {
		return fmt.Errorf("failed to push event: %s", err)
	}
	return nil
}

func (ix *Indexer) DidForUser(ctx context.Context, uid models.Uid) (string, error) {
	var ai models.ActorInfo
	if err := ix.db.First(&ai, "uid = ?", uid).Error; err != nil {
		return "", err
	}

	return ai.Did, nil
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

func isNotFound(err error) bool {
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return true
	}

	return false
}
