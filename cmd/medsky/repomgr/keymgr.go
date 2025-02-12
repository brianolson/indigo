package repomgr

import (
	"context"
	"log/slog"

	did "github.com/whyrusleeping/go-did"
	"go.opentelemetry.io/otel"
)

type KeyManager struct {
	didr DidResolver

	log *slog.Logger
}

type DidResolver interface {
	GetDocument(ctx context.Context, didstr string) (*did.Document, error)
}

// NewKeyManager assumes a caching underlying Did document resolver
func NewKeyManager(didr DidResolver) *KeyManager {
	return &KeyManager{
		didr: didr,
		log:  slog.Default().With("system", "indexer"),
	}
}

func (km *KeyManager) VerifyUserSignature(ctx context.Context, did string, sig []byte, msg []byte) error {
	ctx, span := otel.Tracer("keymgr").Start(ctx, "verifySignature")
	defer span.End()

	k, err := km.getKey(ctx, did)
	if err != nil {
		return err
	}

	err = k.Verify(msg, sig)
	if err != nil {
		// TODO: counter?
		km.log.Info("signature failed to verify", "err", err, "did", did, "pubKey", k, "sigBytes", sig, "msgBytes", msg)
	}
	return err
}

func (km *KeyManager) getKey(ctx context.Context, did string) (*did.PubKey, error) {
	ctx, span := otel.Tracer("keymgr").Start(ctx, "getKey")
	defer span.End()

	doc, err := km.didr.GetDocument(ctx, did)
	if err != nil {
		return nil, err
	}

	pubk, err := doc.GetPublicKey("#atproto")
	if err != nil {
		return nil, err
	}

	return pubk, nil
}
