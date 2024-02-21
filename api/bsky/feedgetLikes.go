// Code generated by cmd/lexgen (see Makefile's lexgen); DO NOT EDIT.

package bsky

// schema: app.bsky.feed.getLikes

import (
	"context"

	"github.com/bluesky-social/indigo/xrpc"
)

// FeedGetLikes_Like is a "like" in the app.bsky.feed.getLikes schema.
type FeedGetLikes_Like struct {
	Actor     *ActorDefs_ProfileView `json:"actor" cborgen:"actor"`
	CreatedAt string                 `json:"createdAt" cborgen:"createdAt"`
	IndexedAt string                 `json:"indexedAt" cborgen:"indexedAt"`
}

// FeedGetLikes_Output is the output of a app.bsky.feed.getLikes call.
type FeedGetLikes_Output struct {
	Cid    *string              `json:"cid,omitempty" cborgen:"cid,omitempty"`
	Cursor *string              `json:"cursor,omitempty" cborgen:"cursor,omitempty"`
	Likes  []*FeedGetLikes_Like `json:"likes" cborgen:"likes"`
	Uri    string               `json:"uri" cborgen:"uri"`
}

// FeedGetLikes calls the XRPC method "app.bsky.feed.getLikes".
//
// cid: CID of the subject record (aka, specific version of record), to filter likes.
// uri: AT-URI of the subject (eg, a post record).
func FeedGetLikes(ctx context.Context, c *xrpc.Client, cid string, cursor string, limit int64, uri string) (*FeedGetLikes_Output, error) {
	var out FeedGetLikes_Output

	params := map[string]interface{}{
		"cid":    cid,
		"cursor": cursor,
		"limit":  limit,
		"uri":    uri,
	}
	if err := c.Do(ctx, xrpc.Query, "", "app.bsky.feed.getLikes", params, nil, &out); err != nil {
		return nil, err
	}

	return &out, nil
}
