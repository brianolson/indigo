// Code generated by cmd/lexgen (see Makefile's lexgen); DO NOT EDIT.

package bsky

// schema: app.bsky.unspecced.getSuggestionsSkeleton

import (
	"context"

	"github.com/bluesky-social/indigo/xrpc"
)

// UnspeccedGetSuggestionsSkeleton_Output is the output of a app.bsky.unspecced.getSuggestionsSkeleton call.
type UnspeccedGetSuggestionsSkeleton_Output struct {
	Actors []*UnspeccedDefs_SkeletonSearchActor `json:"actors" cborgen:"actors"`
	Cursor *string                              `json:"cursor,omitempty" cborgen:"cursor,omitempty"`
	// relativeToDid: DID of the account these suggestions are relative to. If this is returned undefined, suggestions are based on the viewer.
	RelativeToDid *string `json:"relativeToDid,omitempty" cborgen:"relativeToDid,omitempty"`
}

// UnspeccedGetSuggestionsSkeleton calls the XRPC method "app.bsky.unspecced.getSuggestionsSkeleton".
//
// relativeToDid: DID of the account to get suggestions relative to. If not provided, suggestions will be based on the viewer.
// viewer: DID of the account making the request (not included for public/unauthenticated queries). Used to boost followed accounts in ranking.
func UnspeccedGetSuggestionsSkeleton(ctx context.Context, c *xrpc.Client, cursor string, limit int64, relativeToDid string, viewer string) (*UnspeccedGetSuggestionsSkeleton_Output, error) {
	var out UnspeccedGetSuggestionsSkeleton_Output

	params := map[string]interface{}{
		"cursor":        cursor,
		"limit":         limit,
		"relativeToDid": relativeToDid,
		"viewer":        viewer,
	}
	if err := c.Do(ctx, xrpc.Query, "", "app.bsky.unspecced.getSuggestionsSkeleton", params, nil, &out); err != nil {
		return nil, err
	}

	return &out, nil
}
