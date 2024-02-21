// Code generated by cmd/lexgen (see Makefile's lexgen); DO NOT EDIT.

package atproto

// schema: com.atproto.repo.createRecord

import (
	"context"

	"github.com/bluesky-social/indigo/lex/util"
	"github.com/bluesky-social/indigo/xrpc"
)

// RepoCreateRecord_Input is the input argument to a com.atproto.repo.createRecord call.
type RepoCreateRecord_Input struct {
	// collection: The NSID of the record collection.
	Collection string `json:"collection" cborgen:"collection"`
	// record: The record itself. Must contain a $type field.
	Record *util.LexiconTypeDecoder `json:"record" cborgen:"record"`
	// repo: The handle or DID of the repo (aka, current account).
	Repo string `json:"repo" cborgen:"repo"`
	// rkey: The Record Key.
	Rkey *string `json:"rkey,omitempty" cborgen:"rkey,omitempty"`
	// swapCommit: Compare and swap with the previous commit by CID.
	SwapCommit *string `json:"swapCommit,omitempty" cborgen:"swapCommit,omitempty"`
	// validate: Can be set to 'false' to skip Lexicon schema validation of record data.
	Validate *bool `json:"validate,omitempty" cborgen:"validate,omitempty"`
}

// RepoCreateRecord_Output is the output of a com.atproto.repo.createRecord call.
type RepoCreateRecord_Output struct {
	Cid string `json:"cid" cborgen:"cid"`
	Uri string `json:"uri" cborgen:"uri"`
}

// RepoCreateRecord calls the XRPC method "com.atproto.repo.createRecord".
func RepoCreateRecord(ctx context.Context, c *xrpc.Client, input *RepoCreateRecord_Input) (*RepoCreateRecord_Output, error) {
	var out RepoCreateRecord_Output
	if err := c.Do(ctx, xrpc.Procedure, "application/json", "com.atproto.repo.createRecord", nil, input, &out); err != nil {
		return nil, err
	}

	return &out, nil
}
