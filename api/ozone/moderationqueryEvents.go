// Code generated by cmd/lexgen (see Makefile's lexgen); DO NOT EDIT.

package ozone

// schema: tools.ozone.moderation.queryEvents

import (
	"context"

	"github.com/bluesky-social/indigo/xrpc"
)

// ModerationQueryEvents_Output is the output of a tools.ozone.moderation.queryEvents call.
type ModerationQueryEvents_Output struct {
	Cursor *string                        `json:"cursor,omitempty" cborgen:"cursor,omitempty"`
	Events []*ModerationDefs_ModEventView `json:"events" cborgen:"events"`
}

// ModerationQueryEvents calls the XRPC method "tools.ozone.moderation.queryEvents".
//
// addedLabels: If specified, only events where all of these labels were added are returned
// addedTags: If specified, only events where all of these tags were added are returned
// collections: If specified, only events where the subject belongs to the given collections will be returned. When subjectType is set to 'account', this will be ignored.
// comment: If specified, only events with comments containing the keyword are returned. Apply || separator to use multiple keywords and match using OR condition.
// createdAfter: Retrieve events created after a given timestamp
// createdBefore: Retrieve events created before a given timestamp
// hasComment: If true, only events with comments are returned
// includeAllUserRecords: If true, events on all record types (posts, lists, profile etc.) or records from given 'collections' param, owned by the did are returned.
// removedLabels: If specified, only events where all of these labels were removed are returned
// removedTags: If specified, only events where all of these tags were removed are returned
// sortDirection: Sort direction for the events. Defaults to descending order of created at timestamp.
// subjectType: If specified, only events where the subject is of the given type (account or record) will be returned. When this is set to 'account' the 'collections' parameter will be ignored. When includeAllUserRecords or subject is set, this will be ignored.
// types: The types of events (fully qualified string in the format of tools.ozone.moderation.defs#modEvent<name>) to filter by. If not specified, all events are returned.
func ModerationQueryEvents(ctx context.Context, c *xrpc.Client, addedLabels []string, addedTags []string, collections []string, comment string, createdAfter string, createdBefore string, createdBy string, cursor string, hasComment bool, includeAllUserRecords bool, limit int64, policies []string, removedLabels []string, removedTags []string, reportTypes []string, sortDirection string, subject string, subjectType string, types []string) (*ModerationQueryEvents_Output, error) {
	var out ModerationQueryEvents_Output

	params := map[string]interface{}{
		"addedLabels":           addedLabels,
		"addedTags":             addedTags,
		"collections":           collections,
		"comment":               comment,
		"createdAfter":          createdAfter,
		"createdBefore":         createdBefore,
		"createdBy":             createdBy,
		"cursor":                cursor,
		"hasComment":            hasComment,
		"includeAllUserRecords": includeAllUserRecords,
		"limit":                 limit,
		"policies":              policies,
		"removedLabels":         removedLabels,
		"removedTags":           removedTags,
		"reportTypes":           reportTypes,
		"sortDirection":         sortDirection,
		"subject":               subject,
		"subjectType":           subjectType,
		"types":                 types,
	}
	if err := c.Do(ctx, xrpc.Query, "", "tools.ozone.moderation.queryEvents", params, nil, &out); err != nil {
		return nil, err
	}

	return &out, nil
}
