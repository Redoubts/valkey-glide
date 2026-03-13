// Copyright Valkey GLIDE Project Contributors - SPDX Identifier: Apache-2.0

package models

import "strconv"

// FlatArrayToMap converts a flat [k1, v1, k2, v2, ...] array into a map.
// Non-string keys are converted to their positional index.
func FlatArrayToMap(arr []any) map[string]any {
	m := make(map[string]any, len(arr)/2)
	for i := 0; i+1 < len(arr); i += 2 {
		key, ok := arr[i].(string)
		if !ok {
			key = strconv.Itoa(i)
		}
		m[key] = arr[i+1]
	}
	return m
}

// FtSearchResult holds the parsed response from an FT.SEARCH command.
//
// The response shape varies depending on the options used:
//   - Default: Documents maps field names to their string values.
//   - NOCONTENT: Documents maps keys to empty maps.
//   - WITHSORTKEYS: Documents maps keys to []any{sortKey, fieldMap}.
//
// See [valkey.io] for details.
//
// [valkey.io]: https://valkey.io/commands/ft.search/
type FtSearchResult struct {
	// TotalResults is the total number of documents matching the query.
	TotalResults int64
	// Documents maps document keys to their field data.
	// The value type depends on the search options used:
	//   - Default/NOCONTENT: map[string]any with field name → value pairs.
	//   - WITHSORTKEYS: []any{sortKey, fieldMap} where sortKey is a string
	//     and fieldMap is map[string]any.
	Documents map[string]any
}
