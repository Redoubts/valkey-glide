// Copyright Valkey GLIDE Project Contributors - SPDX Identifier: Apache-2.0

package glide

// #include "lib.h"
import "C"

import (
	"context"
	"fmt"

	"github.com/valkey-io/valkey-glide/go/v2/models"
	"github.com/valkey-io/valkey-glide/go/v2/options"
)

// GlideFt provides access to the FT (RediSearch / Valkey Search) command group.
//
// Obtain an instance via [Client.FT] or [ClusterClient.FT].
//
// See [valkey.io Search documentation] for details.
//
// [valkey.io Search documentation]: https://valkey.io/docs/topics/search/
type GlideFt struct {
	client *baseClient
}

// FT returns a [GlideFt] accessor bound to this standalone client.
func (c *Client) FT() *GlideFt { return &GlideFt{client: &c.baseClient} }

// FT returns a [GlideFt] accessor bound to this cluster client.
func (c *ClusterClient) FT() *GlideFt { return &GlideFt{client: &c.baseClient} }

// Create creates a new search index.
//
// See [valkey.io] for details.
//
// Parameters:
//
//	ctx       - The context for controlling the command execution.
//	indexName - The name of the index to create.
//	schema    - A slice of field definitions that describe the index schema.
//	opts      - Optional index creation parameters. Pass nil to use defaults.
//
// Return value:
//
//	"OK" on success.
//
// [valkey.io]: https://valkey.io/commands/ft.create/
func (ft *GlideFt) Create(
	ctx context.Context,
	indexName string,
	schema []options.Field,
	opts *options.FtCreateOptions,
) (string, error) {
	args := []string{indexName}

	if opts != nil {
		optArgs, err := opts.ToArgs()
		if err != nil {
			return models.DefaultStringResponse, err
		}
		args = append(args, optArgs...)
	}

	args = append(args, "SCHEMA")
	for _, field := range schema {
		fieldArgs, err := field.ToArgs()
		if err != nil {
			return models.DefaultStringResponse, err
		}
		args = append(args, fieldArgs...)
	}

	res, err := ft.client.executeCommand(ctx, C.FtCreate, args)
	if err != nil {
		return models.DefaultStringResponse, err
	}
	return handleOkResponse(res)
}

// DropIndex drops an existing search index.
//
// See [valkey.io] for details.
//
// Parameters:
//
//	ctx       - The context for controlling the command execution.
//	indexName - The name of the index to drop.
//
// Return value:
//
//	"OK" on success.
//
// [valkey.io]: https://valkey.io/commands/ft.dropindex/
func (ft *GlideFt) DropIndex(ctx context.Context, indexName string) (string, error) {
	res, err := ft.client.executeCommand(ctx, C.FtDropIndex, []string{indexName})
	if err != nil {
		return models.DefaultStringResponse, err
	}
	return handleOkResponse(res)
}

// List returns a list of all existing index names.
//
// See [valkey.io] for details.
//
// Parameters:
//
//	ctx - The context for controlling the command execution.
//
// Return value:
//
//	A slice of index name strings.
//
// [valkey.io]: https://valkey.io/commands/ft._list/
func (ft *GlideFt) List(ctx context.Context) ([]string, error) {
	res, err := ft.client.executeCommand(ctx, C.FtList, []string{})
	if err != nil {
		return nil, err
	}
	return handleStringArrayResponse(res)
}

// Search executes a search query against an index.
//
// See [valkey.io] for details.
//
// Parameters:
//
//	ctx       - The context for controlling the command execution.
//	indexName - The name of the index to search.
//	query     - The search query string.
//	opts      - Optional search parameters. Pass nil to use defaults.
//
// Return value:
//
//	An [models.FtSearchResult] containing the total count and a map of document
//	keys to their field data. The document value shape depends on the options:
//	  - Default/NOCONTENT: map[string]any with field name → value pairs.
//	  - WITHSORTKEYS: []any{sortKey, fieldMap}.
//
// [valkey.io]: https://valkey.io/commands/ft.search/
func (ft *GlideFt) Search(
	ctx context.Context,
	indexName string,
	query string,
	opts *options.FtSearchOptions,
) (models.FtSearchResult, error) {
	args := []string{indexName, query}

	if opts != nil {
		optArgs, err := opts.ToArgs()
		if err != nil {
			return models.FtSearchResult{}, err
		}
		args = append(args, optArgs...)
	}

	res, err := ft.client.executeCommand(ctx, C.FtSearch, args)
	if err != nil {
		return models.FtSearchResult{}, err
	}
	return handleFtSearchResponse(res)
}

// Aggregate runs an aggregation pipeline against an index.
//
// See [valkey.io] for details.
//
// Parameters:
//
//	ctx       - The context for controlling the command execution.
//	indexName - The name of the index to aggregate.
//	query     - The filter query string.
//	opts      - Optional aggregation parameters. Pass nil to use defaults.
//
// Return value:
//
//	A slice of maps, each representing one result row with field name keys.
//
// [valkey.io]: https://valkey.io/commands/ft.aggregate/
func (ft *GlideFt) Aggregate(
	ctx context.Context,
	indexName string,
	query string,
	opts *options.FtAggregateOptions,
) ([]map[string]any, error) {
	args := []string{indexName, query}

	if opts != nil {
		optArgs, err := opts.ToArgs()
		if err != nil {
			return nil, err
		}
		args = append(args, optArgs...)
	}

	res, err := ft.client.executeCommand(ctx, C.FtAggregate, args)
	if err != nil {
		return nil, err
	}
	return handleFtAggregateResponse(res)
}

// Info returns information and statistics about an index.
//
// See [valkey.io] for details.
//
// Parameters:
//
//	ctx       - The context for controlling the command execution.
//	indexName - The name of the index to inspect.
//
// Return value:
//
//	A map of field names to their values describing the index.
//
// [valkey.io]: https://valkey.io/commands/ft.info/
func (ft *GlideFt) Info(ctx context.Context, indexName string) (map[string]any, error) {
	res, err := ft.client.executeCommand(ctx, C.FtInfo, []string{indexName})
	if err != nil {
		return nil, err
	}
	return handleFtInfoResponse(res)
}

// InfoWithOptions returns information and statistics about an index with additional options.
//
// See [valkey.io] for details.
//
// Parameters:
//
//	ctx       - The context for controlling the command execution.
//	indexName - The name of the index to inspect.
//	opts      - Options controlling scope, shard participation, and consistency.
//
// Return value:
//
//	A map of field names to their values describing the index.
//
// [valkey.io]: https://valkey.io/commands/ft.info/
func (ft *GlideFt) InfoWithOptions(
	ctx context.Context,
	indexName string,
	opts *options.FtInfoOptions,
) (map[string]any, error) {
	args := []string{indexName}
	if opts != nil {
		args = append(args, opts.ToArgs()...)
	}
	res, err := ft.client.executeCommand(ctx, C.FtInfo, args)
	if err != nil {
		return nil, err
	}
	return handleFtInfoResponse(res)
}

// Explain returns the execution plan for a query as a string.
//
// See [valkey.io] for details.
//
// Parameters:
//
//	ctx       - The context for controlling the command execution.
//	indexName - The name of the index.
//	query     - The query to explain.
//
// Return value:
//
//	A string describing the query execution plan.
//
// [valkey.io]: https://valkey.io/commands/ft.explain/
func (ft *GlideFt) Explain(ctx context.Context, indexName, query string) (string, error) {
	res, err := ft.client.executeCommand(ctx, C.FtExplain, []string{indexName, query})
	if err != nil {
		return models.DefaultStringResponse, err
	}
	return handleStringResponse(res)
}

// ExplainCLI returns the execution plan for a query as a slice of strings (CLI format).
//
// See [valkey.io] for details.
//
// Parameters:
//
//	ctx       - The context for controlling the command execution.
//	indexName - The name of the index.
//	query     - The query to explain.
//
// Return value:
//
//	A slice of strings representing the query execution plan.
//
// [valkey.io]: https://valkey.io/commands/ft.explaincli/
func (ft *GlideFt) ExplainCLI(ctx context.Context, indexName, query string) ([]string, error) {
	res, err := ft.client.executeCommand(ctx, C.FtExplainCli, []string{indexName, query})
	if err != nil {
		return nil, err
	}
	return handleStringArrayResponse(res)
}

// AliasAdd adds an alias to an existing index.
//
// See [valkey.io] for details.
//
// Parameters:
//
//	ctx       - The context for controlling the command execution.
//	alias     - The alias name to add.
//	indexName - The index to associate the alias with.
//
// Return value:
//
//	"OK" on success.
//
// [valkey.io]: https://valkey.io/commands/ft.aliasadd/
func (ft *GlideFt) AliasAdd(ctx context.Context, alias, indexName string) (string, error) {
	res, err := ft.client.executeCommand(ctx, C.FtAliasAdd, []string{alias, indexName})
	if err != nil {
		return models.DefaultStringResponse, err
	}
	return handleOkResponse(res)
}

// AliasDel removes an alias from an index.
//
// See [valkey.io] for details.
//
// Parameters:
//
//	ctx   - The context for controlling the command execution.
//	alias - The alias name to remove.
//
// Return value:
//
//	"OK" on success.
//
// [valkey.io]: https://valkey.io/commands/ft.aliasdel/
func (ft *GlideFt) AliasDel(ctx context.Context, alias string) (string, error) {
	res, err := ft.client.executeCommand(ctx, C.FtAliasDel, []string{alias})
	if err != nil {
		return models.DefaultStringResponse, err
	}
	return handleOkResponse(res)
}

// AliasUpdate updates an existing alias to point to a different index.
//
// See [valkey.io] for details.
//
// Parameters:
//
//	ctx       - The context for controlling the command execution.
//	alias     - The alias name to update.
//	indexName - The new index to associate the alias with.
//
// Return value:
//
//	"OK" on success.
//
// [valkey.io]: https://valkey.io/commands/ft.aliasupdate/
func (ft *GlideFt) AliasUpdate(ctx context.Context, alias, indexName string) (string, error) {
	res, err := ft.client.executeCommand(ctx, C.FtAliasUpdate, []string{alias, indexName})
	if err != nil {
		return models.DefaultStringResponse, err
	}
	return handleOkResponse(res)
}

// AliasList returns a map of all aliases to their associated index names.
//
// See [valkey.io] for details.
//
// Parameters:
//
//	ctx - The context for controlling the command execution.
//
// Return value:
//
//	A map where keys are alias names and values are index names.
//
// [valkey.io]: https://valkey.io/commands/ft._aliaslist/
func (ft *GlideFt) AliasList(ctx context.Context) (map[string]string, error) {
	res, err := ft.client.executeCommand(ctx, C.FtAliasList, []string{})
	if err != nil {
		return nil, err
	}
	return handleStringToStringMapResponse(res)
}

// --- response handlers ---

// handleFtSearchResponse parses the raw FT.SEARCH response into an FtSearchResult.
// Format: [count, {key1: fields1, key2: fields2, ...}]
func handleFtSearchResponse(response *C.struct_CommandResponse) (models.FtSearchResult, error) {
	defer C.free_command_response(response)
	data, err := parseInterface(response)
	if err != nil {
		return models.FtSearchResult{}, err
	}
	if data == nil {
		return models.FtSearchResult{}, nil
	}
	arr, ok := data.([]any)
	if !ok || len(arr) < 2 {
		return models.FtSearchResult{}, fmt.Errorf("unexpected FT.SEARCH response format: expected [count, docs] array")
	}
	count, ok := arr[0].(int64)
	if !ok {
		return models.FtSearchResult{}, fmt.Errorf("unexpected FT.SEARCH response format: expected int64 count")
	}
	docs, ok := arr[1].(map[string]any)
	if !ok {
		return models.FtSearchResult{}, fmt.Errorf("unexpected FT.SEARCH response format: expected map for documents")
	}
	return models.FtSearchResult{TotalResults: count, Documents: docs}, nil
}

// handleFtAggregateResponse parses the raw FT.AGGREGATE response.
// The Rust core normalizes the response: it strips the leading count element and
// converts each row from a flat [key, val, ...] array into a map. By the time
// parseInterface returns, we have []any where each element is map[string]any.
func handleFtAggregateResponse(response *C.struct_CommandResponse) ([]map[string]any, error) {
	defer C.free_command_response(response)
	data, err := parseInterface(response)
	if err != nil {
		return nil, err
	}
	if data == nil {
		return nil, nil
	}

	arr, ok := data.([]any)
	if !ok || len(arr) == 0 {
		return nil, nil
	}

	results := make([]map[string]any, 0, len(arr))
	for _, row := range arr {
		if m, ok := row.(map[string]any); ok {
			results = append(results, m)
		}
	}
	return results, nil
}

// handleFtInfoResponse parses the FT.INFO response.
// The response may arrive as a Map (parsed by parseMap) or as a flat key/value array.
func handleFtInfoResponse(response *C.struct_CommandResponse) (map[string]any, error) {
	defer C.free_command_response(response)
	data, err := parseInterface(response)
	if err != nil {
		return nil, err
	}
	if data == nil {
		return nil, nil
	}
	// If the server returns a Map type, parseInterface already gives us map[string]any.
	if m, ok := data.(map[string]any); ok {
		return m, nil
	}
	// Otherwise fall back to flat array parsing.
	arr, ok := data.([]any)
	if !ok {
		return nil, nil
	}
	return models.FlatArrayToMap(arr), nil
}
