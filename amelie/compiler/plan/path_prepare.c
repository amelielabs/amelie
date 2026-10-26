
//
// amelie.
//
// Real-Time SQL OLTP Database.
//
// Copyright (c) 2024 Dmitry Simonenko.
// Copyright (c) 2024 Amelie Labs.
//
// AGPL-3.0 Licensed.
//

#include <amelie_runtime>
#include <amelie_server>
#include <amelie_db>
#include <amelie_repl>
#include <amelie_vm>
#include <amelie_parser.h>
#include <amelie_plan.h>

static bool
path_prepare_match(Path*        prev_path,
                   IndexConfig* prev,
                   Path*        next_path,
                   IndexConfig* next)
{
	// tree
	if (prev->type == INDEX_TREE && next->type == INDEX_TREE)
	{
		// choose index with max number of key matches from start
		if (next_path->match_start > prev_path->match_start)
			return true;
		if (next_path->match_stop > prev_path->match_stop)
			return true;
		return false;
	}

	// hash
	if (prev->type == INDEX_HASH && next->type == INDEX_HASH)
	{
		// choose next only if it is point lookup, unless prev one is
		if (prev_path->type == PATH_LOOKUP)
			return false;
		return next_path->type == PATH_LOOKUP;
	}
	// tree/hash

	// choose hash over tree only in case if it is a point lookup
	if (prev->type == INDEX_HASH)
		return prev_path->type == PATH_LOOKUP;

	return next_path->type == PATH_LOOKUP;
}

static void
path_prepare_target(Target* target, Block* block, PathOps* ops)
{
	unused(block);
	auto table = target->from_table;

	// with or without primary key
	auto primary = table_primary(target->from_table);
	Keys* primary_keys = NULL;
	if (primary)
		primary_keys = &primary->keys;

	// dml
	if (target->dml)
	{
		assert(!target->from_index);
		target->path = path_create(target, primary_keys, ops);
		target->from_index = primary;
		return;
	}

	// FROM USE INDEX (use predefined index)
	if (target->from_index)
	{
		auto keys = &target->from_index->keys;
		target->path = path_create(target, keys, ops);
		return;
	}

	// match the most optimal index to use
	Path*        match_path = NULL;
	IndexConfig* match = NULL;
	list_foreach(&table->config->indexes)
	{
		auto index = list_at(IndexConfig, link);

		// use index only if it has matching keys
		auto path = path_create(target, &index->keys, ops);
		if (!path->match_start && !path->match_stop)
		{
			// use index without matching keys only if it
			// matches select order by
			if (! target->from->select)
				continue;
			auto select = ast_select_of(target->from->select);
			if (ast_list_empty(&select->order_by))
				continue;
			if (! ast_order_list_match_index(&select->order_by, target, index))
				continue;
		}

		if (! match)
		{
			match_path = path;
			match = index;
			continue;
		}

		if (path_prepare_match(match_path, match, path, index))
		{
			match_path = path;
			match = index;
		}
	}

	// downgrade full HASH index scan to heap scan
	if (match && match->type == INDEX_HASH && match_path->type == PATH_SCAN)
	{
		match = NULL;
		match_path = NULL;
	}

	// no primary index (full heap scan)
	if (! match)
		match_path = path_create(target, NULL, ops);

	// set index and path
	target->from_index = match;
	target->path       = match_path;
}

void
path_prepare(From* from, Ast* expr)
{
	// get a list of conditions
	PathOps ops;
	path_ops_init(&ops, &from->list_names);
	if (expr)
		path_ops_create(&ops, expr);

	// create paths for table scans
	for (auto target = from->list; target; target = target->next)
		if (target_is_table(target) && !target->path)
			path_prepare_target(target, from->block, &ops);
}
