
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

#include <amelie_runtime.h>
#include <amelie_io.h>
#include <amelie_lib.h>
#include <amelie_data.h>
#include <amelie_config.h>
#include <amelie_user.h>
#include <amelie_auth.h>
#include <amelie_http.h>
#include <amelie_client.h>
#include <amelie_server.h>
#include <amelie_row.h>
#include <amelie_transaction.h>
#include <amelie_index.h>
#include <amelie_partition.h>
#include <amelie_checkpoint.h>
#include <amelie_wal.h>
#include <amelie_db.h>
#include <amelie_value.h>
#include <amelie_aggr.h>
#include <amelie_executor.h>
#include <amelie_vm.h>
#include <amelie_parser.h>
#include <amelie_planner.h>

static bool
planner_match(AstPath*     prev_path,
              IndexConfig* prev,
              AstPath*     next_path,
              IndexConfig* next)
{
	// tree
	if (prev->type == INDEX_TREE && next->type == INDEX_TREE)
	{
		// choose index with max number of key matches from start
		if (next_path->match > prev_path->match)
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

void
planner(TargetList* target_list, Target* target, Ast* expr)
{
	auto table = target->table;
	assert(table);
	if (target->path)
		return;

	Path path =
	{
		.keys        = NULL,
		.target      = target,
		.target_list = target_list
	};

	// FROM USE INDEX (use predefined index)
	if (target->index)
	{
		path.keys = &target->index->keys;
		target->path = &path_create(&path, expr)->ast;
		return;
	}

	// match the most optimal index to use
	AstPath*     match_path = NULL;
	IndexConfig* match = NULL;
	list_foreach(&table->config->indexes)
	{
		auto index = list_at(IndexConfig, link);
		path.keys = &index->keys;
		auto index_path = path_create(&path, expr);
		if (!match || planner_match(match_path, match, index_path, index))
		{
			match_path = index_path;
			match = index;
		}
	}

	// set index and path
	target->path  = &match_path->ast;
	target->index = match;
}
