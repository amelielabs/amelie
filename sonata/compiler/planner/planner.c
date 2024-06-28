
//
// sonata.
//
// Real-Time SQL Database.
//

#include <sonata_runtime.h>
#include <sonata_io.h>
#include <sonata_lib.h>
#include <sonata_data.h>
#include <sonata_config.h>
#include <sonata_user.h>
#include <sonata_http.h>
#include <sonata_client.h>
#include <sonata_server.h>
#include <sonata_row.h>
#include <sonata_transaction.h>
#include <sonata_index.h>
#include <sonata_partition.h>
#include <sonata_wal.h>
#include <sonata_db.h>
#include <sonata_value.h>
#include <sonata_aggr.h>
#include <sonata_executor.h>
#include <sonata_vm.h>
#include <sonata_parser.h>
#include <sonata_planner.h>

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
