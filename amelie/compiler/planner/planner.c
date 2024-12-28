
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
#include <amelie_json.h>
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
#include <amelie_store.h>
#include <amelie_content.h>
#include <amelie_executor.h>
#include <amelie_vm.h>
#include <amelie_parser.h>
#include <amelie_planner.h>

static bool
planner_match(Plan*        prev_plan,
              IndexConfig* prev,
              Plan*        next_plan,
              IndexConfig* next)
{
	// tree
	if (prev->type == INDEX_TREE && next->type == INDEX_TREE)
	{
		// choose index with max number of key matches from start
		if (next_plan->match_start > prev_plan->match_start)
			return true;
		if (next_plan->match_stop > prev_plan->match_stop)
			return true;
		return false;
	}

	// hash
	if (prev->type == INDEX_HASH && next->type == INDEX_HASH)
	{
		// choose next only if it is point lookup, unless prev one is
		if (prev_plan->type == PLAN_LOOKUP)
			return false;
		return next_plan->type == PLAN_LOOKUP;
	}
	// tree/hash

	// choose hash over tree only in case if it is a point lookup
	if (prev->type == INDEX_HASH)
		return prev_plan->type == PLAN_LOOKUP;

	return next_plan->type == PLAN_LOOKUP;
}

void
planner(Target* target, Ast* expr)
{
	auto table = target->from_table;
	assert(table);
	if (target->plan)
		return;

	// get a list of conditions
	AstList ops;
	ast_list_init(&ops);
	if (expr)
		ops_extract(&ops, expr);

	// FROM USE INDEX (use predefined index)
	if (target->from_table_index)
	{
		auto keys = &target->from_table_index->keys;
		target->plan = plan_create(target, keys, &ops);
		return;
	}

	// match the most optimal index to use
	Plan*        match_plan = NULL;
	IndexConfig* match = NULL;
	list_foreach(&table->config->indexes)
	{
		auto index = list_at(IndexConfig, link);
		auto keys = &index->keys;

		auto plan = plan_create(target, keys, &ops);
		if (!match || planner_match(match_plan, match, plan, index))
		{
			match = index;
			match_plan = plan;
		}
	}

	// set index and plan
	target->from_table_index = match;
	target->plan = match_plan;
}
