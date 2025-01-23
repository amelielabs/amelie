
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
#include <amelie_set.h>
#include <amelie_content.h>
#include <amelie_executor.h>
#include <amelie_func.h>
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

static void
planner_target(Target* target, AstList* ops)
{
	auto table = target->from_table;
	assert(table);

	// FROM USE INDEX (use predefined index)
	if (target->from_table_index)
	{
		auto keys = &target->from_table_index->keys;
		target->plan = plan_create(target, keys, ops);

		auto primary = table_primary(target->from_table);
		if (target->from_table_index != primary)
			target->plan_primary = plan_create(target, &primary->keys, ops);
		else
			target->plan_primary = target->plan;
		return;
	}

	// match the most optimal index to use
	Plan*        match_plan_primary = NULL;
	Plan*        match_plan = NULL;
	IndexConfig* match = NULL;
	list_foreach(&table->config->indexes)
	{
		auto index = list_at(IndexConfig, link);
		auto keys = &index->keys;

		// primary
		auto plan = plan_create(target, keys, ops);
		if (! match)
		{
			match_plan_primary = plan;
			match_plan = plan;
			match = index;
			continue;
		}

		// secondary
		if (planner_match(match_plan, match, plan, index))
		{
			match_plan = plan;
			match = index;
		}
	}

	// set index and plan
	target->from_table_index = match;
	target->plan_primary = match_plan_primary;
	target->plan = match_plan;
}

void
planner(Targets* targets, Ast* expr)
{
	// get a list of conditions
	AstList ops;
	ast_list_init(&ops);
	if (expr)
		ops_extract(&ops, expr);

	// create plans for table scans
	for (auto target = targets->list; target; target = target->next)
		if (target_is_table(target))
			planner_target(target, &ops);
}
