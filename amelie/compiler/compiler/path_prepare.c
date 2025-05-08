
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
#include <amelie_heap.h>
#include <amelie_transaction.h>
#include <amelie_index.h>
#include <amelie_partition.h>
#include <amelie_checkpoint.h>
#include <amelie_wal.h>
#include <amelie_db.h>
#include <amelie_backup.h>
#include <amelie_repl.h>
#include <amelie_value.h>
#include <amelie_set.h>
#include <amelie_content.h>
#include <amelie_executor.h>
#include <amelie_func.h>
#include <amelie_vm.h>
#include <amelie_parser.h>
#include <amelie_compiler.h>

hot static inline void
ops_extract(AstList* self, Ast* expr)
{
	switch (expr->id) {
	case KAND:
	{
		ops_extract(self, expr->l);
		ops_extract(self, expr->r);
		break;
	}
	case KOR:
	{
		// skipped
		break;
	}
	case KGTE:
	case KLTE:
	case '>':
	case '<':
	case '=':
	{
		// [target.]name = value | [target.]name
		Ast* value;
		if (expr->l->id == KNAME || expr->l->id == KNAME_COMPOUND)
			value = expr->r;
		else
		if (expr->r->id == KNAME || expr->r->id == KNAME_COMPOUND)
			value = expr->l;
		else
			break;
		if (value->id == KINT       ||
		    value->id == KSTRING    ||
		    value->id == KTIMESTAMP ||
		    value->id == KUUID      ||
		    value->id == KNAME      ||
		    value->id == KNAME_COMPOUND)
		{
			ast_list_add(self, expr);
		}
		break;
	}
	case KBETWEEN:
	{
		// NOT BETWEEN
		if (! expr->integer)
			break;

		auto x = expr->r->l;
		auto y = expr->r->r;

		// expr >= x AND expr <= y
		auto gte = ast(KGTE);
		gte->l = expr->l;
		gte->r = x;

		auto lte = ast(KLTE);
		lte->l = expr->l;
		lte->r = y;

		auto and = ast(KAND);
		and->l = gte;
		and->r = lte;
		ops_extract(self, and);
	}
	default:
		break;
	}
}

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
path_prepare_target(Target* target, Scope* scope, AstList* ops)
{
	auto table = target->from_table;
	assert(table);

	// FROM USE INDEX (use predefined index)
	if (target->from_index)
	{
		auto keys = &target->from_index->keys;
		target->path = path_create(target, scope, keys, ops);

		auto primary = table_primary(target->from_table);
		if (target->from_index != primary)
			target->path_primary = path_create(target, scope, &primary->keys, ops);
		else
			target->path_primary = target->path;
		return;
	}

	// match the most optimal index to use
	Path*        match_path_primary = NULL;
	Path*        match_path = NULL;
	IndexConfig* match = NULL;
	list_foreach(&table->config->indexes)
	{
		auto index = list_at(IndexConfig, link);
		auto keys = &index->keys;

		// primary
		auto path = path_create(target, scope, keys, ops);
		if (! match)
		{
			match_path_primary = path;
			match_path = path;
			match = index;
			continue;
		}

		// secondary
		if (path_prepare_match(match_path, match, path, index))
		{
			match_path = path;
			match = index;
		}
	}

	// set index and path
	target->from_index = match;
	target->path_primary = match_path_primary;
	target->path = match_path;
}

void
path_prepare(Targets* targets, Ast* expr)
{
	// get a list of conditions
	AstList ops;
	ast_list_init(&ops);
	if (expr)
		ops_extract(&ops, expr);

	// create paths for table scans
	for (auto target = targets->list; target; target = target->next)
		if (target_is_table(target))
			path_prepare_target(target, targets->scope, &ops);
}
