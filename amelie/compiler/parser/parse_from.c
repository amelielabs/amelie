
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

typedef struct
{
	int     level;
	int     level_seq;
	Target* head;
	Target* tail;
	Stmt*   stmt;
} From;

static inline Target*
parse_from_target(From* self)
{
	auto stmt = self->stmt;
	auto target = target_allocate();

	// FROM (SELECT)
	if (stmt_if(stmt, '('))
	{
		if (! stmt_if(stmt, KSELECT))
			error("FROM (<SELECT> expected");
		auto select = parse_select(stmt);
		if (! stmt_if(stmt, ')'))
			error("FROM (SELECT ... <)> expected");
		target->type         = TARGET_SELECT;
		target->from_select  = &select->ast;
		target->from_columns = &select->ret.columns;
		return target;
	}

	// FROM name [(args)]
	// FROM schema.name [(args)]
	Str  schema;
	Str  name;
	auto expr = parse_target(stmt, &schema, &name);
	if (! expr)
		error("FROM target name expected");

	// function()
	if (stmt->id == STMT_SELECT && stmt_if(stmt, '('))
	{
		// find function
		auto call = ast_call_allocate();
		call->fn = function_mgr_find(stmt->function_mgr, &schema, &name);
		if (! call->fn)
			error("FROM %.*s.%.*s(): function not found",
			      str_size(&schema), str_of(&schema),
			      str_size(&name),
			      str_of(&name));
		// parse args ()
		call->ast.r = parse_expr_args(stmt, NULL, ')', false);

		// ensure function can be used inside FROM
		if (call->fn->ret != TYPE_STORE &&
		    call->fn->ret != TYPE_JSON)
			error("FROM %.*s.%.*s(): function must return result set or JSON",
			      str_size(&schema), str_of(&schema),
			      str_size(&name),
			      str_of(&name));

		target->type = TARGET_FUNCTION;
		target->from_function = &call->ast;

		// allocate select to keep returning columns
		auto select = ast_select_allocate();
		ast_list_add(&stmt->select_list, &select->ast);
		target->from_columns = &select->ret.columns;
		str_set_str(&target->name, &call->fn->name);
		return target;
	}

	// cte
	auto cte = cte_list_find(stmt->cte_list, &name);
	if (cte)
	{
		if (cte->stmt == self->stmt->order)
			error("<%.*s> recursive CTE are not supported",
			      str_size(&name), str_of(&name));
		target->type         = TARGET_CTE;
		target->from_cte     = cte;
		target->from_columns = cte->columns;
		str_set_str(&target->name, &cte->name->string);
		// add cte dependency
		cte_deps_add(&stmt->cte_deps, cte);
		return target;
	}

	// table
	auto table = table_mgr_find(&stmt->db->table_mgr, &schema, &name, false);
	if (table)
	{
		if (table->config->shared)
			target->type = TARGET_TABLE_SHARED;
		else
			target->type = TARGET_TABLE;
		target->from_table   = table;
		target->from_columns = &table->config->columns;
		str_set_str(&target->name, &table->config->name);
		return target;
	}

	error("<%.*s.%.*s> relation not found",
	      str_size(&schema), str_of(&schema),
	      str_size(&name),
	      str_of(&name));

	// unreach
	return NULL;
}

static inline Target*
parse_from_add(From* self)
{
	auto stmt = self->stmt;

	// FROM [schema.]name | (SELECT) [AS] [alias] [USE INDEX (name)]
	auto target = parse_from_target(self);
	auto target_list = &stmt->target_list;

	// [AS] [alias]
	auto as = stmt_if(stmt, KAS);
	auto alias = stmt_if(stmt, KNAME);
	if (alias) {
		str_set_str(&target->name, &alias->string);
	} else {
		if (as)
			error("AS <name> expected ");
		if (target->type == TARGET_SELECT)
			error("FROM (SELECT) subquery must have an alias");
	}

	// ensure target does not exists
	if (! str_empty(&target->name))
	{
		auto match = target_list_match(target_list, &target->name);
		if (match)
			error("<%.*s> target is redefined, please use different alias for the target",
			      str_size(&target->name), str_of(&target->name));
	}

	// generate first column to match the target name for function target
	if (target->type == TARGET_FUNCTION)
	{
		auto fn = ast_call_of(target->from_function)->fn;
		auto column = column_allocate();
		column_set_name(column, &target->name);
		column_set_type(column, fn->ret, type_sizeof(fn->ret));
		columns_add(target->from_columns, column);
	}

	// add target
	target_list_add(target_list, target, self->level, self->level_seq++);

	if (self->head == NULL)
		self->head = target;
	else
		self->tail->next_join = target;
	self->tail = target;

	// set outer target
	target->outer = self->head;

	// [USE INDEX (name)]
	if (stmt_if(stmt, KUSE))
	{
		if (! stmt_if(stmt, KINDEX))
			error("USE <INDEX> expected");
		if (! stmt_if(stmt, '('))
			error("USE INDEX <(> expected");
		auto name = stmt_next_shadow(stmt);
		if (name->id != KNAME)
			error("USE INDEX (<index name> expected");
		if (! stmt_if(stmt, ')'))
			error("USE INDEX (<)> expected");
		if (target->type != TARGET_TABLE &&
		    target->type != TARGET_TABLE_SHARED)
			error("USE INDEX expected table target");
		target->from_table_index =
			table_find_index(target->from_table, &name->string, true);
	}

	return target;
}

Target*
parse_from(Stmt* self, int level)
{
	// FROM <[schema.]name | (SELECT)> [AS] [alias]> [, ...]
	// FROM <[schema.]name | (SELECT)> [AS] [alias]> [JOIN <..> ON (expr) ...]
	From from =
	{
		.level     = level,
		.level_seq = 0,
		.head      = NULL,
		.tail      = NULL,
		.stmt      = self
	};

	// FROM name | expr
	parse_from_add(&from);

	for (;;)
	{
		// ,
		if (stmt_if(self, ','))
		{
			// name | expr
			parse_from_add(&from);
			continue;
		}

		TargetJoin join = JOIN_NONE;

		// [JOIN]
		// [INNER JOIN]
		// [LEFT [OUTER] JOIN]
		// [RIGHT [OUTER] JOIN]
		if (stmt_if(self, KJOIN))
			join = JOIN_INNER;
		else
		if (stmt_if(self, KINNER))
		{
			if (! stmt_if(self, KJOIN))
				error("INNER <JOIN> expected");
			join = JOIN_INNER;
		} else
		if (stmt_if(self, KLEFT))
		{
			// [OUTER]
			stmt_if(self, KOUTER);
			if (! stmt_if(self, KJOIN))
				error("LEFT <JOIN> expected");
			join = JOIN_LEFT;
		} else
		if (stmt_if(self, KRIGHT))
		{
			// [OUTER]
			stmt_if(self, KOUTER);
			if (! stmt_if(self, KJOIN))
				error("RIGHT <JOIN> expected");
			join = JOIN_RIGHT;
		}

		// <name|expr> ON expr
		if (join != JOIN_NONE)
		{
			if (join == JOIN_LEFT || join == JOIN_RIGHT)
				error("outer joins currently are not supported");

			// <name|expr>
			auto target = parse_from_add(&from);

			// ON
			if (! stmt_if(self, KON))
				error("JOIN name <ON> expected");

			// expr
			target->join_on = parse_expr(self, NULL);
			target->join    = join;
			continue;
		}

		break;
	}

	return from.head;
}
