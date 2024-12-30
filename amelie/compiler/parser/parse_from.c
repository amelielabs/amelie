
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
#include <amelie_vm.h>
#include <amelie_parser.h>

static inline Target*
parse_from_target(Stmt* self, Targets* targets, bool subquery)
{
	auto target = target_allocate(&self->order_targets);

	// FROM (SELECT)
	if (stmt_if(self, '('))
	{
		if (! stmt_if(self, KSELECT))
			error("FROM (<SELECT> expected");
		if (subquery)
		{
			auto select = parse_select(self, targets->outer, true);
			if (! stmt_if(self, ')'))
				error("FROM (SELECT ... <)> expected");
			target->type         = TARGET_SELECT;
			target->from_select  = &select->ast;
			target->from_columns = &select->ret.columns;
		} else
		{
			// rewrite FROM (SELECT) as CTE statement (this can recurse)
			auto cte = stmt_allocate(self->db, self->function_mgr, self->local,
			                         self->lex,
			                         self->data,
			                         self->values_cache,
			                         self->json,
			                         self->stmt_list,
			                         self->args);
			cte->id = STMT_SELECT;
			stmt_list_insert(self->stmt_list, self, cte);

			auto select = parse_select(cte, NULL, false);
			if (! stmt_if(self, ')'))
				error("FROM (SELECT ... <)> expected");
			cte->ast         = &select->ast;
			cte->cte_columns = &select->ret.columns;
			parse_select_resolve(cte);

			target->type         = TARGET_CTE;
			target->from_cte     = cte;
			target->from_columns = cte->cte_columns;
		}
		return target;
	}

	// FROM name [(args)]
	// FROM schema.name [(args)]
	Str  schema;
	Str  name;
	auto expr = parse_target(self, &schema, &name);
	if (! expr)
		error("FROM target name expected");

	// function()
	if (self->id == STMT_SELECT && stmt_if(self, '('))
	{
		// find function
		auto call = ast_call_allocate();
		call->fn = function_mgr_find(self->function_mgr, &schema, &name);
		if (! call->fn)
			error("FROM %.*s.%.*s(): function not found",
			      str_size(&schema), str_of(&schema),
			      str_size(&name),
			      str_of(&name));
		// parse args ()
		call->ast.r = parse_expr_args(self, NULL, ')', false);

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
		auto select = ast_select_allocate(targets->outer);
		ast_list_add(&self->select_list, &select->ast);
		target->from_columns = &select->ret.columns;
		str_set_str(&target->name, &call->fn->name);
		return target;
	}

	// cte
	auto cte = stmt_list_find(self->stmt_list, &name);
	if (cte)
	{
		if (cte == self)
			error("<%.*s> recursive CTE are not supported",
			      str_size(&name), str_of(&name));
		target->type         = TARGET_CTE;
		target->from_cte     = cte;
		target->from_columns = cte->cte_columns;
		str_set_str(&target->name, &cte->cte_name->string);
		return target;
	}

	// table
	auto table = table_mgr_find(&self->db->table_mgr, &schema, &name, false);
	if (table)
	{
		if (subquery && !table->config->shared)
			error("distributed table '%.*s.%.*s' cannot be used in subquery",
			      str_size(&schema), str_of(&schema),
			      str_size(&name),
			      str_of(&name));
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
parse_from_add(Stmt* self, Targets* targets, bool subquery)
{
	// FROM [schema.]name | (SELECT) [AS] [alias] [USE INDEX (name)]
	auto target = parse_from_target(self, targets, subquery);

	// [AS] [alias]
	auto as = stmt_if(self, KAS);
	auto alias = stmt_if(self, KNAME);
	if (alias) {
		str_set_str(&target->name, &alias->string);
	} else {
		if (as)
			error("AS <name> expected ");
		if ( target->type == TARGET_SELECT ||
		    (target->type == TARGET_CTE && str_empty(&target->name)))
			error("FROM (SELECT) subquery must have an alias");
	}

	// ensure target does not exists
	if (! str_empty(&target->name))
	{
		auto match = targets_match(targets, &target->name);
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
	targets_add(targets, target);

	// [USE INDEX (name)]
	if (stmt_if(self, KUSE))
	{
		if (! stmt_if(self, KINDEX))
			error("USE <INDEX> expected");
		if (! stmt_if(self, '('))
			error("USE INDEX <(> expected");
		auto name = stmt_next_shadow(self);
		if (name->id != KNAME)
			error("USE INDEX (<index name> expected");
		if (! stmt_if(self, ')'))
			error("USE INDEX (<)> expected");
		if (target->type != TARGET_TABLE &&
		    target->type != TARGET_TABLE_SHARED)
			error("USE INDEX expected table target");
		target->from_table_index =
			table_find_index(target->from_table, &name->string, true);
	}

	return target;
}

void
parse_from(Stmt* self, Targets* targets, bool subquery)
{
	// FROM <[schema.]name | (SELECT)> [AS] [alias]> [, ...]
	// FROM <[schema.]name | (SELECT)> [AS] [alias]> [JOIN <..> ON (expr) ...]

	// FROM name | expr
	parse_from_add(self, targets, subquery);

	for (;;)
	{
		// ,
		if (stmt_if(self, ','))
		{
			// name | expr
			parse_from_add(self, targets, subquery);
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
			auto target = parse_from_add(self, targets, subquery);

			// ON
			if (stmt_if(self, KON))
			{
				// expr
				Expr ctx;
				expr_init(&ctx);
				ctx.targets = targets;
				target->join_on = parse_expr(self, &ctx);
				target->join    = join;
				continue;
			}

			// USING (column)
			if (stmt_if(self, KUSING))
			{
				if (! stmt_if(self, '('))
					error("USING <(> expected");
				auto name = stmt_if(self, KNAME);
				if (! name)
					error("USING (<column> expected");
				if (! stmt_if(self, ')'))
					error("USING (column <)> expected");

				// outer.column = target.column
				auto equ = ast('=');
				equ->l = ast(KNAME_COMPOUND);
				equ->r = ast(KNAME_COMPOUND);
				parse_set_target_column(&equ->l->string, &target->prev->name, &name->string);
				parse_set_target_column(&equ->r->string, &target->name, &name->string);

				target->join_on = equ;
				target->join    = join;
				continue;
			}

			error("JOIN name <ON | USING> expected");
			continue;
		}

		break;
	}

	if (targets_count(targets, TARGET_TABLE) > 1)
		error("FROM: only one distributed table can be part of JOIN");
}
