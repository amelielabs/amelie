
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

static inline Target*
parse_from_target(Stmt* self, Targets* targets, bool subquery)
{
	auto target = target_allocate(&self->order_targets);

	// FROM (SELECT)
	if (stmt_if(self, '('))
	{
		auto ast = stmt_expect(self, KSELECT);
		AstSelect* select;
		if (subquery)
		{
			select = parse_select(self, targets->outer, true);
			stmt_expect(self, ')');
			target->type         = TARGET_SELECT;
			target->ast          = ast;
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

			select = parse_select(cte, NULL, false);
			stmt_expect(self, ')');
			cte->ast         = &select->ast;
			cte->cte_columns = &select->ret.columns;
			parse_select_resolve(cte);

			target->type         = TARGET_CTE;
			target->ast          = ast;
			target->from_cte     = cte;
			target->from_columns = cte->cte_columns;
		}
		select->ast.pos_start = ast->pos_start;
		select->ast.pos_end   = ast->pos_end;
		return target;
	}

	// FROM name [(args)]
	// FROM schema.name [(args)]
	Str  schema;
	Str  name;
	auto expr = parse_target(self, &schema, &name);
	if (! expr)
		stmt_error(self, NULL, "target name expected");
	target->ast = expr;

	// function()
	if (self->id == STMT_SELECT && stmt_if(self, '('))
	{
		// find function
		auto call = ast_call_allocate();
		call->fn = function_mgr_find(self->function_mgr, &schema, &name);
		if (! call->fn)
			stmt_error(self, expr, "function not found");

		// parse args ()
		call->ast.r = parse_expr_args(self, NULL, ')', false);

		// ensure function can be used inside FROM
		if (call->fn->type != TYPE_STORE &&
		    call->fn->type != TYPE_JSON)
			stmt_error(self, expr, "function must return result set or JSON");

		target->type = TARGET_FUNCTION;
		target->from_function = &call->ast;

		// allocate select to keep returning columns
		auto select = ast_select_allocate(targets->outer);
		select->ast.pos_start = target->ast->pos_start;
		select->ast.pos_end   = target->ast->pos_end;
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
			stmt_error(self, expr, "recursive CTE are not supported");
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
			stmt_error(self, expr, "distributed table cannot be used in subquery");
		if (table->config->shared)
			target->type = TARGET_TABLE_SHARED;
		else
			target->type = TARGET_TABLE;
		target->from_table   = table;
		target->from_columns = &table->config->columns;
		str_set_str(&target->name, &table->config->name);
		return target;
	}

	stmt_error(self, expr, "relation not found");
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
			stmt_error(self, as, "name expected");
		if ( target->type == TARGET_SELECT ||
		    (target->type == TARGET_CTE && str_empty(&target->name)))
			stmt_error(self, NULL, "subquery must have an alias");
	}

	// ensure target does not exists
	if (! str_empty(&target->name))
	{
		auto match = targets_match(targets, &target->name);
		if (match)
			stmt_error(self, NULL, "target is redefined, please use different alias for the target");
	}

	// generate first column to match the target name for function target
	if (target->type == TARGET_FUNCTION)
	{
		auto fn = ast_call_of(target->from_function)->fn;
		auto column = column_allocate();
		column_set_name(column, &target->name);
		column_set_type(column, fn->type, type_sizeof(fn->type));
		columns_add(target->from_columns, column);
	}

	// add target
	targets_add(targets, target);

	// [USE INDEX (name)]
	if (stmt_if(self, KUSE))
	{
		stmt_expect(self, KINDEX);
		stmt_expect(self, '(');
		auto name = stmt_next_shadow(self);
		if (name->id != KNAME)
			stmt_error(self, name, "<index name> expected");
		stmt_expect(self, ')');
		if (target->type != TARGET_TABLE &&
		    target->type != TARGET_TABLE_SHARED)
			stmt_error(self, NULL, "USE INDEX expects table target");
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
			stmt_expect(self, KJOIN);
			join = JOIN_INNER;
		} else
		if (stmt_if(self, KLEFT))
		{
			// [OUTER]
			stmt_if(self, KOUTER);
			stmt_expect(self, KJOIN);
			join = JOIN_LEFT;
		} else
		if (stmt_if(self, KRIGHT))
		{
			// [OUTER]
			stmt_if(self, KOUTER);
			stmt_expect(self, KJOIN);
			join = JOIN_RIGHT;
		}

		// <name|expr> ON expr
		if (join != JOIN_NONE)
		{
			if (join == JOIN_LEFT || join == JOIN_RIGHT)
				stmt_error(self, NULL, "outer joins currently are not supported");

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
				stmt_expect(self, '(');
				auto name = stmt_expect(self, KNAME);
				stmt_expect(self, ')');

				// outer.column = target.column
				auto equ = ast('=');
				equ->l = ast(KNAME_COMPOUND);
				equ->l->pos_start = name->pos_start;
				equ->l->pos_end   = name->pos_end;

				equ->r = ast(KNAME_COMPOUND);
				equ->r->pos_start = name->pos_start;
				equ->r->pos_end   = name->pos_end;

				parse_set_target_column(&equ->l->string, &target->prev->name, &name->string);
				parse_set_target_column(&equ->r->string, &target->name, &name->string);

				target->join_on = equ;
				target->join    = join;
				continue;
			}

			stmt_error(self, NULL, "ON or USING expected");
			continue;
		}

		break;
	}

	if (targets_count(targets, TARGET_TABLE) > 1)
		stmt_error(self, NULL, "only one distributed table can be part of JOIN");
}
