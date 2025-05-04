
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

static inline Target*
parse_from_target(Scope* self, Targets* targets, AccessType access, bool subquery)
{
	auto target = target_allocate(&self->stmt->order_targets);
	// FROM (expr | SELECT)
	if (scope_if(self, '('))
	{
		auto ast = scope_next(self);
		if (ast->id == KSELECT)
		{
			AstSelect* select;
			if (subquery)
			{
				select = parse_select(self, targets->outer, true);
				scope_expect(self, ')');
				target->type         = TARGET_SELECT;
				target->ast          = ast;
				target->from_select  = &select->ast;
				target->from_columns = &select->ret.columns;
			} else
			{
				// rewrite FROM (SELECT) as CTE statement (this can recurse)
				auto stmt_current = self->stmt;
				auto stmt = stmt_allocate(self);
				stmt->id = STMT_SELECT;
				stmts_insert(&self->stmts, stmt_current, stmt);
				self->stmt = stmt;

				select = parse_select(self, NULL, false);
				scope_expect(self, ')');
				stmt->ast          = &select->ast;
				stmt->cte          = ctes_add(&self->ctes, stmt, NULL);
				stmt->cte->columns = &select->ret.columns;
				parse_select_resolve(self);
				self->stmt = stmt_current;

				target->type         = TARGET_CTE;
				target->ast          = ast;
				target->from_cte     = stmt;
				target->from_columns = stmt->cte->columns;
			}
			select->ast.pos_start = ast->pos_start;
			select->ast.pos_end   = ast->pos_end;
		} else
		{
			scope_push(self, ast);

			// FROM(expr)
			Expr ctx;
			expr_init(&ctx);
			ctx.select = true;
			ctx.aggs = NULL;
			ctx.targets = targets->outer;
			ast = parse_expr(self, &ctx);
			scope_expect(self, ')');

			target->type = TARGET_EXPR;
			target->ast  = ast;

			// allocate select to keep returning columns
			auto select = ast_select_allocate(targets->outer);
			select->ast.pos_start = target->ast->pos_start;
			select->ast.pos_end   = target->ast->pos_end;
			ast_list_add(&self->stmt->select_list, &select->ast);
			target->from_columns = &select->ret.columns;
		}
		return target;
	}

	// FROM name [(args)]
	// FROM schema.name [(args)]
	Str  schema;
	Str  name;
	auto expr = parse_target(self, &schema, &name);
	if (! expr)
		scope_error(self, NULL, "target name expected");
	target->ast = expr;

	// function()
	if (self->stmt->id == STMT_SELECT && scope_if(self, '('))
	{
		// find function
		auto call = ast_call_allocate();
		call->fn = function_mgr_find(self->parser->function_mgr, &schema, &name);
		if (! call->fn)
			scope_error(self, expr, "function not found");

		// ensure function is not a udf
		if (call->fn->flags & FN_UDF)
			scope_error(self, expr, "user-defined function must be invoked using CALL statement");

		// parse args ()
		call->ast.r = parse_expr_args(self, NULL, ')', false);

		// ensure function can be used inside FROM
		if (call->fn->type != TYPE_STORE &&
		    call->fn->type != TYPE_JSON)
			scope_error(self, expr, "function must return result set or JSON");

		target->type = TARGET_FUNCTION;
		target->from_function = &call->ast;

		// allocate select to keep returning columns
		auto select = ast_select_allocate(targets->outer);
		select->ast.pos_start = target->ast->pos_start;
		select->ast.pos_end   = target->ast->pos_end;
		ast_list_add(&self->stmt->select_list, &select->ast);
		target->from_columns = &select->ret.columns;
		str_set_str(&target->name, &call->fn->name);
		return target;
	}

	// cte
	auto cte = ctes_find(&self->ctes, &name);
	if (cte)
	{
		if (cte->stmt == self->stmt)
			scope_error(self, expr, "recursive CTE are not supported");
		target->type         = TARGET_CTE;
		target->from_cte     = cte->stmt;
		target->from_columns = cte->columns;
		str_set_str(&target->name, cte->name);
		return target;
	}

	// table
	auto table = table_mgr_find(&self->parser->db->table_mgr, &schema, &name, false);
	if (table)
	{
		target->type = TARGET_TABLE;
		target->from_access  = access;
		target->from_table   = table;
		target->from_columns = &table->config->columns;
		str_set_str(&target->name, &table->config->name);

		access_add(&self->parser->program->access, table, access);
		return target;
	}

	scope_error(self, expr, "relation not found");
	return NULL;
}

static inline Target*
parse_from_add(Scope* self, Targets* targets, AccessType access, bool subquery)
{
	// FROM [schema.]name | (SELECT) [AS] [alias] [USE INDEX (name)]
	auto target = parse_from_target(self, targets, access, subquery);

	// [AS] [alias]
	auto as = scope_if(self, KAS);
	auto alias = scope_if(self, KNAME);
	if (alias) {
		str_set_str(&target->name, &alias->string);
	} else {
		if (as)
			scope_error(self, as, "name expected");
		if ( target->type == TARGET_SELECT ||
		     target->type == TARGET_EXPR   ||
		    (target->type == TARGET_CTE && str_empty(&target->name)))
			scope_error(self, NULL, "subquery must have an alias");
	}

	// ensure target does not exists
	if (! str_empty(&target->name))
	{
		auto match = targets_match(targets, &target->name);
		if (match)
			scope_error(self, NULL, "target is redefined, please use different alias for the target");
	}

	// generate first column to match the target name for function target
	if (target->type == TARGET_FUNCTION)
	{
		auto fn = ast_call_of(target->from_function)->fn;
		auto column = column_allocate();
		column_set_name(column, &target->name);
		column_set_type(column, fn->type, type_sizeof(fn->type));
		columns_add(target->from_columns, column);
	} else
	if (target->type == TARGET_EXPR)
	{
		auto column = column_allocate();
		column_set_name(column, &target->name);
		columns_add(target->from_columns, column);
	}

	// add target
	targets_add(targets, target);

	// [USE INDEX (name) | HEAP]
	if (scope_if(self, KUSE))
	{
		if (scope_if(self, KINDEX))
		{
			scope_expect(self, '(');
			auto name = scope_next_shadow(self);
			if (name->id != KNAME)
				scope_error(self, name, "<index name> expected");
			scope_expect(self, ')');
			if (target->type != TARGET_TABLE)
				scope_error(self, NULL, "USE INDEX expects table target");
			target->from_index = table_find_index(target->from_table, &name->string, true);
		} else
		if (scope_if(self, KHEAP))
		{
			target->from_heap = true;
		} else {
			scope_error(self, NULL, "USE INDEX or HEAP expected");
		}
	}

	return target;
}

void
parse_from(Scope* self, Targets* targets, AccessType access, bool subquery)
{
	// FROM <[schema.]name | (SELECT)> [AS] [alias]> [, ...]
	// FROM <[schema.]name | (SELECT)> [AS] [alias]> [JOIN <..> ON (expr) ...]

	// FROM name | expr
	parse_from_add(self, targets, access, subquery);

	for (;;)
	{
		// ,
		if (scope_if(self, ','))
		{
			// name | expr
			parse_from_add(self, targets, ACCESS_RO_EXCLUSIVE, subquery);
			continue;
		}

		TargetJoin join = JOIN_NONE;

		// [JOIN]
		// [INNER JOIN]
		// [LEFT [OUTER] JOIN]
		// [RIGHT [OUTER] JOIN]
		if (scope_if(self, KJOIN))
			join = JOIN_INNER;
		else
		if (scope_if(self, KINNER))
		{
			scope_expect(self, KJOIN);
			join = JOIN_INNER;
		} else
		if (scope_if(self, KLEFT))
		{
			// [OUTER]
			scope_if(self, KOUTER);
			scope_expect(self, KJOIN);
			join = JOIN_LEFT;
		} else
		if (scope_if(self, KRIGHT))
		{
			// [OUTER]
			scope_if(self, KOUTER);
			scope_expect(self, KJOIN);
			join = JOIN_RIGHT;
		}

		// <name|expr> ON expr
		if (join != JOIN_NONE)
		{
			if (join == JOIN_LEFT || join == JOIN_RIGHT)
				scope_error(self, NULL, "outer joins currently are not supported");

			// <name|expr>
			auto target = parse_from_add(self, targets, ACCESS_RO_EXCLUSIVE, subquery);

			// ON
			if (scope_if(self, KON))
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
			if (scope_if(self, KUSING))
			{
				scope_expect(self, '(');
				auto name = scope_expect(self, KNAME);
				scope_expect(self, ')');

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

			scope_error(self, NULL, "ON or USING expected");
			continue;
		}

		break;
	}
}
