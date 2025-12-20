
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

#include <amelie_base.h>
#include <amelie_os.h>
#include <amelie_lib.h>
#include <amelie_json.h>
#include <amelie_runtime.h>
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
#include <amelie_catalog.h>
#include <amelie_wal.h>
#include <amelie_storage.h>
#include <amelie_backup.h>
#include <amelie_repl.h>
#include <amelie_value.h>
#include <amelie_set.h>
#include <amelie_output.h>
#include <amelie_executor.h>
#include <amelie_func.h>
#include <amelie_vm.h>
#include <amelie_parser.h>

static Target*
parse_from_target(Stmt* self, From* from, AccessType access, bool subquery)
{
	auto target = target_allocate();

	// force read keywords as names
	stmt_push(self, stmt_next_shadow(self));

	// FROM (SELECT | expr)
	if (stmt_if(self, '('))
	{
		auto ast = stmt_next(self);
		if (ast->id == KSELECT)
		{
			AstSelect* select;
			if (subquery)
			{
				select = parse_select(self, from->outer, true);
				stmt_expect(self, ')');
				target->type    = TARGET_EXPR;
				target->ast     = ast;
				target->columns = &select->ret.columns;
			} else
			{
				// rewrite FROM (SELECT) as dependable statement (this can recurse)
				auto stmt = stmt_allocate(self->parser, &self->parser->lex, self->block);
				stmt->id = STMT_SELECT;
				stmts_insert(&self->block->stmts, self, stmt);
				deps_add_stmt(&self->deps, stmt);

				select = parse_select(stmt, NULL, false);
				stmt_expect(self, ')');
				stmt->ast = &select->ast;
				stmt->ret = &select->ret;
				parse_select_resolve(stmt);

				target->type      = TARGET_STMT;
				target->ast       = ast;
				target->from_stmt = stmt;
				target->columns   = &select->ret.columns;
			}
			select->ast.pos_start = ast->pos_start;
			select->ast.pos_end   = ast->pos_end;
		} else
		{
			stmt_push(self, ast);

			// (expr)
			Expr ctx;
			expr_init(&ctx);
			ctx.select = true;
			ctx.aggs = NULL;
			ctx.from = from->outer;
			ast = parse_expr(self, &ctx);
			stmt_expect(self, ')');

			target->type = TARGET_EXPR;
			target->ast  = ast;

			// allocate select to keep returning columns
			auto select = ast_select_allocate(self, from->outer, from->block);
			select->ast.pos_start = target->ast->pos_start;
			select->ast.pos_end   = target->ast->pos_end;
			target->columns = &select->ret.columns;
		}
		return target;
	}

	// FROM name [(args)]
	auto name = stmt_expect(self, KNAME);
	target->ast = name;

	// function()
	if ((self->id == STMT_SELECT || self->id == STMT_FOR) &&
	    stmt_if(self, '('))
	{
		// find function
		auto func = ast_func_allocate();
		func->fn = function_mgr_find(share()->function_mgr, &name->string);
		if (! func->fn)
			stmt_error(self, name, "function not found");

		// parse args ()
		func->ast.r = parse_expr_args(self, NULL, ')', false);

		target->type = TARGET_FUNCTION;
		target->ast  = &func->ast;

		// allocate select to keep returning columns
		auto select = ast_select_allocate(self, from->outer, from->block);
		select->ast.pos_start = target->ast->pos_start;
		select->ast.pos_end   = target->ast->pos_end;
		target->columns = &select->ret.columns;
		str_set_str(&target->name, &func->fn->name);
		return target;
	}

	// var
	auto var = namespace_find_var(self->block->ns, &name->string);
	if (var)
	{
		target->type     = TARGET_VAR;
		target->from_var = var;
		if (var->writer)
			deps_add_var(&self->deps, var->writer, var);
		if (var->columns.count > 0) {
			target->columns = &var->columns;
		} else
		{
			// allocate select to keep returning columns
			auto select = ast_select_allocate(self, from->outer, from->block);
			select->ast.pos_start = target->ast->pos_start;
			select->ast.pos_end   = target->ast->pos_end;
			target->columns = &select->ret.columns;
		}
		str_set_str(&target->name, var->name);
		return target;
	}

	// cte
	auto stmt = block_find_cte(self->block, &name->string);
	if (stmt)
	{
		if (stmt == self)
			stmt_error(self, name, "recursive CTE are not supported");
		target->type      = TARGET_STMT;
		target->from_stmt = stmt;
		target->columns   = &stmt->cte_columns;
		str_set_str(&target->name, stmt->cte_name);
		deps_add_stmt(&self->deps, stmt);
		return target;
	}

	// table
	auto table = table_mgr_find(&share()->storage->catalog.table_mgr,
	                            self->parser->db,
	                            &name->string, false);
	if (table)
	{
		target->type        = TARGET_TABLE;
		target->from_access = access;
		target->from_table  = table;
		target->columns     = &table->config->columns;
		str_set_str(&target->name, &table->config->name);

		access_add(&self->parser->program->access, &table->rel, access);

		// [USE INDEX (name)]
		if (stmt_if(self, KUSE))
		{
			stmt_expect(self, KINDEX);
			stmt_expect(self, '(');
			auto name_index = stmt_next_shadow(self);
			if (name_index->id != KNAME)
				stmt_error(self, name_index, "<index name> expected");
			stmt_expect(self, ')');
			target->from_index = table_find_index(target->from_table, &name_index->string, true);
		}
		return target;
	}

	stmt_error(self, name, "relation not found");
	return NULL;
}

Target*
parse_from_add(Stmt* self, From* from, AccessType access,
               Str*  as,
               bool  subquery)
{
	// FROM name | (SELECT) [AS] [alias] [USE INDEX (name)]
	auto target = parse_from_target(self, from, access, subquery);

	// [AS] [alias]
	if (as)
	{
		// forced alias
		str_set_str(&target->name, as);
	} else
	{
		Ast* alias = NULL;
		if (stmt_if(self, KAS))
			alias = stmt_expect(self, KNAME);
		else
			alias = stmt_if(self, KNAME);
		if (alias) {
			str_set_str(&target->name, &alias->string);
		} else {
			if (str_empty(&target->name))
				stmt_error(self, NULL, "subquery must have an alias");
		}
	}

	// ensure target does not exists
	if (! str_empty(&target->name))
	{
		auto match = from_match(from, &target->name);
		if (match)
			stmt_error(self, NULL, "target is redefined, please use different alias for the target");
	}

	// generate first column to match the target name for function target
	if (target->type == TARGET_EXPR ||
	    target->type == TARGET_FUNCTION)
	{
		auto column = column_allocate();
		column_set_name(column, &target->name);
		columns_add(target->columns, column);
	} else
	if (target->type == TARGET_VAR && target->from_var->type != TYPE_STORE)
	{
		auto column = column_allocate();
		column_set_name(column, &target->name);
		column_set_type(column, target->from_var->type, type_sizeof(target->from_var->type));
		columns_add(target->columns, column);
	}

	// add target
	from_add(from, target);
	return target;
}

void
parse_from(Stmt* self, From* from, AccessType access, bool subquery)
{
	// FROM <name | (SELECT)> [AS] [alias]> [, ...]
	// FROM <name | (SELECT)> [AS] [alias]> [JOIN <..> ON (expr) ...]

	// FROM name | expr
	parse_from_add(self, from, access, NULL, subquery);

	Ast* join_on = NULL;
	for (;;)
	{
		// ,
		if (stmt_if(self, ','))
		{
			// name | expr
			parse_from_add(self, from, ACCESS_RO_EXCLUSIVE, NULL, subquery);
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

		if (join == JOIN_NONE)
			break;

		if (join == JOIN_LEFT || join == JOIN_RIGHT)
			stmt_error(self, NULL, "outer joins currently are not supported");

		// JOIN target
		auto target = parse_from_add(self, from, ACCESS_RO_EXCLUSIVE, NULL, subquery);

		// save the names count for AND before processing the next expression
		auto names_count = from->list_names.count;

		// ON expr
		auto next = stmt_next(self);
		switch (next->id) {
		case KON:
		{
			// ON expr
			Expr ctx;
			expr_init(&ctx);
			ctx.from  = from;
			ctx.names = &from->list_names;
			target->join_on = parse_expr(self, &ctx);
			target->join    = join;
			break;
		}
		case KUSING:
		{
			// USING (column)
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

			ast_list_add(&from->list_names, equ->l);
			ast_list_add(&from->list_names, equ->r);
			break;
		}
		default:
			stmt_error(self, NULL, "ON or USING expected");
			break;
		}

		// combine join expressions together
		//
		// JOIN ON expr AND ...
		if (target->join_on)
		{
			if (join_on == NULL) {
				join_on = target->join_on;
			} else
			{
				auto and = ast(KAND);
				and->l = join_on;
				and->r = target->join_on;
				and->integer = names_count;
				join_on = and;
			}
		}
	}

	from->join_on = join_on;
}
