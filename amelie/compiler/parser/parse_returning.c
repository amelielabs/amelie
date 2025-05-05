
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

void
parse_returning(Returning* self, Stmt* stmt, Expr* ctx)
{
	// set returning format
	self->format = *stmt->parser->local->format;

	// * | target.* | expr [AS] [name], ...
	for (;;)
	{
		auto as = ast(KAS);

		// target.*, * or expr
		auto expr = stmt_next(stmt);
		switch (expr->id) {
		case '*':
			expr->id = KSTAR;
			break;
		case KNAME_COMPOUND_STAR:
			break;
		default:
			stmt_push(stmt, expr);
			if (ctx)
			{
				ctx->as = as;
				expr = parse_expr(stmt, ctx);
				ctx->as = NULL;
			} else {
				expr = parse_expr(stmt, ctx);
			}
			break;
		}

		// [AS name]
		// [name]
		auto as_has = stmt_if(stmt, KAS);

		// set column name
		auto name = stmt_if(stmt, KNAME);
		if (unlikely(! name))
		{
			if (as_has)
				stmt_error(stmt, NULL, "label expected");
			if (expr->id == KNAME)
			{
				name = ast(KNAME);
				name->string = expr->string;
			} else
			if (expr->id == KAGGR)
			{
				name = ast(KNAME);
				auto agg = ast_agg_of(expr);
				if (agg->function)
					name->string = agg->function->string;
			}

		} else
		{
			// ensure * has no alias
			if (expr->id == '*')
				stmt_error(stmt, name, "* cannot have an alias");
		}

		// add column to the select expression list
		as->l = expr;
		as->r = name;
		if (self->list == NULL)
			self->list = as;
		else
			self->list_tail->next = as;
		self->list_tail = as;

		// ,
		if (stmt_if(stmt, ','))
			continue;

		break;
	}

	// [FORMAT type]
	if (stmt_if(stmt, KFORMAT))
	{
		auto type = stmt_expect(stmt, KSTRING);
		self->format = type->string;
	}

	// [INTO name]
	if (stmt_if(stmt, KINTO))
	{
		auto name = stmt_expect(stmt, KNAME);
		if (stmt->cte)
			stmt_error(stmt, name, "INTO cannot be used with CTE");
		if (stmt->assign)
			stmt_error(stmt, name, "INTO cannot be used with := operator");
		if (ctx && ctx->select && ctx->subquery)
			stmt_error(stmt, name, "INTO cannot be used inside subquery");
		stmt->assign = vars_add(&stmt->scope->vars, &name->string);
	}
}

static inline void
returning_add(Returning* self, Ast* as)
{
	// add new column to use it instead of name
	auto column = column_allocate();
	column_set_name(column, &as->r->string);
	columns_add(&self->columns, column);
	as->r->id = 0;
	as->r->column = column;

	if (self->list == NULL)
		self->list = as;
	else
		self->list_tail->next = as;
	self->list_tail = as;
	self->count++;
}

static inline void
returning_add_target(Returning* self, Target* target, Ast* star)
{
	// import all available columns into the expression list
	Columns* columns = target->from_columns;

	// use CTE arguments, if they are defined
	auto cte = target->from_cte;
	if (target->type == TARGET_CTE && cte->cte->args.count > 0)
		columns = &cte->cte->args;

	list_foreach(&columns->list)
	{
		auto column = list_at(Column, link);
		auto as = ast(KAS);
		// target.order AS column, ...
		as->l = ast(KNAME_COMPOUND);
		as->l->pos_start = star->pos_start;
		as->l->pos_end   = star->pos_end;
		parse_set_target_column(&as->l->string, &target->name, &column->name);

		// as
		as->r = ast(KNAME);
		as->r->string = column->name;
		returning_add(self, as);
	}
}

void
parse_returning_resolve(Returning* self, Stmt* stmt, Targets* targets)
{
	// rewrite returning list by resolving all * and target.*
	auto as = self->list;
	self->list      = NULL;
	self->list_tail = NULL;
	self->count     = 0;
	for (Ast* next = NULL; as; as = next)
	{
		next = as->next;
		switch (as->l->id) {
		case KSTAR:
		{
			// SELECT * (without from)
			auto targets_ref = targets;
			if (unlikely(targets_empty(targets)))
			{
				if (! targets->outer)
					stmt_error(stmt, as->l, "no targets defined");
				targets_ref = targets->outer;
			}

			// import all available columns
			auto join = targets_ref->list;
			while (join)
			{
				returning_add_target(self, join, as->l);
				join = join->next;
			}
			break;
		}
		case KNAME_COMPOUND_STAR:
		{
			// target.*
			Str name;
			str_split(&as->l->string, &name, '.');

			// find nearest target
			auto match = targets_match_outer(targets, &name);
			if (! match)
				stmt_error(stmt, as->l, "target not found");

			if (as->l->string.pos[str_size(&name) + 1] != '*')
				stmt_error(stmt, as->l,"incorrect target column path");

			returning_add_target(self, match, as->l);
			break;
		}
		default:
		{
			// column has no alias
			if (as->r == NULL)
			{
				auto name_sz = palloc(8);
				snprintf(name_sz, 8, "col%d", self->count + 1);
				as->r = ast(KNAME);
				str_set_cstr(&as->r->string, name_sz);
			}
			returning_add(self, as);
			break;
		}
		}
	}
}
