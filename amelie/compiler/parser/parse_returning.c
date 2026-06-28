
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

#include <amelie_runtime>
#include <amelie_server>
#include <amelie_db>
#include <amelie_sync>
#include <amelie_vm>
#include <amelie_parser.h>

void
parse_returning(Returning* self, Stmt* stmt, Expr* ctx)
{
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

			// derive name from the expression, if possible
			switch (expr->id) {
			case KNAME:
			{
				name = ast(KNAME);
				name->string = expr->string;
				break;
			}
			case KAGGR:
			{
				auto agg = ast_agg_of(expr);
				if (agg->function)
				{
					name = ast(KNAME);
					name->string = agg->function->string;
				}
				break;
			}
			case KFUNC:
			{
				auto func = ast_func_of(expr);
				if (func->fn)
				{
					name = ast(KNAME);
					name->string = func->fn->name;
				} else
				if (func->udf)
				{
					name = ast(KNAME);
					name->string = func->udf->config->name;
				}
				break;
			}
			case KMETHOD:
			{
				auto func = ast_func_of(expr->r);
				if (func->fn)
				{
					name = ast(KNAME);
					name->string = func->fn->name;
				}
				break;
			}
			case KVALUE:
			{
				// constified function
				name = ast(KNAME);
				name->string = expr->fn->name;
				break;
			}
			case KCURRENT_TIMESTAMP:
			case KCURRENT_DATE:
			case KCURRENT_USER:
			case KCURRENT_AGENT:
			{
				name = ast(KNAME);
				name->string = expr->string;
				break;
			}
			case '.':
			{
				Str path = expr->r->string;
				for (;;)
				{
					Str part;
					if (str_split(&path, &part, '.'))
					{
						str_advance(&path, str_size(&part) + 1);
						continue;
					}
					path = part;
					break;
				}

				name = ast(KNAME);
				name->string = path;
				break;
			}
			default:
			{
#if 0
				name = ast(KNAME);
				if (parse_expr_is_const(expr))
					str_set(&name->string, "value", 5);
				else
					str_set(&name->string, "expr", 4);
#endif
				break;
			}
			}
		} else
		{
			// ensure * has no alias
			if (expr->id == KSTAR || expr->id == KNAME_COMPOUND_STAR)
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

	// [INTO name, ...]
	auto into = stmt_if(stmt, KINTO);
	if (into)
	{
		if (stmt->is_return)
			stmt_error(stmt, into, "INTO and RETURN cannot be used together");

		if (stmt->cte_name)
			stmt_error(stmt, into, "INTO cannot be used with CTE");

		if (ctx && ctx->select && ctx->subquery)
			stmt_error(stmt, into, "INTO cannot be used inside subquery");

		for (;;)
		{
			// variable name
			auto var = stmt_expect(stmt, KNAME);
			if (self->list_into == NULL)
				self->list_into = var;
			else
				self->list_into_tail->next = var;
			self->list_into_tail = var;
			self->count_into++;

			// ,
			if (stmt_if(stmt, ','))
				continue;

			break;
		}
	}
}

void
parse_returning_star(Returning* self)
{
	// select *
	auto as = ast(KAS);
	as->l = ast(KSTAR);
	as->r = NULL;
	if (self->list == NULL)
		self->list = as;
	else
		self->list_tail->next = as;
	self->list_tail = as;
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
	auto columns = target->columns;
	list_foreach(&columns->list)
	{
		auto column = list_at(Column, link);
		if (column->dropped)
			continue;

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

static void
parse_returning_resolve_exprs(Returning* self, Stmt* stmt, From* from)
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
			auto from_ref = from;
			if (unlikely(from_empty(from)))
			{
				if (! from->outer)
					stmt_error(stmt, as->l, "no targets defined");
				from_ref = from->outer;
			}

			// import all available columns
			auto join = from_ref->list;
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
			auto match = block_find_target(from, &name);
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
				auto name_sz = palloc(32);
				format(name_sz, 32, "col{d}", self->count + 1);
				as->r = ast(KNAME);
				str_set_cstr(&as->r->string, name_sz);
			}
			returning_add(self, as);
			break;
		}
		}
	}
}

void
parse_returning_resolve(Returning* self, Stmt* stmt, From* from)
{
	// rewrite returning list by resolving all * and target.*
	parse_returning_resolve_exprs(self, stmt, from);

	// resolve INTO variables
	if (! self->count_into)
		return;

	if (self->count_into > self->count)
		stmt_error(stmt, NULL, "number of INTO variables exceed returning expressions");

	auto ref = self->list_into;
	for (; ref; ref = ref->next)
	{
		auto var = namespace_find_var(stmt->block->ns, &ref->string);
		if (! var)
			stmt_error(stmt, NULL, "variable '{str}' not found", &ref->string);
		ref->var = var;

		// add dep for previous var updating stmt and update writer to self
		if (var->writer)
			deps_add_var(&stmt->deps, var->writer, var);
		var->writer = stmt;
	}
}
