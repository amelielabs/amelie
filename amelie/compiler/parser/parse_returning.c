
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
#include <amelie_executor.h>
#include <amelie_vm.h>
#include <amelie_parser.h>

void
parse_returning(Returning* self, Stmt* stmt, Expr* ctx)
{
	// * | target.* | expr [AS] [name], ...
	for (;;)
	{
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
			expr = parse_expr(stmt, ctx);
			break;
		}

		// [AS name]
		// [name]
		auto as_has = true;
		auto as = stmt_if(stmt, KAS);
		if (! as)
		{
			as_has = false;
			as = ast(KAS);
		}

		// set column name
		auto name = stmt_if(stmt, KNAME);
		if (unlikely(! name))
		{
			if (as_has)
				error("AS <label> expected");
			if (expr->id == KNAME)
			{
				name = ast(KNAME);
				name->string = expr->string;
			}
		} else
		{
			// ensure * has no alias
			if (expr->id == '*')
				error("<*> cannot have an alias");
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
}

static inline void
returning_add(Returning* self, Ast* as)
{
	// add new column use it instead of name
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
returning_add_target(Returning* self, Target* target)
{
	// import all available columns into the expression list
	list_foreach(&target->from_columns->list)
	{
		auto column = list_at(Column, link);
		auto as = ast(KAS);
		// target.order AS column, ...
		as->l = ast(KNAME_COMPOUND);
		/*
		auto name_size_reserve = str_size(&target->name) + 1 + 4;
		auto name = palloc(name_size_reserve);
		auto name_size =
			snprintf(name, name_size_reserve, "%.*s.%d",
			         str_size(&target->name),
			         str_of(&target->name), column->order);
		str_set(&as->l->string, name, name_size);
		*/
		auto size = str_size(&target->name) + 1 + str_size(&column->name);
		auto name = palloc(size);
		memcpy(name, target->name.pos, str_size(&target->name));
		memcpy(name + str_size(&target->name), ".", 1);
		memcpy(name + str_size(&target->name) + 1, column->name.pos,
		       str_size(&column->name));
		str_set(&as->l->string, name, size);

		// as
		as->r = ast(KNAME);
		as->r->string = column->name;
		returning_add(self, as);
	}
}

void
parse_returning_resolve(Returning* self, Stmt* stmt, Target* target)
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
			// *
			if (unlikely(! target))
			{
				if (! stmt->target_list.count)
					error("'*': no targets defined");
				if (stmt->target_list.count > 1)
					error("'*': explicit target name is required");
				returning_add_target(self, stmt->target_list.list);
				continue;
			}

			// import all available columns
			auto join = target;
			while (join)
			{
				returning_add_target(self, join);
				join = target->next_join;
			}
			break;
		}
		case KNAME_COMPOUND_STAR:
		{
			// target.*

			// find target in the target list
			Str name;
			str_split(&as->l->string, &name, '.');

			// find target
			auto match = target_list_match(&stmt->target_list, &name);
			if (! match)
				error("<%.*s> target not found", str_size(&name), str_of(&name));

			if (as->l->string.pos[str_size(&name) + 1] != '*')
				error("<%.*s> incorrect target column path", str_size(&as->l->string),
				      str_of(&as->l->string));

			returning_add_target(self, match);
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
