
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
#include <amelie_data.h>
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

Ast*
parse_returning(Stmt* self, Expr* ctx)
{
	// expr [AS] [name], ...
	// *
	Ast* expr_head = NULL;
	Ast* expr_prev = NULL;
	for (auto order = 1;; order++)
	{
		// * or expr
		auto expr = stmt_if(self, '*');
		// todo: target.*

		if (! expr)
			expr = parse_expr(self, ctx);

		// [AS name]
		// [name]
		auto as_has = true;
		auto as = stmt_if(self, KAS);
		if (! as)
		{
			as_has = false;
			as = ast(KAS);
		}

		// set column name
		auto name = stmt_if(self, KNAME);
		if (unlikely(! name))
		{
			if (as_has)
				error("AS <label> expected");

			if (expr->id == KNAME) {
				name = expr;
			} else
			if (expr->id != '*')
			{
				auto name_sz = palloc(8);
				snprintf(name_sz, 8, "col%d", order);
				name = ast(KNAME);
				str_set_cstr(&name->string, name_sz);
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
		if (expr_head == NULL)
			expr_head = as;
		else
			expr_prev->next = as;
		expr_prev = as;

		// ,
		if (stmt_if(self, ','))
			continue;

		break;
	}

	return expr_head;
}

typedef struct Returning Returning;

struct Returning
{
	Ast* expr_head;
	Ast* expr_prev;
	int  order;
};

static inline void
returning_init(Returning* self)
{
	memset(self, 0, sizeof(*self));
}

static inline void
returning_add(Returning* self, Ast* as)
{
	if (self->expr_head == NULL)
		self->expr_head = as;
	else
		self->expr_prev->next = as;
	self->expr_prev = as;
	self->order++;
}

static inline void
returning_add_target(Returning* self, Target* target)
{
	// import all available columns into the expression list
	list_foreach(&target->from_columns->list)
	{
		auto column = list_at(Column, link);
		auto as = ast(KAS);
		// target.column AS column, ...
		as->l = ast(KNAME_COMPOUND);
		auto size = str_size(&target->name) + 1 + str_size(&column->name);
		auto name = palloc(size);
		memcpy(name, target->name.pos, str_size(&target->name));
		memcpy(name + str_size(&target->name), ".", 1);
		memcpy(name + str_size(&target->name) + 1, column->name.pos,
		       str_size(&column->name));
		str_set(&as->l->string, name, size);
		as->r = ast(KNAME);
		as->r->string = column->name;

		returning_add(self, as);
	}
}

Ast*
parse_returning_resolve(Stmt* self, Ast* origin, Target* target)
{
	Returning ret;
	returning_init(&ret);
	auto as = origin;
	while (as)
	{
		auto next = as->next;

		// *
		if (as->l->id == '*')
		{
			// import all available columns
			auto join = target;
			while (join)
			{
				returning_add_target(&ret, join);
				join = target->next_join;
			}
			continue;
		}

		// target.*
		if (as->l->id == KNAME_COMPOUND_STAR)
		{
			// find target in the target list
			Str name;
			str_split(&as->l->string, &name, '.');

			// find target
			auto match = target_list_match(&self->target_list, &name);
			if (! match)
				error("<%.*s> target not found", str_size(&name), str_of(&name));

			if (as->l->string.pos[str_size(&name) + 1] != '*')
				error("<%.*s> incorrect target column path", str_size(&as->l->string),
				      str_of(&as->l->string));

			returning_add_target(&ret, target);
			continue;
		}

		returning_add(&ret, as);
		as = next;
	}
	return ret.expr_head;
}
