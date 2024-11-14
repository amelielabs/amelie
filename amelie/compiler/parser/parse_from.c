
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

typedef struct
{
	int     level;
	int     level_seq;
	Target* head;
	Target* tail;
	Stmt*   stmt;
} From;

static inline Ast*
parse_from_analyze(From* self, Cte** cte, Table** table, View** view)
{
	auto stmt = self->stmt;

	// FROM name
	// FROM schema.name
	Str  schema;
	Str  name;
	auto expr = parse_target(stmt, &schema, &name);
	if (! expr)
	{
		// FROM expr

		// (, [, ...
		return parse_expr(stmt, NULL);
	}

	// find cte
	*cte = cte_list_find(stmt->cte_list, &name);
	if (*cte)
	{
		if ((*cte)->stmt == self->stmt->order)
			error("<%.*s> recursive CTE are not supported",
			      str_size(&name), str_of(&name));
		return NULL;
	}

	// find table
	*table = table_mgr_find(&stmt->db->table_mgr, &schema, &name, false);
	if (*table)
		return NULL;

	// find view
	*view = view_mgr_find(&stmt->db->view_mgr, &schema, &name, false);

	// apply view
	if (*view)
	{
		// FROM (SELECT)
		auto lex_prev = stmt->lex;

		// parse view SELECT and return as expression
		Lex lex;
		lex_init(&lex, lex_prev->keywords);
		lex_start(&lex, &(*view)->config->query);
		stmt->lex = &lex;
		stmt_if(stmt, KSELECT);
		auto select = parse_select(stmt);
		stmt->lex = lex_prev;
		return &select->ast;
	}

	// function call
	auto bracket = stmt_if(stmt, '(');
	if (bracket)
	{
		// FROM name()
		// FROM schema.name()
		stmt_push(stmt, bracket);
		stmt_push(stmt, expr);
		return parse_expr(stmt, NULL);
	}

	error("<%.*s> table or view is not found", str_size(&expr->string),
	      str_of(&expr->string));
	return NULL;
}

static inline Target*
parse_from_add(From* self)
{
	auto   stmt  = self->stmt;
	Cte*   cte   = NULL;
	Table* table = NULL;
	View*  view  = NULL;

	// FROM <expr> [alias] [USE INDEX (name)]
	auto expr = parse_from_analyze(self, &cte, &table, &view);
	auto target_list = &self->stmt->target_list;

	// [alias]
	Str  name;
	char name_gen[8];
	auto alias = stmt_if(stmt, KNAME);
	if (alias)
	{
		str_set_str(&name, &alias->string);
	} else
	{
		if (cte) {
			str_set_str(&name, &cte->name->string);
		} else
		if (table) {
			str_set_str(&name, &table->config->name);
		} else
		if (view) {
			str_set_str(&name, &view->config->name);
		} else {
			snprintf(name_gen, sizeof(name_gen), "t%d", target_list->count);
			str_set_cstr(&name, name_gen);
		}
	}

	// ensure target does not exists
	auto target = target_list_match(target_list, &name);
	if (target)
		error("<%.*s> target is redefined, please use different alias for the target",
		      str_size(&name), str_of(&name));

	target = target_list_add(target_list, self->level, self->level_seq++,
	                         &name, expr, NULL, table, cte);
	if (self->head == NULL)
		self->head = target;
	else
		self->tail->next_join = target;
	self->tail = target;

	// set outer target
	target->outer = self->head;

	// set view
	target->view  = view;

	if (cte)
		cte_deps_add(&stmt->cte_deps, cte);

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
		if (! table)
			error("USE INDEX required table table");
		target->index = table_find_index(table, &name->string, true);
	}

	return target;
}

Target*
parse_from(Stmt* self, int level)
{
	// FROM <name [alias]> | <(expr) [alias]> [, ...]
	// FROM <name [alias]> | <(expr) [alias]> [JOIN <..> ON (expr) ...]
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
			target->expr_on = parse_expr(self, NULL);
			target->join    = join;
			continue;
		}

		break;
	}

	return from.head;
}
