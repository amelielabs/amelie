
//
// sonata.
//
// SQL Database for JSON.
//

#include <sonata_runtime.h>
#include <sonata_io.h>
#include <sonata_lib.h>
#include <sonata_data.h>
#include <sonata_config.h>
#include <sonata_auth.h>
#include <sonata_http.h>
#include <sonata_client.h>
#include <sonata_server.h>
#include <sonata_def.h>
#include <sonata_transaction.h>
#include <sonata_index.h>
#include <sonata_storage.h>
#include <sonata_wal.h>
#include <sonata_db.h>
#include <sonata_value.h>
#include <sonata_aggr.h>
#include <sonata_executor.h>
#include <sonata_vm.h>
#include <sonata_parser.h>

typedef struct
{
	int     level;
	int     level_seq;
	Target* head;
	Target* tail;
	Stmt*   stmt;
} From;

static inline Ast*
parse_from_analyze(From* self, Stmt** cte, Table** table, View** view)
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
	*cte = stmt_list_find(stmt->stmt_list, &name);
	if (*cte)
	{
		if (*cte == self->stmt)
			error("<%.*s> recursive CTE are not supported",
			      str_size(&self->stmt->name->string),
			      str_of(&self->stmt->name->string));
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
	Stmt*  cte   = NULL;
	Table* table = NULL;
	View*  view  = NULL;

	// FROM <expr> [alias]
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
	                         &name, expr, table, cte);
	if (self->head == NULL)
		self->head = target;
	else
		self->tail->next_join = target;
	self->tail = target;

	// set outer target
	target->outer = self->head;

	// set view
	target->view  = view;
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

		// JOIN <target> [ON (expr)]
		if (stmt_if(self, KJOIN))
		{
			// JOIN <name|expr>
			auto target = parse_from_add(&from);

			// ON
			if (stmt_if(self, KON))
			{
				// (
				if (! stmt_if(self, '('))
					error("JOIN ON <(> expected");

				// expr
				target->expr_on = parse_expr(self, NULL);

				// )
				if (! stmt_if(self, ')'))
					error("JOIN ON (expr <)> expected");
			}

			continue;
		}

		break;
	}

	return from.head;
}
