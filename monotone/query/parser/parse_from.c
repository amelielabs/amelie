
//
// monotone
//
// SQL OLTP database
//

#include <monotone_runtime.h>
#include <monotone_io.h>
#include <monotone_data.h>
#include <monotone_lib.h>
#include <monotone_config.h>
#include <monotone_auth.h>
#include <monotone_schema.h>
#include <monotone_transaction.h>
#include <monotone_storage.h>
#include <monotone_wal.h>
#include <monotone_db.h>
#include <monotone_request.h>
#include <monotone_value.h>
#include <monotone_aggr.h>
#include <monotone_vm.h>
#include <monotone_parser.h>

typedef struct
{
	int     level;
	int     level_seq;
	Target* head;
	Target* tail;
	Stmt*   stmt;
} From;

static inline Ast*
parse_from_analyze(From* self, Table** table, Meta** view)
{
	auto stmt = self->stmt;
	auto expr = stmt_next(stmt);
	switch (expr->id) {
	case KNAME:
	{
		// FROM table or view
		
		// find table first
		*table = table_mgr_find(&stmt->db->table_mgr, &expr->string, false);
		if (*table)
			break;

		// find view
		*view = meta_mgr_find(&stmt->db->meta_mgr, &expr->string, false);
		if (*view)
		{
			auto bracket = stmt_if(stmt, '(');
			if (bracket)
			{
				// handle as expression if view has a call ()
				*view = NULL;
				stmt_push(stmt, bracket);
				stmt_push(stmt, expr);

				expr = parse_expr(stmt, NULL);
				break;
			}

		} else
			error("<%.*s> table or view does not exists",
			      str_size(&expr->string),
			      str_of(&expr->string));
		break;
	}

	default:
		// FROM expr

		// (, [, or any function
		stmt_push(stmt, expr);
		expr = parse_expr(stmt, NULL);
		break;
	}

	// execute view as a function
	if (*view)
	{
		// view()

		// '(' (name, call)
		auto call = ast('(');
		call->l = expr;
		call->r = ast(KCALL);
		expr = call;
	} else
	if (*table) {
		expr = NULL;
	}

	return expr;
}

static inline Target*
parse_from_add(From* self)
{
	auto   stmt  = self->stmt;
	Table* table = NULL;
	Meta*  view  = NULL;

	// FROM <expr> [alias]
	auto expr = parse_from_analyze(self, &table, &view);
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

	// ensure target is not exists
	auto target = target_list_match(target_list, &name);
	if (target)
		error("<%.*s> target is redefined, please use different alias for the target",
		      str_of(&name), str_size(&name));

	target = target_list_add(target_list, self->level, self->level_seq++,
	                         &name, expr, table);
	if (self->head == NULL)
		self->head = target;
	else
		self->tail->next_join = target;
	self->tail = target;

	// set outer target
	target->outer = self->head;
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
