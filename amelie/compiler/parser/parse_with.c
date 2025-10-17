
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

static void
parse_with_args(Stmt* self, Columns* columns)
{
	// )
	if (stmt_if(self, ')'))
		return;

	for (;;)
	{
		// name
		auto name = stmt_expect(self, KNAME);

		// ensure argument is unique
		auto arg = columns_find(columns, &name->string);
		if (arg)
			stmt_error(self, name, "argument redefined");

		// add argument to the list
		arg = column_allocate();
		column_set_name(arg, &name->string);
		columns_add(columns, arg);

		// ,
		if (! stmt_if(self, ','))
			break;
	}

	// )
	stmt_expect(self, ')');
}

hot void
parse_with_validate(Stmt* stmt)
{
	// ensure stmt is not utility
	if (stmt_is_utility(stmt) ||
	    stmt->id == STMT_IF   ||
	    stmt->id == STMT_FOR  ||
	    stmt->id == STMT_RETURN)
		stmt_error(stmt, stmt->ast, "statement cannot be used inside CTE");
}

hot void
parse_with(Parser* self, Block* block)
{
	auto lex = &self->lex;
	for (;;)
	{
		// name [(args)] AS ( stmt )[, ...]
		auto name = lex_expect(lex, KNAME);

		// ensure CTE is not redefined
		auto cte = block_find_cte(block, &name->string);
		if (cte)
			lex_error(lex, name, "CTE is redefined");
		cte = stmt_allocate(self, lex, block);
		cte->cte_name = &name->string;
		stmts_add(&block->stmts, cte);

		// (args)
		if (lex_if(lex, '('))
			parse_with_args(cte, &cte->cte_columns);

		// AS (
		stmt_expect(cte, KAS);
		auto start = stmt_expect(cte, '(');

		// parse stmt (cannot be a utility statement)
		parse_stmt(cte);
		parse_with_validate(cte);

		// set cte returning columns
		auto ret = cte->ret;
		if (! ret)
			stmt_error(cte, start, "CTE statement must be DML or SELECT");

		// ensure that arguments count match
		if (cte->cte_columns.count > 0)
		{
		    if (cte->cte_columns.count != ret->columns.count)
				stmt_error(cte, start, "CTE arguments count does not match the returning arguments count");
		} else {
			columns_copy(&cte->cte_columns, &ret->columns);
		}

		// )
		stmt_expect(cte, ')');

		// ,
		if (! lex_if(lex, ','))
			break;
	}

	// stmt
	auto stmt = stmt_allocate(self, &self->lex, block);
	stmts_add(&block->stmts, stmt);
	parse_stmt(stmt);
	parse_with_validate(stmt);
}
