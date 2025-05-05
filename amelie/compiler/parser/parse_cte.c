
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

static void
parse_cte_args(Stmt* self)
{
	// )
	if (stmt_if(self, ')'))
		return;

	for (;;)
	{
		// name
		auto name = stmt_expect(self, KNAME);

		// ensure argument is unique
		auto arg = columns_find(&self->cte->args, &name->string);
		if (arg)
			stmt_error(self, name, "argument redefined");

		// add argument to the list
		arg = column_allocate();
		column_set_name(arg, &name->string);
		columns_add(&self->cte->args, arg);

		// ,
		if (! stmt_if(self, ','))
			break;
	}

	// )
	stmt_expect(self, ')');
}

void
parse_cte(Stmt* self)
{
	// name [(args)]
	auto name = stmt_expect(self, KNAME);

	// ensure CTE is not redefined
	auto cte = ctes_find(&self->scope->ctes, &name->string);
	if (cte)
		stmt_error(self, name, "CTE is redefined");

	cte = ctes_add(&self->scope->ctes, self, &name->string);
	self->cte = cte;

	// (args)
	if (stmt_if(self, '('))
		parse_cte_args(self);
}
