
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
#include <amelie_value.h>
#include <amelie_store.h>
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
#include <amelie_executor.h>
#include <amelie_vm.h>
#include <amelie_parser.h>

static void
parse_cte_args(Stmt* self, Columns* columns, const char* clause)
{
	// )
	if (stmt_if(self, ')'))
		return;

	for (;;)
	{
		// name
		auto name = stmt_if(self, KNAME);
		if (! name)
			error("%s name (<name> expected", clause);

		// ensure arg does not exists
		auto arg = columns_find(columns, &name->string);
		if (arg)
			error("<%.*s> cte argument redefined", str_size(&name->string),
			      str_of(&name->string));

		// add argument
		arg = column_allocate();
		columns_add(columns, arg);
		column_set_name(arg, &name->string);

		// ,
		if (! stmt_if(self, ','))
			break;
	}

	// )
	if (! stmt_if(self, ')'))
		error("%s name (<)> expected", clause);
}

void
parse_cte(Stmt* self, bool with, bool with_args)
{
	const char* clause = with? "WITH": "INTO";
	if (self->cte)
		error("%s CTE redefined", clause);

	// name [(args)]
	auto name = stmt_if(self, KNAME);
	if (! name)
		error("%s <name> expected", clause);

	Columns* columns_ref;
	Columns  columns;
	columns_init(&columns);
	guard(columns_free, &columns);

	// reuse existing cte variable (columns are not compared)
	self->cte = cte_list_find(self->cte_list, &name->string);
	if (self->cte)
	{
		// columns are ignored on reused
		columns_ref = &columns;
	} else
	{
		// add new cte definition
		self->cte = cte_list_add(self->cte_list, name, self->order);
		columns_ref = &self->cte->columns;
	}

	if (! with_args)
		return;

	// (args)
	if (stmt_if(self, '('))
		parse_cte_args(self, columns_ref, clause);
}
