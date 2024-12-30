
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
#include <amelie_set.h>
#include <amelie_content.h>
#include <amelie_executor.h>
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
		auto name = stmt_if(self, KNAME);
		if (! name)
			error("WITH name (<name> expected");

		// ensure argument is unique
		auto arg = columns_find(&self->cte_args, &name->string);
		if (arg)
			error("WITH name (<%.*s> argument redefined", str_size(&name->string),
			      str_of(&name->string));

		// add argument to the list
		arg = column_allocate();
		column_set_name(arg, &name->string);
		columns_add(&self->cte_args, arg);

		// ,
		if (! stmt_if(self, ','))
			break;
	}

	// )
	if (! stmt_if(self, ')'))
		error("WITH name (<)> expected");
}

void
parse_cte(Stmt* self)
{
	// name [(args)]
	auto name = stmt_if(self, KNAME);
	if (! name)
		error("WITH <name> expected");

	// reuse existing cte variable (columns are not compared)
	auto cte = stmt_list_find(self->stmt_list, &name->string);
	if (cte)
		error("CTE <%.*s> redefined", str_size(&name->string),
		      str_of(&name->string));
	self->cte_name = name;

	// (args)
	if (stmt_if(self, '('))
		parse_cte_args(self);
}
