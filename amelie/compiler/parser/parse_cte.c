
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

static void
parse_cte_args(Stmt* self, Cte* cte)
{
	// )
	if (stmt_if(self, ')'))
		return;

	Ast* args_tail = NULL;
	for (;;)
	{
		// name
		auto name = stmt_if(self, KNAME);
		if (! name)
			error("WITH name (<name> expected");

		// ensure argument is unique
		for (auto at = cte->args; at; at = at->next) {
			if (str_compare(&at->string, &name->string))
				error("WITH name (<%.*s> argument redefined", str_size(&at->string),
				      str_of(&at->string));
		}

		// add argument to the list
		if (cte->args == NULL)
			cte->args = name;
		else
			args_tail->next = name;
		args_tail = name;
		cte->args_count++;

		// ,
		if (! stmt_if(self, ','))
			break;
	}

	// )
	if (! stmt_if(self, ')'))
		error("WITH name (<)> expected");
}

Cte*
parse_cte(Stmt* self, bool with, bool with_args)
{
	unused(with);

	// name [(args)]
	auto name = stmt_if(self, KNAME);
	if (! name)
		error("WITH <name> expected");

	// reuse existing cte variable (columns are not compared)
	auto cte = cte_list_find(self->cte_list, &name->string);
	if (cte)
		error("CTE <%.*s> redefined", str_size(&name->string),
		      str_of(&name->string));

	cte = cte_list_add(self->cte_list, name, self->order);
	if (! with_args)
		return cte;

	// (args)
	if (stmt_if(self, '('))
		parse_cte_args(self, cte);

	return cte;
}
