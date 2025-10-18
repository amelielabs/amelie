
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

void
parse_return(Stmt* self)
{
	auto stmt = ast_return_allocate();
	self->ast = &stmt->ast;

	// RETURN ;
	auto eos = stmt_if(self, ';');
	if (eos)
	{
		stmt_push(self, eos);
		return;
	}

	// RETURN var [FORMAT spec]
	auto name = stmt_expect(self, KNAME);

	auto var = namespace_find_var(self->block->ns, &name->string);
	if (! var)
		stmt_error(self, name, "variable not found");
	stmt->var = var;
	if (var->writer)
		deps_add_var(&self->deps, var->writer, var);

	// define return columns
	if (var->type == TYPE_STORE)
	{
		stmt->columns = &var->columns;
	} else
	{
		// allocate select to keep returning columns
		auto select = ast_select_allocate(self, NULL, NULL);
		auto columns = &select->ret.columns;

		// create variable column
		auto column = column_allocate();
		column_set_name(column, var->name);
		column_set_type(column, var->type, type_sizeof(var->type));
		columns_add(columns, column);
		stmt->columns = columns;
	}

	// [FORMAT type]
	if (stmt_if(self, KFORMAT))
	{
		auto type = stmt_expect(self, KSTRING);
		stmt->format = type->string;
	} else
	{
		stmt->format = *self->parser->local->format;
	}
}
