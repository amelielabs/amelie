
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

#include <amelie_runtime>
#include <amelie_server>
#include <amelie_engine>
#include <amelie_storage>
#include <amelie_repl>
#include <amelie_vm>
#include <amelie_parser.h>

void
parse_token_create(Stmt* self)
{
	// CREATE TOKEN name [EXPIRE interval]
	auto stmt = ast_token_create_allocate();
	self->ast = &stmt->ast;
	self->ret = &stmt->ret;

	// name
	stmt->user = stmt_expect(self, KNAME);

	// EXPIRE
	if (stmt_if(self, KEXPIRE))
	{
		// value
		stmt->expire = stmt_expect(self, KSTRING);
	}

	// set returning column
	auto column = column_allocate();
	column_set_name(column, &stmt->user->string);
	column_set_type(column, TYPE_JSON, -1);
	columns_add(&stmt->ret.columns, column);
}
