
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

void
parse_execute(Stmt* self)
{
	// EXECUTE [schema.]procedure_name[(expr, ...)]
	auto stmt = ast_execute_allocate();
	self->ast = &stmt->ast;

	// read schema/name
	auto path = stmt_next(self);
	stmt_push(self, path);
	Str schema;
	Str name;
	if (! parse_target(self, &schema, &name))
		stmt_error(self, path, "procedure name expected");

	// find and inline procedure
	auto parser = self->parser;
	auto proc = proc_mgr_find(&parser->db->proc_mgr, &schema, &name, false);
	if (! proc)
		stmt_error(self, path, "procedure not found");
	stmt->proc = proc;

	// (
	stmt_expect(self, '(');

	// prepare registers
	Program* program = proc->data;
	reg_prepare(parser->regs, program->code.regs);

	// read arguments and emit directly as registers values
	list_foreach(&proc->config->columns.list)
	{
		auto column = list_at(Column, link);
		parse_value(self, column, reg_at(parser->regs, column->order));

		// ,
		if (! list_is_last(&proc->config->columns.list, &column->link))
			stmt_expect(self, ',');
	}

	// )
	stmt_expect(self, ')');
}
