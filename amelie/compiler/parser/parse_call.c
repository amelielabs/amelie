
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
parse_call(Stmt* self)
{
	// CALL [schema.]procedure_name[(expr, ...)]
	auto stmt = ast_call_allocate();
	self->ast = &stmt->ast;

	// read schema/name
	Str schema;
	Str name;
	if (! parse_target(self, &schema, &name))
		stmt_error(self, NULL, "proceure name expected");

	// find and inline procedure
	auto parser = self->parser;
	stmt->proc = proc_mgr_find(&parser->db->proc_mgr, &schema, &name, false);
	if (! stmt->proc)
		stmt_error(self,  NULL, "procedure not found");

	// (args)
	stmt_expect(self, '(');
	auto args = parse_expr_args(self, NULL, ')', false);

	// validate arguments count
	auto argc_call = args->integer;
	auto argc = stmt->proc->config->columns.count;
	if (argc_call != argc)
	{
		if (argc == 0)
			stmt_error(self,  NULL, "procedure has no arguments");
		stmt_error(self,  NULL, "expected %d argument%s", argc,
		           argc > 1 ? "s": "");
	}
	stmt->ast.r = args;

	// create arguments as variables
	auto scope = scopes_add(&parser->scopes);
	stmt->scope = scope;

	list_foreach(&stmt->proc->config->columns.list)
	{
		auto column = list_at(Column, link);
		auto var = vars_add(&scope->vars, &column->name);
		var->type = column->type;
	}

	// inline procedure
	Lex lex_current = *self->lex;
	lex_reset(self->lex);
	lex_start(self->lex, &stmt->proc->config->text);
	parse_scope(parser, scope);
	*self->lex = lex_current;

	// CALL_RETURN
	auto ret = stmt_allocate(parser, self->lex, scope);
	ret->id  = STMT_CALL_RETURN;
	ret->ast = &stmt->ast;
	stmts_add(&parser->stmts, ret);
}
