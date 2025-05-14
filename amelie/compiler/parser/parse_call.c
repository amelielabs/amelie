
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
	auto path = stmt_next(self);
	stmt_push(self, path);
	Str schema;
	Str name;
	if (! parse_target(self, &schema, &name))
		stmt_error(self, path, "procedure name expected");

	// find and inline procedure
	auto parser = self->parser;
	stmt->proc = proc_mgr_find(&parser->db->proc_mgr, &schema, &name, false);
	if (! stmt->proc)
		stmt_error(self, path, "procedure not found");
	auto proc = stmt->proc;

	// ensure there are no recursion
	for (auto at = self->scope; at; at = at->parent)
		if (at->call == stmt->proc)
			stmt_error(self, path, "CALL recursion is not supported");

	// track call dependency
	access_add(&self->parser->program->access, &proc->rel, ACCESS_CALL);

	// (args)
	stmt_expect(self, '(');
	auto args = parse_expr_args(self, NULL, ')', false);

	// validate arguments count
	auto argc_call = args->integer;
	auto argc = proc->config->columns.count;
	if (argc_call != argc)
	{
		if (argc == 0)
			stmt_error(self, path, "procedure has no arguments");
		stmt_error(self, path, "expected %d argument%s", argc,
		           argc > 1 ? "s": "");
	}
	stmt->ast.r = args;

	// create scope
	auto scope = scopes_add(&parser->scopes, self->scope, proc);
	stmt->scope = scope;

	// create arguments as variables
	list_foreach(&proc->config->columns.list)
	{
		auto column = list_at(Column, link);
		auto var = vars_add(&scope->vars, &column->name);
		var->type = column->type;
	}

	// create variables
	list_foreach(&proc->config->vars.list)
	{
		auto column = list_at(Column, link);
		auto var = vars_add(&scope->vars, &column->name);
		var->type = column->type;
	}

	// inline procedure
	Lex lex_current = *self->lex;
	lex_reset(self->lex);
	lex_start(self->lex, &proc->config->text);
	parse_scope(parser, scope);
	*self->lex = lex_current;
}
