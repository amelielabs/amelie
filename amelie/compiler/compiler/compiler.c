
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
#include <amelie_db>
#include <amelie_repl>
#include <amelie_vm>
#include <amelie_parser.h>
#include <amelie_copy.h>
#include <amelie_plan.h>
#include <amelie_compiler.h>

void
compiler_init(Compiler* self, SetCache* set_cache)
{
	self->program           = NULL;
	self->program_args      = NULL;
	self->program_returning = NULL;
	self->program_udf       = NULL;
	self->program_explain   = NULL;
	self->program_profile   = NULL;
	self->set_cache         = set_cache;
	self->current           = NULL;
	self->code              = NULL;
	self->code_data         = NULL;
	self->origin            = ORIGIN_FRONTEND;
	parser_init(&self->parser, self->set_cache);
	rmap_init(&self->map);
}

void
compiler_free(Compiler* self)
{
	parser_free(&self->parser);
	rmap_free(&self->map);
}

void
compiler_reset(Compiler* self)
{
	self->program           = NULL;
	self->program_args      = NULL;
	self->program_returning = NULL;
	self->program_udf       = NULL;
	self->program_explain   = NULL;
	self->program_profile   = NULL;
	self->code              = NULL;
	self->code_data         = NULL;
	self->current           = NULL;
	parser_reset(&self->parser);
	rmap_reset(&self->map);
}

void
compiler_set(Compiler* self, Local* local, Program* program)
{
	self->program   = program;
	self->code      = &self->program->code;
	self->code_data = &self->program->code_data;
	parser_prepare(&self->parser, local);
}

static void
compiler_prepare(Compiler* self)
{
	auto stmt = compiler_stmt(self);
	if (! stmt)
		return;

	// set explain/profile
	self->program_explain = self->parser.explain;
	self->program_profile = self->parser.profile;

	// set returning columns
	auto ret = compiler_main(self)->stmts.list_tail->ret;
	if (ret && ret->columns.count > 0)
		self->program_returning = &ret->columns;

	if (stmt->id != STMT_EXECUTE)
		return;

	// handle EXECUTE
	//
	// replace program with the udf program
	//
	auto execute = ast_execute_of(stmt->ast);
	auto udf     = execute->udf;
	self->program      = (Program*)udf->data;
	self->program_udf  = udf;
	self->program_args = set_value(execute->args, 0);

	// set udf returning columns for table results
	if (udf->config->type == TYPE_STORE)
		self->program_returning = &udf->config->returning;
}

hot void
compiler_parse(Compiler* self, Str* text)
{
	parse(&self->parser, self->program, text);
	compiler_prepare(self);
}

void
compiler_parse_udf(Compiler* self, Udf* udf)
{
	parse_udf(&self->parser, self->program, udf);
	compiler_prepare(self);
}

hot void
compiler_parse_copy(Compiler* self, Str* user, Str* rel,
                    Str*      content_type,
                    Str*      content)
{
	parse_copy(&self->parser, self->program, user, rel,
	           content_type, content);
	compiler_prepare(self);
}

hot void
compiler_emit(Compiler* self)
{
	// ddl or dml/query
	if (stmt_is_utility(compiler_stmt(self)))
	{
		emit_utility(self);
	} else
	{
		emit(self);
	}

	// set the max number of registers used
	auto program = self->program;
	code_set_regs(&program->code, self->map.count);
}
