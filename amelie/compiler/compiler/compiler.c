
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
#include <amelie_compiler.h>

void
compiler_init(Compiler*    self,
              Db*          db,
              Local*       local,
              FunctionMgr* function_mgr,
              Udf*         udf)
{
	self->program       = program_allocate();
	self->code          = &self->program->code;
	self->code_data     = &self->program->code_data;
	self->args          = NULL;
	self->udf           = udf;
	self->current       = NULL;
	self->current_scope = NULL;
	self->last          = NULL;
	self->db            = db;
	set_cache_init(&self->values_cache);
	parser_init(&self->parser, db, local, function_mgr, &self->values_cache,
	             self->program);
	rmap_init(&self->map);

}

void
compiler_free(Compiler* self)
{
	if (self->program)
		program_free(self->program);
	parser_free(&self->parser);
	set_cache_free(&self->values_cache);
	rmap_free(&self->map);
}

void
compiler_reset(Compiler* self)
{
	self->code          = &self->program->code;
	self->args          = NULL;
	self->current       = NULL;
	self->current_scope = NULL;
	self->last          = NULL;
	program_reset(self->program);
	parser_reset(&self->parser);
	rmap_reset(&self->map);
}

void
compiler_parse(Compiler* self, Str* text)
{
	parse(&self->parser, text);
	self->current_scope = self->parser.scopes.list;
}

void
compiler_parse_import(Compiler*    self, Str* text, Str* uri,
                      EndpointType type)
{
	parse_import(&self->parser, text, uri, type);
	self->current_scope = self->parser.scopes.list;
}

hot void
compiler_emit(Compiler* self)
{
	auto main = self->parser.scopes.list;
	emit_scope(self, main);

	// no statements (last statement always returns)
	if (! main->stmts.list)
		op0(self, CRET);

	// set the max number of registers used
	code_set_regs(&self->program->code, self->map.count);
	code_set_regs(&self->program->code_backend, self->map.count);
}
