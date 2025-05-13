
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
parser_init(Parser*      self,
            Db*          db,
            Local*       local,
            FunctionMgr* function_mgr,
            SetCache*    values_cache,
            Program*     program,
            Reg*         regs)
{
	self->explain      = EXPLAIN_NONE;
	self->begin        = false;
	self->commit       = false;
	self->execute      = false;
	self->stmt         = NULL;
	self->program      = program;
	self->regs         = regs;
	self->values_cache = values_cache;
	self->function_mgr = function_mgr;
	self->local        = local;
	self->db           = db;
	stmts_init(&self->stmts);
	scopes_init(&self->scopes);
	lex_init(&self->lex, keywords_alpha);
	uri_init(&self->uri);
	json_init(&self->json);
}

void
parser_reset(Parser* self)
{
	self->explain = EXPLAIN_NONE;
	self->begin   = false;
	self->commit  = false;
	self->execute = false;
	self->stmt    = NULL;
	auto stmt = self->stmts.list;
	while (stmt)
	{
		auto next = stmt->next;
		parse_stmt_free(stmt);
		stmt = next;
	}
	stmts_init(&self->stmts);
	scopes_reset(&self->scopes);
	lex_reset(&self->lex);
	uri_reset(&self->uri);
	json_reset(&self->json);
}

void
parser_free(Parser* self)
{
	parser_reset(self);
	uri_free(&self->uri);
	json_free(&self->json);
}
