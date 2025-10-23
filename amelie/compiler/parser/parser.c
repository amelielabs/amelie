
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
parser_init(Parser* self, Local* local, SetCache* set_cache)
{
	self->explain   = false;
	self->profile   = false;
	self->program   = NULL;
	self->set_cache = set_cache;
	self->local     = local;
	namespaces_init(&self->nss);
	lex_init(&self->lex, keywords_alpha);
	uri_init(&self->uri);
	json_init(&self->json);
}

void
parser_reset(Parser* self)
{
	self->explain = false;
	self->profile = false;
	self->program = NULL;
	for (auto ns = self->nss.list; ns; ns = ns->next)
	{
		vars_free(&ns->vars);
		for (auto block = ns->blocks.list; block; block = block->next)
		{
			auto stmt = block->stmts.list;
			while (stmt)
			{
				auto next = stmt->next;
				parse_stmt_free(stmt);
				stmt = next;
			}
		}
	}
	namespaces_init(&self->nss);

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
