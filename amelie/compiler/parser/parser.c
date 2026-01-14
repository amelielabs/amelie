
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
#include <amelie_storage>
#include <amelie_repl>
#include <amelie_vm>
#include <amelie_parser.h>

void
parser_init(Parser* self, Local* local, SetCache* set_cache)
{
	self->explain   = false;
	self->profile   = false;
	self->program   = NULL;
	self->set_cache = set_cache;
	self->db        = &local->db;
	self->local     = local;
	namespaces_init(&self->nss);
	lex_init(&self->lex, keywords_alpha);
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
	json_reset(&self->json);
}

void
parser_free(Parser* self)
{
	parser_reset(self);
	json_free(&self->json);
}
