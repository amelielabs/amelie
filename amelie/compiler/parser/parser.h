#pragma once

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

typedef struct Parser Parser;

struct Parser
{
	bool      begin;
	bool      commit;
	Stmt*     stmt;
	Stmts     stmts;
	Scopes    scopes;
	Program*  program;
	SetCache* values_cache;
	Uri       uri;
	Json      json;
	Lex       lex;
	Local*    local;
};

void parser_init(Parser*, Local*, SetCache*);
void parser_reset(Parser*);
void parser_free(Parser*);

static inline Scope*
parser_scope(Parser* self)
{
	return self->scopes.list_tail;
}
