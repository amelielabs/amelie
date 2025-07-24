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
	bool      execute;
	Stmt*     stmt;
	Stmts     stmts;
	Scopes    scopes;
	Program*  program;
	Reg*      regs;
	SetCache* values_cache;
	Uri       uri;
	Json      json;
	Lex       lex;
	Local*    local;
};

void parser_init(Parser*, Local*, SetCache*, Program*, Reg*);
void parser_reset(Parser*);
void parser_free(Parser*);

static inline Scope*
parser_scope(Parser* self)
{
	return self->scopes.list_tail;
}
