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

enum
{
	EXPLAIN_NONE    = 0,
	EXPLAIN         = 1,
	EXPLAIN_PROFILE = 2
};

struct Parser
{
	int          explain;
	Stmt*        stmt;
	Stmts        stmts;
	Ctes         ctes;
	Vars         vars;
	Columns*     args;
	Program*     program;
	SetCache*    values_cache;
	Uri          uri;
	Json         json;
	Lex          lex;
	FunctionMgr* function_mgr;
	Local*       local;
	Db*          db;
};

void parser_init(Parser*, Db*, Local*, FunctionMgr*, SetCache*, Program*);
void parser_reset(Parser*);
void parser_free(Parser*);

static inline bool
parser_is_profile(Parser* self)
{
	return self->explain == (EXPLAIN|EXPLAIN_PROFILE);
}
