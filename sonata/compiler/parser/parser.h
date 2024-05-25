#pragma once

//
// sonata.
//
// Real-Time SQL Database.
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
	int       explain;
	StmtList  stmt_list;
	Stmt*     stmt;
	CodeData* data;
	Json      json;
	Lex       lex;
	Db*       db;
};

void parser_init(Parser*, Db*, CodeData*);
void parser_reset(Parser*);
void parser_free(Parser*);
