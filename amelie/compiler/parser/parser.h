#pragma once

//
// amelie.
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
	int          explain;
	StmtList     stmt_list;
	Stmt*        stmt;
	CodeData*    data;
	Json         json;
	Lex          lex;
	FunctionMgr* function_mgr;
	Local*       local;
	Db*          db;
};

void parser_init(Parser*, Db*, FunctionMgr*, CodeData*);
void parser_reset(Parser*);
void parser_free(Parser*);
