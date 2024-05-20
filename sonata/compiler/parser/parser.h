#pragma once

//
// sonata.
//
// SQL Database for JSON.
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
	int      explain;
	StmtList stmt_list;
	Stmt*    stmt;
	Lex      lex;
	Db*      db;
};

void parser_init(Parser*, Db*);
void parser_reset(Parser*);
