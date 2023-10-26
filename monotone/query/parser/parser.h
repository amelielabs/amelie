#pragma once

//
// monotone
//
// SQL OLTP database
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
	bool in_transaction;
	bool complete;
	int  explain;
	List stmts;
	int  stmts_count;
	Lex  lex;
	Db*  db;
};

void parser_init(Parser*, Db*);
void parser_reset(Parser*);
