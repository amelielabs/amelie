#pragma once

//
// indigo
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
	int  explain;
	List stmts;
	int  stmts_count;
	Lex  lex;
	Db*  db;
};

void parser_init(Parser*, Db*);
void parser_reset(Parser*);
bool parser_has_utility(Parser*);
