#pragma once

//
// monotone
//
// SQL OLTP database
//

typedef struct Parser Parser;

struct Parser
{
	Query query;
	Lex   lex;
	Db*   db;
};

void parser_init(Parser*, Db*);
void parser_free(Parser*);
void parser_reset(Parser*);
void parser_run(Parser*, Str*);
