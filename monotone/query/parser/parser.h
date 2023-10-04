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
	void* parser;
};

void parser_init(Parser*);
void parser_free(Parser*);
void parser_reset(Parser*);
void parser_run(Parser*, Str*);
