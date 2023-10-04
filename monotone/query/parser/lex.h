#pragma once

//
// monotone
//
// SQL OLTP database
//

typedef struct Lex Lex;

struct Lex
{
	char*       pos;
	char*       end;
	Keyword**   keywords;
	const char* prefix;
};

void lex_init(Lex*, Keyword**);
void lex_reset(Lex*);
void lex_start(Lex*, Str*);
Ast* lex_next(Lex*);
